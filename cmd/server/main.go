package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/enrich"
	"github.com/johneliud/log_ingestion_engine/internal/handler"
	"github.com/johneliud/log_ingestion_engine/internal/metrics"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	"github.com/johneliud/log_ingestion_engine/internal/pipeline"
	"github.com/johneliud/log_ingestion_engine/internal/queue"
	"github.com/johneliud/log_ingestion_engine/internal/queue/consumer"
	"github.com/johneliud/log_ingestion_engine/internal/ratelimit"
	"github.com/johneliud/log_ingestion_engine/internal/router"
	"github.com/johneliud/log_ingestion_engine/internal/spool"
	"github.com/johneliud/log_ingestion_engine/internal/storage"
)

const minimumRateLimiterIdleTimeout = time.Second

// The delay between two attempts to push a spooled chunk into a full pipeline.
const replayRetryDelay = 50 * time.Millisecond

// Names one background component and the channel that reports when it stops.
type backgroundWork struct {
	name       string
	completion <-chan struct{}
}

// Holds the wired request router and the background work the application owns.
type serverApplication struct {
	requestRouter         http.Handler
	inFlightRequests      *sync.WaitGroup
	cancelPipelineWork    context.CancelFunc
	cancelStorageWork     context.CancelFunc
	pipelineCompletion    <-chan struct{}
	backgroundCompletions []backgroundWork
	deadLetterCompletion  <-chan struct{}
	batchWriter           *storage.BatchWriter
	deadLetterWriter      *storage.DeadLetterWriter
	sqliteLogStore        *storage.SQLiteLogStore
	publisher             *queue.Publisher
	rawLogsQueue          *pipeline.RawLogsQueue
	ingestionSpool        *spool.Spool
	logEnricher           *enrich.LogEnricher
	serviceRouter         *router.ServiceRouter
	defaultDestination    model.Destination
	currentTimeProvider   func() time.Time
	shutdownOnce          sync.Once
	shutdownError         error
}

// Loads the application configuration, serves the wired application and exits only after a clean shutdown.
func main() {
	applicationContext, applicationContextCancellation := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer applicationContextCancellation()

	applicationConfiguration, configurationError := config.NewApplicationConfiguration()
	if configurationError != nil {
		slog.Error("loading the application configuration failed", "configuration_error", configurationError)
		os.Exit(1)
	}
	logConfigurationSummary(applicationConfiguration)

	serverApplication, applicationError := newServerApplication(applicationConfiguration)
	if applicationError != nil {
		slog.Error("building the application failed", "application_error", applicationError)
		os.Exit(1)
	}

	httpServer := newHTTPServer(applicationConfiguration, serverApplication.requestRouter)
	serverListenErrorChannel := startHTTPServer(httpServer)
	serveError := waitForTheServerToStop(applicationContext, serverListenErrorChannel)

	shutdownContext, shutdownContextCancellation := context.WithTimeout(context.Background(), applicationConfiguration.ShutdownGracePeriod)
	defer shutdownContextCancellation()
	httpShutdownError := shutdownHTTPServer(httpServer, shutdownContext)
	shutdownError := serverApplication.shutdownBackgroundWork(shutdownContext)
	if serveError != nil {
		slog.Error("http server stopped unexpectedly", "server_error", serveError)
		os.Exit(1)
	}
	if httpShutdownError != nil {
		slog.Error("stopping the http server failed", "shutdown_error", httpShutdownError)
		os.Exit(1)
	}
	if shutdownError != nil {
		slog.Error("stopping the background work failed", "shutdown_error", shutdownError)
		os.Exit(1)
	}
	slog.Info("shutdown complete", "application_environment", applicationConfiguration.ApplicationEnvironment)
}

// Builds every component from the configuration, starts the background work and returns the wired application.
func newServerApplication(applicationConfiguration config.ApplicationConfiguration) (*serverApplication, error) {
	pipelineContext, cancelPipelineWork := context.WithCancel(context.Background())
	storageContext, cancelStorageWork := context.WithCancel(context.Background())

	metricsRegistry := metrics.NewMetrics(applicationConfiguration.MetricsThroughputWindow, nil)

	rawLogsQueue := pipeline.NewRawLogsQueue(applicationConfiguration.RawLogsBufferSize, applicationConfiguration.ChannelSendTimeout)

	spoolFilePath := applicationConfiguration.SpoolFilePath
	if spoolFilePath == "" {
		spoolFilePath = filepath.Join(filepath.Dir(applicationConfiguration.DatabaseDirectoryPath), config.DefaultSpoolFilePath)
	}
	ingestionSpool, spoolError := spool.NewSpool(spoolFilePath)
	if spoolError != nil {
		cancelPipelineWork()
		cancelStorageWork()
		return nil, fmt.Errorf("creating ingestion spool %q: %w", spoolFilePath, spoolError)
	}

	logEnricher := enrich.NewLogEnricher(applicationConfiguration.ApplicationEnvironment, nil)

	serviceRouter, routerError := router.NewServiceRouter(applicationConfiguration.RoutingRules, applicationConfiguration.DefaultDestination)
	if routerError != nil {
		cancelPipelineWork()
		cancelStorageWork()
		return nil, routerError
	}

	publisher := queue.NewPublisher(
		applicationConfiguration,
		applicationConfiguration.RawLogsBufferSize,
		queue.WithPublishedMessageRecorder(metricsRegistry),
		queue.WithPublishSessionCount(applicationConfiguration.PipelineWorkerCount),
	)

	deadLetterWriter := storage.NewDeadLetterWriter(applicationConfiguration)

	batchConsumer := pipeline.NewBatchConsumer(pipeline.BatchConsumerDependencies{
		RawLogsQueue:       rawLogsQueue,
		ConsumerBatchSize:  applicationConfiguration.ConsumerBatchSize,
		WorkerCount:        applicationConfiguration.PipelineWorkerCount,
		Enricher:           logEnricher,
		Router:             serviceRouter,
		QueuePublisher:     publisher,
		DeadLetterSink:     deadLetterWriter.Submit,
		DefaultDestination: model.Destination(applicationConfiguration.DefaultDestination),
	})

	sqliteLogStore, storeError := storage.NewSQLiteLogStore(applicationConfiguration)
	if storeError != nil {
		cancelPipelineWork()
		cancelStorageWork()
		return nil, storeError
	}

	batchWriter := storage.NewBatchWriter(storage.BatchWriterDependencies{
		ApplicationContext: storageContext,
		WriteBatchSize:     applicationConfiguration.WriteBatchSize,
		RetryBackoffDelays: applicationConfiguration.WriteRetryBackoffDelays,
		StoreBatch:         sqliteLogStore.StoreBatch,
		DeadLetterSink:     deadLetterWriter.Submit,
		MetricsRecorder:    metricsRegistry,
		Spool:              ingestionSpool,
	})

	rateLimiter := ratelimit.NewPerIPRateLimiterRegistry(
		applicationConfiguration.RateLimitPerSecond,
		rateLimiterIdleTimeout(applicationConfiguration),
		nil,
	)

	ingestionHandler := handler.NewLogIngestionHandler(
		applicationConfiguration.MaximumBodySizeInBytes,
		handler.WithRawLogPusher(rawLogsQueue),
		handler.WithSpool(ingestionSpool),
		handler.WithTrustedProxies(applicationConfiguration.TrustedProxies),
		handler.WithIngestionMetricsRecorder(metricsRegistry),
	)
	rateLimitMiddleware := ratelimit.NewRateLimitMiddleware(
		rateLimiter,
		handler.WriteJSONError,
		applicationConfiguration.TrustedProxies,
		ratelimit.WithRateLimitedRequestRecorder(metricsRegistry),
	)

	requestRouter := buildRequestRouter(applicationConfiguration, rateLimitMiddleware(ingestionHandler), metricsRegistry)

	var inFlightRequests sync.WaitGroup
	trackedRequestRouter := trackRunningRequests(requestRouter, &inFlightRequests)

	fallbackProcessor := consumer.NewFallbackProcessor(publisher.FallbackChannel(), batchWriter)

	consumerSupervisor := consumer.NewSupervisor(consumer.SupervisorDependencies{
		ApplicationConfiguration: applicationConfiguration,
		StorageHandler:           batchWriter,
		DestinationQueues:        destinationQueues(),
		ReconnectBackoffDelays:   applicationConfiguration.RabbitMQReconnectBackoffDelays,
	})

	if connectError := publisher.Connect(); connectError != nil {
		slog.Warn("rabbitmq is unavailable at startup, logs flow through the in-memory fallback", "error", connectError)
	}

	flushTrigger, flushTriggerCompletion := startFlushTrigger(storageContext, applicationConfiguration.WriteFlushInterval)

	// Start the pipeline before replaying the spool so replayed logs drain as they are pushed; replaying into a queue with no consumers would fill it and drop every log beyond its capacity.
	pipelineCompletion := batchConsumer.Start(pipelineContext)

	unackedSpooledLogs, replayError := ingestionSpool.Unacked()
	if replayError != nil {
		slog.Error("replaying spooled logs failed", "error", replayError)
	}
	replaySpooledLogs(rawLogsQueue, unackedSpooledLogs, applicationConfiguration.ConsumerBatchSize, pipelineContext)

	backgroundCompletions := []backgroundWork{
		{name: "the publisher reconnect loop", completion: publisher.RunReconnectLoop(storageContext)},
		{name: "the batch writer", completion: batchWriter.Run(storageContext, flushTrigger)},
		{name: "the fallback processor", completion: fallbackProcessor.Start(storageContext)},
		{name: "the consumer supervisor", completion: consumerSupervisor.Start(storageContext)},
		{name: "the flush trigger", completion: flushTriggerCompletion},
		{name: "the rate limiter idle cleanup", completion: rateLimiter.StartIdleCleanup(storageContext, applicationConfiguration.RateLimitCleanupInterval)},
	}
	deadLetterCompletion := deadLetterWriter.Run()

	return &serverApplication{
		requestRouter:         trackedRequestRouter,
		inFlightRequests:      &inFlightRequests,
		cancelPipelineWork:    cancelPipelineWork,
		cancelStorageWork:     cancelStorageWork,
		pipelineCompletion:    pipelineCompletion,
		backgroundCompletions: backgroundCompletions,
		deadLetterCompletion:  deadLetterCompletion,
		batchWriter:           batchWriter,
		deadLetterWriter:      deadLetterWriter,
		sqliteLogStore:        sqliteLogStore,
		publisher:             publisher,
		rawLogsQueue:          rawLogsQueue,
		ingestionSpool:        ingestionSpool,
		logEnricher:           logEnricher,
		serviceRouter:         serviceRouter,
		defaultDestination:    model.Destination(applicationConfiguration.DefaultDestination),
		currentTimeProvider:   time.Now,
	}, nil
}

// Counts the running requests so the shutdown can wait for the producers to stop.
func trackRunningRequests(next http.Handler, runningRequests *sync.WaitGroup) http.Handler {
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		runningRequests.Add(1)
		defer runningRequests.Done()
		next.ServeHTTP(responseWriter, request)
	})
}

// Replays the spooled logs into the pipeline. The logs were already accepted from their clients, so the replay waits for the pipeline to free capacity instead of dropping them.
func replaySpooledLogs(rawLogsQueue *pipeline.RawLogsQueue, spooledLogs []model.RawLog, chunkSize int, applicationContext context.Context) {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	for chunkStart := 0; chunkStart < len(spooledLogs); chunkStart += chunkSize {
		chunkEnd := min(chunkStart+chunkSize, len(spooledLogs))
		if pushError := rawLogsQueue.PushBatchWhenCapacityFrees(applicationContext, spooledLogs[chunkStart:chunkEnd]); pushError != nil {
			slog.Error("replaying spooled logs into the pipeline failed", "error", pushError, "chunk_start", chunkStart, "chunk_end", chunkEnd)
			return
		}
	}
}

// Builds the request router with the ingestion and metrics API routes wired.
func buildRequestRouter(applicationConfiguration config.ApplicationConfiguration, ingestionHandler http.Handler, metricsRegistry *metrics.Metrics) *http.ServeMux {
	requestRouter := http.NewServeMux()
	requestRouter.Handle("POST /logs", ingestionHandler)
	requestRouter.HandleFunc("GET /metrics.json", handler.NewMetricsHandler(metricsRegistry, applicationConfiguration))
	requestRouter.HandleFunc("GET /metrics/events", handler.NewMetricsEventsHandler(metricsRegistry, applicationConfiguration))
	return requestRouter
}

// Lists the effective startup settings without exposing any RabbitMQ credential.
func logConfigurationSummary(applicationConfiguration config.ApplicationConfiguration) {
	slog.Info("application configuration loaded",
		"http_port", applicationConfiguration.HTTPPort,
		"application_environment", applicationConfiguration.ApplicationEnvironment,
		"pipeline_worker_count", applicationConfiguration.PipelineWorkerCount,
		"consumer_batch_size", applicationConfiguration.ConsumerBatchSize,
		"raw_logs_buffer_size", applicationConfiguration.RawLogsBufferSize,
		"write_batch_size", applicationConfiguration.WriteBatchSize,
		"write_flush_interval", applicationConfiguration.WriteFlushInterval,
		"rate_limit_per_second", applicationConfiguration.RateLimitPerSecond,
		"default_destination", applicationConfiguration.DefaultDestination,
		"destinations", destinationNames(),
		"database_directory_path", applicationConfiguration.DatabaseDirectoryPath,
		"dead_letter_file_path", applicationConfiguration.DeadLetterFilePath,
		"rabbitmq_host", applicationConfiguration.RabbitMQHost,
		"rabbitmq_port", applicationConfiguration.RabbitMQPort,
	)
}

// Builds the HTTP server that serves the wired request router.
func newHTTPServer(applicationConfiguration config.ApplicationConfiguration, requestRouter http.Handler) *http.Server {
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", applicationConfiguration.HTTPPort),
		Handler: requestRouter,
	}
}

// Starts the HTTP server in the background and returns the channel that reports the listen error.
func startHTTPServer(httpServer *http.Server) <-chan error {
	serverListenErrorChannel := make(chan error, 1)
	go func() {
		serverListenErrorChannel <- httpServer.ListenAndServe()
	}()
	return serverListenErrorChannel
}

// Waits for the signal or a listen failure and returns the listen error when the server failed.
func waitForTheServerToStop(applicationContext context.Context, serverListenErrorChannel <-chan error) error {
	select {
	case serverListenError := <-serverListenErrorChannel:
		if !errors.Is(serverListenError, http.ErrServerClosed) {
			return serverListenError
		}
		return nil
	case <-applicationContext.Done():
		return nil
	}
}

// Stops accepting requests, closes the connections that outlast the deadline and reports the shutdown failure.
func shutdownHTTPServer(httpServer *http.Server, shutdownContext context.Context) error {
	shutdownError := httpServer.Shutdown(shutdownContext)
	if shutdownError == nil {
		return nil
	}

	slog.Warn("the http server did not stop within the shutdown grace period, closing the remaining connections", "error", shutdownError)
	if closeError := httpServer.Close(); closeError != nil {
		return errors.Join(shutdownError, closeError)
	}
	return shutdownError
}

// Stops the background work in order and closes the storage components.
func (application *serverApplication) shutdownBackgroundWork(shutdownContext context.Context) error {
	application.shutdownOnce.Do(func() {
		application.shutdownError = application.stopBackgroundWork(shutdownContext)
	})
	return application.shutdownError
}

// Cancels the pipeline, drains the raw logs, then stops the storage side and closes the storage components.
func (application *serverApplication) stopBackgroundWork(shutdownContext context.Context) error {
	runningRequestsError := application.waitForRunningRequests(shutdownContext)

	application.cancelPipelineWork()
	pipelineError := application.drainPipeline(shutdownContext)

	publisherError := application.publisher.Close()
	undrainedLogsError := application.deadLetterRemainingRawLogs()

	application.cancelStorageWork()
	waitError := application.waitForBackgroundWork(shutdownContext)
	flushError := application.batchWriter.FlushAll()
	storeError := application.sqliteLogStore.Close()
	deadLetterError := application.deadLetterWriter.Close()
	spoolCloseError := application.ingestionSpool.Close()
	deadLetterWaitError := application.waitForDeadLetterWriter(shutdownContext)
	return errors.Join(runningRequestsError, pipelineError, publisherError, undrainedLogsError, waitError, flushError, storeError, deadLetterError, deadLetterWaitError, spoolCloseError)
}

// Waits for the running requests to finish so no producer pushes into the raw logs after the drain.
func (application *serverApplication) waitForRunningRequests(shutdownContext context.Context) error {
	requestsCompleted := make(chan struct{})
	go func() {
		application.inFlightRequests.Wait()
		close(requestsCompleted)
	}()

	select {
	case <-requestsCompleted:
		return nil
	case <-shutdownContext.Done():
		slog.Warn("the running requests did not finish before the shutdown grace period expired")
		return shutdownContext.Err()
	}
}

// Waits for the pipeline to publish every accepted log, dead-lettering the logs that miss the grace period.
func (application *serverApplication) drainPipeline(shutdownContext context.Context) error {
	pipelineError := application.waitForCompletion(shutdownContext, application.pipelineCompletion, "the pipeline did not finish before the shutdown grace period expired")
	if pipelineError == nil {
		return nil
	}

	deadLetterError := application.deadLetterRemainingRawLogs()
	return errors.Join(pipelineError, deadLetterError)
}

// Dead-letters the raw logs the pipeline could not process so no accepted log is lost.
func (application *serverApplication) deadLetterRemainingRawLogs() error {
	undrainedLogs := application.rawLogsQueue.DrainRemainingLogs()
	if len(undrainedLogs) == 0 {
		return nil
	}

	slog.Warn("dead-lettering the logs the pipeline could not process before the shutdown", "log_count", len(undrainedLogs))
	var deadLetterError error
	for _, rawLog := range undrainedLogs {
		enrichedLog := application.logEnricher.Enrich(rawLog)
		routedLog, routeError := application.serviceRouter.Route(enrichedLog)
		if routeError != nil {
			slog.Error("routing the undrained log failed, using the default destination", "error", routeError, "batch_id", rawLog.BatchID)
			routedLog = enrichedLog
			routedLog.Destination = application.defaultDestination
		}

		deadLetterEntry := model.DeadLetterEntry{
			Log:          routedLog,
			ErrorMessage: "the application shut down before the log was processed",
			FailedAt:     application.currentTimeProvider(),
		}
		if submitError := application.deadLetterWriter.Submit(deadLetterEntry); submitError != nil {
			slog.Error("writing the dead letter entry failed", "error", submitError, "batch_id", rawLog.BatchID)
			deadLetterError = errors.Join(deadLetterError, submitError)
		} else if application.ingestionSpool != nil {
			application.ingestionSpool.Ack(rawLog.LogID)
		}
	}
	return deadLetterError
}

// Waits for one background component to finish before the shutdown context expires.
func (application *serverApplication) waitForCompletion(shutdownContext context.Context, completion <-chan struct{}, warningMessage string) error {
	select {
	case <-completion:
		return nil
	case <-shutdownContext.Done():
		slog.Warn(warningMessage)
		return shutdownContext.Err()
	}
}

// Waits for every background component to finish before the shutdown context expires.
func (application *serverApplication) waitForBackgroundWork(shutdownContext context.Context) error {
	var waitError error
	for _, backgroundComponent := range application.backgroundCompletions {
		select {
		case <-backgroundComponent.completion:
		case <-shutdownContext.Done():
			slog.Warn("background work did not finish before the shutdown grace period expired", "background_work", backgroundComponent.name)
			waitError = errors.Join(waitError, shutdownContext.Err())
		}
	}
	return waitError
}

// Waits for the dead letter writer to finish draining after it was closed.
func (application *serverApplication) waitForDeadLetterWriter(shutdownContext context.Context) error {
	select {
	case <-application.deadLetterCompletion:
		return nil
	case <-shutdownContext.Done():
		slog.Warn("the dead letter writer did not finish before the shutdown grace period expired")
		return shutdownContext.Err()
	}
}

// Returns the queue and destination pairs for the configured destinations.
func destinationQueues() []consumer.DestinationQueue {
	return []consumer.DestinationQueue{
		{QueueName: config.QueueService1Name, Destination: model.DestinationService1},
		{QueueName: config.QueueService2Name, Destination: model.DestinationService2},
		{QueueName: config.QueueService3Name, Destination: model.DestinationService3},
	}
}

// Returns the names of the destinations the service routes to.
func destinationNames() []string {
	destinationQueueNames := make([]string, 0, 3)
	for _, destinationQueue := range destinationQueues() {
		destinationQueueNames = append(destinationQueueNames, string(destinationQueue.Destination))
	}
	return destinationQueueNames
}

// Returns how long an unused client address stays in the rate limiter registry.
func rateLimiterIdleTimeout(applicationConfiguration config.ApplicationConfiguration) time.Duration {
	return max(applicationConfiguration.RateLimitCleanupInterval, minimumRateLimiterIdleTimeout)
}

// Starts the ticker that asks the batch writer to flush and stops with the context.
func startFlushTrigger(applicationContext context.Context, flushInterval time.Duration) (<-chan struct{}, <-chan struct{}) {
	flushTrigger := make(chan struct{}, 1)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		flushTicker := time.NewTicker(flushInterval)
		defer flushTicker.Stop()
		for {
			select {
			case <-applicationContext.Done():
				return
			case <-flushTicker.C:
				select {
				case flushTrigger <- struct{}{}:
				default:
				}
			}
		}
	}()
	return flushTrigger, completed
}
