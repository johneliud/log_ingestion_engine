package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
	"github.com/johneliud/log_ingestion_engine/internal/storage"
)

const (
	shutdownGracePeriod           = 5 * time.Second
	minimumRateLimiterIdleTimeout = time.Second
)

// Holds the wired request router and the background work the application owns.
type serverApplication struct {
	requestRouter         *http.ServeMux
	cancelPipelineWork    context.CancelFunc
	cancelStorageWork     context.CancelFunc
	pipelineCompletion    <-chan struct{}
	backgroundCompletions []<-chan struct{}
	deadLetterCompletion  <-chan struct{}
	batchWriter           *storage.BatchWriter
	deadLetterWriter      *storage.DeadLetterWriter
	sqliteLogStore        *storage.SQLiteLogStore
	publisher             *queue.Publisher
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

	serveError := runHTTPServerUntilContextIsCancelled(applicationContext, applicationConfiguration, serverApplication.requestRouter)
	shutdownError := serverApplication.shutdownBackgroundWork(shutdownGracePeriod)
	if serveError != nil {
		slog.Error("http server stopped unexpectedly", "server_error", serveError)
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

	logEnricher := enrich.NewLogEnricher(applicationConfiguration.ApplicationEnvironment, nil)

	serviceRouter, routerError := router.NewServiceRouter(applicationConfiguration.RoutingConfigurationFilePath, applicationConfiguration.DefaultDestination)
	if routerError != nil {
		cancelPipelineWork()
		cancelStorageWork()
		return nil, routerError
	}

	publisher := queue.NewPublisher(applicationConfiguration, applicationConfiguration.RawLogsBufferSize, queue.WithPublishedMessageRecorder(metricsRegistry))

	batchConsumer := pipeline.NewBatchConsumer(
		rawLogsQueue.RawLogsChannel(),
		applicationConfiguration.ConsumerBatchSize,
		applicationConfiguration.PipelineWorkerCount,
		logEnricher,
		serviceRouter,
		publisher,
	)

	sqliteLogStore, storeError := storage.NewSQLiteLogStore(applicationConfiguration)
	if storeError != nil {
		cancelPipelineWork()
		cancelStorageWork()
		return nil, storeError
	}

	deadLetterWriter := storage.NewDeadLetterWriter(applicationConfiguration)

	batchWriter := storage.NewBatchWriter(storage.BatchWriterDependencies{
		ApplicationContext: storageContext,
		WriteBatchSize:     applicationConfiguration.WriteBatchSize,
		RetryBackoffDelays: applicationConfiguration.WriteRetryBackoffDelays,
		StoreBatch:         sqliteLogStore.StoreBatch,
		DeadLetterSink:     deadLetterWriter.Submit,
		MetricsRecorder:    metricsRegistry,
	})

	rateLimiter := ratelimit.NewPerIPRateLimiterRegistry(
		applicationConfiguration.RateLimitPerSecond,
		rateLimiterIdleTimeout(applicationConfiguration),
		nil,
	)

	ingestionHandler := handler.NewLogIngestionHandler(
		applicationConfiguration.MaximumBodySizeInBytes,
		handler.WithRawLogPusher(rawLogsQueue),
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

	pipelineCompletion := batchConsumer.Start(pipelineContext)
	backgroundCompletions := []<-chan struct{}{
		publisher.RunReconnectLoop(storageContext),
		batchWriter.Run(storageContext, flushTrigger),
		fallbackProcessor.Start(storageContext),
		consumerSupervisor.Start(storageContext),
		flushTriggerCompletion,
	}
	deadLetterCompletion := deadLetterWriter.Run()
	rateLimiter.StartIdleCleanup(storageContext, applicationConfiguration.RateLimitCleanupInterval)

	return &serverApplication{
		requestRouter:         requestRouter,
		cancelPipelineWork:    cancelPipelineWork,
		cancelStorageWork:     cancelStorageWork,
		pipelineCompletion:    pipelineCompletion,
		backgroundCompletions: backgroundCompletions,
		deadLetterCompletion:  deadLetterCompletion,
		batchWriter:           batchWriter,
		deadLetterWriter:      deadLetterWriter,
		sqliteLogStore:        sqliteLogStore,
		publisher:             publisher,
	}, nil
}

// Builds the request router with the ingestion, metrics, dashboard and home page routes wired.
func buildRequestRouter(applicationConfiguration config.ApplicationConfiguration, ingestionHandler http.Handler, metricsRegistry *metrics.Metrics) *http.ServeMux {
	requestRouter := http.NewServeMux()
	requestRouter.Handle("POST /logs", ingestionHandler)
	requestRouter.HandleFunc("GET /metrics", handler.MetricsPageEndpointHandler())
	requestRouter.HandleFunc("GET /metrics.json", handler.NewMetricsHandler(metricsRegistry, applicationConfiguration))
	dashboardHandler := handler.NewDashboardHandler()
	requestRouter.HandleFunc("GET /dashboard", dashboardHandler)
	requestRouter.HandleFunc("GET /dashboard/", dashboardHandler)
	requestRouter.HandleFunc("GET /{$}", handler.RootEndpointHandler())
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

// Serves the wired request router until the context is cancelled, then shuts the server down within the shutdown grace period.
func runHTTPServerUntilContextIsCancelled(applicationContext context.Context, applicationConfiguration config.ApplicationConfiguration, requestRouter *http.ServeMux) error {
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", applicationConfiguration.HTTPPort),
		Handler: requestRouter,
	}

	serverListenErrorChannel := make(chan error, 1)
	go func() {
		serverListenErrorChannel <- httpServer.ListenAndServe()
	}()
	select {
	case serverListenError := <-serverListenErrorChannel:
		if !errors.Is(serverListenError, http.ErrServerClosed) {
			return serverListenError
		}
	case <-applicationContext.Done():
	}

	shutdownContext, shutdownContextCancellation := context.WithTimeout(context.Background(), shutdownGracePeriod)
	defer shutdownContextCancellation()
	return httpServer.Shutdown(shutdownContext)
}

// Stops the background work in order and closes the storage components.
func (application *serverApplication) shutdownBackgroundWork(gracePeriod time.Duration) error {
	application.shutdownOnce.Do(func() {
		application.shutdownError = application.stopBackgroundWork(gracePeriod)
	})
	return application.shutdownError
}

// Cancels the pipeline, drains the raw logs, then stops the storage side and closes the storage components.
func (application *serverApplication) stopBackgroundWork(gracePeriod time.Duration) error {
	shutdownContext, shutdownContextCancellation := context.WithTimeout(context.Background(), gracePeriod)
	defer shutdownContextCancellation()

	application.cancelPipelineWork()
	pipelineError := application.waitForCompletion(shutdownContext, application.pipelineCompletion, "the pipeline did not finish before the shutdown grace period expired")

	application.cancelStorageWork()
	waitError := application.waitForBackgroundWork(shutdownContext)
	application.batchWriter.FlushAll()
	deadLetterError := application.deadLetterWriter.Close()
	deadLetterWaitError := application.waitForDeadLetterWriter(shutdownContext)
	storeError := application.sqliteLogStore.Close()
	publisherError := application.publisher.Close()
	return errors.Join(pipelineError, waitError, deadLetterError, deadLetterWaitError, storeError, publisherError)
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
	for _, backgroundCompletion := range application.backgroundCompletions {
		select {
		case <-backgroundCompletion:
		case <-shutdownContext.Done():
			slog.Warn("background work did not finish before the shutdown grace period expired")
			return shutdownContext.Err()
		}
	}
	return nil
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
