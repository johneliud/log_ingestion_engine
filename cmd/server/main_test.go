package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/metrics"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

const (
	testHTTPPort            = 18080
	testShutdownGracePeriod = 5 * time.Second
)

// Returns a configuration that stores everything in a temporary directory and cannot reach the broker.
func testApplicationConfiguration(t *testing.T) config.ApplicationConfiguration {
	t.Helper()

	temporaryDirectory := t.TempDir()
	return config.ApplicationConfiguration{
		HTTPPort:                         testHTTPPort,
		RateLimitPerSecond:               1000,
		RateLimitCleanupInterval:         time.Minute,
		RawLogsBufferSize:                100,
		ConsumerBatchSize:                10,
		ChannelSendTimeout:               100 * time.Millisecond,
		MaximumBodySizeInBytes:           1 << 20,
		ApplicationEnvironment:           "test",
		RoutingConfigurationFilePath:     filepath.Join(temporaryDirectory, "routing.json"),
		DefaultDestination:               "service1",
		RabbitMQHost:                     "127.0.0.1",
		RabbitMQPort:                     1,
		RabbitMQUsername:                 "test-user",
		RabbitMQPassword:                 "test-password",
		RabbitMQReconnectBackoffDelays:   []time.Duration{10 * time.Millisecond},
		PipelineWorkerCount:              2,
		WriteBatchSize:                   100,
		WriteFlushInterval:               10 * time.Millisecond,
		DatabaseDirectoryPath:            temporaryDirectory,
		DatabaseBusyTimeout:              time.Second,
		WriteRetryBackoffDelays:          []time.Duration{10 * time.Millisecond},
		DeadLetterFilePath:               filepath.Join(temporaryDirectory, "logs-failed.json"),
		DeadLetterMaximumFileSizeInBytes: 1 << 20,
		MetricsThroughputWindow:          time.Minute,
		MetricsBacklogTimeout:            10 * time.Millisecond,
		ShutdownGracePeriod:              testShutdownGracePeriod,
		PublishTimeout:                   100 * time.Millisecond,
	}
}

// Builds the wired application and stops its background work when the test ends.
func newTestServerApplication(t *testing.T, applicationConfiguration config.ApplicationConfiguration) *serverApplication {
	t.Helper()

	serverApplication, applicationError := newServerApplication(applicationConfiguration)
	if applicationError != nil {
		t.Fatalf("building the application failed: %v", applicationError)
	}
	t.Cleanup(func() {
		if shutdownError := serverApplication.shutdownBackgroundWork(aTestShutdownContext(t)); shutdownError != nil {
			t.Errorf("stopping the background work failed: %v", shutdownError)
		}
	})
	return serverApplication
}

// Serves one request against the wired router and returns the recorded response.
func serveRequest(requestRouter http.Handler, method string, requestPath string, requestBody string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, requestPath, strings.NewReader(requestBody))
	if requestBody != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	responseRecorder := httptest.NewRecorder()
	requestRouter.ServeHTTP(responseRecorder, request)
	return responseRecorder
}

// Returns one valid log entry as a JSON request body.
func validLogBatchBody() string {
	return `[{"timestamp":"2026-10-26T10:00:00Z","service":"claims","level":"INFO","message":"a valid log entry"}]`
}

// Returns the number of rows stored in the logs table of one database file.
func countStoredLogs(t *testing.T, databaseFilePath string) int {
	t.Helper()

	database, openError := sql.Open("sqlite", databaseFilePath)
	if openError != nil {
		t.Fatalf("opening the database failed: %v", openError)
	}
	defer database.Close()

	var storedLogCount int
	if scanError := database.QueryRow("SELECT COUNT(*) FROM logs").Scan(&storedLogCount); scanError != nil {
		t.Fatalf("counting the stored logs failed: %v", scanError)
	}
	return storedLogCount
}

// Returns the number of rows stored across every destination database.
func countStoredLogsInEveryDatabase(t *testing.T, databaseDirectoryPath string) int {
	t.Helper()

	storedLogCount := 0
	for _, databaseFileName := range []string{config.DatabaseFileService1Name, config.DatabaseFileService2Name, config.DatabaseFileService3Name} {
		storedLogCount += countStoredLogs(t, filepath.Join(databaseDirectoryPath, databaseFileName))
	}
	return storedLogCount
}

// Returns the number of dead letter entries written to the dead letter file.
func countDeadLetterEntries(t *testing.T, deadLetterFilePath string) int {
	t.Helper()

	fileContents, readError := os.ReadFile(deadLetterFilePath)
	if errors.Is(readError, os.ErrNotExist) {
		return 0
	}
	if readError != nil {
		t.Fatalf("reading the dead letter file failed: %v", readError)
	}

	deadLetterEntryCount := 0
	for fileLine := range strings.SplitSeq(strings.TrimSpace(string(fileContents)), "\n") {
		if strings.TrimSpace(fileLine) == "" {
			continue
		}
		deadLetterEntryCount++
	}
	return deadLetterEntryCount
}

// Writes the routing rules that send each service to its destination.
func writeRoutingRules(t *testing.T, routingConfigurationFilePath string, routingRules map[string]string) {
	t.Helper()

	serializedRules, marshalError := json.Marshal(routingRules)
	if marshalError != nil {
		t.Fatalf("marshalling the routing rules failed: %v", marshalError)
	}
	if writeError := os.WriteFile(routingConfigurationFilePath, serializedRules, 0o644); writeError != nil {
		t.Fatalf("writing the routing rules failed: %v", writeError)
	}
}

// Returns one request body with a log entry for every service name.
func logBatchBodyForServices(serviceNames []string) string {
	logEntries := make([]string, 0, len(serviceNames))
	for _, serviceName := range serviceNames {
		logEntries = append(logEntries, fmt.Sprintf(`{"timestamp":"2026-10-26T10:00:00Z","service":%q,"level":"INFO","message":"a log entry for %s"}`, serviceName, serviceName))
	}
	return "[" + strings.Join(logEntries, ",") + "]"
}

// Returns the shutdown context the tests use for the background work.
func aTestShutdownContext(t *testing.T) context.Context {
	t.Helper()

	shutdownContext, shutdownContextCancellation := context.WithTimeout(context.Background(), testShutdownGracePeriod)
	t.Cleanup(shutdownContextCancellation)
	return shutdownContext
}

// Waits for the pipeline workers to stop after the pipeline work was cancelled.
func waitForPipelineCompletion(t *testing.T, serverApplication *serverApplication) {
	t.Helper()

	select {
	case <-serverApplication.pipelineCompletion:
	case <-time.After(2 * time.Second):
		t.Fatal("the pipeline did not stop after its work was cancelled")
	}
}

// Reads the metrics snapshot served by the wired router.
func readMetricsSnapshot(t *testing.T, requestRouter http.Handler) metrics.Snapshot {
	t.Helper()

	responseRecorder := serveRequest(requestRouter, http.MethodGet, "/metrics.json", "")
	if responseRecorder.Code != http.StatusOK {
		t.Fatalf("metrics snapshot status = %d, want %d", responseRecorder.Code, http.StatusOK)
	}
	var snapshot metrics.Snapshot
	if decodeError := json.Unmarshal(responseRecorder.Body.Bytes(), &snapshot); decodeError != nil {
		t.Fatalf("decoding the metrics snapshot failed: %v", decodeError)
	}
	return snapshot
}

func TestServerApplication_RoutesEveryPageAndMethod(t *testing.T) {
	t.Parallel()

	serverApplication := newTestServerApplication(t, testApplicationConfiguration(t))

	routeCases := []struct {
		name           string
		method         string
		requestPath    string
		expectedStatus int
	}{
		{name: "home page", method: http.MethodGet, requestPath: "/", expectedStatus: http.StatusOK},
		{name: "unknown path", method: http.MethodGet, requestPath: "/does-not-exist", expectedStatus: http.StatusNotFound},
		{name: "dashboard page", method: http.MethodGet, requestPath: "/dashboard", expectedStatus: http.StatusOK},
		// {name: "dashboard asset", method: http.MethodGet, requestPath: "/dashboard/css/styles.css", expectedStatus: http.StatusOK},
		{name: "metrics page", method: http.MethodGet, requestPath: "/metrics", expectedStatus: http.StatusOK},
		{name: "metrics snapshot", method: http.MethodGet, requestPath: "/metrics.json", expectedStatus: http.StatusOK},
		{name: "ingestion over get", method: http.MethodGet, requestPath: "/logs", expectedStatus: http.StatusMethodNotAllowed},
		{name: "home page over post", method: http.MethodPost, requestPath: "/", expectedStatus: http.StatusMethodNotAllowed},
	}

	for _, routeCase := range routeCases {
		t.Run(routeCase.name, func(t *testing.T) {
			t.Parallel()

			responseRecorder := serveRequest(serverApplication.requestRouter, routeCase.method, routeCase.requestPath, "")

			if responseRecorder.Code != routeCase.expectedStatus {
				t.Errorf("status = %d, want %d", responseRecorder.Code, routeCase.expectedStatus)
			}
		})
	}
}

func TestServerApplication_StoresAnAcceptedLogThroughTheFallbackPath(t *testing.T) {
	t.Parallel()

	applicationConfiguration := testApplicationConfiguration(t)
	serverApplication := newTestServerApplication(t, applicationConfiguration)

	responseRecorder := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", validLogBatchBody())
	if responseRecorder.Code != http.StatusAccepted {
		t.Fatalf("ingestion status = %d, want %d", responseRecorder.Code, http.StatusAccepted)
	}

	if shutdownError := serverApplication.shutdownBackgroundWork(aTestShutdownContext(t)); shutdownError != nil {
		t.Fatalf("stopping the background work failed: %v", shutdownError)
	}

	storedLogCount := countStoredLogs(t, filepath.Join(applicationConfiguration.DatabaseDirectoryPath, config.DatabaseFileService1Name))
	if storedLogCount != 1 {
		t.Errorf("stored log count = %d, want 1", storedLogCount)
	}
}

func TestServerApplication_RateLimitsRepeatedRequestsFromOneClient(t *testing.T) {
	t.Parallel()

	applicationConfiguration := testApplicationConfiguration(t)
	applicationConfiguration.RateLimitPerSecond = 1
	serverApplication := newTestServerApplication(t, applicationConfiguration)

	firstResponse := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", validLogBatchBody())
	if firstResponse.Code != http.StatusAccepted {
		t.Fatalf("first ingestion status = %d, want %d", firstResponse.Code, http.StatusAccepted)
	}

	secondResponse := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", validLogBatchBody())
	if secondResponse.Code != http.StatusTooManyRequests {
		t.Errorf("second ingestion status = %d, want %d", secondResponse.Code, http.StatusTooManyRequests)
	}
}

func TestServerApplication_CountsAcceptedLogsAndRejectedRequestsInTheMetricsSnapshot(t *testing.T) {
	t.Parallel()

	applicationConfiguration := testApplicationConfiguration(t)
	applicationConfiguration.RateLimitPerSecond = 1
	serverApplication := newTestServerApplication(t, applicationConfiguration)

	acceptedResponse := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", validLogBatchBody())
	if acceptedResponse.Code != http.StatusAccepted {
		t.Fatalf("accepted ingestion status = %d, want %d", acceptedResponse.Code, http.StatusAccepted)
	}
	rejectedResponse := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", validLogBatchBody())
	if rejectedResponse.Code != http.StatusTooManyRequests {
		t.Fatalf("rejected ingestion status = %d, want %d", rejectedResponse.Code, http.StatusTooManyRequests)
	}

	snapshot := readMetricsSnapshot(t, serverApplication.requestRouter)
	if snapshot.TotalLogsReceived != 1 {
		t.Errorf("total logs received = %d, want 1", snapshot.TotalLogsReceived)
	}
	if snapshot.RateLimitedRequests != 1 {
		t.Errorf("rate limited requests = %d, want 1", snapshot.RateLimitedRequests)
	}
}

func TestServerApplication_StoresEveryAcceptedLogOrDeadLettersItWhenTheApplicationShutsDown(t *testing.T) {
	t.Parallel()

	applicationConfiguration := testApplicationConfiguration(t)
	writeRoutingRules(t, applicationConfiguration.RoutingConfigurationFilePath, map[string]string{
		"claims":   "service1",
		"payments": "service2",
		"policy":   "service3",
	})
	serverApplication := newTestServerApplication(t, applicationConfiguration)

	acceptedLogCount := 0
	for _, serviceNames := range [][]string{{"claims", "payments"}, {"policy", "claims"}, {"payments"}} {
		responseRecorder := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", logBatchBodyForServices(serviceNames))
		if responseRecorder.Code != http.StatusAccepted {
			t.Fatalf("ingestion status = %d, want %d", responseRecorder.Code, http.StatusAccepted)
		}
		acceptedLogCount += len(serviceNames)
	}

	if shutdownError := serverApplication.shutdownBackgroundWork(aTestShutdownContext(t)); shutdownError != nil {
		t.Fatalf("stopping the background work failed: %v", shutdownError)
	}

	storedLogCount := countStoredLogsInEveryDatabase(t, applicationConfiguration.DatabaseDirectoryPath)
	deadLetterEntryCount := countDeadLetterEntries(t, applicationConfiguration.DeadLetterFilePath)
	if storedLogCount+deadLetterEntryCount != acceptedLogCount {
		t.Errorf("stored logs %d plus dead letter entries %d = %d, want the %d accepted logs", storedLogCount, deadLetterEntryCount, storedLogCount+deadLetterEntryCount, acceptedLogCount)
	}
	if storedLogCount != acceptedLogCount {
		t.Errorf("stored log count = %d, want the %d accepted logs", storedLogCount, acceptedLogCount)
	}
}

func TestServerApplication_WaitsForTheRunningRequestsBeforeItDrainsTheRawLogs(t *testing.T) {
	t.Parallel()

	var runningRequests sync.WaitGroup
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	requestFinished := make(chan struct{})
	blockingHandler := http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		close(requestStarted)
		<-releaseRequest
		close(requestFinished)
	})
	trackedHandler := trackRunningRequests(blockingHandler, &runningRequests)
	serverApplication := &serverApplication{inFlightRequests: &runningRequests}

	requestServed := make(chan struct{})
	go func() {
		trackedHandler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader("")))
		close(requestServed)
	}()
	<-requestStarted

	shutdownContext, shutdownContextCancellation := context.WithTimeout(context.Background(), testShutdownGracePeriod)
	defer shutdownContextCancellation()
	waitCompleted := make(chan error, 1)
	go func() {
		waitCompleted <- serverApplication.waitForRunningRequests(shutdownContext)
	}()

	close(releaseRequest)
	select {
	case waitError := <-waitCompleted:
		if waitError != nil {
			t.Fatalf("waiting for the running requests failed: %v", waitError)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the shutdown did not finish waiting for the running request")
	}

	select {
	case <-requestFinished:
	default:
		t.Fatal("the shutdown finished before the running request returned")
	}
	<-requestServed
}

func TestServerApplication_DeadLettersTheLogsThePipelineCouldNotProcess(t *testing.T) {
	t.Parallel()

	applicationConfiguration := testApplicationConfiguration(t)
	writeRoutingRules(t, applicationConfiguration.RoutingConfigurationFilePath, map[string]string{"claims": "service2"})
	serverApplication := newTestServerApplication(t, applicationConfiguration)

	serverApplication.cancelPipelineWork()
	waitForPipelineCompletion(t, serverApplication)

	undrainedLogs := []model.RawLog{
		{LogEntry: model.LogEntry{Timestamp: "2026-10-26T10:00:00Z", Service: "claims", Level: model.LogLevelInfo, Message: "the first undrained log"}, BatchID: "batch-one", SourceIP: "203.0.113.10", ReceivedAt: time.Now()},
		{LogEntry: model.LogEntry{Timestamp: "2026-10-26T10:00:00Z", Service: "policy", Level: model.LogLevelInfo, Message: "the second undrained log"}, BatchID: "batch-one", SourceIP: "203.0.113.10", ReceivedAt: time.Now()},
	}
	if pushError := serverApplication.rawLogsQueue.PushBatch(undrainedLogs); pushError != nil {
		t.Fatalf("queueing the undrained logs failed: %v", pushError)
	}

	if deadLetterError := serverApplication.deadLetterRemainingRawLogs(); deadLetterError != nil {
		t.Fatalf("dead-lettering the undrained logs failed: %v", deadLetterError)
	}
	if closeError := serverApplication.deadLetterWriter.Close(); closeError != nil {
		t.Fatalf("closing the dead letter writer failed: %v", closeError)
	}

	deadLetterEntryCount := countDeadLetterEntries(t, applicationConfiguration.DeadLetterFilePath)
	if deadLetterEntryCount != len(undrainedLogs) {
		t.Errorf("dead letter entry count = %d, want %d", deadLetterEntryCount, len(undrainedLogs))
	}
}
