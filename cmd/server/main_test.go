package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/metrics"
)

const testHTTPPort = 18080

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
		if shutdownError := serverApplication.shutdownBackgroundWork(shutdownGracePeriod); shutdownError != nil {
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

	if shutdownError := serverApplication.shutdownBackgroundWork(shutdownGracePeriod); shutdownError != nil {
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
