package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/metrics"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// The JSON fields documented for GET /metrics in docs/api.md.
var documentedMetricsFields = []string{
	"total_logs_received",
	"logs_by_level",
	"logs_by_service",
	"error_rate",
	"throughput",
	"queue_backlog",
	"rate_limited_requests",
	"dead_lettered_logs",
	"write_failures",
}

// Returns a metrics handler bound to the given management API URL and credentials.
func aMetricsHandlerForManagementAPI(managementAPIURL string, metricsRegistry *metrics.Metrics) http.HandlerFunc {
	return NewMetricsHandler(metricsRegistry, config.ApplicationConfiguration{
		RabbitMQManagementURL: managementAPIURL,
		RabbitMQUsername:      "log_engine",
		RabbitMQPassword:      "test_password",
		MetricsBacklogTimeout: time.Second,
	})
}

// Serves GET /metrics and returns the status code, content type and decoded JSON body.
func serveMetricsRequest(t *testing.T, metricsHandler http.HandlerFunc) (int, string, map[string]any) {
	t.Helper()
	responseRecorder := httptest.NewRecorder()
	metricsHandler.ServeHTTP(responseRecorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	var decodedResponse map[string]any
	if decodeError := json.Unmarshal(responseRecorder.Body.Bytes(), &decodedResponse); decodeError != nil {
		t.Fatalf("decoding the metrics response: %v", decodeError)
	}
	return responseRecorder.Code, responseRecorder.Header().Get("Content-Type"), decodedResponse
}

// Returns the numeric value of one top-level field from a decoded metrics response.
func numericFieldFromMetricsResponse(t *testing.T, decodedResponse map[string]any, fieldName string) float64 {
	t.Helper()
	fieldValue, isNumber := decodedResponse[fieldName].(float64)
	if !isNumber {
		t.Fatalf("field %q = %#v, want a JSON number", fieldName, decodedResponse[fieldName])
	}
	return fieldValue
}

func TestMetricsHandler_ReportsBacklogFromTheManagementAPI(t *testing.T) {
	t.Parallel()

	managementAPIServer := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		username, password, hasCredentials := request.BasicAuth()
		if !hasCredentials || username != "log_engine" || password != "test_password" {
			responseWriter.WriteHeader(http.StatusUnauthorized)
			return
		}
		if request.URL.Path != "/api/queues" {
			responseWriter.WriteHeader(http.StatusNotFound)
			return
		}
		responseWriter.Header().Set("Content-Type", "application/json")
		_, _ = responseWriter.Write([]byte(`[
			{"name":"queue_service1","vhost":"/","messages":7},
			{"name":"queue_service2","vhost":"/","messages":5},
			{"name":"queue_service3","vhost":"/","messages":3},
			{"name":"someone_elses_queue","vhost":"/","messages":100},
			{"name":"queue_service1","vhost":"/other","messages":50}
		]`))
	}))
	defer managementAPIServer.Close()

	metricsRegistry := metrics.NewMetrics(time.Minute, nil)
	metricsRegistry.RecordLogsReceived([]model.LogEntry{
		{Service: "claims", Level: model.LogLevelInfo},
		{Service: "claims", Level: model.LogLevelError},
	})
	metricsRegistry.RecordQueueMessagePublished()
	metricsRegistry.RecordQueueMessagePublished()
	metricsRegistry.RecordQueueMessageStored()

	statusCode, contentType, decodedResponse := serveMetricsRequest(t, aMetricsHandlerForManagementAPI(managementAPIServer.URL, metricsRegistry))

	if statusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", statusCode, http.StatusOK)
	}
	if contentType != "application/json" {
		t.Errorf("content type = %q, want %q", contentType, "application/json")
	}

	if backlog := numericFieldFromMetricsResponse(t, decodedResponse, "queue_backlog"); backlog != 15 {
		t.Errorf("queue_backlog = %v, want 15 from the management api, not the fallback estimate of 1", backlog)
	}
	if totalLogsReceived := numericFieldFromMetricsResponse(t, decodedResponse, "total_logs_received"); totalLogsReceived != 2 {
		t.Errorf("total_logs_received = %v, want 2", totalLogsReceived)
	}
	if errorRate := numericFieldFromMetricsResponse(t, decodedResponse, "error_rate"); errorRate != 0.5 {
		t.Errorf("error_rate = %v, want 0.5", errorRate)
	}

	for _, documentedField := range documentedMetricsFields {
		if _, fieldExists := decodedResponse[documentedField]; !fieldExists {
			t.Errorf("metrics response is missing the documented field %q", documentedField)
		}
	}
}

func TestMetricsHandler_FallsBackToTheLocalCounterWhenTheManagementAPIIsUnreachable(t *testing.T) {
	t.Parallel()

	unreachableManagementAPI := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	unreachableManagementAPI.Close()

	metricsRegistry := metrics.NewMetrics(time.Minute, nil)
	for range 5 {
		metricsRegistry.RecordQueueMessagePublished()
	}
	for range 2 {
		metricsRegistry.RecordQueueMessageStored()
	}

	statusCode, _, decodedResponse := serveMetricsRequest(t, aMetricsHandlerForManagementAPI(unreachableManagementAPI.URL, metricsRegistry))

	if statusCode != http.StatusOK {
		t.Errorf("status = %d, want %d; the endpoint must still serve a snapshot when the api is down", statusCode, http.StatusOK)
	}
	if backlog := numericFieldFromMetricsResponse(t, decodedResponse, "queue_backlog"); backlog != 3 {
		t.Errorf("queue_backlog = %v, want the fallback counter value 3", backlog)
	}
}

func TestMetricsHandler_FallsBackToTheLocalCounterWhenTheManagementAPIIsNotConfigured(t *testing.T) {
	t.Parallel()

	metricsRegistry := metrics.NewMetrics(time.Minute, nil)
	metricsRegistry.RecordQueueMessagePublished()
	metricsRegistry.RecordQueueMessagePublished()
	metricsRegistry.RecordQueueMessagePublished()
	metricsRegistry.RecordQueueMessageStored()

	statusCode, _, decodedResponse := serveMetricsRequest(t, aMetricsHandlerForManagementAPI("", metricsRegistry))

	if statusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", statusCode, http.StatusOK)
	}
	if backlog := numericFieldFromMetricsResponse(t, decodedResponse, "queue_backlog"); backlog != 2 {
		t.Errorf("queue_backlog = %v, want the fallback counter value 2", backlog)
	}
}

func TestMetricsHandler_FallsBackToTheLocalCounterWhenTheManagementAPIRejectsTheRequest(t *testing.T) {
	t.Parallel()

	unauthorizedManagementAPI := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		responseWriter.WriteHeader(http.StatusUnauthorized)
	}))
	defer unauthorizedManagementAPI.Close()

	metricsRegistry := metrics.NewMetrics(time.Minute, nil)
	metricsRegistry.RecordQueueMessagePublished()

	statusCode, _, decodedResponse := serveMetricsRequest(t, aMetricsHandlerForManagementAPI(unauthorizedManagementAPI.URL, metricsRegistry))

	if statusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", statusCode, http.StatusOK)
	}
	if backlog := numericFieldFromMetricsResponse(t, decodedResponse, "queue_backlog"); backlog != 1 {
		t.Errorf("queue_backlog = %v, want the fallback counter value 1", backlog)
	}
}
