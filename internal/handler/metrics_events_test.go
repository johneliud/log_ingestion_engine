package handler

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/metrics"
)

// Returns a fake RabbitMQ management API that reports one pipeline queue with the given message count.
func aFakeManagementAPI(t *testing.T, queuedMessages int) *httptest.Server {
	t.Helper()
	managementServer := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/queues" {
			http.NotFound(responseWriter, request)
			return
		}
		username, password, hasBasicAuth := request.BasicAuth()
		if !hasBasicAuth || username != "management-user" || password != "management-password" {
			http.Error(responseWriter, "unauthorized", http.StatusUnauthorized)
			return
		}
		responseWriter.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(responseWriter, fmt.Sprintf(`[{"name":"queue_service1","vhost":"/","messages":%d}]`, queuedMessages))
	}))
	t.Cleanup(managementServer.Close)
	return managementServer
}

// Streams server-sent events until the read deadline and returns the decoded snapshots that arrived.
func readStreamedSnapshots(t *testing.T, streamURL string, readDeadline time.Duration) []metrics.Snapshot {
	t.Helper()

	requestContext, cancelRequest := context.WithCancel(context.Background())
	t.Cleanup(cancelRequest)
	streamRequest, requestError := http.NewRequestWithContext(requestContext, http.MethodGet, streamURL, nil)
	if requestError != nil {
		t.Fatalf("creating the stream request failed: %v", requestError)
	}
	streamResponse, responseError := http.DefaultClient.Do(streamRequest)
	if responseError != nil {
		t.Fatalf("opening the event stream failed: %v", responseError)
	}
	t.Cleanup(func() { streamResponse.Body.Close() })

	decodedSnapshots := make(chan metrics.Snapshot, 64)
	go func() {
		lineScanner := bufio.NewScanner(streamResponse.Body)
		for lineScanner.Scan() {
			line := lineScanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var snapshot metrics.Snapshot
			if unmarshalError := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &snapshot); unmarshalError != nil {
				return
			}
			decodedSnapshots <- snapshot
		}
	}()

	snapshots := make([]metrics.Snapshot, 0)
	readTimer := time.NewTimer(readDeadline)
	defer readTimer.Stop()
	for {
		select {
		case snapshot := <-decodedSnapshots:
			snapshots = append(snapshots, snapshot)
		case <-readTimer.C:
			return snapshots
		}
	}
}

func TestMetricsEventsHandler_StreamsAFreshQueueBacklogFromTheManagementAPI(t *testing.T) {
	t.Parallel()

	managementServer := aFakeManagementAPI(t, 7)
	applicationConfiguration := config.ApplicationConfiguration{
		RabbitMQManagementURL: managementServer.URL,
		RabbitMQUsername:      "management-user",
		RabbitMQPassword:      "management-password",
		RabbitMQVirtualHost:   "/",
		MetricsBacklogTimeout: time.Second,
		CorsAllowedOrigins:    []string{"http://localhost:5173"},
	}

	metricsRegistry := metrics.NewMetrics(time.Minute, nil)
	eventsServer := httptest.NewServer(newMetricsEventsHandler(metricsRegistry, applicationConfiguration, 10*time.Millisecond))
	// Registered before the stream cleanups (LIFO), so the cancelled stream is torn down before Close waits on connections.
	t.Cleanup(eventsServer.Close)

	snapshots := readStreamedSnapshots(t, eventsServer.URL, 2*time.Second)
	backlogWasStreamed := false
	for _, snapshot := range snapshots {
		if snapshot.QueueBacklog == 7 {
			backlogWasStreamed = true
			break
		}
	}
	if !backlogWasStreamed {
		t.Fatalf("streamed backlog never reached 7 from the management api across %d snapshots", len(snapshots))
	}
}

func TestMetricsEventsHandler_RejectsADisallowedOrigin(t *testing.T) {
	t.Parallel()

	applicationConfiguration := config.ApplicationConfiguration{
		CorsAllowedOrigins: []string{"http://localhost:5173"},
	}
	metricsRegistry := metrics.NewMetrics(time.Minute, nil)
	eventsHandler := newMetricsEventsHandler(metricsRegistry, applicationConfiguration, time.Second)

	request := httptest.NewRequest(http.MethodGet, "/metrics/events", nil)
	request.Header.Set("Origin", "https://evil.example.com")
	responseRecorder := httptest.NewRecorder()
	eventsHandler.ServeHTTP(responseRecorder, request)

	if responseRecorder.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d", responseRecorder.Code, http.StatusForbidden)
	}
}

func TestMetricsEventsHandler_FallsBackToTheLocalInFlightEstimateWithoutAManagementAPI(t *testing.T) {
	t.Parallel()

	applicationConfiguration := config.ApplicationConfiguration{
		RabbitMQManagementURL: "",
		MetricsBacklogTimeout: time.Second,
	}
	metricsRegistry := metrics.NewMetrics(time.Minute, nil)
	for range 3 {
		metricsRegistry.RecordQueueMessagePublished()
	}

	eventsServer := httptest.NewServer(newMetricsEventsHandler(metricsRegistry, applicationConfiguration, 10*time.Millisecond))
	// Registered before the stream cleanups (LIFO), so the cancelled stream is torn down before Close waits on connections.
	t.Cleanup(eventsServer.Close)

	snapshots := readStreamedSnapshots(t, eventsServer.URL, time.Second)
	if len(snapshots) == 0 {
		t.Fatal("no snapshot arrived on the stream")
	}
	lastSnapshot := snapshots[len(snapshots)-1]
	if lastSnapshot.QueueBacklog != 3 {
		t.Errorf("streamed backlog = %d, want the local in-flight estimate 3", lastSnapshot.QueueBacklog)
	}
}
