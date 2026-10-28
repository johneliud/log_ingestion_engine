package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/metrics"
)

// How often each connected stream refreshes the queue backlog from the management API, so streamed snapshots carry a fresh count instead of the value left behind by the last JSON request.
const backlogRefreshInterval = 2 * time.Second

// Returns a handler that streams metrics snapshots as server-sent events.
func NewMetricsEventsHandler(metricsRegistry *metrics.Metrics, applicationConfiguration config.ApplicationConfiguration) http.HandlerFunc {
	return newMetricsEventsHandler(metricsRegistry, applicationConfiguration, backlogRefreshInterval)
}

// Returns a metrics events handler with a configurable backlog refresh interval.
func newMetricsEventsHandler(metricsRegistry *metrics.Metrics, applicationConfiguration config.ApplicationConfiguration, backlogRefreshInterval time.Duration) http.HandlerFunc {
	queueBacklogFetcher := newQueueBacklogFetcher(applicationConfiguration)
	corsAllowedOrigins := applicationConfiguration.CorsAllowedOrigins
	return func(responseWriter http.ResponseWriter, request *http.Request) {
		origin := request.Header.Get("Origin")
		if origin != "" && !isOriginAllowed(origin, corsAllowedOrigins) {
			responseWriter.Header().Set("Content-Type", "text/plain")
			responseWriter.WriteHeader(http.StatusForbidden)
			_, _ = responseWriter.Write([]byte("origin not allowed"))
			return
		}

		responseWriter.Header().Set("Content-Type", "text/event-stream")
		responseWriter.Header().Set("Cache-Control", "no-cache")
		responseWriter.Header().Set("Connection", "keep-alive")
		responseWriter.Header().Set("X-Accel-Buffering", "no")
		if origin != "" {
			responseWriter.Header().Set("Access-Control-Allow-Origin", origin)
		}

		flusher, ok := responseWriter.(http.Flusher)
		if !ok {
			slog.Error("streaming unsupported by response writer")
			http.Error(responseWriter, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		channel := metricsRegistry.Subscribe()
		defer metricsRegistry.Unsubscribe(channel)

		streamContext, stopBacklogRefresh := context.WithCancel(request.Context())
		defer stopBacklogRefresh()
		go refreshQueueBacklog(streamContext, metricsRegistry, queueBacklogFetcher, backlogRefreshInterval)

		// Send the current snapshot immediately so the client is never left waiting.
		initial := metricsRegistry.Snapshot()
		if writeError := writeEvent(responseWriter, flusher, initial); writeError != nil {
			slog.Error("writing initial metrics event failed", "error", writeError)
			return
		}

		for {
			select {
			case <-request.Context().Done():
				return
			case snapshot, ok := <-channel:
				if !ok {
					return
				}
				if writeError := writeEvent(responseWriter, flusher, snapshot); writeError != nil {
					slog.Error("writing metrics event failed", "error", writeError)
					return
				}
			}
		}
	}
}

// Refreshes the queue backlog on a fixed interval until the stream context is cancelled.
func refreshQueueBacklog(applicationContext context.Context, metricsRegistry *metrics.Metrics, queueBacklogFetcher *queueBacklogFetcher, backlogRefreshInterval time.Duration) {
	refreshTicker := time.NewTicker(backlogRefreshInterval)
	defer refreshTicker.Stop()
	for {
		select {
		case <-applicationContext.Done():
			return
		case <-refreshTicker.C:
			metricsRegistry.SetQueueBacklog(queueBacklogFetcher.readQueueBacklog(applicationContext, metricsRegistry.FallbackQueueBacklog()))
		}
	}
}

// Reports whether origin is in the allowed list.
func isOriginAllowed(origin string, allowedOrigins []string) bool {
	if len(allowedOrigins) == 0 {
		return true
	}
	return slices.Contains(allowedOrigins, origin)
}

// writeEvent serialises one snapshot as a server-sent event and flushes it to the client.
func writeEvent(responseWriter http.ResponseWriter, flusher http.Flusher, snapshot metrics.Snapshot) error {
	payload, encodeError := json.Marshal(snapshot)
	if encodeError != nil {
		return encodeError
	}
	if _, writeError := responseWriter.Write([]byte("data: ")); writeError != nil {
		return writeError
	}
	if _, writeError := responseWriter.Write(payload); writeError != nil {
		return writeError
	}
	if _, writeError := responseWriter.Write([]byte("\n\n")); writeError != nil {
		return writeError
	}
	flusher.Flush()
	return nil
}
