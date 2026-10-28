package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/johneliud/log_ingestion_engine/internal/metrics"
)

// Returns a handler that streams metrics snapshots as server-sent events.
func NewMetricsEventsHandler(metricsRegistry *metrics.Metrics) http.HandlerFunc {
	return func(responseWriter http.ResponseWriter, request *http.Request) {
		responseWriter.Header().Set("Content-Type", "text/event-stream")
		responseWriter.Header().Set("Cache-Control", "no-cache")
		responseWriter.Header().Set("Connection", "keep-alive")
		responseWriter.Header().Set("X-Accel-Buffering", "no")

		flusher, ok := responseWriter.(http.Flusher)
		if !ok {
			slog.Error("streaming unsupported by response writer")
			http.Error(responseWriter, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		channel := metricsRegistry.Subscribe()
		defer metricsRegistry.Unsubscribe(channel)

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
