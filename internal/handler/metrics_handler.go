package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/metrics"
)

// Returns a handler that refreshes the queue backlog and serves the metrics snapshot as JSON.
func NewMetricsHandler(metricsRegistry *metrics.Metrics, applicationConfiguration config.ApplicationConfiguration) http.HandlerFunc {
	queueBacklogFetcher := newQueueBacklogFetcher(applicationConfiguration)
	return func(responseWriter http.ResponseWriter, request *http.Request) {
		metricsRegistry.SetQueueBacklog(queueBacklogFetcher.readQueueBacklog(request.Context(), metricsRegistry.FallbackQueueBacklog()))

		responseWriter.Header().Set("Content-Type", "application/json")
		if encodeError := json.NewEncoder(responseWriter).Encode(metricsRegistry.Snapshot()); encodeError != nil {
			slog.Error("writing the metrics response failed", "error", encodeError)
		}
	}
}
