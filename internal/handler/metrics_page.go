package handler

import (
	"net/http"

	"github.com/johneliud/log_ingestion_engine/internal/dashboard"
)

// Serves the metrics page showing the runtime counters as readable values.
func MetricsPageEndpointHandler() http.HandlerFunc {
	return func(responseWriter http.ResponseWriter, request *http.Request) {
		page, readError := dashboard.Assets.ReadFile("metrics.html")
		if readError != nil {
			http.Error(responseWriter, "metrics page unavailable", http.StatusInternalServerError)
			return
		}
		responseWriter.Header().Set("Content-Type", "text/html; charset=utf-8")
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(page)
	}
}
