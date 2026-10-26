package handler

import (
	"net/http"

	"github.com/johneliud/log_ingestion_engine/internal/dashboard"
)

// Serves the home page with the architecture overview, diagram and navigation links.
func RootEndpointHandler() http.HandlerFunc {
	return func(responseWriter http.ResponseWriter, request *http.Request) {
		page, readError := dashboard.Assets.ReadFile("root.html")
		if readError != nil {
			http.Error(responseWriter, "home page unavailable", http.StatusInternalServerError)
			return
		}
		responseWriter.Header().Set("Content-Type", "text/html; charset=utf-8")
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(page)
	}
}
