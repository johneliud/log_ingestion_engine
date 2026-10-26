package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
)

const testMaximumBodySizeInBytes = 1 << 20

// Returns a request router built with a minimal configuration for route-level tests.
func aRequestRouter() *http.ServeMux {
	return buildRequestRouter(config.ApplicationConfiguration{
		MaximumBodySizeInBytes:  testMaximumBodySizeInBytes,
		MetricsThroughputWindow: time.Minute,
	})
}

func TestBuildRequestRouter_RoutesEveryPageAndMethod(t *testing.T) {
	t.Parallel()

	routeCases := []struct {
		name           string
		method         string
		requestPath    string
		expectedStatus int
	}{
		{name: "home page", method: http.MethodGet, requestPath: "/", expectedStatus: http.StatusOK},
		{name: "unknown path", method: http.MethodGet, requestPath: "/does-not-exist", expectedStatus: http.StatusNotFound},
		{name: "dashboard page", method: http.MethodGet, requestPath: "/dashboard", expectedStatus: http.StatusOK},
		{name: "dashboard asset", method: http.MethodGet, requestPath: "/dashboard/css/styles.css", expectedStatus: http.StatusOK},
		{name: "metrics page", method: http.MethodGet, requestPath: "/metrics", expectedStatus: http.StatusOK},
		{name: "metrics snapshot", method: http.MethodGet, requestPath: "/metrics.json", expectedStatus: http.StatusOK},
		{name: "ingestion over get", method: http.MethodGet, requestPath: "/logs", expectedStatus: http.StatusMethodNotAllowed},
		{name: "home page over post", method: http.MethodPost, requestPath: "/", expectedStatus: http.StatusMethodNotAllowed},
	}

	for _, routeCase := range routeCases {
		t.Run(routeCase.name, func(t *testing.T) {
			t.Parallel()

			responseRecorder := httptest.NewRecorder()
			aRequestRouter().ServeHTTP(responseRecorder, httptest.NewRequest(routeCase.method, routeCase.requestPath, nil))

			if responseRecorder.Code != routeCase.expectedStatus {
				t.Errorf("status = %d, want %d", responseRecorder.Code, routeCase.expectedStatus)
			}
		})
	}
}
