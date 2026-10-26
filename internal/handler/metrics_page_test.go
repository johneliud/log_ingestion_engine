package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johneliud/log_ingestion_engine/internal/dashboard"
)

func TestMetricsPageEndpointHandler_ServesTheMetricsPage(t *testing.T) {
	t.Parallel()

	responseRecorder := httptest.NewRecorder()
	MetricsPageEndpointHandler().ServeHTTP(responseRecorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if responseRecorder.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", responseRecorder.Code, http.StatusOK)
	}
	if contentType := responseRecorder.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/html") {
		t.Errorf("content type = %q, want text/html", contentType)
	}

	pageBody := responseRecorder.Body.String()
	requiredPageContent := []string{
		`href="/"`,
		`href="/dashboard"`,
		`href="/metrics"`,
		`href="/metrics.json"`,
		`id="total-logs-received"`,
		`id="logs-per-second"`,
		`id="error-rate"`,
		`id="queue-backlog"`,
		`id="rate-limited-requests"`,
		`id="dead-lettered-logs"`,
		`id="write-failures"`,
		`id="logs-by-level-table"`,
		`id="logs-by-service-table"`,
		"/dashboard/css/styles.css",
		"/dashboard/js/metrics.js",
	}
	for _, requiredContent := range requiredPageContent {
		if !strings.Contains(pageBody, requiredContent) {
			t.Errorf("metrics page does not contain %q", requiredContent)
		}
	}

	pageScript, scriptError := dashboard.Assets.ReadFile("static/js/metrics.js")
	if scriptError != nil {
		t.Fatalf("reading the embedded metrics.js: %v", scriptError)
	}
	if !strings.Contains(string(pageScript), `fetch("/metrics.json")`) {
		t.Errorf("metrics.js does not read the JSON snapshot endpoint")
	}
}
