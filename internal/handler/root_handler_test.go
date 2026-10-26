package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johneliud/log_ingestion_engine/internal/dashboard"
)

func TestRootEndpointHandler_ServesTheArchitectureHomePage(t *testing.T) {
	t.Parallel()

	responseRecorder := httptest.NewRecorder()
	RootEndpointHandler().ServeHTTP(responseRecorder, httptest.NewRequest(http.MethodGet, "/", nil))

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
		`id="architecture-diagram"`,
		"/dashboard/js/mermaid.min.js",
		"/dashboard/js/architecture.js",
		"/dashboard/css/styles.css",
		"bg-white",
		"text-slate-900",
		"max-w-screen-2xl",
		"What this project is about",
		"Design decisions",
	}
	for _, requiredContent := range requiredPageContent {
		if !strings.Contains(pageBody, requiredContent) {
			t.Errorf("home page does not contain %q", requiredContent)
		}
	}

	pageScript, scriptError := dashboard.Assets.ReadFile("static/js/architecture.js")
	if scriptError != nil {
		t.Fatalf("reading the embedded architecture.js: %v", scriptError)
	}
	if !strings.Contains(string(pageScript), `fetch("/dashboard/architecture.mmd")`) {
		t.Errorf("architecture.js does not fetch the embedded diagram source")
	}
	if !strings.Contains(string(pageScript), `theme: "neutral"`) {
		t.Errorf("architecture.js does not render the diagram with the light mermaid theme")
	}
	if !strings.Contains(string(pageScript), "useMaxWidth: false") {
		t.Errorf("architecture.js does not keep the diagram at its intrinsic size")
	}
	if strings.Contains(string(pageScript), "replaceChildren(renderedDiagram.svg)") {
		t.Errorf("architecture.js inserts the rendered SVG as text instead of markup")
	}
}
