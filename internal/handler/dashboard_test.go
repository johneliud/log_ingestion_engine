package handler

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/johneliud/log_ingestion_engine/internal/dashboard"
)

// The element ids issue 22 requires on the page: four stat cards, three charts, the top-services table and the refresh stamp.
var requiredDashboardElementIDs = []string{
	"total-logs-received",
	"logs-per-second",
	"error-rate",
	"queue-backlog",
	"throughput-chart",
	"error-rate-chart",
	"top-services-chart",
	"top-services-table",
	"last-refreshed",
}

// Serves one GET against the dashboard handler and returns the recorded response.
func serveDashboardRequest(t *testing.T, requestPath string) *httptest.ResponseRecorder {
	t.Helper()
	responseRecorder := httptest.NewRecorder()
	NewDashboardHandler().ServeHTTP(responseRecorder, httptest.NewRequest(http.MethodGet, requestPath, nil))
	return responseRecorder
}

// Escapes one Tailwind class token so it can be searched for as a CSS class selector.
func cssClassSelector(classToken string) string {
	escapedToken := strings.Builder{}
	for _, character := range classToken {
		isUnescapedIdentifierCharacter := (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '-'
		if isUnescapedIdentifierCharacter {
			escapedToken.WriteRune(character)
			continue
		}
		escapedToken.WriteRune('\\')
		escapedToken.WriteRune(character)
	}
	return "." + escapedToken.String()
}

func TestDashboardHandler_ServesThePageWithEveryRequiredElement(t *testing.T) {
	t.Parallel()

	pagePaths := []string{"/dashboard", "/dashboard/"}
	for _, pagePath := range pagePaths {
		t.Run(pagePath, func(t *testing.T) {
			t.Parallel()

			responseRecorder := serveDashboardRequest(t, pagePath)

			if responseRecorder.Code != http.StatusOK {
				t.Errorf("status = %d, want %d", responseRecorder.Code, http.StatusOK)
			}
			if contentType := responseRecorder.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/html") {
				t.Errorf("content type = %q, want text/html", contentType)
			}

			pageBody := responseRecorder.Body.String()
			for _, requiredElementID := range requiredDashboardElementIDs {
				if !strings.Contains(pageBody, `id="`+requiredElementID+`"`) {
					t.Errorf("page is missing the element id %q", requiredElementID)
				}
			}
			for _, referencedAsset := range []string{"/dashboard/css/styles.css", "/dashboard/js/app.js", "/dashboard/js/chart.umd.js"} {
				if !strings.Contains(pageBody, referencedAsset) {
					t.Errorf("page does not reference %q", referencedAsset)
				}
			}
		})
	}
}

func TestDashboardHandler_ServesCommittedAssets(t *testing.T) {
	t.Parallel()

	assetCases := []struct {
		name              string
		requestPath       string
		expectedStatus    int
		expectedMediaType string
		expectedSubstring string
	}{
		{name: "generated stylesheet", requestPath: "/dashboard/css/styles.css", expectedStatus: http.StatusOK, expectedMediaType: "text/css", expectedSubstring: "min-h-screen"},
		{name: "page script", requestPath: "/dashboard/js/app.js", expectedStatus: http.StatusOK, expectedMediaType: "text/javascript", expectedSubstring: "fetchMetricsSnapshot"},
		{name: "vendored chart library", requestPath: "/dashboard/js/chart.umd.js", expectedStatus: http.StatusOK, expectedMediaType: "text/javascript", expectedSubstring: "Chart.js"},
		{name: "vendored diagram library", requestPath: "/dashboard/js/mermaid.min.js", expectedStatus: http.StatusOK, expectedMediaType: "text/javascript", expectedSubstring: "mermaid"},
		{name: "architecture diagram source", requestPath: "/dashboard/architecture.mmd", expectedStatus: http.StatusOK, expectedMediaType: "text/plain", expectedSubstring: "flowchart LR"},
		{name: "unknown asset", requestPath: "/dashboard/missing.js", expectedStatus: http.StatusNotFound, expectedMediaType: "", expectedSubstring: ""},
		{name: "asset directory", requestPath: "/dashboard/js", expectedStatus: http.StatusNotFound, expectedMediaType: "", expectedSubstring: ""},
	}

	for _, assetCase := range assetCases {
		t.Run(assetCase.name, func(t *testing.T) {
			t.Parallel()

			responseRecorder := serveDashboardRequest(t, assetCase.requestPath)

			if responseRecorder.Code != assetCase.expectedStatus {
				t.Errorf("status = %d, want %d", responseRecorder.Code, assetCase.expectedStatus)
			}
			if assetCase.expectedMediaType != "" {
				if contentType := responseRecorder.Header().Get("Content-Type"); !strings.HasPrefix(contentType, assetCase.expectedMediaType) {
					t.Errorf("content type = %q, want prefix %q", contentType, assetCase.expectedMediaType)
				}
			}
			if assetCase.expectedSubstring != "" && !strings.Contains(responseRecorder.Body.String(), assetCase.expectedSubstring) {
				t.Errorf("body does not contain %q", assetCase.expectedSubstring)
			}
		})
	}
}

func TestDashboardHandler_EveryPageClassIsInTheGeneratedStylesheet(t *testing.T) {
	t.Parallel()

	compiledStyles, stylesError := dashboard.Assets.ReadFile("static/css/styles.css")
	if stylesError != nil {
		t.Fatalf("reading the embedded stylesheet: %v", stylesError)
	}

	classAttributePattern := regexp.MustCompile(`class="([^"]*)"`)
	checkedClassCount := 0
	for _, pageAssetName := range []string{"index.html", "root.html"} {
		pageHTML, pageError := dashboard.Assets.ReadFile(pageAssetName)
		if pageError != nil {
			t.Fatalf("reading the embedded %s: %v", pageAssetName, pageError)
		}
		for _, classAttributeMatch := range classAttributePattern.FindAllStringSubmatch(string(pageHTML), -1) {
			for _, classToken := range strings.Fields(classAttributeMatch[1]) {
				checkedClassCount++
				if !strings.Contains(string(compiledStyles), cssClassSelector(classToken)) {
					t.Errorf("class %q in %s is missing from styles.css; run `make dashboard-css` and commit the result", classToken, pageAssetName)
				}
			}
		}
	}

	if checkedClassCount < 20 {
		t.Errorf("checked only %d class tokens, want at least 20; the page markup or extraction is broken", checkedClassCount)
	}
}

func TestDashboardAssets_ReferenceNoExternalResources(t *testing.T) {
	t.Parallel()

	for _, assetName := range []string{"index.html", "root.html", "static/js/app.js", "static/js/architecture.js"} {
		content, readError := dashboard.Assets.ReadFile(assetName)
		if readError != nil {
			t.Fatalf("reading the embedded %s: %v", assetName, readError)
		}
		if strings.Contains(string(content), "https://") || strings.Contains(string(content), "http://") {
			t.Errorf("%s references an external URL; the page must work fully offline from embedded assets", assetName)
		}
	}

	pageScript, scriptError := dashboard.Assets.ReadFile("static/js/app.js")
	if scriptError != nil {
		t.Fatalf("reading the embedded app.js: %v", scriptError)
	}
	if !strings.Contains(string(pageScript), `"/metrics"`) {
		t.Errorf("app.js does not poll the local /metrics endpoint")
	}
}
