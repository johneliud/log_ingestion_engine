package ingest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

const testMaximumBodySizeInBytes = 1048576

// Sends a POST request to the handler with the given body and content type, returning the recorded response.
func sendLogIngestionRequest(t *testing.T, requestBody string, contentType string, maximumBodySizeInBytes int) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader(requestBody))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	responseRecorder := httptest.NewRecorder()
	NewLogIngestionHandler(maximumBodySizeInBytes).ServeHTTP(responseRecorder, request)
	return responseRecorder
}

func TestNewLogIngestionHandler_ReturnsCorrectStatusCode(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                 string
		requestBody          string
		contentType          string
		maximumBodySizeBytes int
		expectedStatusCode   int
	}{
		{
			name:                 "accepts a valid JSON array and returns accepted",
			requestBody:          `[{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"ERROR","message":"timeout"}]`,
			contentType:          "application/json",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusAccepted,
		},
		{
			name:                 "accepts a valid JSON array with charset parameter",
			requestBody:          `[{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"ERROR","message":"timeout"}]`,
			contentType:          "application/json; charset=utf-8",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusAccepted,
		},
		{
			name:                 "accepts an empty JSON array",
			requestBody:          `[]`,
			contentType:          "application/json",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusAccepted,
		},
		{
			name:                 "rejects a non-JSON content type with unsupported media type",
			requestBody:          `[{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"ERROR","message":"timeout"}]`,
			contentType:          "text/plain",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusUnsupportedMediaType,
		},
		{
			name:                 "rejects a missing content type with unsupported media type",
			requestBody:          `[{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"ERROR","message":"timeout"}]`,
			contentType:          "",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusUnsupportedMediaType,
		},
		{
			name:                 "rejects an XML content type with unsupported media type",
			requestBody:          `<log>timeout</log>`,
			contentType:          "application/xml",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusUnsupportedMediaType,
		},
		{
			name:                 "rejects an oversized body with request entity too large",
			requestBody:          `[{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"ERROR","message":"timeout"}]`,
			contentType:          "application/json",
			maximumBodySizeBytes: 10,
			expectedStatusCode:   http.StatusRequestEntityTooLarge,
		},
		{
			name:                 "rejects malformed JSON with bad request",
			requestBody:          `{invalid`,
			contentType:          "application/json",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusBadRequest,
		},
		{
			name:                 "rejects a JSON object instead of an array with bad request",
			requestBody:          `{"timestamp":"2026-10-24T10:00:00Z"}`,
			contentType:          "application/json",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusBadRequest,
		},
		{
			name:                 "rejects null instead of an array with bad request",
			requestBody:          `null`,
			contentType:          "application/json",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusBadRequest,
		},
		{
			name:                 "rejects an empty body with bad request",
			requestBody:          ``,
			contentType:          "application/json",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusBadRequest,
		},
		{
			name:                 "rejects a JSON array of non-objects with bad request",
			requestBody:          `[1, 2, 3]`,
			contentType:          "application/json",
			maximumBodySizeBytes: testMaximumBodySizeInBytes,
			expectedStatusCode:   http.StatusBadRequest,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			responseRecorder := sendLogIngestionRequest(
				t,
				testCase.requestBody,
				testCase.contentType,
				testCase.maximumBodySizeBytes,
			)

			if responseRecorder.Code != testCase.expectedStatusCode {
				t.Fatalf("status code = %d, want %d (body: %s)", responseRecorder.Code, testCase.expectedStatusCode, responseRecorder.Body.String())
			}
		})
	}
}

func TestNewLogIngestionHandler_ReturnsValidBatchIdentifierOnAcceptedResponse(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name        string
		requestBody string
	}{
		{
			name:        "single entry array returns a valid UUID as batch identifier",
			requestBody: `[{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"ERROR","message":"timeout"}]`,
		},
		{
			name:        "multiple entry array returns a valid UUID as batch identifier",
			requestBody: `[{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"ERROR","message":"timeout"},{"timestamp":"2026-10-24T10:01:00Z","service":"billing","level":"INFO","message":"invoice processed"}]`,
		},
		{
			name:        "empty array returns a valid UUID as batch identifier",
			requestBody: `[]`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			responseRecorder := sendLogIngestionRequest(
				t,
				testCase.requestBody,
				"application/json",
				testMaximumBodySizeInBytes,
			)

			if responseRecorder.Code != http.StatusAccepted {
				t.Fatalf("status code = %d, want %d", responseRecorder.Code, http.StatusAccepted)
			}

			var acceptedResponse model.AcceptedResponse
			if jsonError := json.Unmarshal(responseRecorder.Body.Bytes(), &acceptedResponse); jsonError != nil {
				t.Fatalf("could not parse the accepted response: %v", jsonError)
			}

			if acceptedResponse.Status != "accepted" {
				t.Errorf("status = %q, want %q", acceptedResponse.Status, "accepted")
			}

			if _, parseError := uuid.Parse(acceptedResponse.BatchID); parseError != nil {
				t.Errorf("batchId %q is not a valid UUID: %v", acceptedResponse.BatchID, parseError)
			}
		})
	}
}

func TestNewLogIngestionHandler_SetsJSONContentTypeHeaderOnAcceptedResponse(t *testing.T) {
	t.Parallel()
	responseRecorder := sendLogIngestionRequest(
		t,
		`[]`,
		"application/json",
		testMaximumBodySizeInBytes,
	)

	if responseRecorder.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q, want %q", responseRecorder.Header().Get("Content-Type"), "application/json")
	}
}

func TestNewLogIngestionHandler_ReturnsErrorMessageInJSONOnErrorResponses(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name               string
		requestBody        string
		contentType        string
		expectedStatusCode int
	}{
		{
			name:               "unsupported media type error includes a message",
			requestBody:        `[{}]`,
			contentType:        "text/plain",
			expectedStatusCode: http.StatusUnsupportedMediaType,
		},
		{
			name:               "request entity too large error includes a message",
			requestBody:        `[{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"ERROR","message":"timeout"}]`,
			contentType:        "application/json",
			expectedStatusCode: http.StatusRequestEntityTooLarge,
		},
		{
			name:               "malformed JSON error includes a message",
			requestBody:        `{invalid`,
			contentType:        "application/json",
			expectedStatusCode: http.StatusBadRequest,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			maximumBodySizeInBytes := testMaximumBodySizeInBytes
			if testCase.expectedStatusCode == http.StatusRequestEntityTooLarge {
				maximumBodySizeInBytes = 10
			}
			responseRecorder := sendLogIngestionRequest(
				t,
				testCase.requestBody,
				testCase.contentType,
				maximumBodySizeInBytes,
			)

			if responseRecorder.Code != testCase.expectedStatusCode {
				t.Fatalf("status code = %d, want %d", responseRecorder.Code, testCase.expectedStatusCode)
			}

			var errorResponse model.ErrorResponse
			if jsonError := json.Unmarshal(responseRecorder.Body.Bytes(), &errorResponse); jsonError != nil {
				t.Fatalf("could not parse the error response: %v", jsonError)
			}

			if errorResponse.Error == "" {
				t.Errorf("error message is empty, want a non-empty message")
			}
		})
	}
}
