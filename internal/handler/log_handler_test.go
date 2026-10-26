package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	"github.com/johneliud/log_ingestion_engine/internal/pipeline"
)

const testMaximumBodySizeInBytes = 1048576

// Records the log entries the ingestion handler accepted.
type recordingIngestionMetricsRecorder struct {
	mutex           sync.Mutex
	recordedEntries []model.LogEntry
}

// Records the accepted log entries.
func (recordingRecorder *recordingIngestionMetricsRecorder) RecordLogsReceived(logs []model.LogEntry) {
	recordingRecorder.mutex.Lock()
	defer recordingRecorder.mutex.Unlock()
	recordingRecorder.recordedEntries = append(recordingRecorder.recordedEntries, logs...)
}

// Returns the number of recorded log entries.
func (recordingRecorder *recordingIngestionMetricsRecorder) recordedEntryCount() int {
	recordingRecorder.mutex.Lock()
	defer recordingRecorder.mutex.Unlock()
	return len(recordingRecorder.recordedEntries)
}

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

const validEntryJSON = `{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"ERROR","message":"timeout"}`

func TestNewLogIngestionHandler_QueueAdmission(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name       string
		capacity   int
		prefill    int
		body       string
		wantStatus int
		wantQueued int
		wantError  string
	}{
		{"oversized batch", 1, 0, `[` + validEntryJSON + `,` + validEntryJSON + `]`, http.StatusRequestEntityTooLarge, 0, "batch exceeds ingestion queue capacity"},
		{"temporary overload", 2, 1, `[` + validEntryJSON + `,` + validEntryJSON + `]`, http.StatusServiceUnavailable, 1, "ingestion overloaded"},
		{"accepted batch", 2, 0, `[` + validEntryJSON + `,` + validEntryJSON + `]`, http.StatusAccepted, 2, ""},
		{"only valid entries need capacity", 1, 0, `[` + validEntryJSON + `,{}]`, http.StatusAccepted, 1, ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			queue := pipeline.NewRawLogsQueue(testCase.capacity, 20*time.Millisecond)
			if err := queue.PushBatch(make([]model.RawLog, testCase.prefill)); err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader(testCase.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			NewLogIngestionHandler(testMaximumBodySizeInBytes, WithRawLogPusher(queue)).ServeHTTP(response, request)
			if response.Code != testCase.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", response.Code, testCase.wantStatus, response.Body.String())
			}
			if len(queue.RawLogsChannel()) != testCase.wantQueued {
				t.Fatalf("queued entries = %d, want %d", len(queue.RawLogsChannel()), testCase.wantQueued)
			}
			if testCase.wantError != "" {
				var body model.ErrorResponse
				if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if body.Error != testCase.wantError {
					t.Fatalf("error = %q, want %q", body.Error, testCase.wantError)
				}
			}
		})
	}
}

func TestNewLogIngestionHandler_CountsOnlyTheAcceptedEntriesInTheMetricsRecorder(t *testing.T) {
	t.Parallel()
	recordingRecorder := &recordingIngestionMetricsRecorder{}
	rawLogsQueue := pipeline.NewRawLogsQueue(10, 20*time.Millisecond)
	request := httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader(`[`+validEntryJSON+`,{}]`))
	request.Header.Set("Content-Type", "application/json")
	responseRecorder := httptest.NewRecorder()
	NewLogIngestionHandler(testMaximumBodySizeInBytes, WithRawLogPusher(rawLogsQueue), WithIngestionMetricsRecorder(recordingRecorder)).ServeHTTP(responseRecorder, request)

	if responseRecorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", responseRecorder.Code, http.StatusAccepted)
	}
	if recordingRecorder.recordedEntryCount() != 1 {
		t.Errorf("recorded entry count = %d, want 1", recordingRecorder.recordedEntryCount())
	}
}

func TestNewLogIngestionHandler_CountsAcceptedEntriesWithoutARawLogPusher(t *testing.T) {
	t.Parallel()
	recordingRecorder := &recordingIngestionMetricsRecorder{}
	request := httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader(`[`+validEntryJSON+`,{}]`))
	request.Header.Set("Content-Type", "application/json")
	responseRecorder := httptest.NewRecorder()
	NewLogIngestionHandler(testMaximumBodySizeInBytes, WithIngestionMetricsRecorder(recordingRecorder)).ServeHTTP(responseRecorder, request)

	if responseRecorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", responseRecorder.Code, http.StatusAccepted)
	}
	if recordingRecorder.recordedEntryCount() != 1 {
		t.Errorf("recorded entry count = %d, want 1", recordingRecorder.recordedEntryCount())
	}
}

func TestNewLogIngestionHandler_PartialBatchReturnsAcceptedWithRejectedEntries(t *testing.T) {
	t.Parallel()
	requestBody := fmt.Sprintf(
		`[%s,{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"TRACE","message":"bad level"},%s]`,
		validEntryJSON, validEntryJSON,
	)
	responseRecorder := sendLogIngestionRequest(t, requestBody, "application/json", testMaximumBodySizeInBytes)

	if responseRecorder.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d (body: %s)", responseRecorder.Code, http.StatusAccepted, responseRecorder.Body.String())
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

	if len(acceptedResponse.Rejected) != 1 {
		t.Fatalf("rejected entries = %d, want 1", len(acceptedResponse.Rejected))
	}

	if acceptedResponse.Rejected[0].Index != 1 {
		t.Errorf("rejected entry index = %d, want 1", acceptedResponse.Rejected[0].Index)
	}

	if len(acceptedResponse.Rejected[0].Errors) != 1 {
		t.Fatalf("rejected entry error count = %d, want 1", len(acceptedResponse.Rejected[0].Errors))
	}

	if acceptedResponse.Rejected[0].Errors[0].Field != "level" {
		t.Errorf("rejected entry error field = %q, want %q", acceptedResponse.Rejected[0].Errors[0].Field, "level")
	}
}

func TestNewLogIngestionHandler_AllEntriesInvalidReturns400WithPerEntryErrors(t *testing.T) {
	t.Parallel()
	requestBody := `[
		{"timestamp":"not-a-timestamp","service":"claims","level":"ERROR","message":"timeout"},
		{"timestamp":"2026-10-24T10:00:00Z","service":"claims","level":"TRACE","message":"bad level"}
	]`
	responseRecorder := sendLogIngestionRequest(t, requestBody, "application/json", testMaximumBodySizeInBytes)

	if responseRecorder.Code != http.StatusBadRequest {
		t.Fatalf("status code = %d, want %d (body: %s)", responseRecorder.Code, http.StatusBadRequest, responseRecorder.Body.String())
	}

	var rejectedResponse model.AllEntriesRejectedResponse
	if jsonError := json.Unmarshal(responseRecorder.Body.Bytes(), &rejectedResponse); jsonError != nil {
		t.Fatalf("could not parse the rejected response: %v", jsonError)
	}

	if rejectedResponse.Status != "rejected" {
		t.Errorf("status = %q, want %q", rejectedResponse.Status, "rejected")
	}

	if len(rejectedResponse.Rejected) != 2 {
		t.Fatalf("rejected entries = %d, want 2", len(rejectedResponse.Rejected))
	}

	if rejectedResponse.Rejected[0].Index != 0 {
		t.Errorf("first rejected entry index = %d, want 0", rejectedResponse.Rejected[0].Index)
	}

	if rejectedResponse.Rejected[1].Index != 1 {
		t.Errorf("second rejected entry index = %d, want 1", rejectedResponse.Rejected[1].Index)
	}

	timestampErrorMessage := errorMessageForFieldInRejected(rejectedResponse.Rejected[0].Errors, "timestamp")
	if timestampErrorMessage == "" {
		t.Errorf("expected a timestamp error for the first rejected entry, but none was found")
	}

	levelErrorMessage := errorMessageForFieldInRejected(rejectedResponse.Rejected[1].Errors, "level")
	if levelErrorMessage == "" {
		t.Errorf("expected a level error for the second rejected entry, but none was found")
	}
}

func TestNewLogIngestionHandler_AllValidEntriesReturnAcceptedWithoutRejectedField(t *testing.T) {
	t.Parallel()
	requestBody := fmt.Sprintf(`[%s,%s]`, validEntryJSON, validEntryJSON)
	responseRecorder := sendLogIngestionRequest(t, requestBody, "application/json", testMaximumBodySizeInBytes)

	if responseRecorder.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d", responseRecorder.Code, http.StatusAccepted)
	}

	if strings.Contains(responseRecorder.Body.String(), "rejected") {
		t.Errorf("response body should not contain a rejected field when all entries are valid, but got: %s", responseRecorder.Body.String())
	}
}

func TestNewLogIngestionHandler_PartialBatchWithMultipleFieldErrorsReturnsAllErrorsForRejectedEntry(t *testing.T) {
	t.Parallel()
	requestBody := fmt.Sprintf(
		`[%s,{"timestamp":"bad","service":"","level":"TRACE","message":""}]`,
		validEntryJSON,
	)
	responseRecorder := sendLogIngestionRequest(t, requestBody, "application/json", testMaximumBodySizeInBytes)

	if responseRecorder.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d", responseRecorder.Code, http.StatusAccepted)
	}

	var acceptedResponse model.AcceptedResponse
	if jsonError := json.Unmarshal(responseRecorder.Body.Bytes(), &acceptedResponse); jsonError != nil {
		t.Fatalf("could not parse the accepted response: %v", jsonError)
	}

	if len(acceptedResponse.Rejected) != 1 {
		t.Fatalf("rejected entries = %d, want 1", len(acceptedResponse.Rejected))
	}

	if len(acceptedResponse.Rejected[0].Errors) != 4 {
		t.Errorf("rejected entry error count = %d, want 4 (timestamp, service, level, message)", len(acceptedResponse.Rejected[0].Errors))
	}
}

// Finds the error message for the given field name within a slice of rejected-entry field errors.
func errorMessageForFieldInRejected(fieldErrors []model.FieldError, fieldName string) string {
	for _, fieldError := range fieldErrors {
		if fieldError.Field == fieldName {
			return fieldError.Message
		}
	}
	return ""
}
