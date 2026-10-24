package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/google/uuid"
	"github.com/johneliud/log_ingestion_engine/internal/model"
)

const applicationJSONMediaType = "application/json"

// Returns an HTTP handler that validates a JSON array of log entries and responds with a batch identifier plus any per-entry rejection details.
func NewLogIngestionHandler(maximumBodySizeInBytes int) http.HandlerFunc {
	return func(responseWriter http.ResponseWriter, request *http.Request) {
		mediaType, _, parseError := mime.ParseMediaType(request.Header.Get("Content-Type"))
		if parseError != nil || mediaType != applicationJSONMediaType {
			writeJSONError(responseWriter, http.StatusUnsupportedMediaType, "content type must be application/json")
			return
		}

		request.Body = http.MaxBytesReader(responseWriter, request.Body, int64(maximumBodySizeInBytes))
		requestBody, readError := io.ReadAll(request.Body)
		if readError != nil {
			if _, isMaxBytesError := errors.AsType[*http.MaxBytesError](readError); isMaxBytesError {
				writeJSONError(responseWriter, http.StatusRequestEntityTooLarge, "request body exceeds the maximum allowed size")
				return
			}
			writeJSONError(responseWriter, http.StatusBadRequest, "could not read the request body")
			return
		}

		if len(requestBody) == 0 {
			writeJSONError(responseWriter, http.StatusBadRequest, "request body is empty")
			return
		}

		var logEntries []model.LogEntry
		if jsonError := json.Unmarshal(requestBody, &logEntries); jsonError != nil {
			writeJSONError(responseWriter, http.StatusBadRequest, fmt.Sprintf("malformed JSON: %s", jsonError.Error()))
			return
		}

		if logEntries == nil {
			writeJSONError(responseWriter, http.StatusBadRequest, "request body must be a JSON array")
			return
		}

		validEntries, rejectedEntries := validateLogEntries(logEntries)

		if len(logEntries) > 0 && len(validEntries) == 0 {
			writeJSONResponse(responseWriter, http.StatusBadRequest, model.AllEntriesRejectedResponse{
				Status:   "rejected",
				Rejected: rejectedEntries,
			})
			return
		}

		batchID := uuid.NewString()
		writeJSONResponse(responseWriter, http.StatusAccepted, model.AcceptedResponse{
			Status:   "accepted",
			BatchID:  batchID,
			Rejected: rejectedEntries,
		})
	}
}

type logIngestionHandlerConfiguration struct {
	maximumBodySizeInBytes int
	rawLogPusher           rawLogPusher
	currentTimeProvider    func() time.Time
}

// LogIngestionHandlerOption configures the log ingestion handler.
type LogIngestionHandlerOption func(*logIngestionHandlerConfiguration)

// WithRawLogPusher configures the raw log pusher.
func WithRawLogPusher(rawLogPusher rawLogPusher) LogIngestionHandlerOption {
	return func(configuration *logIngestionHandlerConfiguration) {
		configuration.rawLogPusher = rawLogPusher
	}
}

// WithCurrentTimeProviderForIngestion configures the time provider.
func WithCurrentTimeProviderForIngestion(currentTimeProvider func() time.Time) LogIngestionHandlerOption {
	return func(configuration *logIngestionHandlerConfiguration) {
		if currentTimeProvider != nil {
			configuration.currentTimeProvider = currentTimeProvider
		}
	}
}
