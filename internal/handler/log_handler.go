package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/johneliud/log_ingestion_engine/internal/ingest"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	"github.com/johneliud/log_ingestion_engine/internal/pipeline"
)

const applicationJSONMediaType = "application/json"

type rawLogPusher interface {
	PushBatch([]model.RawLog) error
}

// Returns an HTTP handler that validates a JSON array of log entries and responds with a batch identifier plus any per-entry rejection details.
func NewLogIngestionHandler(maximumBodySizeInBytes int, options ...LogIngestionHandlerOption) http.HandlerFunc {
	handlerConfiguration := logIngestionHandlerConfiguration{
		maximumBodySizeInBytes: maximumBodySizeInBytes,
		currentTimeProvider:    time.Now,
	}

	for _, option := range options {
		if option != nil {
			option(&handlerConfiguration)
		}
	}

	return func(responseWriter http.ResponseWriter, request *http.Request) {
		mediaType, _, parseError := mime.ParseMediaType(request.Header.Get("Content-Type"))
		if parseError != nil || mediaType != applicationJSONMediaType {
			writeJSONError(responseWriter, http.StatusUnsupportedMediaType, "content type must be application/json")
			return
		}

		request.Body = http.MaxBytesReader(responseWriter, request.Body, int64(handlerConfiguration.maximumBodySizeInBytes))
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

		validEntries, rejectedEntries := ingest.ValidateLogEntries(logEntries)

		if len(logEntries) > 0 && len(validEntries) == 0 {
			writeJSONResponse(responseWriter, http.StatusBadRequest, model.AllEntriesRejectedResponse{
				Status:   "rejected",
				Rejected: rejectedEntries,
			})
			return
		}

		if handlerConfiguration.rawLogPusher != nil && len(validEntries) > 0 {
			sourceIP := ingest.ResolveClientIP(request, handlerConfiguration.trustedProxies)
			receivedAt := handlerConfiguration.currentTimeProvider()
			batchID := uuid.NewString()
			rawLogs := make([]model.RawLog, 0, len(validEntries))

			for _, validEntry := range validEntries {
				rawLogs = append(rawLogs, model.RawLog{
					LogEntry:   validEntry,
					BatchID:    batchID,
					SourceIP:   sourceIP,
					ReceivedAt: receivedAt,
				})
			}

			if pushError := handlerConfiguration.rawLogPusher.PushBatch(rawLogs); pushError != nil {
				if errors.Is(pushError, pipeline.ErrBatchTooLarge) {
					writeJSONError(responseWriter, http.StatusRequestEntityTooLarge, "batch exceeds ingestion queue capacity")
					return
				}
				writeJSONError(responseWriter, http.StatusServiceUnavailable, "ingestion overloaded")
				return
			}
			writeJSONResponse(responseWriter, http.StatusAccepted, model.AcceptedResponse{
				Status:   "accepted",
				BatchID:  batchID,
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
	trustedProxies         []string
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

// Returns the trusted proxy CIDRs used when resolving the client IP from forwarded headers.
func WithTrustedProxies(trustedProxies []string) LogIngestionHandlerOption {
	return func(configuration *logIngestionHandlerConfiguration) {
		configuration.trustedProxies = trustedProxies
	}
}
