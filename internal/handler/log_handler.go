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

// rawLogPusher accepts a validated batch into the raw_logs channel.
type rawLogPusher interface {
	PushBatch([]model.RawLog) error
}

// spooler durably records accepted logs before the handler reports acceptance.
type spooler interface {
	Append([]model.RawLog) error
	Ack(logID string)
}

// Counts the log entries accepted by the ingestion handler.
type ingestionMetricsRecorder interface {
	RecordLogsReceived(logs []model.LogEntry)
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
			WriteJSONError(responseWriter, http.StatusUnsupportedMediaType, "content type must be application/json")
			return
		}

		request.Body = http.MaxBytesReader(responseWriter, request.Body, int64(handlerConfiguration.maximumBodySizeInBytes))
		requestBody, readError := io.ReadAll(request.Body)
		if readError != nil {
			if _, isMaxBytesError := errors.AsType[*http.MaxBytesError](readError); isMaxBytesError {
				WriteJSONError(responseWriter, http.StatusRequestEntityTooLarge, "request body exceeds the maximum allowed size")
				return
			}
			WriteJSONError(responseWriter, http.StatusBadRequest, "could not read the request body")
			return
		}

		if len(requestBody) == 0 {
			WriteJSONError(responseWriter, http.StatusBadRequest, "request body is empty")
			return
		}

		var logEntries []model.LogEntry
		if jsonError := json.Unmarshal(requestBody, &logEntries); jsonError != nil {
			WriteJSONError(responseWriter, http.StatusBadRequest, fmt.Sprintf("malformed JSON: %s", jsonError.Error()))
			return
		}

		if logEntries == nil {
			WriteJSONError(responseWriter, http.StatusBadRequest, "request body must be a JSON array")
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
					LogID:      uuid.NewString(),
					BatchID:    batchID,
					SourceIP:   sourceIP,
					ReceivedAt: receivedAt,
				})
			}

			if handlerConfiguration.spooler != nil {
				if pushError := handlerConfiguration.spooler.Append(rawLogs); pushError != nil {
					WriteJSONError(responseWriter, http.StatusServiceUnavailable, "ingestion could not durably accept the batch")
					return
				}
			}

			if pushError := handlerConfiguration.rawLogPusher.PushBatch(rawLogs); pushError != nil {
				handlerConfiguration.voidSpooledBatch(rawLogs)
				if errors.Is(pushError, pipeline.ErrBatchTooLarge) {
					WriteJSONError(responseWriter, http.StatusRequestEntityTooLarge, "batch exceeds ingestion queue capacity")
					return
				}
				WriteJSONError(responseWriter, http.StatusServiceUnavailable, "ingestion overloaded")
				return
			}
			handlerConfiguration.recordLogsReceived(validEntries)
			writeJSONResponse(responseWriter, http.StatusAccepted, model.AcceptedResponse{
				Status:   "accepted",
				BatchID:  batchID,
				Rejected: rejectedEntries,
			})
			return
		}

		handlerConfiguration.recordLogsReceived(validEntries)
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
	spooler                spooler
	currentTimeProvider    func() time.Time
	trustedProxies         []string
	metricsRecorder        ingestionMetricsRecorder
}

// Counts the accepted log entries when a metrics recorder is configured.
func (configuration *logIngestionHandlerConfiguration) recordLogsReceived(validEntries []model.LogEntry) {
	if configuration.metricsRecorder == nil || len(validEntries) == 0 {
		return
	}
	configuration.metricsRecorder.RecordLogsReceived(validEntries)
}

// Marks a spooled batch as handled after the queue refused it, so a batch the client was told was rejected is never replayed into the pipeline at the next startup.
func (configuration *logIngestionHandlerConfiguration) voidSpooledBatch(rawLogs []model.RawLog) {
	if configuration.spooler == nil {
		return
	}
	for _, rawLog := range rawLogs {
		configuration.spooler.Ack(rawLog.LogID)
	}
}

// LogIngestionHandlerOption configures the log ingestion handler.
type LogIngestionHandlerOption func(*logIngestionHandlerConfiguration)

// WithRawLogPusher configures the raw log pusher.
func WithRawLogPusher(rawLogPusher rawLogPusher) LogIngestionHandlerOption {
	return func(configuration *logIngestionHandlerConfiguration) {
		configuration.rawLogPusher = rawLogPusher
	}
}

// WithSpool configures the durable spool that accepts logs before the handler reports a 202.
func WithSpool(spooler spooler) LogIngestionHandlerOption {
	return func(configuration *logIngestionHandlerConfiguration) {
		configuration.spooler = spooler
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

// WithIngestionMetricsRecorder configures the recorder that counts accepted log entries.
func WithIngestionMetricsRecorder(metricsRecorder ingestionMetricsRecorder) LogIngestionHandlerOption {
	return func(configuration *logIngestionHandlerConfiguration) {
		configuration.metricsRecorder = metricsRecorder
	}
}
