package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Writes the response body as JSON with the given status code and content type header.
func writeJSONResponse(responseWriter http.ResponseWriter, statusCode int, responseBody any) {
	encodedBody, encodingError := json.Marshal(responseBody)
	if encodingError != nil {
		slog.Error("encoding json response", "error", encodingError)
		responseWriter.WriteHeader(http.StatusInternalServerError)
		return
	}

	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(statusCode)
	responseWriter.Write(encodedBody)
}

// Writes an error response with the given status code and message.
func WriteJSONError(responseWriter http.ResponseWriter, statusCode int, errorMessage string) {
	writeJSONResponse(responseWriter, statusCode, model.ErrorResponse{Error: errorMessage})
}
