package model

// Holds the status and batch identifier for a successfully accepted log batch.
type AcceptedResponse struct {
	Status  string `json:"status"`
	BatchID string `json:"batchId"`
}

// Holds the error message for a rejected HTTP request.
type ErrorResponse struct {
	Error string `json:"error"`
}
