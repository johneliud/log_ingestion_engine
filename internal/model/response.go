package model

// Holds the status and batch identifier for a successfully accepted log batch, with optional per-entry rejection details.
type AcceptedResponse struct {
	Status   string          `json:"status"`
	BatchID  string          `json:"batchId"`
	Rejected []RejectedEntry `json:"rejected,omitempty"`
}

// Holds the error message for a rejected HTTP request.
type ErrorResponse struct {
	Error string `json:"error"`
}
