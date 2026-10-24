package model

// Holds a single field-level validation error for a log entry.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// Holds the validation errors for a single rejected log entry at its original array index.
type RejectedEntry struct {
	Index  int          `json:"index"`
	Errors []FieldError `json:"errors"`
}

// Holds the response when every entry in a batch fails validation.
type AllEntriesRejectedResponse struct {
	Status   string          `json:"status"`
	Rejected []RejectedEntry `json:"rejected"`
}
