package model

// Represents the severity level of a log entry.
type LogLevel string

// The set of accepted log levels.
const (
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
	LogLevelDebug LogLevel = "DEBUG"
)

// Represents a single log entry submitted by a client over HTTP.
type LogEntry struct {
	Timestamp string   `json:"timestamp"`
	Service   string   `json:"service"`
	Level     LogLevel `json:"level"`
	Message   string   `json:"message"`
}
