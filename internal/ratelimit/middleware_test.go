package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeLimiter struct {
	allowCalls int
	delay      time.Duration
	allowed    bool
}

func (fakeLimiterInstance *fakeLimiter) AllowWithoutConsumingCapacity(sourceIP string) (time.Duration, bool) {
	fakeLimiterInstance.allowCalls++
	return fakeLimiterInstance.delay, fakeLimiterInstance.allowed
}

func TestNewRateLimitMiddleware_CallsHandlerWhenAllowed(t *testing.T) {
	t.Parallel()
	handlerCalled := false
	fake := &fakeLimiter{allowed: true}
	middleware := NewRateLimitMiddleware(fake, func(responseWriter http.ResponseWriter, statusCode int, errorMessage string) {})
	handler := middleware(func(responseWriter http.ResponseWriter, request *http.Request) {
		handlerCalled = true
		responseWriter.WriteHeader(http.StatusAccepted)
	})

	request := httptest.NewRequest(http.MethodPost, "/logs", nil)
	request.RemoteAddr = "203.0.113.1:1234"
	responseRecorder := httptest.NewRecorder()
	handler(responseRecorder, request)

	if !handlerCalled {
		t.Fatalf("handler should be called when allowed")
	}
	if responseRecorder.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d", responseRecorder.Code, http.StatusAccepted)
	}
}

func TestNewRateLimitMiddleware_Returns429AndNeverCallsHandlerWhenRejected(t *testing.T) {
	t.Parallel()
	handlerCalled := false
	fake := &fakeLimiter{allowed: false, delay: 1500 * time.Millisecond}
	middleware := NewRateLimitMiddleware(fake, func(responseWriter http.ResponseWriter, statusCode int, errorMessage string) {
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(statusCode)
		responseWriter.Write([]byte(`{"error":"` + errorMessage + `"}`))
	})
	handler := middleware(func(responseWriter http.ResponseWriter, request *http.Request) {
		handlerCalled = true
		responseWriter.WriteHeader(http.StatusAccepted)
	})

	request := httptest.NewRequest(http.MethodPost, "/logs", nil)
	request.RemoteAddr = "203.0.113.2:1234"
	responseRecorder := httptest.NewRecorder()
	handler(responseRecorder, request)

	if handlerCalled {
		t.Fatalf("handler should not be called when rejected")
	}
	if responseRecorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status code = %d, want %d", responseRecorder.Code, http.StatusTooManyRequests)
	}
	if responseRecorder.Header().Get("Retry-After") != "2" {
		t.Fatalf("Retry-After = %q, want %q", responseRecorder.Header().Get("Retry-After"), "2")
	}
}

func TestNewRateLimitMiddleware_SetsRetryAfterToAtLeastOneSecond(t *testing.T) {
	t.Parallel()
	fake := &fakeLimiter{allowed: false, delay: 100 * time.Millisecond}
	middleware := NewRateLimitMiddleware(fake, func(responseWriter http.ResponseWriter, statusCode int, errorMessage string) {
		responseWriter.WriteHeader(statusCode)
	})
	handler := middleware(func(responseWriter http.ResponseWriter, request *http.Request) {})

	request := httptest.NewRequest(http.MethodPost, "/logs", nil)
	request.RemoteAddr = "203.0.113.3:1234"
	responseRecorder := httptest.NewRecorder()
	handler(responseRecorder, request)

	if responseRecorder.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want %q", responseRecorder.Header().Get("Retry-After"), "1")
	}
}
