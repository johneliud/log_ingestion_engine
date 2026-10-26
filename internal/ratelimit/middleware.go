package ratelimit

import (
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/ingest"
)

// Reports whether a request is allowed and returns the wait duration when not allowed.
type Limiter interface {
	AllowWithoutConsumingCapacity(sourceIP string) (time.Duration, bool)
}

// Counts the requests rejected by the rate limiter.
type rateLimitedRequestRecorder interface {
	RecordRateLimitedRequest()
}

// Holds the optional dependencies of the rate limit middleware.
type rateLimitMiddlewareConfiguration struct {
	metricsRecorder rateLimitedRequestRecorder
}

// Configures the rate limit middleware.
type RateLimitMiddlewareOption func(*rateLimitMiddlewareConfiguration)

// Configures the recorder that counts rejected requests.
func WithRateLimitedRequestRecorder(metricsRecorder rateLimitedRequestRecorder) RateLimitMiddlewareOption {
	return func(configuration *rateLimitMiddlewareConfiguration) {
		configuration.metricsRecorder = metricsRecorder
	}
}

// Returns middleware that rate limits requests by source IP.
func NewRateLimitMiddleware(limiter Limiter, writeJSONError func(http.ResponseWriter, int, string), trustedProxies []string, options ...RateLimitMiddlewareOption) func(http.HandlerFunc) http.HandlerFunc {
	middlewareConfiguration := rateLimitMiddlewareConfiguration{}
	for _, option := range options {
		if option != nil {
			option(&middlewareConfiguration)
		}
	}

	return func(nextHandler http.HandlerFunc) http.HandlerFunc {
		return func(responseWriter http.ResponseWriter, request *http.Request) {
			sourceIP := ingest.ResolveClientIP(request, trustedProxies)
			delay, allowed := limiter.AllowWithoutConsumingCapacity(sourceIP)
			if !allowed {
				retryAfterSeconds := max(1, int(math.Ceil(delay.Seconds())))
				responseWriter.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
				if middlewareConfiguration.metricsRecorder != nil {
					middlewareConfiguration.metricsRecorder.RecordRateLimitedRequest()
				}
				writeJSONError(responseWriter, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
			nextHandler(responseWriter, request)
		}
	}
}
