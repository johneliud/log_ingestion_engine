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

// Returns middleware that rate limits requests by source IP.
func NewRateLimitMiddleware(limiter Limiter, writeJSONError func(http.ResponseWriter, int, string), trustedProxies []string) func(http.HandlerFunc) http.HandlerFunc {
	return func(nextHandler http.HandlerFunc) http.HandlerFunc {
		return func(responseWriter http.ResponseWriter, request *http.Request) {
			sourceIP := ingest.ResolveClientIP(request, trustedProxies)
			delay, allowed := limiter.AllowWithoutConsumingCapacity(sourceIP)
			if !allowed {
				retryAfterSeconds := max(1, int(math.Ceil(delay.Seconds())))
				responseWriter.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
				writeJSONError(responseWriter, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
			nextHandler(responseWriter, request)
		}
	}
}
