package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Tracks the last time a limiter was used.
type rateLimiterEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

// Holds per-IP limiters with idle cleanup.
type PerIPRateLimiterRegistry struct {
	mutex               sync.RWMutex
	entries             map[string]*rateLimiterEntry
	ratePerSecond       rate.Limit
	burst               int
	idleTimeout         time.Duration
	currentTimeProvider func() time.Time
}

// Returns a new per-IP rate limiter registry.
func NewPerIPRateLimiterRegistry(ratePerSecond int, idleTimeout time.Duration, currentTimeProvider func() time.Time) *PerIPRateLimiterRegistry {
	if currentTimeProvider == nil {
		currentTimeProvider = time.Now
	}
	return &PerIPRateLimiterRegistry{
		entries:             make(map[string]*rateLimiterEntry),
		ratePerSecond:       rate.Limit(ratePerSecond),
		burst:               ratePerSecond,
		idleTimeout:         idleTimeout,
		currentTimeProvider: currentTimeProvider,
	}
}

// Returns the wait duration for allowing a request without consuming capacity on rejection.
func (perIPRateLimiterRegistry *PerIPRateLimiterRegistry) AllowWithoutConsumingCapacity(sourceIP string) (time.Duration, bool) {
	perIPRateLimiterRegistry.mutex.Lock()
	defer perIPRateLimiterRegistry.mutex.Unlock()

	entry, exists := perIPRateLimiterRegistry.entries[sourceIP]
	if !exists {
		limiter := rate.NewLimiter(perIPRateLimiterRegistry.ratePerSecond, perIPRateLimiterRegistry.burst)
		entry = &rateLimiterEntry{limiter: limiter}
		perIPRateLimiterRegistry.entries[sourceIP] = entry
	}

	reservation := entry.limiter.Reserve()
	delay := reservation.Delay()
	if delay > 0 {
		reservation.Cancel()
		entry.lastUsed = perIPRateLimiterRegistry.currentTimeProvider()
		return delay, false
	}
	entry.lastUsed = perIPRateLimiterRegistry.currentTimeProvider()
	return 0, true
}

// Removes limiters idle long enough to have refilled.
func (perIPRateLimiterRegistry *PerIPRateLimiterRegistry) sweepIdleLimiters(currentTime time.Time) {
	perIPRateLimiterRegistry.mutex.Lock()
	defer perIPRateLimiterRegistry.mutex.Unlock()

	for sourceIP, entry := range perIPRateLimiterRegistry.entries {
		if currentTime.Sub(entry.lastUsed) >= perIPRateLimiterRegistry.idleTimeout {
			delete(perIPRateLimiterRegistry.entries, sourceIP)
		}
	}
}

// Starts the idle cleanup goroutine stopped by the context.
func (perIPRateLimiterRegistry *PerIPRateLimiterRegistry) StartIdleCleanup(applicationContext context.Context, cleanupInterval time.Duration) {
	ticker := time.NewTicker(cleanupInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				perIPRateLimiterRegistry.sweepIdleLimiters(perIPRateLimiterRegistry.currentTimeProvider())
			case <-applicationContext.Done():
				return
			}
		}
	}()
}
