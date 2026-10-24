package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPerIPRateLimiterRegistry_AllowsWithoutConsumingCapacityForRejectedRequests(t *testing.T) {
	t.Parallel()
	currentTime := time.Unix(1700000000, 0)
	timeIncrement := 0
	registry := NewPerIPRateLimiterRegistry(1, time.Hour, func() time.Time {
		timeIncrement++
		return currentTime.Add(time.Duration(timeIncrement) * time.Millisecond)
	})

	delay, allowed := registry.AllowWithoutConsumingCapacity("203.0.113.5")
	if !allowed {
		t.Fatalf("first request should be allowed")
	}
	if delay != 0 {
		t.Fatalf("first request should have zero delay")
	}

	delay, allowed = registry.AllowWithoutConsumingCapacity("203.0.113.5")
	if allowed {
		t.Fatalf("second request should be rate limited")
	}
	if delay <= 0 {
		t.Fatalf("rejected request should have a positive delay")
	}
}

func TestPerIPRateLimiterRegistry_IsolatesLimitersPerIP(t *testing.T) {
	t.Parallel()
	registry := NewPerIPRateLimiterRegistry(1, time.Hour, time.Now)

	delay, allowed := registry.AllowWithoutConsumingCapacity("203.0.113.10")
	if !allowed || delay != 0 {
		t.Fatalf("first IP first request should be allowed")
	}

	delay, allowed = registry.AllowWithoutConsumingCapacity("203.0.113.10")
	if allowed || delay <= 0 {
		t.Fatalf("first IP should be limited")
	}

	delay, allowed = registry.AllowWithoutConsumingCapacity("198.51.100.20")
	if !allowed || delay != 0 {
		t.Fatalf("second IP should have its own limiter and be allowed")
	}
}

func TestPerIPRateLimiterRegistry_SweepRemovesIdleLimiters(t *testing.T) {
	t.Parallel()
	currentTime := time.Unix(1700000000, 0)
	registry := NewPerIPRateLimiterRegistry(1000, 10*time.Second, func() time.Time {
		return currentTime
	})

	_, _ = registry.AllowWithoutConsumingCapacity("203.0.113.30")
	registry.sweepIdleLimiters(currentTime.Add(11 * time.Second))

	registry.mutex.RLock()
	_, exists := registry.entries["203.0.113.30"]
	registry.mutex.RUnlock()
	if exists {
		t.Fatalf("idle limiter should have been swept")
	}
}

func TestPerIPRateLimiterRegistry_StartIdleCleanupStopsOnContext(t *testing.T) {
	t.Parallel()
	applicationContext, applicationContextCancellation := context.WithCancel(context.Background())
	registry := NewPerIPRateLimiterRegistry(1000, time.Millisecond, time.Now)
	registry.StartIdleCleanup(applicationContext, 10*time.Millisecond)

	_, _ = registry.AllowWithoutConsumingCapacity("203.0.113.40")
	applicationContextCancellation()
	time.Sleep(50 * time.Millisecond)
}

func TestPerIPRateLimiterRegistry_ConcurrentAccessIsSafe(t *testing.T) {
	t.Parallel()
	registry := NewPerIPRateLimiterRegistry(1000, time.Hour, time.Now)
	var waitGroup sync.WaitGroup
	for range 50 {
		waitGroup.Go(func() {
			for range 100 {
				_, _ = registry.AllowWithoutConsumingCapacity("203.0.113.50")
			}
		})
	}
	waitGroup.Wait()
}
