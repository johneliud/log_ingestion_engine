package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

func TestMetrics_SnapshotMatchesKnownInputs(t *testing.T) {
	t.Parallel()

	metricsRegistry := NewMetrics(time.Minute, nil)

	logs := []model.LogEntry{
		{Service: "claims", Level: model.LogLevelInfo},
		{Service: "claims", Level: model.LogLevelError},
		{Service: "billing", Level: model.LogLevelWarn},
	}
	metricsRegistry.RecordLogsReceived(logs)
	metricsRegistry.RecordRateLimitedRequest()
	metricsRegistry.RecordRateLimitedRequest()
	metricsRegistry.RecordDeadLetteredLog()
	metricsRegistry.RecordWriteFailure()
	metricsRegistry.SetQueueBacklog(42)

	snapshot := metricsRegistry.Snapshot()

	if snapshot.TotalLogsReceived != 3 {
		t.Errorf("TotalLogsReceived = %d, want 3", snapshot.TotalLogsReceived)
	}
	if snapshot.LogsByLevel[string(model.LogLevelInfo)] != 1 {
		t.Errorf("LogsByLevel[INFO] = %d, want 1", snapshot.LogsByLevel[string(model.LogLevelInfo)])
	}
	if snapshot.LogsByLevel[string(model.LogLevelError)] != 1 {
		t.Errorf("LogsByLevel[ERROR] = %d, want 1", snapshot.LogsByLevel[string(model.LogLevelError)])
	}
	if snapshot.LogsByLevel[string(model.LogLevelWarn)] != 1 {
		t.Errorf("LogsByLevel[WARN] = %d, want 1", snapshot.LogsByLevel[string(model.LogLevelWarn)])
	}
	if snapshot.LogsByService["claims"] != 2 {
		t.Errorf("LogsByService[claims] = %d, want 2", snapshot.LogsByService["claims"])
	}
	if snapshot.LogsByService["billing"] != 1 {
		t.Errorf("LogsByService[billing] = %d, want 1", snapshot.LogsByService["billing"])
	}
	if snapshot.ErrorRate != 1.0/3.0 {
		t.Errorf("ErrorRate = %f, want %f", snapshot.ErrorRate, 1.0/3.0)
	}
	if snapshot.QueueBacklog != 42 {
		t.Errorf("QueueBacklog = %d, want 42", snapshot.QueueBacklog)
	}
	if snapshot.RateLimitedRequests != 2 {
		t.Errorf("RateLimitedRequests = %d, want 2", snapshot.RateLimitedRequests)
	}
	if snapshot.DeadLetteredLogs != 1 {
		t.Errorf("DeadLetteredLogs = %d, want 1", snapshot.DeadLetteredLogs)
	}
	if snapshot.WriteFailures != 1 {
		t.Errorf("WriteFailures = %d, want 1", snapshot.WriteFailures)
	}
}

func TestMetrics_ErrorRateIsZeroWhenNoLogsReceived(t *testing.T) {
	t.Parallel()

	metricsRegistry := NewMetrics(time.Minute, nil)
	snapshot := metricsRegistry.Snapshot()

	if snapshot.ErrorRate != 0 {
		t.Errorf("ErrorRate = %f, want 0 when no logs have been received", snapshot.ErrorRate)
	}
}

func TestMetrics_ThroughputReflectsLogsWithinWindow(t *testing.T) {
	t.Parallel()

	currentTime := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	metricsRegistry := NewMetrics(5*time.Second, func() time.Time { return currentTime })

	for range 10 {
		metricsRegistry.RecordLogsReceived([]model.LogEntry{
			{Service: "claims", Level: model.LogLevelInfo},
		})
	}

	snapshot := metricsRegistry.Snapshot()
	expectedThroughput := 10.0 / 5.0
	if snapshot.Throughput != expectedThroughput {
		t.Errorf("Throughput = %f, want %f", snapshot.Throughput, expectedThroughput)
	}
}

func TestMetrics_ThroughputExpiresLogsOutsideWindow(t *testing.T) {
	t.Parallel()

	currentTime := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	metricsRegistry := NewMetrics(2*time.Second, func() time.Time { return currentTime })

	metricsRegistry.RecordLogsReceived([]model.LogEntry{
		{Service: "claims", Level: model.LogLevelInfo},
	})

	currentTime = currentTime.Add(3 * time.Second)
	metricsRegistry.RecordLogsReceived([]model.LogEntry{
		{Service: "claims", Level: model.LogLevelInfo},
	})

	snapshot := metricsRegistry.Snapshot()
	expectedThroughput := 1.0 / 2.0
	if snapshot.Throughput != expectedThroughput {
		t.Errorf("Throughput = %f, want %f after window expiry", snapshot.Throughput, expectedThroughput)
	}
	if snapshot.TotalLogsReceived != 2 {
		t.Errorf("TotalLogsReceived = %d, want 2; total is cumulative, not windowed", snapshot.TotalLogsReceived)
	}
}

func TestMetrics_UpdatesAreSafeUnderConcurrency(t *testing.T) {
	t.Parallel()

	metricsRegistry := NewMetrics(time.Minute, nil)
	const goroutineCount = 8
	const logsPerGoroutine = 100

	var waitGroup sync.WaitGroup

	for range goroutineCount {
		waitGroup.Go(func() {
			for range logsPerGoroutine {
				metricsRegistry.RecordLogsReceived([]model.LogEntry{
					{Service: "claims", Level: model.LogLevelInfo},
				})
			}
		})
		waitGroup.Go(func() {
			for range logsPerGoroutine {
				metricsRegistry.RecordRateLimitedRequest()
			}
		})
		waitGroup.Go(func() {
			for range logsPerGoroutine {
				metricsRegistry.RecordDeadLetteredLog()
			}
		})
		waitGroup.Go(func() {
			for range logsPerGoroutine {
				metricsRegistry.RecordWriteFailure()
			}
		})
	}

	waitGroup.Wait()

	snapshot := metricsRegistry.Snapshot()
	expectedCount := uint64(goroutineCount * logsPerGoroutine)
	if snapshot.TotalLogsReceived != expectedCount {
		t.Errorf("TotalLogsReceived = %d, want %d", snapshot.TotalLogsReceived, expectedCount)
	}
	if snapshot.RateLimitedRequests != expectedCount {
		t.Errorf("RateLimitedRequests = %d, want %d", snapshot.RateLimitedRequests, expectedCount)
	}
	if snapshot.DeadLetteredLogs != expectedCount {
		t.Errorf("DeadLetteredLogs = %d, want %d", snapshot.DeadLetteredLogs, expectedCount)
	}
	if snapshot.WriteFailures != expectedCount {
		t.Errorf("WriteFailures = %d, want %d", snapshot.WriteFailures, expectedCount)
	}
}

func TestMetrics_SnapshotDoesNotShareMapsWithRegistry(t *testing.T) {
	t.Parallel()

	metricsRegistry := NewMetrics(time.Minute, nil)
	metricsRegistry.RecordLogsReceived([]model.LogEntry{
		{Service: "claims", Level: model.LogLevelInfo},
	})

	snapshot := metricsRegistry.Snapshot()
	snapshot.LogsByLevel[string(model.LogLevelInfo)] = 999
	snapshot.LogsByService["claims"] = 999

	freshSnapshot := metricsRegistry.Snapshot()
	if freshSnapshot.LogsByLevel[string(model.LogLevelInfo)] != 1 {
		t.Errorf("LogsByLevel[INFO] = %d after mutating a previous snapshot, want 1", freshSnapshot.LogsByLevel[string(model.LogLevelInfo)])
	}
	if freshSnapshot.LogsByService["claims"] != 1 {
		t.Errorf("LogsByService[claims] = %d after mutating a previous snapshot, want 1", freshSnapshot.LogsByService["claims"])
	}
}

func TestMetrics_ZeroOrNegativeWindowFallsBackToOneMinute(t *testing.T) {
	t.Parallel()

	metricsRegistry := NewMetrics(0, nil)
	if metricsRegistry.throughputWindow != time.Minute {
		t.Errorf("throughputWindow = %s, want %s for a zero window", metricsRegistry.throughputWindow, time.Minute)
	}

	zeroWindowMetrics := NewMetrics(-time.Second, nil)
	if zeroWindowMetrics.throughputWindow != time.Minute {
		t.Errorf("throughputWindow = %s, want %s for a negative window", zeroWindowMetrics.throughputWindow, time.Minute)
	}
}

func TestMetrics_DefaultCurrentTimeProviderIsUsed(t *testing.T) {
	t.Parallel()

	metricsRegistry := NewMetrics(time.Minute, nil)
	metricsRegistry.RecordLogsReceived([]model.LogEntry{
		{Service: "claims", Level: model.LogLevelInfo},
	})

	snapshot := metricsRegistry.Snapshot()
	if snapshot.TotalLogsReceived != 1 {
		t.Errorf("TotalLogsReceived = %d, want 1", snapshot.TotalLogsReceived)
	}
}

func TestMetrics_FallbackQueueBacklogCounterTracksPublishedMinusStored(t *testing.T) {
	t.Parallel()

	metricsRegistry := NewMetrics(time.Minute, nil)
	metricsRegistry.RecordQueueMessageStored()
	metricsRegistry.RecordQueueMessageStored()
	if fallbackBacklog := metricsRegistry.FallbackQueueBacklog(); fallbackBacklog != 0 {
		t.Errorf("fallback backlog = %d, want 0; the counter must not fall below zero", fallbackBacklog)
	}

	for range 4 {
		metricsRegistry.RecordQueueMessagePublished()
	}
	metricsRegistry.RecordQueueMessageStored()
	if fallbackBacklog := metricsRegistry.FallbackQueueBacklog(); fallbackBacklog != 3 {
		t.Errorf("fallback backlog = %d, want 3 (four published minus one stored)", fallbackBacklog)
	}
}

func TestMetrics_FallbackQueueBacklogCounterDropsForReleasedMessages(t *testing.T) {
	t.Parallel()

	metricsRegistry := NewMetrics(time.Minute, nil)
	metricsRegistry.RecordQueueMessagePublished()
	metricsRegistry.RecordQueueMessageReleased()
	if fallbackBacklog := metricsRegistry.FallbackQueueBacklog(); fallbackBacklog != 0 {
		t.Errorf("fallback backlog = %d, want 0 (the published message left the pipeline)", fallbackBacklog)
	}

	metricsRegistry.RecordQueueMessageReleased()
	if fallbackBacklog := metricsRegistry.FallbackQueueBacklog(); fallbackBacklog != 0 {
		t.Errorf("fallback backlog = %d, want 0; the counter must not fall below zero", fallbackBacklog)
	}
}

// Returns a metrics registry whose broadcast and history sampling loop is not running, so tests can drive the sampling deterministically.
func newMetricsRegistryWithoutBroadcastLoop(currentTimeProvider func() time.Time) *Metrics {
	return &Metrics{
		logsByLevel:         make(map[model.LogLevel]uint64),
		logsByService:       make(map[string]uint64),
		throughputWindow:    time.Minute,
		currentTimeProvider: currentTimeProvider,
		subscribers:         make(map[chan Snapshot]struct{}),
		changeNotifier:      make(chan struct{}, 1),
		broadcastDone:       make(chan struct{}),
	}
}

func TestMetrics_LogsPerSecondMatchesThroughput(t *testing.T) {
	t.Parallel()

	currentTime := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	metricsRegistry := newMetricsRegistryWithoutBroadcastLoop(func() time.Time { return currentTime })
	metricsRegistry.RecordLogsReceived([]model.LogEntry{
		{Service: "claims", Level: model.LogLevelInfo},
		{Service: "claims", Level: model.LogLevelInfo},
		{Service: "claims", Level: model.LogLevelInfo},
		{Service: "claims", Level: model.LogLevelInfo},
		{Service: "claims", Level: model.LogLevelInfo},
	})

	snapshot := metricsRegistry.Snapshot()
	expectedLogsPerSecond := 5.0 / 60.0
	if snapshot.LogsPerSecond != expectedLogsPerSecond {
		t.Errorf("LogsPerSecond = %f, want %f", snapshot.LogsPerSecond, expectedLogsPerSecond)
	}
	if snapshot.LogsPerSecond != snapshot.Throughput {
		t.Errorf("LogsPerSecond = %f, want the throughput value %f", snapshot.LogsPerSecond, snapshot.Throughput)
	}
}

func TestMetrics_HistorySamplingRecordsThroughputAndErrorRateOverTime(t *testing.T) {
	t.Parallel()

	currentTime := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	metricsRegistry := newMetricsRegistryWithoutBroadcastLoop(func() time.Time { return currentTime })
	metricsRegistry.RecordLogsReceived([]model.LogEntry{
		{Service: "claims", Level: model.LogLevelInfo},
		{Service: "claims", Level: model.LogLevelError},
	})

	metricsRegistry.sampleHistoryIfDue()
	currentTime = currentTime.Add(historySampleInterval)
	metricsRegistry.RecordLogsReceived([]model.LogEntry{
		{Service: "claims", Level: model.LogLevelInfo},
	})
	metricsRegistry.sampleHistoryIfDue()

	snapshot := metricsRegistry.Snapshot()
	if len(snapshot.ThroughputHistory) != 2 {
		t.Fatalf("throughput history point count = %d, want 2", len(snapshot.ThroughputHistory))
	}
	if len(snapshot.ErrorRateHistory) != 2 {
		t.Fatalf("error rate history point count = %d, want 2", len(snapshot.ErrorRateHistory))
	}
	if snapshot.ThroughputHistory[0].Time != "2026-10-26T12:00:00Z" {
		t.Errorf("first throughput sample time = %q, want %q", snapshot.ThroughputHistory[0].Time, "2026-10-26T12:00:00Z")
	}
	if snapshot.ThroughputHistory[0].Value != 2.0/60.0 {
		t.Errorf("first throughput sample = %f, want %f", snapshot.ThroughputHistory[0].Value, 2.0/60.0)
	}
	if snapshot.ErrorRateHistory[0].Value != 0.5 {
		t.Errorf("first error rate sample = %f, want 0.5", snapshot.ErrorRateHistory[0].Value)
	}
	if snapshot.ErrorRateHistory[1].Value != 1.0/3.0 {
		t.Errorf("second error rate sample = %f, want %f", snapshot.ErrorRateHistory[1].Value, 1.0/3.0)
	}
}

func TestMetrics_HistorySamplingSkipsSamplesInsideTheIntervalAndKeepsOnlyRecentPoints(t *testing.T) {
	t.Parallel()

	currentTime := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	metricsRegistry := newMetricsRegistryWithoutBroadcastLoop(func() time.Time { return currentTime })

	metricsRegistry.sampleHistoryIfDue()
	metricsRegistry.sampleHistoryIfDue()
	if historyPointCount := len(metricsRegistry.Snapshot().ThroughputHistory); historyPointCount != 1 {
		t.Errorf("throughput history point count = %d, want 1; a sample inside the interval must be skipped", historyPointCount)
	}

	for range maximumHistoryPoints + 5 {
		currentTime = currentTime.Add(historySampleInterval)
		metricsRegistry.sampleHistoryIfDue()
	}
	if historyPointCount := len(metricsRegistry.Snapshot().ThroughputHistory); historyPointCount != maximumHistoryPoints {
		t.Errorf("throughput history point count = %d, want at most %d", historyPointCount, maximumHistoryPoints)
	}
}

func TestMetrics_SubscribeAndUnsubscribeAreSafeWhileSnapshotsBroadcast(t *testing.T) {
	t.Parallel()

	metricsRegistry := NewMetrics(time.Minute, nil)
	const iterationCount = 200

	var recorders sync.WaitGroup
	recorders.Add(1)
	go func() {
		defer recorders.Done()
		for range iterationCount {
			metricsRegistry.RecordLogsReceived([]model.LogEntry{
				{Service: "claims", Level: model.LogLevelInfo},
			})
		}
	}()

	var subscribers sync.WaitGroup
	for range 4 {
		subscribers.Add(1)
		go func() {
			defer subscribers.Done()
			for range iterationCount {
				channel := metricsRegistry.Subscribe()
				select {
				case <-channel:
				default:
				}
				metricsRegistry.Unsubscribe(channel)
			}
		}()
	}

	subscribers.Wait()
	recorders.Wait()

	// The subscriber map must end up empty and the registry must still serve snapshots without panicking.
	if remainingSubscribers := len(metricsRegistry.subscribers); remainingSubscribers != 0 {
		t.Errorf("remaining subscriber count = %d, want 0", remainingSubscribers)
	}
	if snapshot := metricsRegistry.Snapshot(); snapshot.TotalLogsReceived != iterationCount {
		t.Errorf("TotalLogsReceived = %d, want %d", snapshot.TotalLogsReceived, iterationCount)
	}
}

func TestMetricsBroadcastReplacesBufferedSnapshot(t *testing.T) {
	registry := newMetricsRegistryWithoutBroadcastLoop(time.Now)
	channel := registry.Subscribe()
	defer registry.Unsubscribe(channel)
	registry.RecordLogsReceived([]model.LogEntry{{Service: "one", Level: model.LogLevelInfo}})
	registry.broadcastIfDirty()
	registry.RecordLogsReceived([]model.LogEntry{{Service: "two", Level: model.LogLevelInfo}})
	registry.broadcastIfDirty()
	select {
	case snapshot := <-channel:
		if snapshot.TotalLogsReceived != 2 {
			t.Fatalf("buffered total = %d, want latest total 2", snapshot.TotalLogsReceived)
		}
	default:
		t.Fatal("missing snapshot")
	}
}
