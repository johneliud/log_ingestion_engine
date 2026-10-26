package metrics

import (
	"sync"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Holds an immutable point-in-time view of every tracked metric.
type Snapshot struct {
	TotalLogsReceived   uint64            `json:"total_logs_received"`
	LogsByLevel         map[string]uint64 `json:"logs_by_level"`
	LogsByService       map[string]uint64 `json:"logs_by_service"`
	ErrorRate           float64           `json:"error_rate"`
	Throughput          float64           `json:"throughput"`
	QueueBacklog        int               `json:"queue_backlog"`
	RateLimitedRequests uint64            `json:"rate_limited_requests"`
	DeadLetteredLogs    uint64            `json:"dead_lettered_logs"`
	WriteFailures       uint64            `json:"write_failures"`
}

// Owns every metric counter and the sliding throughput window used to compute logs per second.
type Metrics struct {
	mutex                  sync.RWMutex
	totalLogsReceived      uint64
	logsByLevel            map[model.LogLevel]uint64
	logsByService          map[string]uint64
	rateLimitedRequests    uint64
	deadLetteredLogs       uint64
	writeFailures          uint64
	queueBacklog           int
	throughputWindow       time.Duration
	throughputWindowCount  int
	throughputWindowCounts []uint64
	lastThroughputSecond   int64
	hasRecordedThroughput  bool
	currentTimeProvider    func() time.Time
}

