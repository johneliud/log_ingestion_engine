package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
)

// The management API path that lists every queue on the broker.
const managementQueuesPath = "api/queues"

// Caps the management API response body so a misbehaving broker cannot exhaust memory.
const maximumManagementResponseBytes = 4 << 20

// Represents one queue as reported by the RabbitMQ management API.
type managementQueueEntry struct {
	Name     string `json:"name"`
	Messages int    `json:"messages"`
}

// Fetches pipeline queue message totals from the RabbitMQ management API over HTTP.
type queueBacklogFetcher struct {
	managementAPIURL string
	username         string
	password         string
	queueNames       []string
	requestTimeout   time.Duration
	httpClient       *http.Client
}

// Returns a fetcher for the configured management API, credentials and pipeline queues.
func newQueueBacklogFetcher(applicationConfiguration config.ApplicationConfiguration) *queueBacklogFetcher {
	return &queueBacklogFetcher{
		managementAPIURL: applicationConfiguration.RabbitMQManagementURL,
		username:         applicationConfiguration.RabbitMQUsername,
		password:         applicationConfiguration.RabbitMQPassword,
		queueNames:       []string{config.QueueService1Name, config.QueueService2Name, config.QueueService3Name},
		requestTimeout:   applicationConfiguration.MetricsBacklogTimeout,
		httpClient:       &http.Client{},
	}
}

