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
	Vhost    string `json:"vhost"`
	Messages int    `json:"messages"`
}

// Fetches pipeline queue message totals from the RabbitMQ management API over HTTP.
type queueBacklogFetcher struct {
	managementAPIURL string
	username         string
	password         string
	virtualHost      string
	queueNames       []string
	requestTimeout   time.Duration
	httpClient       *http.Client
}

// Returns a fetcher for the configured management API, credentials and pipeline queues.
func newQueueBacklogFetcher(applicationConfiguration config.ApplicationConfiguration) *queueBacklogFetcher {
	virtualHost := applicationConfiguration.RabbitMQVirtualHost
	if virtualHost == "" {
		virtualHost = config.DefaultRabbitMQVirtualHost
	}
	return &queueBacklogFetcher{
		managementAPIURL: applicationConfiguration.RabbitMQManagementURL,
		username:         applicationConfiguration.RabbitMQUsername,
		password:         applicationConfiguration.RabbitMQPassword,
		virtualHost:      virtualHost,
		queueNames:       []string{config.QueueService1Name, config.QueueService2Name, config.QueueService3Name},
		requestTimeout:   applicationConfiguration.MetricsBacklogTimeout,
		httpClient:       &http.Client{},
	}
}

// Returns the backlog from the management API, or the supplied fallback when it cannot be read.
func (queueBacklogFetcher *queueBacklogFetcher) readQueueBacklog(applicationContext context.Context, fallbackBacklog int) int {
	if queueBacklogFetcher.managementAPIURL == "" {
		return fallbackBacklog
	}

	backlog, fetchError := queueBacklogFetcher.fetchQueueBacklog(applicationContext)
	if fetchError != nil {
		slog.Warn("reading queue backlog from the management api failed, using the fallback counter", "error", fetchError)
		return fallbackBacklog
	}
	return backlog
}

// Requests the queue list from the management API and sums the messages across the pipeline queues.
func (queueBacklogFetcher *queueBacklogFetcher) fetchQueueBacklog(applicationContext context.Context) (int, error) {
	queuesURL, urlError := url.JoinPath(queueBacklogFetcher.managementAPIURL, managementQueuesPath)
	if urlError != nil {
		return 0, fmt.Errorf("building the management api queues url: %w", urlError)
	}

	requestContext, requestContextCancellation := context.WithTimeout(applicationContext, queueBacklogFetcher.requestTimeout)
	defer requestContextCancellation()

	managementRequest, requestError := http.NewRequestWithContext(requestContext, http.MethodGet, queuesURL, nil)
	if requestError != nil {
		return 0, fmt.Errorf("creating the management api request: %w", requestError)
	}
	managementRequest.SetBasicAuth(queueBacklogFetcher.username, queueBacklogFetcher.password)

	managementResponse, responseError := queueBacklogFetcher.httpClient.Do(managementRequest)
	if responseError != nil {
		return 0, fmt.Errorf("requesting the queue list: %w", responseError)
	}
	defer managementResponse.Body.Close()

	if managementResponse.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("management api returned status %q", managementResponse.Status)
	}

	var managementQueues []managementQueueEntry
	decodeError := json.NewDecoder(io.LimitReader(managementResponse.Body, maximumManagementResponseBytes)).Decode(&managementQueues)
	if decodeError != nil {
		return 0, fmt.Errorf("decoding the management api queue list: %w", decodeError)
	}
	return queueBacklogFetcher.sumPipelineQueueMessages(managementQueues), nil
}

// Returns the total messages across the pipeline queues of the configured virtual host, ignoring every other queue.
func (queueBacklogFetcher *queueBacklogFetcher) sumPipelineQueueMessages(managementQueues []managementQueueEntry) int {
	totalPipelineMessages := 0
	for _, managementQueue := range managementQueues {
		if managementQueue.Vhost != queueBacklogFetcher.virtualHost {
			continue
		}
		if slices.Contains(queueBacklogFetcher.queueNames, managementQueue.Name) {
			totalPipelineMessages += managementQueue.Messages
		}
	}
	return totalPipelineMessages
}
