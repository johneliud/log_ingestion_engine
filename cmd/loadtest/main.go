package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	loadTestServiceName          = "load-test"
	loadTestLogLevel             = "INFO"
	loadTestSourceAddressPrefix  = "198.51.100."
	requestResultsSampleCapacity = 4096
)

// Holds the command line knobs for one load test run.
type loadTestSettings struct {
	targetURL            string
	targetDuration       time.Duration
	targetLogsPerSecond  int
	logsPerRequest       int
	sourceIPCount        int
	senderCount          int
	clientTimeout        time.Duration
	keepAliveConnections int
}

// Aggregates what one run produced across all senders.
type loadTestRecord struct {
	sentRequestCount   atomic.Int64
	acceptedLogCount   atomic.Int64
	rateLimitedCount   atomic.Int64
	otherFailureCount  atomic.Int64
	lateSendCount      atomic.Int64
	totalLatency       atomic.Int64
	requestCount       atomic.Int64
	latencySampleMutex sync.Mutex
	latencySamples     []time.Duration
	failureStatusMutex sync.Mutex
	failureStatuses    map[int]int
	failureResponses   map[int]string
}

// Reads the command line flags into one validated settings value.
func loadTestSettingsFromFlags() (loadTestSettings, error) {
	var settings loadTestSettings
	flag.StringVar(&settings.targetURL, "target", "http://localhost:8080", "base URL of the log ingestion engine")
	flag.DurationVar(&settings.targetDuration, "duration", 30*time.Second, "how long to send logs")
	flag.IntVar(&settings.targetLogsPerSecond, "rate", 1000, "logs per second to attempt")
	flag.IntVar(&settings.logsPerRequest, "batch", 100, "log entries per HTTP request")
	flag.IntVar(&settings.sourceIPCount, "source-ips", 50, "number of spoofed source IPs to round-robin")
	flag.IntVar(&settings.senderCount, "senders", 8, "concurrent sender goroutines")
	flag.DurationVar(&settings.clientTimeout, "client-timeout", 10*time.Second, "per-request HTTP timeout")
	flag.Parse()

	if settings.targetLogsPerSecond <= 0 || settings.logsPerRequest <= 0 || settings.sourceIPCount <= 0 || settings.senderCount <= 0 {
		return settings, fmt.Errorf("rate, batch, source-ips and senders must all be positive")
	}
	requestInterval := time.Duration(float64(time.Second) * float64(settings.logsPerRequest) / float64(settings.targetLogsPerSecond))
	if requestInterval < minimumRequestInterval {
		return settings, fmt.Errorf("batch %d wants a %s interval, which is below the %s pacing precision; raise the batch size or lower the rate",
			settings.logsPerRequest, requestInterval, minimumRequestInterval)
	}
	if settings.keepAliveConnections == 0 {
		settings.keepAliveConnections = settings.senderCount
	}
	return settings, nil
}

// Requests are paced no tighter than this so the scheduler can keep up.
const minimumRequestInterval = 500 * time.Microsecond

// Returns the source addresses one run rotates through.
func buildSourceIPAddresses(sourceIPCount int) []string {
	sourceIPAddresses := make([]string, 0, sourceIPCount)
	for sourceIPIndex := range sourceIPCount {
		sourceIPAddresses = append(sourceIPAddresses, loadTestSourceAddressPrefix+fmt.Sprintf("%03d", sourceIPIndex+1))
	}
	return sourceIPAddresses
}

// Returns one request body holding the requested number of valid log entries.
func buildLogBatchRequestBody(logsPerRequest int, requestNumber int64, sentAt time.Time) []byte {
	logEntryStrings := make([]string, 0, logsPerRequest)
	for logEntryIndex := range logsPerRequest {
		logEntry := map[string]string{
			"timestamp": sentAt.Format(time.RFC3339),
			"service":   loadTestServiceName,
			"level":     loadTestLogLevel,
			"message":   fmt.Sprintf("load test log %d entry %d", requestNumber, logEntryIndex),
		}
		serializedEntry, marshalError := json.Marshal(logEntry)
		if marshalError != nil {
			slog.Error("marshalling log entry failed", "error", marshalError)
			os.Exit(1)
		}
		logEntryStrings = append(logEntryStrings, string(serializedEntry))
	}
	return []byte("[" + strings.Join(logEntryStrings, ",") + "]")
}

// Runs the paced senders and returns the aggregated record of the run.
func runLoadTest(settings loadTestSettings) *loadTestRecord {
	loadTestRecord := &loadTestRecord{latencySamples: make([]time.Duration, 0, requestResultsSampleCapacity), failureStatuses: map[int]int{}, failureResponses: map[int]string{}}
	sourceIPAddresses := buildSourceIPAddresses(settings.sourceIPCount)

	httpClient := &http.Client{
		Timeout: settings.clientTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        settings.keepAliveConnections,
			MaxIdleConnsPerHost: settings.keepAliveConnections,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	runStartedAt := time.Now()
	runFinishedAt := runStartedAt.Add(settings.targetDuration)
	requestInterval := time.Duration(float64(time.Second) * float64(settings.logsPerRequest) / float64(settings.targetLogsPerSecond))

	senderWaitGroup := sync.WaitGroup{}
	for senderIndex := range settings.senderCount {
		senderWaitGroup.Add(1)
		go func(senderNumber int) {
			defer senderWaitGroup.Done()
			for {
				requestNumber := loadTestRecord.sentRequestCount.Add(1)
				targetSendAt := runStartedAt.Add(time.Duration(requestNumber) * requestInterval)
				if targetSendAt.After(runFinishedAt) {
					loadTestRecord.sentRequestCount.Add(-1)
					return
				}
				if sleepDuration := time.Until(targetSendAt); sleepDuration > 0 {
					time.Sleep(sleepDuration)
				}
				if lateBy := time.Since(targetSendAt); lateBy > time.Millisecond {
					loadTestRecord.lateSendCount.Add(1)
				}
				sendLoadTestRequest(settings, httpClient, sourceIPAddresses, requestNumber, loadTestRecord)
			}
		}(senderIndex)
	}
	senderWaitGroup.Wait()
	return loadTestRecord
}

// Sends one batched request and updates the record with the response outcome.
func sendLoadTestRequest(settings loadTestSettings, httpClient *http.Client, sourceIPAddresses []string, requestNumber int64, loadTestRecord *loadTestRecord) {
	sentAt := time.Now()
	requestBody := buildLogBatchRequestBody(settings.logsPerRequest, requestNumber, sentAt)
	request, requestError := http.NewRequest(http.MethodPost, settings.targetURL+"/logs", bytes.NewReader(requestBody))
	if requestError != nil {
		loadTestRecord.otherFailureCount.Add(1)
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Forwarded-For", sourceIPAddresses[int(requestNumber)%len(sourceIPAddresses)])

	sendStartedAt := time.Now()
	response, sendError := httpClient.Do(request)
	latency := time.Since(sendStartedAt)
	if sendError != nil {
		loadTestRecord.otherFailureCount.Add(1)
		return
	}
	failureResponseBody, readError := io.ReadAll(io.LimitReader(response.Body, 512))
	if readError != nil {
		failureResponseBody = nil
	}
	response.Body.Close()

	loadTestRecord.failureStatusMutex.Lock()
	loadTestRecord.failureStatuses[response.StatusCode]++
	if _, alreadyRecorded := loadTestRecord.failureResponses[response.StatusCode]; !alreadyRecorded {
		loadTestRecord.failureResponses[response.StatusCode] = strings.TrimSpace(string(failureResponseBody))
	}
	loadTestRecord.failureStatusMutex.Unlock()

	switch response.StatusCode {
	case http.StatusAccepted:
		loadTestRecord.acceptedLogCount.Add(int64(settings.logsPerRequest))
	case http.StatusTooManyRequests:
		loadTestRecord.rateLimitedCount.Add(1)
	default:
		loadTestRecord.otherFailureCount.Add(1)
	}
	loadTestRecord.requestCount.Add(1)
	loadTestRecord.totalLatency.Add(int64(latency))
	loadTestRecord.latencySampleMutex.Lock()
	if len(loadTestRecord.latencySamples) < requestResultsSampleCapacity {
		loadTestRecord.latencySamples = append(loadTestRecord.latencySamples, latency)
	}
	loadTestRecord.latencySampleMutex.Unlock()
}

// Prints the summary numbers of one run.
func printLoadTestSummary(settings loadTestSettings, loadTestRecord *loadTestRecord) {
	requestCount := loadTestRecord.requestCount.Load()
	meanLatencyNanoseconds := int64(0)
	if requestCount > 0 {
		meanLatencyNanoseconds = loadTestRecord.totalLatency.Load() / requestCount
	}

	latencySamples := append([]time.Duration(nil), loadTestRecord.latencySamples...)
	slices.Sort(latencySamples)
	percentile := func(percent float64) string {
		if len(latencySamples) == 0 {
			return "n/a"
		}
		sampleIndex := int(float64(len(latencySamples)-1) * percent)
		return latencySamples[sampleIndex].Truncate(time.Microsecond).String()
	}

	fmt.Println("--- load test summary ---")
	fmt.Printf("target URL: %s\n", settings.targetURL)
	fmt.Printf("target: %d logs/s, batch %d, %d source IPs, %d senders, %s\n", settings.targetLogsPerSecond, settings.logsPerRequest, settings.sourceIPCount, settings.senderCount, settings.targetDuration)
	fmt.Printf("requests sent: %d, accepted: %d logs (%.1f logs/s), rate-limited responses: %d, other failures: %d, late sends: %d\n",
		loadTestRecord.sentRequestCount.Load(), loadTestRecord.acceptedLogCount.Load(),
		float64(loadTestRecord.acceptedLogCount.Load())/settings.targetDuration.Seconds(),
		loadTestRecord.rateLimitedCount.Load(), loadTestRecord.otherFailureCount.Load(), loadTestRecord.lateSendCount.Load())
	fmt.Printf("latency mean %s, p50 %s, p95 %s, p99 %s\n",
		(time.Duration(meanLatencyNanoseconds)).Truncate(time.Microsecond).String(), percentile(0.50), percentile(0.95), percentile(0.99))
	loadTestRecord.failureStatusMutex.Lock()
	defer loadTestRecord.failureStatusMutex.Unlock()
	for statusCode, statusCount := range loadTestRecord.failureStatuses {
		responseBody := loadTestRecord.failureResponses[statusCode]
		fmt.Printf("status %d seen %d times, body example: %s\n", statusCode, statusCount, responseBody)
	}
}

func main() {
	settings, settingsError := loadTestSettingsFromFlags()
	if settingsError != nil {
		fmt.Fprintln(os.Stderr, settingsError)
		os.Exit(1)
	}
	fmt.Printf("sending %d logs/s in batches of %d to %s for %s\n", settings.targetLogsPerSecond, settings.logsPerRequest, settings.targetURL, settings.targetDuration)
	loadTestRecord := runLoadTest(settings)
	printLoadTestSummary(settings, loadTestRecord)
}
