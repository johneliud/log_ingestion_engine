package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	"github.com/johneliud/log_ingestion_engine/internal/queue"
	"github.com/johneliud/log_ingestion_engine/internal/spool"
)

const (
	integrationTestRepositoryRoot  = "../.."
	integrationTestWaitTimeout     = 20 * time.Second
	integrationTestPollInterval    = 50 * time.Millisecond
	integrationTestUnreachablePort = 1
)

// Represents one row of the logs table of a destination database.
type storedLogRow struct {
	Timestamp   string
	Service     string
	Level       string
	Message     string
	ReceivedAt  string
	SourceIP    string
	Environment string
}

// Returns the configuration for an integration test that talks to the configured broker.
func integrationTestConfiguration(t *testing.T) config.ApplicationConfiguration {
	t.Helper()

	repositoryConfigurationFilePath := filepath.Join(integrationTestRepositoryRoot, ".env")
	if os.Getenv("CONFIG_FILE_PATH") == "" {
		if _, statError := os.Stat(repositoryConfigurationFilePath); statError == nil {
			t.Setenv("CONFIG_FILE_PATH", repositoryConfigurationFilePath)
		}
	}

	brokerConfiguration, configurationError := config.NewApplicationConfiguration()
	if configurationError != nil {
		t.Skipf("skipping the integration test because the broker configuration is missing: %v", configurationError)
	}

	applicationConfiguration := integrationTestBaseConfiguration(t)
	applicationConfiguration.RabbitMQHost = brokerConfiguration.RabbitMQHost
	applicationConfiguration.RabbitMQPort = brokerConfiguration.RabbitMQPort
	applicationConfiguration.RabbitMQUsername = brokerConfiguration.RabbitMQUsername
	applicationConfiguration.RabbitMQPassword = brokerConfiguration.RabbitMQPassword
	applicationConfiguration.RabbitMQVirtualHost = brokerConfiguration.RabbitMQVirtualHost
	return applicationConfiguration
}

// Returns the configuration for an integration test that runs without a reachable broker.
func integrationTestConfigurationWithoutBroker(t *testing.T) config.ApplicationConfiguration {
	t.Helper()

	applicationConfiguration := integrationTestBaseConfiguration(t)
	applicationConfiguration.RabbitMQHost = "127.0.0.1"
	applicationConfiguration.RabbitMQPort = integrationTestUnreachablePort
	applicationConfiguration.RabbitMQUsername = "integration-test"
	applicationConfiguration.RabbitMQPassword = "integration-test"
	return applicationConfiguration
}

// Returns the configuration every integration test starts from, with temporary storage paths.
func integrationTestBaseConfiguration(t *testing.T) config.ApplicationConfiguration {
	t.Helper()

	applicationConfiguration := testApplicationConfiguration(t)
	applicationConfiguration.ApplicationEnvironment = "integration-test"
	applicationConfiguration.RawLogsBufferSize = 1000
	applicationConfiguration.WriteBatchSize = 100
	applicationConfiguration.WriteFlushInterval = 200 * time.Millisecond
	applicationConfiguration.WriteRetryBackoffDelays = []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}
	applicationConfiguration.DeadLetterMaximumFileSizeInBytes = 1 << 20
	applicationConfiguration.PublishTimeout = 2 * time.Second
	applicationConfiguration.RabbitMQReconnectBackoffDelays = []time.Duration{100 * time.Millisecond}
	return applicationConfiguration
}

// Names the environment variable that turns skipped broker tests into failures in credentialed CI.
const brokerTestsRequiredEnvironmentName = "CI_REQUIRE_BROKER_TESTS"

// Returns whether credentialed CI set the flag that makes a missing broker fail instead of skip.
func brokerTestsAreRequired() bool {
	return os.Getenv(brokerTestsRequiredEnvironmentName) == "1"
}

// Fails the test when credentialed CI cannot reach the broker, and skips it everywhere else.
func requireTheBroker(t *testing.T, applicationConfiguration config.ApplicationConfiguration) {
	t.Helper()

	connection, connectionError := queue.DialBroker(applicationConfiguration)
	if connectionError != nil {
		if brokerTestsAreRequired() {
			t.Fatalf("credentialed ci requires rabbitmq, but the broker is unreachable: %v", connectionError)
		}
		t.Skipf("skipping the integration test because rabbitmq is unavailable: %v", connectionError)
	}
	if closeError := connection.Close(); closeError != nil {
		t.Fatalf("closing the broker connection failed: %v", closeError)
	}
}

// Removes the messages left in the destination queues so the test counts only the logs it sent.
func purgeTheDestinationQueues(t *testing.T, applicationConfiguration config.ApplicationConfiguration) {
	t.Helper()

	connection, connectionError := queue.DialBroker(applicationConfiguration)
	if connectionError != nil {
		t.Skipf("skipping the integration test because rabbitmq is unavailable: %v", connectionError)
	}
	t.Cleanup(func() {
		if closeError := connection.Close(); closeError != nil {
			t.Errorf("closing the broker connection failed: %v", closeError)
		}
	})

	channel, channelError := connection.Channel()
	if channelError != nil {
		t.Fatalf("opening the broker channel failed: %v", channelError)
	}
	t.Cleanup(func() {
		if closeError := channel.Close(); closeError != nil {
			t.Errorf("closing the broker channel failed: %v", closeError)
		}
	})

	if topologyError := queue.DeclareTopology(channel); topologyError != nil {
		t.Fatalf("declaring the rabbitmq topology failed: %v", topologyError)
	}
	for _, queueName := range []string{config.QueueService1Name, config.QueueService2Name, config.QueueService3Name} {
		if _, purgeError := channel.QueuePurge(queueName, false); purgeError != nil {
			t.Fatalf("purging the queue %q failed: %v", queueName, purgeError)
		}
	}
}

// Returns the rows of the logs table of one database file.
func readStoredLogRows(t *testing.T, databaseFilePath string) []storedLogRow {
	t.Helper()

	database, openError := sql.Open("sqlite", databaseFilePath)
	if openError != nil {
		t.Fatalf("opening the database failed: %v", openError)
	}
	t.Cleanup(func() {
		if closeError := database.Close(); closeError != nil {
			t.Errorf("closing the database failed: %v", closeError)
		}
	})

	rows, queryError := database.Query("SELECT timestamp, service, level, message, received_at, source_ip, env FROM logs")
	if queryError != nil {
		t.Fatalf("reading the stored logs failed: %v", queryError)
	}
	t.Cleanup(func() {
		if rowsCloseError := rows.Close(); rowsCloseError != nil {
			t.Errorf("closing the stored log rows failed: %v", rowsCloseError)
		}
	})

	storedLogRows := make([]storedLogRow, 0)
	for rows.Next() {
		var storedLog storedLogRow
		if scanError := rows.Scan(&storedLog.Timestamp, &storedLog.Service, &storedLog.Level, &storedLog.Message, &storedLog.ReceivedAt, &storedLog.SourceIP, &storedLog.Environment); scanError != nil {
			t.Fatalf("scanning the stored log failed: %v", scanError)
		}
		storedLogRows = append(storedLogRows, storedLog)
	}
	if rowsError := rows.Err(); rowsError != nil {
		t.Fatalf("reading the stored log rows failed: %v", rowsError)
	}
	return storedLogRows
}

// Returns the number of rows stored for one service in one database file.
func countStoredLogsForService(t *testing.T, databaseFilePath string, serviceName string) int {
	t.Helper()

	storedLogCount := 0
	for _, storedLogRow := range readStoredLogRows(t, databaseFilePath) {
		if storedLogRow.Service == serviceName {
			storedLogCount++
		}
	}
	return storedLogCount
}

// Waits until the database holds the expected number of rows for the service and reports how long it took.
func waitForStoredLogCount(t *testing.T, databaseFilePath string, serviceName string, expectedLogCount int) time.Duration {
	t.Helper()

	waitStartedAt := time.Now()
	pollTicker := time.NewTicker(integrationTestPollInterval)
	defer pollTicker.Stop()
	for {
		storedLogCount := countStoredLogsForService(t, databaseFilePath, serviceName)
		if storedLogCount >= expectedLogCount {
			return time.Since(waitStartedAt)
		}
		if time.Since(waitStartedAt) > integrationTestWaitTimeout {
			t.Fatalf("stored logs for %q = %d, want %d", serviceName, storedLogCount, expectedLogCount)
		}
		<-pollTicker.C
	}
}

// Returns the dead letter entries of the dead letter file and every rotated file next to it.
func readDeadLetterEntries(t *testing.T, deadLetterFilePath string) []model.DeadLetterEntry {
	t.Helper()

	deadLetterFilePaths := []string{deadLetterFilePath}
	for rotationIndex := 1; ; rotationIndex++ {
		rotatedFilePath := fmt.Sprintf("%s.%d%s", strings.TrimSuffix(deadLetterFilePath, filepath.Ext(deadLetterFilePath)), rotationIndex, filepath.Ext(deadLetterFilePath))
		if _, statError := os.Stat(rotatedFilePath); statError != nil {
			break
		}
		deadLetterFilePaths = append(deadLetterFilePaths, rotatedFilePath)
	}

	deadLetterEntries := make([]model.DeadLetterEntry, 0)
	for _, deadLetterFilePath := range deadLetterFilePaths {
		fileContents, readError := os.ReadFile(deadLetterFilePath)
		if errors.Is(readError, os.ErrNotExist) {
			continue
		}
		if readError != nil {
			t.Fatalf("reading the dead letter file failed: %v", readError)
		}
		for fileLine := range strings.SplitSeq(strings.TrimSpace(string(fileContents)), "\n") {
			if strings.TrimSpace(fileLine) == "" {
				continue
			}
			var deadLetterEntry model.DeadLetterEntry
			if unmarshalError := json.Unmarshal([]byte(fileLine), &deadLetterEntry); unmarshalError != nil {
				t.Fatalf("parsing the dead letter entry failed: %v", unmarshalError)
			}
			deadLetterEntries = append(deadLetterEntries, deadLetterEntry)
		}
	}
	return deadLetterEntries
}

// Waits until the dead letter file holds the expected number of entries.
func waitForDeadLetterEntries(t *testing.T, deadLetterFilePath string, expectedEntryCount int) []model.DeadLetterEntry {
	t.Helper()

	waitStartedAt := time.Now()
	pollTicker := time.NewTicker(integrationTestPollInterval)
	defer pollTicker.Stop()
	for {
		deadLetterEntries := readDeadLetterEntries(t, deadLetterFilePath)
		if len(deadLetterEntries) >= expectedEntryCount {
			return deadLetterEntries
		}
		if time.Since(waitStartedAt) > integrationTestWaitTimeout {
			t.Fatalf("dead letter entries = %d, want %d", len(deadLetterEntries), expectedEntryCount)
		}
		<-pollTicker.C
	}
}

func TestIntegration_FullPipelineStoresTheEnrichedAndRoutedLogsInTheDestinationDatabases(t *testing.T) {
	applicationConfiguration := integrationTestConfiguration(t)
	requireTheBroker(t, applicationConfiguration)
	purgeTheDestinationQueues(t, applicationConfiguration)

	writeRoutingRules(t, applicationConfiguration.RoutingConfigurationFilePath, map[string]string{
		"claims":   "service1",
		"payments": "service2",
		"policy":   "service3",
	})
	serverApplication := newTestServerApplication(t, applicationConfiguration)

	responseRecorder := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", logBatchBodyForServices([]string{"claims", "payments", "policy"}))
	if responseRecorder.Code != http.StatusAccepted {
		t.Fatalf("ingestion status = %d, want %d", responseRecorder.Code, http.StatusAccepted)
	}

	destinationFiles := map[string]string{
		config.DatabaseFileService1Name: "claims",
		config.DatabaseFileService2Name: "payments",
		config.DatabaseFileService3Name: "policy",
	}
	for destinationFileName, serviceName := range destinationFiles {
		databaseFilePath := filepath.Join(applicationConfiguration.DatabaseDirectoryPath, destinationFileName)
		waitForStoredLogCount(t, databaseFilePath, serviceName, 1)

		storedLogRows := readStoredLogRows(t, databaseFilePath)
		if len(storedLogRows) != 1 {
			t.Fatalf("stored log count in %q = %d, want 1", destinationFileName, len(storedLogRows))
		}
		storedLogRow := storedLogRows[0]
		if storedLogRow.Service != serviceName {
			t.Errorf("stored service = %q, want %q", storedLogRow.Service, serviceName)
		}
		if storedLogRow.Level != string(model.LogLevelInfo) {
			t.Errorf("stored level = %q, want %q", storedLogRow.Level, model.LogLevelInfo)
		}
		if storedLogRow.SourceIP != "192.0.2.1" {
			t.Errorf("stored source ip = %q, want %q", storedLogRow.SourceIP, "192.0.2.1")
		}
		if storedLogRow.Environment != applicationConfiguration.ApplicationEnvironment {
			t.Errorf("stored environment = %q, want %q", storedLogRow.Environment, applicationConfiguration.ApplicationEnvironment)
		}
		if _, parseError := time.Parse("2006-01-02T15:04:05.000000Z07:00", storedLogRow.ReceivedAt); parseError != nil {
			t.Errorf("stored received at %q is not a microsecond timestamp: %v", storedLogRow.ReceivedAt, parseError)
		}
	}
}

func TestIntegration_ReplaysASpoolLargerThanTheRawLogsQueueCapacity(t *testing.T) {
	applicationConfiguration := integrationTestConfiguration(t)
	requireTheBroker(t, applicationConfiguration)
	purgeTheDestinationQueues(t, applicationConfiguration)

	applicationConfiguration.RawLogsBufferSize = 100
	applicationConfiguration.ConsumerBatchSize = 50
	applicationConfiguration.ChannelSendTimeout = 20 * time.Millisecond
	applicationConfiguration.SpoolFilePath = filepath.Join(t.TempDir(), "ingestion-spool.log")
	writeRoutingRules(t, applicationConfiguration.RoutingConfigurationFilePath, map[string]string{"claims": "service1"})

	// More than twice the queue capacity: a replay that runs before the pipeline consumers would fill the queue and drop the overflow.
	const spooledLogCount = 250
	spoolingSpool, spoolError := spool.NewSpool(applicationConfiguration.SpoolFilePath)
	if spoolError != nil {
		t.Fatalf("creating the spool failed: %v", spoolError)
	}
	spooledLogs := make([]model.RawLog, 0, spooledLogCount)
	for range spooledLogCount {
		spooledLogs = append(spooledLogs, model.RawLog{
			LogEntry: model.LogEntry{
				Timestamp: "2026-10-28T12:00:00.000000Z",
				Service:   "claims",
				Level:     model.LogLevelInfo,
				Message:   "spooled before the restart",
			},
			LogID:      uuid.NewString(),
			BatchID:    uuid.NewString(),
			ReceivedAt: time.Now(),
		})
	}
	if appendError := spoolingSpool.Append(spooledLogs); appendError != nil {
		t.Fatalf("filling the spool failed: %v", appendError)
	}
	if closeError := spoolingSpool.Close(); closeError != nil {
		t.Fatalf("closing the spool failed: %v", closeError)
	}

	newTestServerApplication(t, applicationConfiguration)

	databaseFilePath := filepath.Join(applicationConfiguration.DatabaseDirectoryPath, config.DatabaseFileService1Name)
	waitForStoredLogCount(t, databaseFilePath, "claims", spooledLogCount)

	if deadLetterEntryCount := len(readDeadLetterEntries(t, applicationConfiguration.DeadLetterFilePath)); deadLetterEntryCount != 0 {
		t.Errorf("dead letter entry count = %d, want 0", deadLetterEntryCount)
	}
}

func TestIntegration_RabbitMQOutageStoresTheAcceptedLogsThroughTheFallback(t *testing.T) {
	applicationConfiguration := integrationTestConfigurationWithoutBroker(t)
	writeRoutingRules(t, applicationConfiguration.RoutingConfigurationFilePath, map[string]string{"claims": "service1"})
	serverApplication := newTestServerApplication(t, applicationConfiguration)

	responseRecorder := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", logBatchBodyForServices([]string{"claims", "claims", "claims"}))
	if responseRecorder.Code != http.StatusAccepted {
		t.Fatalf("ingestion status = %d, want %d", responseRecorder.Code, http.StatusAccepted)
	}

	databaseFilePath := filepath.Join(applicationConfiguration.DatabaseDirectoryPath, config.DatabaseFileService1Name)
	waitForStoredLogCount(t, databaseFilePath, "claims", 3)

	if deadLetterEntryCount := len(readDeadLetterEntries(t, applicationConfiguration.DeadLetterFilePath)); deadLetterEntryCount != 0 {
		t.Errorf("dead letter entry count = %d, want 0", deadLetterEntryCount)
	}
}

func TestIntegration_SQLiteBatchingFlushesAtTheBatchSizeAndAtTheFlushInterval(t *testing.T) {
	batchingCases := []struct {
		name                 string
		writeBatchSize       int
		writeFlushInterval   time.Duration
		logCount             int
		maximumExpectedDelay time.Duration
	}{
		{name: "the batch size trigger", writeBatchSize: 100, writeFlushInterval: 30 * time.Second, logCount: 100, maximumExpectedDelay: 10 * time.Second},
		{name: "the flush interval trigger", writeBatchSize: 1000, writeFlushInterval: 200 * time.Millisecond, logCount: 1, maximumExpectedDelay: 10 * time.Second},
	}

	for _, batchingCase := range batchingCases {
		t.Run(batchingCase.name, func(t *testing.T) {
			applicationConfiguration := integrationTestConfigurationWithoutBroker(t)
			applicationConfiguration.WriteBatchSize = batchingCase.writeBatchSize
			applicationConfiguration.WriteFlushInterval = batchingCase.writeFlushInterval
			writeRoutingRules(t, applicationConfiguration.RoutingConfigurationFilePath, map[string]string{"claims": "service1"})
			serverApplication := newTestServerApplication(t, applicationConfiguration)

			serviceNames := make([]string, 0, batchingCase.logCount)
			for logIndex := 0; logIndex < batchingCase.logCount; logIndex++ {
				serviceNames = append(serviceNames, "claims")
			}
			responseRecorder := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", logBatchBodyForServices(serviceNames))
			if responseRecorder.Code != http.StatusAccepted {
				t.Fatalf("ingestion status = %d, want %d", responseRecorder.Code, http.StatusAccepted)
			}

			databaseFilePath := filepath.Join(applicationConfiguration.DatabaseDirectoryPath, config.DatabaseFileService1Name)
			flushDuration := waitForStoredLogCount(t, databaseFilePath, "claims", batchingCase.logCount)
			if flushDuration > batchingCase.maximumExpectedDelay {
				t.Errorf("flush duration = %s, want at most %s", flushDuration, batchingCase.maximumExpectedDelay)
			}
		})
	}
}

func TestIntegration_RetriesAndDeadLettersTheLogsThatCannotBeStored(t *testing.T) {
	applicationConfiguration := integrationTestConfigurationWithoutBroker(t)
	applicationConfiguration.DeadLetterMaximumFileSizeInBytes = 1024
	writeRoutingRules(t, applicationConfiguration.RoutingConfigurationFilePath, map[string]string{"claims": "service1"})
	serverApplication := newTestServerApplication(t, applicationConfiguration)

	if closeError := serverApplication.sqliteLogStore.Close(); closeError != nil {
		t.Fatalf("closing the sqlite store failed: %v", closeError)
	}

	serviceNames := make([]string, 0, 10)
	for logIndex := 0; logIndex < 10; logIndex++ {
		serviceNames = append(serviceNames, "claims")
	}
	responseRecorder := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", logBatchBodyForServices(serviceNames))
	if responseRecorder.Code != http.StatusAccepted {
		t.Fatalf("ingestion status = %d, want %d", responseRecorder.Code, http.StatusAccepted)
	}

	deadLetterEntries := waitForDeadLetterEntries(t, applicationConfiguration.DeadLetterFilePath, 10)
	for _, deadLetterEntry := range deadLetterEntries {
		if deadLetterEntry.Log.Service != "claims" {
			t.Errorf("dead lettered service = %q, want %q", deadLetterEntry.Log.Service, "claims")
		}
		if deadLetterEntry.ErrorMessage == "" {
			t.Error("the dead letter entry carries no error message")
		}
		if deadLetterEntry.Log.RetryCount != 3 {
			t.Errorf("dead lettered retry count = %d, want 3", deadLetterEntry.Log.RetryCount)
		}
		if deadLetterEntry.FailedAt.IsZero() {
			t.Error("the dead letter entry carries no failure timestamp")
		}
	}

	rotatedFilePath := fmt.Sprintf("%s.%d%s", strings.TrimSuffix(applicationConfiguration.DeadLetterFilePath, filepath.Ext(applicationConfiguration.DeadLetterFilePath)), 1, filepath.Ext(applicationConfiguration.DeadLetterFilePath))
	if _, statError := os.Stat(rotatedFilePath); statError != nil {
		t.Errorf("the dead letter file was not rotated: %v", statError)
	}
}

func TestIntegration_GracefulShutdownStoresEveryAcceptedLog(t *testing.T) {
	applicationConfiguration := integrationTestConfiguration(t)
	requireTheBroker(t, applicationConfiguration)
	purgeTheDestinationQueues(t, applicationConfiguration)

	writeRoutingRules(t, applicationConfiguration.RoutingConfigurationFilePath, map[string]string{"claims": "service1"})
	serverApplication := newTestServerApplication(t, applicationConfiguration)

	acceptedLogCount := 200
	batchCount := 4
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		serviceNames := make([]string, 0, acceptedLogCount/batchCount)
		for logIndex := 0; logIndex < acceptedLogCount/batchCount; logIndex++ {
			serviceNames = append(serviceNames, "claims")
		}
		responseRecorder := serveRequest(serverApplication.requestRouter, http.MethodPost, "/logs", logBatchBodyForServices(serviceNames))
		if responseRecorder.Code != http.StatusAccepted {
			t.Fatalf("ingestion status = %d, want %d", responseRecorder.Code, http.StatusAccepted)
		}
	}

	if shutdownError := serverApplication.shutdownBackgroundWork(aTestShutdownContext(t)); shutdownError != nil {
		t.Fatalf("stopping the background work failed: %v", shutdownError)
	}

	databaseFilePath := filepath.Join(applicationConfiguration.DatabaseDirectoryPath, config.DatabaseFileService1Name)
	storedLogCount := countStoredLogsForService(t, databaseFilePath, "claims")
	if storedLogCount > acceptedLogCount {
		t.Fatalf("stored log count = %d, want at most the %d accepted logs", storedLogCount, acceptedLogCount)
	}

	newTestServerApplication(t, applicationConfiguration)
	waitForStoredLogCount(t, databaseFilePath, "claims", acceptedLogCount)
}
