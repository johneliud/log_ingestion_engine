package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// Supplies only the required settings so tests can mutate or omit single values.
func aValidMinimalEnvironment(t *testing.T) map[string]string {
	t.Helper()
	return map[string]string{
		environmentNameRabbitMQHost:     "localhost",
		environmentNameRabbitMQPort:     "5672",
		environmentNameRabbitMQUsername: "log_engine",
		environmentNameRabbitMQPassword: "test_password",
	}
}

func TestNewApplicationConfigurationFromEnvironment_AppliesDefaultsForMissingSettings(t *testing.T) {
	t.Parallel()
	applicationConfiguration, configurationError := newApplicationConfigurationFromEnvironment(aValidMinimalEnvironment(t))
	if configurationError != nil {
		t.Fatalf("expected the minimal environment to be valid, got error: %v", configurationError)
	}

	if applicationConfiguration.HTTPPort != defaultHTTPPort {
		t.Errorf("HTTPPort = %d, want %d", applicationConfiguration.HTTPPort, defaultHTTPPort)
	}

	if applicationConfiguration.RateLimitPerSecond != defaultRateLimitPerSecond {
		t.Errorf("RateLimitPerSecond = %d, want %d", applicationConfiguration.RateLimitPerSecond, defaultRateLimitPerSecond)
	}

	if applicationConfiguration.RateLimitCleanupInterval != defaultRateLimitCleanupInterval {
		t.Errorf("RateLimitCleanupInterval = %s, want %s", applicationConfiguration.RateLimitCleanupInterval, defaultRateLimitCleanupInterval)
	}

	if applicationConfiguration.RawLogsBufferSize != defaultRawLogsBufferSize {
		t.Errorf("RawLogsBufferSize = %d, want %d", applicationConfiguration.RawLogsBufferSize, defaultRawLogsBufferSize)
	}

	if applicationConfiguration.ConsumerBatchSize != defaultConsumerBatchSize {
		t.Errorf("ConsumerBatchSize = %d, want %d", applicationConfiguration.ConsumerBatchSize, defaultConsumerBatchSize)
	}

	if applicationConfiguration.ChannelSendTimeout != defaultChannelSendTimeout {
		t.Errorf("ChannelSendTimeout = %s, want %s", applicationConfiguration.ChannelSendTimeout, defaultChannelSendTimeout)
	}

	if applicationConfiguration.MaximumBodySizeInBytes != defaultMaximumBodySizeInBytes {
		t.Errorf("MaximumBodySizeInBytes = %d, want %d", applicationConfiguration.MaximumBodySizeInBytes, defaultMaximumBodySizeInBytes)
	}

	if applicationConfiguration.ApplicationEnvironment != defaultApplicationEnvironment {
		t.Errorf("ApplicationEnvironment = %q, want %q", applicationConfiguration.ApplicationEnvironment, defaultApplicationEnvironment)
	}

	if applicationConfiguration.RoutingConfigurationFilePath != defaultRoutingConfigurationFilePath {
		t.Errorf("RoutingConfigurationFilePath = %q, want %q", applicationConfiguration.RoutingConfigurationFilePath, defaultRoutingConfigurationFilePath)
	}

	if applicationConfiguration.DefaultDestination != service1Destination {
		t.Errorf("DefaultDestination = %q, want %q", applicationConfiguration.DefaultDestination, service1Destination)
	}

	if applicationConfiguration.RabbitMQManagementURL != "" {
		t.Errorf("RabbitMQManagementURL = %q, want the empty default", applicationConfiguration.RabbitMQManagementURL)
	}

	if applicationConfiguration.PublishTimeout != defaultPublishTimeout {
		t.Errorf("PublishTimeout = %s, want %s", applicationConfiguration.PublishTimeout, defaultPublishTimeout)
	}

	expectedRabbitMQReconnectBackoffDelays := []time.Duration{time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}

	if len(applicationConfiguration.RabbitMQReconnectBackoffDelays) != len(expectedRabbitMQReconnectBackoffDelays) {
		t.Fatalf("RabbitMQReconnectBackoffDelays has %d entries, want %d", len(applicationConfiguration.RabbitMQReconnectBackoffDelays), len(expectedRabbitMQReconnectBackoffDelays))
	}

	for reconnectBackoffDelayIndex, expectedReconnectBackoffDelay := range expectedRabbitMQReconnectBackoffDelays {
		if applicationConfiguration.RabbitMQReconnectBackoffDelays[reconnectBackoffDelayIndex] != expectedReconnectBackoffDelay {
			t.Errorf("RabbitMQReconnectBackoffDelays[%d] = %s, want %s", reconnectBackoffDelayIndex, applicationConfiguration.RabbitMQReconnectBackoffDelays[reconnectBackoffDelayIndex], expectedReconnectBackoffDelay)
		}
	}

	if applicationConfiguration.PipelineWorkerCount != defaultPipelineWorkerCount {
		t.Errorf("PipelineWorkerCount = %d, want %d", applicationConfiguration.PipelineWorkerCount, defaultPipelineWorkerCount)
	}

	if applicationConfiguration.WriteBatchSize != defaultWriteBatchSize {
		t.Errorf("WriteBatchSize = %d, want %d", applicationConfiguration.WriteBatchSize, defaultWriteBatchSize)
	}

	if applicationConfiguration.WriteFlushInterval != defaultWriteFlushInterval {
		t.Errorf("WriteFlushInterval = %s, want %s", applicationConfiguration.WriteFlushInterval, defaultWriteFlushInterval)
	}

	if applicationConfiguration.DatabaseDirectoryPath != defaultDatabaseDirectoryPath {
		t.Errorf("DatabaseDirectoryPath = %q, want %q", applicationConfiguration.DatabaseDirectoryPath, defaultDatabaseDirectoryPath)
	}

	if applicationConfiguration.DatabaseBusyTimeout != defaultDatabaseBusyTimeout {
		t.Errorf("DatabaseBusyTimeout = %s, want %s", applicationConfiguration.DatabaseBusyTimeout, defaultDatabaseBusyTimeout)
	}

	if applicationConfiguration.DeadLetterFilePath != defaultDeadLetterFilePath {
		t.Errorf("DeadLetterFilePath = %q, want %q", applicationConfiguration.DeadLetterFilePath, defaultDeadLetterFilePath)
	}

	if applicationConfiguration.DeadLetterMaximumFileSizeInBytes != defaultDeadLetterMaximumFileSizeInBytes {
		t.Errorf("DeadLetterMaximumFileSizeInBytes = %d, want %d", applicationConfiguration.DeadLetterMaximumFileSizeInBytes, defaultDeadLetterMaximumFileSizeInBytes)
	}

	expectedRetryBackoffDelays := []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}

	for retryBackoffDelayIndex, expectedRetryBackoffDelay := range expectedRetryBackoffDelays {
		if applicationConfiguration.WriteRetryBackoffDelays[retryBackoffDelayIndex] != expectedRetryBackoffDelay {
			t.Errorf("WriteRetryBackoffDelays[%d] = %s, want %s", retryBackoffDelayIndex, applicationConfiguration.WriteRetryBackoffDelays[retryBackoffDelayIndex], expectedRetryBackoffDelay)
		}
	}
}

func TestNewApplicationConfigurationFromEnvironment_ParsesConfiguredValues(t *testing.T) {
	t.Parallel()
	environmentVariables := aValidMinimalEnvironment(t)
	environmentVariables[environmentNameHTTPPort] = "9443"
	environmentVariables[environmentNameRateLimitPerSecond] = "250"
	environmentVariables[environmentNameRateLimitCleanupInterval] = "2m"
	environmentVariables[environmentNameRawLogsBufferSize] = "500"
	environmentVariables[environmentNameConsumerBatchSize] = "25"
	environmentVariables[environmentNameChannelSendTimeout] = "250ms"
	environmentVariables[environmentNameMaximumBodySizeInBytes] = "2048"
	environmentVariables[environmentNameApplicationEnvironment] = "staging"
	environmentVariables[environmentNameRoutingConfigurationFilePath] = "rules.json"
	environmentVariables[environmentNameDefaultDestination] = "service2"
	environmentVariables[environmentNameRabbitMQHost] = "broker.internal"
	environmentVariables[environmentNameRabbitMQPort] = "5673"
	environmentVariables[environmentNameRabbitMQManagementURL] = "https://broker.internal:15672"
	environmentVariables[environmentNamePublishTimeout] = "3s"
	environmentVariables[environmentNameRabbitMQReconnectBackoffDelayList] = "250ms, 2s, 15s"
	environmentVariables[environmentNamePipelineWorkerCount] = "8"
	environmentVariables[environmentNameWriteBatchSize] = "200"
	environmentVariables[environmentNameWriteFlushInterval] = "2s"
	environmentVariables[environmentNameDatabaseDirectoryPath] = "/var/log-engine"
	environmentVariables[environmentNameDatabaseBusyTimeout] = "2500ms"
	environmentVariables[environmentNameWriteRetryBackoffDelayList] = "500ms, 2s, 4s, 8s"
	environmentVariables[environmentNameDeadLetterFilePath] = "failed/letters.jsonl"
	environmentVariables[environmentNameDeadLetterMaximumFileSizeInBytes] = "20971520"
	applicationConfiguration, configurationError := newApplicationConfigurationFromEnvironment(environmentVariables)

	if configurationError != nil {
		t.Fatalf("expected the configured environment to be valid, got error: %v", configurationError)
	}

	if applicationConfiguration.HTTPPort != 9443 {
		t.Errorf("HTTPPort = %d, want 9443", applicationConfiguration.HTTPPort)
	}

	if applicationConfiguration.RateLimitPerSecond != 250 {
		t.Errorf("RateLimitPerSecond = %d, want 250", applicationConfiguration.RateLimitPerSecond)
	}

	if applicationConfiguration.RateLimitCleanupInterval != 2*time.Minute {
		t.Errorf("RateLimitCleanupInterval = %s, want 2m", applicationConfiguration.RateLimitCleanupInterval)
	}

	if applicationConfiguration.RawLogsBufferSize != 500 {
		t.Errorf("RawLogsBufferSize = %d, want 500", applicationConfiguration.RawLogsBufferSize)
	}

	if applicationConfiguration.ConsumerBatchSize != 25 {
		t.Errorf("ConsumerBatchSize = %d, want 25", applicationConfiguration.ConsumerBatchSize)
	}

	if applicationConfiguration.ChannelSendTimeout != 250*time.Millisecond {
		t.Errorf("ChannelSendTimeout = %s, want 250ms", applicationConfiguration.ChannelSendTimeout)
	}

	if applicationConfiguration.MaximumBodySizeInBytes != 2048 {
		t.Errorf("MaximumBodySizeInBytes = %d, want 2048", applicationConfiguration.MaximumBodySizeInBytes)
	}

	if applicationConfiguration.ApplicationEnvironment != "staging" {
		t.Errorf("ApplicationEnvironment = %q, want %q", applicationConfiguration.ApplicationEnvironment, "staging")
	}

	if applicationConfiguration.RoutingConfigurationFilePath != "rules.json" {
		t.Errorf("RoutingConfigurationFilePath = %q, want %q", applicationConfiguration.RoutingConfigurationFilePath, "rules.json")
	}

	if applicationConfiguration.DefaultDestination != "service2" {
		t.Errorf("DefaultDestination = %q, want %q", applicationConfiguration.DefaultDestination, "service2")
	}

	if applicationConfiguration.RabbitMQHost != "broker.internal" {
		t.Errorf("RabbitMQHost = %q, want %q", applicationConfiguration.RabbitMQHost, "broker.internal")
	}

	if applicationConfiguration.RabbitMQPort != 5673 {
		t.Errorf("RabbitMQPort = %d, want 5673", applicationConfiguration.RabbitMQPort)
	}

	if applicationConfiguration.RabbitMQManagementURL != "https://broker.internal:15672" {
		t.Errorf("RabbitMQManagementURL = %q, want %q", applicationConfiguration.RabbitMQManagementURL, "https://broker.internal:15672")
	}

	if applicationConfiguration.PublishTimeout != 3*time.Second {
		t.Errorf("PublishTimeout = %s, want 3s", applicationConfiguration.PublishTimeout)
	}

	expectedRabbitMQReconnectBackoffDelays := []time.Duration{250 * time.Millisecond, 2 * time.Second, 15 * time.Second}

	if len(applicationConfiguration.RabbitMQReconnectBackoffDelays) != len(expectedRabbitMQReconnectBackoffDelays) {
		t.Fatalf("RabbitMQReconnectBackoffDelays has %d entries, want %d", len(applicationConfiguration.RabbitMQReconnectBackoffDelays), len(expectedRabbitMQReconnectBackoffDelays))
	}

	for reconnectBackoffDelayIndex, expectedReconnectBackoffDelay := range expectedRabbitMQReconnectBackoffDelays {
		if applicationConfiguration.RabbitMQReconnectBackoffDelays[reconnectBackoffDelayIndex] != expectedReconnectBackoffDelay {
			t.Errorf("RabbitMQReconnectBackoffDelays[%d] = %s, want %s", reconnectBackoffDelayIndex, applicationConfiguration.RabbitMQReconnectBackoffDelays[reconnectBackoffDelayIndex], expectedReconnectBackoffDelay)
		}
	}

	if applicationConfiguration.PipelineWorkerCount != 8 {
		t.Errorf("PipelineWorkerCount = %d, want 8", applicationConfiguration.PipelineWorkerCount)
	}

	if applicationConfiguration.WriteBatchSize != 200 {
		t.Errorf("WriteBatchSize = %d, want 200", applicationConfiguration.WriteBatchSize)
	}

	if applicationConfiguration.WriteFlushInterval != 2*time.Second {
		t.Errorf("WriteFlushInterval = %s, want 2s", applicationConfiguration.WriteFlushInterval)
	}

	if applicationConfiguration.DatabaseDirectoryPath != "/var/log-engine" {
		t.Errorf("DatabaseDirectoryPath = %q, want %q", applicationConfiguration.DatabaseDirectoryPath, "/var/log-engine")
	}

	if applicationConfiguration.DatabaseBusyTimeout != 2500*time.Millisecond {
		t.Errorf("DatabaseBusyTimeout = %s, want 2.5s", applicationConfiguration.DatabaseBusyTimeout)
	}

	expectedRetryBackoffDelays := []time.Duration{500 * time.Millisecond, 2 * time.Second, 4 * time.Second, 8 * time.Second}

	if len(applicationConfiguration.WriteRetryBackoffDelays) != len(expectedRetryBackoffDelays) {
		t.Fatalf("WriteRetryBackoffDelays has %d entries, want %d", len(applicationConfiguration.WriteRetryBackoffDelays), len(expectedRetryBackoffDelays))
	}

	for retryBackoffDelayIndex, expectedRetryBackoffDelay := range expectedRetryBackoffDelays {
		if applicationConfiguration.WriteRetryBackoffDelays[retryBackoffDelayIndex] != expectedRetryBackoffDelay {
			t.Errorf("WriteRetryBackoffDelays[%d] = %s, want %s", retryBackoffDelayIndex, applicationConfiguration.WriteRetryBackoffDelays[retryBackoffDelayIndex], expectedRetryBackoffDelay)
		}
	}

	if applicationConfiguration.DeadLetterFilePath != "failed/letters.jsonl" {
		t.Errorf("DeadLetterFilePath = %q, want %q", applicationConfiguration.DeadLetterFilePath, "failed/letters.jsonl")
	}

	if applicationConfiguration.DeadLetterMaximumFileSizeInBytes != 20971520 {
		t.Errorf("DeadLetterMaximumFileSizeInBytes = %d, want 20971520", applicationConfiguration.DeadLetterMaximumFileSizeInBytes)
	}
}

func TestNewApplicationConfigurationFromEnvironment_RejectsInvalidSettings(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                    string
		environmentMutation     func(map[string]string)
		expectedErrorSubstrings []string
	}{
		{
			name: "rate limit is not an integer",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameRateLimitPerSecond] = "abc"
			},
			expectedErrorSubstrings: []string{"RATE_LIMIT", "not a valid integer"},
		},
		{
			name: "rate limit is zero",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameRateLimitPerSecond] = "0"
			},
			expectedErrorSubstrings: []string{"RATE_LIMIT", "positive integer"},
		},
		{
			name: "rate limit is negative",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameRateLimitPerSecond] = "-5"
			},
			expectedErrorSubstrings: []string{"RATE_LIMIT", "positive integer"},
		},
		{
			name: "http port is above the TCP range",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameHTTPPort] = "70000"
			},
			expectedErrorSubstrings: []string{"HTTP_PORT", "TCP port between 1024 and 65535"},
		},
		{
			name: "rabbitmq port is missing",
			environmentMutation: func(environmentVariables map[string]string) {
				delete(environmentVariables, environmentNameRabbitMQPort)
			},
			expectedErrorSubstrings: []string{"RABBITMQ_PORT", errMissingRequiredSetting.Error()},
		},
		{
			name: "rabbitmq port is above the TCP range",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameRabbitMQPort] = "65536"
			},
			expectedErrorSubstrings: []string{"RABBITMQ_PORT", "TCP port between 1024 and 65535"},
		},
		{
			name: "rabbitmq host is missing",
			environmentMutation: func(environmentVariables map[string]string) {
				delete(environmentVariables, environmentNameRabbitMQHost)
			},
			expectedErrorSubstrings: []string{"RABBITMQ_HOST", errMissingRequiredSetting.Error()},
		},
		{
			name: "rabbitmq password is empty",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameRabbitMQPassword] = ""
			},
			expectedErrorSubstrings: []string{"RABBITMQ_PASS", errMissingRequiredSetting.Error()},
		},
		{
			name: "channel send timeout is not a duration",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameChannelSendTimeout] = "fast"
			},
			expectedErrorSubstrings: []string{"CHANNEL_SEND_TIMEOUT", "not a valid duration"},
		},
		{
			name: "publish timeout is not positive",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNamePublishTimeout] = "-5s"
			},
			expectedErrorSubstrings: []string{"PUBLISH_TIMEOUT", "positive duration"},
		},
		{
			name: "write batch size is zero",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameWriteBatchSize] = "0"
			},
			expectedErrorSubstrings: []string{"WRITE_BATCH_SIZE", "positive integer"},
		},
		{
			name: "database busy timeout is not positive",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameDatabaseBusyTimeout] = "0s"
			},
			expectedErrorSubstrings: []string{"DB_BUSY_TIMEOUT", "positive duration"},
		},
		{
			name: "retry backoff list contains a zero duration",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameWriteRetryBackoffDelayList] = "1s,0s,10s"
			},
			expectedErrorSubstrings: []string{"RETRY_BACKOFFS", "positive durations"},
		},
		{
			name: "retry backoff list contains a non duration",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameWriteRetryBackoffDelayList] = "1s,whenever"
			},
			expectedErrorSubstrings: []string{"RETRY_BACKOFFS", "not a valid duration"},
		},
		{
			name: "reconnect backoff list contains a zero duration",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameRabbitMQReconnectBackoffDelayList] = "1s,0s"
			},
			expectedErrorSubstrings: []string{"RABBITMQ_RECONNECT_BACKOFFS", "positive durations"},
		},
		{
			name: "reconnect backoff list contains a non duration",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameRabbitMQReconnectBackoffDelayList] = "soon"
			},
			expectedErrorSubstrings: []string{"RABBITMQ_RECONNECT_BACKOFFS", "not a valid duration"},
		},
		{
			name: "consumer batch size exceeds the raw logs buffer",
			environmentMutation: func(environmentVariables map[string]string) {
				environmentVariables[environmentNameRawLogsBufferSize] = "20"
				environmentVariables[environmentNameConsumerBatchSize] = "50"
			},
			expectedErrorSubstrings: []string{"CONSUMER_BATCH_SIZE", "must not exceed", "RAW_LOGS_BUFFER"},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			environmentVariables := aValidMinimalEnvironment(t)
			testCase.environmentMutation(environmentVariables)
			_, configurationError := newApplicationConfigurationFromEnvironment(environmentVariables)
			if configurationError == nil {
				t.Fatalf("expected an error but configuration parsing succeeded")
			}

			for _, expectedErrorSubstring := range testCase.expectedErrorSubstrings {
				if !strings.Contains(configurationError.Error(), expectedErrorSubstring) {
					t.Errorf("error %q does not contain %q", configurationError.Error(), expectedErrorSubstring)
				}
			}
		})
	}
}

func TestNewApplicationConfigurationFromEnvironment_WrapsMissingRequiredSetting(t *testing.T) {
	t.Parallel()
	environmentVariables := aValidMinimalEnvironment(t)
	delete(environmentVariables, environmentNameRabbitMQUsername)
	_, configurationError := newApplicationConfigurationFromEnvironment(environmentVariables)
	if configurationError == nil {
		t.Fatalf("expected an error for the missing RabbitMQ username")
	}

	if !errors.Is(configurationError, errMissingRequiredSetting) {
		t.Errorf("error %v should wrap errMissingRequiredSetting", configurationError)
	}
}
