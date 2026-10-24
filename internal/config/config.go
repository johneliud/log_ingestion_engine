package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	environmentNameHTTPPort                         = "HTTP_PORT"
	environmentNameRateLimitPerSecond               = "RATE_LIMIT"
	environmentNameRateLimitCleanupInterval         = "RATE_LIMIT_CLEANUP_INTERVAL"
	environmentNameRawLogsBufferSize                = "RAW_LOGS_BUFFER"
	environmentNameConsumerBatchSize                = "CONSUMER_BATCH_SIZE"
	environmentNameChannelSendTimeout               = "CHANNEL_SEND_TIMEOUT"
	environmentNameMaximumBodySizeInBytes           = "MAX_BODY_BYTES"
	environmentNameApplicationEnvironment           = "APP_ENV"
	environmentNameRoutingConfigurationFilePath     = "ROUTING_CONFIG_PATH"
	environmentNameDefaultDestination               = "DEFAULT_DESTINATION"
	environmentNameRabbitMQHost                     = "RABBITMQ_HOST"
	environmentNameRabbitMQPort                     = "RABBITMQ_PORT"
	environmentNameRabbitMQUsername                 = "RABBITMQ_USER"
	environmentNameRabbitMQPassword                 = "RABBITMQ_PASS"
	environmentNameRabbitMQManagementURL            = "RABBITMQ_MGMT_URL"
	environmentNamePublishTimeout                   = "PUBLISH_TIMEOUT"
	environmentNamePipelineWorkerCount              = "PIPELINE_WORKER_COUNT"
	environmentNameWriteBatchSize                   = "WRITE_BATCH_SIZE"
	environmentNameWriteFlushInterval               = "WRITE_FLUSH_INTERVAL"
	environmentNameDatabaseDirectoryPath            = "DB_DIR"
	environmentNameConfigurationFilePath            = "CONFIG_FILE_PATH"
	environmentNameWriteRetryBackoffDelayList       = "RETRY_BACKOFFS"
	environmentNameDeadLetterFilePath               = "DEAD_LETTER_PATH"
	environmentNameDeadLetterMaximumFileSizeInBytes = "DEAD_LETTER_MAX_BYTES"
)

const (
	defaultHTTPPort                         = 8080
	defaultRateLimitPerSecond               = 1000
	defaultRateLimitCleanupInterval         = time.Minute
	defaultRawLogsBufferSize                = 10000
	defaultConsumerBatchSize                = 50
	defaultChannelSendTimeout               = 100 * time.Millisecond
	defaultMaximumBodySizeInBytes           = 1048576
	defaultApplicationEnvironment           = "development"
	defaultRoutingConfigurationFilePath     = "configs/routing.json"
	service1Destination                     = "service1"
	defaultPublishTimeout                   = 5 * time.Second
	defaultPipelineWorkerCount              = 4
	defaultWriteBatchSize                   = 100
	defaultWriteFlushInterval               = time.Second
	defaultDatabaseDirectoryPath            = "./data"
	defaultConfigurationFilePath            = ".env"
	defaultDeadLetterFilePath               = "logs-failed.json"
	defaultDeadLetterMaximumFileSizeInBytes = 10485760
)

var defaultWriteRetryBackoffDelays = []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}

var errMissingRequiredSetting = errors.New("required environment variable is not set")

// Holds every runtime setting, loaded once in internal/config and passed down to all modules.
type ApplicationConfiguration struct {
	HTTPPort                         int
	RateLimitPerSecond               int
	RateLimitCleanupInterval         time.Duration
	RawLogsBufferSize                int
	ConsumerBatchSize                int
	ChannelSendTimeout               time.Duration
	MaximumBodySizeInBytes           int
	ApplicationEnvironment           string
	RoutingConfigurationFilePath     string
	DefaultDestination               string
	RabbitMQHost                     string
	RabbitMQPort                     int
	RabbitMQUsername                 string
	RabbitMQPassword                 string
	RabbitMQManagementURL            string
	PublishTimeout                   time.Duration
	PipelineWorkerCount              int
	WriteBatchSize                   int
	WriteFlushInterval               time.Duration
	DatabaseDirectoryPath            string
	WriteRetryBackoffDelays          []time.Duration
	DeadLetterFilePath               string
	DeadLetterMaximumFileSizeInBytes int
}

// Builds the application configuration from the merged process environment and configuration file, returning an error naming the first invalid setting.
func NewApplicationConfiguration() (ApplicationConfiguration, error) {
	environmentVariables := mapFromOSEnviron(os.Environ())
	configurationFilePath := environmentVariables[environmentNameConfigurationFilePath]
	if configurationFilePath == "" {
		configurationFilePath = defaultConfigurationFilePath
	}
	configurationFileSettings, configurationFileError := readOptionalConfigurationFile(configurationFilePath)
	if configurationFileError != nil {
		return ApplicationConfiguration{}, configurationFileError
	}
	mergedSettings := mergedEnvironmentWithFileSettings(environmentVariables, configurationFileSettings)
	applicationConfiguration, configurationError := newApplicationConfigurationFromEnvironment(mergedSettings)
	if configurationError != nil {
		return ApplicationConfiguration{}, configurationError
	}
	return applicationConfiguration, nil
}

// Rejects a consumer batch size larger than the raw logs buffer.
func validateConsumerBatchFitsBuffer(applicationConfiguration ApplicationConfiguration) error {
	if applicationConfiguration.ConsumerBatchSize > applicationConfiguration.RawLogsBufferSize {
		return fmt.Errorf("environment variable %s (%d) must not exceed environment variable %s (%d)", environmentNameConsumerBatchSize, applicationConfiguration.ConsumerBatchSize, environmentNameRawLogsBufferSize, applicationConfiguration.RawLogsBufferSize)
	}
	return nil
}

// Returns the TCP port for the setting, falling back to the default when empty unless the default is zero.
func parsePortSetting(environmentVariables map[string]string, settingName string, defaultPort int) (int, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		if defaultPort == 0 {
			return 0, fmt.Errorf("%w: %s", errMissingRequiredSetting, settingName)
		}
		return defaultPort, nil
	}

	parsedPort, parsingError := strconv.Atoi(rawSettingValue)
	if parsingError != nil {
		return 0, fmt.Errorf("environment variable %s has value %q, which is not a valid integer: %w", settingName, rawSettingValue, parsingError)
	}

	if parsedPort < 1024 || parsedPort > 65535 {
		return 0, fmt.Errorf("environment variable %s must be a TCP port between 1024 and 65535, but shows %q", settingName, rawSettingValue)
	}
	return parsedPort, nil
}

// Returns the integer for the setting, falling back to the default when empty and rejecting non-positive values.
func parsePositiveIntegerSetting(environmentVariables map[string]string, settingName string, defaultValue int) (int, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		return defaultValue, nil
	}

	parsedSettingValue, parsingError := strconv.Atoi(rawSettingValue)
	if parsingError != nil {
		return 0, fmt.Errorf("environment variable %s has value %q, which is not a valid integer: %w", settingName, rawSettingValue, parsingError)
	}

	if parsedSettingValue <= 0 {
		return 0, fmt.Errorf("environment variable %s must be a positive integer, but shows %d", settingName, parsedSettingValue)
	}
	return parsedSettingValue, nil
}

// Returns the duration for the setting, falling back to the default when empty and rejecting non-positive values.
func parsePositiveDurationSetting(environmentVariables map[string]string, settingName string, defaultValue time.Duration) (time.Duration, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		return defaultValue, nil
	}

	parsedSettingValue, parsingError := time.ParseDuration(rawSettingValue)
	if parsingError != nil {
		return 0, fmt.Errorf("environment variable %s has value %q, which is not a valid duration: %w", settingName, rawSettingValue, parsingError)
	}

	if parsedSettingValue <= 0 {
		return 0, fmt.Errorf("environment variable %s must be a positive duration, but shows %q", settingName, rawSettingValue)
	}
	return parsedSettingValue, nil
}

// Returns the comma-separated durations for the setting, falling back to the default when empty and rejecting non-positive entries.
func parsePositiveDurationListSetting(environmentVariables map[string]string, settingName string, defaultValue []time.Duration) ([]time.Duration, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		return defaultValue, nil
	}

	rawSettingValues := strings.Split(rawSettingValue, ",")
	parsedSettingValues := make([]time.Duration, 0, len(rawSettingValues))
	for _, rawSettingEntry := range rawSettingValues {
		trimmedSettingEntry := strings.TrimSpace(rawSettingEntry)
		parsedSettingEntry, parsingError := time.ParseDuration(trimmedSettingEntry)
		if parsingError != nil {
			return nil, fmt.Errorf("environment variable %s contains value %q, which is not a valid duration: %w", settingName, trimmedSettingEntry, parsingError)
		}

		if parsedSettingEntry <= 0 {
			return nil, fmt.Errorf("environment variable %s must contain only positive durations, but includes %q", settingName, trimmedSettingEntry)
		}

		parsedSettingValues = append(parsedSettingValues, parsedSettingEntry)
	}
	return parsedSettingValues, nil
}

// Returns the string for the setting and an error naming the variable when it is missing or empty.
func parseRequiredStringSetting(environmentVariables map[string]string, settingName string) (string, error) {
	settingValue := environmentVariables[settingName]
	if settingValue == "" {
		return "", fmt.Errorf("%w: %s", errMissingRequiredSetting, settingName)
	}
	return settingValue, nil
}

// Returns the string for the setting, falling back to the default when missing or empty.
func parseStringSetting(environmentVariables map[string]string, settingName string, defaultValue string) (string, error) {
	settingValue := environmentVariables[settingName]
	if settingValue == "" {
		return defaultValue, nil
	}
	return settingValue, nil
}
