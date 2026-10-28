package config

import (
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const (
	environmentNameHTTPPort                         = "HTTP_PORT"
	environmentNameRateLimitPerSecond               = "RATE_LIMIT"
	environmentNameRateLimitCleanupIntervalSeconds  = "RATE_LIMIT_CLEANUP_INTERVAL_IN_SECONDS"
	environmentNameRawLogsBufferSize                = "RAW_LOGS_BUFFER"
	environmentNameConsumerBatchSize                = "CONSUMER_BATCH_SIZE"
	environmentNameChannelSendTimeoutMillis         = "CHANNEL_SEND_TIMEOUT_IN_MILLISECONDS"
	environmentNameMaximumBodySizeInBytes           = "MAX_BODY_BYTES"
	environmentNameApplicationEnvironment           = "APP_ENV"
	environmentNameRoutingRules                     = "ROUTING_RULES"
	environmentNameDefaultDestination               = "DEFAULT_DESTINATION"
	environmentNameTrustedProxies                   = "TRUSTED_PROXIES"
	environmentNameRabbitMQHost                     = "RABBITMQ_HOST"
	environmentNameRabbitMQPort                     = "RABBITMQ_PORT"
	environmentNameRabbitMQUsername                 = "RABBITMQ_USER"
	environmentNameRabbitMQPassword                 = "RABBITMQ_PASS"
	environmentNameRabbitMQManagementURL            = "RABBITMQ_MGMT_URL"
	environmentNameRabbitMQVirtualHost              = "RABBITMQ_VHOST"
	environmentNameRabbitMQUseTLS                   = "RABBITMQ_USE_TLS"
	environmentNamePublishTimeoutSeconds            = "PUBLISH_TIMEOUT_IN_SECONDS"
	environmentNameRabbitMQReconnectBackoffSeconds  = "RABBITMQ_RECONNECT_BACKOFFS_IN_SECONDS"
	environmentNamePipelineWorkerCount              = "PIPELINE_WORKER_COUNT"
	environmentNameWriteBatchSize                   = "WRITE_BATCH_SIZE"
	environmentNameWriteFlushIntervalSeconds        = "WRITE_FLUSH_INTERVAL_IN_SECONDS"
	environmentNameDatabaseDirectoryPath            = "DB_DIR"
	environmentNameDatabaseBusyTimeoutSeconds       = "DB_BUSY_TIMEOUT_IN_SECONDS"
	environmentNameWriteRetryBackoffSeconds         = "RETRY_BACKOFFS_IN_SECONDS"
	environmentNameDeadLetterFilePath               = "DEAD_LETTER_PATH"
	environmentNameDeadLetterMaximumFileSizeInBytes = "DEAD_LETTER_MAX_BYTES"
	environmentNameMetricsThroughputWindowSeconds   = "METRICS_THROUGHPUT_WINDOW_IN_SECONDS"
	environmentNameMetricsBacklogTimeoutSeconds     = "METRICS_BACKLOG_TIMEOUT_IN_SECONDS"
	environmentNameShutdownGracePeriodSeconds       = "SHUTDOWN_GRACE_PERIOD_IN_SECONDS"
	environmentNameSpoolFilePath                    = "SPOOL_PATH"
	environmentNameCorsAllowedOrigins               = "CORS_ALLOWED_ORIGINS"
)

const (
	defaultHTTPPort                 = 8080
	defaultRateLimitPerSecond       = 1000
	defaultRateLimitCleanupInterval = time.Minute
	defaultRawLogsBufferSize        = 10000
	defaultConsumerBatchSize        = 50
	defaultChannelSendTimeout       = 100 * time.Millisecond
	defaultMaximumBodySizeInBytes   = 1048576
	defaultApplicationEnvironment   = "production"
	service1Destination             = "service1"
	defaultPublishTimeout           = 5 * time.Second
	defaultRabbitMQUseTLS           = false
	// DefaultRabbitMQVirtualHost is the virtual host used when none is configured.
	DefaultRabbitMQVirtualHost              = "/"
	defaultPipelineWorkerCount              = 4
	defaultWriteBatchSize                   = 100
	defaultWriteFlushInterval               = time.Second
	defaultDatabaseDirectoryPath            = "./data"
	defaultDatabaseBusyTimeout              = 5 * time.Second
	defaultDeadLetterFilePath               = "logs-failed.json"
	defaultDeadLetterMaximumFileSizeInBytes = 10485760
	DefaultSpoolFilePath                    = "ingestion-spool.log"
	defaultMetricsThroughputWindow          = time.Minute
	defaultMetricsBacklogTimeout            = 2 * time.Second
	defaultShutdownGracePeriod              = 30 * time.Second

	RoutingKeyLogWrite = "log.write"

	ExchangeService1Name = "exchange_service1"
	ExchangeService2Name = "exchange_service2"
	ExchangeService3Name = "exchange_service3"

	QueueService1Name = "queue_service1"
	QueueService2Name = "queue_service2"
	QueueService3Name = "queue_service3"

	DatabaseFileService1Name = "logs_service1.db"
	DatabaseFileService2Name = "logs_service2.db"
	DatabaseFileService3Name = "logs_service3.db"
)

var defaultWriteRetryBackoffDelays = []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}

var defaultRabbitMQReconnectBackoffDelays = []time.Duration{time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}

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
	RoutingRules                     map[string]string
	DefaultDestination               string
	TrustedProxies                   []string
	RabbitMQHost                     string
	RabbitMQPort                     int
	RabbitMQUsername                 string
	RabbitMQPassword                 string
	RabbitMQManagementURL            string
	RabbitMQVirtualHost              string
	RabbitMQUseTLS                   bool
	PublishTimeout                   time.Duration
	RabbitMQReconnectBackoffDelays   []time.Duration
	PipelineWorkerCount              int
	WriteBatchSize                   int
	WriteFlushInterval               time.Duration
	DatabaseDirectoryPath            string
	DatabaseBusyTimeout              time.Duration
	WriteRetryBackoffDelays          []time.Duration
	DeadLetterFilePath               string
	DeadLetterMaximumFileSizeInBytes int
	SpoolFilePath                    string
	CorsAllowedOrigins               []string
	MetricsThroughputWindow          time.Duration
	MetricsBacklogTimeout            time.Duration
	ShutdownGracePeriod              time.Duration
}

// Builds the application configuration from .env, returning an error naming the first invalid setting.
func NewApplicationConfiguration() (ApplicationConfiguration, error) {
	environmentVariables, fileError := godotenv.Read(".env")
	if fileError != nil {
		return ApplicationConfiguration{}, fmt.Errorf("loading .env: %w", fileError)
	}
	applicationConfiguration, configurationError := newApplicationConfigurationFromEnvironment(environmentVariables)
	if configurationError != nil {
		return ApplicationConfiguration{}, configurationError
	}
	return applicationConfiguration, nil
}

// Parses every setting from the environment variable map, applying defaults for missing values.
func newApplicationConfigurationFromEnvironment(environmentVariables map[string]string) (ApplicationConfiguration, error) {
	applicationConfiguration := ApplicationConfiguration{}

	httpPort, httpPortError := parsePortSetting(environmentVariables, environmentNameHTTPPort, defaultHTTPPort)
	if httpPortError != nil {
		return ApplicationConfiguration{}, httpPortError
	}
	applicationConfiguration.HTTPPort = httpPort

	rateLimitPerSecond, rateLimitPerSecondError := parsePositiveIntegerSetting(environmentVariables, environmentNameRateLimitPerSecond, defaultRateLimitPerSecond)
	if rateLimitPerSecondError != nil {
		return ApplicationConfiguration{}, rateLimitPerSecondError
	}
	applicationConfiguration.RateLimitPerSecond = rateLimitPerSecond

	rateLimitCleanupInterval, rateLimitCleanupIntervalError := parsePositiveDurationFromSeconds(environmentVariables, environmentNameRateLimitCleanupIntervalSeconds, defaultRateLimitCleanupInterval)
	if rateLimitCleanupIntervalError != nil {
		return ApplicationConfiguration{}, rateLimitCleanupIntervalError
	}
	applicationConfiguration.RateLimitCleanupInterval = rateLimitCleanupInterval

	rawLogsBufferSize, rawLogsBufferSizeError := parsePositiveIntegerSetting(environmentVariables, environmentNameRawLogsBufferSize, defaultRawLogsBufferSize)
	if rawLogsBufferSizeError != nil {
		return ApplicationConfiguration{}, rawLogsBufferSizeError
	}
	applicationConfiguration.RawLogsBufferSize = rawLogsBufferSize

	consumerBatchSize, consumerBatchSizeError := parsePositiveIntegerSetting(environmentVariables, environmentNameConsumerBatchSize, defaultConsumerBatchSize)
	if consumerBatchSizeError != nil {
		return ApplicationConfiguration{}, consumerBatchSizeError
	}
	applicationConfiguration.ConsumerBatchSize = consumerBatchSize

	channelSendTimeout, channelSendTimeoutError := parsePositiveDurationFromMilliseconds(environmentVariables, environmentNameChannelSendTimeoutMillis, defaultChannelSendTimeout)
	if channelSendTimeoutError != nil {
		return ApplicationConfiguration{}, channelSendTimeoutError
	}
	applicationConfiguration.ChannelSendTimeout = channelSendTimeout

	maximumBodySizeInBytes, maximumBodySizeInBytesError := parsePositiveIntegerSetting(environmentVariables, environmentNameMaximumBodySizeInBytes, defaultMaximumBodySizeInBytes)
	if maximumBodySizeInBytesError != nil {
		return ApplicationConfiguration{}, maximumBodySizeInBytesError
	}
	applicationConfiguration.MaximumBodySizeInBytes = maximumBodySizeInBytes

	applicationEnvironment, applicationEnvironmentError := parseStringSetting(environmentVariables, environmentNameApplicationEnvironment, defaultApplicationEnvironment)
	if applicationEnvironmentError != nil {
		return ApplicationConfiguration{}, applicationEnvironmentError
	}
	applicationConfiguration.ApplicationEnvironment = applicationEnvironment

	routingRules, routingRulesError := parseRoutingRulesSetting(environmentVariables, environmentNameRoutingRules)
	if routingRulesError != nil {
		return ApplicationConfiguration{}, routingRulesError
	}
	applicationConfiguration.RoutingRules = routingRules

	defaultDestination, defaultDestinationError := parseStringSetting(environmentVariables, environmentNameDefaultDestination, service1Destination)
	if defaultDestinationError != nil {
		return ApplicationConfiguration{}, defaultDestinationError
	}
	applicationConfiguration.DefaultDestination = defaultDestination

	trustedProxies, trustedProxiesError := parseCIDRListSetting(environmentVariables, environmentNameTrustedProxies)
	if trustedProxiesError != nil {
		return ApplicationConfiguration{}, trustedProxiesError
	}
	applicationConfiguration.TrustedProxies = trustedProxies

	rabbitMQHost, rabbitMQHostError := parseRequiredStringSetting(environmentVariables, environmentNameRabbitMQHost)
	if rabbitMQHostError != nil {
		return ApplicationConfiguration{}, rabbitMQHostError
	}
	applicationConfiguration.RabbitMQHost = rabbitMQHost

	rabbitMQPort, rabbitMQPortError := parsePortSetting(environmentVariables, environmentNameRabbitMQPort, 0)
	if rabbitMQPortError != nil {
		return ApplicationConfiguration{}, rabbitMQPortError
	}
	applicationConfiguration.RabbitMQPort = rabbitMQPort

	rabbitMQUsername, rabbitMQUsernameError := parseRequiredStringSetting(environmentVariables, environmentNameRabbitMQUsername)
	if rabbitMQUsernameError != nil {
		return ApplicationConfiguration{}, rabbitMQUsernameError
	}
	applicationConfiguration.RabbitMQUsername = rabbitMQUsername

	rabbitMQPassword, rabbitMQPasswordError := parseRequiredStringSetting(environmentVariables, environmentNameRabbitMQPassword)
	if rabbitMQPasswordError != nil {
		return ApplicationConfiguration{}, rabbitMQPasswordError
	}
	applicationConfiguration.RabbitMQPassword = rabbitMQPassword

	rabbitMQManagementURL, rabbitMQManagementURLError := parseStringSetting(environmentVariables, environmentNameRabbitMQManagementURL, "")
	if rabbitMQManagementURLError != nil {
		return ApplicationConfiguration{}, rabbitMQManagementURLError
	}
	applicationConfiguration.RabbitMQManagementURL = rabbitMQManagementURL

	rabbitMQVirtualHost, rabbitMQVirtualHostError := parseStringSetting(environmentVariables, environmentNameRabbitMQVirtualHost, DefaultRabbitMQVirtualHost)
	if rabbitMQVirtualHostError != nil {
		return ApplicationConfiguration{}, rabbitMQVirtualHostError
	}
	applicationConfiguration.RabbitMQVirtualHost = rabbitMQVirtualHost

	rabbitMQUseTLS, rabbitMQUseTLSError := parseBooleanSetting(environmentVariables, environmentNameRabbitMQUseTLS, defaultRabbitMQUseTLS)
	if rabbitMQUseTLSError != nil {
		return ApplicationConfiguration{}, rabbitMQUseTLSError
	}
	applicationConfiguration.RabbitMQUseTLS = rabbitMQUseTLS

	publishTimeout, publishTimeoutError := parsePositiveDurationFromSeconds(environmentVariables, environmentNamePublishTimeoutSeconds, defaultPublishTimeout)
	if publishTimeoutError != nil {
		return ApplicationConfiguration{}, publishTimeoutError
	}
	applicationConfiguration.PublishTimeout = publishTimeout

	rabbitMQReconnectBackoffDelays, rabbitMQReconnectBackoffDelayListError := parsePositiveDurationListFromSeconds(environmentVariables, environmentNameRabbitMQReconnectBackoffSeconds, defaultRabbitMQReconnectBackoffDelays)
	if rabbitMQReconnectBackoffDelayListError != nil {
		return ApplicationConfiguration{}, rabbitMQReconnectBackoffDelayListError
	}
	applicationConfiguration.RabbitMQReconnectBackoffDelays = rabbitMQReconnectBackoffDelays

	pipelineWorkerCount, pipelineWorkerCountError := parsePositiveIntegerSetting(environmentVariables, environmentNamePipelineWorkerCount, defaultPipelineWorkerCount)
	if pipelineWorkerCountError != nil {
		return ApplicationConfiguration{}, pipelineWorkerCountError
	}
	applicationConfiguration.PipelineWorkerCount = pipelineWorkerCount

	writeBatchSize, writeBatchSizeError := parsePositiveIntegerSetting(environmentVariables, environmentNameWriteBatchSize, defaultWriteBatchSize)
	if writeBatchSizeError != nil {
		return ApplicationConfiguration{}, writeBatchSizeError
	}
	applicationConfiguration.WriteBatchSize = writeBatchSize

	writeFlushInterval, writeFlushIntervalError := parsePositiveDurationFromSeconds(environmentVariables, environmentNameWriteFlushIntervalSeconds, defaultWriteFlushInterval)
	if writeFlushIntervalError != nil {
		return ApplicationConfiguration{}, writeFlushIntervalError
	}
	applicationConfiguration.WriteFlushInterval = writeFlushInterval

	databaseDirectoryPath, databaseDirectoryPathError := parseStringSetting(environmentVariables, environmentNameDatabaseDirectoryPath, defaultDatabaseDirectoryPath)
	if databaseDirectoryPathError != nil {
		return ApplicationConfiguration{}, databaseDirectoryPathError
	}
	applicationConfiguration.DatabaseDirectoryPath = databaseDirectoryPath

	databaseBusyTimeout, databaseBusyTimeoutError := parsePositiveDurationFromSeconds(environmentVariables, environmentNameDatabaseBusyTimeoutSeconds, defaultDatabaseBusyTimeout)
	if databaseBusyTimeoutError != nil {
		return ApplicationConfiguration{}, databaseBusyTimeoutError
	}
	applicationConfiguration.DatabaseBusyTimeout = databaseBusyTimeout

	writeRetryBackoffDelays, writeRetryBackoffDelayListError := parsePositiveDurationListFromSeconds(environmentVariables, environmentNameWriteRetryBackoffSeconds, defaultWriteRetryBackoffDelays)
	if writeRetryBackoffDelayListError != nil {
		return ApplicationConfiguration{}, writeRetryBackoffDelayListError
	}
	applicationConfiguration.WriteRetryBackoffDelays = writeRetryBackoffDelays

	deadLetterFilePath, deadLetterFilePathError := parseStringSetting(environmentVariables, environmentNameDeadLetterFilePath, defaultDeadLetterFilePath)
	if deadLetterFilePathError != nil {
		return ApplicationConfiguration{}, deadLetterFilePathError
	}
	applicationConfiguration.DeadLetterFilePath = deadLetterFilePath

	deadLetterMaximumFileSizeInBytes, deadLetterMaximumFileSizeInBytesError := parsePositiveIntegerSetting(environmentVariables, environmentNameDeadLetterMaximumFileSizeInBytes, defaultDeadLetterMaximumFileSizeInBytes)
	if deadLetterMaximumFileSizeInBytesError != nil {
		return ApplicationConfiguration{}, deadLetterMaximumFileSizeInBytesError
	}
	applicationConfiguration.DeadLetterMaximumFileSizeInBytes = deadLetterMaximumFileSizeInBytes

	spoolFilePath, spoolFilePathError := parseStringSetting(environmentVariables, environmentNameSpoolFilePath, DefaultSpoolFilePath)
	if spoolFilePathError != nil {
		return ApplicationConfiguration{}, spoolFilePathError
	}
	applicationConfiguration.SpoolFilePath = spoolFilePath

	corsAllowedOrigins, corsAllowedOriginsError := parseStringListSetting(environmentVariables, environmentNameCorsAllowedOrigins, nil)
	if corsAllowedOriginsError != nil {
		return ApplicationConfiguration{}, corsAllowedOriginsError
	}
	applicationConfiguration.CorsAllowedOrigins = corsAllowedOrigins

	metricsThroughputWindow, metricsThroughputWindowError := parsePositiveDurationFromSeconds(environmentVariables, environmentNameMetricsThroughputWindowSeconds, defaultMetricsThroughputWindow)
	if metricsThroughputWindowError != nil {
		return ApplicationConfiguration{}, metricsThroughputWindowError
	}
	applicationConfiguration.MetricsThroughputWindow = metricsThroughputWindow

	metricsBacklogTimeout, metricsBacklogTimeoutError := parsePositiveDurationFromSeconds(environmentVariables, environmentNameMetricsBacklogTimeoutSeconds, defaultMetricsBacklogTimeout)
	if metricsBacklogTimeoutError != nil {
		return ApplicationConfiguration{}, metricsBacklogTimeoutError
	}
	applicationConfiguration.MetricsBacklogTimeout = metricsBacklogTimeout

	shutdownGracePeriod, shutdownGracePeriodError := parsePositiveDurationFromSeconds(environmentVariables, environmentNameShutdownGracePeriodSeconds, defaultShutdownGracePeriod)
	if shutdownGracePeriodError != nil {
		return ApplicationConfiguration{}, shutdownGracePeriodError
	}
	applicationConfiguration.ShutdownGracePeriod = shutdownGracePeriod

	validationError := validateConsumerBatchFitsBuffer(applicationConfiguration)
	if validationError != nil {
		return ApplicationConfiguration{}, validationError
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

	parsedSettingValues := make([]time.Duration, 0)
	for rawSettingEntry := range strings.SplitSeq(rawSettingValue, ",") {
		trimmedSettingEntry := strings.TrimSpace(rawSettingEntry)
		if trimmedSettingEntry == "" {
			continue
		}
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

// Parses the comma-separated CIDR prefixes used as trusted proxies.
func parseCIDRListSetting(environmentVariables map[string]string, settingName string) ([]string, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		return nil, nil
	}

	parsedSettingValues := make([]string, 0)
	for rawSettingEntry := range strings.SplitSeq(rawSettingValue, ",") {
		trimmedSettingEntry := strings.TrimSpace(rawSettingEntry)
		if trimmedSettingEntry == "" {
			continue
		}
		if _, parseError := netip.ParsePrefix(trimmedSettingEntry); parseError != nil {
			return nil, fmt.Errorf("environment variable %s contains value %q, which is not a valid CIDR prefix: %w", settingName, trimmedSettingEntry, parseError)
		}
		parsedSettingValues = append(parsedSettingValues, trimmedSettingEntry)
	}
	return parsedSettingValues, nil
}

// Returns the comma-separated strings for the setting, falling back to the default when empty.
func parseStringListSetting(environmentVariables map[string]string, settingName string, defaultValue []string) ([]string, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		return defaultValue, nil
	}

	parsedSettingValues := make([]string, 0)
	for rawSettingEntry := range strings.SplitSeq(rawSettingValue, ",") {
		trimmedSettingEntry := strings.TrimSpace(rawSettingEntry)
		if trimmedSettingEntry != "" {
			parsedSettingValues = append(parsedSettingValues, trimmedSettingEntry)
		}
	}
	return parsedSettingValues, nil
}

// Returns the string for the setting, falling back to the default when missing or empty.
func parseStringSetting(environmentVariables map[string]string, settingName string, defaultValue string) (string, error) {
	settingValue := environmentVariables[settingName]
	if settingValue == "" {
		return defaultValue, nil
	}
	return settingValue, nil
}

// Returns the boolean for the setting, falling back to the default when missing or empty.
func parseBooleanSetting(environmentVariables map[string]string, settingName string, defaultValue bool) (bool, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		return defaultValue, nil
	}

	parsedSettingValue, parsingError := strconv.ParseBool(rawSettingValue)
	if parsingError != nil {
		return false, fmt.Errorf("environment variable %s has value %q, which is not a valid boolean: %w", settingName, rawSettingValue, parsingError)
	}

	return parsedSettingValue, nil
}

// Returns the duration for the setting, interpreted as seconds, falling back to the default when empty.
func parsePositiveDurationFromSeconds(environmentVariables map[string]string, settingName string, defaultValue time.Duration) (time.Duration, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		return defaultValue, nil
	}
	seconds, err := strconv.Atoi(rawSettingValue)
	if err != nil {
		return 0, fmt.Errorf("environment variable %s has value %q, which is not a valid integer: %w", settingName, rawSettingValue, err)
	}
	if seconds <= 0 {
		return 0, fmt.Errorf("environment variable %s must be a positive integer, but shows %d", settingName, seconds)
	}
	return time.Duration(seconds) * time.Second, nil
}

// Returns the duration for the setting, interpreted as milliseconds, falling back to the default when empty.
func parsePositiveDurationFromMilliseconds(environmentVariables map[string]string, settingName string, defaultValue time.Duration) (time.Duration, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		return defaultValue, nil
	}
	millis, err := strconv.Atoi(rawSettingValue)
	if err != nil {
		return 0, fmt.Errorf("environment variable %s has value %q, which is not a valid integer: %w", settingName, rawSettingValue, err)
	}
	if millis <= 0 {
		return 0, fmt.Errorf("environment variable %s must be a positive integer, but shows %d", settingName, millis)
	}
	return time.Duration(millis) * time.Millisecond, nil
}

// Returns the comma-separated durations for the setting, interpreted as seconds, falling back to the default when empty.
func parsePositiveDurationListFromSeconds(environmentVariables map[string]string, settingName string, defaultValue []time.Duration) ([]time.Duration, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		return defaultValue, nil
	}
	parsedSettingValues := make([]time.Duration, 0)
	for rawSettingEntry := range strings.SplitSeq(rawSettingValue, ",") {
		trimmedSettingEntry := strings.TrimSpace(rawSettingEntry)
		if trimmedSettingEntry == "" {
			continue
		}
		seconds, parsingError := strconv.Atoi(trimmedSettingEntry)
		if parsingError != nil {
			return nil, fmt.Errorf("environment variable %s contains value %q, which is not a valid integer: %w", settingName, trimmedSettingEntry, parsingError)
		}
		if seconds <= 0 {
			return nil, fmt.Errorf("environment variable %s must contain only positive integers, but includes %q", settingName, trimmedSettingEntry)
		}
		parsedSettingValues = append(parsedSettingValues, time.Duration(seconds)*time.Second)
	}
	return parsedSettingValues, nil
}

// Parses comma-separated key=value routing rules.
func parseRoutingRulesSetting(environmentVariables map[string]string, settingName string) (map[string]string, error) {
	rawSettingValue := environmentVariables[settingName]
	if rawSettingValue == "" {
		return map[string]string{}, nil
	}
	rules := make(map[string]string)
	for pair := range strings.SplitSeq(rawSettingValue, ",") {
		trimmed := strings.TrimSpace(pair)
		if trimmed == "" {
			continue
		}

		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("environment variable %s contains invalid rule %q, expected key=value", settingName, trimmed)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" || value == "" {
			return nil, fmt.Errorf("environment variable %s contains empty key or value in rule %q", settingName, trimmed)
		}
		rules[key] = value
	}
	return rules, nil
}
