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

