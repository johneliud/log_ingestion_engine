# Configuration Reference

Every runtime setting of the log ingestion engine. Each entry in the table below is defined as a constant in `internal/config/config.go` and documented with its default in `configs/.env.example`. The process environment always wins over the configuration file, and a missing configuration file is silently ignored.

## Where settings come from

1. The process environment (`os.Environ`).
2. The configuration file, `.env` by default, overridable with `CONFIG_FILE_PATH`; file settings never override the process environment.

`RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER` and `RABBITMQ_PASS` are the only required settings; everything else falls back to the default shown below. No credential is ever hardcoded in the source: the binary reads them from the configuration file or the environment, and the compose stack injects the same variables into RabbitMQ.

## Application settings

| Variable | What it does | Default |
|----------|--------------|---------|
| `CONFIG_FILE_PATH` | Where the optional configuration file loads from | `.env` |
| `HTTP_PORT` | TCP port `POST /logs` and `GET /metrics.json` listen on (1024–65535) | `8080` |
| `APP_ENV` | Environment name embedded by the enricher into each stored row (the enricher writes it into `env` for every stored row) | `production` |
| `MAX_BODY_BYTES` | Largest accepted request body in bytes; larger batches fail with the body-too-large error | `1048576` |
| `TRUSTED_PROXIES` | Comma-separated CIDR prefixes allowed to forward `X-Forwarded-For`; empty means the header is ignored and the socket address decides | *(empty)* |

## Rate limiting

| Variable | What it does | Default |
|----------|--------------|---------|
| `RATE_LIMIT` | Allowed requests per second per source IP (the limit counts requests, not logs) | `1000` |
| `RATE_LIMIT_CLEANUP_INTERVAL` | How often the idle per-IP limiter entries are evicted | `1m` |

## Ingestion buffer and pipeline

| Variable | What it does | Default |
|----------|--------------|---------|
| `RAW_LOGS_BUFFER` | Capacity of the bounded `raw_logs` channel; when it is full, `POST /logs` answers `503 ingestion overloaded` | `10000` |
| `CHANNEL_SEND_TIMEOUT` | How long a handler is allowed to wait for buffer space per pushed log | `100ms` |
| `PIPELINE_WORKER_COUNT` | Workers that enrich, route and publish, each with its own publisher confirm loop | `4` |
| `CONSUMER_BATCH_SIZE` | Raw logs one pipeline worker takes in a pass; must not exceed `RAW_LOGS_BUFFER` | `50` |
| `ROUTING_CONFIG_PATH` | JSON map of `service → destination` routing rules; a missing file falls back to the default destination | `configs/routing.json` |
| `DEFAULT_DESTINATION` | Destination used when a service has no routing rule (or the rule file is missing) | `service1` |

## RabbitMQ

| Variable | What it does | Default |
|----------|--------------|---------|
| `RABBITMQ_HOST` | Broker hostname (required) | *(required)* |
| `RABBITMQ_PORT` | Broker AMQP port (required; validated as a port) | *(required)* |
| `RABBITMQ_USER` | Broker username (required; never hardcoded) | *(required)* |
| `RABBITMQ_PASS` | Broker password (required; never hardcoded) | *(required)* |
| `RABBITMQ_VHOST` | Virtual host the application connects to and fetches the queue backlog from | `/` |
| `RABBITMQ_USE_TLS` | When `true`, the publisher and consumer URI uses `amqps` | `false` |
| `PUBLISH_TIMEOUT` | How long a publisher waits for one log to be confirmed before falling back | `5s` |
| `RABBITMQ_RECONNECT_BACKOFFS` | Comma-separated retry delays for reconnects; the last value repeats | `1s,5s,10s,30s` |
| `RABBITMQ_MGMT_URL` | Base URL of the management API used by `queue_backlog`; unset disables the live queue sum | *(empty)* |

## Storage

| Variable | What it does | Default |
|----------|--------------|---------|
| `DB_DIR` | Directory holding one SQLite database per destination | `./data` |
| `DB_BUSY_TIMEOUT` | SQLite busy timeout while a write contention is retried | `5s` |
| `WRITE_BATCH_SIZE` | Queued logs buffered per destination before a write flush triggers | `100` |
| `WRITE_FLUSH_INTERVAL` | Maximum time a partial buffer waits before flushing on the time trigger | `1s` |
| `RETRY_BACKOFFS` | Comma-separated write retry delays, one retry attempt per entry; the log is dead-lettered after the last retry fails | `1s,5s,10s` |
| `DEAD_LETTER_PATH` | Newline-delimited JSON file for logs whose write retries were exhausted | `logs-failed.json` |
| `DEAD_LETTER_MAX_BYTES` | Dead letter file size after which it is rotated to `NAME.1` | `10485760` |
| `SPOOL_PATH` | Durable write-ahead spool file for accepted logs; written and fsynced before `POST /logs` returns `202`, replayed on startup to recover any unacknowledged entry | `ingestion-spool.log` |

## Metrics, dashboard and shutdown

| Variable | What it does | Default |
|----------|--------------|---------|
| `METRICS_THROUGHPUT_WINDOW` | Window over which `GET /metrics.json` throughput is averaged | `1m` |
| `METRICS_BACKLOG_TIMEOUT` | Time budget for one management-API call when refreshing `queue_backlog` | `2s` |
| `SHUTDOWN_GRACE_PERIOD` | Single deadline shared by every graceful shutdown phase | `30s` |
| `CORS_ALLOWED_ORIGINS` | Comma-separated list of origins allowed to consume `GET /metrics/events`. Empty allows all origins | *(empty)* |

## Fixed names (not overridable)

These constants keep the broker topology and the database files aligned across every module; they are not environment settings and have no default in `.env.example`:

| Constant | Value |
|----------|-------|
| `RoutingKeyLogWrite` | `log.write` |
| `ExchangeService1Name` … `ExchangeService3Name` | `exchange_service1` … `exchange_service3` |
| `QueueService1Name` … `QueueService3Name` | `queue_service1` … `queue_service3` |
| `DatabaseFileService1Name` … `DatabaseFileService3Name` | `logs_service1.db` … `logs_service3.db` |

## Coverage audit

- Every env var parsed in `internal/config/config.go` is present in `configs/.env.example`.
- Every parsed setting feeds a configuration field that is consumed by non-test code (`internal/*` or `cmd/*`); there are no unused settings to remove.
- No code outside `internal/config` reads the process environment directly (`os.Getenv` / `os.LookupEnv` are only used by the config package), so credentials cannot be bypassed.
- Credentials appear in source only as expectations inside test files; runtime values come from the configuration file or environment.
