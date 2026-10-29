# Configuration reference (key variables)

All settings are read from `.env` via `godotenv`; process environment overrides `.env`. No external JSON config is used.

**Application**

- `HTTP_PORT` : server port (default 8080)
- `APP_ENV` : environment name written into each stored row (default `production`)
- `MAX_BODY_BYTES` : max request body (default 1 048 576)
- `TRUSTED_PROXIES` : comma‑separated CIDRs allowed to forward `X‑Forwarded‑For` (empty = ignore header)

**Rate limiting**

- `RATE_LIMIT` : requests/s per IP (default 1000)
- `RATE_LIMIT_CLEANUP_INTERVAL_IN_SECONDS` : idle entry eviction (default 60)

**Ingestion buffer & pipeline**

- `RAW_LOGS_BUFFER` : channel capacity; full → 503 (default 10 000)
- `CHANNEL_SEND_TIMEOUT_IN_MILLISECONDS` : per‑log wait for space (default 100)
- `PIPELINE_WORKER_COUNT` : enrichment/pipeline workers (default 4)
- `CONSUMER_BATCH_SIZE` : logs per worker pass (default 50)
- `ROUTING_RULES` : `service=destination` pairs (empty = use `DEFAULT_DESTINATION`)
- `DEFAULT_DESTINATION` : fallback destination (default `service1`)

**RabbitMQ**

- `RABBITMQ_HOST`, `PORT`, `USER`, `PASS` : required
- `VHOST` : virtual host (default `/`)
- `PUBLISH_TIMEOUT_IN_SECONDS` : confirm wait (default 5)
- `RABBITMQ_RECONNECT_BACKOFFS_IN_SECONDS` : `1,5,10,30`
- `RABBITMQ_MGMT_URL` : management API URL (optional)

**Storage**

- `DB_DIR` : SQLite directory (default `./data`)
- `DB_BUSY_TIMEOUT_IN_SECONDS` : busy retry (default 5)
- `WRITE_BATCH_SIZE` : buffered logs per destination (default 100)
- `WRITE_FLUSH_INTERVAL_IN_SECONDS` : time trigger (default 1)
- `RETRY_BACKOFFS_IN_SECONDS` : write retry delays `1,5,10`; after last retry → dead‑letter
- `DEAD_LETTER_PATH` : NDJSON file (default `logs-failed.json`)
- `DEAD_LETTER_MAX_BYTES` : rotation size (default 10 485 760 ≈ 10 MB)
- `SPOOL_PATH` : durable spool file (`ingestion-spool.log`)

**Metrics & shutdown**

- `METRICS_THROUGHPUT_WINDOW_IN_SECONDS` : throughput window (default 60)
- `METRICS_BACKLOG_TIMEOUT_IN_SECONDS` : mgmt‑API call budget (default 2)
- `SHUTDOWN_GRACE_PERIOD_IN_SECONDS` : shared deadline (default 30)
- `CORS_ALLOWED_ORIGINS` : allowed event origins (empty = deny)

**Fixed names (not overridable)** : broker topology & DB file names (`exchange_service*`, `queue_service*`, `logs_service*.db`).

**Coverage audit** : every env var in `internal/config/config.go` appears in `configs/.env.example`; no unused settings; credentials only from config/environment.