# Log Ingestion Engine

A Go service that receives batched JSON logs, validates, enriches, routes via RabbitMQ, stores in SQLite, and exposes metrics.

```mermaid
flowchart TB
    client(["Clients"])

    subgraph ingress["HTTP ingress"]
        direction TB
        rateLimit["Rate limiter<br/>per IP, in-memory"]
        logsEndpoint["POST /logs"]
        validate["Validate batch<br/>body size + per-entry errors"]
    end

    spool[("Write-ahead spool<br/>fsync before ack")]
    rawQueue[("raw_logs<br/>bounded channel")]

    subgraph processing["Pipeline worker pool"]
        direction TB
        enrich["Enrich<br/>received_at · source_ip · environment"]
        route["Route<br/>service → destination rules"]
    end

    subgraph publishing["Publisher"]
        direction TB
        rabbitPublisher["RabbitMQ publisher<br/>persistent + confirms<br/>5s publish timeout<br/>auto-reconnect with backoff"]
        fallback[("Bounded in-memory fallback<br/>used while disconnected")]
    end

    broker[("RabbitMQ<br/>exchange_service1..3<br/>queue_service1..3<br/>routing key: log.write")]

    subgraph delivery["Consumers and storage"]
        direction TB
        supervisor["Supervisor<br/>one connection<br/>declares topology, reconnects"]
        amqpConsumer["AMQP consumers<br/>one connection per queue<br/>ack after stored"]
        fallbackConsumer["Fallback consumer"]
        batchWriter["Batch writer<br/>per-destination buffers<br/>retry with backoff"]
        sqlite[("SQLite<br/>logs_service1.db<br/>logs_service2.db<br/>logs_service3.db")]
        deadLetters[("Dead-letter NDJSON<br/>one shared file<br/>rotated by size")]
        replay["cmd/replay"]
    end

    client --> rateLimit --> logsEndpoint --> validate --> spool --> rawQueue
    rawQueue --> enrich --> route --> rabbitPublisher
    rabbitPublisher -->|"connected"| broker
    rabbitPublisher -->|"publish fails"| fallback
    fallback -.->|"fallback full: dead letter"| deadLetters
    broker -->|"deliver"| supervisor
    supervisor --> amqpConsumer
    fallback -->|"stored directly, never re-published"| fallbackConsumer
    amqpConsumer --> batchWriter
    fallbackConsumer --> batchWriter
    batchWriter -->|"stored"| sqlite
    batchWriter -->|"retries exhausted"| deadLetters
    deadLetters -.-> replay
    replay -.->|"replay"| sqlite
```

## Architecture

- **HTTP ingress** : rate‑limited `POST /logs` → validation → write‑ahead spool (fsync before 202) → bounded `raw_logs` channel (10 000 entries).
- **Pipeline** : N configurable workers enrich & route each log, then publish to RabbitMQ with per‑log confirm (default 5 s timeout).
- **Publisher** : single process, persistent connection, per‑log confirms, in‑memory fallback (10 000) when broker down.
- **Consumers** : one AMQP connection per destination, ack after store into per‑destination SQLite (`logs_service*.db`).
- **Batch writer** : one sink goroutine, per‑destination buffers, exponential retry; writes to SQLite; dead‑letter sink routes exhausted retries to a shared NDJSON file (rotated by size).
- **Spool** : durable log; every accepted batch is fsynced before the handler returns 202. Unacked entries are replayed on restart.
- **Dead‑letter** : single writer; entries exceeding `DEAD_LETTER_MAX_BYTES` (default 10 MB) are rotated (`*.1.json`, `*.2.json`, …).
- **Metrics** : in‑memory counters; `GET /metrics.json` snapshot; backlog refreshed from RabbitMQ management API every 2 s; event stream every 200 ms.
- **Frontend** : React + Vite, Chart.js, Space Grotesk from Google Fonts CDN, Mermaid diagram.

## Key decisions

- Spool fsync on the critical path guarantees 202 durability; it adds latency but prevents lost 202s.
- Publisher confirm loop is the throughput bottleneck; scaling workers helps up to ~11‑12 for 1 k logs/s.
- Bounded `raw_logs` channel back‑pressures into 503s when publishing slows; all‑or‑nothing at batch granularity.
- Graceful shutdown (30 s default): drain pipeline first; only if the period expires are remaining logs dead‑lettered and acked in the spool.
- `429` responses include a `Retry‑After` header (ceil seconds, min 1).
- Retries generate new `log_id`s, so a retried batch and a replayed spool entry are stored as distinct rows.
- Spool compaction only when every spooled ID has been acknowledged; under continuous traffic this is rare, so the spool can grow.

## Quickstart

```bash
git clone https://github.com/johneliud/log_ingestion_engine.git
cd log_ingestion_engine
cp configs/.env.example .env       # fill RABBITMQ_USER / RABBITMQ_PASS
go run ./cmd/server              # defaults to :8080
# frontend
cd frontend && npm install && npm run dev
```

## Graceful shutdown (30 s default)

1. HTTP server stops, waits for in‑flight requests.
2. Shutdown waits for running requests to finish.
3. Pipeline workers drain `raw_logs` until completion **or** grace period expires.
4. If the period expires, leftover logs are enriched, routed, written to the dead‑letter file, and acked in the spool.
5. Storage components are cancelled and awaited.
6. Batch writer flushes all destination buffers to SQLite; dead‑letter writer drains its queue.

## Rate limiting

- `RATE_LIMIT` requests/s per IP (default 1000).
- When a bucket is empty the request gets `429` with a `Retry‑After` header (whole seconds, min 1).
- Counters are in‑memory; buckets refill continuously.

## Tests

- 172 tests across 15 packages (`go test ./...`).
- Integration suite (`go test ./cmd/server -race -run TestIntegration`) exercises the full pipeline, fallback, shutdown, and dead‑letter rotation.
- Unit tests per package cover handler, spool, storage, pipeline, queue, ratelimit, router, metrics, model, ingest.

## Known limitations

- Single instance only (process‑local rate limiter, metrics, queue backlog).
- Metrics not persisted across restarts.
- No authentication on `POST /logs`.
- Credentials from `.env` / environment.
- No built‑in alerting.
- `queue_backlog` accuracy depends on RabbitMQ management API availability.
