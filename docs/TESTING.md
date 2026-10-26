# Testing Guide

How to run and verify the log ingestion engine test suite.

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Go | 1.26.3 | Matches `go.mod`; any later patch works |
| Docker | any recent | Only needed for local broker integration, not unit tests |
| RabbitMQ | any | Not required for unit tests |

## Quick start

Run the full suite with the race detector (the same command CI uses):

```bash
go test ./... -race
```

All 129 tests across 13 packages should pass. No broker, database files or environment variables are required for unit tests.

## Static checks before committing

```bash
gofmt -l .          # prints nothing when all files are formatted
go vet ./...        # no warnings
go test ./... -race # all tests pass
```

These are the exact gates in `.github/workflows/ci.yml`.

## Running tests by package

| Package | What it covers |
|---------|---------------|
| `internal/config` | Environment parsing, file loading, validation, defaults |
| `internal/enrich` | Enrichment fields, microsecond timestamp format, IP fallback |
| `internal/handler` | HTTP handlers: log ingestion, metrics snapshot, root/home page, management-API backlog, dashboard page and assets |
| `internal/ingest` | Log validation and client IP resolution |
| `cmd/server` | Composition root: route registration, exact home path and 404/405 behaviour, ingestion through the fallback into SQLite, rate limiting and the metrics counters |
| `internal/model` | Contract tests for log levels and destinations |
| `internal/metrics` | Snapshot counters, error rate, throughput window, fallback backlog counter |
| `internal/pipeline` | Batch consumer, bounded raw-logs queue, cancellation drain |
| `internal/queue` | Publisher, reconnect backoff, fallback activation |
| `internal/queue/consumer` | RabbitMQ delivery handling, ack-after-store, requeue, fallback drain, consumer supervisor reconnect backoff |
| `internal/ratelimit` | Per-IP limiter, idle cleanup, middleware rejection |
| `internal/router` | Service routing rules, defaults, file loading |
| `internal/storage` | Batch writer, SQLite store, dead-letter rotation, replay |

### Target a single package

```bash
go test ./internal/storage -race -v
```

### Target a single test

```bash
go test ./internal/queue/consumer -race -run TestQueueConsumer_AcknowledgesOnlyAfterStorageCompletes -v
```

### Run without cached results

```bash
go test ./... -count=1 -race
```

## What the tests do and do not need

| Aspect | Detail |
|--------|--------|
| RabbitMQ broker | Not needed. Publisher and consumer tests use fake connect/publish/subscriber seams |
| SQLite files | Tests create temporary files under `t.TempDir()` |
| `.env` file | Not needed for unit tests. Config tests build their own environment maps or use temp files |
| Network ports | None. HTTP handler tests use `httptest.NewRecorder` |
| Broker in `cmd/server` tests | Not needed. The wired application is built with an unreachable broker address so the in-memory fallback path is exercised |
| Docker | Not needed for unit tests; only for manual end-to-end runs |
| Real sleeps | Not used. Time is injected via `WaitForRetryDelay`, `waitForReconnectDelay`, and clock functions |

## Test conventions

Follow the rules in `AGENT.md` section 9:

- Table-driven tests with case names that read as sentences.
- Test names use the `TestFunctionName_Behaviour` pattern.
- Fakes are plain structs or functions matching the seam type; no mocking libraries.
- `t.Parallel()` on every independent test.
- `t.Helper()` on helper functions.
- Time-based behaviour uses injected clocks or channels; no `time.Sleep`.

## CI

`.github/workflows/ci.yml` runs on every push and pull request to `main`:

1. Checkout and Go 1.26.3 setup.
2. `gofmt -l .` — fails if any file is unformatted.
3. `go vet ./...` — fails on vet warnings.
4. `go test ./... -race` — fails on any test failure or data race.

## Docker broker for manual integration

The unit tests never touch a broker. To run the application end-to-end:

```bash
# start the broker (requires RABBITMQ_USER and RABBITMQ_PASS in .env)
docker compose up -d

# start the server
go run ./cmd/server

# send a test log
curl -s -X POST http://localhost:8080/logs \
  -H 'Content-Type: application/json' \
  -d '[{"timestamp":"2026-10-26T12:00:00Z","service":"claims","level":"INFO","message":"test"}]'
```

To stop:

```bash
docker compose down
```
