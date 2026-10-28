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

All 148 tests across 15 packages should pass. No broker, database files or environment variables are required for unit tests. The integration tests in `cmd/server` skip themselves when the broker is unavailable.

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
| `internal/handler` | HTTP handlers: log ingestion and JSON metrics snapshot (`POST /logs`, `GET /metrics.json`) |
| `internal/ingest` | Log validation and client IP resolution |
| `internal/spool` | Durable write-ahead spool: append+fsync before `202`, ack after storage/dead-letter, unacked replay on startup, compaction |
| `cmd/server` | Composition root: route registration, exact home path and 404/405 behaviour, ingestion through the fallback into SQLite, rate limiting, the metrics counters, the graceful shutdown drain, the wait for the running requests and the dead-letter recovery of the logs the pipeline could not process. The integration suite in `cmd/server/integration_test.go` covers the full pipeline, the broker outage fallback, the SQLite batching triggers, the retries with the dead-letter contents and rotation, and the graceful shutdown with a real broker |
| `internal/model` | Contract tests for log levels and destinations |
| `internal/metrics` | Snapshot counters, error rate, throughput window, fallback backlog counter |
| `internal/pipeline` | Batch consumer, bounded raw-logs queue, cancellation drain, recovery of the logs left in the queue, dead-lettering of the logs that cannot be routed or published |
| `internal/queue` | Publisher, reconnect backoff, fallback activation, rejection of the logs that arrive after the close |
| `internal/queue/consumer` | RabbitMQ delivery handling, ack-after-store, requeue, fallback drain, consumer supervisor reconnect backoff |
| `cmd/loadtest` | Load generator helpers: source IP pool and batched request body building |
| `internal/ratelimit` | Per-IP limiter, idle cleanup and its stop path, middleware rejection |
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
| Broker in `cmd/server` unit tests | Not needed. The wired application is built with an unreachable broker address so the in-memory fallback path is exercised |
| Broker in `cmd/server` integration tests | Two of the five need it and skip cleanly when it is missing. The other three always run against an unreachable broker |
| Docker | Not needed for unit tests; only for manual end-to-end runs |
| Real sleeps | Not used. Time is injected via `WaitForRetryDelay`, `waitForReconnectDelay`, and clock functions. The integration tests poll a condition on a ticker until a deadline |

## Integration tests

The integration suite lives in `cmd/server/integration_test.go` and wires the whole application exactly like `main` does.

| Test | What it proves | Broker |
|------|----------------|--------|
| `TestIntegration_FullPipelineStoresTheEnrichedAndRoutedLogsInTheDestinationDatabases` | A log is enriched, routed to its destination database and stored with the enrichment fields | Required |
| `TestIntegration_RabbitMQOutageStoresTheAcceptedLogsThroughTheFallback` | An unreachable broker does not lose the accepted logs; the fallback stores them and no dead letter is written | Not needed |
| `TestIntegration_SQLiteBatchingFlushesAtTheBatchSizeAndAtTheFlushInterval` | The batch size trigger flushes 100 logs without waiting for the interval, and the interval trigger flushes a single log on time | Not needed |
| `TestIntegration_RetriesAndDeadLettersTheLogsThatCannotBeStored` | A failing store is retried three times, then the dead letter carries the log, the error message, the retry count and the failure time, and the file is rotated | Not needed |
| `TestIntegration_GracefulShutdownStoresEveryAcceptedLog` | Every accepted log ends up stored, even when the shutdown happens while the logs are still in the broker queues | Required |

Run them on their own:

```bash
go test ./cmd/server -race -run TestIntegration -v
```

The broker tests read the broker settings from the environment or from the `.env` file at the repository root, and they skip themselves when the broker cannot be reached:

```bash
docker compose up -d
go test ./cmd/server -race -run TestIntegration -v
```

The broker tests purge the three destination queues before they start, so do not run them against a broker a running instance of the server uses.

## Test conventions

- Table-driven tests with case names that read as sentences.
- Test names use the `TestFunctionName_Behaviour` pattern.
- Fakes are plain structs or functions matching the seam type; no mocking libraries.
- `t.Parallel()` on every independent test.
- `t.Helper()` on helper functions.
- Time-based behaviour uses injected clocks or channels; no `time.Sleep`.

## CI

`.github/workflows/ci.yml` runs on every push and pull request to `main`:

1. Checkout and Go 1.26.3 setup.
2. `gofmt -l .` fails if any file is unformatted.
3. `go vet ./...` fails on vet warnings.
4. `go test ./... -race` fails on any test failure or data race.

CI starts a RabbitMQ service container and passes the credentials to the test step, so the integration tests run against a real broker there. The credentials come from the `RABBITMQ_USER` and `RABBITMQ_PASS` repository secrets. External pull requests (forks) cannot read the secrets: their credential check is skipped and the broker tests skip themselves. On trusted events the workflow requires the secrets and sets `CI_REQUIRE_BROKER_TESTS=1`, so a missing credential or an unreachable broker fails the build with a readable message instead of silently skipping the coverage. Locally the broker tests still skip themselves when the broker cannot be reached.

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

### Manual verification with curl

Start the server (`go run ./cmd/server`) with RabbitMQ running, then run each test below.

#### Test 1 — Valid batch ingestion (expect `202 Accepted`)

```bash
curl -s -i -X POST localhost:8080/logs \
  -H 'Content-Type: application/json' \
  -d '[{"timestamp":"2026-10-28T10:00:00Z","service":"claims","level":"INFO","message":"test log 1"},{"timestamp":"2026-10-28T10:00:01Z","service":"billing","level":"ERROR","message":"test error"}]'
```

#### Test 2 — Partial batch (some valid, some invalid) (expect `202 Accepted` with `rejected` array)

```bash
curl -s -i -X POST localhost:8080/logs \
  -H 'Content-Type: application/json' \
  -d '[{"timestamp":"2026-10-28T10:00:00Z","service":"claims","level":"INFO","message":"valid"},{"timestamp":"not-a-time","service":"billing","level":"ERROR","message":"bad timestamp"}]'
```

#### Test 3 — All entries invalid (expect `400 Bad Request`)

```bash
curl -s -i -X POST localhost:8080/logs \
  -H 'Content-Type: application/json' \
  -d '[{"timestamp":"bad","service":"x","level":"BAD","message":""},{"timestamp":"also bad","service":"y","level":"WORSE","message":""}]'
```

#### Test 4 — Wrong content type (expect `415 Unsupported Media Type`)

```bash
curl -s -i -X POST localhost:8080/logs -H 'Content-Type: text/plain' -d 'hello'
```

#### Test 5 — Read metrics snapshot

```bash
curl -s localhost:8080/metrics.json | python3 -m json.tool
```

#### Test 6 — Server-sent events stream

```bash
timeout 3 curl -N -s localhost:8080/metrics/events
```

#### Test 7 — Rate limiting (rapid requests)

Stop the running server, then start it with a lower request limit:

```bash
RATE_LIMIT=2 go run ./cmd/server
```

In another terminal, send requests rapidly. A request exceeding two requests per second from the same IP should return `429 Too Many Requests` (the limiter replenishes over time):

```bash
for i in $(seq 1 5); do
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST localhost:8080/logs -H 'Content-Type: application/json' -d '[{"timestamp":"2026-10-28T10:00:00Z","service":"test","level":"INFO","message":"rate test"}]')
  echo "Request $i: $code"
done
curl -s localhost:8080/metrics.json | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Rate limited: {d[\"rate_limited_requests\"]}, Total: {d[\"total_logs_received\"]}')"
```

#### Test 8 — Verify SQLite storage

```bash
sqlite3 data/logs_service1.db "SELECT service, level, COUNT(*) FROM logs GROUP BY service, level ORDER BY service, level;"
```

#### Test 9 — Spool file durability

```bash
ls -la logs/ingestion-spool.log logs/ingestion-spool.log.acked
```

#### Test 10 — Graceful shutdown

```bash
kill -TERM <server-pid>
# Watch the server log for "shutdown complete"
```

To stop:

```bash
docker compose down
```
