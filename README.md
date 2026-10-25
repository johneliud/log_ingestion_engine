# Log Ingestion Engine

[![CI](https://github.com/johneliud/log_ingestion_engine/actions/workflows/ci.yml/badge.svg)](https://github.com/johneliud/log_ingestion_engine/actions/workflows/ci.yml)

A Go service that accepts logs over HTTP, validates and enriches them, routes them through RabbitMQ, stores them in SQLite and reports metrics.

## Clone

```bash
git clone https://github.com/johneliud/log_ingestion_engine.git
cd log_ingestion_engine
```

## Prerequisites

- Go 1.26 or newer (`go version`)
- Docker (only for RabbitMQ during development)

## Setup

1. Copy `configs/.env.example` to `.env` and replace the credential placeholders:

   ```bash
   cp configs/.env.example .env
   ```

Go dependencies resolve from `go.mod` automatically on the first build.

## Running

1. `go run ./cmd/server`. The server listens on `HTTP_PORT` (default `8080`) and shuts down cleanly on interrupt.
2. Confirm it is up with `curl -s -i localhost:8080`.
3. Configuration auto-loading: The file path is `.env` in the working directory overridable with `CONFIG_FILE_PATH`; values already set in the environment always win.

All settings come from environment variables with documented defaults in `configs/.env.example`; RabbitMQ credentials are required and the server fails fast with a readable error when a required value is missing or invalid including a malformed configuration file (a missing file is simply ignored).

## Client IP resolution

Client IP is taken from the first entry of `X-Forwarded-For` (trimmed, validated as an IP, ignoring an empty or invalid first entry) when a trusted proxy forwarded the request, otherwise from the socket address host, otherwise `"unknown"`. `X-Forwarded-For` is client-controlled and only honored when the request comes from a configured trusted proxy; without one, the socket address host is used. The same function is used by the rate-limit middleware and for capturing `source_ip`. All `"unknown"` callers share one bucket. The limiter is in-memory and single-instance (lost on restart).

## Dead letters and replay

After the configured write retries are exhausted (`RETRY_BACKOFFS`, default `1s,5s,10s`), the affected logs are appended to `DEAD_LETTER_PATH` (default `logs-failed.json`) as newline-delimited JSON containing the original log, the error message and the failure timestamp. A single writer goroutine serializes file I/O. When adding an entry would exceed `DEAD_LETTER_MAX_BYTES` (default 10 MB), the file is rotated to `logs-failed.1.json`, `logs-failed.2.json`, and so on.

Replay a dead letter file back into the destination SQLite databases (the logs are already enriched and routed):

```bash
go run ./cmd/replay                    # replays DEAD_LETTER_PATH
go run ./cmd/replay logs-failed.1.json # replays a rotated file
```

Replay stops at the first unreadable line so a corrupt file is never silently skipped. The command does not delete or truncate the replayed file; remove it after verifying the rows.

## RabbitMQ outages and reconnects

When a publish fails or the broker connection drops, the publisher logs a warning, marks itself disconnected and writes subsequent logs to a bounded in-memory fallback instead of blocking ingestion. The publisher retries the connection automatically using `RABBITMQ_RECONNECT_BACKOFFS` (default `1s,5s,10s,30s`, reusing the last delay after the list is exhausted) and resumes publishing once the broker returns.

Fallback logs are not re-published after reconnecting. They stay in the fallback channel and the fallback consumer stores them directly in SQLite while new logs go to RabbitMQ, so each accepted log follows exactly one path: broker or fallback. If the fallback fills before the broker returns, `Publish` reports and logs an error rather than dropping the log silently; during a sustained outage with more than the fallback capacity in flight, that is the only documented loss point.

## Tests

```bash
go test ./... -race
go vet ./...
gofmt -l .
```
