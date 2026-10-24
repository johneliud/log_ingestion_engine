# Log Ingestion Engine

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

## Tests

```bash
go test ./... -race
go vet ./...
gofmt -l .
```
