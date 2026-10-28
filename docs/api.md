# API Reference

## POST /logs

Accepts a JSON array of log entries for ingestion. Each entry is validated independently; valid entries are accepted and invalid entries are rejected with field-level errors.

### Request

- **Content-Type:** `application/json` (required)
- **Body:** JSON array of log entry objects

Each log entry object:

| Field       | Type   | Required | Constraints                                              |
|-------------|--------|----------|----------------------------------------------------------|
| `timestamp` | string | yes      | RFC 3339 timestamp (e.g. `2026-10-24T10:00:00Z`)        |
| `service`   | string | yes      | 1–100 characters                                         |
| `level`     | string | yes      | One of `INFO`, `WARN`, `ERROR`, `DEBUG`                  |
| `message`   | string | yes      | 1–10,000 characters                                      |

### Responses

| Status | Situation                          | Body shape                                                                |
|--------|------------------------------------|---------------------------------------------------------------------------|
| 202    | All or some entries valid          | `AcceptedResponse` (with optional `rejected` array)                      |
| 400    | Malformed JSON                     | `{"error":"malformed JSON: ..."}`                                        |
| 400    | All entries invalid                | `AllEntriesRejectedResponse` with per-entry errors                      |
| 400    | Empty body, null, or non-array     | `{"error":"..."}`                                                        |
| 400    | Body read fails mid-stream         | `{"error":"could not read the request body"}`                            |
| 413    | Body over configured limit         | `{"error":"request body exceeds the maximum allowed size"}`              |
| 413    | Valid entries exceed queue capacity | `{"error":"batch exceeds ingestion queue capacity"}`                     |
| 429    | Source IP over its `RATE_LIMIT`    | `{"error":"rate limit exceeded"}`                                        |
| 503    | Queue remains overloaded until timeout | `{"error":"ingestion overloaded"}`                                  |
| 415    | Content type not application/json  | `{"error":"content type must be application/json"}`                      |

The rate limit (`429`) is applied by the per-IP middleware before the handler runs: each source IP gets `RATE_LIMIT` (default 1000) requests per second, and a request uses one token no matter how many log entries its batch carries.

Storage writes are idempotent. Every accepted entry receives a `log_id` (UUID, generated at ingestion) that is carried through the pipeline, the queue payload and the dead-letter file, and `StoreBatch` writes with `ON CONFLICT (log_id) DO NOTHING` against a unique index, so a redelivered queue message or a replayed dead-letter entry cannot duplicate a stored row. Logs stored before this change keep their own rows and gain an empty `log_id`.

### Response shapes

#### AcceptedResponse (202)

When all entries are valid, the `rejected` field is omitted:

```json
{
    "status": "accepted",
    "batchId": "550e8400-e29b-41d4-a716-446655440000"
}
```

When some entries are valid and some are invalid (partial batch), the `rejected` array lists each invalid entry with its zero-based index and field-level errors:

```json
{
    "status": "accepted",
    "batchId": "550e8400-e29b-41d4-a716-446655440000",
    "rejected": [
        {
            "index": 0,
            "errors": [
                {"field": "timestamp", "message": "timestamp is required"}
            ]
        }
    ]
}
```

#### AllEntriesRejectedResponse (400)

When every entry in a non-empty batch fails validation, the response is `400` with `status: "rejected"` and per-entry errors:

```json
{
    "status": "rejected",
    "rejected": [
        {
            "index": 0,
            "errors": [
                {"field": "level", "message": "level must be one of INFO, WARN, ERROR, DEBUG"}
            ]
        },
        {
            "index": 1,
            "errors": [
                {"field": "timestamp", "message": "timestamp must be a valid RFC 3339 timestamp"},
                {"field": "service", "message": "service is required"}
            ]
        }
    ]
}
```

An entry may have multiple field errors when more than one field fails validation. The `index` is the zero-based position of the entry in the original request array.

### Validation rules

| Field       | Rule                                           | Error message                                              |
|-------------|------------------------------------------------|------------------------------------------------------------|
| `timestamp` | Must not be empty                              | `timestamp is required`                                    |
| `timestamp` | Must parse as RFC 3339 (fractional seconds OK) | `timestamp must be a valid RFC 3339 timestamp`            |
| `service`   | Must not be empty                              | `service is required`                                      |
| `service`   | At most 100 characters                          | `service must be at most 100 characters`                  |
| `level`     | Must not be empty                              | `level is required`                                       |
| `level`     | One of INFO, WARN, ERROR, DEBUG                | `level must be one of INFO, WARN, ERROR, DEBUG`           |
| `message`   | Must not be empty                              | `message is required`                                      |
| `message`   | At most 10,000 characters                      | `message must be at most 10000 characters`               |

## GET /metrics.json

Returns a snapshot of the service's runtime metrics as JSON. The endpoint never authenticates, never fails with a non-200 status for metric reasons, and refreshes `queue_backlog` on every request.

### Responses

| Status | Situation          | Body shape         |
|--------|--------------------|--------------------|
| 200    | `GET /metrics.json`| `MetricsSnapshot`  |

### Response shape

#### MetricsSnapshot (200)

```json
{
    "total_logs_received": 1284,
    "logs_by_level": {"INFO": 900, "WARN": 300, "ERROR": 84},
    "logs_by_service": {"claims": 700, "billing": 584},
    "error_rate": 0.065,
    "throughput": 42.5,
    "queue_backlog": 17,
    "rate_limited_requests": 3,
    "dead_lettered_logs": 0,
    "write_failures": 0
}
```

| Field                   | Type                 | Meaning                                                       |
|-------------------------|----------------------|---------------------------------------------------------------|
| `total_logs_received`   | integer              | Total log entries accepted since process start                |
| `logs_by_level`         | object of integers   | Accepted entries grouped by `INFO`/`WARN`/`ERROR`/`DEBUG`     |
| `logs_by_service`       | object of integers   | Accepted entries grouped by `service` name                    |
| `error_rate`            | number               | `ERROR` entries divided by total entries (0 when none)        |
| `throughput`            | number               | Accepted entries per second over the configured window (`METRICS_THROUGHPUT_WINDOW`) |
| `queue_backlog`         | integer              | Messages waiting across the three `queue_service*` queues     |
| `rate_limited_requests` | integer              | Requests rejected or throttled by the rate limiter            |
| `dead_lettered_logs`    | integer              | Entries routed to the dead-letter destination                 |
| `write_failures`        | integer              | Storage write attempts that failed                            |

### queue_backlog sourcing

On each `GET /metrics.json` request the service queries the RabbitMQ management API (`GET {RABBITMQ_MGMT_URL}/api/queues`, basic auth, bounded by `METRICS_BACKLOG_TIMEOUT`) and sums the `messages` field across `queue_service1`, `queue_service2` and `queue_service3` inside the configured virtual host (`RABBITMQ_VHOST`, default `/`), ignoring any other queue on the broker and any same-named queue in another virtual host.

When the management API URL is not configured, unreachable, or rejects the request, the service logs a warning and reports a local fallback counter instead: an in-process estimate defined as messages published minus messages stored or released (`RecordQueueMessagePublished` / `RecordQueueMessageStored` / `RecordQueueMessageReleased`), floored at zero. The fallback value is intentionally not part of the snapshot; only `queue_backlog` carries the effective value.

## GET /metrics/events

Streams metrics snapshots as server-sent events (`text/event-stream`). The client connects with `EventSource` and receives an initial snapshot immediately, followed by a new snapshot whenever any counter changes (debounced to 200 ms). The connection stays open until the client closes it; `EventSource` reconnects automatically on connection loss.

Each event is a single `data:` line containing a JSON `MetricsSnapshot`, followed by a blank line.

### Responses

| Status | Situation          | Body shape         |
|--------|--------------------|--------------------|
| 200    | `GET /metrics/events` | Server-sent events stream of `MetricsSnapshot` |

## Other endpoints

| Method | Path                 | Body shape |
|--------|----------------------|------------|

Unknown paths return `404` and a wrong method on a known path returns `405`.

The user interface is served by the React frontend in `frontend/` (Vite dev server or `npm run preview`), not by the Go backend. The backend exposes only the ingestion API (`POST /logs`) and the metrics snapshot (`GET /metrics.json`).
