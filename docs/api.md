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
| 413    | Body over configured limit         | `{"error":"request body exceeds the maximum allowed size"}`              |
| 413    | Valid entries exceed queue capacity | `{"error":"batch exceeds ingestion queue capacity"}`                     |
| 503    | Queue remains overloaded until timeout | `{"error":"ingestion overloaded"}`                                  |
| 415    | Content type not application/json  | `{"error":"content type must be application/json"}`                      |

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
