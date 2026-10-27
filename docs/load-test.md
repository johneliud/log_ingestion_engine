# Load Test

How the 1,000 logs per second target was measured (issue 29), what was run and where the real limits are.

## Method

- **Tool:** `cmd/loadtest`, a load generator written with the Go standard library and checked into this repository (see `go run ./cmd/loadtest -help`). It schedules requests on a fixed pace (`target rate / batch size`), spreads them over round-robin spoofed source IPs in `198.51.100.0/24` and reports accepted logs, status codes and latency percentiles in its summary.
- **Several source IPs:** the generator sends `X-Forwarded-For` from a rotated pool of 50 addresses with the server started as `TRUSTED_PROXIES=127.0.0.1/32`, so every spoofed address gets its own rate-limit bucket.
- **Target:** the server compiled from `cmd/server` on the same machine (Linux laptop, Go 1.26.3), broker `rabbitmq:3-management` from the compose stack, SQLite in a temp directory (`DB_DIR`).
- **Accounting:** after each run the queue backlog is watched until it drains, then the stored row delta from `logs_service1.db` is compared against the logs the server accepted, and the dead-letter file is checked.

## Results

### Sustained 1,000 logs per second (12 pipeline workers, batch 100)

| Number | Result |
|--------|--------|
| Duration | 30 s |
| Requests sent / accepted | 300 / 300 (`202 accepted` only) |
| Sustained ingestion | **1000.0 logs/s, zero rejections** |
| Response latency | mean 2.5 ms, p50 2.0 ms, p95 5.1 ms, p99 9.5 ms |
| Storage | all 30,000 accepted logs stored, drained within 47 s of the run ending |
| Dead letters / write failures / rate-limited | 0 / 0 / 0 |
| Server footprint | ~26 MB RSS, ~9% CPU (one busy core group, four shared cores otherwise idle) |

End-to-end accounting for every run below always closed: accepted + rejected = sent, and everything accepted was stored when the queue drained — no log was ever dropped or dead-lettered during these runs.

### Scaling and the heavy tail

| Run | Pipeline workers | Target logs/s | Batch | Senders | Source IPs | Duration | Sustained accepted | Rejections |
|-----|------------------|---------------|-------|---------|------------|----------|--------------------|------------|
| A | 4 | 1,000 | 100 | 8 | 50 | 30 s | ~630 logs/s | 110 × `503` (11,000 logs) |
| B | 12 | 2,000 | 100 | 16 | 50 | 30 s | ~1,150 logs/s | 254 × `503` (25,400 logs) |
| C | 12 | 1,000 | 100 | 8 | 50 | 30 s | 1,000 logs/s | none (table above) |

`503 {"error":"ingestion overloaded"}` is the bounded `raw_logs` channel answering under backpressure; the rejected requests are turned away whole (HTTP-level, no partial batches).

### Rate limiter

| Run | Setting | Result |
|-----|---------|--------|
| C (single IP) | 1500 req/s from one IP, batch 1, 15 s | 15,993 × `202`, 6,507 × `429 {"error":"rate limit exceeded"}` — admission held at ~1,000 requests/s for that IP |
| A, B, C (50 IPs) | 10–20 req/s per IP | zero `429` responses; spreading the sending across IPs never starves a legitimate bucket |

## Bottleneck analysis

1. **Publishing is the ceiling, and it scales by worker count.** Each pipeline worker publishes its log and waits for the RabbitMQ publisher confirm before taking the next one (`PUBLISH_TIMEOUT` defaults to 5 s). With 4 workers the engine admitted ~110–130 logs/s per worker (~630 overall); with 12 workers per-worker throughput stayed in the same band and the overall ceiling rose to roughly 1,150–1,300 logs/s. Sustaining 1,000 logs/s comfortably needs raising `PIPELINE_WORKER_COUNT` above the default 4.
2. **The bounded buffer converts a publisher deficit into 503s.** When publishing is slower than arrival, the 10,000-entry `raw_logs` channel fills within seconds; each handler push then races a 100 ms timeout, and the first push that loses fails the whole request. This is the designed behavior (reject rather than drop), but it means the maximum *accepted* rate is set by the publisher loop, and per-request failures are all-or-nothing at batch granularity.
3. **Consumers and storage are not the limit but they set recovery time.** The consumer/batch-writer side drained up to ~14,900 queued messages in tens of seconds (~900+ messages/s sustained against a single destination) and always caught up without dead letters or write failures. `queue_backlog` on `/metrics.json` is the gauge to watch during a burst.
4. **The per-IP map and metrics counters are not a bottleneck at target load.** All outbound requests share one rate-limit registry, one mutex-guarded limiter map and the metrics snapshot lock; at 20 requests/s spread over 50 IPs the limiter never got close to its 1,000 request/s buckets and the server stayed below ~10% CPU.
5. **Unrouted services are expensive at load.** A run without a routing rule for the test service emitted a `WARN` line per log (router default-destination path) which itself measurably slowed ingestion before replacing it by a rule. Configure `ROUTING_CONFIG_PATH` for high-volume services.

## Reproduce

```bash
docker compose up -d
cp configs/.env.example .env   # fill in RABBITMQ_USER and RABBITMQ_PASS
go run ./cmd/server
TRUSTED_PROXIES=127.0.0.1/32 must be set for the spoofed X-Forwarded-For to be honored

go run ./cmd/loadtest -target http://localhost:8080 -duration 30s -rate 1000 -batch 100 -source-ips 50 -senders 8
```

Then reconcile: `GET /metrics.json` (throughput, `queue_backlog`, counters), the row count in `DB_DIR`/`logs_service1.db`, `DEAD_LETTER_PATH`, and the generator summary.
