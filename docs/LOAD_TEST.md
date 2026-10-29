# Load test

Goal: verify 1 k logs/s sustained ingestion.

**Method**

- `cmd/loadtest` generates requests at a target rate, spreads over 50 spoofed IPs (198.51.100.0/24) behind a trusted proxy.
- Server built from `cmd/server` (Go 1.26), RabbitMQ from compose, SQLite temp dir.
- After each run the backlog is drained, stored row delta in `logs_service1.db` is compared, dead‑letter file checked.

**Results (excerpt)**

- 30 s run, 300 requests → 300 accepted (202), sustained 1000 logs/s, zero rejections.
- Latency: mean 2.9 ms, p50 2.2 ms, p95 7.1 ms, p99 14.1 ms.
- Storage: all 30 000 logs stored, drained within ~47 s.
- No dead letters / write failures / rate‑limited.

**Scaling**

| Run | Workers | Target logs/s | Batch | Senders | IPs | Accepted |
|---|---|---|---|---|---|---|
| A | 4 | 1 000 | 100 | 8 | 50 | ~630 logs/s (110× 503) |
| B | 12 | 2 000 | 100 | 16 | 50 | ~1 150 logs/s (254× 503) |
| C | 12 | 1 000 | 100 | 8 | 50 | 1 000 logs/s (no 503) |

**Bottleneck analysis (bullets)**  

1. Publishing is the ceiling; per‑log confirm (5 s timeout) limits throughput; workers scale up to ~11‑12 for 1 k logs/s.  
2. Bounded `raw_logs` channel (10 000) back‑pressures into 503s; all‑or‑nothing at batch granularity.  
3. Consumers & SQLite not limiting; they drain ~15 k messages/s.  
4. Rate‑limit map not a bottleneck at target load.  
5. Unrouted services emit WARN per log and slow ingestion; set `ROUTING_RULES`.

**Reproduce**

```bash
docker compose up -d
cp configs/.env.example .env   # fill RABBITMQ_USER / RABBITMQ_PASS
TRUSTED_PROXIES=127.0.0.1/32 go run ./cmd/server
go run ./cmd/loadtest -target http://localhost:8080 -duration 30s -rate 1000 -batch 100 -source-ips 50 -senders 8
```

Then reconcile: metrics, row counts in `DB_DIR/logs_service1.db`, dead‑letter file.