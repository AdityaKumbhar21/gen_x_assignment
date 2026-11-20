# Webhook Ingestion Service — Backend Technical Assessment

## Context
Our webhook ingestion service is dropping requests and timing out health checks during traffic spikes, and downstream partners are reporting duplicate events whenever their webhook sender retries after a slow response.

Your goal is to diagnose the root causes under realistic concurrent load and refactor the service so it handles traffic spikes reliably and idempotently.

## Setup
Run from this backend folder with Python 3.11 or newer and `uv` installed (verified on Python 3.12.11 and 3.13.4).

```bash
uv sync --locked --extra dev
```

## Running the Benchmark & Chaos Harness
The test suite starts both the FastAPI ingestion service and a mock flaky downstream receiver (`mock_downstream.py`), then simulates a 60-request burst containing duplicate retries while continuously probing `/health`:

```bash
uv run --extra dev python -m pytest -v -s
```

The original acceptance test is unchanged. The suite also includes the additional concurrency, retry, recovery, and lifecycle tests described below, including a real 60-second outage.

## Acceptance Criteria
1. **Event-Loop Responsiveness:** Every `GET /health` probe sent while burst traffic is in flight must respond in **< 50ms**.
2. **Fast Ingestion Acknowledgment:** `POST /webhooks/ingest` must acknowledge valid requests (`200` or `202`) with **p95 latency < 250ms**.
3. **Strict Idempotency:** Concurrent duplicate deliveries sharing the same `X-Idempotency-Key` must never produce duplicate database rows or duplicate downstream deliveries.
4. **Resilient Downstream Delivery:** Transient `503` errors from the downstream service must be retried safely so all unique events reach `"delivered"` status.

## Constraints
- **No external infrastructure:** Keep the solution self-contained using Python and SQLite (do not add Redis, Celery, RabbitMQ, or PostgreSQL).
- Complete `DECISIONS.md` explaining your root-cause diagnosis and architectural choices.

## Implemented behavior

Ingestion commits one durable event before returning `202 Accepted`. CPU processing and downstream delivery run afterward in four fixed workers. The original processor, acceptance test and flaky receiver are unchanged.

Idempotency is scoped to `UNIQUE(tenant_id, idempotency_key)`. The service stores a separate SHA-256 request fingerprint over canonical JSON of the validated original `event_id`, `tenant_id` and `content`. Identical retries return the existing job; changed payloads under the same tenant/key return `409 Conflict`. Different tenants may use the same key independently.

The response includes `job_id`, `event_id`, and `delivery_status`. Repeating an accepted request returns its current delivery status without creating more work. A successful acknowledgment indicates durable acceptance, not completed downstream delivery.

There are three durable states: `pending`, `delivered`, and `failed`. Claim and retry metadata live on pending rows. Successfully processed output is reused for delivery retries, and temporary raw input is cleared once that output is committed.

## Run locally

In separate terminals, from this backend folder:

```sh
uv run --extra dev python -m uvicorn mock_downstream:app --host 127.0.0.1 --port 9001
```

```sh
uv run --extra dev python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

No frontend service is required. Next.js has its own dependencies and generated mock data.

## Configuration and resource lifetime

| Environment variable | Default | Purpose |
|---|---|---|
| `WEBHOOK_DB_PATH` | `webhooks.db` | File-backed SQLite database |
| `DOWNSTREAM_URL` | `http://127.0.0.1:9001/events` | Receiver endpoint |
| `WEBHOOK_JOB_WORKERS` | `4` | End-to-end worker count, CPU pool size and outbound connection limit |
| `WEBHOOK_POLL_INTERVAL` | `0.025` | Idle polling interval, seconds |
| `WEBHOOK_LEASE_SECONDS` | `15` | Claim expiry, seconds |
| `WEBHOOK_RENEW_INTERVAL` | `5` | Active-claim renewal interval, seconds; must be below the lease duration |
| `WEBHOOK_SHUTDOWN_GRACE_SECONDS` | `3` | Worker-drain interval before coordinator cancellation |

Application lifespan initializes SQLite, creates the shared HTTP client, starts and warms a spawn-based `ProcessPoolExecutor`, and starts fixed workers and one renewal loop. On shutdown it stops new claims, drains workers, accounts for running CPU futures, joins the pool outside the event-loop thread, and closes the client and database. Partial startup failure follows the same cleanup path. The drain interval cannot forcibly terminate arbitrary running CPU calls; `SIGKILL` bypasses graceful cleanup.

SQLite uses WAL, `synchronous=FULL`, a 100 ms busy timeout on each connection, and short write transactions. Acceptance has a 200 ms connection/write-lock admission budget; an in-progress commit is allowed to finish before acknowledging. Network requests, request hashing, preprocessing, retry sleeps and executor waits happen outside transactions. Unsupported starter/legacy schemas fail startup without altering their rows. Back up the original database with SQLite's backup facilities and select a fresh `WEBHOOK_DB_PATH`; automatic historical-data migration is intentionally out of scope.

Confirmed downstream `503` rejection and known pre-send connection failures retry using persisted timestamps: initially 50–100 ms, exponential backoff capped at one second. Workers process other jobs while waiting for a retry to become due. HTTP connect/read/write/pool timeouts are 0.5/1.0/1.0/0.5 seconds. Permanent or observed ambiguous outcomes become `failed` with an explanatory error; retries from the sender do not overwrite them.

`/health` is responsive without performing a database or network query. An unexpected required-task exit marks the service unhealthy, returning `503` for health and new ingestion; there is no custom worker restart framework.

## Recovery and guarantee limits

Committed pending rows survive a process crash. A restarted worker atomically claims unowned or expired work, reusing saved output. A crash with an active claim may delay recovery until its 15-second lease expires. Startup never clears another process's live claims.

Local uniqueness and live ownership prevent ordinary concurrent duplicate jobs and deliveries. The outbound idempotency header is a deterministic hash of the tenant/key pair. A receiver must honor that token or provide reconciliation to guarantee exactly one remote side effect after an accepted request loses its response, an owner pauses past its lease, or a crash occurs before the local delivered commit. The supplied mock does not deduplicate. An observed ambiguous outcome is marked failed for reconciliation; a crash may leave no observation and recovery can replay the remote action.

For four ASGI workers, set `WEBHOOK_JOB_WORKERS=1` to keep the aggregate CPU pool at four processes on the measured host. Keeping the default would create 16 CPU processes. SQLite still allows only one writer. Tests cover separate application instances sharing a database; four-ASGI-process performance has not been benchmarked.

## Validation and measured results

```sh
uv sync --locked --extra dev
uv run --extra dev python -m pytest -v -s
uv run --extra dev python .repro/measure_burst.py --output .repro/submission-burst-results.json
```

Run server-based checks sequentially. The original harness uses ports 8901/9001; the additional deterministic receiver uses 8903/9003.

The final clean-source review on Python 3.12.11 passed all **36 tests in 73.35 seconds**. The fresh diagnostic recorded ingestion p95 **31.17 ms**, maximum health latency **15.81 ms**, 60 successful acknowledgments, 30 delivered database rows, and 30 total/unique downstream acceptances. PII checks passed. Additional tests exercise every first attempt receiving 503, a real 60-second outage, conflicting requests, concurrent claims and initialization, abrupt process exit, renewal, and shutdown during CPU/HTTP work. These are local measurements, not guarantees for arbitrary hardware or payload sizes.

See [DECISIONS.md](DECISIONS.md), [implementations.md](implementations.md), and the recorded results under `.repro/`. Benchmark scripts and measurement JSON in `.repro/` are useful source/evidence and should be included in the submission; generated `.log` files are local-only. `.gitignore` excludes environments, caches, package build metadata, databases/journals, local settings and temporary files while preserving application and test sources.

The final readiness evidence is in [.repro/submission-readiness-results.json](.repro/submission-readiness-results.json) and [.repro/submission-burst-results.json](.repro/submission-burst-results.json). No Git history/remote was supplied, so clean-checkout verification used an artifact-free source snapshot selected using Git ignore rules. Local environments, caches, generated logs, `.DS_Store` and `DECISIONS copy.md` are excluded from submission; their local copies are preserved.
