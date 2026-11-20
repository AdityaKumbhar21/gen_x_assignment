# Architecture & Debugging Decisions

## 1. Root-Cause Diagnosis

- **Event-loop / CPU behavior:** The async handler directly ran the 120,000-iteration hashing loop. Under the original 60-request burst, maximum health latency was 1,522 ms and ingestion p95 was 3,346 ms. `async` did not offload the synchronous work. Waiting for the receiver's 250 ms simulated latency also made the acknowledgment target unattainable.
- **Concurrency & race conditions:** The required idempotency header was discarded. There was no unique request identity or atomic claim, so 30 unique events produced 60 rows and 53 downstream acceptances. Updating by event_id marked both duplicate rows delivered even when one attempt failed.
- **Resource & connection management:** Every event created a new HTTP client. There was no durable retry/recovery loop. A separately tested unique event remained pending after a downstream 503. The new implementation reuses one client and one serialized SQLite connection per ASGI process; each process has its own resources.

## 2. Concurrency & Offloading Strategy

The request validates its payload, computes a single cheap request digest, and commits durable acceptance before returning 202. Four fixed end-to-end workers claim an event, offload preprocessing, save the result, make one HTTP attempt, and record its outcome. Only pending, delivered and failed are durable states. Claims, cached output and retry timestamps avoid an extra stage/state framework.

The existing processor is unchanged. The executor decision was measured on CPython 3.13.4 with the GIL enabled and 10 logical CPUs, over five bursts of 30 jobs and five of 60 jobs per configuration. A separate process sent real HTTP probes and every processor output was checked.

| Executor | Median 60-job batch | Worst health latency | Probes >=50 ms |
|---|---:|---:|---:|
| Inline | 1,627.799 ms | 1,644.551 ms | 10 |
| One thread | 1,722.978 ms | 17.190 ms | 0 |
| Two threads | 1,693.215 ms | 38.568 ms | 0 |
| Four threads | 1,649.262 ms | 97.177 ms | 16 |
| Two processes | 911.414 ms | 1.482 ms | 0 |
| Four processes | 474.956 ms | 2.034 ms | 0 |

Choose a reusable four-process ProcessPoolExecutor for the measured single-ASGI-worker setup. One/two threads passed the isolated health check, but four threads violated it and gave little throughput scaling. Most hash calls operate on 32-byte digests, below hashlib's large-input GIL-release threshold. Four processes were about 3.47 times faster than four threads. The isolated benchmark excludes SQLite and downstream work; the full harness validates the combined result.

At most four jobs can be submitted because each of four worker tasks owns at most one CPU future and does not advance until it finishes. There is no per-request task fan-out, custom scheduler or restart supervisor. BackgroundTasks alone would not make accepted work durable, and an async task directly executing the processor would still block the loop.

FastAPI application lifespan owns the pool, client, database, workers and renewal task. Spawn-based pool warm-up completes before readiness (about 480 ms in the benchmark). Cleanup is registered immediately after each resource is acquired. Shutdown stops claims, drains workers, records known outcomes, cancels coordinators if needed, and joins the pool via a cleanup thread while the event loop and claim renewal can still run. Then it closes the client/database. Cancelling a coordinator cannot stop an already-running child call; shielded CPU futures are accounted for before safe claims are released. SIGKILL does not run this cleanup.

## 3. Idempotency & SQLite Concurrency

The contract is UNIQUE(tenant_id, idempotency_key); the README does not explicitly prescribe global key uniqueness. A separate immutable request_fingerprint is SHA-256 over canonical UTF-8 JSON of the validated original event_id, tenant_id and content, with sorted keys, compact separators and ensure_ascii=False. This is computed before any transaction. It is deliberately separate from the expensive redacted-content fingerprint: changing one email to another must conflict even if both redact to the same text.

Acceptance uses a short BEGIN IMMEDIATE transaction, INSERT ON CONFLICT(tenant_id,idempotency_key) DO NOTHING, then reads the existing immutable fingerprint if necessary. Identical retries return the existing acknowledgment; changed payloads return 409 without mutation. Two tenants can independently reuse a key. A successful response is returned only after commit.

SQLite uses WAL, synchronous=FULL and busy_timeout=100 milliseconds on every connection. A connection-level asyncio lock prevents unrelated coroutines interleaving transactions. Acceptance's 200 ms deadline applies to acquiring that connection/write lock, not to cancelling a durable commit already underway. An initialization-only bounded retry handles simultaneous WAL/schema setup. Fresh schema creation and one supported version are recognized; unsupported existing data is rejected with a clear fresh-path instruction. There is no legacy migration engine.

A worker atomically selects and claims due pending work inside one short write transaction, using a fresh token and a 15-second lease. The same query recovers expired claims. One simple task renews active claims every five seconds. Conditional result updates require row ID, token, pending status and an unexpired lease; current time is read after acquiring the write lock so waiting cannot resurrect expired ownership. Before sending HTTP, the worker rechecks/extends ownership and increments the attempt count. All updates target the exact internal row, never all matching event_ids.

Canonicalization, hashing, CPU execution, HTTP requests, capacity waits and retry sleeps occur outside transactions. Processed output is cached and raw input is cleared in a short commit before delivery. Retried delivery does not repeat preprocessing.

## 4. Failure Modes & Production Trade-offs

**Downstream unreachable for 60 seconds:** Acceptance remains independent of delivery. Confirmed 503 rejection and known pre-send connection failures keep the row pending with durable retry scheduling. First retry is 50–100 ms, exponentially increasing with jitter to a one-second cap. Four workers bound HTTP concurrency; idle polling is 25 ms. Jobs are not discarded after a short retry budget. A real 60-second outage test confirms retained work, continued successful ingestion, healthy liveness and delivery after recovery.

**Six-second completion window:** The mock can fail each first attempt only. Even if all 30 first attempts fail, 60 HTTP attempts at 250 ms each need approximately 3.75 seconds of four-worker HTTP capacity, plus preprocessing and storage overhead. A separate deterministic HTTP receiver tests this worst case, keeping the original receiver and acceptance test unchanged. The final clean-source review on Python 3.12.11 settled all deliveries in 4.13 seconds, with p95 acknowledgment 74.71 ms and maximum health 19.05 ms.

**Crash 50 ms after acknowledgment:** The input and tenant-scoped deduplication identity are committed first. Unclaimed work resumes immediately; active claims may wait until lease expiry. Saved cleaned output is reused. An abrupt writer-process exit test bypasses cleanup after committing acceptance and a claim, then verifies restart recovery without a sender retry. Separate lifespan tests cover restart after cancelling CPU coordination and graceful child cleanup.

**Remote exactly-once boundary:** Receiver acceptance and the local delivered commit cannot be one SQLite transaction. If a response is lost, an owner pauses beyond its lease, or the process crashes between those effects, replay can duplicate the remote side effect. The outbound key hashes the tenant/key pair, but the receiver must honor it or provide outcome reconciliation. The supplied mock does neither. Observed ambiguous outcomes become failed with a reconciliation message; an unobserved crash can leave pending work that recovery replays. Exactly-once remote effects across those boundaries depend on downstream cooperation. Normal concurrent retries and the supplied explicit 503 rejection scenario are covered by local claims and tests.

**Four ASGI workers:** Each lifespan has its own CPU pool, HTTP client and worker loops. Leaving four CPU workers per ASGI process creates 16 CPU processes and can oversubscribe the measured host; use WEBHOOK_JOB_WORKERS=1 per process for an aggregate four-process CPU budget. SQLite constraints/claims coordinate across processes; startup does not clear another owner's live claims. SQLite's single writer becomes a throughput limit. Tests cover separate application instances and concurrent fresh database connections, not a four-ASGI-process performance benchmark.

**Validation (2026-11-20):** A fresh source snapshot and new Python 3.12.11 virtual environment were installed using `uv sync --locked --extra dev`; `uv run --extra dev python -m pytest -v -s` then reported **36 passed in 73.35s (0:01:13)**, including the real 60-second outage. The original acceptance test, processor and flaky receiver match the pre-implementation SHA-256 baseline. A fresh 60-request diagnostic returned 60 HTTP 202s, ingestion p95 31.17 ms, maximum health 15.81 ms, 30 rows all delivered, 30 downstream acceptances and passing PII redaction. Shareable evidence is in `.repro/submission-readiness-results.json` and `.repro/submission-burst-results.json`; `.repro/submission-tests.log` is a local-only generated log. The supplied folder has no Git history/remote, so this verifies an artifact-free source snapshot rather than an actual remote clone. All application/test/dependency files were identical between the supplied folder and the clean run. No external infrastructure or new dependencies were introduced.

**Storage scope:** Original input is stored until preprocessing succeeds. Clearing raw_content is a logical update, not secure erasure of earlier SQLite/WAL pages or backups. The assessment verifies redaction of cleaned output and downstream data; production storage retention/access controls remain separate operational concerns.
