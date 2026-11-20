# FastAPI implementation plan

Status: implemented and verified. All 36 tests pass, including the unchanged acceptance test and the real 60-second outage. This document records the agreed approach and implementation sequence. It covers only `fastapi-webhook-assessment`; the Next.js application remains independent.

This revision replaces the earlier pipeline proposal with one table, three durable states, four fixed end-to-end worker loops, and a small lease-renewal loop. There is no custom scheduler, worker-restart framework or legacy-data migration framework.

## 1. Requirements and acceptance gates

Use the existing Python, FastAPI, aiosqlite, HTTPX and SQLite stack, managed through this project's `uv.lock` and `.venv`. Add no Redis, Celery, RabbitMQ, PostgreSQL or external infrastructure. Standard-library child processes are permitted.

| Requirement | Implementation responsibility | Acceptance gate |
|---|---|---|
| Responsive event loop | Offload preprocessing; bound active work | Every burst-time `/health` response is below 50 ms |
| Fast acknowledgment | Commit durable acceptance before background work | All valid harness requests return 200/202; ingestion p95 below 250 ms |
| Strict idempotency | Database uniqueness and atomic worker claims | 60 duplicate requests produce 30 event rows and exactly 30 accepted downstream deliveries |
| Resilient delivery | Persist retry scheduling and recover work | All 30 unique events reach `delivered`, within the harness's six-second delivery wait |
| PII handling and deterministic processing | Preserve the existing processor and remove temporary raw input after processing | Existing redaction checks and fingerprint-equivalence checks pass |
| Explain the design | Complete `DECISIONS.md` after implementation | Diagnosis, pool choice, concurrency, outage, crash and multiple-worker behavior documented |

The README does not explicitly require global idempotency-key uniqueness. The agreed contract is `UNIQUE(tenant_id, idempotency_key)`. Do not loosen the original acceptance thresholds or modify the original test to manufacture a pass.

## 2. Measured executor decision

Keep `scrub_and_fingerprint()` unchanged, including its 120,000 SHA-256 iterations. Use a reusable `ProcessPoolExecutor` with four workers for the measured single-ASGI-worker setup. Keep the worker count configurable for deployment CPU limits.

| Executor | Median 60-job batch | Maximum HTTP health latency | Health probes at least 50 ms |
|---|---:|---:|---:|
| Inline | 1,627.799 ms | 1,644.551 ms | 10 |
| One thread | 1,722.978 ms | 17.190 ms | 0 |
| Two threads | 1,693.215 ms | 38.568 ms | 0 |
| Four threads | 1,649.262 ms | 97.177 ms | 16 |
| Two processes | 911.414 ms | 1.482 ms | 0 |
| Four processes | 474.956 ms | 2.034 ms | 0 |

These measurements used CPython 3.13.4 with the GIL enabled, 10 logical CPUs, five bursts each of 30 and 60 jobs, warmed pools and a separate HTTP load-driver process. Every processor output was checked. Four processes delivered about 3.47 times the throughput of four threads. One and two threads passed the isolated health test, but did not scale throughput comparably.

Keep at most four CPU futures submitted at once. `max_workers` limits executing workers, not the submitted backlog. Four fixed end-to-end workers each hold at most one CPU future and one claimed event, so no independent CPU submission queue or second delivery pipeline is needed. A worker completes preprocessing, saves its output, attempts delivery, records the result, and then claims another event. The remaining backlog stays in SQLite. Do not create a task per request or use the default thread pool for preprocessing.

The CPU benchmark excludes SQLite and downstream delivery. It selects the executor; it does not prove the completed system meets the acceptance gates. Full results and rerunnable code are in [.repro/CPU_BENCHMARK.md](.repro/CPU_BENCHMARK.md), [.repro/cpu-benchmark-results.json](.repro/cpu-benchmark-results.json) and [.repro/benchmark_cpu.py](.repro/benchmark_cpu.py).

## 3. Application-managed lifespan and cleanup

Replace the existing startup-only `@app.on_event` handler with an `asynccontextmanager` passed to `FastAPI(lifespan=...)`. Keep resource handles, a task list and a stop event in `app.state`; do not add a separate runtime framework. Do not construct the pool, open connections or start tasks at module import time.

### Startup, before yielding readiness

1. Read configuration and validate worker counts and timeout values.
2. Initialize a fresh database or validate the supported schema. Enable and verify WAL before traffic; initialize connection settings and the due-work index. Reject unsupported legacy schemas with an actionable message.
3. Create one lifespan-managed `httpx.AsyncClient` per ASGI process, with explicit connection limits and connect/read/write/pool timeouts.
4. Create the process pool with an explicit `spawn` multiprocessing context. Submit a bounded warm-up that imports and exercises the real top-level processor before the application becomes ready. The benchmark's four-process startup including warm-up was about 480 ms; keep this outside request handling.
5. Start four ordinary worker tasks and one small renewal task for their active claims. SQLite is their source of truth. Each idle worker waits on the stop event with a 25 ms timeout before querying for due work again. No wake-event machinery or custom scheduling abstraction is needed.
6. Recover expired claims through the same atomic claim query used for normal work. Do not globally clear claims during startup or add a recovery scanner.
7. Yield control only after required resources and workers are ready. Retain task handles and observe exceptions with a simple completion callback. Unexpected exit of a required worker or renewal task, or a broken pool, marks the service unhealthy and logs the cause; do not build an automatic restart supervisor. Normal job-level retryable failures stay within the worker loop.

Register cleanup immediately after acquiring each resource, using `AsyncExitStack` or equivalent `try/finally` blocks. Failure halfway through startup must close everything already created. Do not register cleanup only after all startup steps succeed.

Only serializable processor inputs and outputs cross the process boundary. Keep FastAPI objects, SQLite connections, HTTP clients and asyncio state in the parent process. The processor remains a top-level importable callable.

### Normal shutdown, in dependency order

1. Mark the runtime as stopping and prevent new job claims or CPU submissions. Stop retry polling from starting additional attempts.
2. Drain in-flight preprocessing and HTTP attempts within a configurable drain interval. Keep database connections, the HTTP client and claim renewal available while active work needs them. Commit completed outcomes in short transactions.
3. Stop and await the worker tasks, keeping the renewal task available until their remaining work is accounted for. Observe all exceptions. If draining times out, cancel remaining coordinators carefully and leave unfinished durable jobs recoverable. Never mark work delivered merely because shutdown began.
4. Shut down the process pool with `shutdown(wait=True, cancel_futures=True)`. Perform the blocking join through a dedicated/default cleanup thread, rather than blocking the event loop. The preprocessing executor remains the process pool; this thread is only for cleanup.
5. Keep claim renewal available until remaining CPU calls are accounted for; then stop it and close the HTTP client and database resources after their users have finished. Release only claims known to be safe to retry. An uncertain HTTP outcome follows the documented downstream policy. Cleanup must continue even if an earlier cleanup step raises.

Cancelling an asyncio await does not terminate an already running process-pool call. `cancel_futures=True` cancels work that has not started; running calls still finish. Track their futures and avoid declaring a claim reusable while its old owner can still act on it. Pure CPU children never deliver events or modify the database. Lease ownership checks prevent an obsolete parent result overwriting a newer owner.

The drain interval is not a promise that Python can forcibly stop arbitrary running CPU code within that deadline. The existing finite processor should finish normally; a stuck child or forced server termination needs the documented hard-stop/restart path. Do not depend on newer executor termination APIs absent from the project's Python >=3.11 support range.

Lifespan provides ownership and graceful cleanup during normal startup/shutdown. `SIGKILL`, interpreter crashes and host failure can bypass cleanup. Committed SQLite jobs and restart recovery cover that boundary; in-memory `BackgroundTasks` alone cannot do so.

## 4. Durable event schema and migration

Extend the events table to represent acceptance, processing and delivery without requiring the cleaned output to exist at acknowledgment time.

| Field/group | Purpose |
|---|---|
| `id` | Internal primary key; all job updates use it |
| `tenant_id`, `idempotency_key` | Immutable identity with `UNIQUE(tenant_id, idempotency_key)` |
| `event_id` | Original business identifier, retained in the downstream payload |
| `request_fingerprint` | Immutable, non-null digest of the validated original request |
| Temporary original job input | Enough information to resume preprocessing after acknowledgment; clear after cleaned output is durably stored |
| `cleaned_text`, `fingerprint` | Existing processor output; nullable until processing succeeds |
| `status` | Exactly `pending`, `delivered` or `failed` |
| `attempt_count`, `next_attempt_at`, `last_error` | Durable retry scheduling and diagnosis |
| `claim_token`, `lease_expires_at` | Ownership metadata on a pending row; not separate job states |
| Creation/update/delivery timestamps | State transitions and operational inspection |

`pending` covers unprocessed, processed, currently claimed and waiting-to-retry work. A non-null cleaned output/fingerprint pair means preprocessing has completed; retries reuse it. A claim token means the row is currently owned. `next_attempt_at` determines when an unclaimed or expired row is eligible. Do not add `processing`, `ready`, `delivering`, `retry_wait` or `delivery_unknown` states. Add a CHECK constraint requiring cleaned_text and fingerprint to be either both null or both present.

The only durable status transitions are insertion as pending, pending to delivered on confirmed success, and pending to failed for permanent or unresolved ambiguous outcomes. Claims, output persistence and retry scheduling update metadata while status stays pending. Delivered and failed rows retain their idempotency identity; retries from the sender do not create replacement jobs.

Add one index supporting pending/due-work selection. Use database-comparable persisted timestamps for leases and retries. Retain the acceptance fingerprint after temporary raw input is cleared. Avoid logging original content.

Keep schema handling explicit and small: create the target schema for a fresh database, recognize one supported schema version, and fail startup on an unsupported existing schema without modifying its rows. Document backing up the old database and selecting a fresh WEBHOOK_DB_PATH. Do not build an archival tool, historical identity mapper, migration registry or general legacy-data migration engine. Legacy rows lack the original key and unredacted input; never invent these values. An actual data migration can be a separately scoped task if needed.

## 5. Tenant-scoped acceptance and fingerprint contract

Before opening a transaction, validate the existing `WebhookPayload` and required `X-Idempotency-Key` header. Keep missing-header and invalid-body behavior explicit.

Build canonical JSON from the validated original `event_id`, `tenant_id` and `content` fields using `sort_keys=True`, `separators=(",", ":")` and `ensure_ascii=False`. Encode UTF-8 and compute a single SHA-256 digest as `request_fingerprint`. JSON field order and insignificant JSON formatting do not change identity; whitespace inside string values remains meaningful. Ignored extra fields do not change the effective validated payload.

This cheap request fingerprint is separate from the expensive fingerprint produced after PII redaction. Two different email addresses can redact to identical text and must still be detected as different requests.

In one short acceptance transaction:

1. Attempt `INSERT ... ON CONFLICT(tenant_id, idempotency_key) DO NOTHING`, persisting the request fingerprint and recoverable input together.
2. For a conflict, retrieve the existing row and immutable request fingerprint within the same transaction.
3. Resolve the result and commit or roll back before returning a response.

| Request | Response and effect |
|---|---|
| New tenant/key | Commit one durable job; return `202 Accepted` with its identity/status |
| Same tenant/key and same fingerprint | Return the existing acceptance/status with 200/202; create no new job or delivery |
| Same tenant/key and different fingerprint | Return `409 Conflict`; preserve the original job unchanged |
| Different tenant with the same key | Accept an independent job |

A concurrent conflicting pair has exactly one accepted original and one conflict. Never overwrite the accepted payload through an UPSERT update. Do not wait for CPU or downstream HTTP before acknowledgment. If persistence fails, do not return a successful acceptance.

## 6. SQLite configuration and transaction boundaries

- Enable and verify `PRAGMA journal_mode=WAL` during initialization. WAL still permits only one writer.
- Start with `PRAGMA busy_timeout=100` milliseconds on every connection. This is an initial tuning value, not a benchmarked optimum.
- Keep total ingestion connection/lock-wait and retry time below a 200 ms budget, leaving headroom for the 250 ms acknowledgment target. Bound retries by an absolute deadline and remaining budget rather than multiplying full timeout periods.
- The admission deadline ends once the write lock is acquired. Do not cancel a COMMIT already in progress; wait for its outcome before returning an acknowledgment. Full-burst p95 still must remain below 250 ms.
- Use short `BEGIN IMMEDIATE` transactions for multi-statement acceptance and claims. Commit or roll back promptly. Roll back before contention backoff.
- Give one coroutine exclusive connection ownership throughout each transaction. A shared aiosqlite connection does not by itself isolate concurrent application transactions.
- Use an explicit durability setting consistent with commit-before-acknowledgment, initially `synchronous=FULL`. Measure its cost in the full harness.

**Never perform network requests or CPU work inside a database transaction.** This includes request canonicalization/hashing, redaction, process-pool execution, HTTP calls, retry sleeps and waits for worker or connection capacity. Async code can still hold a database lock while suspended; `await` does not relax this rule.

| Stage | Transaction content | Work after transaction ends |
|---|---|---|
| Accept | Insert tenant/key/fingerprint/input or inspect conflict | Return acknowledgment/conflict |
| Claim event | Select and claim one due pending row; fetch its input/output | Run processor only if output is absent |
| Save preprocessing | Persist cleaned output/fingerprint and clear raw input; remain pending | Same worker sends downstream HTTP |
| Record outcome | Mark delivered/failed, or schedule retry while remaining pending; clear ownership | Same worker claims another event |

The four end-to-end worker slots bound CPU submissions and HTTP concurrency before any claim occurs. Lease renewal uses its own short transaction, and recovery is part of claiming. Prepare retry calculations outside the write transaction. Close/finalize cursors and release the transaction before leaving each database helper.

## 7. Worker, retry and recovery behavior

Use four fixed end-to-end worker loops and an HTTPX connection limit of four. Each loop handles one event at a time. CPU submissions and outbound requests are both bounded by the worker count. This deliberately trades maximum pipeline throughput for a smaller implementation that should fit the assessment load; verify the complete harness before accepting that trade-off.

### Atomic claim and recovery

Use a short BEGIN IMMEDIATE transaction: select one row with status=pending, next_attempt_at at or before now, and either no claim or an expired lease; update that row with a fresh token and lease, fetch it and commit. The write lock serializes competing claims. Return no job when none is eligible. This select-and-update is protected by the same write transaction; it is not an unprotected check followed by a later write.

Use conditional result/retry updates matching internal row ID, pending status and claim token. Before HTTP, check and extend the owned, unexpired lease in a short transaction; if ownership is lost, discard the local result and do not send. Never update all matching event_id rows.

Start with a 15-second lease and renew the small set of active claims every five seconds in one ordinary coroutine. Renewal must match the token and an unexpired lease; never resurrect an expired ownership token. This one loop is the minimum addition to prevent healthy long-running work from being reclaimed. There is no separate lease manager or recovery subsystem. If renewal fails, retain bounded local work, require a successful ownership check before sending, and surface the failure rather than silently assuming ownership.

After a crash, the same claim query recovers expired pending rows, skipping CPU work when cleaned output is already stored. Recovery can wait up to the lease period; the six-second normal-delivery window does not include a forced crash. Keep the duplicate-side-effect limitation under paused/crashed owners explicit below.

### Retry and polling defaults

| Setting | Initial value | Reason |
|---|---|---|
| End-to-end workers / CPU processes / HTTP limit | 4 / 4 / 4 | Bounded concurrency with no second pipeline |
| Idle polling | 25 ms, interruptible by stop event | Prompt pickup without a scheduling framework |
| First transient retry | 50-100 ms | Quick recovery from the mock's first-attempt 503 |
| Subsequent retries | Exponential backoff with bounded jitter; delay capped at 1 second | Durable retry scheduling without multi-second normal-test delays |
| HTTP connect/read/write/pool timeouts | 0.5 / 1.0 / 1.0 / 0.5 seconds | Finite waits; read timeout exceeds the mock's 250 ms processing delay |
| Lease / renewal interval | 15 / 5 seconds | Avoid reclaiming healthy work; keep recovery simple |

For attempt number n starting at 1, use a ceiling min(1 second, 100 ms * 2**(n-1)) and choose delay uniformly between half that ceiling and the ceiling. Saturate the exponent once the cap is reached. Persist attempt_count, next_attempt_at and last_error; clear the claim and remain pending. Return immediately to claiming other events rather than sleeping a worker until its failed event is due. Only an idle worker polls; SQL timestamps determine eligibility.

The mock can reject each event's first attempt only. Even if all 30 first attempts fail, at most 60 HTTP attempts at 250 ms each consume about 3.75 seconds of four-worker HTTP capacity. Add the measured approximately 247 ms for 30 CPU jobs, first-retry delay, polling and SQLite overhead. This suggests headroom inside six seconds, but is a sizing estimate, not a passing benchmark. Test both the original random-failure harness and a separate deterministic all-first-attempts-503 case; do not alter the original mock to make acceptance easier.

Retry confirmed 503 rejection and known pre-send connection failures. Permanent errors set status=failed with a useful last_error. During a 60-second outage, pending jobs remain on disk, retry delay saturates at one second, and active work stays bounded. Retryable failures are not dropped after a short attempt limit. CPU output is reused on delivery retries.

### Guarantees requiring downstream cooperation

Distinguish confirmed rejection or pre-send connection failure from ambiguous response loss after transmission. If downstream accepted an event and its response was lost, or the parent crashed before recording success, SQLite cannot determine that remote outcome atomically. Database claims prevent ordinary concurrent local delivery while ownership is valid; they cannot fence a remote side effect from an owner paused beyond its lease.

Without receiver deduplication or reconciliation, mark an observed ambiguous outcome failed with last_error indicating that delivery needs reconciliation; do not add another durable state. A crash can leave no such observation, so expired-claim recovery may replay a previously accepted remote event. Thus eventual replay after crashes and exactly-once remote effects cannot both be guaranteed by this service alone. If the receiver supports a stable tenant-scoped deduplication token, ambiguous attempts can be retried safely under that contract. Any receiver token must preserve tenant scope. The supplied mock supports neither deduplication nor outcome lookup; do not silently change it to hide these limits.

```mermaid
sequenceDiagram
    participant Sender
    participant API as FastAPI
    participant DB as SQLite
    participant Workers as Lifespan-managed workers
    participant CPU as Bounded process pool
    participant Receiver as Downstream
    Sender->>API: Tenant, key and payload
    Note over API: Validate and compute request fingerprint
    API->>DB: Short acceptance transaction
    DB-->>API: Commit / existing event / conflict
    API-->>Sender: 202 or existing acknowledgment; 409 on mismatch
    Workers->>DB: Short atomic pending-event claim; commit
    opt Cleaned output not yet stored
        Workers->>CPU: Original content, no transaction open
        CPU-->>Workers: Cleaned text and fingerprint
        Workers->>DB: Persist output, remain pending; commit
    end
    Workers->>DB: Check and extend owned lease; commit
    Workers->>Receiver: HTTP delivery, no transaction open
    Receiver-->>Workers: Success or failure
    Workers->>DB: Delivered / failed, or pending with retry time; commit
```

## 8. Files and implementation sequence

| Order | File | Planned work |
|---|---|---|
| 1 | `app/database.py` | Target-schema initialization/validation, connection settings, tenant uniqueness, atomic acceptance/claims and conditional updates |
| 2 | `app/main.py` | Small canonical-request fingerprint helper, fast acknowledgment and direct lifespan ownership of resources |
| 3 | `app/processor.py` | Preserve existing algorithm; keep it importable/picklable |
| 4 | `app/downstream.py` | Reuse the lifespan client, preserve payload compatibility, classify outcomes |
| 5 | `app/workers.py` (new) | Four ordinary worker loops, a small lease-renewal loop, retry calculation and stop/drain handling |
| 6 | `tests/` | Meaningful concurrency, conflict, recovery and lifecycle coverage while retaining the original burst test |
| 7 | `README.md`, `DECISIONS.md` | uv commands, defaults, measured results, explicit unsupported-schema handling and guarantee limits |

Use one ASGI worker for the assessment baseline. With four ASGI workers, each lifespan owns a separate pool/client and worker set. Four pools of four processes create 16 CPU processes and can oversubscribe this host. Divide a deployment-wide CPU/HTTP budget across ASGI workers; retain SQLite uniqueness and atomic claims for coordination. Document SQLite's single-writer scaling limit.

## 9. Verification and completion checklist

Run from this backend folder; no frontend service is required:

```sh
uv sync --locked --extra dev
uv run --extra dev python -m pytest -v -s
uv run --extra dev python .repro/measure_burst.py --output .repro/implementation-burst-results.json
uv run --extra dev python -m pytest tests/test_recovery.py -q -s
```

Run server-based checks sequentially so ports 8901/9001 do not collide. Preserve the original baseline reproduction artifacts or save post-change measurements separately. Re-run the CPU benchmark only if the processor, executor design or target environment changes.

- [x] Original burst test passes unchanged: all responses 200/202, ingestion p95 <250 ms, every health probe <50 ms, 30 database rows, 30 unique/total downstream acceptances and all rows delivered.
- [x] Same-tenant simultaneous duplicates create one job; duplicates after delivery remain deduplicated.
- [x] Same tenant/key with changed content or event ID returns 409 without changing the original job; include different original emails that redact identically.
- [x] Different tenants can independently use the same key, including under concurrent load.
- [x] Canonically equivalent validated requests produce identical request fingerprints.
- [x] A unique 503-failed event retries to delivered without requiring a duplicate sender request.
- [x] All 30 first attempts can fail with 503 and still settle inside six seconds under the selected polling/retry defaults.
- [x] Delivery retries reuse stored processor output.
- [x] A 60-second outage retains accepted jobs and resumes delivery after recovery.
- [x] Restart after committed acknowledgment recovers unfinished work; concurrent workers cannot claim the same live job.
- [x] Lease renewal preserves live claims, stale tokens cannot change outcomes, and expired claims are recovered through the normal claim path.
- [x] Slow HTTP and CPU operations hold no transaction; lock contention is bounded and never produces a false success acknowledgment.
- [x] Normal lifespan exit leaves no live worker tasks, HTTP clients, database resources or pool children; startup failure also cleans up acquired resources.
- [x] Shutdown during CPU work accounts for the still-running child future; cancellation does not release ownership prematurely.
- [x] Hard-kill recovery and ambiguous HTTP outcomes match the documented guarantee boundary.
- [x] Two application instances sharing SQLite preserve tenant isolation and uniqueness with a divided CPU budget; four-ASGI-process performance is not benchmarked.
- [x] Unsupported schemas fail clearly without modifying or deleting existing data; a fresh database initializes directly.
- [x] `DECISIONS.md` contains measured results and actual implemented behavior for all four questions.

Verification completed: 36 tests passed in 73.14 seconds. The unchanged burst diagnostic recorded p95 acknowledgment 28.20 ms, maximum health latency 18.97 ms, and exactly 30 delivered rows/remote acceptances. A separate all-first-attempts-503 HTTP burst recorded p95 72.15 ms, maximum health 12.68 ms and 4.17-second delivery settlement. See .repro/implementation-tests.log and .repro/implementation-burst-results.json. The isolated CPU benchmark was used only to select the executor.

## References

- [FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/)
- [Python executor shutdown semantics](https://docs.python.org/3.13/library/concurrent.futures.html#concurrent.futures.Executor.shutdown)
- [SQLite WAL](https://www.sqlite.org/wal.html), [busy timeout](https://www.sqlite.org/pragma.html#pragma_busy_timeout), and [transactions](https://www.sqlite.org/lang_transaction.html)
- [HTTPX client lifetime and connection reuse](https://www.python-httpx.org/async/#opening-and-closing-clients)
