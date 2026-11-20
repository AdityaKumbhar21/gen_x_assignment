# CPU executor benchmark and proposed backend fix

The measurements below remain the benchmark record. Architecture notes describe an earlier draft; [implementations.md](../implementations.md) is the current plan, simplified to three durable states and four end-to-end workers with bounded CPU submissions.

## Constraints checked

- Use Python and SQLite only; no Redis, Celery, RabbitMQ, PostgreSQL, or external queue.
- Health probes must all finish in less than 50 ms during the supplied burst.
- Valid ingestion requests must return 200/202 with p95 below 250 ms.
- Concurrent retries with the same tenant and idempotency key must create one event and one downstream delivery in the assessment harness. The README does not explicitly prescribe global key uniqueness; the selected contract is UNIQUE(tenant_id, idempotency_key).
- Transient downstream 503 responses must be retried until every unique event reaches delivered status.
- Preserve the existing redaction and fingerprint computation.
- Complete DECISIONS.md when implementing the selected architecture, including 60-second outage, crash after acknowledgment, and four-ASGI-worker behavior. It remains an unanswered template today.

## Method

Command, from the backend directory:

```sh
uv run --extra dev python .repro/benchmark_cpu.py
```

Machine: Apple arm64, 10 logical CPUs, CPython 3.13.4 with the GIL enabled. The original function in app/processor.py was imported unchanged, including all 120,000 SHA-256 iterations. One direct call took a median 26.437 ms over nine samples.

Each executor configuration ran five 30-job bursts and five 60-job bursts. Thirty jobs represent the unique workload after successful deduplication; sixty represent the existing duplicate workload. Inputs match the original test. Every cleaned string and fingerprint was checked against direct execution.

The HTTP driver ran in a separate process from a single-Uvicorn-worker benchmark app and probed /health every 50 ms after the previous probe completed. At most the configured worker count of jobs could be submitted to each executor at once. Process pools used spawn and all pools were warmed before readiness. Warm-up was excluded from batch timing and recorded separately.

This isolates CPU scheduling and HTTP responsiveness. It excludes SQLite and downstream delivery, so it does not establish full-system acceptance. Configurations ran sequentially; timings are measurements of this host, not guarantees across deployment hardware or arbitrary payload sizes. Faster runs naturally contain fewer health probes.

## Results

Batch durations are medians; health latency is the maximum across every probe in all ten bursts.

| Execution | Workers | 30-job batch ms | 60-job batch ms | Max health ms | Probes >=50 ms / total |
|---|---:|---:|---:|---:|---:|
| Inline | 1 | 808.415 | 1627.799 | 1644.551 | 10 / 10 |
| Thread pool | 1 | 858.873 | 1722.978 | 17.190 | 0 / 228 |
| Process pool | 1 | 891.647 | 1817.792 | 7.497 | 0 / 268 |
| Thread pool | 2 | 850.055 | 1693.215 | 38.568 | 0 / 209 |
| Process pool | 2 | 458.521 | 911.414 | 1.482 | 0 / 136 |
| Thread pool | 4 | 826.970 | 1649.262 | 97.177 | 16 / 163 |
| Process pool | 4 | 246.759 | 474.956 | 2.034 | 0 / 74 |

Raw measurements, including each probe and startup time: cpu-benchmark-results.json. Server logs: cpu-inline-1.log, cpu-thread-*.log, and cpu-process-*.log. All benchmark servers and pools were stopped afterward.

## Decision

Select a reusable ProcessPoolExecutor with four workers for the measured single-ASGI-worker setup. Four processes were approximately 3.47 times as fast as four threads on the 60-job workload, with no health-target violations and a maximum health latency of 2.034 ms. Server startup including pool warm-up took 480.392 ms, so warm the pool during application lifespan before accepting requests.

One and two threads also passed this isolated health test. Their lack of throughput scaling and the four-thread health failures make the process pool the stronger choice here. The hashing loop repeatedly hashes 32-byte digests. Python documents GIL release for hashlib inputs larger than 2047 bytes per call; that release does not apply to these repeated digest inputs. See https://docs.python.org/3.13/library/hashlib.html.

Keep CPU worker count configurable for deployment hardware. Bound outstanding submissions as well as executor worker count. A process pool's max_workers alone does not bound its submitted backlog. Retain at most four active CPU jobs, with remaining work durable in SQLite.

## Proposed final fix

1. Validate the payload and header. Compute a deterministic request_fingerprint over the validated original event_id, tenant_id and content before beginning any database transaction. Atomically persist it with the tenant-scoped idempotency key and enough job input to recover after acknowledgment. Commit before returning 202. An existing (tenant_id, idempotency_key) with an identical request fingerprint returns the existing acknowledgment and creates no new work. A different fingerprint returns HTTP 409 Conflict without changing the accepted job.
2. Let SQLite enforce UNIQUE(tenant_id, idempotency_key) with a targeted ON CONFLICT(tenant_id, idempotency_key) DO NOTHING operation. For a conflict, read the existing request_fingerprint and compare it within the acceptance transaction; exactly one concurrent request may insert the job. Do not use an unprotected SELECT followed by INSERT or rely on an in-memory lock. Enable WAL at startup, configure a busy timeout on every connection, and use short transactions. Never perform network requests, CPU preprocessing, fingerprint computation, retry sleeps or waits for executor/HTTP capacity inside a transaction. WAL helps reader/writer concurrency but still allows only one writer.
3. Start a fixed number of worker tasks during lifespan. Atomically claim a job before running it. Submit unchanged preprocessing to the warmed process pool, persist its cleaned output and fingerprint, and clear any temporary raw job input after successful preprocessing.
4. Separate CPU capacity from HTTP capacity. Reuse one lifespan-managed httpx.AsyncClient with explicit connection limits and timeouts. Bound outbound deliveries independently; select the exact limit by rerunning the complete harness.
5. Persist retry attempts, errors and next_attempt_at. Use capped exponential backoff with jitter for transient 503/connection failures. Cap the delay, rather than abandoning all transient failures after a short retry budget. Reuse the processed payload on delivery retries. Delays and concurrency must allow the supplied burst to settle within its six-second delivery window.
6. Mark only the exact claimed row delivered after downstream confirms success. Use conditional state updates and worker-ownership tokens. Recover pending jobs and expired claims after restart; maintain or renew claims during live work to prevent ordinary concurrent workers taking the same event.
7. Shut down worker tasks, HTTP clients and the CPU pool cleanly. Across four ASGI workers, database uniqueness and claims remain necessary and pool counts multiply: four workers with four CPU processes each would create 16 CPU processes. Divide the CPU budget across workers instead of keeping the single-worker setting unchanged.
8. Migrate any existing database schema explicitly. CREATE TABLE IF NOT EXISTS cannot retrofit uniqueness or new job-state columns onto the existing table. Reconcile existing duplicates according to documented policy instead of silently deleting user data.
9. Run the existing acceptance test unchanged, then rerun the diagnostic burst and add focused restart, outage and concurrent-claim checks. Also verify identical retries within a tenant, concurrent conflicting payloads within a tenant, and independent reuse of the same key by different tenants. Populate DECISIONS.md with measured choices and actual behavior after implementation.

### Request fingerprint contract

Persist request_fingerprint as its own immutable, non-null field when accepting a new job. Compute a single SHA-256 digest of UTF-8 JSON containing the validated original fields event_id, tenant_id and content, using sort_keys=True, separators=(",", ":") and ensure_ascii=False. JSON object key order and insignificant request JSON whitespace therefore do not affect identity; string values remain exact, including whitespace inside content. Ignored extra fields do not affect the effective validated payload.

This request fingerprint is separate from the existing expensive fingerprint of redacted content. Hashing only redacted content would fail to detect changes such as two different email addresses that both become [REDACTED]. Persist the request fingerprint before acknowledgment, retain it after temporary raw input is removed, and never replace it when a conflicting retry arrives.

| Existing tenant/key | Incoming payload | Outcome |
|---|---|---|
| None | Valid | Commit a new job and return 202 |
| Same tenant/key | Same request fingerprint | Return existing acknowledgment; no new processing or delivery |
| Same tenant/key | Different request fingerprint | Return 409; preserve original job |
| Different tenant, same key | Any valid payload | Independent new job |

Worker ownership and delivery status updates use the internal row identity. Any downstream idempotency token must also preserve tenant scope; forwarding a bare idempotency key would incorrectly merge unrelated tenants at a deduplicating receiver. An exact downstream contract must be documented separately because the supplied mock currently keys its failure simulation only by event_id and performs no deduplication.

### SQLite configuration and transaction boundaries

Enable PRAGMA journal_mode=WAL once during startup/database initialization, before accepting traffic. Set PRAGMA busy_timeout=100 on every application connection as the initial lock-wait setting, in milliseconds. This is a starting value to validate with the complete burst test, not a measured optimum. Keep any contention retries bounded by a total ingestion wait budget below 200 ms so a multi-second lock wait cannot consume the 250 ms acknowledgment budget. If a retry is required, roll back and release the transaction before waiting. WAL and busy_timeout do not replace uniqueness constraints or atomic claims.

Use BEGIN IMMEDIATE for the short multi-statement acceptance and job-claim write transactions, then promptly COMMIT or ROLLBACK. Give one coroutine exclusive ownership of its connection for the transaction so unrelated tasks cannot interleave statements on a shared connection. Never hold a transaction while awaiting application work.

The required boundaries are:

1. Outside a transaction: validate and canonicalize the request, compute request_fingerprint, and prepare job input.
2. Acceptance transaction: insert the tenant/key/fingerprint/job input, or inspect the existing immutable fingerprint and identity. Commit or roll back before constructing and returning the acknowledgment or conflict response.
3. Outside a transaction: acquire CPU worker capacity. In a short claim transaction, atomically claim a pending job and fetch its input; commit before submitting preprocessing to the process pool.
4. Outside a transaction: run all redaction and hashing. In a separate short transaction, conditionally persist cleaned output and its fingerprint for the owned job and clear temporary raw input; commit.
5. Outside a transaction: acquire outbound capacity. In a short transaction, claim ready delivery work; commit. Then make the downstream HTTP request with no database transaction open.
6. Outside a transaction: calculate any retry delay and next attempt time. In a separate short transaction, conditionally mark the owned row delivered or record retry state; commit. Any retry sleep occurs afterward.

Worker-lease renewal, recovery scans and status transitions likewise use independent short transactions. An asynchronous HTTP call or awaited process-pool future still violates this rule if a database transaction remains open while it runs.

SQLite references: https://www.sqlite.org/wal.html, https://www.sqlite.org/pragma.html#pragma_busy_timeout, and https://www.sqlite.org/lang_transaction.html.

During a 60-second downstream outage, accepted work should remain durable with bounded active HTTP attempts and scheduled retries. A process crash after acknowledgment should leave recoverable work because acceptance was committed first.

There is a limit to the strict downstream guarantee: if downstream accepts an event and its response is lost, SQLite cannot atomically commit that remote HTTP side effect and its local delivered status. Crash/timeout-safe exactly-once remote effects require downstream deduplication using a stable key or a receiver lookup/reconciliation protocol. The provided mock currently implements neither. Database claims and safe retries can satisfy its ordinary concurrent-duplicate/transient-503 scenarios, but the implementation must document this wider boundary rather than promise an impossible guarantee.

No application changes were made as part of this benchmark; the fix above is the selected design. Full acceptance remains to be verified after implementation.
