# Backend reproduction

This is the original failing-baseline record. The backend has since been implemented; current validation is documented in `../README.md` and `../DECISIONS.md`. Post-change metrics are in `implementation-burst-results.json`; the original `burst-results.json` is preserved. Run `uv run --extra dev python .repro/measure_burst.py --output .repro/implementation-burst-results.json` to keep that baseline intact. The old `measure_no_retry.py` probe looks for synchronous HTTP 500s; use `tests/test_recovery.py` for current asynchronous retry assertions.

Run these commands from `fastapi-webhook-assessment`. Dependencies are managed by uv in this folder's `.venv`; versions are recorded in `uv.lock`. No Next.js dependencies or services are required.

```sh
uv sync --locked --extra dev
uv run --extra dev python -m pytest -v -s
uv run --extra dev python .repro/measure_burst.py
uv run --extra dev python .repro/measure_no_retry.py
```

The tests and diagnostic scripts start the original ingestion and mock downstream processes on ports 8901 and 9001 and stop them afterward. Keep those ports free and run these commands sequentially. Each diagnostic uses a temporary database and leaves application code unchanged.

## Recorded results

- Original suite: one failed test; 53 HTTP 200 responses and seven HTTP 500 responses. See `pytest.log`.
- Diagnostic burst: 60 requests, 30 unique event IDs, ingestion p95 3345.81 ms (target below 250 ms), maximum health latency 1522.01 ms (target below 50 ms).
- SQLite contained 60 rows rather than 30. Downstream accepted 53 deliveries for 30 unique events, including 23 duplicate deliveries.
- All 60 rows were marked delivered: the event-ID-only update also updates the duplicate row whose own forwarding attempt failed. This masks individual failed attempts.
- PII redaction passed for the sample inputs.
- Separate unique-event failure: `retry_probe_4` received HTTP 500 following downstream HTTP 503. After six seconds, its row was still pending and it had not been accepted downstream.

Machine-readable results: `burst-results.json` and `no-retry-results.json`. Tracebacks: `burst.log` and `no-retry.log`.

The mock downstream injects random first-attempt failures. Counts and timings vary between runs. The no-retry diagnostic tries up to 20 unique events to observe one failure.
