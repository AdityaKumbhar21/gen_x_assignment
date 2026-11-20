"""Measure every acceptance criterion without stopping at the first failure.

Run from the backend folder: uv run --extra dev python .repro/measure_burst.py
Uses the original test fixture and its unmodified application processes.
"""

import asyncio
import argparse
from collections import Counter
import json
from pathlib import Path
import runpy
import sqlite3
import tempfile
import time

import httpx


async def main(output):
    original = runpy.run_path("tests/test_burst_load.py")
    with tempfile.TemporaryDirectory(prefix="burst-", dir=".repro") as temp:
        services = original["running_services"].__wrapped__(Path(temp))
        database = await anext(services)
        try:
            health_times = []
            ingest_times = []
            stop = asyncio.Event()
            async with httpx.AsyncClient(timeout=15) as client:
                await client.post("http://127.0.0.1:9001/reset")

                async def probe():
                    while not stop.is_set():
                        started = time.perf_counter()
                        response = await client.get("http://127.0.0.1:8901/health")
                        response.raise_for_status()
                        health_times.append((time.perf_counter() - started) * 1000)
                        await asyncio.sleep(0.05)

                async def send(index):
                    started = time.perf_counter()
                    response = await client.post(
                        "http://127.0.0.1:8901/webhooks/ingest",
                        headers={"X-Idempotency-Key": f"idem_{index:03d}"},
                        json={
                            "event_id": f"evt_{index:03d}",
                            "tenant_id": f"tenant_{index % 3}",
                            "content": f"Customer user{index}@example.com SSN 123-45-6789 submitted ticket #{index}",
                        },
                    )
                    ingest_times.append((time.perf_counter() - started) * 1000)
                    return response.status_code

                probe_task = asyncio.create_task(probe())
                try:
                    statuses = await asyncio.gather(*(send(i) for i in range(30) for _ in range(2)))
                finally:
                    stop.set()
                    await probe_task
                await asyncio.sleep(6)
                stats = (await client.get("http://127.0.0.1:9001/stats")).json()

            with sqlite3.connect(database) as connection:
                rows = connection.execute("SELECT event_id, cleaned_text, status FROM events").fetchall()
            ordered = sorted(ingest_times)
            report = {
                "requests": 60,
                "expected_unique_events": 30,
                "http_status_counts": dict(Counter(statuses)),
                "ingest_p95_ms": round(ordered[int(len(ordered) * 0.95) - 1], 2),
                "ingest_target_ms": 250,
                "health_max_ms": round(max(health_times), 2),
                "health_target_ms": 50,
                "database_rows": len(rows),
                "database_unique_event_ids": len({row[0] for row in rows}),
                "database_status_counts": dict(Counter(row[2] for row in rows)),
                "downstream_total_accepted": stats["total_accepted"],
                "downstream_unique_events": stats["unique_events"],
                "pii_redaction_passed": all("123-45-6789" not in row[1] and "@example.com" not in row[1] for row in rows),
                "settle_wait_seconds": 6,
            }
            Path(output).write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(report, indent=2))
        finally:
            await services.aclose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='.repro/burst-results.json')
    asyncio.run(main(parser.parse_args().output))
