"""Observe a unique failed event without a duplicate masking its status."""

import asyncio
import json
from pathlib import Path
import runpy
import sqlite3
import tempfile

import httpx


async def main():
    fixture = runpy.run_path("tests/test_burst_load.py")["running_services"].__wrapped__
    with tempfile.TemporaryDirectory(prefix="retry-", dir=".repro") as temp:
        services = fixture(Path(temp))
        database = await anext(services)
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                failed_id = None
                for index in range(20):
                    event_id = f"retry_probe_{index}"
                    response = await client.post(
                        "http://127.0.0.1:8901/webhooks/ingest",
                        headers={"X-Idempotency-Key": event_id},
                        json={"event_id": event_id, "tenant_id": "test", "content": "Synthetic test event"},
                    )
                    if response.status_code == 500:
                        failed_id = event_id
                        break
                await asyncio.sleep(6)
                stats = (await client.get("http://127.0.0.1:9001/stats")).json()
            with sqlite3.connect(database) as connection:
                rows = connection.execute("SELECT event_id, status FROM events WHERE event_id = ?", (failed_id,)).fetchall()
            report = {
                "failed_event_id": failed_id,
                "settle_wait_seconds": 6,
                "database_rows_for_failed_event": rows,
                "failed_event_accepted_downstream": any(event["event_id"] == failed_id for event in stats["events"]),
                "note": "The existing mock fails randomly; null failed_event_id means no 503 occurred in 20 attempts.",
            }
            Path(".repro/no-retry-results.json").write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(report, indent=2))
        finally:
            await services.aclose()


asyncio.run(main())
