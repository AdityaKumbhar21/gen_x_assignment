import asyncio
from contextlib import AsyncExitStack
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

import httpx
import pytest

from app.database import Database
import app.main as main
from app.processor import scrub_and_fingerprint
import app.workers as workers
from test_application import wait_delivered


def slow_processor(content):
    time.sleep(0.3)
    return scrub_and_fingerprint(content)


@pytest.fixture
def configured(monkeypatch, tmp_path):
    path = str(tmp_path / 'recovery.db')
    monkeypatch.setenv('WEBHOOK_DB_PATH', path)
    monkeypatch.setenv('WEBHOOK_JOB_WORKERS', '2')
    return path


async def insert(app, key='key'):
    return await app.state.db.accept(event_id=key, tenant_id='tenant', idempotency_key=key,
                                     request_fingerprint=key, content='user@example.com')


async def until(predicate, timeout=4):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


async def test_shutdown_during_cpu_joins_child_and_restart_reprocesses(configured, monkeypatch):
    monkeypatch.setenv('WEBHOOK_SHUTDOWN_GRACE_SECONDS', '0.01')
    monkeypatch.setattr(workers, 'scrub_and_fingerprint', slow_processor)
    received = []

    async def receiver(request):
        received.append(json.loads(request.content))
        return httpx.Response(200)

    app = main.create_app(downstream_transport=httpx.MockTransport(receiver))
    async with app.router.lifespan_context(app):
        await insert(app)
        await until(lambda: bool(app.state.cpu_futures))
        child_handles = list(app.state.pool._processes.values())
    assert all(not child.is_alive() for child in child_handles)
    assert not app.state.cpu_futures
    assert not received
    with sqlite3.connect(configured) as db:
        assert db.execute('SELECT status,claim_token,raw_content FROM events').fetchone() == (
            'pending', None, 'user@example.com')
    # Restart uses the same durable database without another sender request.
    monkeypatch.setattr(workers, 'scrub_and_fingerprint', scrub_and_fingerprint)
    restarted = main.create_app(downstream_transport=httpx.MockTransport(receiver))
    async with restarted.router.lifespan_context(restarted):
        await wait_delivered(restarted, 1)
    assert len(received) == 1
    assert received[0]['text'] == '[REDACTED]'


async def test_shutdown_during_http_records_uncertain_outcome(configured, monkeypatch):
    monkeypatch.setenv('WEBHOOK_SHUTDOWN_GRACE_SECONDS', '0.01')
    sending = asyncio.Event()

    async def receiver(request):
        sending.set()
        await asyncio.Event().wait()

    app = main.create_app(downstream_transport=httpx.MockTransport(receiver))
    async with app.router.lifespan_context(app):
        await insert(app)
        await asyncio.wait_for(sending.wait(), 3)
    with sqlite3.connect(configured) as db:
        status, error, token = db.execute('SELECT status,last_error,claim_token FROM events').fetchone()
    assert status == 'failed'
    assert 'outcome unknown' in error
    assert token is None


async def test_renewal_keeps_slow_cpu_claim_owned(configured, monkeypatch):
    monkeypatch.setenv('WEBHOOK_LEASE_SECONDS', '0.2')
    monkeypatch.setenv('WEBHOOK_RENEW_INTERVAL', '0.04')
    monkeypatch.setattr(workers, 'scrub_and_fingerprint', slow_processor)
    app = main.create_app(downstream_transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    async with app.router.lifespan_context(app):
        await insert(app)
        await until(lambda: bool(app.state.cpu_futures))
        await asyncio.sleep(0.23)
        other = await Database.open(configured)
        try:
            assert await other.claim(1) is None
        finally:
            await other.close()
        await wait_delivered(app, 1)


async def test_abrupt_process_exit_preserves_committed_job(configured):
    script = """
import asyncio, os, sys
from app.database import Database
async def run():
    db = await Database.open(sys.argv[1])
    await db.accept(event_id='crashed', tenant_id='tenant', idempotency_key='key',
                    request_fingerprint='original', content='user@example.com')
    await db.claim(0.05)
    os._exit(0)  # Deliberately bypass close/rollback/finally after both commits.
asyncio.run(run())
"""
    process = await asyncio.create_subprocess_exec(sys.executable, '-c', script, configured)
    assert await asyncio.wait_for(process.wait(), 5) == 0
    received = []

    async def receiver(request):
        received.append(json.loads(request.content))
        return httpx.Response(200)

    app = main.create_app(downstream_transport=httpx.MockTransport(receiver))
    async with app.router.lifespan_context(app):
        rows = await wait_delivered(app, 1)
    assert len(received) == 1
    assert rows[0]['request_fingerprint'] == 'original'


async def test_two_application_instances_share_claims_without_duplicate_delivery(configured):
    accepted = []

    async def receiver(request):
        await asyncio.sleep(0.01)
        accepted.append(request.headers['X-Idempotency-Key'])
        return httpx.Response(200)

    apps = [main.create_app(downstream_transport=httpx.MockTransport(receiver)) for _ in range(2)]
    async with AsyncExitStack() as stack:
        for app in apps:
            await stack.enter_async_context(app.router.lifespan_context(app))
        clients = [await stack.enter_async_context(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url='http://app')) for app in apps]

        async def send(index, client):
            return await client.post('/webhooks/ingest', headers={'X-Idempotency-Key': str(index)},
                                     json=dict(event_id=str(index), tenant_id='tenant', content='raw'))

        responses = await asyncio.gather(*(send(index, client) for index in range(30) for client in clients))
        assert all(response.status_code == 202 for response in responses)
        await wait_delivered(apps[0], 30)
        assert len(accepted) == len(set(accepted)) == 30


async def test_unexpected_worker_exit_makes_health_and_ingestion_unavailable(configured):
    app = main.create_app()
    async with app.router.lifespan_context(app):
        app.state.workers[0].cancel()
        await until(lambda: not app.state.healthy)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://app') as client:
            assert (await client.get('/health')).status_code == 503
            response = await client.post('/webhooks/ingest', headers={'X-Idempotency-Key': 'key'},
                                          json=dict(event_id='event', tenant_id='tenant', content='raw'))
            assert response.status_code == 503
            assert await app.state.db.rows() == []


async def test_sixty_second_outage_retains_work_and_recovers(configured):
    outage = True
    attempts = 0

    async def receiver(request):
        nonlocal attempts
        attempts += 1
        return httpx.Response(503 if outage else 200)

    app = main.create_app(downstream_transport=httpx.MockTransport(receiver))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://app') as client:
            for key in ('initial', 'late'):
                start = time.perf_counter()
                response = await client.post('/webhooks/ingest', headers={'X-Idempotency-Key': key},
                    json=dict(event_id=key, tenant_id='tenant', content='user@example.com'))
                assert response.status_code == 202
                assert time.perf_counter() - start < 0.25
                if key == 'initial':
                    await asyncio.sleep(60)
                    rows = await app.state.db.rows()
                    assert rows[0]['status'] == 'pending'
                    assert rows[0]['cleaned_text'] == '[REDACTED]'
                    assert rows[0]['raw_content'] is None
                    assert 5 < attempts < 140  # Backoff, no busy-looping or dropped job.
            assert (await client.get('/health')).status_code == 200
        outage = False
        delivered = await wait_delivered(app, 2)
        assert sum(row['attempt_count'] for row in delivered) == attempts


async def test_all_first_attempts_503_meet_unchanged_latency_and_delivery_targets(tmp_path):
    """Independent HTTP processes, with stricter deterministic failure than the mock."""
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ, WEBHOOK_DB_PATH=str(tmp_path / 'worst-case.db'),
               DOWNSTREAM_URL='http://127.0.0.1:9003/events', WEBHOOK_JOB_WORKERS='4')
    commands = [
        [sys.executable, '-m', 'uvicorn', 'support_receiver:app', '--app-dir', 'tests',
         '--host', '127.0.0.1', '--port', '9003', '--log-level', 'warning'],
        [sys.executable, '-m', 'uvicorn', 'app.main:app', '--host', '127.0.0.1',
         '--port', '8903', '--log-level', 'warning'],
    ]
    processes = [subprocess.Popen(command, cwd=root, env=env) for command in commands]
    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            for url in ('http://127.0.0.1:9003/stats', 'http://127.0.0.1:8903/health'):
                async with asyncio.timeout(10):
                    while True:
                        try:
                            if (await client.get(url)).status_code == 200:
                                break
                        except httpx.ConnectError:
                            pass
                        await asyncio.sleep(0.025)
            stop = asyncio.Event()
            health_times = []

            async def probe():
                while not stop.is_set():
                    start = time.perf_counter()
                    assert (await client.get('http://127.0.0.1:8903/health')).status_code == 200
                    health_times.append((time.perf_counter() - start) * 1000)
                    await asyncio.sleep(0.05)

            async def send(index):
                start = time.perf_counter()
                response = await client.post('http://127.0.0.1:8903/webhooks/ingest',
                    headers={'X-Idempotency-Key': str(index)},
                    json=dict(event_id=str(index), tenant_id=f'tenant-{index % 3}',
                              content=f'user{index}@example.com SSN 123-45-6789'))
                return response.status_code, (time.perf_counter() - start) * 1000

            probe_task = asyncio.create_task(probe())
            try:
                responses = await asyncio.gather(*(send(index) for index in range(30) for _ in range(2)))
                assert all(code in (200, 202) for code, _ in responses)
                p95 = sorted(latency for _, latency in responses)[56]
                assert p95 < 250
                start = time.monotonic()
                async with asyncio.timeout(6):
                    while True:
                        stats = (await client.get('http://127.0.0.1:9003/stats')).json()
                        if len(stats['accepted']) == 30:
                            break
                        await asyncio.sleep(0.025)
                assert len({item['event_id'] for item in stats['accepted']}) == 30
                assert set(stats['attempts'].values()) == {2}
                assert max(health_times) < 50
                with sqlite3.connect(env['WEBHOOK_DB_PATH']) as db:
                    rows = db.execute('SELECT status,raw_content,cleaned_text FROM events').fetchall()
                assert len(rows) == 30
                assert all(status == 'delivered' and raw is None and '@example.com' not in clean
                           and '123-45-6789' not in clean for status, raw, clean in rows)
                print(f'All-first-503: p95={p95:.2f}ms max_health={max(health_times):.2f}ms '
                      f'delivery_settle={time.monotonic() - start:.2f}s')
            finally:
                stop.set()
                await probe_task
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            process.wait(timeout=5)
