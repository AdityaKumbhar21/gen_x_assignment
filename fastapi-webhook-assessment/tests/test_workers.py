import asyncio
from concurrent.futures import ProcessPoolExecutor
import json
import multiprocessing
from types import SimpleNamespace

import httpx
import pytest

from app.database import Database
from app.downstream import DeliveryError, forward_event
from app.workers import retry_delay, worker_loop


class CountingPool:
    def __init__(self, pool):
        self.pool = pool
        self.submissions = 0

    def submit(self, *args, **kwargs):
        self.submissions += 1
        return self.pool.submit(*args, **kwargs)


@pytest.fixture
async def state(tmp_path):
    db = await Database.open(str(tmp_path / 'jobs.db'))
    pool = ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context('spawn'))
    state = SimpleNamespace(db=db, pool=CountingPool(pool), client=None,
                            downstream_url='http://receiver/events', stop=asyncio.Event(),
                            renew_stop=asyncio.Event(), active={}, cpu_futures=set(),
                            lease_seconds=15, poll_interval=0.025, renew_interval=5)
    try:
        yield state
    finally:
        await asyncio.to_thread(pool.shutdown, wait=True, cancel_futures=True)
        await db.close()


async def insert(state, *, tenant='t', key='k'):
    return await state.db.accept(event_id='event', tenant_id=tenant, idempotency_key=key,
                                 request_fingerprint='request', content='Email user@example.com')


async def wait_status(db, status, timeout=4):
    async with asyncio.timeout(timeout):
        while True:
            rows = await db.rows()
            if rows and all(row['status'] == status for row in rows):
                return rows
            await asyncio.sleep(0.01)


async def test_retry_reuses_preprocessing_and_releases_transaction(state):
    attempts = []

    async def receive(request):
        assert not state.db.connection.in_transaction
        attempts.append(json.loads(request.content))
        return httpx.Response(503 if len(attempts) == 1 else 200)

    await insert(state)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        state.client = client
        task = asyncio.create_task(worker_loop(state))
        try:
            rows = await wait_status(state.db, 'delivered')
        finally:
            state.stop.set()
            await task
    assert len(attempts) == 2
    assert attempts[0] == attempts[1]
    assert attempts[0]['text'] == 'Email [REDACTED]'
    assert state.pool.submissions == 1
    assert rows[0]['attempt_count'] == 2
    assert rows[0]['raw_content'] is None


async def test_slow_http_does_not_hold_transaction_or_block_acceptance(state):
    sending, release = asyncio.Event(), asyncio.Event()

    async def receive(request):
        sending.set()
        await release.wait()
        return httpx.Response(200)

    await insert(state)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        state.client = client
        task = asyncio.create_task(worker_loop(state))
        try:
            await asyncio.wait_for(sending.wait(), 3)
            assert not state.db.connection.in_transaction
            await asyncio.wait_for(insert(state, key='other'), 0.2)
        finally:
            state.stop.set()
            release.set()
            await task
    assert len(await state.db.rows()) == 2


@pytest.mark.parametrize('failure,retryable', [(httpx.ConnectError, True),
                                              (httpx.ReadTimeout, False)])
async def test_delivery_error_classification(failure, retryable):
    async def receive(request):
        raise failure('simulated', request=request)

    job = dict(tenant_id='t', idempotency_key='k', event_id='event',
               cleaned_text='clean', fingerprint='fp')
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        with pytest.raises(DeliveryError) as caught:
            await forward_event(client, 'http://receiver/events', job)
    assert caught.value.retryable is retryable


async def test_permanent_error_stops_retries_and_scopes_receiver_key(state):
    tokens = []

    async def receive(request):
        tokens.append(request.headers['X-Idempotency-Key'])
        return httpx.Response(400)

    await insert(state)
    await insert(state, tenant='other')
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        state.client = client
        task = asyncio.create_task(worker_loop(state))
        try:
            rows = await wait_status(state.db, 'failed')
            await asyncio.sleep(0.15)
        finally:
            state.stop.set()
            await task
    assert len(set(tokens)) == 2
    assert all(row['attempt_count'] == 1 for row in rows)


def test_retry_delays_are_bounded_for_first_and_later_failures():
    for _ in range(50):
        assert 0.05 <= retry_delay(1) <= 0.1
        assert 0.5 <= retry_delay(1_000_000) <= 1
