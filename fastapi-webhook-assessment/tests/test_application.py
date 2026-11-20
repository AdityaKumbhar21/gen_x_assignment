import asyncio
from concurrent.futures import ProcessPoolExecutor
import json
import time

import httpx
import pytest

import app.main as main


@pytest.fixture
def configured(monkeypatch, tmp_path):
    monkeypatch.setenv('WEBHOOK_DB_PATH', str(tmp_path / 'app.db'))
    monkeypatch.setenv('WEBHOOK_JOB_WORKERS', '2')


async def wait_delivered(app, count):
    async with asyncio.timeout(6):
        while True:
            rows = await app.state.db.rows()
            if len(rows) == count and all(row['status'] == 'delivered' for row in rows):
                return rows
            await asyncio.sleep(0.01)


async def test_api_tenant_identity_conflicts_and_validation(configured):
    accepted = []

    async def receiver(request):
        accepted.append(request.headers['X-Idempotency-Key'])
        return httpx.Response(200)

    app = main.create_app(downstream_transport=httpx.MockTransport(receiver))
    payload = dict(event_id='event', tenant_id='tenant-a', content='first@example.com')
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://app') as client:
            async def send(body):
                return await client.post('/webhooks/ingest', headers={'X-Idempotency-Key': 'key'}, json=body)

            responses = await asyncio.gather(*(send(payload) for _ in range(20)))
            assert all(response.status_code == 202 for response in responses)
            assert len({response.json()['job_id'] for response in responses}) == 1
            # Different original emails redact identically but must conflict.
            assert (await send(dict(payload, content='second@example.com'))).status_code == 409
            assert (await send(dict(payload, event_id='other'))).status_code == 409
            assert (await send(dict(payload, tenant_id='tenant-b'))).status_code == 202
            rows = await wait_delivered(app, 2)
            assert (await send(payload)).status_code == 202
            assert len(accepted) == 2
            assert len(set(accepted)) == 2
            assert all(row['raw_content'] is None for row in rows)
            assert all(row['cleaned_text'] == '[REDACTED]' for row in rows)
            assert (await client.post('/webhooks/ingest', json=payload)).status_code == 400
            assert (await client.post('/webhooks/ingest', json={})).status_code == 422


def test_canonical_fingerprint_preserves_original_values():
    left = main.WebhookPayload(event_id='é', tenant_id='tenant', content=' a@example.com ')
    right = main.WebhookPayload.model_validate_json(
        json.dumps(dict(content=' a@example.com ', tenant_id='tenant', event_id='é'), indent=4))
    assert main.request_fingerprint(left) == main.request_fingerprint(right)
    assert main.request_fingerprint(left) != main.request_fingerprint(left.model_copy(update={'content': 'a@example.com'}))


async def test_lifespan_joins_pool_closes_resources_and_bounds_submissions(configured, monkeypatch):
    pools = []

    class TrackedPool:
        def __init__(self, **kwargs):
            self.inner = ProcessPoolExecutor(**kwargs)
            self.pending = 0
            self.maximum = 0
            self.closed = False
            pools.append(self)

        def submit(self, *args):
            self.pending += 1
            self.maximum = max(self.maximum, self.pending)
            future = self.inner.submit(*args)
            future.add_done_callback(lambda _: setattr(self, 'pending', self.pending - 1))
            return future

        def shutdown(self, **kwargs):
            self.inner.shutdown(**kwargs)
            self.closed = True

    monkeypatch.setattr(main, 'ProcessPoolExecutor', TrackedPool)
    app = main.create_app(downstream_transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    async with app.router.lifespan_context(app):
        for index in range(12):
            await app.state.db.accept(event_id=str(index), tenant_id='tenant',
                                      idempotency_key=str(index), request_fingerprint=str(index), content='raw')
        await wait_delivered(app, 12)
        children = list(pools[0].inner._processes.values())
    assert pools[0].closed
    assert pools[0].maximum <= 2
    assert pools[0].pending == 0
    assert all(not child.is_alive() for child in children)
    assert app.state.client.is_closed
    assert not app.state.active
    assert all(task.done() for task in app.state.workers)
    assert app.state.renew_task.done()
    with pytest.raises(ValueError, match='no active connection'):
        await app.state.db.rows()


async def test_startup_warmup_failure_cleans_previously_opened_resources(configured, monkeypatch):
    class FailingPool:
        closed = False

        def __init__(self, **kwargs):
            pass

        def submit(self, *args):
            raise RuntimeError('warmup failed')

        def shutdown(self, **kwargs):
            self.closed = True

    monkeypatch.setattr(main, 'ProcessPoolExecutor', FailingPool)
    app = main.create_app()
    with pytest.raises(RuntimeError, match='warmup failed'):
        async with app.router.lifespan_context(app):
            pytest.fail('Application must not become ready')
    assert app.state.pool.closed
    assert app.state.client.is_closed
    with pytest.raises(ValueError, match='no active connection'):
        await app.state.db.rows()


@pytest.mark.parametrize('setting,value', [('WEBHOOK_JOB_WORKERS', '0'),
                                         ('WEBHOOK_POLL_INTERVAL', 'nan'),
                                         ('WEBHOOK_RENEW_INTERVAL', '15')])
async def test_invalid_configuration_fails_before_acquiring_resources(configured, monkeypatch, setting, value):
    monkeypatch.setenv(setting, value)
    app = main.create_app()
    with pytest.raises(ValueError, match='Invalid worker'):
        async with app.router.lifespan_context(app):
            pytest.fail('Invalid configuration must not start the app')
    assert not hasattr(app.state, 'pool')
    assert not hasattr(app.state, 'db')
