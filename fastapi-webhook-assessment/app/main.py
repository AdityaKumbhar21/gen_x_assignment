import asyncio
from concurrent.futures import ProcessPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager
import hashlib
import json
import logging
import math
import multiprocessing
import os

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from app.database import Database, IdempotencyConflict, StorageBusy
from app.processor import scrub_and_fingerprint
from app.workers import renew_loop, worker_loop

logger = logging.getLogger(__name__)


class WebhookPayload(BaseModel):
    event_id: str
    tenant_id: str
    content: str


def request_fingerprint(payload: WebhookPayload) -> str:
    original = json.dumps(payload.model_dump(), sort_keys=True,
                          separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    return hashlib.sha256(original).hexdigest()


async def stop_workers(state):
    state.stop.set()
    if state.workers:
        _, pending = await asyncio.wait(state.workers, timeout=state.shutdown_grace)
        for task in pending:
            task.cancel()
        await asyncio.gather(*state.workers, return_exceptions=True)


async def close_pool(state):
    try:
        # Joining running child calls is blocking, so keep the event loop/renewal live.
        await asyncio.to_thread(state.pool.shutdown, wait=True, cancel_futures=True)
        if state.cpu_futures:
            await asyncio.gather(*list(state.cpu_futures), return_exceptions=True)
    finally:
        state.renew_stop.set()
        if state.renew_task is not None:
            await asyncio.gather(state.renew_task, return_exceptions=True)
        # Only CPU-only cancelled work remains here; HTTP cancellation was recorded.
        for job in list(state.active.values()):
            await state.db.finish(job, status='pending')
        state.active.clear()


def create_app(*, downstream_transport=None):
    @asynccontextmanager
    async def lifespan(app):
        state = app.state
        workers = int(os.getenv('WEBHOOK_JOB_WORKERS', '4'))
        state.poll_interval = float(os.getenv('WEBHOOK_POLL_INTERVAL', '0.025'))
        state.lease_seconds = float(os.getenv('WEBHOOK_LEASE_SECONDS', '15'))
        state.renew_interval = float(os.getenv('WEBHOOK_RENEW_INTERVAL', '5'))
        state.shutdown_grace = float(os.getenv('WEBHOOK_SHUTDOWN_GRACE_SECONDS', '3'))
        timings = (state.poll_interval, state.lease_seconds, state.renew_interval, state.shutdown_grace)
        if (not all(math.isfinite(value) for value in timings) or
                workers < 1 or state.poll_interval <= 0 or state.shutdown_grace <= 0 or
                not 0 < state.renew_interval < state.lease_seconds):
            raise ValueError('Invalid worker, polling, lease or shutdown configuration')
        state.stop = asyncio.Event()
        state.renew_stop = asyncio.Event()
        state.workers = []
        state.renew_task = None
        state.cpu_futures = set()
        state.active = {}
        state.healthy = True
        state.downstream_url = os.getenv('DOWNSTREAM_URL', 'http://127.0.0.1:9001/events')
        async with AsyncExitStack() as stack:
            state.db = await Database.open(os.getenv('WEBHOOK_DB_PATH', 'webhooks.db'))
            stack.push_async_callback(state.db.close)
            state.client = await stack.enter_async_context(httpx.AsyncClient(
                timeout=httpx.Timeout(connect=0.5, read=1.0, write=1.0, pool=0.5),
                limits=httpx.Limits(max_connections=workers, max_keepalive_connections=workers),
                transport=downstream_transport, trust_env=False,
            ))
            state.pool = ProcessPoolExecutor(max_workers=workers,
                                            mp_context=multiprocessing.get_context('spawn'))
            stack.push_async_callback(close_pool, state)
            # Register before awaiting warm-up, so partial startup failure also cleans up.
            loop = asyncio.get_running_loop()
            await asyncio.gather(*(loop.run_in_executor(state.pool, scrub_and_fingerprint,
                                                        'startup warmup') for _ in range(workers)))
            stack.push_async_callback(stop_workers, state)

            def observe(task):
                if state.stop.is_set():
                    return
                state.healthy = False
                if task.cancelled():
                    logger.error('Required webhook task was cancelled: %s', task.get_name())
                else:
                    error = task.exception()
                    logger.error('Required webhook task exited: %s', task.get_name(),
                                 exc_info=(type(error), error, error.__traceback__) if error else None)

            state.renew_task = asyncio.create_task(renew_loop(state), name='webhook-renewal')
            state.renew_task.add_done_callback(observe)
            for index in range(workers):
                task = asyncio.create_task(worker_loop(state), name=f'webhook-worker-{index}')
                task.add_done_callback(observe)
                state.workers.append(task)
            yield

    app = FastAPI(title='Webhook Ingestion Service', lifespan=lifespan)

    @app.get('/health')
    async def health(request: Request):
        state = request.app.state
        if not state.healthy or state.stop.is_set():
            raise HTTPException(status_code=503, detail='Webhook workers are unavailable')
        return {'status': 'ok'}

    @app.post('/webhooks/ingest', status_code=202)
    async def ingest_webhook(payload: WebhookPayload, request: Request,
                             x_idempotency_key: str | None = Header(default=None)):
        if not x_idempotency_key:
            raise HTTPException(status_code=400, detail='Missing X-Idempotency-Key header')
        if not request.app.state.healthy or request.app.state.stop.is_set():
            raise HTTPException(status_code=503, detail='Webhook workers are unavailable')
        fingerprint = request_fingerprint(payload)  # Before opening any transaction.
        try:
            event = await request.app.state.db.accept(
                event_id=payload.event_id, tenant_id=payload.tenant_id,
                idempotency_key=x_idempotency_key, request_fingerprint=fingerprint,
                content=payload.content,
            )
        except IdempotencyConflict:
            raise HTTPException(status_code=409, detail='Idempotency key already used with a different payload') from None
        except StorageBusy:
            raise HTTPException(status_code=503, detail='Unable to durably accept webhook; retry later') from None
        return {'status': 'accepted', 'event_id': event['event_id'],
                'job_id': event['id'], 'delivery_status': event['status']}

    return app


app = create_app()
