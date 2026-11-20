"""Fixed worker loops, durable retry timestamps, and live-claim renewal."""
import asyncio
import logging
import random
import sqlite3
import time

from app.database import is_busy
from app.downstream import DeliveryError, forward_event
from app.processor import scrub_and_fingerprint

logger = logging.getLogger(__name__)


def retry_delay(attempt):
    ceiling = min(1.0, 0.1 * 2 ** min(max(attempt - 1, 0), 4))
    return random.uniform(ceiling / 2, ceiling)


async def wait_or_stop(event, seconds):
    try:
        await asyncio.wait_for(event.wait(), timeout=seconds)
    except TimeoutError:
        pass


async def handle_job(state, job):
    if job['cleaned_text'] is None:
        future = asyncio.get_running_loop().run_in_executor(
            state.pool, scrub_and_fingerprint, job['raw_content'])
        state.cpu_futures.add(future)

        def completed(done):
            state.cpu_futures.discard(done)
            if not done.cancelled():
                done.exception()  # Also observe exceptions after coordinator cancellation.

        future.add_done_callback(completed)
        cleaned, fingerprint = await asyncio.shield(future)
        if not await state.db.save_processed(job, cleaned, fingerprint):
            return
        job.update(cleaned_text=cleaned, fingerprint=fingerprint, raw_content=None)
    if state.stop.is_set():
        await state.db.finish(job, status='pending')
        return
    if not await state.db.begin_delivery(job, state.lease_seconds):
        return
    job['attempt_count'] += 1
    job['sending'] = True
    try:
        await forward_event(state.client, state.downstream_url, job)
    except DeliveryError as error:
        job['outcome'] = {
            'status': 'pending' if error.retryable else 'failed',
            'error': str(error),
            'next_attempt_at': time.time() + retry_delay(job['attempt_count']) if error.retryable else 0,
        }
    else:
        job['outcome'] = {'status': 'delivered'}
    await state.db.finish(job, **job['outcome'])


async def worker_loop(state):
    while not state.stop.is_set():
        try:
            job = await state.db.claim(state.lease_seconds)
        except sqlite3.OperationalError as error:
            if not is_busy(error):
                raise
            await wait_or_stop(state.stop, state.poll_interval)
            continue
        if job is None:
            await wait_or_stop(state.stop, state.poll_interval)
            continue
        token = job['claim_token']
        state.active[token] = job
        retain_claim = False
        try:
            # No new CPU submission if shutdown began during the claim transaction.
            if state.stop.is_set():
                await state.db.finish(job, status='pending')
                continue
            await handle_job(state, job)
        except asyncio.CancelledError:
            if job.get('sending'):
                outcome = job.get('outcome') or {
                    'status': 'failed',
                    'error': 'Delivery outcome unknown during shutdown; reconciliation required',
                }
                await asyncio.shield(state.db.finish(job, **outcome))
            else:
                # A shielded CPU future may still run. Lifespan joins it before release.
                retain_claim = True
            raise
        finally:
            if not retain_claim:
                state.active.pop(token, None)


async def renew_loop(state):
    while not state.renew_stop.is_set():
        await wait_or_stop(state.renew_stop, state.renew_interval)
        if state.renew_stop.is_set():
            break
        jobs = list(state.active.values())
        if not jobs:
            continue
        try:
            renewed = await state.db.renew(jobs, state.lease_seconds)
        except sqlite3.OperationalError as error:
            if not is_busy(error):
                raise
            logger.warning('Claim renewal encountered SQLite contention')
            continue
        for job in jobs:
            token = job['claim_token']
            if token in state.active and token not in renewed:
                logger.warning('Ownership expired or completed for event row %s', job['id'])
