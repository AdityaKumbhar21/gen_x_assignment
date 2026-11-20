import asyncio
import sqlite3
import time

import pytest

from app.database import Database, IdempotencyConflict, StorageBusy


async def accept(db, *, tenant="t1", key="key", fingerprint="request-a", content="raw"):
    return await db.accept(event_id="event", tenant_id=tenant, idempotency_key=key,
                           request_fingerprint=fingerprint, content=content)


@pytest.fixture
async def databases(tmp_path):
    path = str(tmp_path / "jobs.db")
    first = await Database.open(path)
    second = await Database.open(path)
    try:
        yield first, second, path
    finally:
        await first.close()
        await second.close()


async def test_concurrent_acceptance_tenant_scope_and_conflicts(databases):
    first, second, _ = databases
    results = await asyncio.gather(*(accept(db) for db in (first, second) for _ in range(15)))
    assert len({row["id"] for row in results}) == 1
    assert len(await first.rows()) == 1
    with pytest.raises(IdempotencyConflict):
        await accept(second, fingerprint="different", content="changed")
    await accept(second, tenant="t2")
    rows = await first.rows()
    assert len(rows) == 2
    assert rows[0]["raw_content"] == "raw"
    assert rows[0]["request_fingerprint"] == "request-a"


async def test_conflicting_race_accepts_exactly_one_payload(databases):
    first, second, _ = databases
    results = await asyncio.gather(accept(first), accept(second, fingerprint="request-b"),
                                   return_exceptions=True)
    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, IdempotencyConflict) for result in results) == 1
    assert len(await first.rows()) == 1


async def test_claim_recovery_renewal_and_stale_owner(databases):
    first, second, _ = databases
    await accept(first)
    claimed = await asyncio.gather(first.claim(0.08), second.claim(0.08))
    assert sum(job is not None for job in claimed) == 1
    original = next(job for job in claimed if job)
    assert await first.save_processed(original, "cleaned", "processed-fingerprint")
    await asyncio.sleep(0.1)
    recovered = await second.claim(1)
    assert recovered["id"] == original["id"]
    assert recovered["claim_token"] != original["claim_token"]
    assert recovered["cleaned_text"] == "cleaned"
    assert recovered["raw_content"] is None
    assert not await first.save_processed(original, "bad", "bad")
    assert not await first.begin_delivery(original, 1)
    assert not await first.finish(original, status="delivered")
    assert await first.renew([original], 1) == set()
    assert await second.renew([recovered], 1) == {recovered["claim_token"]}
    assert await second.begin_delivery(recovered, 1)
    assert await second.finish(recovered, status="pending", error="503",
                               next_attempt_at=time.time() + 0.08)
    assert await first.claim(1) is None
    await asyncio.sleep(0.1)
    retry = await first.claim(1)
    assert retry["attempt_count"] == 1
    assert retry["fingerprint"] == "processed-fingerprint"
    assert await first.finish(retry, status="delivered")
    assert await second.claim(1) is None
    assert (await first.rows())[0]["status"] == "delivered"


async def test_wal_timeout_and_bounded_lock_wait(databases):
    first, _, path = databases
    assert (await first._one("PRAGMA journal_mode"))[0] == "wal"
    assert (await first._one("PRAGMA busy_timeout"))[0] == 100
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        start = time.perf_counter()
        with pytest.raises(StorageBusy):
            await accept(first)
        assert time.perf_counter() - start < 0.25
    finally:
        blocker.rollback()
        blocker.close()
    assert not first.connection.in_transaction
    assert len(await first.rows()) == 0
    await accept(first)


@pytest.mark.parametrize('table', ['events', 'unrelated'])
async def test_reject_legacy_database_without_changing_data(tmp_path, table):
    path = str(tmp_path / "old.db")
    with sqlite3.connect(path) as db:
        db.execute(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY, content TEXT)")
        db.execute(f"INSERT INTO {table} VALUES(1,'preserve')")
    with pytest.raises(RuntimeError, match="fresh WEBHOOK_DB_PATH"):
        await Database.open(path)
    with sqlite3.connect(path) as db:
        assert db.execute(f"SELECT * FROM {table}").fetchall() == [(1, "preserve")]
        assert db.execute("PRAGMA user_version").fetchone()[0] == 0


@pytest.mark.parametrize('operation', ['renew', 'begin_delivery', 'save_processed', 'finish'])
async def test_lease_expiring_while_waiting_for_writer_cannot_be_used(databases, operation):
    first, second, _ = databases
    await accept(first)
    job = await first.claim(0.04)
    async with first.transaction():
        if operation == 'renew':
            call = second.renew([job], 1)
        elif operation == 'begin_delivery':
            call = second.begin_delivery(job, 1)
        elif operation == 'save_processed':
            call = second.save_processed(job, 'clean', 'fp')
        else:
            call = second.finish(job, status='delivered')
        task = asyncio.create_task(call)
        await asyncio.sleep(0.07)
    assert not await task
    assert (await first.rows())[0]['status'] == 'pending'


async def test_concurrent_fresh_initialization(tmp_path):
    path = str(tmp_path / 'fresh.db')
    opened = []
    try:
        results = await asyncio.gather(*(Database.open(path) for _ in range(4)), return_exceptions=True)
        opened = [result for result in results if isinstance(result, Database)]
        assert len(opened) == 4, results
        await accept(opened[0])
        assert len(await opened[3].rows()) == 1
    finally:
        await asyncio.gather(*(db.close() for db in opened))


async def test_cancelled_transaction_rolls_back_before_reusing_connection(databases):
    first, _, _ = databases
    entered = asyncio.Event()

    async def cancelled_write():
        async with first.transaction():
            await first._execute("UPDATE events SET status='failed'")
            entered.set()
            await asyncio.Event().wait()

    await accept(first)
    task = asyncio.create_task(cancelled_write())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not first.connection.in_transaction
    assert (await first.rows())[0]['status'] == 'pending'
    await accept(first, key='second')


async def test_commit_is_allowed_to_finish_after_lock_wait_budget(databases, monkeypatch):
    first, _, _ = databases
    commit = first.connection.commit

    async def slow_commit():
        await asyncio.sleep(0.22)
        await commit()

    monkeypatch.setattr(first.connection, 'commit', slow_commit)
    accepted = await accept(first)
    assert accepted['status'] == 'pending'
    assert len(await first.rows()) == 1
    assert not first.connection.in_transaction


async def test_schema_validation_uses_one_snapshot_during_other_startup(tmp_path, monkeypatch):
    path = str(tmp_path / 'startup.db')
    # WAL permits the other initializer to commit while this initializer reads.
    with sqlite3.connect(path) as connection:
        connection.execute('PRAGMA journal_mode=WAL')
    read_one = Database._one
    interleaved = False
    opened = []

    async def interleave(self, sql, parameters=()):
        nonlocal interleaved
        row = await read_one(self, sql, parameters)
        if sql == 'PRAGMA user_version' and not interleaved:
            interleaved = True
            opened.append(await Database.open(path))
        return row

    monkeypatch.setattr(Database, '_one', interleave)
    try:
        opened.append(await Database.open(path))
        await accept(opened[-1])
        assert len(await opened[0].rows()) == 1
    finally:
        await asyncio.gather(*(db.close() for db in opened))
