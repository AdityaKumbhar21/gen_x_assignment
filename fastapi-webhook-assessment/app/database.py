"""Short SQLite transactions; application work always runs outside them."""
import asyncio
from contextlib import asynccontextmanager
import sqlite3
import time
import uuid

import aiosqlite


class IdempotencyConflict(Exception):
    pass


class StorageBusy(Exception):
    pass


SCHEMA_VERSION = 1
SCHEMA = """
CREATE TABLE events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 event_id TEXT NOT NULL, tenant_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 request_fingerprint TEXT NOT NULL, raw_content TEXT, cleaned_text TEXT, fingerprint TEXT,
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','delivered','failed')),
 attempt_count INTEGER NOT NULL DEFAULT 0, next_attempt_at REAL NOT NULL DEFAULT 0,
 last_error TEXT, claim_token TEXT, lease_expires_at REAL,
 created_at REAL NOT NULL, updated_at REAL NOT NULL,
 UNIQUE(tenant_id,idempotency_key),
 CHECK((cleaned_text IS NULL) = (fingerprint IS NULL)),
 CHECK((claim_token IS NULL) = (lease_expires_at IS NULL)),
 CHECK(raw_content IS NOT NULL OR cleaned_text IS NOT NULL)
)
"""
REQUIRED_COLUMNS = {
    'id', 'event_id', 'tenant_id', 'idempotency_key', 'request_fingerprint',
    'raw_content', 'cleaned_text', 'fingerprint', 'status', 'attempt_count',
    'next_attempt_at', 'last_error', 'claim_token', 'lease_expires_at',
    'created_at', 'updated_at',
}


def is_busy(error):
    return (getattr(error, 'sqlite_errorcode', 0) & 255) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)


class Database:
    def __init__(self, connection):
        self.connection = connection
        self.lock = asyncio.Lock()

    @classmethod
    async def open(cls, path):
        # Concurrent ASGI startup can briefly contend on the WAL/schema transition.
        # Retry only initialization, closing each failed connection first.
        for attempt in range(10):
            try:
                return await cls._open_once(path)
            except sqlite3.OperationalError as error:
                if not is_busy(error) or attempt == 9:
                    raise
                await asyncio.sleep(0.025)

    @classmethod
    async def _open_once(cls, path):
        connection = await aiosqlite.connect(path, isolation_level=None, timeout=0.1)
        connection.row_factory = aiosqlite.Row
        db = cls(connection)
        try:
            await db._execute('PRAGMA busy_timeout=100')
            # Version, columns and indexes must describe one schema snapshot.
            await db._execute('BEGIN')
            try:
                await db._validate_schema(allow_empty=True)
            finally:
                await connection.rollback()
            if (await db._one('PRAGMA journal_mode=WAL'))[0].lower() != 'wal':
                raise RuntimeError('A file-backed SQLite database with WAL is required')
            await db._execute('PRAGMA synchronous=FULL')
            async with db.transaction():
                exists = await db._one("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
                if not exists:
                    await db._execute(SCHEMA)
                    await db._execute('CREATE INDEX events_due ON events(status,next_attempt_at,id)')
                    await db._execute(f'PRAGMA user_version={SCHEMA_VERSION}')
                await db._validate_schema()
            return db
        except BaseException:
            await connection.close()
            raise

    async def _validate_schema(self, allow_empty=False):
        version = (await self._one('PRAGMA user_version'))[0]
        columns = await self._all('PRAGMA table_info(events)')
        if allow_empty and not columns and version == 0:
            tables = await self._one("SELECT name FROM sqlite_master WHERE type='table' "
                                     "AND name NOT LIKE 'sqlite_%' LIMIT 1")
            if tables is None:
                return
        unique = False
        for index in await self._all('PRAGMA index_list(events)'):
            if index['unique']:
                name = index['name'].replace('"', '""')
                fields = await self._all(f'PRAGMA index_info("{name}")')
                unique |= [field['name'] for field in fields] == ['tenant_id', 'idempotency_key']
        if version != SCHEMA_VERSION or {c['name'] for c in columns} != REQUIRED_COLUMNS or not unique:
            raise RuntimeError('Unsupported webhook database schema. Back up the existing database and select a fresh WEBHOOK_DB_PATH; no legacy rows were changed.')

    async def close(self):
        await self.connection.close()

    async def _execute(self, sql, parameters=()):
        async with self.connection.execute(sql, parameters) as cursor:
            return cursor.rowcount

    async def _one(self, sql, parameters=()):
        async with self.connection.execute(sql, parameters) as cursor:
            return await cursor.fetchone()

    async def _all(self, sql, parameters=()):
        async with self.connection.execute(sql, parameters) as cursor:
            return await cursor.fetchall()

    @asynccontextmanager
    async def transaction(self, *, lock_timeout=None):
        acquired = False
        try:
            async with asyncio.timeout(lock_timeout):
                await self.lock.acquire()
                acquired = True
                await self._execute('BEGIN IMMEDIATE')
            yield
            # Do not cancel a durable COMMIT merely because lock admission had a deadline.
            await self.connection.commit()
        except BaseException:
            if acquired:
                # Queued SQLite operations complete before the rollback executes.
                await self.connection.rollback()
            raise
        finally:
            if acquired:
                self.lock.release()

    async def accept(self, *, event_id, tenant_id, idempotency_key, request_fingerprint, content):
        now = time.time()
        try:
            async with self.transaction(lock_timeout=0.2):
                await self._execute('''INSERT INTO events
                    (event_id,tenant_id,idempotency_key,request_fingerprint,raw_content,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?) ON CONFLICT(tenant_id,idempotency_key) DO NOTHING''',
                    (event_id, tenant_id, idempotency_key, request_fingerprint, content, now, now))
                row = await self._one('''SELECT id,event_id,status,request_fingerprint FROM events
                    WHERE tenant_id=? AND idempotency_key=?''', (tenant_id, idempotency_key))
                if row['request_fingerprint'] != request_fingerprint:
                    raise IdempotencyConflict
                return dict(row)
        except (TimeoutError, sqlite3.OperationalError) as error:
            if isinstance(error, TimeoutError) or is_busy(error):
                raise StorageBusy('Acceptance could not acquire its write lock within budget') from error
            raise

    async def claim(self, lease_seconds):
        token = uuid.uuid4().hex
        async with self.transaction():
            now = time.time()
            row = await self._one('''SELECT * FROM events WHERE status='pending' AND next_attempt_at<=?
                AND (claim_token IS NULL OR lease_expires_at<=?) ORDER BY next_attempt_at,id LIMIT 1''', (now, now))
            if row is None:
                return None
            await self._execute('UPDATE events SET claim_token=?,lease_expires_at=?,updated_at=? WHERE id=?',
                                (token, now + lease_seconds, now, row['id']))
            result = dict(row)
            result.update(claim_token=token, lease_expires_at=now + lease_seconds)
            return result

    async def save_processed(self, job, cleaned_text, fingerprint):
        async with self.transaction():
            now = time.time()
            return await self._execute('''UPDATE events SET cleaned_text=?,fingerprint=?,raw_content=NULL,
                updated_at=? WHERE id=? AND status='pending' AND claim_token=? AND lease_expires_at>?''',
                (cleaned_text, fingerprint, now, job['id'], job['claim_token'], now)) == 1

    async def renew(self, jobs, lease_seconds):
        renewed = set()
        async with self.transaction():
            now = time.time()
            for job in jobs:
                changed = await self._execute('''UPDATE events SET lease_expires_at=?,updated_at=?
                    WHERE id=? AND status='pending' AND claim_token=? AND lease_expires_at>?''',
                    (now + lease_seconds, now, job['id'], job['claim_token'], now))
                if changed:
                    renewed.add(job['claim_token'])
        return renewed

    async def begin_delivery(self, job, lease_seconds):
        async with self.transaction():
            now = time.time()
            return await self._execute('''UPDATE events SET attempt_count=attempt_count+1,
                lease_expires_at=?,updated_at=? WHERE id=? AND status='pending'
                AND claim_token=? AND lease_expires_at>?''',
                (now + lease_seconds, now, job['id'], job['claim_token'], now)) == 1

    async def finish(self, job, *, status, error=None, next_attempt_at=0):
        if status not in ('pending', 'delivered', 'failed'):
            raise ValueError('Unknown job status')
        async with self.transaction():
            now = time.time()
            return await self._execute('''UPDATE events SET status=?,last_error=?,next_attempt_at=?,
                claim_token=NULL,lease_expires_at=NULL,updated_at=?
                WHERE id=? AND status='pending' AND claim_token=? AND lease_expires_at>?''',
                (status, error, next_attempt_at, now, job['id'], job['claim_token'], now)) == 1

    async def rows(self):
        async with self.lock:
            return [dict(row) for row in await self._all('SELECT * FROM events ORDER BY id')]
