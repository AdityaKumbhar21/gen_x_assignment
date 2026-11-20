"""Deterministic rejection receiver for additional tests, not the acceptance mock."""
import asyncio
from collections import Counter

from fastapi import FastAPI, HTTPException

app = FastAPI()
attempts = Counter()
accepted = []


@app.post('/events')
async def receive(payload: dict):
    await asyncio.sleep(0.25)
    event_id = payload['event_id']
    attempts[event_id] += 1
    if attempts[event_id] == 1:
        raise HTTPException(status_code=503, detail='Reject every first attempt')
    accepted.append(payload)
    return {'accepted': True}


@app.get('/stats')
async def stats():
    return {'attempts': dict(attempts), 'accepted': accepted}
