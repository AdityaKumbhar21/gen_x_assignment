"""One delivery attempt using the lifespan-owned HTTP client."""
import hashlib
import json

import httpx


class DeliveryError(Exception):
    def __init__(self, message, *, retryable=False):
        super().__init__(message)
        self.retryable = retryable


async def forward_event(client: httpx.AsyncClient, url: str, job: dict):
    scoped_key = json.dumps([job['tenant_id'], job['idempotency_key']],
                            ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    token = hashlib.sha256(scoped_key).hexdigest()
    try:
        response = await client.post(
            url, headers={'X-Idempotency-Key': token},
            json={'event_id': job['event_id'], 'text': job['cleaned_text'],
                  'fingerprint': job['fingerprint']},
        )
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as error:
        raise DeliveryError(f'Pre-send {type(error).__name__}', retryable=True) from error
    except httpx.HTTPError as error:
        raise DeliveryError(f'Delivery outcome unknown: {type(error).__name__}; reconciliation required') from error
    if response.status_code == 503:
        # The assessment receiver explicitly rejects before accepting on 503.
        raise DeliveryError('Downstream rejected event with HTTP 503', retryable=True)
    if not 200 <= response.status_code < 300:
        raise DeliveryError(f'Downstream HTTP {response.status_code}; not automatically retried')
