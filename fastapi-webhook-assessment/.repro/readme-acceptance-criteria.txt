1. **Event-Loop Responsiveness:** Every `GET /health` probe sent while burst traffic is in flight must respond in **< 50ms**.
2. **Fast Ingestion Acknowledgment:** `POST /webhooks/ingest` must acknowledge valid requests (`200` or `202`) with **p95 latency < 250ms**.
3. **Strict Idempotency:** Concurrent duplicate deliveries sharing the same `X-Idempotency-Key` must never produce duplicate database rows or duplicate downstream deliveries.
4. **Resilient Downstream Delivery:** Transient `503` errors from the downstream service must be retried safely so all unique events reach `"delivered"` status.

