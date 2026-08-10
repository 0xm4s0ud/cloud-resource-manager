-- Add request body fingerprint for idempotency key reuse detection.

ALTER TABLE capacity_requests
    ADD COLUMN IF NOT EXISTS body_hash TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_capacity_requests_customer_reserved
    ON capacity_requests (customer_id)
    WHERE status = 'RESERVED';
