DROP INDEX IF EXISTS idx_capacity_requests_customer_reserved;

ALTER TABLE capacity_requests
    DROP COLUMN IF EXISTS body_hash;
