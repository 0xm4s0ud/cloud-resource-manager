-- Draft schema for golang-migrate (000001_init.up.sql).

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE resource_pools (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind                 TEXT NOT NULL UNIQUE,
    unit                 TEXT NOT NULL,
    total                BIGINT NOT NULL CHECK (total >= 0),
    oversubscribe_factor DOUBLE PRECISION NOT NULL DEFAULT 1.0 CHECK (oversubscribe_factor > 0),
    dedicated_amount     BIGINT NOT NULL DEFAULT 0 CHECK (dedicated_amount >= 0),
    reserved_amount      BIGINT NOT NULL DEFAULT 0 CHECK (reserved_amount >= 0),
    policy               JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code              TEXT NOT NULL UNIQUE,
    name              TEXT NOT NULL,
    resource_template JSONB NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE customer_quotas (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id   TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    "limit"       BIGINT NOT NULL CHECK ("limit" >= 0),
    used_amount   BIGINT NOT NULL DEFAULT 0 CHECK (used_amount >= 0),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id, resource_kind),
    CHECK (used_amount <= "limit")
);

CREATE TABLE capacity_requests (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id      TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    product_id       UUID REFERENCES products (id),
    quantity         INT NOT NULL DEFAULT 1 CHECK (quantity > 0),
    status           TEXT NOT NULL CHECK (status IN ('RESERVED', 'REJECTED', 'COMPLETED', 'CANCELLED', 'EXPIRED')),
    reject_reason    TEXT,
    expires_at       TIMESTAMPTZ,
    extension_count  INT NOT NULL DEFAULT 0 CHECK (extension_count >= 0),
    correlation_id   TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id, idempotency_key)
);

CREATE INDEX idx_capacity_requests_customer ON capacity_requests (customer_id);
CREATE INDEX idx_capacity_requests_status_expires ON capacity_requests (status, expires_at)
    WHERE status = 'RESERVED';

CREATE TABLE request_lines (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id UUID NOT NULL REFERENCES capacity_requests (id) ON DELETE CASCADE,
    pool_id    UUID NOT NULL REFERENCES resource_pools (id),
    kind       TEXT NOT NULL,
    amount     BIGINT NOT NULL CHECK (amount > 0)
);

CREATE INDEX idx_request_lines_request ON request_lines (request_id);

CREATE TABLE holds (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id UUID NOT NULL REFERENCES capacity_requests (id) ON DELETE CASCADE,
    pool_id    UUID NOT NULL REFERENCES resource_pools (id),
    amount     BIGINT NOT NULL CHECK (amount > 0),
    status     TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'DEDICATED', 'EXPIRED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_holds_request ON holds (request_id);
CREATE INDEX idx_holds_active ON holds (status) WHERE status = 'ACTIVE';

CREATE TABLE dedications (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id UUID NOT NULL REFERENCES capacity_requests (id) ON DELETE CASCADE,
    pool_id    UUID NOT NULL REFERENCES resource_pools (id),
    hold_id    UUID NOT NULL REFERENCES holds (id),
    amount     BIGINT NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dedications_request ON dedications (request_id);

CREATE TABLE request_events (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id UUID NOT NULL REFERENCES capacity_requests (id) ON DELETE CASCADE,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'created', 'reserved', 'rejected', 'confirmed', 'cancelled', 'expired', 'extended'
    )),
    reason     TEXT NOT NULL DEFAULT '',
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_request_events_request ON request_events (request_id, at);
