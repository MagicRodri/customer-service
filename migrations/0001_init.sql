CREATE TABLE IF NOT EXISTS customers (
    id                   UUID PRIMARY KEY,
    email                TEXT        NOT NULL UNIQUE,
    name                 TEXT        NOT NULL,
    tier                 TEXT        NOT NULL DEFAULT 'STANDARD',
    status               TEXT        NOT NULL DEFAULT 'ACTIVE',
    lifetime_spend_cents BIGINT      NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL makes Postgres log the pre-image of every row, so the
-- technical CDC stream carries a populated `before` block on UPDATE/DELETE.
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Transactional outbox. Rows are written in the same transaction as the state
-- change they describe, then picked up by the Debezium outbox connector and
-- routed onto business.customer.events.
CREATE TABLE IF NOT EXISTS outbox (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    trace_id       TEXT        NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Consumed event IDs, recorded inside the handler's transaction so that a
-- redelivery after a crash cannot apply the same effect twice.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    source_topic TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Landing table for the technical (raw CDC) stream. Audit only.
CREATE TABLE IF NOT EXISTS technical_audit_log (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source      TEXT        NOT NULL,
    operation   TEXT        NOT NULL,
    row_key     TEXT        NOT NULL,
    before_row  JSONB,
    after_row   JSONB,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON outbox (created_at);
CREATE INDEX IF NOT EXISTS idx_customers_status ON customers (status);
