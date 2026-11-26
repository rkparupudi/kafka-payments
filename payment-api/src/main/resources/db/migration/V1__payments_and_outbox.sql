CREATE TABLE payments (
    payment_id        UUID PRIMARY KEY,
    idempotency_key   VARCHAR(64)  NOT NULL,
    payer_account_id  VARCHAR(32)  NOT NULL,
    merchant_id       VARCHAR(32)  NOT NULL,
    amount_minor      BIGINT       NOT NULL CHECK (amount_minor > 0),
    currency          CHAR(3)      NOT NULL,
    channel           VARCHAR(8)   NOT NULL,
    status            VARCHAR(16)  NOT NULL,   -- PENDING | APPROVED | DECLINED | STEP_UP
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_payer_idem UNIQUE (payer_account_id, idempotency_key)
);

CREATE TABLE outbox (
    id              UUID PRIMARY KEY,
    aggregate_type  VARCHAR(64)  NOT NULL,     -- destination topic
    aggregate_id    VARCHAR(64)  NOT NULL,     -- Kafka message key
    type            VARCHAR(64)  NOT NULL,
    payload         JSONB        NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ix_outbox_created ON outbox (created_at, id);
