-- Global dedup marker: one row per payment ever posted.
CREATE TABLE processed_payments (
    payment_id    UUID PRIMARY KEY,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Double-entry ledger. In production: PARTITION BY RANGE (posted_at) monthly; processed_payments keeps
-- dedup global because partitioned unique constraints must include the partition key.
CREATE TABLE ledger_entries (
    entry_id      BIGSERIAL PRIMARY KEY,
    payment_id    UUID         NOT NULL,
    leg           VARCHAR(6)   NOT NULL CHECK (leg IN ('DEBIT', 'CREDIT')),
    account_id    VARCHAR(48)  NOT NULL,
    amount_minor  BIGINT       NOT NULL CHECK (amount_minor > 0),
    currency      CHAR(3)      NOT NULL,
    posted_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_payment_leg UNIQUE (payment_id, leg)
);
CREATE INDEX ix_ledger_account ON ledger_entries (account_id, posted_at);

CREATE TABLE outbox (
    id              UUID PRIMARY KEY,
    aggregate_type  VARCHAR(64)  NOT NULL,
    aggregate_id    VARCHAR(64)  NOT NULL,
    type            VARCHAR(64)  NOT NULL,
    payload         JSONB        NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ix_outbox_created ON outbox (created_at, id);

-- Invariant check (run in reconciliation): every payment's legs net to zero.
CREATE VIEW ledger_imbalances AS
SELECT payment_id,
       SUM(CASE leg WHEN 'DEBIT' THEN -amount_minor ELSE amount_minor END) AS net
FROM ledger_entries
GROUP BY payment_id
HAVING SUM(CASE leg WHEN 'DEBIT' THEN -amount_minor ELSE amount_minor END) <> 0;
