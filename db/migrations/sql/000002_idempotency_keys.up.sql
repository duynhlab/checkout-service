-- Idempotency keys for POST /confirm (RFC-0015 P2). Canonical schema required
-- by pkg/idempotency (ADR-010) — this service is its second consumer, after
-- payment. The package owns the row lifecycle (Claim → Checkpoint → Finish /
-- Release); this service owns the table. subject_id stores the created order
-- id (numeric) once the confirm checkpoint is reached.

CREATE TABLE IF NOT EXISTS idempotency_keys (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id        VARCHAR(255) NOT NULL,                 -- OIDC token subject (opaque string, ADR-042)
    idem_key       TEXT        NOT NULL,
    request_method TEXT        NOT NULL,
    request_path   TEXT        NOT NULL,
    request_hash   TEXT        NOT NULL,
    locked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    subject_id     BIGINT,
    response_code  INT,
    response_body  JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, idem_key)
);
