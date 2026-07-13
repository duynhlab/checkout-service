-- Promo codes + redemption ledger (RFC-0015 P4, ADR-022). Applying a code to
-- a session never counts a use; redemption is counted at confirm, atomically,
-- serialized per code by a FOR UPDATE row lock. UNIQUE(code, session_id) is
-- the idempotency anchor: a crash-re-driven confirm can never double-count.

CREATE TABLE IF NOT EXISTS promo_codes (
    code            TEXT PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('percent','fixed')),
    -- percent: whole percentage points (10 = 10%), 1..100;
    -- fixed: int64 minor units, > 0.
    value           BIGINT NOT NULL CHECK (value > 0),
    expires_at      TIMESTAMPTZ,          -- NULL = never expires
    max_redemptions INT,                  -- NULL = unlimited
    redeemed_count  INT NOT NULL DEFAULT 0 CHECK (redeemed_count >= 0),
    per_user_limit  INT,                  -- NULL = unlimited
    CHECK (kind <> 'percent' OR value <= 100)
);

CREATE TABLE IF NOT EXISTS promo_redemptions (
    id          BIGSERIAL PRIMARY KEY,
    code        TEXT NOT NULL REFERENCES promo_codes(code),
    user_id     TEXT NOT NULL,
    session_id  UUID NOT NULL,
    order_id    TEXT,
    redeemed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (code, session_id)
);

CREATE INDEX IF NOT EXISTS idx_promo_redemptions_code_user
    ON promo_redemptions (code, user_id);

-- Dev/demo seeds (idempotent).
INSERT INTO promo_codes (code, kind, value, expires_at, max_redemptions, per_user_limit) VALUES
    ('WELCOME10', 'percent', 10,  NULL,                          NULL, NULL),
    ('SAVE5',     'fixed',   500, NULL,                          NULL, NULL),
    ('EXPIRED1',  'percent', 15,  now() - interval '1 day',      NULL, NULL),
    ('ONETIME',   'fixed',   300, NULL,                          NULL, 1),
    ('SCARCE',    'fixed',   200, NULL,                          5,    NULL)
ON CONFLICT (code) DO NOTHING;
