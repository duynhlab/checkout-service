-- Checkout sessions (RFC-0015): ephemeral, short-TTL quote driven by an
-- explicit FSM. One active (non-terminal) session per user, enforced by a
-- partial unique index. All money columns are int64 minor units.

CREATE TABLE IF NOT EXISTS checkout_sessions (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id              TEXT        NOT NULL,
    status               TEXT        NOT NULL DEFAULT 'open'
        CHECK (status IN ('open','address_set','shipping_set','ready','confirming','completed','cancelled','expired')),
    address              JSONB,
    shipping_method      TEXT,
    shipping_fee_minor   BIGINT      NOT NULL DEFAULT 0,
    tax_minor            BIGINT      NOT NULL DEFAULT 0,
    promo_code           TEXT,
    discount_minor       BIGINT      NOT NULL DEFAULT 0,
    subtotal_minor       BIGINT      NOT NULL DEFAULT 0,
    total_minor          BIGINT      NOT NULL DEFAULT 0,
    currency             TEXT        NOT NULL DEFAULT 'USD',
    payment_method_token TEXT,
    order_id             TEXT,
    expires_at           TIMESTAMPTZ NOT NULL,
    expired_reason       TEXT
        CHECK (expired_reason IS NULL OR expired_reason IN ('timer','lazy')),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One active session per user: POST /sessions is idempotent against this.
CREATE UNIQUE INDEX IF NOT EXISTS uq_checkout_sessions_active_user
    ON checkout_sessions (user_id)
    WHERE status IN ('open','address_set','shipping_set','ready','confirming');

CREATE TABLE IF NOT EXISTS checkout_session_items (
    id               BIGSERIAL PRIMARY KEY,
    session_id       UUID   NOT NULL REFERENCES checkout_sessions(id) ON DELETE CASCADE,
    product_id       TEXT   NOT NULL,
    product_name     TEXT   NOT NULL,
    quantity         INT    NOT NULL CHECK (quantity > 0),
    -- unit_price_minor: product-authoritative price at snapshot time;
    -- cart_price_minor: cart's denormalized price, kept for the diff.
    unit_price_minor BIGINT NOT NULL,
    cart_price_minor BIGINT NOT NULL,
    price_changed    BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_checkout_session_items_session
    ON checkout_session_items (session_id);
