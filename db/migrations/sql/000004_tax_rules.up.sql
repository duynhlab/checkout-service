-- Flat-rate tax rule table (RFC-0015 P3). Rates are basis points applied to
-- (subtotal + shipping_fee) at PUT …/shipping and at confirm-time requotes.
-- Region matches the checkout address country; the DEFAULT row is the
-- fallback for regions without an explicit rule. Deliberately naive — the
-- point is totals composition in minor units, not a tax engine.

CREATE TABLE IF NOT EXISTS tax_rules (
    region   TEXT PRIMARY KEY,
    rate_bps INT  NOT NULL CHECK (rate_bps >= 0 AND rate_bps <= 10000)
);

INSERT INTO tax_rules (region, rate_bps) VALUES
    ('VN', 800),
    ('US', 725),
    ('DEFAULT', 1000)
ON CONFLICT (region) DO NOTHING;
