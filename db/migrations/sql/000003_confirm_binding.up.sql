-- Session↔idempotency-key binding for the confirm flow (RFC-0015 P2).
-- confirm_key_id records which idempotency claim (idempotency_keys.id) drove
-- the session into `confirming`. It is the session-level mutual exclusion:
-- while set, only the same claim may re-enter (crash recovery) — a different
-- Idempotency-Key can never act on a confirming session, which is what makes
-- "one order per session funnel" enforceable rather than aspirational.
-- Kept on completed rows as recovery provenance; cleared on requote.

ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS confirm_key_id BIGINT;
