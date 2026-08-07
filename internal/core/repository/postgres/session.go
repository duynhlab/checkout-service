// Package postgres implements the SessionRepository port against PostgreSQL.
// Raw SQL over pgxpool (simple protocol — pooler-safe), per-query timeouts,
// pgx sentinel errors translated to domain errors at this boundary.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// queryTimeout bounds every repository query.
const queryTimeout = 3 * time.Second

// uniqueViolation is the Postgres error code for a unique-index conflict.
const uniqueViolation = "23505"

// invalidTextRepresentation is the Postgres error code raised when a value
// cannot be cast to the column type — here, a non-UUID session id. Treated as
// "no such session" so garbage ids answer 404, not 500.
const invalidTextRepresentation = "22P02"

// SessionRepository persists checkout sessions.
type SessionRepository struct {
	db *pgxpool.Pool
}

// Ensure interface compliance.
var _ domain.SessionRepository = (*SessionRepository)(nil)

// NewSessionRepository wires the repository.
func NewSessionRepository(db *pgxpool.Pool) *SessionRepository {
	return &SessionRepository{db: db}
}

// Create inserts the session and its item snapshot in one transaction. A
// second active session for the user maps to domain.ErrActiveSessionExists
// (partial unique index).
func (r *SessionRepository) Create(ctx context.Context, s *domain.Session) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	addrJSON, err := marshalAddress(s.Address)
	if err != nil {
		return err
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO checkout_sessions
			(user_id, status, address, subtotal_minor, total_minor, currency, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at, updated_at`,
		s.UserID, s.Status, addrJSON, s.SubtotalMinor, s.TotalMinor, s.Currency, s.ExpiresAt,
	).Scan(&s.ID, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return domain.ErrActiveSessionExists
		}
		return fmt.Errorf("insert session: %w", err)
	}

	for i := range s.Items {
		it := &s.Items[i]
		if _, err := tx.Exec(ctx, `
			INSERT INTO checkout_session_items
				(session_id, product_id, product_name, quantity, unit_price_minor, cart_price_minor, price_changed)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			s.ID, it.ProductID, it.ProductName, it.Quantity, it.UnitPriceMinor, it.CartPriceMinor, it.PriceChanged,
		); err != nil {
			return fmt.Errorf("insert item %s: %w", it.ProductID, err)
		}
	}

	return tx.Commit(ctx)
}

// FindByID loads a session with its items. Ownership is NOT checked here —
// the logic layer owns that rule.
func (r *SessionRepository) FindByID(ctx context.Context, id string) (_ *domain.Session, err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	return r.scanSession(ctx, `
		SELECT id, user_id, status, address, shipping_method, shipping_fee_minor,
		       tax_minor, promo_code, discount_minor, subtotal_minor, total_minor,
		       currency, payment_method_token, order_id, confirm_key_id, expires_at,
		       expired_reason, created_at, updated_at
		FROM checkout_sessions WHERE id = $1`, id)
}

// FindActiveByUserID loads the user's single active session, if any.
func (r *SessionRepository) FindActiveByUserID(ctx context.Context, userID string) (_ *domain.Session, err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	return r.scanSession(ctx, `
		SELECT id, user_id, status, address, shipping_method, shipping_fee_minor,
		       tax_minor, promo_code, discount_minor, subtotal_minor, total_minor,
		       currency, payment_method_token, order_id, confirm_key_id, expires_at,
		       expired_reason, created_at, updated_at
		FROM checkout_sessions
		WHERE user_id = $1
		  AND status IN ('open','address_set','shipping_set','ready','confirming')`, userID)
}

// UpdateStatus conditionally moves status (optimistic concurrency on `from`).
func (r *SessionRepository) UpdateStatus(ctx context.Context, id string, from, to domain.SessionStatus) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions SET status = $3, updated_at = now()
		WHERE id = $1 AND status = $2`, id, from, to)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	return nil
}

// SetAddress persists the address and address_set in one conditional write.
// An address change INVALIDATES the shipping quote (RFC-0015: rates and tax
// are destination-dependent): method, fee, and tax reset and the total drops
// back to subtotal − discount, forcing the funnel through PUT shipping again.
func (r *SessionRepository) SetAddress(ctx context.Context, id string, from domain.SessionStatus, addr *domain.Address, discountMinor int64) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	addrJSON, err := marshalAddress(addr)
	if err != nil {
		return err
	}
	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions
		SET status = 'address_set', address = $3,
		    shipping_method = NULL, shipping_fee_minor = 0, tax_minor = 0,
		    discount_minor = $4,
		    total_minor = subtotal_minor - $4, updated_at = now()
		WHERE id = $1 AND status = $2`, id, from, addrJSON, discountMinor)
	if err != nil {
		return fmt.Errorf("set address: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	return nil
}

// BeginConfirm moves ready → confirming and binds the session to the
// idempotency claim in one CAS — the session-level mutual exclusion for the
// confirm flow (RFC-0015 P2). Idempotent for the same claim; a different
// claim (or any other status) is ErrStaleTransition.
func (r *SessionRepository) BeginConfirm(ctx context.Context, id string, keyID int64) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions
		SET status = 'confirming', confirm_key_id = $2, updated_at = now()
		WHERE id = $1 AND status = 'ready'
		  AND (confirm_key_id IS NULL OR confirm_key_id = $2)`, id, keyID)
	if err != nil {
		return fmt.Errorf("begin confirm: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	return nil
}

// RequoteItems drops a confirming session back to shipping_set with fresh
// product-authoritative prices, releasing the claim binding — the
// PRICE_CHANGED / STOCK_UNAVAILABLE path. Every write is conditional on the
// session still being confirming under THIS claim, so it can never race a
// concurrent completion.
func (r *SessionRepository) RequoteItems(ctx context.Context, id string, keyID int64, items []domain.SessionItem, subtotalMinor, taxMinor, discountMinor int64) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("requote begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		UPDATE checkout_sessions
		SET status = 'shipping_set', confirm_key_id = NULL,
		    subtotal_minor = $3, tax_minor = $4, discount_minor = $5,
		    total_minor = $3 + shipping_fee_minor + $4 - $5,
		    updated_at = now()
		WHERE id = $1 AND status = 'confirming' AND confirm_key_id = $2`,
		id, keyID, subtotalMinor, taxMinor, discountMinor)
	if err != nil {
		return fmt.Errorf("requote session: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	for _, it := range items {
		if _, err := tx.Exec(ctx, `
			UPDATE checkout_session_items
			SET unit_price_minor = $3, price_changed = $4
			WHERE session_id = $1 AND product_id = $2`,
			id, it.ProductID, it.UnitPriceMinor, it.PriceChanged); err != nil {
			return fmt.Errorf("requote item %s: %w", it.ProductID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("requote commit: %w", err)
	}
	return nil
}

// CompleteSession moves confirming → completed and records the order id, CAS
// on the claim binding. The binding is deliberately KEPT on the completed row:
// it is the recovery proof that lets the same claim rebuild and cache the
// response after a crash between completion and Finish.
func (r *SessionRepository) CompleteSession(ctx context.Context, id string, keyID int64, orderID string) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions
		SET status = 'completed', order_id = $3, updated_at = now()
		WHERE id = $1 AND status = 'confirming' AND confirm_key_id = $2`, id, keyID, orderID)
	if err != nil {
		return fmt.Errorf("complete session: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	return nil
}

// SetShipping persists the shipping choice and shipping_set in one
// conditional write. total_minor is recomputed in SQL from the persisted
// components so the stored total can never drift from its parts.
func (r *SessionRepository) SetShipping(ctx context.Context, id string, from domain.SessionStatus, asOf time.Time, method string, feeMinor, taxMinor, discountMinor int64) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// asOf fences the quote to the exact row revision it was priced against:
	// a concurrent PUT address (same-status re-entry) bumps updated_at, so a
	// fee quoted for the OLD destination can never persist next to the new
	// address (review finding — the status CAS alone cannot see that race).
	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions
		SET status = 'shipping_set', shipping_method = $3, shipping_fee_minor = $4,
		    tax_minor = $5, discount_minor = $6,
		    total_minor = subtotal_minor + $4 + $5 - $6,
		    updated_at = now()
		WHERE id = $1 AND status = $2 AND updated_at = $7`,
		id, from, method, feeMinor, taxMinor, discountMinor, asOf)
	if err != nil {
		return fmt.Errorf("set shipping: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	return nil
}

// SetPaymentToken persists the tok_ reference and ready in one conditional
// write. The logic layer has already validated the token shape — PAN-shaped
// input never reaches this statement.
func (r *SessionRepository) SetPaymentToken(ctx context.Context, id string, from domain.SessionStatus, token string) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions
		SET status = 'ready', payment_method_token = $3, updated_at = now()
		WHERE id = $1 AND status = $2`, id, from, token)
	if err != nil {
		return fmt.Errorf("set payment token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	return nil
}

// ExpireDue is the abandonment timer's write (RFC-0015 P2, ADR-019): expire
// the session ONLY if its DB deadline has actually elapsed — the timer firing
// is a wake-up, never a verdict. Returns the outcome plus, for OutcomeNotDue,
// how long until the real deadline so the workflow can re-arm.
func (r *SessionRepository) ExpireDue(ctx context.Context, id string, lockTakeover time.Duration) (_ domain.ExpireOutcome, _ time.Duration, err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions
		SET status = 'expired', expired_reason = 'timer', updated_at = now()
		WHERE id = $1
		  AND status NOT IN ('completed','cancelled','expired','confirming')
		  AND expires_at <= now()`, id)
	if err != nil {
		return "", 0, fmt.Errorf("expire due: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return domain.OutcomeExpired, 0, nil
	}

	// Parked-confirming recovery (review finding — the lost-key lockout): a
	// confirming session may be expired ONLY when its bound claim proves the
	// confirm is dead AND never authorized an order — the claim is unfinished,
	// has no attempt marker (subject_id IS NULL, set BEFORE any CreateOrder),
	// and its lock is stale past the takeover window (deadline fencing ⇒ the
	// owner cannot still write). A claim with a marker stays parked for the
	// operator runbook: an order may exist and auto-expiry could invite an
	// unnoticed second purchase.
	tag, err = r.db.Exec(ctx, `
		UPDATE checkout_sessions cs
		SET status = 'expired', expired_reason = 'timer', confirm_key_id = NULL, updated_at = now()
		FROM idempotency_keys ik
		WHERE cs.id = $1 AND cs.status = 'confirming' AND cs.expires_at <= now()
		  AND ik.id = cs.confirm_key_id
		  AND ik.subject_id IS NULL
		  AND ik.response_code IS NULL
		  AND ik.locked_at < now() - $2::interval`, id, lockTakeover.String())
	if err != nil {
		return "", 0, fmt.Errorf("expire parked confirm: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return domain.OutcomeExpired, 0, nil
	}

	// Neither write hit: find out why — deadline moved (re-arm), a parked
	// confirm not yet provably dead (re-check after the takeover window), or
	// out of jurisdiction (terminal / order-attempted confirming → exit).
	var status string
	var expiresAt time.Time
	err = r.db.QueryRow(ctx, `
		SELECT status, expires_at FROM checkout_sessions WHERE id = $1`, id).
		Scan(&status, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.OutcomeGone, 0, nil
	}
	if err != nil {
		return "", 0, fmt.Errorf("expire due read-back: %w", err)
	}
	switch domain.SessionStatus(status) { //nolint:exhaustive // non-terminal statuses fall through to re-arm
	case domain.StatusCompleted, domain.StatusCancelled, domain.StatusExpired:
		return domain.OutcomeGone, 0, nil
	case domain.StatusConfirming:
		var attempted bool
		if err := r.db.QueryRow(ctx, `
			SELECT ik.subject_id IS NOT NULL OR ik.response_code IS NOT NULL
			FROM checkout_sessions cs JOIN idempotency_keys ik ON ik.id = cs.confirm_key_id
			WHERE cs.id = $1`, id).Scan(&attempted); err != nil || attempted {
			// Order attempted (or binding unreadable): ops-only recovery.
			return domain.OutcomeGone, 0, nil
		}
		// Provably-dead check not passed yet: look again after the window.
		return domain.OutcomeNotDue, lockTakeover, nil
	}
	remaining := time.Until(expiresAt)
	if remaining <= 0 {
		// Raced the deadline between UPDATE and SELECT: ask for a quick retry.
		remaining = time.Second
	}
	return domain.OutcomeNotDue, remaining, nil
}

// GetTaxRateBps looks up the flat tax rate (basis points) for a region,
// falling back to the seeded DEFAULT row (RFC-0015 P3).
func (r *SessionRepository) GetTaxRateBps(ctx context.Context, region string) (_ int32, err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	var bps int32
	// ORDER BY makes the exact-region row win deterministically — UNION ALL
	// + LIMIT without it rides on unguaranteed planner row order (review).
	err = r.db.QueryRow(ctx, `
		SELECT rate_bps FROM tax_rules
		WHERE region IN ($1, 'DEFAULT')
		ORDER BY (region = 'DEFAULT')
		LIMIT 1`, strings.ToUpper(region)).Scan(&bps)
	if err != nil {
		return 0, fmt.Errorf("tax rate lookup: %w", err)
	}
	return bps, nil
}

// ReapFinishedIdempotencyKeys deletes FINISHED idempotency rows older than
// ttl (the Stripe-style replay window; security review: response_body caches
// address-bearing session JSON and must not be retained forever). Unfinished
// rows are never touched — a parked confirming session's claim binding must
// not rot (doubt-cycle b/c). A finished row reaped after ttl only downgrades
// a very late same-key replay from a cached 201 to a 409; the session row
// itself still shows the order.
func (r *SessionRepository) ReapFinishedIdempotencyKeys(ctx context.Context, ttl time.Duration) (_ int64, err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tag, err := r.db.Exec(ctx, `
		DELETE FROM idempotency_keys
		WHERE response_code IS NOT NULL AND created_at < now() - $1::interval`,
		ttl.String())
	if err != nil {
		return 0, fmt.Errorf("reap idempotency keys: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Touch bumps expires_at — the reset-on-activity half of the expiry contract
// (RFC-0015 P2): every successful mutation both signals the abandonment
// workflow (timer reset) and bumps the DB expiry here, so the lazy backstop
// never expires a session the timer considers alive. Terminal sessions are
// left untouched — a late Touch is a harmless no-op, like a late timer.
func (r *SessionRepository) Touch(ctx context.Context, id string, expiresAt time.Time) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	_, err = r.db.Exec(ctx, `
		UPDATE checkout_sessions SET expires_at = $2, updated_at = now()
		WHERE id = $1 AND status NOT IN ('completed','cancelled','expired')`, id, expiresAt)
	if err != nil {
		return fmt.Errorf("touch session: %w", err)
	}
	return nil
}

// MarkExpired conditionally expires a non-terminal session. A late call
// against a terminal session is a no-op — never an error (RFC-0015: a
// late-firing timer must be harmless).
func (r *SessionRepository) MarkExpired(ctx context.Context, id string, reason domain.ExpiredReason) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// `confirming` is deliberately NOT expirable: a confirm may have an order
	// handoff in flight (P2) and must finish or drop back to shipping_set —
	// never be yanked to expired mid-flight. Mirrors the FSM table, which has
	// no confirming→expired edge.
	_, err = r.db.Exec(ctx, `
		UPDATE checkout_sessions
		SET status = 'expired', expired_reason = $2, updated_at = now()
		WHERE id = $1 AND status NOT IN ('completed','cancelled','expired','confirming')`, id, reason)
	if err != nil {
		return fmt.Errorf("mark expired: %w", err)
	}
	return nil
}

// scanSession loads one session row plus its items.
func (r *SessionRepository) scanSession(ctx context.Context, query string, arg any) (*domain.Session, error) {
	var (
		s          domain.Session
		addrJSON   []byte
		shipMethod *string
		promo      *string
		payToken   *string
		orderID    *string
		confirmKey *int64
		expReason  *string
	)
	err := r.db.QueryRow(ctx, query, arg).Scan(
		&s.ID, &s.UserID, &s.Status, &addrJSON, &shipMethod, &s.ShippingFeeMinor,
		&s.TaxMinor, &promo, &s.DiscountMinor, &s.SubtotalMinor, &s.TotalMinor,
		&s.Currency, &payToken, &orderID, &confirmKey, &s.ExpiresAt, &expReason,
		&s.CreatedAt, &s.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrSessionNotFound
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == invalidTextRepresentation {
			// A non-UUID id cannot name a session: report not-found, not 500.
			return nil, domain.ErrSessionNotFound
		}
		return nil, fmt.Errorf("scan session: %w", err)
	}
	if len(addrJSON) > 0 {
		var addr domain.Address
		if err := json.Unmarshal(addrJSON, &addr); err != nil {
			return nil, fmt.Errorf("decode address: %w", err)
		}
		s.Address = &addr
	}
	if shipMethod != nil {
		s.ShippingMethod = *shipMethod
	}
	if promo != nil {
		s.PromoCode = *promo
	}
	if payToken != nil {
		s.PaymentMethodToken = *payToken
	}
	if orderID != nil {
		s.OrderID = *orderID
	}
	s.ConfirmKeyID = confirmKey
	if expReason != nil {
		reason := domain.ExpiredReason(*expReason)
		s.ExpiredReason = &reason
	}

	rows, err := r.db.Query(ctx, `
		SELECT product_id, product_name, quantity, unit_price_minor, cart_price_minor, price_changed
		FROM checkout_session_items WHERE session_id = $1 ORDER BY id`, s.ID)
	if err != nil {
		return nil, fmt.Errorf("load items: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var it domain.SessionItem
		if err := rows.Scan(&it.ProductID, &it.ProductName, &it.Quantity,
			&it.UnitPriceMinor, &it.CartPriceMinor, &it.PriceChanged); err != nil {
			return nil, fmt.Errorf("scan item: %w", err)
		}
		s.Items = append(s.Items, it)
	}
	return &s, rows.Err()
}

// marshalAddress encodes a nullable address for the jsonb column. It returns
// a *string (not []byte): under pgx's simple query protocol — which this
// service uses for pooler safety — a []byte parameter is encoded as bytea
// hex, which Postgres rejects for jsonb ("invalid input syntax for type
// json"). A string round-trips correctly under both protocols.
func marshalAddress(addr *domain.Address) (*string, error) {
	if addr == nil {
		return nil, nil
	}
	b, err := json.Marshal(addr)
	if err != nil {
		return nil, fmt.Errorf("encode address: %w", err)
	}
	j := string(b)
	return &j, nil
}
