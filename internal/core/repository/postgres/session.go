// Package postgres implements the SessionRepository port against PostgreSQL.
// Raw SQL over pgxpool (simple protocol — pooler-safe), per-query timeouts,
// pgx sentinel errors translated to domain errors at this boundary.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
func (r *SessionRepository) Create(ctx context.Context, s *domain.Session) error {
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
func (r *SessionRepository) FindByID(ctx context.Context, id string) (*domain.Session, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	return r.scanSession(ctx, `
		SELECT id, user_id, status, address, shipping_method, shipping_fee_minor,
		       tax_minor, promo_code, discount_minor, subtotal_minor, total_minor,
		       currency, payment_method_token, order_id, expires_at, expired_reason,
		       created_at, updated_at
		FROM checkout_sessions WHERE id = $1`, id)
}

// FindActiveByUserID loads the user's single active session, if any.
func (r *SessionRepository) FindActiveByUserID(ctx context.Context, userID string) (*domain.Session, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	return r.scanSession(ctx, `
		SELECT id, user_id, status, address, shipping_method, shipping_fee_minor,
		       tax_minor, promo_code, discount_minor, subtotal_minor, total_minor,
		       currency, payment_method_token, order_id, expires_at, expired_reason,
		       created_at, updated_at
		FROM checkout_sessions
		WHERE user_id = $1
		  AND status IN ('open','address_set','shipping_set','ready','confirming')`, userID)
}

// UpdateStatus conditionally moves status (optimistic concurrency on `from`).
func (r *SessionRepository) UpdateStatus(ctx context.Context, id string, from, to domain.SessionStatus) error {
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
func (r *SessionRepository) SetAddress(ctx context.Context, id string, from domain.SessionStatus, addr *domain.Address) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	addrJSON, err := marshalAddress(addr)
	if err != nil {
		return err
	}
	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions SET status = 'address_set', address = $3, updated_at = now()
		WHERE id = $1 AND status = $2`, id, from, addrJSON)
	if err != nil {
		return fmt.Errorf("set address: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	return nil
}

// SetShipping persists the shipping choice and shipping_set in one
// conditional write. total_minor is recomputed in SQL from the persisted
// components so the stored total can never drift from its parts.
func (r *SessionRepository) SetShipping(ctx context.Context, id string, from domain.SessionStatus, method string, feeMinor int64) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions
		SET status = 'shipping_set', shipping_method = $3, shipping_fee_minor = $4,
		    total_minor = subtotal_minor + $4 + tax_minor - discount_minor,
		    updated_at = now()
		WHERE id = $1 AND status = $2`, id, from, method, feeMinor)
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
func (r *SessionRepository) SetPaymentToken(ctx context.Context, id string, from domain.SessionStatus, token string) error {
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

// Touch bumps expires_at — the reset-on-activity half of the expiry contract
// (RFC-0015 P2): every successful mutation both signals the abandonment
// workflow (timer reset) and bumps the DB expiry here, so the lazy backstop
// never expires a session the timer considers alive. Terminal sessions are
// left untouched — a late Touch is a harmless no-op, like a late timer.
func (r *SessionRepository) Touch(ctx context.Context, id string, expiresAt time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	_, err := r.db.Exec(ctx, `
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
func (r *SessionRepository) MarkExpired(ctx context.Context, id string, reason domain.ExpiredReason) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// `confirming` is deliberately NOT expirable: a confirm may have an order
	// handoff in flight (P2) and must finish or drop back to shipping_set —
	// never be yanked to expired mid-flight. Mirrors the FSM table, which has
	// no confirming→expired edge.
	_, err := r.db.Exec(ctx, `
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
		expReason  *string
	)
	err := r.db.QueryRow(ctx, query, arg).Scan(
		&s.ID, &s.UserID, &s.Status, &addrJSON, &shipMethod, &s.ShippingFeeMinor,
		&s.TaxMinor, &promo, &s.DiscountMinor, &s.SubtotalMinor, &s.TotalMinor,
		&s.Currency, &payToken, &orderID, &s.ExpiresAt, &expReason,
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
