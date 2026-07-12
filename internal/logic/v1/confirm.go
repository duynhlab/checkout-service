package v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/duynhlab/pkg/idempotency"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	"github.com/duynhlab/checkout-service/middleware"
)

// Confirm-flow errors (see errors.go for the P1 set).
var (
	// ErrPriceChanged — re-validation found drifted prices; the session was
	// requoted back to shipping_set and the idempotency key was NOT consumed.
	ErrPriceChanged = errors.New("catalog prices changed; session requoted")
	// ErrStockUnavailable — re-validation found a shortage (or delisted line);
	// same requote semantics as ErrPriceChanged.
	ErrStockUnavailable = errors.New("requested quantity no longer available")
	// ErrConfirmInFlight — the session is bound to a different idempotency
	// claim (or a same-key race lost); 409.
	ErrConfirmInFlight = errors.New("another confirm owns this session")
	// ErrKeyConflict — same Idempotency-Key reused for a different request.
	ErrKeyConflict = errors.New("idempotency key reused with a different request")
	// ErrOrderRejected — order-service rejected a request checkout validated
	// locally; a bug, surfaced loudly as 500.
	ErrOrderRejected = errors.New("order rejected the confirm handoff")
)

// ConfirmDeadline bounds the WHOLE confirm execution. This is the fencing
// invariant (doubt-cycle b): every write this flow performs is ctx-bound, so
// no execution can write after confirmDeadline — and the idempotency lock
// takeover window is startup-validated to be much larger, so a takeover
// PROVES the previous owner is dead. Two live same-key executions cannot
// exist.
// Exported so cmd can validate lockTakeover > 4×ConfirmDeadline at startup.
const ConfirmDeadline = 15 * time.Second

// confirmPath is the Claim scope (same key on another endpoint = conflict).
const confirmPath = "/checkout/v1/private/checkout/sessions/confirm"

// Order-side validateCreate mirror — enforced BEFORE Claim/BeginConfirm so a
// rejection has zero side effects and a marker-set re-entry can never hit
// order-side InvalidArgument (poison-pill guard, doubt-cycle b).
const (
	maxOrderItems      = 200
	maxQuantity        = 10_000
	maxProductNameRune = 255
	maxUnitPriceMinor  = 1_000_000_000_000
)

// attemptSentinel marks "an order attempt was authorized" on the idempotency
// record BEFORE CreateOrder is first called (order ids start at 1, so 0 is
// unambiguous). Marker set ⇒ re-validation is skipped forever after — a
// requote after a possible order would lie about it.
var attemptSentinel = int64(0)

// OrderCreator is the logic-layer port for the order.v1 confirm handoff.
type OrderCreator interface {
	CreateOrder(ctx context.Context, userID string, items []domain.SessionItem, paymentToken, idemKey string) (orderID, status string, err error)
}

// IdemStore is the slice of pkg/idempotency this flow uses.
type IdemStore interface {
	Claim(ctx context.Context, userID int64, key, method, path, hash string) (*idempotency.Record, bool, error)
	Checkpoint(ctx context.Context, id int64, subjectID *int64) error
	Release(ctx context.Context, id int64) error
	Finish(ctx context.Context, id int64, code int, body []byte) error
}

// WithConfirm wires the P2 confirm dependencies (kept off the P1 constructor
// so existing call sites stay valid). Either being nil disables Confirm.
func (s *CheckoutService) WithConfirm(idem IdemStore, orders OrderCreator) *CheckoutService {
	s.idem = idem
	s.orders = orders
	return s
}

// Confirm executes POST …/sessions/:id/confirm — the idempotent order
// handoff (RFC-0015 P2, design: doubt-cycle b v3). On ErrPriceChanged /
// ErrStockUnavailable the returned session is the freshly requoted one.
func (s *CheckoutService) Confirm(ctx context.Context, userID, id, idemKey string) (*domain.Session, error) {
	ctx, cancel := context.WithTimeout(ctx, ConfirmDeadline)
	defer cancel()
	start := s.now()
	defer func() { confirmDuration.Record(ctx, s.now().Sub(start).Seconds()) }()
	ctx, span := middleware.StartSpan(ctx, "checkout.session.confirm", trace.WithAttributes(
		attribute.String("layer", "logic"),
	))
	defer span.End()

	if s.idem == nil || s.orders == nil {
		return nil, ErrUpstream
	}
	uid, err := strconv.ParseInt(userID, 10, 64)
	if err != nil || uid <= 0 || uid > math.MaxInt32 {
		// Platform user ids are numeric (JWT sub); anything else is a bug.
		return nil, fmt.Errorf("non-numeric user id in confirm: %w", err)
	}

	session, err := s.ownedSession(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if s.lazyExpire(ctx, session) {
		return nil, ErrSessionExpired
	}
	// Pre-claim bounds: zero side effects on rejection.
	if err := validateOrderBounds(session); err != nil {
		return nil, err
	}

	key, replay, err := s.claimConfirm(ctx, uid, idemKey, id)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if replay != nil {
		return replay, nil
	}

	// Entry gate + session↔key mutual exclusion. A deterministic rejection
	// releases the claim: holding the lock for the 90s takeover window would
	// turn an ordinary double-tab 409 into a black hole of ErrLocked
	// (review finding); no order attempt exists for THIS key yet, so the
	// release is side-effect free.
	if done, err := s.enterConfirm(ctx, session, key.ID); done || err != nil {
		if err != nil {
			_ = s.idem.Release(ctx, key.ID)
		}
		return session, err
	}
	// Re-read after winning the gate: a concurrent PUT (ready→ready token
	// re-attach) between our read and the CAS must not send a stale token or
	// items to order (security-review TOCTOU finding).
	session, err = s.repo.FindByID(ctx, session.ID)
	if err != nil || session.Status != domain.StatusConfirming ||
		session.ConfirmKeyID == nil || *session.ConfirmKeyID != key.ID {
		return nil, ErrConfirmInFlight
	}

	// Re-validate ONLY while no order attempt was ever authorized: the marker
	// makes "requote coexists with an existing order" structurally impossible.
	if key.SubjectID == nil {
		if requoted, rerr := s.revalidate(ctx, session, key.ID); rerr != nil {
			return requoted, rerr
		}
		if err := s.idem.Checkpoint(ctx, key.ID, &attemptSentinel); err != nil {
			span.RecordError(err)
			return nil, err // key stays locked; same-key retry re-drives
		}
	}

	orderID, err := s.placeOrder(ctx, session, key.ID, userID, idemKey)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	session, err = s.completeConfirm(ctx, session, key.ID, orderID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	confirmedCounter.Add(ctx, 1)
	if s.notifier != nil {
		s.notifier.SessionFinalized(ctx, session.ID)
	}
	return session, s.finishConfirm(ctx, key.ID, session)
}

// claimConfirm claims the idempotency key, translating the pkg sentinels and
// decoding a finished record into its cached replay session.
func (s *CheckoutService) claimConfirm(ctx context.Context, uid int64, idemKey, sessionID string) (*idempotency.Record, *domain.Session, error) {
	key, proceed, err := s.idem.Claim(ctx, uid, idemKey, "POST", confirmPath, sessionID)
	if err != nil {
		switch {
		case errors.Is(err, idempotency.ErrConflict):
			return nil, nil, ErrKeyConflict
		case errors.Is(err, idempotency.ErrLocked):
			return nil, nil, ErrConfirmInFlight
		default:
			return nil, nil, err
		}
	}
	if !proceed {
		replay, rerr := replaySession(key)
		return nil, replay, rerr
	}
	return key, nil, nil
}

// enterConfirm applies the entry gate and the session↔key mutual exclusion.
// done=true means the completed-recovery path already produced the answer
// (rebuild + Finish); a non-nil error rejects the confirm.
func (s *CheckoutService) enterConfirm(ctx context.Context, session *domain.Session, keyID int64) (bool, error) {
	switch session.Status { //nolint:exhaustive // every other status has no confirm entry
	case domain.StatusReady:
		if err := s.repo.BeginConfirm(ctx, session.ID, keyID); err != nil {
			return false, ErrConfirmInFlight
		}
		return false, nil
	case domain.StatusConfirming:
		if session.ConfirmKeyID == nil || *session.ConfirmKeyID != keyID {
			return false, ErrConfirmInFlight
		}
		return false, nil
	case domain.StatusCompleted:
		if session.ConfirmKeyID != nil && *session.ConfirmKeyID == keyID {
			// Crash between completion and Finish: rebuild and cache. This IS
			// the completion (the original attempt died before counting).
			confirmedCounter.Add(ctx, 1)
			return true, s.finishConfirm(ctx, keyID, session)
		}
		return false, ErrInvalidTransition
	default:
		return false, ErrInvalidTransition
	}
}

// placeOrder runs the order attempt / re-drive — always calling CreateOrder:
// it is idempotent by the deterministic composed key, and its response
// carries the canonical order id string + actual status (which may be past
// pending on a late replay).
func (s *CheckoutService) placeOrder(ctx context.Context, session *domain.Session, keyID int64, userID, idemKey string) (string, error) {
	orderID, _, err := s.orders.CreateOrder(ctx, userID, session.Items, session.PaymentMethodToken,
		"checkout:"+session.ID+":"+idemKey)
	if err != nil {
		if isOrderValidationBug(err) {
			// InvalidArgument created nothing order-side: release so the
			// (buggy) caller's next key isn't wedged behind this lock.
			_ = s.idem.Release(ctx, keyID)
			return "", ErrOrderRejected
		}
		// Transient (order down, timeout): stay confirming+bound, unlock the
		// key so an immediate same-key retry re-drives. Release failure only
		// delays that retry until the takeover window.
		_ = s.idem.Release(ctx, keyID)
		return "", ErrUpstream
	}
	if oid, perr := strconv.ParseInt(orderID, 10, 64); perr == nil {
		// Best-effort provenance; completion uses the string id.
		_ = s.idem.Checkpoint(ctx, keyID, &oid)
	}
	return orderID, nil
}

// completeConfirm CASes the session to completed under the claim binding. A
// stale answer is only tolerated when a same-key re-entry already completed
// it (foreign keys are fenced out) — never cache an outcome the session does
// not show.
func (s *CheckoutService) completeConfirm(ctx context.Context, session *domain.Session, keyID int64, orderID string) (*domain.Session, error) {
	err := s.repo.CompleteSession(ctx, session.ID, keyID, orderID)
	if err == nil {
		session.Status = domain.StatusCompleted
		session.OrderID = orderID
		return session, nil
	}
	if !errors.Is(err, domain.ErrStaleTransition) {
		return nil, err
	}
	fresh, ferr := s.repo.FindByID(ctx, session.ID)
	if ferr != nil || fresh.Status != domain.StatusCompleted ||
		fresh.ConfirmKeyID == nil || *fresh.ConfirmKeyID != keyID {
		return nil, ErrConfirmInFlight
	}
	return fresh, nil
}

// revalidate re-checks prices and stock against product (the checkout-time
// authority). A drift requotes the session back to shipping_set — the key is
// NOT consumed. Returns (requotedSession, ErrPriceChanged|ErrStockUnavailable)
// on drift, (nil, ErrUpstream) on transport trouble, (nil, nil) when clean.
func (s *CheckoutService) revalidate(ctx context.Context, session *domain.Session, keyID int64) (*domain.Session, error) {
	ids := make([]string, 0, len(session.Items))
	for _, it := range session.Items {
		ids = append(ids, it.ProductID)
	}
	infos, err := s.products.GetProducts(ctx, ids)
	if err != nil || (len(ids) > 0 && len(infos) == 0) {
		// Transport error — or a suspicious empty answer for a non-empty ask
		// (degraded product must not read as "everything delisted"). Release
		// failure only delays the same-key retry until the takeover window.
		_ = s.idem.Release(ctx, keyID)
		return nil, ErrUpstream
	}
	byID := make(map[string]ProductInfo, len(infos))
	for _, p := range infos {
		byID[p.ProductID] = p
	}

	var priceDrift, stockShort bool
	var subtotal int64
	fresh := make([]domain.SessionItem, len(session.Items))
	for i, it := range session.Items {
		info, ok := byID[it.ProductID]
		if !ok {
			// Delisted: keep the snapshot price, flag the line; it can never
			// confirm — the user edits the cart and requotes.
			stockShort = true
			it.PriceChanged = true
		} else {
			if info.UnitPriceMinor != it.UnitPriceMinor {
				priceDrift = true
				it.PriceChanged = true
				it.UnitPriceMinor = info.UnitPriceMinor
			}
			if it.Quantity > info.AvailableQty {
				stockShort = true
			}
		}
		subtotal += it.UnitPriceMinor * int64(it.Quantity)
		fresh[i] = it
	}
	if !priceDrift && !stockShort {
		return nil, nil
	}

	total := subtotal + session.ShippingFeeMinor + session.TaxMinor - session.DiscountMinor
	if err := s.repo.RequoteItems(ctx, session.ID, keyID, fresh, subtotal, total); err != nil {
		return nil, ErrConfirmInFlight
	}
	_ = s.idem.Release(ctx, keyID) // failure only delays the retry (takeover window)
	session.Status = domain.StatusShippingSet
	session.Items = fresh
	session.SubtotalMinor = subtotal
	session.TotalMinor = total
	session.ConfirmKeyID = nil
	priceChangedCounter.Add(ctx, 1)
	if stockShort {
		return session, ErrStockUnavailable
	}
	return session, ErrPriceChanged
}

// finishConfirm caches the completed session as the replay body. The domain
// JSON excludes the payment token and the claim binding (json:"-"), so no
// secret-shaped data enters the idempotency cache.
func (s *CheckoutService) finishConfirm(ctx context.Context, keyID int64, session *domain.Session) error {
	body, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("encode confirm body: %w", err)
	}
	if err := s.idem.Finish(ctx, keyID, 201, body); err != nil {
		return err // same-key retry re-enters the completed path and re-Finishes
	}
	return nil
}

// replaySession decodes a cached confirm response.
func replaySession(key *idempotency.Record) (*domain.Session, error) {
	var session domain.Session
	if err := json.Unmarshal(key.ResponseBody, &session); err != nil {
		return nil, fmt.Errorf("corrupt confirm cache: %w", err)
	}
	return &session, nil
}

// validateOrderBounds mirrors order-side validateCreate so a request that
// would be InvalidArgument there fails HERE, before any claim or binding.
func validateOrderBounds(session *domain.Session) error {
	if n := len(session.Items); n == 0 || n > maxOrderItems {
		return ErrOrderRejected
	}
	for _, it := range session.Items {
		if !isInt32ID(it.ProductID) {
			return ErrOrderRejected
		}
		if it.Quantity < 1 || it.Quantity > maxQuantity {
			return ErrOrderRejected
		}
		if utf8.RuneCountInString(it.ProductName) > maxProductNameRune {
			return ErrOrderRejected
		}
		if it.UnitPriceMinor < 0 || it.UnitPriceMinor > maxUnitPriceMinor {
			return ErrOrderRejected
		}
	}
	if session.PaymentMethodToken != "" && !domain.ValidPaymentToken(session.PaymentMethodToken) {
		return ErrOrderRejected
	}
	return nil
}

// isInt32ID reports whether s fits order's INTEGER schema columns.
func isInt32ID(s string) bool {
	n, err := strconv.ParseInt(s, 10, 64)
	return err == nil && n > 0 && n <= math.MaxInt32
}

// isOrderValidationBug reports whether the order error is InvalidArgument —
// impossible after the local bounds mirror, hence a bug (500), not a retry.
func isOrderValidationBug(err error) bool {
	return status.Code(err) == codes.InvalidArgument
}
