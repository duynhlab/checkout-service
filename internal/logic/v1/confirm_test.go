package v1

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/duynhlab/pkg/idempotency"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// --- confirm doubles ---

type fakeIdem struct {
	record      *idempotency.Record
	proceed     bool
	claimErr    error
	claimCalls  int
	checkpoints []int64
	chkErr      error
	released    int
	finished    int
	finishCode  int
	finishBody  []byte
}

func (f *fakeIdem) Claim(_ context.Context, _ int64, _, _, _, _ string) (*idempotency.Record, bool, error) {
	f.claimCalls++
	return f.record, f.proceed, f.claimErr
}

func (f *fakeIdem) Checkpoint(_ context.Context, _ int64, subjectID *int64) error {
	if f.chkErr != nil {
		return f.chkErr
	}
	f.checkpoints = append(f.checkpoints, *subjectID)
	if f.record != nil {
		v := *subjectID
		f.record.SubjectID = &v
	}
	return nil
}

func (f *fakeIdem) Release(_ context.Context, _ int64) error { f.released++; return nil }

func (f *fakeIdem) Finish(_ context.Context, _ int64, code int, body []byte) error {
	f.finished++
	f.finishCode = code
	f.finishBody = body
	return nil
}

type fakeOrders struct {
	orderID     string
	status      string
	err         error
	calls       int
	gotKey      string
	gotTok      string
	gotFee      int64
	gotTax      int64
	gotDiscount int64
}

func (f *fakeOrders) CreateOrder(_ context.Context, _ string, _ []domain.SessionItem, tok, idemKey string, fee, tax, discount int64) (string, string, error) {
	f.calls++
	f.gotTok = tok
	f.gotKey = idemKey
	f.gotFee, f.gotTax, f.gotDiscount = fee, tax, discount
	if f.err != nil {
		return "", "", f.err
	}
	return f.orderID, f.status, nil
}

func readySession() *domain.Session {
	s := liveSession(domain.StatusReady)
	s.Items = []domain.SessionItem{
		{ProductID: "1", ProductName: "Mouse", Quantity: 2, UnitPriceMinor: 2999, CartPriceMinor: 2999},
	}
	s.SubtotalMinor = 5998
	s.TotalMinor = 5998
	s.PaymentMethodToken = "tok_visa_ok"
	return s
}

func confirmSvc(repo *fakeRepo, prods *fakeProducts, idem *fakeIdem, orders *fakeOrders) *CheckoutService {
	return newSvc(repo, &fakeCart{}, prods).WithConfirm(idem, orders)
}

func inStock() *fakeProducts {
	return &fakeProducts{infos: []ProductInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5}}}
}

// --- happy path ---

func TestConfirm_FreshCreatesOrderAndCompletes(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{orderID: "42", status: "pending"}

	s, err := confirmSvc(repo, inStock(), idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1")
	if err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	if s.Status != domain.StatusCompleted || s.OrderID != "42" {
		t.Errorf("session = %s/%s, want completed/42", s.Status, s.OrderID)
	}
	if repo.confirmedKey != 11 || repo.completedOrder != "42" {
		t.Errorf("repo bound=%d completed=%s, want 11/42", repo.confirmedKey, repo.completedOrder)
	}
	// Marker (0) MUST be checkpointed before the order attempt; the real id follows.
	if len(idem.checkpoints) != 2 || idem.checkpoints[0] != 0 || idem.checkpoints[1] != 42 {
		t.Errorf("checkpoints = %v, want [0 42] (marker before CreateOrder)", idem.checkpoints)
	}
	if orders.gotKey != "checkout:sess-1:key-1" {
		t.Errorf("order idem key = %s, want deterministic composed key", orders.gotKey)
	}
	if idem.finished != 1 || idem.finishCode != 201 {
		t.Errorf("finish = %d/%d, want one 201", idem.finished, idem.finishCode)
	}
	if strings.Contains(string(idem.finishBody), "tok_visa_ok") {
		t.Error("payment token leaked into the idempotency cache")
	}
}

func TestConfirm_ReplayReturnsCacheWithoutSideEffects(t *testing.T) {
	code := 201
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{proceed: false, record: &idempotency.Record{
		ID: 11, ResponseCode: &code,
		ResponseBody: []byte(`{"id":"sess-1","status":"completed","order_id":"42"}`),
	}}
	orders := &fakeOrders{}

	s, err := confirmSvc(repo, inStock(), idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if s.OrderID != "42" || orders.calls != 0 || repo.completedOrder != "" {
		t.Errorf("replay must be pure cache: order=%s calls=%d", s.OrderID, orders.calls)
	}
}

// --- requote (key never burned, no order ever created) ---

func TestConfirm_PriceDriftRequotesWithoutBurningKey(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{}
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 3499, AvailableQty: 5}}}

	s, err := confirmSvc(repo, prods, idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrPriceChanged) {
		t.Fatalf("err = %v, want ErrPriceChanged", err)
	}
	if s == nil || s.Status != domain.StatusShippingSet || s.Items[0].UnitPriceMinor != 3499 || s.SubtotalMinor != 6998 {
		t.Errorf("requoted session = %+v, want shipping_set at 3499", s)
	}
	if repo.requoted == nil || repo.requoted.keyID != 11 {
		t.Error("requote must go through the CAS-guarded repo write")
	}
	if orders.calls != 0 || len(idem.checkpoints) != 0 {
		t.Error("requote must never attempt an order or set the marker")
	}
	if idem.released != 1 || idem.finished != 0 {
		t.Errorf("released=%d finished=%d, want key released not consumed", idem.released, idem.finished)
	}
}

// An unknown SKU is PERSISTENT: it lasts until an operator seeds the missing
// balance row. Sending it down the transport-trouble branch would leave the
// session in `confirming` forever -- lazyExpire skips that state, the FSM has no
// confirming → cancelled edge, and FindActiveByUserID keeps handing the dead
// session back instead of letting the shopper open a new checkout. So it must
// REQUOTE out of confirming, exactly like a definite answer, while still refusing
// to claim the item is unavailable.
func TestConfirm_UnknownSKURequotesInsteadOfStranding(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{}
	prods := inStock()
	prods.unknown = []string{"1"}

	s, err := confirmSvc(repo, prods, idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrAvailabilityUnknown) {
		t.Fatalf("err = %v, want ErrAvailabilityUnknown", err)
	}
	if errors.Is(err, ErrStockUnavailable) {
		t.Error("a data gap must never be reported as out-of-stock")
	}
	if s == nil || s.Status != domain.StatusShippingSet {
		t.Fatalf("session = %+v, want a requoted shipping_set session (not stranded in confirming)", s)
	}
	if s.ConfirmKeyID != nil {
		t.Error("the confirm claim must be cleared so a FRESH key can retry (else 409 forever)")
	}
	if repo.requoted == nil || repo.requoted.keyID != 11 {
		t.Error("the escape must go through the CAS-guarded requote write")
	}
	if idem.released != 1 || idem.finished != 0 {
		t.Errorf("released=%d finished=%d, want the key released and NOT consumed", idem.released, idem.finished)
	}
	if orders.calls != 0 {
		t.Error("no order may be created when availability is unknown")
	}
}

func TestConfirm_StockShortageRequotes(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 2999, AvailableQty: 1}}}

	_, err := confirmSvc(repo, prods, idem, &fakeOrders{}).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrStockUnavailable) {
		t.Fatalf("err = %v, want ErrStockUnavailable", err)
	}
}

// A line the catalog no longer answers for is out-of-stock, not a transient.
//
// The session carries TWO lines on purpose. With one line, "delisted" and "the
// upstream answered about nothing we asked for" are the same observation, and
// revalidate deliberately resolves that ambiguity the retryable way (ErrUpstream —
// a degraded upstream must not read as "everything delisted"). Two lines make the
// answer non-empty, so the missing one is unambiguously delisted.
func TestConfirm_DelistedLineIsStockUnavailable(t *testing.T) {
	session := readySession()
	session.Items = append(session.Items, domain.SessionItem{
		ProductID: "2", ProductName: "Pad", Quantity: 1, UnitPriceMinor: 500, CartPriceMinor: 500,
	})
	repo := &fakeRepo{byID: session}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	// Only line 2 is still in the catalog; line 1 is gone.
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "2", UnitPriceMinor: 500, AvailableQty: 1}}}

	s, err := confirmSvc(repo, prods, idem, &fakeOrders{}).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrStockUnavailable) {
		t.Fatalf("err = %v, want ErrStockUnavailable (delisted)", err)
	}
	if !s.Items[0].PriceChanged || s.Items[0].UnitPriceMinor != 2999 {
		t.Errorf("delisted line = %+v, want flagged with snapshot price kept", s.Items[0])
	}
}

// The one-line case the test above avoids, asserted on purpose rather than left
// implicit: an answer that covers nothing we asked for is treated as a degraded
// upstream (retryable), NOT as a delisted basket.
func TestConfirm_EmptyCatalogAnswerIsRetryable(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "999", UnitPriceMinor: 1, AvailableQty: 1}}}

	_, err := confirmSvc(repo, prods, idem, &fakeOrders{}).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrUpstream) {
		t.Fatalf("err = %v, want ErrUpstream — an empty answer must not read as delisted", err)
	}
}

// An UNSELLABLE line is a definite answer: requote, never a retryable 503.
//
// This is the case the availability rework got wrong at first, and the cost was not
// a bad error code — it was a dead end. mergeCatalog filters unsellable lines, so a
// one-line basket produced an empty MERGED result; the guard read that as a degraded
// upstream and answered 503 "retry with the same Idempotency-Key". The condition is
// deterministic, so the retry could never succeed, and the session was left at
// `confirming`, which lazyExpire skips and the FSM cannot leave — so the shopper
// could not buy anything at all until the TTL elapsed.
//
// A single line is used on purpose: that is the shape that broke.
func TestConfirm_UnsellableLineRequotesRatherThanRetrying(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	// Product ANSWERS about the requested sku — it just is not sellable any more.
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, Sellable: false}}}
	checker := &fakeChecker{res: AvailabilityResult{CanFulfill: true}}
	svc := NewCheckoutService(repo, &fakeCart{}, prices, checker, time.Minute).
		WithConfirm(idem, &fakeOrders{})

	_, err := svc.Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrStockUnavailable) {
		t.Fatalf("err = %v, want ErrStockUnavailable — an unsellable line is definite, not retryable", err)
	}
	if errors.Is(err, ErrUpstream) {
		t.Error("an unsellable line must not be reported as a degraded upstream")
	}
}

// The same shape with a WRONG CURRENCY, which mergeCatalog also filters.
func TestConfirm_WrongCurrencyLineRequotesRatherThanRetrying(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", UnitPriceMinor: 2999, Sellable: true, Currency: "EUR"}}}
	checker := &fakeChecker{res: AvailabilityResult{CanFulfill: true}}
	svc := NewCheckoutService(repo, &fakeCart{}, prices, checker, time.Minute).
		WithConfirm(idem, &fakeOrders{})

	if _, err := svc.Confirm(context.Background(), "7", "sess-1", "key-1"); !errors.Is(err, ErrStockUnavailable) {
		t.Fatalf("err = %v, want ErrStockUnavailable", err)
	}
}

// --- transients: key released, session stays confirming+bound ---

func TestConfirm_ProductOutageIsRetryable(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	prods := &fakeProducts{err: errors.New("dial refused")}

	_, err := confirmSvc(repo, prods, idem, &fakeOrders{}).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrUpstream) || idem.released != 1 {
		t.Fatalf("err=%v released=%d, want ErrUpstream + key released", err, idem.released)
	}
	if repo.requoted != nil {
		t.Error("outage must not requote (degraded product ≠ everything delisted)")
	}
}

func TestConfirm_EmptyProductAnswerIsOutageNotMassDelist(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}

	_, err := confirmSvc(repo, &fakeProducts{}, idem, &fakeOrders{}).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrUpstream) {
		t.Fatalf("err = %v, want ErrUpstream", err)
	}
}

func TestConfirm_OrderOutageReleasesKeyStaysConfirming(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{err: status.Error(codes.Unavailable, "fulfillment temporarily unavailable")}

	_, err := confirmSvc(repo, inStock(), idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrUpstream) {
		t.Fatalf("err = %v, want ErrUpstream", err)
	}
	if idem.released != 1 || repo.completedOrder != "" || idem.finished != 0 {
		t.Error("transient order failure: release key, no completion, no cache")
	}
}

// --- crash re-entry (marker semantics) ---

func TestConfirm_MarkerReentrySkipsRevalidationAndRedrives(t *testing.T) {
	sess := readySession()
	sess.Status = domain.StatusConfirming
	bound := int64(11)
	sess.ConfirmKeyID = &bound
	repo := &fakeRepo{byID: sess}
	marker := int64(0)
	idem := &fakeIdem{record: &idempotency.Record{ID: 11, SubjectID: &marker}, proceed: true}
	orders := &fakeOrders{orderID: "42", status: "pending"}
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 9999, AvailableQty: 0}}} // drifted AND short

	s, err := confirmSvc(repo, prods, idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1")
	if err != nil {
		t.Fatalf("re-drive: %v", err)
	}
	// Marker set ⇒ price/stock drift is IGNORED (an order may already exist);
	// CreateOrder replays/creates under the same deterministic key.
	if repo.requoted != nil {
		t.Error("marker re-entry must never requote")
	}
	if orders.calls != 1 || s.OrderID != "42" || s.Status != domain.StatusCompleted {
		t.Errorf("re-drive = calls %d, %s/%s, want 1 call completed/42", orders.calls, s.Status, s.OrderID)
	}
}

func TestConfirm_ForeignKeyOnConfirmingSessionFencedOut(t *testing.T) {
	sess := readySession()
	sess.Status = domain.StatusConfirming
	bound := int64(99)
	sess.ConfirmKeyID = &bound
	repo := &fakeRepo{byID: sess}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{}

	_, err := confirmSvc(repo, inStock(), idem, orders).Confirm(context.Background(), "7", "sess-1", "key-2")
	if !errors.Is(err, ErrConfirmInFlight) || orders.calls != 0 {
		t.Fatalf("err=%v calls=%d, want fenced out with zero order calls", err, orders.calls)
	}
}

func TestConfirm_CompletedWithBindingRebuildsAndFinishes(t *testing.T) {
	sess := readySession()
	sess.Status = domain.StatusCompleted
	sess.OrderID = "42"
	bound := int64(11)
	sess.ConfirmKeyID = &bound
	repo := &fakeRepo{byID: sess}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{}

	s, err := confirmSvc(repo, inStock(), idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1")
	if err != nil || s.OrderID != "42" {
		t.Fatalf("completed recovery = (%v, %v), want cached 42", s, err)
	}
	if idem.finished != 1 || orders.calls != 0 {
		t.Errorf("finished=%d calls=%d, want Finish without a new order attempt", idem.finished, orders.calls)
	}
}

func TestConfirm_CompletedForeignKeyGetsNo201(t *testing.T) {
	sess := readySession()
	sess.Status = domain.StatusCompleted
	bound := int64(99)
	sess.ConfirmKeyID = &bound
	repo := &fakeRepo{byID: sess}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}

	_, err := confirmSvc(repo, inStock(), idem, &fakeOrders{}).Confirm(context.Background(), "7", "sess-1", "key-2")
	if !errors.Is(err, ErrInvalidTransition) || idem.finished != 0 {
		t.Fatalf("err=%v finished=%d, want no post-hoc 201 minting", err, idem.finished)
	}
}

// --- guards ---

func TestConfirm_BoundsRejectedBeforeClaim(t *testing.T) {
	sess := readySession()
	sess.Items[0].Quantity = 20_000
	repo := &fakeRepo{byID: sess}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}

	_, err := confirmSvc(repo, inStock(), idem, &fakeOrders{}).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrOrderRejected) {
		t.Fatalf("err = %v, want ErrOrderRejected", err)
	}
	if idem.claimCalls != 0 || repo.confirmedKey != 0 {
		t.Error("bounds violation must have ZERO side effects (no claim, no binding)")
	}
}

func TestConfirm_LockedKeyIs409(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{claimErr: idempotency.ErrLocked}

	_, err := confirmSvc(repo, inStock(), idem, &fakeOrders{}).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrConfirmInFlight) {
		t.Fatalf("err = %v, want ErrConfirmInFlight", err)
	}
}

func TestConfirm_NotReadySessionRejected(t *testing.T) {
	repo := &fakeRepo{byID: readySession()}
	repo.byID.Status = domain.StatusAddressSet
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}

	_, err := confirmSvc(repo, inStock(), idem, &fakeOrders{}).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("err = %v, want ErrInvalidTransition", err)
	}
}

func TestConfirm_StaleCompleteRecoversOnlyWhenSameKeyWon(t *testing.T) {
	// Crash-race branch: CompleteSession answers stale; the re-read decides.
	completed := readySession()
	completed.Status = domain.StatusCompleted
	completed.OrderID = "42"
	bound := int64(11)
	completed.ConfirmKeyID = &bound

	repo := &fakeRepo{byID: readySession(), completeErr: domain.ErrStaleTransition, afterComplete: completed}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{orderID: "42", status: "pending"}

	s, err := confirmSvc(repo, inStock(), idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1")
	if err != nil || s.OrderID != "42" {
		t.Fatalf("stale recovery = (%v, %v), want the same-key completion adopted", s, err)
	}
	if idem.finished != 1 {
		t.Errorf("finished = %d, want the recovered outcome cached", idem.finished)
	}
}

func TestConfirm_RequoteRecomputesTaxOnFreshSubtotal(t *testing.T) {
	sess := readySession() // subtotal 5998, VN address
	sess.ShippingFeeMinor = 300
	sess.TaxMinor = 504 // stale: computed on the old subtotal
	repo := &fakeRepo{byID: sess, taxBps: 800}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 3499, AvailableQty: 5}}}
	q := &fakeQuoter{fee: 300}

	s, err := confirmSvc(repo, prods, idem, &fakeOrders{}).WithQuoter(q).
		Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrPriceChanged) {
		t.Fatalf("err = %v, want ErrPriceChanged", err)
	}
	// New subtotal 2×3499 = 6998; tax = (6998+300)×800bps = 583 (floored).
	wantTax := int64((6998 + 300) * 800 / 10_000)
	if repo.requoted == nil || repo.requoted.tax != wantTax {
		t.Fatalf("requoted tax = %+v, want %d recomputed on the fresh subtotal", repo.requoted, wantTax)
	}
	if s.TaxMinor != wantTax || s.TotalMinor != 6998+300+wantTax {
		t.Errorf("session tax/total = %d/%d, want %d/%d", s.TaxMinor, s.TotalMinor, wantTax, 6998+300+wantTax)
	}
}

func TestConfirm_RequoteTaxLookupFailureIsRetryable(t *testing.T) {
	sess := readySession()
	repo := &fakeRepo{byID: sess, taxErr: errors.New("db down")}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 3499, AvailableQty: 5}}}

	_, err := confirmSvc(repo, prods, idem, &fakeOrders{}).WithQuoter(&fakeQuoter{fee: 300}).
		Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrUpstream) || idem.released != 1 {
		t.Fatalf("err=%v released=%d, want ErrUpstream + key released (never persist a lying tax)", err, idem.released)
	}
	if repo.requoted != nil {
		t.Error("requote persisted despite the failed rate lookup")
	}
}

// --- P4: promo redemption inside confirm ---

func readyPromoSession() *domain.Session {
	s := readySession() // subtotal 5998
	s.PromoCode = "SAVE5"
	s.DiscountMinor = 500
	s.TotalMinor = 5998 - 500
	return s
}

func TestConfirm_RedeemsBeforeMarkerAndSendsComponents(t *testing.T) {
	repo := &fakeRepo{byID: readyPromoSession(), promo: &domain.Promo{Code: "SAVE5", Kind: "fixed", Value: 500}}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{orderID: "42", status: "pending"}

	s, err := confirmSvc(repo, inStock(), idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1")
	if err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	if len(repo.redeemed) != 1 || repo.redeemed[0] != "SAVE5" {
		t.Fatalf("redeemed = %v, want exactly one SAVE5", repo.redeemed)
	}
	if orders.gotDiscount != 500 {
		t.Errorf("order discount = %d, want 500 (the charged total must match the session)", orders.gotDiscount)
	}
	if repo.backfilled != "42" {
		t.Errorf("backfilled order = %q, want 42 (redemption provenance)", repo.backfilled)
	}
	if s.Status != domain.StatusCompleted {
		t.Errorf("status = %s, want completed", s.Status)
	}
}

func TestConfirm_MarkerReentrySkipsRedeem(t *testing.T) {
	// Marker set ⇒ the redemption already committed (redeem sits strictly
	// before the marker): a re-drive must NOT touch the promo tables — an
	// expiry flip between attempts can never strip a session whose order may
	// exist (doubt-cycle d).
	sess := readyPromoSession()
	sess.Status = domain.StatusConfirming
	bound := int64(11)
	sess.ConfirmKeyID = &bound
	marker := int64(0)
	repo := &fakeRepo{byID: sess}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11, SubjectID: &marker}, proceed: true}
	orders := &fakeOrders{orderID: "42", status: "pending"}

	if _, err := confirmSvc(repo, inStock(), idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1"); err != nil {
		t.Fatalf("re-drive: %v", err)
	}
	if len(repo.redeemed) != 0 {
		t.Fatalf("redeemed = %v, want none on a marker re-entry", repo.redeemed)
	}
	if orders.calls != 1 {
		t.Errorf("order calls = %d, want the re-drive", orders.calls)
	}
}

func TestConfirm_ExhaustedStripsAndKeepsKeyUsable(t *testing.T) {
	repo := &fakeRepo{byID: readyPromoSession(), redeemErr: domain.ErrPromoExhausted}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{}

	s, err := confirmSvc(repo, inStock(), idem, orders).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrPromoExhausted) {
		t.Fatalf("err = %v, want ErrPromoExhausted", err)
	}
	if !repo.promoStripped || s.Status != domain.StatusShippingSet || s.DiscountMinor != 0 {
		t.Fatalf("session = %+v stripped=%v, want promo stripped to shipping_set", s, repo.promoStripped)
	}
	if orders.calls != 0 || len(idem.checkpoints) != 0 {
		t.Error("no order attempt and no marker may exist on the strip path")
	}
	if idem.released != 1 || idem.finished != 0 {
		t.Errorf("released=%d finished=%d, want key released not consumed", idem.released, idem.finished)
	}
}

func TestConfirm_RedeemTransientIsRetryable(t *testing.T) {
	repo := &fakeRepo{byID: readyPromoSession(), redeemErr: errors.New("db down")}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}

	_, err := confirmSvc(repo, inStock(), idem, &fakeOrders{}).Confirm(context.Background(), "7", "sess-1", "key-1")
	if !errors.Is(err, ErrUpstream) || idem.released != 1 || repo.promoStripped {
		t.Fatalf("err=%v released=%d stripped=%v, want retryable with session intact", err, idem.released, repo.promoStripped)
	}
}
