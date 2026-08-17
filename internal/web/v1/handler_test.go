package v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/httpx"
	"github.com/duynhlab/pkg/idempotency"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	logicv1 "github.com/duynhlab/checkout-service/internal/logic/v1"
)

func init() { gin.SetMode(gin.TestMode) }

// --- logic doubles (repo/cart/product level, driving the real logic layer) ---

type fakeRepo struct {
	active      *domain.Session
	byID        *domain.Session
	createErr   error
	findErr     error
	beginErr    error
	completeErr error
	// findErrOnCall fails only the Nth FindByID call (1-based): the confirm
	// flow re-reads the session AFTER winning the gate, and that second read
	// hitting a failover must not masquerade as "confirm in flight".
	findErrOnCall  int
	findCalls      int
	persistedToken string
	// promo overrides the default WELCOME10 when a test needs a code with caps
	// on it; userRedemptions is what CountUserRedemptions reports. Both exist so
	// the exhausted branches are reachable from the HTTP layer at all — the
	// hardcoded promo below has neither cap, so before this the apply path's
	// 409 could only be exercised in the logic package.
	promo           *domain.Promo
	userRedemptions int
	// redeemErr is what RedeemPromo returns. The confirm gate maps the domain
	// promo errors onto the 409 requote envelope, and that mapping is only
	// reachable from the HTTP layer through this hook.
	redeemErr error
}

func (f *fakeRepo) Create(_ context.Context, s *domain.Session) error {
	if f.createErr != nil {
		return f.createErr
	}
	s.ID = "11111111-1111-1111-1111-111111111111"
	return nil
}

// FindByID is the single injection point for datastore failure: every
// session-scoped endpoint loads the session before doing anything else, so one
// hook here exercises the whole respondSessionError surface.
func (f *fakeRepo) FindByID(_ context.Context, _ string) (*domain.Session, error) {
	f.findCalls++
	if f.findErr != nil || f.findCalls == f.findErrOnCall {
		if f.findErr != nil {
			return nil, f.findErr
		}
		return nil, errUnavailableForTest()
	}
	if f.byID == nil {
		return nil, domain.ErrSessionNotFound
	}
	return f.byID, nil
}

func (f *fakeRepo) FindActiveByUserID(_ context.Context, _ string) (*domain.Session, error) {
	if f.active == nil {
		return nil, domain.ErrSessionNotFound
	}
	return f.active, nil
}

func (f *fakeRepo) UpdateStatus(_ context.Context, _ string, _, _ domain.SessionStatus) error {
	return nil
}

func (f *fakeRepo) SetAddress(_ context.Context, _ string, _ domain.SessionStatus, _ *domain.Address, _ int64) error {
	return nil
}

func (f *fakeRepo) MarkExpired(_ context.Context, _ string, _ domain.ExpiredReason) error {
	return nil
}

func (f *fakeRepo) SetShipping(_ context.Context, _ string, _ domain.SessionStatus, _ time.Time, _ string, _, _, _ int64) error {
	return nil
}

func (f *fakeRepo) GetTaxRateBps(_ context.Context, _ string) (int32, error) { return 1000, nil }

func (f *fakeRepo) SetPaymentToken(_ context.Context, _ string, _ domain.SessionStatus, token string) error {
	f.persistedToken = token
	return nil
}

func (f *fakeRepo) Touch(_ context.Context, _ string, _ time.Time) error { return nil }

func (f *fakeRepo) BeginConfirm(_ context.Context, _ string, keyID int64) error {
	if f.beginErr != nil {
		return f.beginErr
	}
	if f.byID != nil {
		f.byID.Status = domain.StatusConfirming
		k := keyID
		f.byID.ConfirmKeyID = &k
	}
	return nil
}

func (f *fakeRepo) RequoteItems(_ context.Context, _ string, _ int64, _ []domain.SessionItem, _, _, _ int64) error {
	return nil
}

func (f *fakeRepo) CompleteSession(_ context.Context, _ string, _ int64, _ string) error {
	return f.completeErr
}

func (f *fakeRepo) GetPromo(_ context.Context, code string) (*domain.Promo, error) {
	if f.promo != nil {
		if code != f.promo.Code {
			return nil, domain.ErrPromoNotFound
		}
		return f.promo, nil
	}
	if code != "WELCOME10" {
		return nil, domain.ErrPromoNotFound
	}
	return &domain.Promo{Code: code, Kind: "percent", Value: 10}, nil
}

func (f *fakeRepo) CountUserRedemptions(_ context.Context, _, _ string) (int, error) {
	return f.userRedemptions, nil
}

func (f *fakeRepo) SetPromo(_ context.Context, _ string, _ domain.SessionStatus, code string, discountMinor int64) error {
	if f.byID != nil {
		f.byID.PromoCode = code
		f.byID.DiscountMinor = discountMinor
		f.byID.TotalMinor = f.byID.SubtotalMinor + f.byID.ShippingFeeMinor + f.byID.TaxMinor - discountMinor
	}
	return nil
}

func (f *fakeRepo) StripPromo(_ context.Context, _ string, _ int64) error { return nil }

func (f *fakeRepo) RedeemPromo(_ context.Context, _, _, _ string) error { return f.redeemErr }

func (f *fakeRepo) BackfillRedemptionOrder(_ context.Context, _, _, _ string) error { return nil }

type fakeCart struct {
	lines []logicv1.CartLine
	err   error
}

func (f *fakeCart) GetCart(_ context.Context, _ string) ([]logicv1.CartLine, error) {
	return f.lines, f.err
}

// fakeProducts satisfies BOTH halves of the split catalog read from one
// ProductInfo fixture (prices from product, availability from inventory since
// RFC-0021 phase 4). The handler tests care about HTTP behaviour, not which
// authority answered, so one fixture is the right level of detail here.
type fakeProducts struct {
	infos []logicv1.ProductInfo
	// unknown makes CheckAvailability report SKUs inventory does not track: the
	// data-gap answer, which must never reach the shopper as "unavailable".
	unknown []string
}

func (f *fakeProducts) BatchGetCurrentPrices(_ context.Context, _ []string) ([]logicv1.PriceInfo, error) {
	out := make([]logicv1.PriceInfo, 0, len(f.infos))
	for _, i := range f.infos {
		out = append(out, logicv1.PriceInfo{
			ProductID: i.ProductID, Name: i.Name,
			UnitPriceMinor: i.UnitPriceMinor, Sellable: true, Currency: "USD",
		})
	}
	return out, nil
}

func (f *fakeProducts) CheckAvailability(_ context.Context, lines []logicv1.AvailabilityLine) (logicv1.AvailabilityResult, error) {
	qty := make(map[string]int, len(f.infos))
	for _, i := range f.infos {
		qty[i.ProductID] = i.AvailableQty
	}
	res := logicv1.AvailabilityResult{CanFulfill: true, UnknownSKUIDs: f.unknown}
	if len(f.unknown) > 0 {
		res.CanFulfill = false // an unknown SKU can never be promised
	}
	for _, l := range lines {
		if have := qty[l.SKUID]; have < l.Quantity {
			res.CanFulfill = false
			res.Shortages = append(res.Shortages, logicv1.Shortage{
				SKUID: l.SKUID, Requested: int64(l.Quantity), AvailableToPromise: int64(have),
			})
		}
	}
	return res, nil
}

// newRouter mounts the real routes with a fake JWT middleware injecting userID
// (empty = unauthenticated 401, mirroring authmw's fail-closed behavior).
func newRouter(repo *fakeRepo, cart *fakeCart, prods *fakeProducts, userID string) *gin.Engine {
	r := gin.New()
	svc := logicv1.NewCheckoutService(repo, cart, prods, prods, time.Minute)
	RegisterRoutes(r, NewHandler(svc), func(c *gin.Context) {
		if userID == "" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Set(authmw.CtxUserID, userID)
		c.Next()
	})
	return r
}

func doJSON(r *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(rec, req)
	return rec
}

func liveSession(userID string, status domain.SessionStatus) *domain.Session {
	return &domain.Session{
		ID: "11111111-1111-1111-1111-111111111111", UserID: userID, Status: status,
		Address:   &domain.Address{FullName: "A", Line1: "1", City: "HN", Country: "VN"},
		ExpiresAt: time.Now().Add(time.Hour),
	}
}

// --- tests ---

func TestCreateSession_201WithSnapshot(t *testing.T) {
	r := newRouter(&fakeRepo{},
		&fakeCart{lines: []logicv1.CartLine{{ProductID: "1", ProductName: "Mouse", Quantity: 1, CartPriceMinor: 2999}}},
		&fakeProducts{infos: []logicv1.ProductInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 3499}}}, "7")

	rec := doJSON(r, http.MethodPost, "/checkout/v1/private/checkout/sessions", "")
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var body sessionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid body: %v", err)
	}
	if !body.Items[0].PriceChanged || body.Items[0].UnitPrice != 34.99 {
		t.Errorf("item = %+v, want price_changed with product price 34.99 (dollars on the wire)", body.Items[0])
	}
	if body.ID == "" {
		t.Error("id missing from unwrapped response")
	}
	if body.Total != 34.99 {
		t.Errorf("total = %v, want 34.99 (dollars on the wire)", body.Total)
	}
}

func TestCreateSession_200OnExistingActive(t *testing.T) {
	r := newRouter(&fakeRepo{active: liveSession("7", domain.StatusOpen)}, &fakeCart{}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodPost, "/checkout/v1/private/checkout/sessions", "")
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (idempotent create)", rec.Code)
	}
}

func TestCreateSession_EmptyCartIs409(t *testing.T) {
	r := newRouter(&fakeRepo{}, &fakeCart{}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodPost, "/checkout/v1/private/checkout/sessions", "")
	if rec.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", rec.Code)
	}
}

// A dependency being down is 503 + Retry-After, not 500, and the body stays opaque.
//
// This test used to assert 500 (it was named ...IsOpaque500) and it was two
// properties in one name: the opacity, which was the point, and the status, which
// was incidental. 500 tells the client not to retry and tells on-call that checkout
// is broken, when the truth is that cart, product or inventory is unreachable. The
// confirm path always answered 503 here; create was inconsistent with it.
//
// RFC-0021 phase 4 is what made this worth fixing rather than noting: session create
// now consults inventory as the availability authority, so an inventory outage
// reaches this arm on the happy-path route. It was found by the local-stack e2e
// audit, not here — a unit test can assert the logic error and still miss the status.
func TestCreateSession_UpstreamDownIsRetryableAndOpaque(t *testing.T) {
	r := newRouter(&fakeRepo{}, &fakeCart{err: errors.New("dial tcp 10.1.2.3: refused")}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodPost, "/checkout/v1/private/checkout/sessions", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 — a dependency outage is retryable, not a checkout bug", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Error("503 without Retry-After: the client is told to retry but not when")
	}
	if strings.Contains(rec.Body.String(), "10.1.2.3") {
		t.Errorf("body leaks upstream internals: %s", rec.Body.String())
	}
}

func TestGetSession_AntiIDOR404ForForeignOwner(t *testing.T) {
	r := newRouter(&fakeRepo{byID: liveSession("7", domain.StatusOpen)}, &fakeCart{}, &fakeProducts{}, "8")
	rec := doJSON(r, http.MethodGet, "/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111", "")
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (foreign session indistinguishable from missing)", rec.Code)
	}
}

func TestGetSession_Expired410WithCode(t *testing.T) {
	stale := liveSession("7", domain.StatusReady)
	stale.ExpiresAt = time.Now().Add(-time.Minute)
	r := newRouter(&fakeRepo{byID: stale}, &fakeCart{}, &fakeProducts{}, "7")

	rec := doJSON(r, http.MethodGet, "/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111", "")
	if rec.Code != http.StatusGone {
		t.Fatalf("status = %d, want 410", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "SESSION_EXPIRED") {
		t.Errorf("body = %s, want code SESSION_EXPIRED", rec.Body.String())
	}
}

func TestSetAddress_200MovesToAddressSet(t *testing.T) {
	r := newRouter(&fakeRepo{byID: liveSession("7", domain.StatusOpen)}, &fakeCart{}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodPut, "/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/address",
		`{"full_name":"Alice","line1":"1 Main St","city":"HN","country":"VN"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"address_set"`) {
		t.Errorf("body = %s, want address_set", rec.Body.String())
	}
}

func TestSetAddress_MissingFieldsIs400(t *testing.T) {
	r := newRouter(&fakeRepo{byID: liveSession("7", domain.StatusOpen)}, &fakeCart{}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodPut, "/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/address",
		`{"line1":"1 Main St"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestSetAddress_OnCompletedIs409InvalidTransition(t *testing.T) {
	r := newRouter(&fakeRepo{byID: liveSession("7", domain.StatusCompleted)}, &fakeCart{}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodPut, "/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/address",
		`{"full_name":"Alice","line1":"1 Main St","city":"HN","country":"VN"}`)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "INVALID_TRANSITION") {
		t.Errorf("got (%d, %s), want 409 INVALID_TRANSITION", rec.Code, rec.Body.String())
	}
}

func TestCancelSession_200(t *testing.T) {
	r := newRouter(&fakeRepo{byID: liveSession("7", domain.StatusShippingSet)}, &fakeCart{}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodDelete, "/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111", "")
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestRemovePromo_200Detaches(t *testing.T) {
	r := newRouter(&fakeRepo{byID: liveSession("7", domain.StatusShippingSet)}, &fakeCart{}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodDelete, "/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/promo", "")
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

// applyPromo drives POST …/promo against a session that is ready for one.
func applyPromo(repo *fakeRepo, code string) *httptest.ResponseRecorder {
	repo.byID = liveSession("7", domain.StatusShippingSet)
	r := newRouter(repo, &fakeCart{}, &fakeProducts{}, "7")
	return doJSON(r,
		http.MethodPost, "/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/promo",
		`{"code":"`+code+`"}`)
}

// A spent cap is a refusal the shopper can act on, so it must reach them as
// 409 PROMO_EXHAUSTED — the same answer the confirm gate has always given for
// the same condition. It used to fall through respondSessionError's default and
// answer 500, which told the shopper the service was broken and told the SPA
// not to bother explaining. Both cap branches are covered because they are
// separate checks over different sources: the global count comes back on the
// promo row, the per-user one costs an extra query.
func TestApplyPromo_ExhaustedIs409(t *testing.T) {
	maxOne, limitOne := 1, 1

	for _, tc := range []struct {
		name string
		repo *fakeRepo
	}{
		{
			name: "global cap spent",
			repo: &fakeRepo{promo: &domain.Promo{
				Code: "SCARCE", Kind: "fixed", Value: 200,
				MaxRedemptions: &maxOne, RedeemedCount: 1,
			}},
		},
		{
			name: "per-user limit reached",
			repo: &fakeRepo{
				promo: &domain.Promo{
					Code: "ONETIME", Kind: "fixed", Value: 300,
					PerUserLimit: &limitOne,
				},
				userRedemptions: 1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := applyPromo(tc.repo, tc.repo.promo.Code)
			if rec.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409 (body %s)", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "PROMO_EXHAUSTED") {
				t.Errorf("body = %s, want code PROMO_EXHAUSTED", rec.Body.String())
			}
		})
	}
}

// The 409 above must not have been bought by widening the promo arms: an
// unknown code is still a 404, not a conflict.
func TestApplyPromo_UnknownStays404(t *testing.T) {
	rec := applyPromo(&fakeRepo{}, "NO-SUCH-CODE")
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "PROMO_INVALID") {
		t.Errorf("got (%d, %s), want 404 PROMO_INVALID", rec.Code, rec.Body.String())
	}
}

func TestRoutes_Unauthenticated401(t *testing.T) {
	r := newRouter(&fakeRepo{}, &fakeCart{}, &fakeProducts{}, "")
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/checkout/v1/private/checkout/sessions"},
		{http.MethodGet, "/checkout/v1/private/checkout/sessions/x"},
		{http.MethodPut, "/checkout/v1/private/checkout/sessions/x/address"},
		{http.MethodDelete, "/checkout/v1/private/checkout/sessions/x"},
	} {
		if rec := doJSON(r, tc.method, tc.path, "{}"); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s = %d, want 401", tc.method, tc.path, rec.Code)
		}
	}
}

// --- PUT shipping / PUT payment (P2) ---

func TestSetShipping_200MovesToShippingSet(t *testing.T) {
	repo := &fakeRepo{byID: liveSession("7", domain.StatusAddressSet)}
	r := newRouter(repo, &fakeCart{}, &fakeProducts{}, "7")

	rec := doJSON(r, http.MethodPut,
		"/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/shipping",
		`{"shipping_method":"standard"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var body sessionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid body: %v", err)
	}
	if body.Status != "shipping_set" || body.ShippingMethod != "standard" || body.ShippingFee != 0 {
		t.Errorf("body = %+v, want shipping_set/standard/fee 0 (P2 stub)", body)
	}
}

func TestSetShipping_MissingMethodIs400(t *testing.T) {
	repo := &fakeRepo{byID: liveSession("7", domain.StatusAddressSet)}
	r := newRouter(repo, &fakeCart{}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodPut,
		"/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/shipping", `{}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestSetShipping_BeforeAddressIs409(t *testing.T) {
	repo := &fakeRepo{byID: liveSession("7", domain.StatusOpen)}
	r := newRouter(repo, &fakeCart{}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodPut,
		"/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/shipping",
		`{"shipping_method":"standard"}`)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "INVALID_TRANSITION") {
		t.Fatalf("resp = %d %s, want 409 INVALID_TRANSITION", rec.Code, rec.Body.String())
	}
}

func TestSetPayment_200MovesToReady(t *testing.T) {
	repo := &fakeRepo{byID: liveSession("7", domain.StatusShippingSet)}
	r := newRouter(repo, &fakeCart{}, &fakeProducts{}, "7")

	rec := doJSON(r, http.MethodPut,
		"/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/payment",
		`{"payment_method_token":"tok_visa_ok"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "tok_visa_ok") {
		t.Error("payment token echoed in the response body — must never serialize outward")
	}
	var body sessionResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Status != "ready" {
		t.Errorf("status = %s, want ready", body.Status)
	}
}

func TestSetPayment_PANLikeIs400AndNeverPersisted(t *testing.T) {
	repo := &fakeRepo{byID: liveSession("7", domain.StatusShippingSet)}
	r := newRouter(repo, &fakeCart{}, &fakeProducts{}, "7")

	rec := doJSON(r, http.MethodPut,
		"/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/payment",
		`{"payment_method_token":"tok_4111111111111111"}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "VALIDATION_ERROR") {
		t.Fatalf("resp = %d %s, want 400 VALIDATION_ERROR", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "4111111111111111") {
		t.Error("PAN-shaped input echoed in the error body")
	}
	if repo.persistedToken != "" {
		t.Error("PAN-shaped token reached the repository")
	}
}

// --- POST confirm (P2) ---

// confirmRouterUnknownSKU is confirmRouter with inventory reporting the basket's
// SKU as untracked.
func confirmRouterUnknownSKU(repo *fakeRepo, idem logicv1.IdemStore, orders logicv1.OrderCreator, userID string) *gin.Engine {
	r := gin.New()
	prods := &fakeProducts{
		infos:   []logicv1.ProductInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5}},
		unknown: []string{"1"},
	}
	svc := logicv1.NewCheckoutService(repo, &fakeCart{}, prods, prods, time.Minute).WithConfirm(idem, orders)
	RegisterRoutes(r, NewHandler(svc), func(c *gin.Context) {
		c.Set(authmw.CtxUserID, userID)
		c.Next()
	})
	return r
}

// confirmRouter wires the real logic layer with confirm deps faked at the
// port level (idempotency + order), mirroring how newRouter drives everything
// through the genuine service.
func confirmRouter(repo *fakeRepo, idem logicv1.IdemStore, orders logicv1.OrderCreator, userID string) *gin.Engine {
	r := gin.New()
	prods := &fakeProducts{infos: []logicv1.ProductInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5}}}
	svc := logicv1.NewCheckoutService(repo, &fakeCart{}, prods, prods, time.Minute).WithConfirm(idem, orders)
	RegisterRoutes(r, NewHandler(svc), func(c *gin.Context) {
		c.Set(authmw.CtxUserID, userID)
		c.Next()
	})
	return r
}

type webIdem struct {
	rec      *idempotency.Record
	claimErr error
}

func (f *webIdem) Claim(_ context.Context, _, _, _, _, _ string) (*idempotency.Record, bool, error) {
	if f.claimErr != nil {
		return nil, false, f.claimErr
	}
	return f.rec, true, nil
}
func (f *webIdem) Checkpoint(_ context.Context, _ int64, subjectID *int64) error {
	v := *subjectID
	f.rec.SubjectID = &v
	return nil
}
func (f *webIdem) Release(_ context.Context, _ int64) error                 { return nil }
func (f *webIdem) Finish(_ context.Context, _ int64, _ int, _ []byte) error { return nil }

type webOrders struct{ err error }

func (f *webOrders) CreateOrder(_ context.Context, _ string, _ []domain.SessionItem, _, _ string, _, _, _ int64) (string, string, error) {
	if f.err != nil {
		return "", "", f.err
	}
	return "42", "pending", nil
}

func readyWebSession(userID string) *domain.Session {
	s := liveSession(userID, domain.StatusReady)
	s.Items = []domain.SessionItem{{ProductID: "1", ProductName: "Mouse", Quantity: 1, UnitPriceMinor: 2999, CartPriceMinor: 2999}}
	s.SubtotalMinor, s.TotalMinor = 2999, 2999
	s.PaymentMethodToken = "tok_visa_ok"
	return s
}

func doConfirm(r *gin.Engine, key string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/confirm", nil)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	r.ServeHTTP(rec, req)
	return rec
}

func TestConfirm_201WithOrderID(t *testing.T) {
	repo := &fakeRepo{byID: readyWebSession("7")}
	r := confirmRouter(repo, &webIdem{rec: &idempotency.Record{ID: 11}}, &webOrders{}, "7")

	rec := doConfirm(r, "key-1")
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var body sessionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid body: %v", err)
	}
	if body.Status != "completed" || body.OrderID != "42" {
		t.Errorf("body = %+v, want completed with order 42", body)
	}
	if strings.Contains(rec.Body.String(), "tok_visa_ok") {
		t.Error("payment token leaked into the confirm response")
	}
}

func TestConfirm_MissingKeyIs400IdempotencyKeyRequired(t *testing.T) {
	repo := &fakeRepo{byID: readyWebSession("7")}
	r := confirmRouter(repo, &webIdem{rec: &idempotency.Record{ID: 11}}, &webOrders{}, "7")

	rec := doConfirm(r, "")
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "IDEMPOTENCY_KEY_REQUIRED") {
		t.Fatalf("resp = %d %s, want 400 IDEMPOTENCY_KEY_REQUIRED", rec.Code, rec.Body.String())
	}
}

func TestConfirm_OversizedKeyIs400(t *testing.T) {
	repo := &fakeRepo{byID: readyWebSession("7")}
	r := confirmRouter(repo, &webIdem{rec: &idempotency.Record{ID: 11}}, &webOrders{}, "7")

	if rec := doConfirm(r, strings.Repeat("k", 121)); rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (key cap keeps the composed order key under 200)", rec.Code)
	}
}

func TestConfirm_UpstreamDownIs503WithRetryAfter(t *testing.T) {
	repo := &fakeRepo{byID: readyWebSession("7")}
	r := confirmRouter(repo, &webIdem{rec: &idempotency.Record{ID: 11}}, &webOrders{err: errors.New("dial refused")}, "7")

	rec := doConfirm(r, "key-1")
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("resp = %d retry-after=%q, want 503 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
}

// The confirm gate's requote answers carry the refreshed session ALONGSIDE the
// error, so the SPA can show what changed instead of stranding the shopper on a
// dead confirm screen. Only the 503 data-gap arm below was pinned; the four 409
// arms each built that envelope by hand, so nothing would have caught one of
// them losing the session. This covers the 409 shape.
func TestConfirm_PriceChangedIs409WithRequotedSession(t *testing.T) {
	repo := &fakeRepo{byID: readyWebSession("7")}
	r := gin.New()
	// The catalog now says 3999 while the session snapshot holds 2999, so the
	// confirm gate requotes instead of charging the stale price.
	prods := &fakeProducts{infos: []logicv1.ProductInfo{
		{ProductID: "1", Name: "Mouse", UnitPriceMinor: 3999, AvailableQty: 5},
	}}
	svc := logicv1.NewCheckoutService(repo, &fakeCart{}, prods, prods, time.Minute).
		WithConfirm(&webIdem{rec: &idempotency.Record{ID: 12}}, &webOrders{})
	RegisterRoutes(r, NewHandler(svc), func(c *gin.Context) {
		c.Set(authmw.CtxUserID, "7")
		c.Next()
	})

	rec := doConfirm(r, "key-price")
	if rec.Code != http.StatusConflict {
		t.Fatalf("resp = %d, want 409 — body %s", rec.Code, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v — body %s", err, rec.Body.String())
	}
	errObj, ok := body["error"].(map[string]any)
	if !ok || errObj["code"] != httpx.CodePriceChanged {
		t.Errorf("error = %v, want code %q", body["error"], httpx.CodePriceChanged)
	}
	if _, ok := body["session"]; !ok {
		t.Fatalf("no session in the requote envelope: %s", rec.Body.String())
	}
}

// An untracked SKU is 503, not 409 STOCK_UNAVAILABLE: nothing about the basket is
// wrong, inventory simply cannot answer for it. The response carries the REQUOTED
// session, because the alternative is a shopper stranded on a dead confirm screen
// -- `confirming` has no FSM exit.
func TestConfirm_UnknownSKUIs503WithRequotedSession(t *testing.T) {
	repo := &fakeRepo{byID: readyWebSession("7")}
	r := confirmRouterUnknownSKU(repo, &webIdem{rec: &idempotency.Record{ID: 11}}, &webOrders{}, "7")

	rec := doConfirm(r, "key-1")
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("resp = %d retry-after=%q, want 503 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
	body := rec.Body.String()
	if !strings.Contains(body, "shipping_set") {
		t.Errorf("body = %s, want the requoted session so the SPA can recover", body)
	}
	if strings.Contains(body, "no longer available") || strings.Contains(body, "STOCK_UNAVAILABLE") {
		t.Errorf("body claims the item is gone on a data gap: %s", body)
	}
}

func TestCreateSession_UnknownSKUIs503AndOpaque(t *testing.T) {
	prods := &fakeProducts{
		infos:   []logicv1.ProductInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5}},
		unknown: []string{"1"},
	}
	cart := &fakeCart{lines: []logicv1.CartLine{{ProductID: "1", Quantity: 1}}}
	r := newRouter(&fakeRepo{}, cart, prods, "7")

	rec := doJSON(r, http.MethodPost, "/checkout/v1/private/checkout/sessions", "")
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("resp = %d retry-after=%q, want 503 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
	// The SKU ids belong in the log and the span, never in the body.
	if strings.Contains(rec.Body.String(), "does not track") {
		t.Errorf("body leaks the internal reason: %s", rec.Body.String())
	}
}

func TestConfirm_NotReadyIs409InvalidTransition(t *testing.T) {
	s := readyWebSession("7")
	s.Status = domain.StatusOpen
	r := confirmRouter(&fakeRepo{byID: s}, &webIdem{rec: &idempotency.Record{ID: 11}}, &webOrders{}, "7")

	rec := doConfirm(r, "key-1")
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "INVALID_TRANSITION") {
		t.Fatalf("resp = %d %s, want 409 INVALID_TRANSITION", rec.Code, rec.Body.String())
	}
}

func TestSetAddress_CanonicalizesCountry(t *testing.T) {
	repo := &fakeRepo{byID: liveSession("7", domain.StatusOpen)}
	r := newRouter(repo, &fakeCart{}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodPut,
		"/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111/address",
		`{"full_name":"A","line1":"1","city":"NYC","country":"us"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var body sessionResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Address == nil || body.Address.Country != "US" {
		t.Errorf("country = %+v, want canonical US (it picks the money buckets)", body.Address)
	}
}

// --- datastore unavailability must be 503 + Retry-After, never a bare 500 ---

// The bug this guards: respondSessionError had no arm for the datastore, so a
// CNPG failover answered 500 with no Retry-After on every session-scoped
// endpoint. 500 tells the SPA "do not retry" and pages on-call with "checkout is
// broken" during a routine switchover.
//
// These assert the STATUS CODE at the transport boundary on purpose. The
// previous instance of this bug (create answering 500 for a down dependency)
// survived a full unit suite because every test asserted the logic-layer error
// instead, and only e2e caught it.
func TestSessionEndpointsAnswer503WhenDatastoreUnavailable(t *testing.T) {
	// The shape the repository actually produces: the sentinel wrapping the
	// real cause, not the bare sentinel.
	unavailable := fmt.Errorf("%w: %w", domain.ErrUnavailable, context.DeadlineExceeded)
	const id = "11111111-1111-1111-1111-111111111111"

	endpoints := []struct {
		name, method, path, body string
	}{
		{"get", http.MethodGet, "/checkout/v1/private/checkout/sessions/" + id, ""},
		{"address", http.MethodPut, "/checkout/v1/private/checkout/sessions/" + id + "/address",
			`{"full_name":"A","line1":"1","city":"HN","country":"VN"}`},
		{"shipping", http.MethodPut, "/checkout/v1/private/checkout/sessions/" + id + "/shipping",
			`{"shipping_method":"standard"}`},
		{"payment", http.MethodPut, "/checkout/v1/private/checkout/sessions/" + id + "/payment",
			`{"payment_method_token":"tok_visa"}`},
		{"apply promo", http.MethodPost, "/checkout/v1/private/checkout/sessions/" + id + "/promo",
			`{"code":"SAVE10"}`},
		{"remove promo", http.MethodDelete, "/checkout/v1/private/checkout/sessions/" + id + "/promo", ""},
		{"cancel", http.MethodDelete, "/checkout/v1/private/checkout/sessions/" + id, ""},
		// Confirm is covered separately: it needs an Idempotency-Key before it
		// reaches the repository at all, so it cannot share this table.
	}

	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			repo := &fakeRepo{findErr: unavailable}
			rec := doJSON(newRouter(repo, &fakeCart{}, &fakeProducts{}, "user-1"),
				ep.method, ep.path, ep.body)

			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("status = %d, want 503", rec.Code)
			}
			if got := rec.Header().Get("Retry-After"); got != "2" {
				t.Errorf("Retry-After = %q, want %q — without it the SPA has no wait to honour", got, "2")
			}
		})
	}
}

func TestCreateSessionAnswers503WhenDatastoreUnavailable(t *testing.T) {
	repo := &fakeRepo{createErr: fmt.Errorf("%w: %w", domain.ErrUnavailable, context.DeadlineExceeded)}
	cart := &fakeCart{lines: []logicv1.CartLine{
		{ProductID: "1", ProductName: "Mouse", Quantity: 1, CartPriceMinor: 2999},
	}}
	prods := &fakeProducts{infos: []logicv1.ProductInfo{
		{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5},
	}}
	rec := doJSON(newRouter(repo, cart, prods, "user-1"),
		http.MethodPost, "/checkout/v1/private/checkout/sessions", "")

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Errorf("Retry-After = %q, want %q", got, "2")
	}
}

// The negative control. Without it the test above passes even if the new arm
// were widened to catch every error, which would bury real bugs behind a
// "retry" the shopper follows forever.
func TestUnclassifiedErrorStays500(t *testing.T) {
	repo := &fakeRepo{findErr: errors.New("marshal address: unexpected type")}
	rec := doJSON(newRouter(repo, &fakeCart{}, &fakeProducts{}, "user-1"),
		http.MethodGet, "/checkout/v1/private/checkout/sessions/11111111-1111-1111-1111-111111111111", "")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: a genuine bug must not be advertised as retryable", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Errorf("Retry-After = %q, want empty on a 500", got)
	}
}

// Confirm reaches respondSessionError through its own switch's default arm, so
// it needs its own case — and it is the most important one: a shopper stranded
// on a 500 confirm screen does not know whether their order exists.
func TestConfirmAnswers503WhenDatastoreUnavailable(t *testing.T) {
	repo := &fakeRepo{findErr: fmt.Errorf("%w: %w", domain.ErrUnavailable, context.DeadlineExceeded)}
	r := confirmRouter(repo, &webIdem{rec: &idempotency.Record{ID: 11}}, &webOrders{}, "7")

	rec := doConfirm(r, "key-1")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Errorf("Retry-After = %q, want %q", got, "2")
	}
}

// errUnavailableForTest is the exact shape the repository produces.
func errUnavailableForTest() error {
	return fmt.Errorf("%w: %w", domain.ErrUnavailable, context.DeadlineExceeded)
}

// The review finding these pin down: three confirm-flow sites mapped ANY error
// from a repository call onto ErrConfirmInFlight, so a failover striking after
// the initial session load answered 409 "A confirm is already in flight" — a
// 4xx, which does not even burn error budget, while telling the shopper a
// confirm exists that does not. Each subtest injects unavailability at a
// different depth of the flow; all must surface as 503.
func TestConfirmMidFlowUnavailabilityIs503Not409(t *testing.T) {
	tests := []struct {
		name string
		repo *fakeRepo
		idem *webIdem
	}{
		{
			// Site 1: the entry-gate CAS (ready -> confirming).
			name: "BeginConfirm",
			repo: &fakeRepo{byID: readyWebSession("7"), beginErr: errUnavailableForTest()},
			idem: &webIdem{rec: &idempotency.Record{ID: 11}},
		},
		{
			// Site 2: the post-gate TOCTOU re-read (second FindByID).
			name: "post-gate re-read",
			repo: &fakeRepo{byID: readyWebSession("7"), findErrOnCall: 2},
			idem: &webIdem{rec: &idempotency.Record{ID: 11}},
		},
		{
			// Site 3: the stale-CAS recovery re-read inside completeConfirm.
			name: "completeConfirm re-read",
			repo: &fakeRepo{byID: readyWebSession("7"), completeErr: domain.ErrStaleTransition, findErrOnCall: 3},
			idem: &webIdem{rec: &idempotency.Record{ID: 11}},
		},
		{
			// Site 4: the idempotency store runs on the SAME pool but sits
			// outside the repository package, so its errors must cross the
			// classifier via the wiring adapter. Claim is one of the first DB
			// writes of a confirm — during a real switchover this is the arm
			// that fires most often.
			name: "idempotency Claim",
			repo: &fakeRepo{byID: readyWebSession("7")},
			idem: &webIdem{claimErr: errUnavailableForTest()},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := confirmRouter(tc.repo, tc.idem, &webOrders{}, "7")
			rec := doConfirm(r, "key-1")

			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("status = %d, want 503: %s", rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("Retry-After"); got != "2" {
				t.Errorf("Retry-After = %q, want %q", got, "2")
			}
		})
	}
}

// The 409 mapping must survive for what it is FOR: a genuinely concurrent
// confirm. Without this control, the fix above could be "widened" to
// propagate every error and nobody would notice the 409 contract breaking.
func TestConcurrentConfirmStays409(t *testing.T) {
	// A session already confirming under a DIFFERENT key.
	s := readyWebSession("7")
	s.Status = domain.StatusConfirming
	other := int64(99)
	s.ConfirmKeyID = &other
	repo := &fakeRepo{byID: s}
	r := confirmRouter(repo, &webIdem{rec: &idempotency.Record{ID: 11}}, &webOrders{}, "7")

	rec := doConfirm(r, "key-1")
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
}

// Every arm of the confirm requote answers 409 with the SAME envelope shape —
// an `error` object carrying the arm's code, and the requoted `session` beside
// it. The SPA reads both on every arm, so a fifth arm added without the session
// would strand a shopper on a dead confirm screen. The price-changed arm has
// its own test above; this covers the other three, which respondRequote made
// share one call site.
func TestConfirm_RequoteArmsShareOneEnvelope(t *testing.T) {
	cases := []struct {
		name     string
		wantCode string
		arrange  func(*fakeRepo) *fakeProducts
	}{
		{
			// Same price, no stock: revalidation requotes and reports the
			// shortage rather than charging for something undeliverable.
			name:     "stock ran out",
			wantCode: httpx.CodeStockUnavailable,
			arrange: func(_ *fakeRepo) *fakeProducts {
				return &fakeProducts{infos: []logicv1.ProductInfo{
					{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 0},
				}}
			},
		},
		{
			name:     "promo spent between apply and confirm",
			wantCode: httpx.CodePromoExhausted,
			arrange: func(r *fakeRepo) *fakeProducts {
				r.byID.PromoCode = "WELCOME10"
				r.redeemErr = domain.ErrPromoExhausted
				return &fakeProducts{infos: []logicv1.ProductInfo{
					{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5},
				}}
			},
		},
		{
			name:     "promo expired between apply and confirm",
			wantCode: httpx.CodePromoExpired,
			arrange: func(r *fakeRepo) *fakeProducts {
				r.byID.PromoCode = "WELCOME10"
				r.redeemErr = domain.ErrPromoExpired
				return &fakeProducts{infos: []logicv1.ProductInfo{
					{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5},
				}}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepo{byID: readyWebSession("7")}
			prods := tc.arrange(repo)
			r := gin.New()
			svc := logicv1.NewCheckoutService(repo, &fakeCart{}, prods, prods, time.Minute).
				WithConfirm(&webIdem{rec: &idempotency.Record{ID: 12}}, &webOrders{})
			RegisterRoutes(r, NewHandler(svc), func(c *gin.Context) {
				c.Set(authmw.CtxUserID, "7")
				c.Next()
			})

			rec := doConfirm(r, "key-"+tc.wantCode)
			if rec.Code != http.StatusConflict {
				t.Fatalf("resp = %d, want 409 — body %s", rec.Code, rec.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v — body %s", err, rec.Body.String())
			}
			errObj, ok := body["error"].(map[string]any)
			if !ok || errObj["code"] != tc.wantCode {
				t.Errorf("error = %v, want code %q", body["error"], tc.wantCode)
			}
			if _, ok := body["session"]; !ok {
				t.Errorf("no session in the requote envelope: %s", rec.Body.String())
			}
		})
	}
}
