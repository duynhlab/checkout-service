package v1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/duynhlab/pkg/authmw"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	logicv1 "github.com/duynhlab/checkout-service/internal/logic/v1"
)

func init() { gin.SetMode(gin.TestMode) }

// --- logic doubles (repo/cart/product level, driving the real logic layer) ---

type fakeRepo struct {
	active    *domain.Session
	byID      *domain.Session
	createErr error
}

func (f *fakeRepo) Create(_ context.Context, s *domain.Session) error {
	if f.createErr != nil {
		return f.createErr
	}
	s.ID = "11111111-1111-1111-1111-111111111111"
	return nil
}

func (f *fakeRepo) FindByID(_ context.Context, _ string) (*domain.Session, error) {
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

func (f *fakeRepo) SetAddress(_ context.Context, _ string, _ domain.SessionStatus, _ *domain.Address) error {
	return nil
}

func (f *fakeRepo) MarkExpired(_ context.Context, _ string, _ domain.ExpiredReason) error {
	return nil
}

type fakeCart struct {
	lines []logicv1.CartLine
	err   error
}

func (f *fakeCart) GetCart(_ context.Context, _ string) ([]logicv1.CartLine, error) {
	return f.lines, f.err
}

type fakeProducts struct{ infos []logicv1.ProductInfo }

func (f *fakeProducts) GetProducts(_ context.Context, _ []string) ([]logicv1.ProductInfo, error) {
	return f.infos, nil
}

// newRouter mounts the real routes with a fake JWT middleware injecting userID
// (empty = unauthenticated 401, mirroring authmw's fail-closed behavior).
func newRouter(repo *fakeRepo, cart *fakeCart, prods *fakeProducts, userID string) *gin.Engine {
	r := gin.New()
	svc := logicv1.NewCheckoutService(repo, cart, prods, time.Minute)
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

func TestCreateSession_UpstreamDownIsOpaque500(t *testing.T) {
	r := newRouter(&fakeRepo{}, &fakeCart{err: errors.New("dial tcp 10.1.2.3: refused")}, &fakeProducts{}, "7")
	rec := doJSON(r, http.MethodPost, "/checkout/v1/private/checkout/sessions", "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
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
