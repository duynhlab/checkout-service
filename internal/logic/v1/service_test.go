package v1

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// --- doubles ---

type fakeRepo struct {
	active    *domain.Session
	activeErr error
	// activeSecond, when set, is returned from the SECOND FindActiveByUserID
	// call onward (simulating a concurrent winner appearing between the
	// pre-create lookup and the post-conflict re-fetch).
	activeSecond *domain.Session
	activeCalls  int
	byID         *domain.Session
	byIDErr      error
	createErr    error
	created      *domain.Session
	updated      []string // "from→to"
	updateErr    error
	addressed    *domain.Address
	setAddrErr   error
	expired      []domain.ExpiredReason
	markExpErr   error
	createCalls  int
}

func (f *fakeRepo) Create(_ context.Context, s *domain.Session) error {
	f.createCalls++
	if f.createErr != nil {
		return f.createErr
	}
	s.ID = "sess-1"
	f.created = s
	return nil
}

func (f *fakeRepo) FindByID(_ context.Context, _ string) (*domain.Session, error) {
	return f.byID, f.byIDErr
}

func (f *fakeRepo) FindActiveByUserID(_ context.Context, _ string) (*domain.Session, error) {
	f.activeCalls++
	if f.activeCalls > 1 && f.activeSecond != nil {
		return f.activeSecond, nil
	}
	if f.active == nil && f.activeErr == nil {
		return nil, domain.ErrSessionNotFound
	}
	return f.active, f.activeErr
}

func (f *fakeRepo) UpdateStatus(_ context.Context, _ string, from, to domain.SessionStatus) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updated = append(f.updated, string(from)+"→"+string(to))
	return nil
}

func (f *fakeRepo) SetAddress(_ context.Context, _ string, _ domain.SessionStatus, addr *domain.Address) error {
	if f.setAddrErr != nil {
		return f.setAddrErr
	}
	f.addressed = addr
	return nil
}

func (f *fakeRepo) MarkExpired(_ context.Context, _ string, reason domain.ExpiredReason) error {
	f.expired = append(f.expired, reason)
	return f.markExpErr
}

type fakeCart struct {
	lines []CartLine
	err   error
}

func (f *fakeCart) GetCart(_ context.Context, _ string) ([]CartLine, error) { return f.lines, f.err }

type fakeProducts struct {
	infos []ProductInfo
	err   error
}

func (f *fakeProducts) GetProducts(_ context.Context, _ []string) ([]ProductInfo, error) {
	return f.infos, f.err
}

func newSvc(repo *fakeRepo, cart *fakeCart, prods *fakeProducts) *CheckoutService {
	return NewCheckoutService(repo, cart, prods, time.Minute)
}

func liveSession(status domain.SessionStatus) *domain.Session {
	return &domain.Session{
		ID: "sess-1", UserID: "7", Status: status,
		ExpiresAt: time.Now().Add(time.Hour),
	}
}

// --- CreateSession ---

func TestCreateSession_SnapshotsWithProductPricesAndFlagsDiffs(t *testing.T) {
	repo := &fakeRepo{}
	cart := &fakeCart{lines: []CartLine{
		{ProductID: "1", ProductName: "Mouse", Quantity: 2, CartPriceMinor: 2999},
		{ProductID: "2", ProductName: "Keyboard", Quantity: 1, CartPriceMinor: 7999},
	}}
	prods := &fakeProducts{infos: []ProductInfo{
		{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5},
		{ProductID: "2", Name: "Keyboard", UnitPriceMinor: 8499, AvailableQty: 3}, // price moved
	}}

	s, created, err := newSvc(repo, cart, prods).CreateSession(context.Background(), "7")
	if err != nil || !created {
		t.Fatalf("CreateSession() = (%v, %v, %v), want created session", s, created, err)
	}
	if s.Status != domain.StatusOpen || s.Currency != "USD" {
		t.Errorf("status/currency = %s/%s, want open/USD", s.Status, s.Currency)
	}
	if s.Items[0].PriceChanged || !s.Items[1].PriceChanged {
		t.Errorf("price_changed flags = %v/%v, want false/true", s.Items[0].PriceChanged, s.Items[1].PriceChanged)
	}
	if s.Items[1].UnitPriceMinor != 8499 || s.Items[1].CartPriceMinor != 7999 {
		t.Errorf("item[1] prices = %d/%d, want product 8499 / cart 7999", s.Items[1].UnitPriceMinor, s.Items[1].CartPriceMinor)
	}
	if want := int64(2*2999 + 8499); s.SubtotalMinor != want || s.TotalMinor != want {
		t.Errorf("subtotal/total = %d/%d, want %d (product-authoritative)", s.SubtotalMinor, s.TotalMinor, want)
	}
	if s.ExpiresAt.IsZero() {
		t.Error("ExpiresAt not set")
	}
}

func TestCreateSession_ReturnsExistingActiveSession(t *testing.T) {
	repo := &fakeRepo{active: liveSession(domain.StatusAddressSet)}

	s, created, err := newSvc(repo, &fakeCart{}, &fakeProducts{}).CreateSession(context.Background(), "7")
	if err != nil || created {
		t.Fatalf("want existing session with created=false, got (%v, %v)", created, err)
	}
	if s.ID != "sess-1" || repo.createCalls != 0 {
		t.Errorf("reused = %s createCalls=%d, want sess-1 and no create", s.ID, repo.createCalls)
	}
}

func TestCreateSession_ExpiredActiveSessionIsRetiredAndReplaced(t *testing.T) {
	stale := liveSession(domain.StatusOpen)
	stale.ExpiresAt = time.Now().Add(-time.Minute)
	repo := &fakeRepo{active: stale}
	cart := &fakeCart{lines: []CartLine{{ProductID: "1", ProductName: "Mouse", Quantity: 1, CartPriceMinor: 2999}}}
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999}}}

	_, created, err := newSvc(repo, cart, prods).CreateSession(context.Background(), "7")
	if err != nil || !created {
		t.Fatalf("want a fresh session after lazy expiry, got (%v, %v)", created, err)
	}
	if len(repo.expired) != 1 || repo.expired[0] != domain.ExpiredByLazy {
		t.Errorf("expired = %v, want one lazy expiry", repo.expired)
	}
}

func TestCreateSession_EmptyCartIsErrEmptyCart(t *testing.T) {
	_, _, err := newSvc(&fakeRepo{}, &fakeCart{}, &fakeProducts{}).CreateSession(context.Background(), "7")
	if !errors.Is(err, ErrEmptyCart) {
		t.Errorf("err = %v, want ErrEmptyCart", err)
	}
}

func TestCreateSession_MissingProductKeepsLineFlagged(t *testing.T) {
	repo := &fakeRepo{}
	cart := &fakeCart{lines: []CartLine{{ProductID: "9", ProductName: "Ghost", Quantity: 1, CartPriceMinor: 500}}}

	s, _, err := newSvc(repo, cart, &fakeProducts{}).CreateSession(context.Background(), "7")
	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	if !s.Items[0].PriceChanged || s.Items[0].UnitPriceMinor != 500 {
		t.Errorf("ghost line = %+v, want flagged with cart price kept", s.Items[0])
	}
}

func TestCreateSession_UpstreamErrorsAreOpaque(t *testing.T) {
	for name, svc := range map[string]*CheckoutService{
		"cart down":    newSvc(&fakeRepo{}, &fakeCart{err: errors.New("dial tcp: refused")}, &fakeProducts{}),
		"product down": newSvc(&fakeRepo{}, &fakeCart{lines: []CartLine{{ProductID: "1", Quantity: 1}}}, &fakeProducts{err: errors.New("boom")}),
	} {
		if _, _, err := svc.CreateSession(context.Background(), "7"); !errors.Is(err, ErrUpstream) {
			t.Errorf("%s: err = %v, want ErrUpstream", name, err)
		}
	}
}

func TestCreateSession_LosingCreateRaceReturnsWinner(t *testing.T) {
	// Sequence: FindActive #1 → nothing; Create → unique-violation (a
	// concurrent request won); FindActive #2 → the winner. The caller must
	// receive the winner with created=false, never an error.
	winner := liveSession(domain.StatusOpen)
	repo := &fakeRepo{createErr: domain.ErrActiveSessionExists, activeSecond: winner}
	cart := &fakeCart{lines: []CartLine{{ProductID: "1", ProductName: "Mouse", Quantity: 1, CartPriceMinor: 100}}}
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 100}}}

	s, created, err := newSvc(repo, cart, prods).CreateSession(context.Background(), "7")
	if err != nil || created {
		t.Fatalf("want the race winner with created=false, got (created=%v, err=%v)", created, err)
	}
	if s != winner {
		t.Errorf("session = %+v, want the winner", s)
	}
	if repo.createCalls != 1 || repo.activeCalls != 2 {
		t.Errorf("calls create=%d active=%d, want 1 create and 2 active lookups", repo.createCalls, repo.activeCalls)
	}
}

// --- GetSession / ownership / lazy expiry ---

func TestGetSession_OwnerScopedAntiIDOR(t *testing.T) {
	repo := &fakeRepo{byID: liveSession(domain.StatusOpen)} // owned by user 7

	if _, err := newSvc(repo, nil, nil).GetSession(context.Background(), "8", "sess-1"); !errors.Is(err, ErrSessionNotFound) {
		t.Errorf("foreign owner err = %v, want ErrSessionNotFound (indistinguishable from missing)", err)
	}
	if _, err := newSvc(repo, nil, nil).GetSession(context.Background(), "7", "sess-1"); err != nil {
		t.Errorf("owner err = %v, want nil", err)
	}
}

func TestGetSession_LazyExpiryRecordsAndRejects(t *testing.T) {
	stale := liveSession(domain.StatusReady)
	stale.ExpiresAt = time.Now().Add(-time.Second)
	repo := &fakeRepo{byID: stale}

	_, err := newSvc(repo, nil, nil).GetSession(context.Background(), "7", "sess-1")
	if !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("err = %v, want ErrSessionExpired", err)
	}
	if len(repo.expired) != 1 || repo.expired[0] != domain.ExpiredByLazy {
		t.Errorf("expired = %v, want one lazy record", repo.expired)
	}
}

func TestGetSession_LazyExpiryAnswerDoesNotDependOnWrite(t *testing.T) {
	stale := liveSession(domain.StatusOpen)
	stale.ExpiresAt = time.Now().Add(-time.Second)
	repo := &fakeRepo{byID: stale, markExpErr: errors.New("db down")}

	if _, err := newSvc(repo, nil, nil).GetSession(context.Background(), "7", "sess-1"); !errors.Is(err, ErrSessionExpired) {
		t.Errorf("err = %v, want ErrSessionExpired even when the record write fails", err)
	}
}

// --- SetAddress ---

func TestSetAddress_MovesToAddressSet(t *testing.T) {
	repo := &fakeRepo{byID: liveSession(domain.StatusOpen)}
	addr := &domain.Address{FullName: "Alice", Line1: "1 Main St", City: "HN", Country: "VN"}

	s, err := newSvc(repo, nil, nil).SetAddress(context.Background(), "7", "sess-1", addr)
	if err != nil {
		t.Fatalf("SetAddress() error = %v", err)
	}
	if s.Status != domain.StatusAddressSet || repo.addressed != addr {
		t.Errorf("status = %s addressed=%v, want address_set persisted", s.Status, repo.addressed)
	}
}

func TestSetAddress_FromTerminalIsInvalidTransition(t *testing.T) {
	repo := &fakeRepo{byID: liveSession(domain.StatusCompleted)}

	_, err := newSvc(repo, nil, nil).SetAddress(context.Background(), "7", "sess-1", &domain.Address{})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("err = %v, want ErrInvalidTransition", err)
	}
}

// --- Cancel ---

func TestCancel_FromEachActiveState(t *testing.T) {
	for _, st := range []domain.SessionStatus{domain.StatusOpen, domain.StatusAddressSet, domain.StatusShippingSet, domain.StatusReady} {
		repo := &fakeRepo{byID: liveSession(st)}
		if err := newSvc(repo, nil, nil).Cancel(context.Background(), "7", "sess-1"); err != nil {
			t.Errorf("Cancel from %s: %v, want nil", st, err)
		}
	}
}

func TestCancel_IsIdempotentOnCancelled(t *testing.T) {
	repo := &fakeRepo{byID: liveSession(domain.StatusCancelled)}
	if err := newSvc(repo, nil, nil).Cancel(context.Background(), "7", "sess-1"); err != nil {
		t.Errorf("re-cancel err = %v, want nil (idempotent)", err)
	}
	if len(repo.updated) != 0 {
		t.Errorf("updates = %v, want none", repo.updated)
	}
}

func TestCancel_CompletedIsInvalidTransition(t *testing.T) {
	repo := &fakeRepo{byID: liveSession(domain.StatusCompleted)}
	if err := newSvc(repo, nil, nil).Cancel(context.Background(), "7", "sess-1"); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("err = %v, want ErrInvalidTransition", err)
	}
}
