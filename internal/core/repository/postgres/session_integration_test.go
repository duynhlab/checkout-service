//go:build integration

// Integration tests for the PostgreSQL SessionRepository. They run a real
// Postgres via testcontainers-go and apply the service's migrations, so they
// exercise the actual SQL — the partial unique index, the conditional
// transitions, and the jsonb address round-trip. Run with:
//
//	go test -tags=integration ./internal/core/repository/...
//
// Requires a reachable Docker daemon; excluded from the default unit run by
// the `integration` build tag.
package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/duynhlab/pkg/idempotency"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// newTestDB starts a throwaway Postgres, applies the migrations, and returns
// a pool for the repository under test. Torn down via t.Cleanup.
func newTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("checkout"),
		postgres.WithUsername("checkout"),
		postgres.WithPassword("secret"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	applyMigrations(t, ctx, dsn)

	// Mirror production's pool settings (internal/core/database.go): the
	// simple query protocol changes how parameters are encoded (this is what
	// caught the jsonb-as-bytea bug), so the tests must run the same way.
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	poolCfg.ConnConfig.StatementCacheCapacity = 0
	poolCfg.ConnConfig.DescriptionCacheCapacity = 0
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// applyMigrations runs every db/migrations/sql/*.up.sql in lexical order over
// a simple-protocol connection (multi-statement files in one round).
func applyMigrations(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect for migrations: %v", err)
	}
	defer conn.Close(ctx)

	dir := filepath.Join("..", "..", "..", "..", "db", "migrations", "sql")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	for _, f := range files {
		sqlBytes, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if _, err := conn.Exec(ctx, string(sqlBytes)); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}
}

func newSession(userID string) *domain.Session {
	return &domain.Session{
		UserID: userID, Status: domain.StatusOpen,
		Items: []domain.SessionItem{
			{ProductID: "1", ProductName: "Mouse", Quantity: 2, UnitPriceMinor: 2999, CartPriceMinor: 2999},
			{ProductID: "2", ProductName: "Hub", Quantity: 1, UnitPriceMinor: 3999, CartPriceMinor: 3499, PriceChanged: true},
		},
		SubtotalMinor: 9997, TotalMinor: 9997, Currency: "USD",
		ExpiresAt: time.Now().Add(30 * time.Minute),
	}
}

func TestSessionRepository_CreateAndFindRoundTrip(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	s := newSession("7")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if s.ID == "" {
		t.Fatal("Create did not assign an id")
	}

	got, err := repo.FindByID(ctx, s.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got.UserID != "7" || got.Status != domain.StatusOpen || len(got.Items) != 2 {
		t.Errorf("round-trip = %+v, want user 7, open, 2 items", got)
	}
	if !got.Items[1].PriceChanged || got.Items[1].UnitPriceMinor != 3999 || got.Items[1].CartPriceMinor != 3499 {
		t.Errorf("item[1] = %+v, want flagged 3999/3499", got.Items[1])
	}

	active, err := repo.FindActiveByUserID(ctx, "7")
	if err != nil || active.ID != s.ID {
		t.Errorf("FindActiveByUserID = (%v, %v), want the created session", active, err)
	}
}

func TestSessionRepository_OneActiveSessionPerUser(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	if err := repo.Create(ctx, newSession("7")); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	err := repo.Create(ctx, newSession("7"))
	if !errors.Is(err, domain.ErrActiveSessionExists) {
		t.Fatalf("second Create err = %v, want ErrActiveSessionExists (partial unique index)", err)
	}
	// A different user is unaffected.
	if err := repo.Create(ctx, newSession("8")); err != nil {
		t.Errorf("other user's Create: %v", err)
	}
}

func TestSessionRepository_ConditionalTransitions(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	s := newSession("7")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("Create: %v", err)
	}

	addr := &domain.Address{FullName: "Alice", Line1: "1 Main St", City: "HN", Country: "VN"}
	if err := repo.SetAddress(ctx, s.ID, domain.StatusOpen, addr); err != nil {
		t.Fatalf("SetAddress: %v", err)
	}
	// Stale `from` loses the optimistic-concurrency check.
	if err := repo.UpdateStatus(ctx, s.ID, domain.StatusOpen, domain.StatusCancelled); !errors.Is(err, domain.ErrStaleTransition) {
		t.Fatalf("stale UpdateStatus err = %v, want ErrStaleTransition", err)
	}
	if err := repo.UpdateStatus(ctx, s.ID, domain.StatusAddressSet, domain.StatusCancelled); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	got, err := repo.FindByID(ctx, s.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got.Status != domain.StatusCancelled || got.Address == nil || got.Address.FullName != "Alice" {
		t.Errorf("session = %+v, want cancelled with the jsonb address intact", got)
	}
	// Cancelled ⇒ no longer the active session.
	if _, err := repo.FindActiveByUserID(ctx, "7"); !errors.Is(err, domain.ErrSessionNotFound) {
		t.Errorf("FindActive after cancel err = %v, want ErrSessionNotFound", err)
	}
}

func TestSessionRepository_MarkExpired(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	s := newSession("7")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := repo.MarkExpired(ctx, s.ID, domain.ExpiredByLazy); err != nil {
		t.Fatalf("MarkExpired: %v", err)
	}
	got, _ := repo.FindByID(ctx, s.ID)
	if got.Status != domain.StatusExpired || got.ExpiredReason == nil || *got.ExpiredReason != domain.ExpiredByLazy {
		t.Errorf("session = %+v, want expired(lazy)", got)
	}
	// Late timer against a terminal session: a no-op, never an error, and the
	// original reason is preserved.
	if err := repo.MarkExpired(ctx, s.ID, domain.ExpiredByTimer); err != nil {
		t.Fatalf("late MarkExpired: %v", err)
	}
	got, _ = repo.FindByID(ctx, s.ID)
	if *got.ExpiredReason != domain.ExpiredByLazy {
		t.Errorf("reason = %s, want lazy preserved", *got.ExpiredReason)
	}
}

func TestSessionRepository_GarbageIDIsNotFoundNot500(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))

	// A non-UUID id raises Postgres 22P02; the repo must answer "no such
	// session" so the web layer 404s instead of 500ing on garbage ids.
	_, err := repo.FindByID(context.Background(), "not-a-uuid")
	if !errors.Is(err, domain.ErrSessionNotFound) {
		t.Fatalf("FindByID(garbage) err = %v, want ErrSessionNotFound", err)
	}
}

func TestSessionRepository_TouchBumpsExpiryOnActiveOnly(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	s := newSession("7")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Reset-on-activity (RFC-0015 P2): every successful mutation bumps the
	// DB expiry so the lazy backstop agrees with the workflow timer.
	newExpiry := time.Now().Add(45 * time.Minute)
	if err := repo.Touch(ctx, s.ID, newExpiry); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	got, err := repo.FindByID(ctx, s.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got.ExpiresAt.Before(newExpiry.Add(-2 * time.Second)) {
		t.Errorf("expires_at = %v, want bumped to ~%v", got.ExpiresAt, newExpiry)
	}

	// A terminal session is never touched back to life.
	if err := repo.MarkExpired(ctx, s.ID, domain.ExpiredByLazy); err != nil {
		t.Fatalf("MarkExpired: %v", err)
	}
	late := time.Now().Add(2 * time.Hour)
	if err := repo.Touch(ctx, s.ID, late); err != nil {
		t.Fatalf("Touch on terminal must be a harmless no-op, got: %v", err)
	}
	got, _ = repo.FindByID(ctx, s.ID)
	if got.Status != domain.StatusExpired || got.ExpiresAt.After(time.Now().Add(90*time.Minute)) {
		t.Errorf("terminal session mutated by Touch: %+v", got)
	}
}

// TestIdempotencyKeysMigrationWorksWithPkg proves migration 000002 satisfies
// pkg/idempotency (ADR-010 consumer #2) under THIS service's pool settings —
// the simple query protocol is exactly what turned response_body jsonb writes
// into bytea errors in P1, so the full Claim→Checkpoint→Finish→replay cycle
// must round-trip here, not just in pkg's own tests.
func TestIdempotencyKeysMigrationWorksWithPkg(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()
	repo := idempotency.New(pool, time.Minute)

	rec, fresh, err := repo.Claim(ctx, 7, "checkout:sess-1:key-1", "POST", "/confirm", "h1")
	if err != nil || !fresh {
		t.Fatalf("Claim fresh = (%v, %v), want fresh claim", fresh, err)
	}
	subject := int64(42)
	if err := repo.Checkpoint(ctx, rec.ID, &subject); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := repo.Finish(ctx, rec.ID, 201, []byte(`{"order_id":"42"}`)); err != nil {
		t.Fatalf("Finish (jsonb under simple protocol): %v", err)
	}

	replay, fresh, err := repo.Claim(ctx, 7, "checkout:sess-1:key-1", "POST", "/confirm", "h1")
	if err != nil || fresh {
		t.Fatalf("Claim replay = (%v, %v), want existing record", fresh, err)
	}
	if !replay.Finished() || *replay.ResponseCode != 201 || *replay.SubjectID != 42 {
		t.Errorf("replay = %+v, want finished 201 subject 42", replay)
	}
}

func TestSessionRepository_ShippingAndPaymentWrites(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	s := newSession("7")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := repo.SetAddress(ctx, s.ID, domain.StatusOpen, &domain.Address{FullName: "A", Line1: "1", City: "HN", Country: "VN"}); err != nil {
		t.Fatalf("SetAddress: %v", err)
	}

	// Shipping write recomputes total in SQL from the persisted components.
	if err := repo.SetShipping(ctx, s.ID, domain.StatusAddressSet, "standard", 0); err != nil {
		t.Fatalf("SetShipping: %v", err)
	}
	got, _ := repo.FindByID(ctx, s.ID)
	if got.Status != domain.StatusShippingSet || got.ShippingMethod != "standard" || got.ShippingFeeMinor != 0 {
		t.Errorf("after shipping = %+v, want shipping_set/standard/0", got)
	}
	if got.TotalMinor != got.SubtotalMinor {
		t.Errorf("total = %d, want subtotal %d (fee 0 stub)", got.TotalMinor, got.SubtotalMinor)
	}

	// Stale `from` is optimistic-concurrency rejected.
	if err := repo.SetPaymentToken(ctx, s.ID, domain.StatusAddressSet, "tok_visa_ok"); !errors.Is(err, domain.ErrStaleTransition) {
		t.Fatalf("stale SetPaymentToken err = %v, want ErrStaleTransition", err)
	}
	if err := repo.SetPaymentToken(ctx, s.ID, domain.StatusShippingSet, "tok_visa_ok"); err != nil {
		t.Fatalf("SetPaymentToken: %v", err)
	}
	got, _ = repo.FindByID(ctx, s.ID)
	if got.Status != domain.StatusReady || got.PaymentMethodToken != "tok_visa_ok" {
		t.Errorf("after payment = %+v, want ready with token", got)
	}
}

func TestSessionRepository_ConfirmBindingLifecycle(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	s := newSession("7")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("Create: %v", err)
	}
	step := func(name string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	step("address", repo.SetAddress(ctx, s.ID, domain.StatusOpen, &domain.Address{FullName: "A", Line1: "1", City: "HN", Country: "VN"}))
	step("shipping", repo.SetShipping(ctx, s.ID, domain.StatusAddressSet, "standard", 0))
	step("payment", repo.SetPaymentToken(ctx, s.ID, domain.StatusShippingSet, "tok_visa_ok"))

	// BeginConfirm binds claim 11; re-entry with the same claim is idempotent;
	// a different claim is fenced out.
	step("begin", repo.BeginConfirm(ctx, s.ID, 11))
	if err := repo.BeginConfirm(ctx, s.ID, 11); !errors.Is(err, domain.ErrStaleTransition) {
		// status is confirming (not ready) — same claim re-entry is handled in
		// logic by reading the binding, not by re-running the CAS.
		t.Fatalf("second BeginConfirm err = %v, want ErrStaleTransition (status moved)", err)
	}
	got, _ := repo.FindByID(ctx, s.ID)
	if got.Status != domain.StatusConfirming || got.ConfirmKeyID == nil || *got.ConfirmKeyID != 11 {
		t.Fatalf("after begin = %+v, want confirming bound to 11", got)
	}

	// A foreign claim cannot requote or complete.
	if err := repo.RequoteItems(ctx, s.ID, 99, got.Items, 1, 1); !errors.Is(err, domain.ErrStaleTransition) {
		t.Fatalf("foreign requote err = %v, want ErrStaleTransition", err)
	}
	if err := repo.CompleteSession(ctx, s.ID, 99, "42"); !errors.Is(err, domain.ErrStaleTransition) {
		t.Fatalf("foreign complete err = %v, want ErrStaleTransition", err)
	}

	// The bound claim completes; the binding stays as recovery proof.
	step("complete", repo.CompleteSession(ctx, s.ID, 11, "42"))
	got, _ = repo.FindByID(ctx, s.ID)
	if got.Status != domain.StatusCompleted || got.OrderID != "42" || got.ConfirmKeyID == nil || *got.ConfirmKeyID != 11 {
		t.Errorf("completed = %+v, want completed order 42 binding kept", got)
	}
}

func TestSessionRepository_RequoteResetsPricesAndClearsBinding(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	s := newSession("7")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := repo.SetAddress(ctx, s.ID, domain.StatusOpen, &domain.Address{FullName: "A", Line1: "1", City: "HN", Country: "VN"}); err != nil {
		t.Fatalf("SetAddress: %v", err)
	}
	if err := repo.SetShipping(ctx, s.ID, domain.StatusAddressSet, "standard", 0); err != nil {
		t.Fatalf("SetShipping: %v", err)
	}
	if err := repo.SetPaymentToken(ctx, s.ID, domain.StatusShippingSet, "tok_visa_ok"); err != nil {
		t.Fatalf("SetPaymentToken: %v", err)
	}
	if err := repo.BeginConfirm(ctx, s.ID, 7); err != nil {
		t.Fatalf("BeginConfirm: %v", err)
	}

	fresh := []domain.SessionItem{
		{ProductID: "1", UnitPriceMinor: 3499, PriceChanged: true},
		{ProductID: "2", UnitPriceMinor: 3999, PriceChanged: false},
	}
	if err := repo.RequoteItems(ctx, s.ID, 7, fresh, 2*3499+3999, 2*3499+3999); err != nil {
		t.Fatalf("RequoteItems: %v", err)
	}
	got, _ := repo.FindByID(ctx, s.ID)
	if got.Status != domain.StatusShippingSet || got.ConfirmKeyID != nil {
		t.Errorf("after requote = %s bound=%v, want shipping_set unbound", got.Status, got.ConfirmKeyID)
	}
	if got.Items[0].UnitPriceMinor != 3499 || !got.Items[0].PriceChanged || got.Items[1].UnitPriceMinor != 3999 {
		t.Errorf("items = %+v, want fresh prices applied", got.Items)
	}
	if got.SubtotalMinor != 2*3499+3999 {
		t.Errorf("subtotal = %d, want recomputed", got.SubtotalMinor)
	}
}
