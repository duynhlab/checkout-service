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
