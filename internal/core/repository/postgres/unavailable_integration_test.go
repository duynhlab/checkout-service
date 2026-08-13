//go:build integration

// These tests exist because the classifier in errors.go encodes ASSUMPTIONS
// about what a real Postgres emits when it stops being able to serve us. The
// unit tests prove the mapping is what I wrote; only these prove what I wrote
// matches what Postgres actually sends. Run with:
//
//	go test -tags=integration ./internal/core/repository/...
package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/duynhlab/pkg/idempotency"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// A demoted primary that the pooler still points at answers writes with
// read_only_sql_transaction (25006). This simulates it exactly, without needing
// a second node: the SQLSTATE the classifier keys on is identical.
func TestRepositoryReportsUnavailableOnAReadOnlyPrimary(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()
	repo := NewSessionRepository(pool)

	s := newSession("readonly-victim")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("precondition: create failed: %v", err)
	}

	var dbName string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&dbName); err != nil {
		t.Fatalf("could not read the database name: %v", err)
	}
	// ALTER DATABASE takes an identifier, not a parameter — pgx.Identifier does
	// the quoting so the test cannot become an injection example.
	ident := pgx.Identifier{dbName}.Sanitize()

	// default_transaction_read_only applies to every NEW transaction on this
	// database, including the ones the repository opens on pooled connections.
	if _, err := pool.Exec(ctx,
		`ALTER DATABASE `+ident+` SET default_transaction_read_only = on`); err != nil {
		t.Fatalf("could not force read-only: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`ALTER DATABASE `+ident+` RESET default_transaction_read_only`)
	})
	// The setting binds at connection start, so retire the pooled connections
	// that predate it.
	pool.Reset()

	err := repo.Touch(ctx, s.ID, time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("a write against a read-only database must fail")
	}
	if !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("SQLSTATE 25006 must classify as unavailable, got %#v (%v)", err, err)
	}
}

// A business error must survive all of this untouched: if the classifier were
// too broad, "no such session" would start telling shoppers to retry forever.
func TestBusinessErrorsAreNotClassifiedAsUnavailable(t *testing.T) {
	pool := newTestDB(t)
	repo := NewSessionRepository(pool)

	_, err := repo.FindByID(context.Background(), "11111111-1111-1111-1111-111111111111")
	if !errors.Is(err, domain.ErrSessionNotFound) {
		t.Fatalf("want ErrSessionNotFound, got %v", err)
	}
	if errors.Is(err, domain.ErrUnavailable) {
		t.Error("a missing row was classified as datastore unavailability")
	}
}

// WrapIdem over the real pkg store against the real schema: the adapter must
// be transparent on the happy path (this also exercises the constructor the
// unit tests bypass by building the struct directly).
func TestWrapIdemIsTransparentOnARealStore(t *testing.T) {
	pool := newTestDB(t)
	s := WrapIdem(idempotency.New(pool, time.Minute))
	ctx := context.Background()

	rec, proceed, err := s.Claim(ctx, "7", "wrap-key", "POST", "/confirm", "h1")
	if err != nil || !proceed || rec == nil {
		t.Fatalf("claim: rec=%v proceed=%v err=%v", rec, proceed, err)
	}
	subject := int64(42)
	if err := s.Checkpoint(ctx, rec.ID, &subject); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := s.Finish(ctx, rec.ID, 201, []byte(`{}`)); err != nil {
		t.Fatalf("finish: %v", err)
	}
	// A finished key replays instead of proceeding.
	rec2, proceed2, err := s.Claim(ctx, "7", "wrap-key", "POST", "/confirm", "h1")
	if err != nil || proceed2 {
		t.Fatalf("replay claim: proceed=%v err=%v — want cached replay", proceed2, err)
	}
	if rec2.ResponseCode == nil || *rec2.ResponseCode != 201 {
		t.Fatalf("replayed code = %v, want 201", rec2.ResponseCode)
	}
	// Release on a fresh claim leaves the key reclaimable.
	rec3, _, err := s.Claim(ctx, "7", "wrap-key-2", "POST", "/confirm", "h2")
	if err != nil {
		t.Fatalf("claim 2: %v", err)
	}
	if err := s.Release(ctx, rec3.ID); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, proceed, err := s.Claim(ctx, "7", "wrap-key-2", "POST", "/confirm", "h2"); err != nil || !proceed {
		t.Fatalf("reclaim after release: proceed=%v err=%v", proceed, err)
	}
}

// The promo lock convoy must surface as CONTENTION (55P03, a 500-class error),
// never as fake unavailability. Before the SET LOCAL lock_timeout, a waiter
// queued behind a held FOR UPDATE died at the 3s query deadline, which the
// classifier reads as ErrUnavailable — a hot promo code could manufacture
// fake-failover 503s and on-call breadcrumbs during a flash sale.
func TestRedeemPromoLockConvoyIsContentionNotUnavailability(t *testing.T) {
	pool := newTestDB(t)
	repo := NewSessionRepository(pool)
	ctx := context.Background()

	// tx1 parks on the code row, simulating a slow concurrent redemption.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := holder.Exec(ctx,
		`SELECT 1 FROM promo_codes WHERE code = 'SAVE5' FOR UPDATE`); err != nil {
		t.Fatalf("holder lock: %v", err)
	}

	start := time.Now()
	err = repo.RedeemPromo(ctx, "SAVE5", "7", "33333333-3333-3333-3333-333333333333")
	waited := time.Since(start)

	if err == nil {
		t.Fatal("a redemption queued behind a held lock must fail, not hang past the deadline")
	}
	if errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("lock contention was classified as datastore unavailability: %v", err)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("want SQLSTATE 55P03 (lock_not_available), got %v", err)
	}
	// The whole point of the 2s lock_timeout: fail BEFORE the 3s query deadline.
	if waited >= 3*time.Second {
		t.Fatalf("waited %v — the lock_timeout did not fire before the query deadline", waited)
	}
}

// An infrastructure failure inside ExpireDue must propagate, never read as
// "session gone" (OutcomeGone permanently stops the abandonment workflow).
// Honest scope note: dropping the table breaks the flow at the FIRST statement
// that touches it — the parked-confirm UPDATE — because every statement in the
// method reads the same objects, so the binding-read branch itself cannot be
// failure-injected from the schema. That branch's verdict table is pinned by
// the unit test TestConfirmingBindingOutcome; this test guards the method-wide
// property against a real Postgres.
func TestExpireDueBindingReadFailurePropagatesInsteadOfGone(t *testing.T) {
	pool := newTestDB(t)
	repo := NewSessionRepository(pool)
	ctx := context.Background()

	s := newSession("expire-victim")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repo.UpdateStatus(ctx, s.ID, domain.StatusOpen, domain.StatusReady); err != nil {
		t.Fatalf("to ready: %v", err)
	}
	if err := repo.BeginConfirm(ctx, s.ID, 42); err != nil {
		t.Fatalf("begin confirm: %v", err)
	}

	// Break the binding read deterministically: this test owns its database.
	if _, err := pool.Exec(ctx, `DROP TABLE idempotency_keys CASCADE`); err != nil {
		t.Fatalf("drop: %v", err)
	}

	outcome, _, err := repo.ExpireDue(ctx, s.ID, time.Minute)
	if err == nil {
		t.Fatalf("want the binding-read failure to propagate, got outcome %q with nil error", outcome)
	}
	if outcome == domain.OutcomeGone {
		t.Fatal("an infrastructure failure was converted into OutcomeGone — the workflow would abandon the session forever")
	}
	// 42P01 (undefined_table) is OUR bug class, so it must stay unclassified;
	// the point here is propagation, not the verdict.
	if errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("undefined_table misclassified as unavailability: %v", err)
	}
}
