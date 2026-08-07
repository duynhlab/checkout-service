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

	rec, proceed, err := s.Claim(ctx, 7, "wrap-key", "POST", "/confirm", "h1")
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
	rec2, proceed2, err := s.Claim(ctx, 7, "wrap-key", "POST", "/confirm", "h1")
	if err != nil || proceed2 {
		t.Fatalf("replay claim: proceed=%v err=%v — want cached replay", proceed2, err)
	}
	if rec2.ResponseCode == nil || *rec2.ResponseCode != 201 {
		t.Fatalf("replayed code = %v, want 201", rec2.ResponseCode)
	}
	// Release on a fresh claim leaves the key reclaimable.
	rec3, _, err := s.Claim(ctx, 7, "wrap-key-2", "POST", "/confirm", "h2")
	if err != nil {
		t.Fatalf("claim 2: %v", err)
	}
	if err := s.Release(ctx, rec3.ID); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, proceed, err := s.Claim(ctx, 7, "wrap-key-2", "POST", "/confirm", "h2"); err != nil || !proceed {
		t.Fatalf("reclaim after release: proceed=%v err=%v", proceed, err)
	}
}
