package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// The point of these cases is the BOUNDARY, not the happy path: a wrong verdict
// either way is a real incident. Classifying a bug as unavailable tells the
// shopper "retry" forever and hides the bug from on-call; classifying an outage
// as a bug tells the shopper "do not retry" during a routine failover.
func TestClassifyPostgresErrorCodes(t *testing.T) {
	tests := []struct {
		name        string
		code        string
		unavailable bool
	}{
		// Class 08 — connection exception. The whole class qualifies: every
		// member means the connection, not the statement, is the problem.
		{"connection_exception", "08000", true},
		{"connection_does_not_exist", "08003", true},
		{"connection_failure", "08006", true},
		{"sqlclient_unable_to_establish", "08001", true},

		// The failover signals proper.
		{"admin_shutdown is the CNPG switchover signal", "57P01", true},
		{"crash_shutdown", "57P02", true},
		{"cannot_connect_now while recovering", "57P03", true},
		// Reached a standby: the pooler still points at the demoted primary.
		{"read_only_sql_transaction", "25006", true},

		// Everything below is OUR bug or the caller's, and must keep answering
		// 500/4xx. A unique violation especially: it is already translated to a
		// domain error upstream, and turning it into "retry" would loop.
		{"unique_violation", "23505", false},
		{"foreign_key_violation", "23503", false},
		{"not_null_violation", "23502", false},
		{"invalid_text_representation", "22P02", false},
		{"syntax_error", "42601", false},
		{"undefined_column", "42703", false},
		{"insufficient_privilege", "42501", false},
		// Contention, not unavailability. Deliberately NOT retryable-503: a
		// deadlock is a lock-ordering bug and must stay visible as one.
		{"serialization_failure", "40001", false},
		{"deadlock_detected", "40P01", false},
		// Class 53 is resource exhaustion. too_many_connections is arguably
		// unavailability, but it is the pooler's job to bound connections, so a
		// service that trips it has a pool misconfiguration to fix, not a
		// transient blip to retry.
		{"too_many_connections", "53300", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := fmt.Errorf("insert session: %w", &pgconn.PgError{Code: tc.code})
			got := classify(in)

			if errors.Is(got, domain.ErrUnavailable) != tc.unavailable {
				t.Fatalf("SQLSTATE %s: unavailable = %v, want %v",
					tc.code, !tc.unavailable, tc.unavailable)
			}
			// Whatever the verdict, the original error must survive: it is the
			// only thing that tells an operator WHICH failure happened, since
			// the response body is deliberately opaque.
			var pgErr *pgconn.PgError
			if !errors.As(got, &pgErr) || pgErr.Code != tc.code {
				t.Errorf("SQLSTATE %s: lost the underlying pg error, got %v", tc.code, got)
			}
		})
	}
}

func TestClassifyNonPostgresErrors(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		unavailable bool
	}{
		{"nil stays nil", nil, false},
		{
			// This package puts a 3s deadline on every query, so an unreachable
			// or hung Postgres surfaces here rather than as a SQLSTATE. It is
			// the most likely shape of a failover, not an edge case.
			name:        "the repository query deadline",
			err:         fmt.Errorf("find session: %w", context.DeadlineExceeded),
			unavailable: true,
		},
		{
			// The client hung up. Reporting our own unavailability here would
			// bill a shopper's closed tab to the error budget.
			name:        "caller cancelled",
			err:         fmt.Errorf("find session: %w", context.Canceled),
			unavailable: false,
		},
		{
			// pgx.ErrNoRows is a business answer ("no such session"), already
			// translated at each call site. It must never become a 503.
			name:        "no rows",
			err:         fmt.Errorf("scan: %w", pgx.ErrNoRows),
			unavailable: false,
		},
		{
			name:        "an ordinary error is not unavailability",
			err:         errors.New("marshal address: unexpected type"),
			unavailable: false,
		},
		{
			// Idempotent: the sentinel must not stack on repeated passes.
			name:        "already classified",
			err:         fmt.Errorf("%w: %w", domain.ErrUnavailable, context.DeadlineExceeded),
			unavailable: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classify(tc.err)

			if tc.err == nil {
				if got != nil {
					t.Fatalf("classify(nil) = %v, want nil", got)
				}
				return
			}
			if errors.Is(got, domain.ErrUnavailable) != tc.unavailable {
				t.Fatalf("unavailable = %v, want %v (err %v)",
					!tc.unavailable, tc.unavailable, got)
			}
		})
	}
}

// classify runs on every repository method return, including the overwhelmingly
// common nil and business-error cases, so it must not allocate a new chain when
// it has no verdict to add.
func TestClassifyLeavesUnrelatedErrorsIdentical(t *testing.T) {
	in := errors.New("boom")
	if got := classify(in); got != in {
		t.Fatalf("classify wrapped an unrelated error: %v", got)
	}
}

// A second pass must not nest the sentinel twice — otherwise an operator reads
// "unavailable: unavailable: …" in the logs.
func TestClassifyIsIdempotent(t *testing.T) {
	once := classify(fmt.Errorf("q: %w", &pgconn.PgError{Code: "57P01"}))
	twice := classify(once)
	if once != twice {
		t.Fatalf("classify is not idempotent:\n once  = %v\n twice = %v", once, twice)
	}
}

// The deterministic half of the failover proof, and the case that actually
// reaches shoppers: Postgres is not answering at all. A killed IDLE connection
// turned out not to qualify — pgx transparently dials a replacement, so the
// repository never sees an error, which is why that scenario is absent here.
// What a failover window really looks like from the client is an address that
// will not accept a connection.
func TestRepositoryReportsUnavailableWhenPostgresIsUnreachable(t *testing.T) {
	// Port 1 is reserved and never listening; the pool is lazy, so nothing is
	// dialled until the query below.
	pool, err := pgxpool.New(context.Background(),
		"postgres://nobody:nobody@127.0.0.1:1/nothing?connect_timeout=1")
	if err != nil {
		t.Fatalf("building the pool must not fail — it connects lazily: %v", err)
	}
	defer pool.Close()

	_, err = NewSessionRepository(pool).FindByID(context.Background(),
		"11111111-1111-1111-1111-111111111111")
	if err == nil {
		t.Fatal("a query against an unreachable Postgres must fail")
	}
	if !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("an unreachable Postgres must classify as unavailable, got %#v (%v)", err, err)
	}
}
