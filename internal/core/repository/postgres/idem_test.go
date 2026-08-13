package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/duynhlab/pkg/idempotency"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// fakeIdem returns RAW errors, the way *idempotency.Repository does — that is
// the whole point: the adapter, not the handler, must do the classifying.
type fakeIdem struct{ err error }

func (f fakeIdem) Claim(context.Context, string, string, string, string, string) (*idempotency.Record, bool, error) {
	return nil, false, f.err
}
func (f fakeIdem) Checkpoint(context.Context, int64, *int64) error  { return f.err }
func (f fakeIdem) Release(context.Context, int64) error             { return f.err }
func (f fakeIdem) Finish(context.Context, int64, int, []byte) error { return f.err }

func TestIdemStoreClassifiesInfrastructureFailures(t *testing.T) {
	raw := fmt.Errorf("claim: %w", &pgconn.PgError{Code: "57P01"}) // CNPG switchover
	s := IdemStore{inner: fakeIdem{err: raw}}

	_, _, err := s.Claim(context.Background(), "a11ce000-0000-4000-8000-000000000001", "k", "POST", "/p", "h")
	if !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("Claim: a switchover error must classify as unavailable, got %v", err)
	}
	if err := s.Finish(context.Background(), 1, 201, nil); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("Finish: want unavailable, got %v", err)
	}
	if err := s.Release(context.Background(), 1); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("Release: want unavailable, got %v", err)
	}
	if err := s.Checkpoint(context.Background(), 1, nil); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("Checkpoint: want unavailable, got %v", err)
	}
}

// The pkg sentinels drive the 409/replay contract in the logic layer; the
// adapter must hand them through untouched or every concurrent double-tap
// would start reading as an outage.
func TestIdemStoreLeavesBusinessSentinelsAlone(t *testing.T) {
	for _, sentinel := range []error{idempotency.ErrConflict, idempotency.ErrLocked} {
		s := IdemStore{inner: fakeIdem{err: sentinel}}
		_, _, err := s.Claim(context.Background(), "a11ce000-0000-4000-8000-000000000001", "k", "POST", "/p", "h")
		if !errors.Is(err, sentinel) {
			t.Fatalf("sentinel %v did not survive the adapter: %v", sentinel, err)
		}
		if errors.Is(err, domain.ErrUnavailable) {
			t.Fatalf("sentinel %v was misclassified as unavailability", sentinel)
		}
	}
}
