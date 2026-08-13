package postgres

import (
	"context"

	"github.com/duynhlab/pkg/idempotency"
)

// idemStore is the slice of pkg/idempotency the confirm flow uses — kept
// structurally identical to logic/v1.IdemStore so the adapter can wrap the
// concrete *idempotency.Repository without this package importing the logic
// layer.
type idemStore interface {
	Claim(ctx context.Context, userID, key, method, path, hash string) (*idempotency.Record, bool, error)
	Checkpoint(ctx context.Context, id int64, subjectID *int64) error
	Release(ctx context.Context, id int64) error
	Finish(ctx context.Context, id int64, code int, body []byte) error
}

// IdemStore routes pkg/idempotency errors through the same classifier as the
// session repository. The idempotency table lives on the SAME pool as the
// sessions, but the pkg store returns raw pgx errors — without this wrapper,
// the Claim (one of the first writes of every confirm) answered a bare 500
// during a failover while every repository method around it answered 503.
//
// Classification happens here, at wiring time, so the logic layer keeps
// depending only on its own IdemStore port. The pkg sentinels
// (idempotency.ErrConflict/ErrLocked) are plain errors, not PgErrors or
// transport failures, so classify leaves them untouched.
type IdemStore struct {
	inner idemStore
}

// WrapIdem builds the classifying adapter around a pkg/idempotency store.
func WrapIdem(inner *idempotency.Repository) IdemStore {
	return IdemStore{inner: inner}
}

func (s IdemStore) Claim(ctx context.Context, userID, key, method, path, hash string) (_ *idempotency.Record, _ bool, err error) {
	defer func() { err = classify(err) }()
	return s.inner.Claim(ctx, userID, key, method, path, hash)
}

func (s IdemStore) Checkpoint(ctx context.Context, id int64, subjectID *int64) (err error) {
	defer func() { err = classify(err) }()
	return s.inner.Checkpoint(ctx, id, subjectID)
}

func (s IdemStore) Release(ctx context.Context, id int64) (err error) {
	defer func() { err = classify(err) }()
	return s.inner.Release(ctx, id)
}

func (s IdemStore) Finish(ctx context.Context, id int64, code int, body []byte) (err error) {
	defer func() { err = classify(err) }()
	return s.inner.Finish(ctx, id, code, body)
}
