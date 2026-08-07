package domain

import "errors"

var (
	// ErrSessionNotFound — unknown id OR a session owned by someone else
	// (anti-IDOR: foreign sessions are indistinguishable from absent ones).
	ErrSessionNotFound = errors.New("checkout session not found")
	// ErrActiveSessionExists — the partial unique index rejected a second
	// active session for the user.
	ErrActiveSessionExists = errors.New("an active checkout session already exists")
	// ErrStaleTransition — a conditional update found a different current
	// status than expected (concurrent mutation lost the race).
	ErrStaleTransition = errors.New("session was modified concurrently")
	// ErrUnavailable — the datastore could not serve the request (failover,
	// unreachable primary, exhausted pool). It says nothing about whether the
	// request was valid, so it is safe to retry AS-IS: every write in this
	// service is guarded by an idempotency key or a conditional update, so a
	// retry either lands once or loses the CAS race and reports it.
	ErrUnavailable = errors.New("datastore temporarily unavailable")
)
