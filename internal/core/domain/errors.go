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
)
