package v1

import "errors"

var (
	// ErrSessionNotFound covers unknown ids AND foreign owners (anti-IDOR).
	ErrSessionNotFound = errors.New("checkout session not found")
	// ErrSessionExpired — the lazy-expiry check rejected the operation; maps
	// to 410 SESSION_EXPIRED at the web boundary.
	ErrSessionExpired = errors.New("checkout session expired")
	// ErrInvalidTransition — the FSM rejected the move; 409 INVALID_TRANSITION.
	ErrInvalidTransition = errors.New("invalid session state transition")
	// ErrEmptyCart — session creation with an empty cart; 409 CONFLICT.
	ErrEmptyCart = errors.New("cart is empty")
	// ErrUpstream — a dependency (cart/product) failed; 500 opaque.
	ErrUpstream = errors.New("upstream dependency failed")
)
