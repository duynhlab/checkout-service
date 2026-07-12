package v1

import "github.com/duynhlab/checkout-service/internal/core/domain"

// transitions is the checkout FSM (RFC-0015 §Session state machine), mirrored
// from payment's transition-table pattern. States advance strictly forward
// through the funnel; edits under way re-enter the same or an earlier state
// (never a forward jump); a price change at confirm drops back to
// shipping_set for a requote; terminal states have no outgoing edges.
var transitions = map[domain.SessionStatus]map[domain.SessionStatus]bool{
	domain.StatusOpen: {
		domain.StatusAddressSet: true,
		domain.StatusCancelled:  true,
		domain.StatusExpired:    true,
	},
	domain.StatusAddressSet: {
		domain.StatusAddressSet:  true, // edit address again
		domain.StatusShippingSet: true,
		domain.StatusCancelled:   true,
		domain.StatusExpired:     true,
	},
	domain.StatusShippingSet: {
		domain.StatusAddressSet:  true, // address change invalidates the quote
		domain.StatusShippingSet: true, // change shipping method
		domain.StatusReady:       true,
		domain.StatusCancelled:   true,
		domain.StatusExpired:     true,
	},
	domain.StatusReady: {
		domain.StatusAddressSet:  true, // same-or-earlier edits allowed…
		domain.StatusShippingSet: true, // …never forward jumps
		domain.StatusReady:       true, // re-attach payment token
		domain.StatusConfirming:  true,
		domain.StatusCancelled:   true,
		domain.StatusExpired:     true,
	},
	domain.StatusConfirming: {
		domain.StatusCompleted:   true,
		domain.StatusShippingSet: true, // PRICE_CHANGED → requote and re-confirm
	},
	// Terminal states: no rows — CanTransition answers false for everything.
}

// CanTransition reports whether the FSM permits moving from → to. Unknown
// states have no edges (fail closed).
func CanTransition(from, to domain.SessionStatus) bool {
	return transitions[from][to]
}
