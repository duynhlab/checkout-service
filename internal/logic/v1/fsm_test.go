package v1

import (
	"testing"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// TestCanTransition pins the FULL RFC-0015 state machine — every state pair
// is asserted so any table edit shows up here first. Forward-only through the
// funnel; edits re-enter same-or-earlier states; terminal states never move.
func TestCanTransition(t *testing.T) {
	all := []domain.SessionStatus{
		domain.StatusOpen, domain.StatusAddressSet, domain.StatusShippingSet,
		domain.StatusReady, domain.StatusConfirming, domain.StatusCompleted,
		domain.StatusCancelled, domain.StatusExpired,
	}

	// The complete set of legal edges (RFC-0015 §Session state machine).
	legal := map[domain.SessionStatus][]domain.SessionStatus{
		// PUT address re-enters address_set from any pre-confirm state
		// (same-or-earlier mutation); PUT shipping needs an address first.
		domain.StatusOpen: {domain.StatusAddressSet, domain.StatusCancelled, domain.StatusExpired},
		domain.StatusAddressSet: {
			domain.StatusAddressSet, // edit address again
			domain.StatusShippingSet, domain.StatusCancelled, domain.StatusExpired,
		},
		domain.StatusShippingSet: {
			domain.StatusAddressSet,  // change address → totals must be requoted
			domain.StatusShippingSet, // change shipping method
			domain.StatusReady, domain.StatusCancelled, domain.StatusExpired,
		},
		domain.StatusReady: {
			domain.StatusAddressSet,  // edit under way re-enters earlier state
			domain.StatusShippingSet, // change method from ready
			domain.StatusReady,       // re-attach payment token
			domain.StatusConfirming, domain.StatusCancelled, domain.StatusExpired,
		},
		domain.StatusConfirming: {
			domain.StatusCompleted,
			domain.StatusShippingSet, // PRICE_CHANGED drops back for a requote
		},
		domain.StatusCompleted: {},
		domain.StatusCancelled: {},
		domain.StatusExpired:   {},
	}

	for _, from := range all {
		allowed := map[domain.SessionStatus]bool{}
		for _, to := range legal[from] {
			allowed[to] = true
		}
		for _, to := range all {
			got := CanTransition(from, to)
			if got != allowed[to] {
				t.Errorf("CanTransition(%s → %s) = %v, want %v", from, to, got, allowed[to])
			}
		}
	}
}

// TestCanTransition_TerminalStatesNeverMove is the safety property stated on
// its own: no terminal state has any outgoing edge, including self-loops.
func TestCanTransition_TerminalStatesNeverMove(t *testing.T) {
	for _, from := range []domain.SessionStatus{domain.StatusCompleted, domain.StatusCancelled, domain.StatusExpired} {
		for _, to := range []domain.SessionStatus{
			domain.StatusOpen, domain.StatusAddressSet, domain.StatusShippingSet,
			domain.StatusReady, domain.StatusConfirming, domain.StatusCompleted,
			domain.StatusCancelled, domain.StatusExpired,
		} {
			if CanTransition(from, to) {
				t.Errorf("terminal %s must not transition to %s", from, to)
			}
		}
	}
}
