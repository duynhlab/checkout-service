package domain

// ValidPaymentToken enforces the platform's PCI discipline on the session's
// payment_method_token before it is persisted: a short opaque token (`tok_` +
// [A-Za-z0-9_], ≤ 64 chars) with no card-number-like digit run. This is
// defense-in-depth — payment validates identically and authoritatively at
// authorize; this copy keeps PAN-shaped strings out of the checkout_sessions
// table and out of the order handoff. Same rule as order/payment (RFC-0015).
func ValidPaymentToken(s string) bool {
	if len(s) < 4 || s[:4] != "tok_" || len(s) > 64 {
		return false
	}
	// Count TOTAL digits, not the longest contiguous run: separators like `_`
	// must not let a grouped PAN ("tok_4111_1111_1111_1111") slip through.
	digits := 0
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			digits++
			if digits >= 12 { // a card number is 13–19 digits
				return false
			}
		case (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_':
			// allowed
		default:
			return false
		}
	}
	return true
}
