package v1

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// Availability canary (RFC-0021 P3): a percentage dial that opens the inventory
// READ path gradually once CHECKOUT_AVAILABILITY_SOURCE is `inventory`.
//
// STICKY PER USER, deliberately — unlike the shadow sampler, which is a plain
// per-request coin flip. Shadow only OBSERVES: Product stays authoritative, so a
// different decision on the next request costs nothing. The canary MOVES the
// authority, and a user's funnel spans two reads — CreateSession and Confirm. A
// per-request flip would let a session be created against Inventory and confirmed
// against Product, so a session accepted as in-stock could be refused at confirm
// (or the reverse): a failure the canary mechanism invented rather than one it
// found. Bucketing by user id makes each user's whole funnel use one authority.
//
// The user id is the bucket key rather than the session id because CreateSession
// has no session yet, and because a user who abandons and starts again should land
// on the same side — otherwise "it worked yesterday" reports become unreproducible.
//
// THE EXACT GUARANTEE, and its boundary. Membership is RECOMPUTED on every read
// from the process's current source + percentage; it is not persisted with the
// session. So the property is "one authority per user FOR A GIVEN
// CONFIGURATION" — not "per session, forever". Moving the dial (or the source)
// while a session is open can still split that session's two reads: created under
// pct=0 on Product, confirmed after a ramp to pct=20 on Inventory.
//
// That residue is accepted rather than designed away, for two reasons:
//
//   - The blast radius is a REQUOTE, never an oversell or a double charge:
//     checkout only ever CHECKS stock, it never reserves (ADR-020), so a confirm
//     whose new authority disagrees bounces through the existing
//     STOCK_UNAVAILABLE/PRICE_CHANGED requote path — a first-class, already-tested
//     outcome, not a corruption.
//   - Persisting the authority on the session would mean a schema column for a
//     temporary rollout dial that phase 4 deletes.
//
// Operationally that makes it a runbook rule, not a code rule: space ramp steps by
// at least the session TTL (30m by default), or accept that sessions open across
// the step may requote. The same is true of the `inventory` source flip itself,
// which predates this dial — the dial only makes the boundary finer.
//
// Spacing alone is not sufficient DURING a ramp: a rolling restart runs two pct
// values at once, so for the length of the rollout a user can create on an old pod
// and confirm on a new one. Same benign outcome, same reason.
//
// Why that outcome is benign today rests on facts OUTSIDE this repo, so they are
// named here to be re-checked if they change: both arms read price from the same
// product rows through the same conversion, so arm choice cannot change what is
// charged; `mergeCatalog`'s Sellable/Currency guards are inert because
// product-service hardcodes both; and CreateSession's snapshot reads only
// UnitPriceMinor, never AvailableQty, so a split can only change the CONFIRM-time
// stock verdict. Give product-service a real lifecycle/currency column and the two
// arms genuinely diverge — at which point persisting the authority per session
// stops being over-engineering.

// userBucketHash maps a user id to a stable 32-bit bucket, salted.
//
// FNV-1a, NOT maphash: maphash seeds itself per process, so two checkout replicas
// would disagree about the same user and stickiness would silently hold only
// within one pod. It must also survive restarts, so nothing here may depend on
// process state. The exact values are pinned by a test: changing the hash
// re-shuffles every user's assignment, which mid-rollout means moving users
// between authorities for no reason.
//
// WHY A KEY AT ALL. Unkeyed, the bucket is a pure function of a public algorithm
// and an input the caller knows — their own `sub`. Registration is open and
// unverified, so a caller who wants the inventory arm can register, compute the
// bucket offline, and repeat until they land inside the canary: about 100/pct
// attempts, which the edge rate limit (100/min) makes cheap even at pct=1. That
// does not escalate privilege — both arms are intended-authoritative and the
// oversell gate is the saga's Reserve, not this read — but it breaks the one thing
// the dial is FOR: bounding how much traffic reaches inventory-service. An
// operator who reads "pct=1" as a blast-radius bound would be wrong against
// adversarial traffic.
//
// WHY HMAC AND NOT A SALTED FNV. Prefixing a salt to FNV-1a does NOT key it:
// FNV is a streaming hash, so the whole salt collapses into a single 32-bit
// intermediate state before the user id is mixed in. Its entropy is therefore
// capped at 32 bits no matter how long the salt is, two distinct salts sharing
// that state bucket every user identically (a birthday collision at ~2^16 tries),
// and an attacker never needs the salt itself — only its 32-bit image, which
// ~30-45 observed arm assignments plus offline work recovers. HMAC-SHA256 keyed
// with the salt is a real keyed construction: deterministic, so cross-replica
// stickiness holds, and not invertible from observed assignments.
//
// An EMPTY key is the default and deliberately allowed: buckets are then
// effectively public, which is fine before the dial is opened and for local runs.
// Startup logs a fingerprint and warns when the dial is partly open with no key —
// see cmd/main.go.
func userBucketHash(salt, userID string) uint32 {
	mac := hmac.New(sha256.New, []byte(salt))
	// Write on a hash never returns an error (documented on hash.Hash).
	_, _ = mac.Write([]byte(userID))
	// The first 4 bytes are as uniform as any others; the bucket only needs 32
	// bits. BigEndian so the value is stable and readable, not host-dependent.
	return binary.BigEndian.Uint32(mac.Sum(nil)[:4])
}

// SaltFingerprint is a short, non-reversible identifier for a canary key, so two
// replicas can be compared without printing the key itself. Empty key ⇒ empty
// fingerprint, which is what makes "no key configured" visible in a startup log.
//
// A differing fingerprint between pods means every user's arm assignment differs
// between them — the silent re-shuffle this package exists to avoid — so it is
// logged rather than left to be discovered from confused traffic.
func SaltFingerprint(salt string) string {
	if salt == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(salt))
	return hex.EncodeToString(sum[:4])
}

// inCanary reports whether this user's reads go to the canary (inventory) side.
//
// The bounds are absolute rather than probabilistic: 100 includes everyone so
// enabling the flag with no canary configured behaves exactly as it did before
// the dial existed, and 0 includes nobody so an operator can flip the source with
// zero blast radius and then open up. Out-of-range values clamp — flagx rejects
// them at startup, so this only guards a future caller.
func inCanary(salt, userID string, pct int) bool {
	if pct >= fullCanaryPct {
		return true
	}
	if pct <= 0 {
		return false
	}
	// An empty id has no meaningful bucket, and mapping every subject-less caller
	// into one arm wholesale is the wrong default: excluded, so the canary can only
	// ever act on an identified user.
	if userID == "" {
		return false
	}
	return userBucketHash(salt, userID)%100 < uint32(pct) //nolint:gosec // 0<pct<100
}
