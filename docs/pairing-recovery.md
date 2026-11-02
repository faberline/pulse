# Retry-safe pairing candidate

The Rust connector owns this flow. Swift only sends the person's code and shows state. Another UI can use the same `NativeCommand` and `NativeState` contract. The local executor uses the same Rust flow with its separate role and identity.

## Recovery order

1. Rust creates an attempt UUID and a random 256-bit candidate token. It saves the code, device, role, attempt and token in Keychain before any HTTP request.
2. Rust calls `GET /v1/device-pairings/identity` with the candidate bearer token. A valid response must match the device and role. This read does not change the lease or epoch.
3. Only a marked credential denial allows `POST /v1/device-pairings/claim`. Its strict JSON fields are `code`, `attempt_id`, `device`, `role` and `token`.
4. The server consumes the unused code and installs the identity in one transaction. It stores hashes of the secrets. An identical retry returns the same identity. It does not change the lease, epoch or code expiry.
5. After server proof, Rust prepares the local copy fence. It saves the active credential in Keychain, clears the pending claim, then activates the copy and connects.

Keychain keeps active and pending values in one versioned item. A pending claim survives network failure or an interrupted active-credential save. Startup checks it before connecting. A retry with the same code reuses the saved attempt. Existing plain credential items remain readable.

## Limits and rejection

Codes still expire after ten minutes. A claim retry requires the original expiry. An already installed candidate can be proved after that expiry. The proof cannot consume a code, extend it, or restore a revoked identity.

Each role still permits one active identity. A device cannot change roles. Migration 11 retains token hashes across rotation, including credentials present at upgrade. A later code cannot install one of those old tokens. Hashes overwritten before this migration cannot be recovered.

The server marks definitive claim rejection with `x-pulse-pairing-denied: 1` on status 400 or 403. Rust then clears only the pending claim. A storage error returns 503 without a denial marker. Unmarked 401/403, bad bodies, wrong proof and network failure preserve the attempt. Rust does not fall back to the legacy redeem endpoint.

Both new endpoints send `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`. Tokens and codes stay out of journal databases, copy restrictions, UI state and logs. The server requires a code to claim an identity. Identity proof itself grants no task or tool authority.

The old `/redeem` endpoint remains for existing clients. Its lost-response window remains. Upgrade the server before using the new connector. Pending recovery runs at startup or on an explicit pairing request. It does not use idle polling or call a model.

## Verification boundary

`tests/pairing.rs` checks exact and concurrent retry, unchanged lease/epoch/expiry, changed claims, reused attempts, expiry, revocation, rotation, role limits, legacy token history and absence of plaintext secrets on disk.

`client::pairing_tests` uses real loopback HTTP and WebSocket with a test vault. It checks a lost server response, connector restart after code expiry, active-plus-pending startup, failed vault writes, ambiguous denial, bad proof, rejected code, revoked claims, storage errors and response cache controls. The vault codec test checks legacy upgrade, pending records and strict decoding.

These tests do not prove actual macOS Keychain access or GUI recovery. Those checks, current-head hosted CI and the full D01–D10 acceptance remain separate. The goal stays active.
