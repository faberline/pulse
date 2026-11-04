# Retry-safe pairing candidate

The Rust connector owns this flow. Swift only sends the person's code and shows state. Another UI can use the same `NativeCommand` and `NativeState` contract. The local executor uses the same Rust flow with its separate role and identity.

## Recovery order

1. Rust creates an attempt UUID and a random 256-bit candidate token. It saves the code, device, role, attempt and token in Keychain before any HTTP request.
2. Rust calls `GET /v1/device-pairings/identity` with the candidate bearer token. A valid response must match the device and role. This read does not change the lease or epoch.
3. Only a marked credential denial allows `POST /v1/device-pairings/claim`. Its strict JSON fields are `code`, `attempt_id`, `device`, `role`, `token` and optional `server_generation`.
4. The server consumes the unused code and installs the identity in one transaction. It stores hashes of the secrets. An identical retry returns the same identity. It does not change the lease, epoch or code expiry.
5. After server proof, Rust prepares the local copy fence. It saves the active credential in Keychain, clears the pending claim, then activates the copy and connects.

Keychain keeps active and pending values in one versioned item. A pending claim survives network failure or an interrupted active-credential save. Startup checks it before connecting. A retry with the same code reuses the saved attempt. Existing plain credential items remain readable.

## Limits and rejection

Codes still expire after ten minutes. A claim retry requires the original expiry. An already installed candidate can be proved after that expiry. The proof cannot consume a code, extend it, or restore a revoked identity.

Each role still permits one active identity. A device cannot change roles. Migration 11 retains token hashes across rotation, including credentials present at upgrade. A later code cannot install one of those old tokens. Hashes overwritten before this migration cannot be recovered.

The server marks definitive claim rejection with `x-pulse-pairing-denied: 1` on status 400 or 403. Rust then clears only the pending claim. A storage error returns 503 without a denial marker. Unmarked 401/403, bad bodies, wrong proof and network failure preserve the attempt. Rust does not fall back to the legacy redeem endpoint.

Both new endpoints send `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`. Tokens and codes stay out of journal databases, copy restrictions, UI state and logs. The server requires a code to claim an identity. Identity proof itself grants no task or tool authority.

The old `/redeem` endpoint remains for existing clients. Its lost-response window remains. Upgrade the server before using the new connector. Pending recovery runs at startup or on an explicit pairing request. It does not use idle polling or call a model.

## Pairing after a server restore

A restored server has a new generation UUID. It adds `x-pulse-restore-generation` to marked credential denials from the identity endpoint. Successful identity proofs include the same `server_generation`. Claims must match the current generation before the server uses a code or installs a token. The legacy redeem endpoint cannot pair with a restored server. Normal servers without a restore generation retain the existing wire format and claim hashes.

Use a new empty local data directory after a restore. Pair before entering input. Rust refuses to adopt the generation if the profile has an active vault credential, old input, work, cached data, changed counters or prior copy restrictions. The old profile stays available for manual review. Rust does not silently move its input into a new identity.

Rust saves the generation outside SQLite in the current restrictions file, using format 9. It binds the device, role and server hash. It saves a pending fence before the local SQL marker, then marks the fence committed. A failed SQL write can resume with the same generation. A database-only rollback that removes the committed marker fails before migrations or network use. Keep the current restrictions file; rolling back both files remains unsupported.

Rust then creates a new attempt and token. It saves the generation-bound pending claim in the vault before sending the claim. A failed vault save sends no claim. Restart can finish the saved attempt after a lost response or code expiry. A later server generation requires a new profile. Generation metadata grants no tool or task authority.

This is a prerequisite for offline restore. The formal database cutover and crash-resume command is still pending. Do not set restore generation settings by hand. These fixtures set a generation only to test the pairing protocol.

## Verification boundary

`tests/pairing.rs` checks exact and concurrent retry, unchanged lease/epoch/expiry, changed claims, reused attempts, expiry, revocation, rotation, role limits, legacy token history and absence of plaintext secrets on disk.

`client::pairing_tests` uses real loopback HTTP and WebSocket with a test vault. It checks a lost server response, connector restart after code expiry, active-plus-pending startup, failed vault writes, ambiguous denial, bad proof, rejected code, revoked claims, storage errors and response cache controls. The vault codec test checks legacy upgrade, pending records and strict decoding.

These tests do not prove actual macOS Keychain access or GUI recovery. Those checks, current-head hosted CI and the full D01–D10 acceptance remain separate. The goal stays active.
