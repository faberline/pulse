CREATE TABLE link_pair_claims (
    code_digest TEXT PRIMARY KEY REFERENCES link_pairings(digest) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL UNIQUE,
    request_digest TEXT NOT NULL,
    device TEXT NOT NULL,
    role TEXT NOT NULL,
    token_digest TEXT NOT NULL
);
CREATE UNIQUE INDEX link_credential_digest ON link_device(token_digest);
-- Keep retired hashes even when a stable device rotates its credential.
CREATE TABLE link_pair_tokens (token_digest TEXT PRIMARY KEY);
INSERT INTO link_pair_tokens SELECT token_digest FROM link_device;
PRAGMA user_version=11;
