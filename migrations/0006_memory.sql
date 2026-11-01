-- Knowledge has no authority. Its source is a trusted Human request, not a tool.
CREATE TABLE memories (
    id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL CHECK(revision > 0),
    label TEXT NOT NULL,
    body TEXT NOT NULL,
    source_id TEXT NOT NULL UNIQUE,
    source_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);
-- Replay evidence contains no source text. Redaction removes its fingerprint too.
CREATE TABLE memory_events (
    source_id TEXT PRIMARY KEY,
    memory_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    fingerprint TEXT,
    outcome TEXT NOT NULL CHECK(outcome IN ('stored','forgotten','redacted'))
);
CREATE INDEX memory_event_target ON memory_events(memory_id,revision);
PRAGMA user_version=6;
