-- Delivery/cache only. Server memory truth remains in memories/memory_events.
CREATE TABLE local_memory_queue (
    source_id TEXT PRIMARY KEY,
    request TEXT CHECK(request IS NULL OR json_valid(request)),
    phase TEXT NOT NULL,
    saved_at INTEGER NOT NULL
);
CREATE TABLE local_memory_cache (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL CHECK(json_valid(data))
);
PRAGMA user_version=7;
