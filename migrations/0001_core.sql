CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE events (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    origin TEXT NOT NULL,
    digest TEXT NOT NULL,
    payload TEXT NOT NULL,
    received_at INTEGER NOT NULL,
    outcome TEXT,
    UNIQUE(source, source_id)
);
CREATE TABLE conflicts (
    event_id TEXT NOT NULL REFERENCES events(id),
    observed_digest TEXT NOT NULL,
    observed_at INTEGER NOT NULL
);
CREATE TABLE tasks (id TEXT PRIMARY KEY, data TEXT NOT NULL CHECK(json_valid(data)));
CREATE TABLE event_tasks (
    event_id TEXT NOT NULL REFERENCES events(id),
    task_id TEXT NOT NULL REFERENCES tasks(id),
    PRIMARY KEY(event_id, task_id)
);
CREATE TABLE runs (id TEXT PRIMARY KEY, data TEXT NOT NULL CHECK(json_valid(data)));
CREATE TABLE operations (id TEXT PRIMARY KEY, data TEXT NOT NULL CHECK(json_valid(data)));
CREATE TABLE receipts (
    operation_id TEXT PRIMARY KEY REFERENCES operations(id),
    digest TEXT NOT NULL,
    data TEXT NOT NULL CHECK(json_valid(data))
);
CREATE TABLE budget (
    call_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    day TEXT NOT NULL,
    reserved INTEGER NOT NULL CHECK(reserved > 0),
    charged INTEGER NOT NULL CHECK(charged >= 0),
    state TEXT NOT NULL CHECK(state IN ('reserved','unknown','settled')),
    rate_version TEXT NOT NULL,
    evidence TEXT
);
CREATE INDEX budget_task ON budget(task_id);
CREATE INDEX budget_day ON budget(day);
CREATE TABLE outbox (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0,1))
);
PRAGMA user_version = 1;
