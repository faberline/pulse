CREATE TABLE local_schedule_delivery (
    notice_id TEXT PRIMARY KEY REFERENCES local_schedule_notices(id),
    state TEXT NOT NULL CHECK(state IN ('pending','offered','submitted','failed','unknown','suppressed')),
    attempt_id TEXT UNIQUE,
    offered_at INTEGER,
    result TEXT CHECK(result IN ('submitted','failed','denied'))
);
PRAGMA user_version=13;
