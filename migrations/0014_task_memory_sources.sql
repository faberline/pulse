CREATE TABLE task_memory_sources (
    task_id TEXT NOT NULL REFERENCES tasks(id),
    task_revision TEXT NOT NULL,
    request_id TEXT NOT NULL UNIQUE,
    data TEXT NOT NULL CHECK(json_valid(data)),
    PRIMARY KEY(task_id,task_revision)
);
PRAGMA user_version=14;
