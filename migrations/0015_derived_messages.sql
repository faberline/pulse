CREATE TABLE memory_derived_messages (
    id TEXT PRIMARY KEY REFERENCES link_chat(id),
    task_id TEXT NOT NULL,
    task_revision TEXT NOT NULL,
    redacted INTEGER NOT NULL DEFAULT 0 CHECK(redacted IN (0,1)),
    FOREIGN KEY(task_id,task_revision) REFERENCES task_memory_sources(task_id,task_revision)
);
CREATE INDEX memory_derived_task ON memory_derived_messages(task_id,task_revision);
CREATE TABLE link_erased_frames (
    device TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    data TEXT NOT NULL CHECK(json_valid(data)),
    PRIMARY KEY(device,sequence),
    FOREIGN KEY(device,sequence) REFERENCES link_outbox(device,sequence) ON DELETE CASCADE
);
ALTER TABLE link_device ADD COLUMN projection_erasure INTEGER NOT NULL DEFAULT 0 CHECK(projection_erasure IN (0,1));
INSERT INTO memory_derived_messages(id,task_id,task_revision)
SELECT c.id,b.task_id,b.task_revision FROM link_chat c JOIN runs r ON c.id LIKE 'model:'||r.id||':'||json_extract(r.data,'$.token.revision')||':%'
JOIN task_memory_sources b ON b.task_id=json_extract(r.data,'$.token.task_id') AND b.task_revision=json_extract(r.data,'$.token.revision');
PRAGMA user_version=15;
