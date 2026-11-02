CREATE TABLE memory_erased_reads (
    id TEXT PRIMARY KEY REFERENCES link_reads(id)
);
INSERT OR IGNORE INTO memory_derived_messages(id,task_id,task_revision)
SELECT c.id,r.task_id,r.task_revision FROM link_chat c JOIN link_reads r ON r.id=c.id
JOIN task_memory_sources b ON b.task_id=r.task_id AND b.task_revision=r.task_revision;
INSERT OR IGNORE INTO memory_derived_messages(id,task_id,task_revision)
SELECT c.id,r.task_id,b.task_revision FROM link_chat c JOIN operation_requests r ON r.id=c.id
JOIN task_memory_sources b ON b.task_id=r.task_id AND b.task_revision=json_extract(r.data,'$.proposal.token.revision');
PRAGMA user_version=16;
