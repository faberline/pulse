-- UUID spelling is not a second source identity. Ambiguous historical duplicates
-- fail the migration instead of silently discarding a source or request.
ALTER TABLE memory_events ADD COLUMN source_uuid TEXT GENERATED ALWAYS AS (lower(CASE length(source_id) WHEN 32 THEN substr(source_id,1,8)||'-'||substr(source_id,9,4)||'-'||substr(source_id,13,4)||'-'||substr(source_id,17,4)||'-'||substr(source_id,21,12) WHEN 38 THEN substr(source_id,2,36) WHEN 45 THEN substr(source_id,10,36) ELSE source_id END)) VIRTUAL;
ALTER TABLE memories ADD COLUMN source_uuid TEXT GENERATED ALWAYS AS (lower(CASE length(source_id) WHEN 32 THEN substr(source_id,1,8)||'-'||substr(source_id,9,4)||'-'||substr(source_id,13,4)||'-'||substr(source_id,17,4)||'-'||substr(source_id,21,12) WHEN 38 THEN substr(source_id,2,36) WHEN 45 THEN substr(source_id,10,36) ELSE source_id END)) VIRTUAL;
ALTER TABLE local_memory_queue ADD COLUMN source_uuid TEXT GENERATED ALWAYS AS (lower(CASE length(source_id) WHEN 32 THEN substr(source_id,1,8)||'-'||substr(source_id,9,4)||'-'||substr(source_id,13,4)||'-'||substr(source_id,17,4)||'-'||substr(source_id,21,12) WHEN 38 THEN substr(source_id,2,36) WHEN 45 THEN substr(source_id,10,36) ELSE source_id END)) VIRTUAL;
CREATE UNIQUE INDEX memory_event_uuid ON memory_events(source_uuid);
CREATE UNIQUE INDEX memory_live_uuid ON memories(source_uuid);
CREATE UNIQUE INDEX local_memory_uuid ON local_memory_queue(source_uuid);
PRAGMA user_version=8;
