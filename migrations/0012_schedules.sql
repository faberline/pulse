CREATE TABLE schedules (id TEXT PRIMARY KEY, data TEXT NOT NULL CHECK(json_valid(data)));
CREATE INDEX schedule_due ON schedules(json_extract(data,'$.state'), json_extract(data,'$.request.due_at'));
CREATE TABLE local_schedule_notices (id TEXT PRIMARY KEY, data TEXT NOT NULL CHECK(json_valid(data)), received_at INTEGER NOT NULL);
CREATE TABLE local_schedule_inputs (source_id TEXT PRIMARY KEY, text TEXT NOT NULL, request TEXT NOT NULL CHECK(json_valid(request)));
ALTER TABLE link_device ADD COLUMN schedule_notices INTEGER NOT NULL DEFAULT 0 CHECK(schedule_notices IN (0,1));
PRAGMA user_version=12;
