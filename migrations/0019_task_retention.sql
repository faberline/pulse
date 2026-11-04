-- Terminal time belongs to the same transaction as the state transition.
-- Old terminal tasks without that evidence are not assigned an invented time.
CREATE TABLE task_retention (
    task_id TEXT PRIMARY KEY REFERENCES tasks(id),
    terminal_at INTEGER NOT NULL CHECK(terminal_at >= 0),
    erased_at INTEGER CHECK(erased_at IS NULL OR erased_at >= terminal_at)
);
CREATE TRIGGER task_retention_reopened AFTER UPDATE OF data ON tasks
WHEN json_extract(NEW.data,'$.state') NOT IN ('succeeded','failed','cancelled')
BEGIN
    DELETE FROM task_retention WHERE task_id=NEW.id;
END;
CREATE TABLE task_retention_messages (
    id TEXT PRIMARY KEY REFERENCES link_chat(id),
    task_id TEXT NOT NULL REFERENCES tasks(id)
);
CREATE INDEX task_retention_messages_task ON task_retention_messages(task_id);
PRAGMA user_version=19;
