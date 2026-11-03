ALTER TABLE link_device ADD COLUMN worker_cleanup INTEGER NOT NULL DEFAULT 0 CHECK(worker_cleanup IN (0,1));
CREATE TABLE link_worker_cleanup (
    operation_id TEXT PRIMARY KEY REFERENCES memory_erased_reads(id),
    receipt TEXT NOT NULL CHECK(json_valid(receipt)),
    received_at INTEGER NOT NULL
);
PRAGMA user_version=18;
