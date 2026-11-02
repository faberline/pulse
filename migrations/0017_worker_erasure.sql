ALTER TABLE memory_erased_reads ADD COLUMN context TEXT CHECK(context IS NULL OR json_valid(context));
ALTER TABLE link_device ADD COLUMN worker_erasure INTEGER NOT NULL DEFAULT 0 CHECK(worker_erasure IN (0,1));
CREATE TABLE link_erased_work (
    device TEXT NOT NULL, sequence INTEGER NOT NULL,
    context TEXT NOT NULL CHECK(json_valid(context)),
    PRIMARY KEY(device,sequence),
    FOREIGN KEY(device,sequence) REFERENCES link_outbox(device,sequence) ON DELETE CASCADE
);
CREATE TABLE link_worker_fences (
    operation_id TEXT PRIMARY KEY REFERENCES memory_erased_reads(id),
    device TEXT NOT NULL, sequence INTEGER NOT NULL,
    FOREIGN KEY(device,sequence) REFERENCES link_outbox(device,sequence)
);
PRAGMA user_version=17;
