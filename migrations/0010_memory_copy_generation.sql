ALTER TABLE local_memory_access ADD COLUMN generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0);
ALTER TABLE local_memory_access ADD COLUMN serial INTEGER NOT NULL DEFAULT 0 CHECK(serial>=0);
PRAGMA user_version=10;
