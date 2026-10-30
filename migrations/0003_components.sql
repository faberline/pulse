-- The old helper held both human and tool authority. Revoke those tokens.
-- Preserve all tasks, receipts and journals.
ALTER TABLE link_pairings ADD COLUMN role TEXT NOT NULL DEFAULT 'local_executor'
    CHECK(role IN ('chat', 'local_executor'));
ALTER TABLE link_device ADD COLUMN role TEXT NOT NULL DEFAULT 'local_executor'
    CHECK(role IN ('chat', 'local_executor'));
ALTER TABLE link_device ADD COLUMN stopped INTEGER NOT NULL DEFAULT 0;
UPDATE link_pairings SET used=1;
UPDATE link_device SET revoked=1, epoch=epoch+1, lease_until=0;
PRAGMA user_version=3;
