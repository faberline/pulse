CREATE TABLE link_pairings (digest TEXT PRIMARY KEY, expires INTEGER NOT NULL, used INTEGER NOT NULL DEFAULT 0);
CREATE TABLE link_device (id TEXT PRIMARY KEY, token_digest TEXT NOT NULL, revoked INTEGER NOT NULL DEFAULT 0, epoch INTEGER NOT NULL DEFAULT 0, lease_until INTEGER NOT NULL DEFAULT 0, in_cursor INTEGER NOT NULL DEFAULT 0);
CREATE TABLE link_inbox (device TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT NOT NULL, digest TEXT NOT NULL, outcome TEXT, PRIMARY KEY(device,sequence), UNIQUE(device,id));
CREATE TABLE link_outbox (device TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(device,sequence));
CREATE TABLE link_reads (id TEXT PRIMARY KEY, device TEXT NOT NULL, epoch INTEGER NOT NULL, task_id TEXT NOT NULL, task_revision TEXT NOT NULL, request TEXT NOT NULL, state TEXT NOT NULL, result TEXT);
CREATE TABLE link_chat (id TEXT PRIMARY KEY, body TEXT NOT NULL, created_at INTEGER NOT NULL);
PRAGMA user_version=2;
