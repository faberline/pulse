# Local withdrawal recovery candidate

Rust Core now keeps bodyless delivery evidence in the existing current memory restriction file. This closes the local database-only restore gap for recorded cancellations and deletions. Swift remains a display and input layer. Memory is still excluded from model prompts. The full D06, D08 and K15 requirements remain open.

## Order and identity

The Rust chat connector resolves a Human cancellation to exact source UUIDs and an optional known memory revision. It releases the planning transaction before Core takes its keeper lease. Core saves the original bodyless request before changing SQLite. It clears the matching local payloads, queues that same request and checks WAL cleanup before returning success.

The external record stores the original cancellation UUID, source time, target UUIDs, optional memory reference and delivery phase. It stores no label, body, token or pairing code. It uses the existing keeper, device, server fingerprint, copy generation, serial, private atomic file replacement and file lock. There is no extra service or task database.

On reopen, Core restores pending withdrawal requests before the journal can send old sources. A database snapshot from before cancellation cannot remove this evidence. Replays keep the same request UUID, time and payload. They do not choose a newer revision or expand the cancellation target.

Server acknowledgments update the external phase before SQLite. A generic stored result cannot acknowledge a revocation. Only the current credential fingerprint may consume a pending delivery. After a local database restore, completed withdrawals still block their old source bodies. They do not need a new POST. Fresh server views are filtered by exact source and known revision. A newer independent Human source remains usable.

If a withdrawal file write succeeds but SQL fails, the running journal refuses memory access. A later valid bind or reopen applies the saved evidence. The app does not report successful erasure while a checkpoint is busy. Old positive requests cannot reuse a withdrawn source UUID or a cancellation UUID.

## Bounds and upgrade

Format 4 retains up to 10,100 withdrawal records, including completed records. At most 10,000 records and 10,000 distinct sources may lack a known memory reference. It reserves record slots and 512 KiB for known-memory deletion. It permits 100 pending withdrawals and 20,100 total distinct target source UUIDs. The existing 8 MiB file limit still applies. The local journal retains its 1,000-row bound. New positive sources retain their separate pending limit. Capacity failure does not discard existing withdrawals.

Completed records remain as restrictions against old database copies. Acknowledgment frees pending space, but does not erase the restriction or renew source time. Reconciliation uses bulk SQL for completed records. No filesystem scan or model call is added to the idle UI timer.

An older journal's remaining bodyless pending requests are captured during upgrade. Invalid requests or wrong journal bindings cause refusal. Older applications which only read format 3 refuse format 4. The current restriction file must stay with a database restore. A pre-upgrade snapshot which has already lost an unrecorded cancellation cannot provide that missing evidence.

## Evidence and remaining limits

`memory_chat::tests::withdrawal_tests` checks snapshots before cancellation, original-request replay, completed receipts, stale views, UUID aliases, new independent sources, changed claims, wrong origin and scope, stale credentials, SQL failure, format-3 import and capacity. Its parent process also requires exit 73 from an actual child process after durable evidence and failed SQL, then checks recovery and physical removal from the current journal files.

These are Core and real loopback HTTP tests. They do not accept the macOS GUI, Keychain, a formal restore tool or closed-copy cleanup. Rolling back both the database and its current restriction file still cannot prove freshness. Server-wide restore, task/source dependencies, derived-content removal and memory use in model context remain separate required work. Current-head CI must also pass before acceptance.
