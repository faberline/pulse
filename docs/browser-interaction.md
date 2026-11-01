# Browser interaction work in progress

Work for [issue #28](https://github.com/faberline/pulse/issues/28), on top of draft PR #27. The full issue and D01-D10 goal remain open. This is an intermediate implementation record, not acceptance.

## Original screenshot delivery

The local process now saves PNG chunks and the read receipt in one journal transaction. It reserves enough outbox space before accepting browser work. A restart replays the stored chunks and receipt; it does not navigate again.

Each chunk is bound to the original read intent, device epoch, task and revision. The server accepts at most 128 artifacts, each at most 4 MiB. It checks chunk positions, exact byte lengths, metadata, final SHA-256 and PNG dimensions. Partial images are unavailable. A completed image can be downloaded only by a live, authenticated chat role after the read result is known. The endpoint accepts an operation ID and digest, never a filesystem path.

The chat Rust process downloads the image without blocking the connection loop. It checks the size, digest and dimensions before writing its private cache. The SwiftUI viewer checks the source cache path, rejects symlinks, verifies the hash and ImageIO type/dimensions, and refuses animated images. Source URL, title and observation time appear beside the image. A different task revision is marked as old evidence. Clicking the preview opens the original image within Pulse. Image bytes are not added to the model context.

Database schema is now 5. Migration 0005 adds artifact metadata, chunks and PNG storage. Old migrations are unchanged. The prior schema-three test fixture now correctly removes both later migrations before reconstructing schema three.

## Local evidence, 2026-11-02

All 120 normal Rust tests, including the existing 155 hard-exit samples, passed. All 13 Swift tests passed. Workspace clippy and the signed Mac bundle build passed. New tests cover multipart journal upload, duplicate frames, partial reads, exact source metadata, wrong epoch/task/revision/digest, size limits, authenticated HTTP download, revoked/incorrect roles and the viewer's hash/path/dimension/symlink checks. These tests use local fixtures, without API calls or Chrome.

A separate real SwiftUI/server/local Chrome run sent a URL task through the single composer. The page contained an unseen random marker. Luna used two calls and one read; local estimates settled at 379 micro USD. The original 20,290-byte PNG in the server matched the chat cache byte for byte. The preview and original-size sheet visibly showed the correct marker. The model correctly stated that it had received text only. Test processes were stopped and reaped. Evidence: `/private/tmp/pulse-browser-native-wjdvmpx_`.

Hosted checks are still required for the final issue #28 candidate. Screenshot delivery does not yet enable browser effects.

## Next required implementation

Keep a bounded local browser session alive for interaction. Bind its element catalog, source image and one-use observation nonce to the task, revision, device epoch and expiry. Add native approval/rejection cards. Consume the exact core approval and enqueue the corresponding local work in one transaction. Recheck the actual session, URL, element, form data and picture before click or text input. Record a known receipt or preserve Unknown without repeating the effect. Complete the actual UI plus countable browser fixture acceptance from issue #28 before opening the finished PR.

Page scripts stay disabled. Any HTTP form write needs a specific, consumed grant for the exact destination and body. Do not relax the read-only proxy globally. Text input must reject control characters and WebDriver private key codes that could submit a form. macOS computer operation remains a separate required part of the full goal.
