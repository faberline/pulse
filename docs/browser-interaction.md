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

Hosted checks are still required for the final issue #28 candidate. The evidence above covers the earlier screenshot delivery commit only.

## Source-bound approval core

The core now has typed browser observations, element/form data and click/text intents. A browser intent requires an explicit `browser-interact:` task scope. A plain browser-read task cannot gain action permission. The core checks the original completed observation receipt, its linked device Tool event, device epoch, task revision and original PNG. The image hash and observation must still match when consuming approval. Text rejects control characters and WebDriver private key codes.

`propose_operation_in`, `prepare_approved_operation_in` and `record_receipt_in` let the server share a transaction with its durable checkpoint and dispatch. A forced transport write failure proves that approval consumption, operation creation and the tool counter roll back together. Four new core tests cover scope/source/nonce/image/epoch failures, source-link removal after approval, one-use dispatch and submit-key injection. Together with the artifact work, 124 normal Rust tests and workspace clippy passed locally. Those checks cover the core boundary. The later integration below adds product dispatch.

## Browser action integration, candidate in progress

The local operator must set `PULSE_BROWSER_INTERACTION=1` as well as the existing `PULSE_BROWSER_DRIVER` path when starting `pulse-executor`. This setting belongs to the local process environment. It is not a server setting or a model argument. Without it, the local process advertises browser reads only. See [browser setup](browser-chat.md#use) for the driver and private-profile boundary.

The Human can request `操作網頁 <URL> ...` in the single composer. This creates an explicit `browser-interact:` scope. A read request cannot gain this scope from a model reply or page content. The local process must announce interaction support on its current authenticated connection.

`browser_inspect` opens a private, guarded Chrome session. The local process keeps at most two sessions for separate tasks. Each session has a 120-second lifetime. The observation includes a new nonce, the page URL, the PNG hash and at most 16 supported controls. The model receives typed control data and page text. It does not receive PNG bytes, driver commands, scripts or an approval grant.

The model can propose `browser_click` or `browser_type` for an observed element. Each proposal stops the task at `waiting_approval`. The Swift card shows the original image, target, action, form URL/method/fields or replacement text, and expiry. Approval requires a current task revision, a live chat connection and a verified source image. A pending decision disables a second click. The Human can reject or withdraw a grant. The core checks the original Human event and owner again before consumption.

The server consumes approval, writes the operation, increments the tool count, saves the run checkpoint and enqueues the local work in one transaction. Backpressure rolls the whole transaction back. The grant binds the operation, run, task, revision, device epoch, source observation, intent hash and expiry. The local process checks all of them. It then checks the current URL, title, screenshot and control catalog before the first effect command.

Only fixed WebDriver commands can click or replace plain text. Input rejects control characters and WebDriver special key codes. Page scripts stay disabled. Form support is limited to same-origin URL-encoded GET/POST forms with bounded plain fields and explicit UTF-8 `accept-charset`. Unsupported controls, external form associations and base targets exclude the form from the catalog. Automatic direction fields, charset fields and hard line wrapping are also excluded. This avoids showing incomplete form data as approved data. A form without explicit UTF-8 is unavailable in this candidate, even if its page normally uses UTF-8.

An HTTP POST needs one exact local proxy grant for its URL and encoded body. Wrong URLs, bodies, framing, expiry and replays cannot use it. Chrome can return from a click before its network request reaches the proxy. The executor waits up to two seconds for that same request. It never sends a second click. HTTPS payloads remain encrypted in the CONNECT tunnel. Their boundary is the disabled page scripts and fixed, verified form command; the proxy does not claim to inspect TLS form bodies.

A known receipt resolves the operation and can continue the same model run. A pre-command rejection records that no effect was issued and stops the task. A timeout or error after an effect command preserves Unknown. The journal never repeats accepted/running work after restart. A cancellation, revised task, expired session, new connection epoch or local stop invalidates old local work. An old receipt can end uncertainty. It cannot complete a new goal or start the old model run again.

Execution versions are `pulse-remote-9`, `effective-read-4`, `pulse-read-4` and `human-browser-actions-1`. Checkpoint schema is 1. Database schema is 5. Old migrations are unchanged.

The later scoped computer-read candidate uses `pulse-remote-10` and `human-computer-read-1`. It preserves this browser path. The native browser evidence recorded here was produced at the earlier candidate, and still needs the real SwiftUI/provider approval acceptance.

## Candidate checks, 2026-11-02

Four server integration tests cover approval cards and their source image, no dispatch before approval, one-use dispatch, duplicate results, expiry, rejection, revision, cancellation, restart, outbox rollback, known/unknown receipts and interruptions after dispatch. Provider tests reject forged grants, effects and special submit keys. A proxy test sends a delayed valid request once, then verifies replay, wrong URL/body, invalid framing, withdrawal and expiry. A Swift test verifies the approval button's task, image, connection, expiry and pending-decision gates. The Swift suite has 14 tests.

The workspace suite passed 130 normal Rust tests, including its existing 155 hard-exit child samples. Workspace clippy, Rust formatting, 14 Swift tests and the signed debug app bundle passed. The expanded Human-scope routing test also passed separately after adding interaction-scope cases. Hosted checks for this candidate remain pending.

`cargo run --locked -p pulse-link --example browser_interaction -- <absolute-chromedriver-path>` runs a real Chrome component fixture. Its model and Human decisions are controlled test inputs. This is distinct from the pending full desktop/provider acceptance.

The native fixture observed counter 0 before approval and counter 1 after one approved POST. It then replaced one text field without submitting again. Nine forged bindings and a repeated grant did not change the counter. Two concurrent task sessions had separate identities. One task could not borrow the other's observation. Rejecting the second left the first approval intact. After fixing the click/network timing race, this whole component test passed three consecutive times. Evidence directories: `/var/folders/r_/zwj_t8895ms334wn1pq5gf_w0000gn/T/pulse-browser-interaction-XTgdeF`, `pulse-browser-interaction-3rZRnr` and `pulse-browser-interaction-XTxCno` under the same temporary root.

Native `reject` and `stale` modes each kept the counter at 0. The stale mode used a real HTML refresh to change the visible page after observation. Evidence: `pulse-browser-interaction-un7xbT` and `pulse-browser-interaction-wBYGxk` under that temporary root.

Native `effect` and `result` modes hard-exited with code 73 after one actual POST. The former cut precedes the journal result transaction. The latter follows it, before delivery. Each test closes its Chrome child before the executor process exits. `recover <fixture-directory> <cut>` reopened the on-disk journal twice. It found no work to execute. The first cut produced Unknown. The second preserved the completed result and PNG frames. Each fixture counter stayed 1. Evidence: `pulse-browser-interaction-neuHhm` and `pulse-browser-interaction-BT7KFG`. These are process-loss tests, not power-loss tests or a full server/network outage.

These component tests make no provider API calls. They do not replace the issue's full SwiftUI + server + local process + real-provider acceptance. The Mac was locked when that UI check was attempted. The waiting fixture had zero tasks and zero model calls and was stopped. No result is claimed for its approval buttons.

After the form encoding check was tightened, a new native suite passed all 12 cases: seven unsupported forms, rejection, changed view, both process-loss cuts with journal recovery, and the full two-task click/type fixture. None of the seven unsupported form buttons was exposed for approval. Their counters stayed 0. The supported form still sent once. Case logs and the suite index are in `/private/tmp/pulse-browser-action-final-native.json`. This suite remains component evidence with controlled decisions.

## Remaining acceptance

Run the actual single-composer workflow with both configured providers. Check the real source image and approval card in SwiftUI. Approve only the owned counter fixture. Verify the counter and journal, including rejection, stale view, local disconnect and restart. Run checks at the final candidate head and verify its hosted CI. Keep issue #28 and D03/D05 unaccepted until those checks pass. Actual macOS computer operation remains a separate required part of D04. The full D01-D10 goal remains active.
