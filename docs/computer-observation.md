# Native macOS observation, in progress

This is preparation for D04. The full D01–D10 goal remains active. It does not pass computer-operation acceptance. Browser issue #28 still needs its real SwiftUI/provider approval checks.

## Current implementation

Cargo builds `pulse-computer` from `crates/pulse-macos` as a local native helper. The main app bundle includes and signs it. The helper has permission status, selected-window identity probe, observation, fixed Accessibility press and plain-text replacement commands. Both providers can now propose those effects after a current observation. The same composer shows the original image and a Human approval card. Server dispatch, local execution and original result-image receipts use the existing durable queue. There are no script, shell, Apple Events or keyboard-event commands. A separate local Human-only permission request path is described below. The Rust local executor owns its bridge. The journal saves the goal and local Human selection intent together. A private local channel probes the selected target and admits its read scope. This candidate still needs actual UI, capture and effect acceptance.

The helper uses Apple's [Accessibility API](https://developer.apple.com/documentation/applicationservices/1460720-axisprocesstrusted) and [ScreenCaptureKit screenshot API](https://developer.apple.com/documentation/screencapturekit/scscreenshotmanager/captureimage(contentfilter:configuration:completionhandler:)). It checks [screen capture access](https://developer.apple.com/documentation/coregraphics/cgpreflightscreencaptureaccess()) before capture. Status, inventory, probe, observation and effects never request permissions. A positive preflight is a prerequisite, not proof that an observation will succeed.

## Human permission controls

The picker displays the Rust executor's current Accessibility and screen-capture flags.
Opening the picker checks status without prompting. It then loads inventory if screen capture is allowed.
Rust skips window inventory when screen capture is missing.
The Human can press a separate button for each missing permission, then press Check Again.
Swift only displays status and sends these explicit input commands.

Rust chat handles the frontend's `computer_permissions` command, then sends a `permissions` frame over the private local channel, not the server.
The version 1 permission frame binds an exact server origin and a UUID request ID.
It permits only `accessibility`, `screen_capture`, or a status-only check.
The helper command `--request-permission` is eligible only for this local Human path.
It is absent from `HumanAction` and model `ReadOp`. No task permit or operation grant is created.
The channel rejects replayed prompt UUIDs, including case aliases, and allows at most 64 per executor lifetime.
Prompts are never put in a journal, retried on reconnect, or requested by startup and CI.

For Accessibility, Rust uses [AXIsProcessTrustedWithOptions](https://developer.apple.com/documentation/applicationservices/1459186-axisprocesstrustedwithoptions) with the prompt option.
Apple documents that this prompt is asynchronous and does not change the returned trust value.
For screen capture, it uses [CGRequestScreenCaptureAccess](https://developer.apple.com/documentation/coregraphics/cgrequestscreencaptureaccess()).
The response always queries actual permissions again. A submitted request is not an OS grant.
The Human handles the system prompt or settings. No agent may click an OS permission grant.
Closing the picker cancels the local call and reaps its child. It cannot undo an already opened system prompt.
The Human must check again after a grant. A helper timeout is not proof that no prompt was shown.
Permission identity depends on the signed helper and responsible parent. A CLI result is not GUI permission proof.
The new signed bundle and actual Human prompt/grant flow still require machine acceptance.

The local Human selects an exact bundle ID, process ID, launch time and window ID. A server or model request cannot change that local selection, choose a helper executable or supply a private path. An effect needs a separate Human approval of its exact source and intent. The target must be running. ScreenCaptureKit's window must have that exact owner, bundle and window ID, be on screen, and have normal window layer 0. An Accessibility window must uniquely match its title and frame. Missing, ambiguous or changed targets are rejected. The selected window can be in the background so that using the composer does not itself invalidate an observation. The helper never activates an app, launches a target, returns an app inventory, or falls back to a whole-display screenshot. The fixed effect checks below add to these read rules.

The observation records the selected target, process launch time, task/revision/device/operation metadata, bounded Accessibility text/control data, exact PNG hash/dimensions and a fresh nonce with a 120-second lifetime. It compares window identity, frame and the Accessibility catalog before and after capture. Accessibility messages use a process-local 250 ms timeout. The observation has an eight-second deadline, and the one-shot helper has a 15-second process watchdog. The parent must close stdin.

The catalog visits at most 128 nodes with depth 8 and exposes at most 16 supported button/text targets. Secure text field values, labels, actions and descendants are skipped. Long or uncounted text inputs cannot become text targets. The catalog is at most 32 KiB. The PNG is at most 4 MiB, 8192 pixels per side and 16 million pixels. The image stays in a local private directory. The helper uses owner-only file modes, descriptor-relative writes, no-follow opens, exact cached-byte checks and a 128-file limit. A failed new write removes its partial cache entry.

The request binding is a validated metadata format. This helper does **not** authenticate a Human authorization event. Its local path field is private IPC data. The server authenticates the chat role, saves the Human selection event and checks the exact task scope before dispatch. Only bounded metadata and verified PNG bytes cross the local boundary. A control's `can_press` or `can_set_text` field reports an observed OS capability. It does not authorize or execute an effect.

## Local checks, 2026-11-02

Earlier observation-stage evidence had 177 normal Rust tests and 25 Swift tests passing locally. Seven native contract/storage tests cover the read-only request surface, invalid probe/binding/expected-launch formats, source/target limits, UTF-8 bounds, cache tampering, modes, symlinks, FIFO entries and capacity cleanup. The desktop tests also check immutable selection/retry metadata, computer image sources and cross-source browser approval denial. They do not prove the actual picker, target identity probe or capture succeeds with macOS permissions.

The signed helper's real process check passed with no capture. It checked permission status, rejected a nonexistent target and refused click/type/script or oversized input. `scripts/check-computer-helper.py` runs the same no-capture check in macOS CI. The owned fixture app also built and passed ad hoc signature verification. It was not launched while the Mac was locked.

An earlier local preflight returned Accessibility=false and screen_capture=true. Evidence is in `/private/tmp/pulse-computer-preflight-t10g676s`. Permission identity can depend on the signed app and its responsible parent process. That probe is not a grant for another process. No permission was requested or changed.

## Rust local bridge

`ComputerRuntime` uses only the fixed `pulse-computer` sibling inside the local executor's app bundle. No model or server argument can select another executable. It starts a separate process group, clears the inherited environment, sets only a fixed PATH and local HOME, writes one bounded request, closes stdin and bounds stdout/stderr. A permission status probe takes at most three seconds. A Human request or observation call takes at most 18 seconds; the Rust helper retains its own 15-second watchdog. One shared permit limits a runtime to one child at a time. Cancellation, timeout and output-limit failures stop the owned process group. Normal exits are reaped, and their old process IDs are not signalled again.

The Rust boundary checks the native protocol version, exact operation/task/revision/device/epoch, configured bundle/process/window, process launch time, capture time, nonce, control IDs and count, and image metadata. It accepts only the expected hash-derived cache entry. It refuses symlinks, non-regular files, wrong owner/mode, length/hash/dimension mismatches and stale observations. Only typed metadata and verified PNG bytes leave the local boundary. The private native cache path is removed from that metadata. The evidence object has no automatic serializer for image bytes.

An operator can still restrict the picker to one target through `PULSE_COMPUTER_BUNDLE_ID`, `PULSE_COMPUTER_PID` and `PULSE_COMPUTER_WINDOW_ID` in the local process environment. All three must be set together. With no operator target, only an explicit local Human picker selection can admit a task. With an operator target, that additional restriction remains enforced. A task/revision/selection without a live local permit stops before spawning a capture child. At artifact capacity it also stops before spawning. The existing local executor opens this optional runtime. Repo and browser tools keep their existing dispatch paths.

The core has typed `ComputerObservation` metadata with canonical string counters, bounded frame/control data and a path-free image reference. These are format and source checks, not a new authorization mechanism. The original observation candidate used `pulse-remote-13`, `effective-read-6`, `pulse-tools-6`, `approved-computer-effects-1` and schema 5. The task memory source candidate first used `pulse-remote-14`, `effective-read-6`, `pulse-tools-6`, `approved-effects-memory-fences-2` and schema 14. The derived chat erasure candidate first used `pulse-remote-15`, `approved-effects-memory-fences-3` and schema 15; see [derived chat erasure](derived-chat-erasure.md). The tool-result erasure candidate first used `pulse-remote-16`, `approved-effects-memory-fences-4` and schema 16; see [tool result erasure](derived-tool-results.md). The worker erasure candidate now uses `pulse-remote-17`, `approved-effects-memory-fences-5` and schema 17; see [worker source erasure](worker-source-erasure.md). This change fences earlier remote checkpoints. It does not accept earlier unfinished work or enable memory prompts. See [task memory sources](task-memory-sources.md).

## Composer and local Human selection

Press **程式視窗** beside the existing composer. The sheet lists at most 64 normal on-screen windows after a screen-capture preflight. It never requests an OS grant. The list stays in the UI; only the chosen window is attached to the draft. The same text box accepts the goal. If a plain text request asks to read an unselected window, the server asks for a selection instead of starting a repo task.

On Send, the chat child atomically saves the Human action, pending message and selection intent in the existing journal. This works while the server or local process is offline. It does not wait for an OS probe before saving the goal. The server can save the task, but a fresh model call still waits for its exact live local capability. A full outbox rolls back the entire intent. The unsaved draft stays available.

After the authoritative task snapshot matches, the chat child calls a private Unix socket owned by the independent local process. Selection channel protocol 2 distinguishes an explicit Human reselection from a retry. This channel admits a selection; it never executes a tool or carries task state. Its directory is owner-only and its socket is 0600. Both sides check peer UID. The socket name is derived from the configured server origin; the full origin is also checked in the bounded request. A live owner, symlink or ordinary file cannot be replaced. A stale owned socket can be recovered. Drop cleanup removes only the socket inode owned by this process. The operator-only `PULSE_LOCAL_CONTROL_DIR` can place this channel in a private test directory. It is not a server/model parameter or a new UI setting.

The local process probes the exact process launch, ScreenCaptureKit window and unique Accessibility window before admitting the task. The probe does not collect Accessibility contents or pixels. Local permits bind the task, revision and selection. At most two permits live in memory. Their 15-minute lifetime starts at the original Send timestamp; a retry cannot renew it. Task cancellation, deadline, revision, changed scope or a saved final model reply removes a permit when the local snapshot arrives. This releases the read scope without accepting the task. A local process restart clears permits. This is a permission cache, not another task database. Missing local connection or permission leaves the saved goal visible and waiting. It cannot start a fresh provider call. Unprepared intents retry at most once every 30 seconds with the original timestamp. The journal retains at most 32 selection entries for their fixed 15-minute lifetime. A prepared intent is never automatically granted again after a local process restart. Stop or lease loss also cancels an in-flight identity probe.

Capability probes run separately from the connection loop. A slow OS request cannot hold heartbeats, acknowledgments or cancellation. Nonempty capabilities refresh about every five seconds and expire at the server after 20 seconds without a refresh. Only the authenticated live local role and current connection epoch can report them. Before a fresh paid call and before dispatch, the server requires a free local process with that exact task/revision/window permit. Known image evidence can still be summarized offline. The local runtime checks its permit again before and after capture. An in-flight read is interrupted if its local permit is withdrawn.

The UI remains one composer and never owns the local process lifetime. A queued, waiting or undispatched approval task has a **重新選擇視窗** action. This opens the same picker and names the existing task. The saved command keeps the task ID, revision and original selection time on retry. Choosing the same scope keeps its revision only while queued or waiting for a device. Pending/approved but undispatched actions, saved final replies and known-not-executed failures use a fresh revision, even for the same window. This invalidates old approvals and requires a fresh inspect. Choosing a new process/window scope uses the existing Core revision command. That command keeps the goal, cost ledger, counters and deadline, and invalidates old approvals. Local preparation waits for the new authoritative scope before probing. An old permit cannot replace a newer revision or roll back a newer Human selection.

Cancelled, expired, dispatched, reconciling and outcome-unknown tasks cannot use this action. Recovery never clears an unknown operation or dispatches it again. Controlled journal, server, socket and Swift tests cover these rules, including nonzero recorded usage and full-outbox rollback. Actual UI and local/server restart acceptance remain part of D08. This change does not migrate older execution-version checkpoints.

## Scoped read and original image transport

The authenticated chat action `submit_computer` saves the exact bundle, PID, window and process launch time as a Human-selected scope. Its scope has one canonical form. A local-role message cannot create that Human event. A later authenticated Human or model read must name the current task/revision and the same selection, and pass local capability admission. Another app, a reused process instance, a repo tool, a cancelled task or a stale revision cannot borrow it.

`computer_inspect` checks the selection against the local permit and any operator restriction before starting the native child. Native protocol `pulse-computer-observe-6` also receives the expected launch time. The Rust macOS adapter compares it before collecting Accessibility data or taking a screenshot. The caller checks it again in the result. This inspect request does not grant permission or perform effects.

Computer PNG metadata has an explicit computer source with the work binding and selected process instance. It has no browser URL or private path. Both the local journal and server compare the exact operation, task, revision, device epoch, process, window, title, capture time and PNG metadata. A completed observation requires the full verified original PNG. The server cannot publish a partial image or a result from another tool kind. Retained evidence can outlive the 120-second action lifetime. Saving old evidence does not renew that lifetime.

The local result and image chunks commit together in the existing bounded outbox. Reopening the local journal resends those bytes without taking another screenshot. The server retains the original source and PNG after database reopen. Only a live chat credential can download it. The Swift image view labels it as a program window, verifies the source and original bytes, and cannot use it to approve a browser action.

The provider tool schema fixes the exact selected window and process instance. The fixed press/type proposals have no script, path or approval-grant arguments. Computer tasks wait before a fresh model call when capability is missing or stale. Only a dispatched Human grant can reach computer press/type execution. Controlled process, transport and Swift tests cover this path; they do not prove actual macOS capture or a finished single-composer workflow.

Use the actual Rust local process for a no-pairing/no-capture probe:

```sh
target/debug/Pulse.app/Contents/MacOS/pulse-executor --computer-status
```

This returned `installed=true`, `accessibility=false`, `screen_capture=true` locally. The signed Rust → Swift process check passed. Controlled process-fixture tests cover selection release, source and PNG verification, bound executor dispatch, 20 corrupt/forged result cases, oversized output/diagnostics, bad exits, timeout, cancellation/reaping, concurrent work limits and pre-spawn denial. Four private socket tests cover probes, per-task grants, capacity, wrong origin, expiry, slow-probe cancellation/reaping, stale socket recovery, owned cleanup and refusal to replace files. Two server admission tests cover missing/stale/old-epoch and borrowed capabilities, zero model reservations while waiting, and one scoped model dispatch. Four core tests cover metadata limits, canonical scope/counters, retained evidence and permit lifetime. Four transport tests cover scope, original images, reopen/replay and authenticated HTTP download. These are controlled process/format/transport checks, not actual app capture or effect acceptance.

## Approved native actions, awaiting actual acceptance

Core now accepts a typed `ComputerIntent` through the existing operation-request tables. The proposal must retain a completed computer-inspect receipt, its linked Tool event and the exact original PNG. Scope, process launch, task/revision, device/epoch, nonce, image, frame, control catalog and intended effect must agree. The action uses the existing Human approval, rejection, withdrawal, revision and cancellation rules. Approval consumption and delivery belong in one transaction. A replay cannot consume the same grant twice. A dispatched unknown action remains unknown after reopen/recovery. No new database or task service was added.

The Rust adapter admits a dispatched grant only when its exact original observation still exists locally. At most two observations are retained in memory. They are cleared on disconnect/restart and pruned with local selections. It checks the private original PNG again and removes the source observation before child I/O. A second call cannot reuse it. An action waits up to three seconds for a background capability probe to release the shared helper gate. It then rechecks its grant and selection before consuming the view. Local stop, selection withdrawal and expiry interrupt the owned child. An interrupted or unverifiable call is Unknown. It cannot be reported as a safe rejection after a command may have started.

Native protocol 4 adds `act`. Only this request may use 64 KiB; status, probe and observe retain their 16 KiB limit. Its view is path-free. Nested unknown fields, script/keyboard arguments, secure targets and text containing control codes are rejected. The native helper does not authenticate a Human. Its trusted Rust caller must validate the server grant and mark the existing journal work running before calling it.

The helper recaptures the exact selected process/window. The original image hash, frame, title, catalog and process launch must still agree. It resolves the bounded Accessibility path, checks every ancestor's owner and secure-field status, then checks the catalog, permissions and expiry again. It issues one fixed [`AXUIElementPerformAction`](https://developer.apple.com/documentation/applicationservices/1462091-axuielementperformaction) for press or [`AXUIElementSetAttributeValue`](https://developer.apple.com/documentation/applicationservices/1460434-axuielementsetattributevalue) for plain text. It never activates an app or sends submit keys. Apple's action API can time out after an app handled a command. Any error after sending the command is therefore Unknown and never retried. Success also requires a fresh, verified result image.

Controlled Core transactions cover source tampering, rejected/withdrawn approvals, cancellation, revision, expiry, reopen and atomic delivery rollback. Controlled native children cover press and Unicode text, one-use views, missing grants, stale bindings, bad images, restart, interruption and child reaping. Rust adapter tests now cover strict native request parsing. Swift tests cover display guards and protocol decoding. The signed helper check also sends a well-formed action for a nonexistent owned fixture and verifies NotExecuted without capture. These checks do not prove actual AX effects, real UI approval or correct fixture counts.

The provider schema, server proposal/dispatch/receipt path and Swift computer approval card are now connected. Both providers expose `computer_press` and `computer_type` only after a current, same-scope inspect. The schema fixes the selected process/window, source nonce and eligible control IDs. It has no model-supplied grant. After an effect, a fresh inspect is required. A direct Human `read` action cannot bypass effect approval.

The server reuses the browser operation transaction path. It checks the live exact local device/epoch, fresh selected-window capability and an explicit interaction-capability flag before consuming the grant. Old read-only capability reports cannot receive native effects. Local acceptance reserves space for the result image before marking work running. The original result PNG and receipt commit in the same journal outbox. Reopen resends those bytes without another command. A late result after server restart remains evidence; it cannot turn an unknown dispatch into a second execution or an accepted task.

The existing composer card supports both resource types. A computer approval requires the current task/revision/scope, valid native intent, exact original process/window source and verified image digest, length, dimensions and capture time. A browser image cannot approve a native action, and a native image cannot approve a browser action. Pending approvals stay in the bounded snapshot even when another task adds newer chat messages. Rejection, withdrawal and cancellation retain the existing Human command path.

Six controlled workflow tests cover the real server, journal and Rust executor with mock native children: zero commands before approval, original-image projection, read-only capability denial, atomic dispatch rollback, one dispatch, lost ACK/local reopen, server reopen/late receipt, Unknown versus NotExecuted, direct-read bypass, rejection, cancellation, tampered source, pre-acceptance image capacity and same-task reselection of pending/approved work. Model schema tests cover both providers, fixed scopes/control IDs, denied grants/keys/scripts and fresh-inspect requirements. The Swift card test checks native source/task binding and changed or cross-resource images. These are integration checks, not actual macOS effects or hosted model/UI acceptance.

No paid call, real window capture, OS permission grant or actual press/text effect was performed for this integration. Current-head hosted CI is unrun while GitHub push approval is pending. D04/D05/D08 and the full D01–D10 goal remain unaccepted.

## Owned fixture

Build the fixture with `sh scripts/build-computer-fixture.sh`. This creates `target/debug/PulseComputerFixture.app`. It has a random marker, a visible counter, one count button, a Message field and a secure text field with a known test value. It reports only its own bundle ID, process ID and window ID on stdout. This is a separate test app, not a second product composer.

Check permissions without a target:

```sh
target/debug/Pulse.app/Contents/MacOS/pulse-computer --status
```

The native helper's internal caller passes `--bundle-id`, `--pid`, `--window-id` and `--cache`. Stdin contains one bounded `probe` or `observe` request, then EOF. User selection belongs in the composer; these are not user-entered fields. Do not use another app or profile as a test substitute.

## Remaining D04 work

1. Unlock the Mac. Run the actual owned fixture capture. Verify the marker, PNG, window identity and secure-field exclusion. Resolve the exact signed helper's missing permission through the Human, if needed. No agent may click an OS permission grant.
2. Run the real composer picker → local probe → provider inspect → original image workflow with the owned fixture. Verify rejected/ambiguous windows, missing permission, cancellation, two tasks, local shutdown while selecting, and same-task reselection after local restart. No grant is implied by compilation or mock probes.
3. Verify the connected press/text workflow with both providers and the actual composer. Recheck the actual app/window, process launch, image, catalog, nonce, task/revision, live lease, stop state and grant immediately before effect. Unknown effects must stop and never repeat. Keep local paths and Accessibility handles out of server/model input.
4. Run the actual single-composer UI/provider workflow and count effects. Test rejection, stale app/window, secure fields, wrong scope, cancellation, two tasks, permission loss, local/server restart, disconnected receipts and no duplicate effect. Verify exact-head Linux/Mac/coverage CI and native evidence before claiming D04 acceptance.

Actual native observation/effects, OS permission handling, model integration, the SwiftUI workflow and complete computer operation are still unaccepted. Compiling the helper or passing its contract tests does not replace them.
