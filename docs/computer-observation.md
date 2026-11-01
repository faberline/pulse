# Native macOS observation, in progress

This is preparation for D04. The full D01–D10 goal remains active. It does not pass computer-operation acceptance. Browser issue #28 still needs its real SwiftUI/provider approval checks after the Mac is unlocked.

## Current implementation

The Swift package builds `pulse-computer` as a local native helper. The main app bundle includes and signs it. The helper has only a permission status command and a window observation command. There are no click, type, script, shell, Apple Events or permission-request commands. The Rust local executor now owns its bridge. Computer observation is not connected to server dispatch or model tools yet.

The helper uses Apple's [Accessibility API](https://developer.apple.com/documentation/applicationservices/1460720-axisprocesstrusted) and [ScreenCaptureKit screenshot API](https://developer.apple.com/documentation/screencapturekit/scscreenshotmanager/captureimage(contentfilter:configuration:completionhandler:)). It checks [screen capture access](https://developer.apple.com/documentation/coregraphics/cgpreflightscreencaptureaccess()) before capture. It does not call either OS permission-request API. A positive preflight is a prerequisite, not proof that an observation will succeed.

The local operator selects an exact bundle ID, process ID and window ID through process arguments. Requests cannot supply those arguments, a file path or an effect. The target must be running and frontmost. ScreenCaptureKit's window must have that exact owner, bundle and window ID, be on screen, and have normal window layer 0. An Accessibility window must uniquely match its title and frame. Missing, ambiguous or changed targets are rejected. The helper never activates an app, launches a target, returns an app inventory, or falls back to a whole-display screenshot.

The observation records the selected target, process launch time, task/revision/device/operation metadata, bounded Accessibility text/control data, exact PNG hash/dimensions and a fresh nonce with a 120-second lifetime. It compares window identity, frame and the Accessibility catalog before and after capture. Accessibility messages use a process-local 250 ms timeout. The observation has an eight-second deadline, and the one-shot helper has a 15-second process watchdog. The parent must close stdin.

The catalog visits at most 128 nodes with depth 8 and exposes at most 16 supported button/text targets. Secure text field values, labels, actions and descendants are skipped. Long or uncounted text inputs cannot become text targets. The catalog is at most 32 KiB. The PNG is at most 4 MiB, 8192 pixels per side and 16 million pixels. The image stays in a local private directory. The helper uses owner-only file modes, descriptor-relative writes, no-follow opens, exact cached-byte checks and a 128-file limit. A failed new write removes its partial cache entry.

The request binding is a validated metadata format. This helper does **not** authenticate a Human authorization event. Its local path field is private IPC data. Server integration must validate the core scope and source event, keep that path local, and publish only bounded, authenticated artifact data. A control's `can_press` or `can_set_text` field reports an observed OS capability. It does not authorize or execute an effect.

## Local checks, 2026-11-02

All 18 Swift tests passed. Four new tests cover the read-only request surface, invalid binding formats, source/target limits, UTF-8 bounds, cache tampering, modes, symlinks, FIFO entries and capacity cleanup. They do not require an installed target app or permission grant.

The signed helper's real process check passed with no capture. It checked permission status, rejected a nonexistent target and refused click/type/script or oversized input. `scripts/check-computer-helper.py` runs the same no-capture check in macOS CI. The owned fixture app also built and passed ad hoc signature verification. It was not launched while the Mac was locked.

An earlier local preflight returned Accessibility=false and screen_capture=true. Evidence is in `/private/tmp/pulse-computer-preflight-t10g676s`. Permission identity can depend on the signed app and its responsible parent process. That probe is not a grant for another process. No permission was requested or changed.

## Rust local bridge

`ComputerRuntime` uses only the fixed `pulse-computer` sibling inside the local executor's app bundle. No model or server argument can select another executable. It starts a separate process group, clears the inherited environment, sets only a fixed PATH and local HOME, writes one bounded request, closes stdin and bounds stdout/stderr. A permission probe takes at most three seconds. An observation call takes at most 18 seconds; the Swift child retains its own 15-second watchdog. One shared permit limits a runtime to one child at a time. Cancellation, timeout and output-limit failures stop the owned process group. Normal exits are reaped, and their old process IDs are not signalled again.

The Rust boundary checks the native protocol version, exact operation/task/revision/device/epoch, configured bundle/process/window, process launch time, capture time, nonce, control IDs and count, and image metadata. It accepts only the expected hash-derived cache entry. It refuses symlinks, non-regular files, wrong owner/mode, length/hash/dimension mismatches and stale observations. Only typed metadata and verified PNG bytes leave the local boundary. The private native cache path is removed from that metadata. The evidence object has no automatic serializer for image bytes.

The operator can select a target through `PULSE_COMPUTER_BUNDLE_ID`, `PULSE_COMPUTER_PID` and `PULSE_COMPUTER_WINDOW_ID` in the local process environment. These are internal setup values, not model arguments or a finished app-selection UI. All three must be set together. Without a selected target, observation stops before spawning a child. At artifact capacity it also stops before spawning. The existing local executor opens this optional runtime. Repo and browser tools keep their existing dispatch paths. There is still no computer operation kind on the server wire or in model tools.

The core has typed `ComputerObservation` metadata with canonical string counters, bounded frame/control data and a path-free image reference. These are format and source checks, not a new authorization mechanism. Authenticated Human scope and durable source/approval dispatch still have to be connected before any model can request a computer read or effect. Workflow versions and database schema are unchanged.

Use the actual Rust local process for a no-pairing/no-capture probe:

```sh
target/debug/Pulse.app/Contents/MacOS/pulse-executor --computer-status
```

This returned `installed=true`, `accessibility=false`, `screen_capture=true` locally. The signed Rust → Swift process check passed. Six process-fixture tests cover source and PNG verification, 20 corrupt/forged result cases, oversized output/diagnostics, bad exits, timeout, cancellation/reaping, concurrent work limits and pre-spawn denial. The fixtures record that a response was actually constructed, so an unrelated child startup failure cannot pass a source-denial test. Two core tests cover native metadata limits, canonical counters and private path exclusion. These are mock process/format checks, not actual app capture or effect acceptance.

## Owned fixture

Build the fixture with `sh scripts/build-computer-fixture.sh`. This creates `target/debug/PulseComputerFixture.app`. It has a random marker, a visible counter, one count button, a Message field and a secure text field with a known test value. It reports only its own bundle ID, process ID and window ID on stdout. This is a separate test app, not a second product composer.

Check permissions without a target:

```sh
target/debug/Pulse.app/Contents/MacOS/pulse-computer --status
```

After a Human has selected an owned test window, the local caller can pass `--bundle-id`, `--pid`, `--window-id` and `--cache`. Stdin must contain one bounded `observe` request, then EOF. This is an internal operator interface. App selection through the single composer remains to be implemented. Do not use another app or profile as a test substitute.

## Remaining D04 work

1. Unlock the Mac. Run the actual owned fixture capture. Verify the marker, PNG, window identity and secure-field exclusion. Resolve the exact signed helper's missing permission through the Human, if needed. No agent may click an OS permission grant.
2. Connect the native Rust bridge and path-free observation types to a Human-selected app/window scope and authenticated server artifact path. Add the single-composer app selection. Announce capability only after live native checks. Missing capability must wait without paid model calls.
3. Connect fixed Accessibility press/text commands to the same durable one-use approval and receipt boundary. Recheck the actual app/window, process launch, image, catalog, nonce, task/revision, live lease, stop state and grant immediately before effect. Unknown effects must stop and never repeat. Keep local paths and Accessibility handles out of server/model input.
4. Run the actual single-composer UI/provider workflow and count effects. Test rejection, stale app/window, secure fields, wrong scope, cancellation, two tasks, permission loss, local/server restart, disconnected receipts and no duplicate effect. Verify exact-head Linux/Mac/coverage CI and native evidence before claiming D04 acceptance.

Native observation, press/text effects, OS permission handling, Rust/server/model integration and complete computer operation are still unaccepted. Compiling the helper or passing its contract tests does not replace them.
