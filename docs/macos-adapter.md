# Rust macOS adapter candidate

SwiftUI is a replaceable input and display layer. Rust owns the task system and native execution.
This candidate implements the U13 boundary. Actual native UI acceptance remains open.

## Code boundary

| Component | Work |
|---|---|
| `pulse-core` | Shared target, source, effect and approval rules. Pulse's own approval UI is never a native tool target. |
| `pulse-link` | Pairing, task journal, scoped local permits, one-use grants, bounded child processes and result delivery. |
| `pulse-macos` | Rust `pulse-computer` child. Exact app/window discovery, AX reads, single fixed AX effect, screenshot and private PNG. |
| `PulseDesktop` | Composer, picker, original image, approval display guards and explicit Human commands. |

The adapter is an internal executor child. It is not a service or another task store.
Existing `unsafe_code=forbid` rules remain on Core and Link.
The new crate denies unsafe code outside its `os` module. That module contains the Apple FFI boundary.
It uses the [objc2 bindings](https://github.com/madsmtm/objc2), [Accessibility](https://developer.apple.com/documentation/applicationservices/1462085-axuielementcopyattributevalue) and [ScreenCaptureKit](https://developer.apple.com/documentation/screencapturekit/scscreenshotmanager).

## Local picker protocol

Any frontend can use `list_computer_windows` and `cancel_computer_windows` on the Rust chat pipe.
Each request carries a random `request_id`. Replies keep that ID in `NativeState.computer_windows`.
Rust chat asks the local executor through the existing private Unix socket.
The socket checks peer UID and exact server origin. Inventory uses a version 3, newline-terminated frame.
The version 2 selection/probe frame remains supported.
The version 1 permission frame adds local Human status and request controls. Closing the stream cancels a call.
Opening the picker checks status without prompting, then reads inventory if allowed. Separate buttons request the system prompt.
Rust checks both sides of the private channel. The request never reaches a server or model tool.

The executor permits one bounded native child. It passes only fixed operator filters to that child.
Filters apply before window titles are read. The helper returns at most 64 choices and a bounded JSON reply.
Choices do not create a task permit. Choosing a window still requires the existing live identity probe.
The list stays local. It is never stored in task history, sent to the server or added to model context.
Closing a picker drops its request stream. The executor then kills and reaps its owned list child.
Changing a request generation cancels the prior request. Old replies cannot fill a new picker.

## Native effect boundary

The helper protocol is `pulse-computer-observe-6`. The caller rejects older helpers.
Requests use the shared Rust source and intent types. Extra fields on fixed effects are rejected.
The child checks the selected process launch time, window, AX owner, frame, text and PNG before an effect.
It skips secure fields and their children. It offers only AX press and bounded plain-text replacement.
It does not use coordinate fallback, synthetic keys, scripts or shell.
Only the separate local Human control path can request an OS prompt; no tool operation can do so.

The Rust caller still owns the durable Human grant. Helper input is not a grant.
The child sends one command. An AX failure or missing result remains Unknown and cannot trigger a retry.
Success needs a fresh, valid result image. The private cache rejects symlinks, hardlinks, bad modes,
tampered files, FIFO entries and capacity overflow. Publication cannot overwrite an existing artifact.

## Evidence and remaining work

The former seven Swift helper tests moved to Rust contract and cache tests.
Rust process tests cover inventory scope/capacity, wrong origin, extra input, cancellation and child reaping.
Swift tests cover the picker state format and original-image approval display.
The build script takes `pulse-computer` from Cargo output. Swift no longer builds an execution helper.
Set `PULSE_APP_OUTPUT` to a new path to build a separate review bundle. The script refuses to overwrite that path.
The default output remains `target/debug/Pulse.app` with `--debug`, or `target/release/Pulse.app` otherwise.

A direct command-line status check reported no Accessibility or screen-capture grants in that launch context.
This is not proof of the GUI app's permission state. No real capture or effect was accepted from that check.
Actual owned-fixture capture, counters/text, original-image approval, both providers and restart faults remain required.
The new Human permission UI and OS prompt flow are also unaccepted until checked on the machine.
The Human reported no prompt and a closed Pulse window in the isolated test scene.
That UI process exited with code 0. Its old logs do not establish which step ran.
Local diagnostics now record fixed UI lifecycle events, permission queue status,
and the Rust request/helper stages. They omit chat content, paths, window titles and credentials.
These records help locate a failure. They do not prove a prompt appeared or a grant was made.
Passing builds, contract tests and CI does not accept D04, D05, D08 or the full D01–D10 goal.
