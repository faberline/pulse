# Browser read through chat

Candidate for [issue #26](https://github.com/faberline/pulse/issues/26). This advances D02, D03 and D08. The full Dot parity goal remains active.

## Use

Set `PULSE_BROWSER_DRIVER` to the absolute path of an installed, compatible official ChromeDriver when starting `pulse-executor`. Pulse does not download a driver. Without this setting, the executor keeps its repo tools. Browser tasks wait without model calls.

```sh
PULSE_BROWSER_DRIVER=/ABSOLUTE/PATH/chromedriver target/debug/pulse-executor --server https://YOUR-PULSE-SERVER/ --repo /ABSOLUTE/PATH/pulse --data-dir /PRIVATE/DIRECTORY/local
```

Pair the local process through the existing one-use pairing flow. Pair the chat separately. Then type in the single chat box:

```text
任務 網頁：瀏覽 https://example.com/ 請摘要這一頁並附來源
修改 網頁：瀏覽 https://example.org/ 改讀這一頁
取消 網頁
```

Only an explicit Human browser instruction selects a URL. A URL quoted in ordinary prose does not add browser permission. Task scope stores the canonical URL, including its web path, query and fragment. A revision can select a new URL. Old results remain evidence for their original revision.

## Boundaries

Browser tasks expose only `browser_read`. Its schema contains the selected URL. Repo tasks expose only the four existing repo tools. Both request history and server dispatch check the task scope. Model output cannot select a driver, profile, script, CDP command or system path.

The authenticated local process announces browser support after Welcome. This capability is tied to its current device epoch. An old local client receives no browser commands. A disconnected, stopped, revoked or unconfigured local process cannot dispatch browser work.

Each read creates a new private profile and a local proxy. Chrome must use that proxy, including for loopback. QUIC and page scripts are disabled. Non-proxied WebRTC UDP is disabled. Downloads are denied through a fixed browser command. Its fallback download directory is inside the temporary profile. Pulse does not open a personal profile.

The proxy checks every destination before connecting. It resolves a hostname once and rejects the whole result if any address is private or reserved. It connects to the checked address. Public traffic uses HTTP(S) on ports 80 or 443. Loopback, private IPs, local hostnames, embedded credentials and common secret query keys are rejected. Redirect and subresource requests use the same proxy. HTTP reads cannot forward request bodies or pipelined requests. Each session has bounded connections, bytes and time. Chrome is trusted to enforce its proxy and script settings; this is not an OS sandbox for a compromised browser binary.

`PULSE_BROWSER_FIXTURE_ORIGIN` is an optional **local operator** test setting. It accepts exactly one `http://127.0.0.1:PORT/` origin. Other loopback ports remain blocked. Neither a server request nor a model call can set this exception.

Navigation and observation take at most 35 seconds in total. Each driver response is at most 4 MiB. Page text is at most 32 KiB. Evidence includes the final URL, title, observation time, text, truncation flag and PNG SHA-256.

The current Rust runtime hands the PNG bytes to its caller. The local journal saves image chunks and the result in one transaction. Restart delivery uses that journal, without another navigation. The runtime no longer writes an extra `browser/artifacts` copy. Old files in that directory do not block new captures and are retained for source recovery. The response, PNG, session and journal limits still apply. Server and chat image limits remain separate; see [original screenshot delivery](browser-interaction.md#original-screenshot-delivery).

The model receives bounded text evidence. It does not receive screenshot bytes. The chat can show the original PNG through the verified image transport. Script-dependent pages can be incomplete. A read task does not permit clicks, typing, login or form submission. [Browser interaction](browser-interaction.md) needs its separate Human scope and approval.

The existing durable intent, journal, receipt, ACK and fencing rules apply. Completed operation receipts are replayed without another navigation. Unknown reads stop for Human review. Revision, cancellation and restart cannot restore an old run. During a read, loss of the local lease or the stop switch drops the browser future. An observed task revision or cancellation also stops it. Task updates arrive through snapshots; this is not an immediate per-task cancel channel. A missing snapshot entry does not itself prove cancellation. The 35-second read limit still applies. Dispatch and receipt checks enforce the server's task state independently.

The original read candidate used `pulse-remote-8`, `effective-read-3`, `pulse-read-3` and `read-only-3`, with checkpoint schema 1 and database schema 4. Later candidates are recorded in [browser interaction](browser-interaction.md) and [task memory sources](task-memory-sources.md). Incompatible in-flight runs remain stopped.

## Verification on 2026-11-02

Normal CI uses mock drivers and local proxy fixtures. It uses no API keys, Internet sites or installed browser. Tests cover resource separation, strict tool arguments, URL semantics, private ranges, proxy controls, free capability waiting, local role and epoch checks, two-task separation, duplicate frames and receipts, completed read identity, revision, cancellation, server recovery and local journal recovery. Existing oversize, timeout and crash tests remain in the workspace suite.

Native checks are separate:

```sh
cargo run --locked -p pulse-link --example browser_guarded -- /ABSOLUTE/PATH/chromedriver
```

The native check used Chrome 156.0.8078.79 and official ChromeDriver 156.0.8078.80. A random-marker fixture produced real text and PNG evidence. Cross-port private image/frame requests and a redirect reached zero forbidden endpoints. Page scripts did not run. Download navigation was rejected. A deliberately stalled navigation was cancelled, and its profile was removed. A public HTTPS page at example.com produced a title, URL, time, text and PNG. Evidence and cleanup checks are saved at `/var/folders/r_/zwj_t8895ms334wn1pq5gf_w0000gn/T/pulse-browser-guarded-4C6CLC`.

Three actual components were then run: the SwiftUI app, server and independent local executor. The native UI sent the URL task. The random page marker was absent from the Human request. Each provider read it through Chrome and answered through the same run. Each used exactly two model calls and one browser read. Both finished at `blocked / model_reply_ready`, awaiting acceptance. Their PNGs were inspected separately. The UI showed both answers and their source URLs. No screenshot was sent to either model.

- Luna: 373 micro USD in settled local estimates. Evidence: `/private/tmp/pulse-browser-native-7sty7vhv`.
- Haiku: 754 micro USD in settled local estimates. Evidence: `/private/tmp/pulse-browser-native-yizgubkk`.

Both native sessions were stopped and their three processes were reaped. These temporary paths are local evidence, not portable CI artifacts. The usage estimates are not provider invoices. Native paid tests are not part of normal CI.

Browser interaction, screenshot transport, macOS computer operation, complete fault acceptance, performance and the eight-hour soak remain unfinished. D03 and the full goal are not accepted by this candidate.

Browser configuration references: [Chromium proxy rules](https://chromium.googlesource.com/chromium/src/+/main/net/docs/proxy.md), [ChromeDriver options](https://developer.chrome.com/docs/chromedriver/capabilities), [Browser download control](https://chromedevtools.github.io/devtools-protocol/tot/Browser/#method-setDownloadBehavior).

Local fmt and workspace clippy passed. All 117 normal Rust tests passed, including the existing 155 hard-exit subprocess samples. All 12 Swift tests and the ad hoc signed Mac bundle build passed. Current-head hosted CI must be checked on the PR; local success does not replace it.
