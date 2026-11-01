import XCTest

@testable import PulseDesktop

final class DesktopTests: XCTestCase {
  func testRustTaskProjectionKeepsRevisionAsString() throws {
    let json =
      #"{"id":"task","goal":"中文","revision":"9007199254740993","state":"blocked","cancel_requested":false,"evidence":[]}"#
    let task = try JSONDecoder().decode(PulseTask.self, from: Data(json.utf8))
    XCTAssertEqual(task.revision, "9007199254740993")
    XCTAssertEqual(task.status, "等待下一步")
    XCTAssertFalse(task.cancelRequested)
  }
  func testShortPipeStateArrivesBeforeChildExit() {
    let ready = expectation(description: "short state received while child still runs")
    let transport = ChatTransport(
      executable: URL(fileURLWithPath: "/usr/bin/python3"),
      onState: { _, state in
        if state.server == "https://pulse.example/" { ready.fulfill() }
      }, onExit: { _ in })
    defer { transport.stop() }
    let script =
      #"import json,sys,time; print(json.dumps(dict(server='https://pulse.example/',connected=False,stopped=False,local_connected=False,paired=False,message='',pending=0,synced_at=None,tasks=[],messages=[])));sys.stdout.flush();time.sleep(30)"#
    transport.start(arguments: ["-c", script], generation: UUID())
    wait(for: [ready], timeout: 3)
  }
  func testFrameDecoderHandlesSplitUTF8AndMultipleFrames() throws {
    var decoder = FrameDecoder()
    let bytes = Data("中文\nnext\npart".utf8)
    XCTAssertTrue(try decoder.accept(bytes.prefix(2)).isEmpty)
    let frames = try decoder.accept(bytes.dropFirst(2))
    XCTAssertEqual(frames.map { String(decoding: $0, as: UTF8.self) }, ["中文", "next"])
    XCTAssertEqual(try decoder.accept(Data("ial\n".utf8)), [Data("partial".utf8)])
  }
  func testFrameLimitAppliesBeforeAccumulationAndOnNewline() throws {
    var decoder = FrameDecoder()
    _ = try decoder.accept(Data(repeating: 65, count: FrameDecoder.limit))
    XCTAssertThrowsError(try decoder.accept(Data([65, 10])))
    var other = FrameDecoder()
    XCTAssertThrowsError(try other.accept(Data(repeating: 65, count: FrameDecoder.limit + 1)))
  }
  func testServerPolicyAndCommandLineCannotSelectRepository() {
    var settings = ConnectionSettings()
    for value in [
      "http://example.com", "https://user:pass@example.com", "https://example.com/path",
      "https://example.com/?token=x",
    ] {
      settings.server = value
      XCTAssertFalse(settings.valid)
    }
    settings.server = "http://127.0.0.1:8721"
    XCTAssertFalse(settings.valid)
    settings.development = true
    XCTAssertTrue(settings.valid)
    XCTAssertEqual(ConnectionSettings.load(arguments: ["--repo", "/tmp"]).server, "")
  }
  func testPairFileIsOnlyUsedForOneLaunchAndNeverStoredInPreferences() throws {
    let settings = ConnectionSettings.load(arguments: [
      "--server", "https://pulse.example", "--pair-code-file", "/private/code",
    ])
    XCTAssertTrue(settings.arguments.contains("--pair-code-file"))
    let saved = try JSONEncoder().encode(settings)
    XCTAssertFalse(String(decoding: saved, as: UTF8.self).contains("/private/code"))
    XCTAssertNil(try JSONDecoder().decode(ConnectionSettings.self, from: saved).pairFile)
  }
  func testPendingIdentityAndConnectionBinding() {
    let pending = PendingSubmit(
      sourceID: "source", taskID: "task", text: "中文\n第二行",
      binding: ["https://pulse.example/", "/private"])
    XCTAssertEqual(pending.command["source_id"] as? String, "source")
    XCTAssertEqual((pending.command["action"] as? [String: String])?["goal"], "中文\n第二行")
    var state = NativeState()
    state.server = "https://other.example/"
    state.messages = [ChatMessage(id: "source", body: "已保存任務：中文")]
    XCTAssertFalse(pending.confirmed(by: state))
    state.server = "https://pulse.example/"
    XCTAssertTrue(pending.confirmed(by: state))
  }
  func testDurableReceiptClearsOnlyOriginalDraftAndKeepsLaterEdits() {
    let pending = PendingSubmit(
      sourceID: "source", taskID: "task", text: "draft\n中文",
      binding: ["https://pulse.example/", "/private"])
    var state = NativeState()
    state.server = "https://pulse.example/"
    XCTAssertNil(pending.confirmedDraft(pending.text, by: state))
    state.messages = [ChatMessage(id: "source", body: "已保存任務：draft")]
    XCTAssertEqual(pending.confirmedDraft(pending.text, by: state), "")
    XCTAssertEqual(pending.confirmedDraft("new draft", by: state), "new draft")
    state.server = "https://another.example/"
    XCTAssertNil(pending.confirmedDraft(pending.text, by: state))
  }
  func testOriginBindingHandlesTrailingSlashAndDefaultPort() {
    XCTAssertEqual(serverBinding("https://PULSE.example:443"), "https://pulse.example/")
    XCTAssertNotEqual(serverBinding("https://pulse.example:444"), "https://pulse.example/")
  }
  func testQueryReceiptClearsDraftWithoutCreatingTask() {
    let pending = PendingSubmit(sourceID: "query", taskID: "unused", text: "查詢 A、B",
      binding: ["https://pulse.example/", "/private"])
    var state = NativeState()
    state.server = "https://pulse.example/"
    state.messages = [ChatMessage(id: "query", body: "已保存訊息：查詢 A、B")]
    XCTAssertTrue(state.tasks.isEmpty)
    XCTAssertEqual(pending.confirmedDraft(pending.text, by: state), "")
    XCTAssertTrue(state.messages[0].isUser)
    XCTAssertEqual(state.messages[0].displayBody, "查詢 A、B")
  }
  func testSafeLinksRejectExecutionAndCredentials() {
    for value in [
      "javascript:alert(1)", "file:///tmp/a", "http://example.com",
      "https://user:password@example.com",
    ] {
      XCTAssertFalse(safeLink(URL(string: value)!))
    }
    XCTAssertTrue(safeLink(URL(string: "https://example.com/result")!))
  }
  func testRichContentKeepsCodeAndBoundsTableColumns() {
    let blocks = richBlocks("Hello\n```rust\nlet x = 1;\n```\n| A | B |\n|---|---|\n|一|二|")
    XCTAssertEqual(
      blocks, [.text("Hello"), .code("let x = 1;"), .table([["A", "B"], ["一", "二"]])])
    let wide = "|" + Array(repeating: "x", count: 30).joined(separator: "|") + "|\n|---|---|"
    guard case .table(let rows) = richBlocks(wide).first else { return XCTFail("missing table") }
    XCTAssertEqual(rows[0].count, 16)
  }
}
