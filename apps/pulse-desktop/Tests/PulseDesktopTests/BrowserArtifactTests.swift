import XCTest
import CryptoKit
@testable import PulseDesktop

final class BrowserArtifactTests: XCTestCase {
  func testWindowReselectionRetainsTaskRevisionAndDoesNotCreateAnotherGoal() throws {
    let selection = ComputerArtifactSource.Selection(target: .init(bundle_id: "com.faberline.PulseComputerFixture", pid: 42, window_id: 7), process_launched_at: 1)
    let pending = PendingWindowReselection(sourceID: "selection", taskID: "same-task", revision: "3", selection: selection, selectedAt: 1000, binding: ["https://pulse.example/", "private"])
    XCTAssertEqual(pending.command["kind"] as? String, "rebind_computer")
    XCTAssertEqual(pending.command["task_id"] as? String, "same-task")
    XCTAssertEqual(pending.command["revision"] as? String, "3")
    XCTAssertEqual(pending.command["selected_at"] as? Int64, 1000)
    XCTAssertNil(pending.command["id"]);XCTAssertNil(pending.command["goal"])
    func task(_ state: String, cancel: Bool = false) -> PulseTask {
      PulseTask(id: "same-task", goal: "read", revision: "3", state: state, scope: "computer:com.app:42:7:1", reason: nil, cancelRequested: cancel)
    }
    XCTAssertTrue(task("waiting_device").canReselectWindow)
    XCTAssertFalse(task("outcome_unknown").canReselectWindow)
    XCTAssertFalse(task("dispatched").canReselectWindow)
    XCTAssertFalse(task("waiting_device", cancel: true).canReselectWindow)
  }
  func testComputerSubmitRetainsOneSelectionAndTimestampUntilCloudReceipt() throws {
    let selection = ComputerArtifactSource.Selection(target: .init(bundle_id: "com.faberline.PulseComputerFixture", pid: 42, window_id: 7), process_launched_at: 1)
    let pending = PendingSubmit(sourceID: "source", taskID: "task", text: "讀取視窗", binding: ["https://pulse.example/", "private"], computer: selection, selectedAt: 1000)
    let command = pending.command
    XCTAssertEqual(command["kind"] as? String, "submit_computer")
    XCTAssertEqual(command["selected_at"] as? Int64, 1000)
    XCTAssertEqual(command["id"] as? String, "task")
    let encoded = try JSONSerialization.data(withJSONObject: command["selection"]!)
    XCTAssertEqual(try JSONDecoder().decode(ComputerArtifactSource.Selection.self, from: encoded), selection)
    var state = NativeState(); state.server = "https://pulse.example/"
    XCTAssertNil(pending.confirmedDraft("讀取視窗", by: state))
    state.messages = [ChatMessage(id: "source", body: "已保存訊息：讀取視窗")]
    XCTAssertEqual(pending.confirmedDraft("讀取視窗", by: state), "")
    XCTAssertEqual(pending.confirmedDraft("新的草稿", by: state), "新的草稿")
    XCTAssertEqual(pending.command["selected_at"] as? Int64, 1000)
  }
  func testComputerPNGNeedsExactProcessSourceAndCannotMasqueradeAsBrowser() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    try FileManager.default.createDirectory(at: root.appendingPathComponent("artifacts"), withIntermediateDirectories: true)
    let data = Data(base64Encoded: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")!
    let digest = SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    let path = root.appendingPathComponent("artifacts").appendingPathComponent(digest + ".png")
    try data.write(to: path)
    var value: [String: Any] = ["task_id":"task", "revision":"1", "digest":digest, "length":data.count,
      "title":"Fixture", "observed_at":1000, "width":1, "height":1,
      "computer":["binding":["operation_id":"read", "task_id":"task", "revision":"1", "device":UUID().uuidString, "epoch":"2"],
        "selection":["target":["bundle_id":"com.faberline.PulseComputerFixture", "pid":42, "window_id":7], "process_launched_at":1]]]
    func decode() throws -> BrowserArtifact { try JSONDecoder().decode(BrowserArtifact.self, from: JSONSerialization.data(withJSONObject: value)) }
    let metadata = try decode()
    XCTAssertTrue(metadata.validSource)
    XCTAssertEqual(metadata.url, "")
    XCTAssertEqual(metadata.sourceLabel, "com.faberline.PulseComputerFixture · 視窗 7")
    XCTAssertEqual(verifiedBrowserImageData(LocalBrowserArtifact(operation_id: "read", artifact: metadata, path: path.path), directory: root.path), data)
    XCTAssertNil(verifiedBrowserImageData(LocalBrowserArtifact(operation_id: "other", artifact: metadata, path: path.path), directory: root.path))
    value["url"] = "https://example.com/"
    XCTAssertFalse(try decode().validSource)
    value.removeValue(forKey: "url")
    value["revision"] = "2"
    XCTAssertFalse(try decode().validSource)
    value["revision"] = "1"
    value.removeValue(forKey: "computer")
    XCTAssertFalse(try decode().validSource)
  }
  func testOriginalPNGNeedsExactSourceCachePathHashSizeAndDimensions() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    try FileManager.default.createDirectory(at: root.appendingPathComponent("artifacts"), withIntermediateDirectories: true)
    let data = Data(base64Encoded: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")!
    let digest = SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    let path = root.appendingPathComponent("artifacts").appendingPathComponent(digest + ".png")
    try data.write(to: path)
    func metadata(width: UInt32 = 1, length: UInt32? = nil, hash: String? = nil) -> BrowserArtifact {
      BrowserArtifact(task_id: "page", revision: "1", digest: hash ?? digest, length: length ?? UInt32(data.count), url: "https://example.com/", title: "Source page", observed_at: 1, width: width, height: 1)
    }
    func image(_ meta: BrowserArtifact = metadata(), _ location: String? = nil) -> LocalBrowserArtifact {
      LocalBrowserArtifact(operation_id: "read", artifact: meta, path: location ?? path.path)
    }
    XCTAssertEqual(verifiedBrowserImageData(image(), directory: root.path), data)
    XCTAssertNil(verifiedBrowserImageData(image(metadata(width: 8193)), directory: root.path))
    XCTAssertNil(verifiedBrowserImageData(image(metadata(width: 2)), directory: root.path))
    XCTAssertNil(verifiedBrowserImageData(image(metadata(length: 4 * 1024 * 1024 + 1)), directory: root.path))
    XCTAssertNil(verifiedBrowserImageData(image(metadata(hash: String(repeating: "0", count: 64))), directory: root.path))
    XCTAssertNil(verifiedBrowserImageData(image(metadata(), root.appendingPathComponent("other.png").path), directory: root.path))
    try FileManager.default.removeItem(at: path)
    let outside = root.appendingPathComponent("original.png")
    try data.write(to: outside)
    try FileManager.default.createSymbolicLink(at: path, withDestinationURL: outside)
    XCTAssertNil(verifiedBrowserImageData(image(), directory: root.path))
  }
}
