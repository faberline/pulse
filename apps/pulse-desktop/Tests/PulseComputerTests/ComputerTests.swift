import XCTest
import CryptoKit
import Darwin
@testable import PulseComputerCore

final class ComputerTests: XCTestCase {
  func request(_ changes: [String: Any] = [:]) throws -> Data {
    var binding: [String: Any] = ["operation_id": "observe-1", "task_id": "task", "revision": "1", "device": UUID().uuidString, "epoch": "3"]
    for (key, value) in changes { binding[key] = value }
    return try JSONSerialization.data(withJSONObject: ["kind": "observe", "binding": binding])
  }
  func testReadOnlyProtocolRejectsEffectsExtraFieldsAndInvalidBindings() throws {
    XCTAssertEqual(try ComputerRequest.decode(Data(#"{"kind":"status"}"#.utf8)).kind, "status")
    XCTAssertEqual(try ComputerRequest.decode(request()).binding?.operation_id, "observe-1")
    for data in [Data(#"{"kind":"click"}"#.utf8), Data(#"{"kind":"type","text":"send"}"#.utf8), Data(#"{"kind":"status","target":"other"}"#.utf8), Data(#"{"kind":"observe","binding":{}}"#.utf8), Data("not json".utf8), Data(repeating: 32, count: 16_385)] {
      XCTAssertThrowsError(try ComputerRequest.decode(data)) { XCTAssertEqual($0 as? ComputerError, .invalidRequest) }
    }
    for changes: [String: Any] in [["revision": "0"], ["revision": "01"], ["revision": "18446744073709551616"], ["revision": 1], ["epoch": "-1"], ["device": "forged"], ["task_id": "bad\ncontrol"], ["operation_id": ""], ["task_id": String(repeating: "a", count: 129)], ["grant": "approved"]] {
      XCTAssertThrowsError(try ComputerRequest.decode(request(changes)))
    }
    var object = try JSONSerialization.jsonObject(with: request()) as! [String: Any]
    for key in ["script", "bundle_id", "window_id", "path", "effect", "grant"] {
      object[key] = "forged"
      XCTAssertThrowsError(try ComputerRequest.decode(JSONSerialization.data(withJSONObject: object)))
      object.removeValue(forKey: key)
    }
  }
  func testTargetsAndFramesHaveNoWildcardOrUnboundedCoordinates() throws {
    XCTAssertNoThrow(try ComputerTarget(bundleID: "com.faberline.PulseComputerFixture", pid: 20, windowID: 30))
    for id in ["", "*", "com.*", "com..app", ".com.app", "com.app.", "com/app", "com.app\n"] {
      XCTAssertThrowsError(try ComputerTarget(bundleID: id, pid: 20, windowID: 30))
    }
    XCTAssertThrowsError(try ComputerTarget(bundleID: "com.app", pid: 0, windowID: 30))
    XCTAssertThrowsError(try ComputerTarget(bundleID: "com.app", pid: 20, windowID: 0))
    XCTAssertThrowsError(try ComputerRect(CGRect(x: CGFloat.nan, y: 0, width: 100, height: 100)))
    XCTAssertThrowsError(try ComputerRect(CGRect(x: 0, y: 0, width: 9000, height: 100)))
    XCTAssertThrowsError(try ComputerRect(CGRect.zero))
    XCTAssertEqual(boundedText("中文測試", bytes: 7), "中文")
    XCTAssertEqual(boundedText(String(repeating: "文", count: 100_000), bytes: 256).utf8.count, 255)
  }
  func testPrivatePNGCacheRefusesSymlinksBadModesTamperAndFIFO() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
    defer { try? FileManager.default.removeItem(at: root) }
    let png = Data(base64Encoded: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")!
    let digest = SHA256.hash(data: png).map { String(format: "%02x", $0) }.joined()
    let path = try PrivatePNG.save(png, directory: root)
    XCTAssertEqual(URL(fileURLWithPath: path).lastPathComponent, digest + ".png")
    XCTAssertEqual(try Data(contentsOf: URL(fileURLWithPath: path)), png)
    XCTAssertEqual(try PrivatePNG.save(png, directory: root), path)
    try FileManager.default.setAttributes([.posixPermissions: 0o644], ofItemAtPath: path)
    XCTAssertThrowsError(try PrivatePNG.save(png, directory: root))
    try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: path)
    try Data("bad".utf8).write(to: URL(fileURLWithPath: path))
    XCTAssertThrowsError(try PrivatePNG.save(png, directory: root))
    try FileManager.default.removeItem(atPath: path)
    let outside = root.appendingPathComponent("original")
    try png.write(to: outside)
    try FileManager.default.createSymbolicLink(atPath: path, withDestinationPath: outside.path)
    XCTAssertThrowsError(try PrivatePNG.save(png, directory: root))
    try FileManager.default.removeItem(atPath: path)
    XCTAssertEqual(mkfifo(path, 0o600), 0)
    XCTAssertThrowsError(try PrivatePNG.save(png, directory: root))
    try FileManager.default.removeItem(atPath: path)
    try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: root.path)
    XCTAssertThrowsError(try PrivatePNG.save(png, directory: root))
    try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: root.path)
    let alias = root.appendingPathComponent("alias")
    try FileManager.default.createSymbolicLink(at: alias, withDestinationURL: root)
    XCTAssertThrowsError(try PrivatePNG.save(png, directory: alias))
  }
  func testFullCacheLeavesNoPartialArtifact() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
    defer { try? FileManager.default.removeItem(at: root) }
    for index in 0..<128 { try Data().write(to: root.appendingPathComponent("existing-\(index)")) }
    let png = Data(base64Encoded: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")!
    XCTAssertThrowsError(try PrivatePNG.save(png, directory: root))
    XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: root.path).count, 128)
  }
}
