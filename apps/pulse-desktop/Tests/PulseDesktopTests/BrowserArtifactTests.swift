import XCTest
import CryptoKit
@testable import PulseDesktop

final class BrowserArtifactTests: XCTestCase {
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
