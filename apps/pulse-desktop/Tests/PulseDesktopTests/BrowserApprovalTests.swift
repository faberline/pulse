import XCTest
@testable import PulseDesktop

final class BrowserApprovalTests: XCTestCase {
  func testApprovalNeedsCurrentTaskAndVerifiedSourceImageAndLiveConnection() throws {
    let data = Data(#"{"proposal":{"id":"request","token":{"task_id":"task","revision":"2"},"intent":{"source_operation_id":"inspect","observation":{"url":"https://example.com/","title":"Fixture","png_digest":"digest","elements":[],"observed_at":10,"expires_at":100},"element_id":"button","effect":{"kind":"click"}},"expires_at":90},"intent_digest":"intent","state":"pending"}"#.utf8)
    let request = try JSONDecoder().decode(BrowserApproval.self, from: data)
    func task(_ revision: String = "2", _ state: String = "waiting_approval", cancel: Bool = false) -> PulseTask {
      PulseTask(id: "task", goal: "Test", revision: revision, state: state, scope: nil, reason: nil, cancelRequested: cancel)
    }
    let artifact = BrowserArtifact(task_id: "task", revision: "2", digest: "digest", length: 100, url: "https://example.com/", title: "Fixture", observed_at: 10, width: 1, height: 1)
    func enabled(_ task: PulseTask? = task(), connected: Bool = true, image: BrowserArtifact? = artifact, digest: String? = "digest", hasImage: Bool = true, pending: Bool = false, now: Int64 = 20) -> Bool {
      request.canApprove(task: task, connected: connected, artifact: image, verifiedDigest: digest, hasImage: hasImage, pending: pending, now: now)
    }
    XCTAssertTrue(enabled())
    XCTAssertFalse(enabled(nil))
    XCTAssertFalse(enabled(task("1")))
    XCTAssertFalse(enabled(task("2", "cancelled")))
    XCTAssertFalse(enabled(task(cancel: true)))
    XCTAssertFalse(enabled(connected: false))
    XCTAssertFalse(enabled(image: nil))
    XCTAssertFalse(enabled(digest: "old-image"))
    XCTAssertFalse(enabled(hasImage: false))
    XCTAssertFalse(enabled(pending: true))
    XCTAssertFalse(enabled(now: 90))
    var changed = try JSONSerialization.jsonObject(with: data) as! [String: Any]
    changed["state"] = "dispatched"
    let used = try JSONDecoder().decode(BrowserApproval.self, from: JSONSerialization.data(withJSONObject: changed))
    XCTAssertFalse(used.canApprove(task: task(), connected: true, artifact: artifact, verifiedDigest: "digest", hasImage: true, pending: false, now: 20))
  }
}
