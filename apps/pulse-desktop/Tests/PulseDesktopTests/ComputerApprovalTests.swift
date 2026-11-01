import XCTest
@testable import PulseDesktop

final class ComputerApprovalTests: XCTestCase {
  func testNativeApprovalUsesExactOriginalSourceAndTaskAndRejectsBrowserOrChangedImages() throws {
    let scope = "computer:com.faberline.PulseComputerFixture:42:7:1"
    let digest = String(repeating: "a", count: 64)
    let binding: [String: Any] = ["operation_id":"inspect", "task_id":"task", "revision":"1", "device":UUID().uuidString, "epoch":"2"]
    let target: [String: Any] = ["bundle_id":"com.faberline.PulseComputerFixture", "pid":42, "window_id":7]
    let selected: [String: Any] = ["target":target, "process_launched_at":1]
    let observation: [String: Any] = ["binding":binding, "target":target, "process_launched_at":1,
      "title":"Owned fixture", "frame":["x":0.0,"y":0.0,"width":400.0,"height":300.0],
      "elements":[["id":"window.0", "role":"AXButton", "label":"Count once", "value":NSNull(), "can_press":true, "can_set_text":false]],
      "truncated":false, "nonce":UUID().uuidString, "observed_at":1000, "expires_at":121000,
      "image":["digest":digest, "length":100, "width":1, "height":1]]
    let wire: [String: Any] = ["proposal":["id":"request", "target":scope, "token":["task_id":"task","revision":"1"],
      "intent":["observation":observation,"element_id":"window.0","effect":["kind":"press"]],"expires_at":61000],
      "intent_digest":"intent", "state":"pending"]
    let request = try JSONDecoder().decode(BrowserApproval.self, from: JSONSerialization.data(withJSONObject: wire))
    XCTAssertEqual(request.proposal.intent.source_operation_id,"inspect")
    XCTAssertEqual(request.control?.label,"Count once")
    let meta: [String: Any] = ["task_id":"task", "revision":"1", "computer":["binding":binding,"selection":selected],
      "digest":digest, "length":100, "width":1, "height":1, "title":"Owned fixture", "observed_at":1000]
    func artifact(_ value: [String: Any]) throws -> BrowserArtifact {
      try JSONDecoder().decode(BrowserArtifact.self, from: JSONSerialization.data(withJSONObject:value))
    }
    let image = try artifact(meta)
    func task(revision: String = "1", state: String = "waiting_approval", cancel: Bool = false) -> PulseTask {
      PulseTask(id:"task",goal:"Count once",revision:revision,state:state,scope:scope,reason:nil,cancelRequested:cancel)
    }
    func enabled(_ image: BrowserArtifact? = image, task: PulseTask? = task(), connected: Bool = true,
      verified: String? = digest, hasImage: Bool = true, pending: Bool = false, now: Int64 = 2000) -> Bool {
      request.canApprove(task:task,connected:connected,artifact:image,verifiedDigest:verified,hasImage:hasImage,pending:pending,now:now)
    }
    XCTAssertTrue(enabled())
    XCTAssertFalse(enabled(nil)); XCTAssertFalse(enabled(task:nil)); XCTAssertFalse(enabled(task:task(revision:"2")))
    XCTAssertFalse(enabled(task:task(state:"outcome_unknown"))); XCTAssertFalse(enabled(task:task(cancel:true)))
    XCTAssertFalse(enabled(connected:false)); XCTAssertFalse(enabled(verified:"other")); XCTAssertFalse(enabled(hasImage:false))
    XCTAssertFalse(enabled(pending:true)); XCTAssertFalse(enabled(now:61000))
    for attack in ["browser","epoch","operation","window","launch","title","time","length"] {
      var changed = meta
      switch attack {
      case "browser": changed.removeValue(forKey:"computer");changed["url"] = "https://example.com/"
      case "epoch", "operation":
        var source = changed["computer"] as! [String: Any];var b = binding
        b[attack == "epoch" ? "epoch" : "operation_id"] = attack == "epoch" ? "3" : "other"
        source["binding"] = b;changed["computer"] = source
      case "window", "launch":
        var source = changed["computer"] as! [String: Any];var selection = selected
        if attack == "window" {var t = target;t["window_id"] = 8;selection["target"] = t}
        else {selection["process_launched_at"] = 2}
        source["selection"] = selection;changed["computer"] = source
      case "title": changed["title"] = "Other window"
      case "time": changed["observed_at"] = 1001
      case "length": changed["length"] = 101
      default: XCTFail(attack)
      }
      XCTAssertFalse(enabled(try artifact(changed)),attack)
    }
    var changed = wire;var proposal = changed["proposal"] as! [String: Any]
    proposal["target"] = "computer:com.faberline.Other:42:7:1";changed["proposal"] = proposal
    let other = try JSONDecoder().decode(BrowserApproval.self, from: JSONSerialization.data(withJSONObject:changed))
    XCTAssertFalse(other.canApprove(task:task(),connected:true,artifact:image,verifiedDigest:digest,hasImage:true,pending:false,now:2000))
    let message: [String: Any] = ["id":"request", "body":"程式視窗操作提議", "artifact_operation_id":"inspect", "artifact":meta,"approval":wire]
    let decoded = try JSONDecoder().decode(ChatMessage.self, from: JSONSerialization.data(withJSONObject:message))
    XCTAssertEqual(decoded.artifactID,"inspect");XCTAssertNotNil(decoded.approval?.proposal.intent.computer)
  }
}
