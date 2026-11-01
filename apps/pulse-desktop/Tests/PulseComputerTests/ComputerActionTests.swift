import XCTest
@testable import PulseComputerCore

final class ComputerActionTests: XCTestCase {
  func request() -> [String: Any] {
    let now = Int64(Date().timeIntervalSince1970 * 1000)
    let source: [String: Any] = ["operation_id":"read-1", "task_id":"task", "revision":"1", "device":UUID().uuidString, "epoch":"2"]
    var binding = source; binding["operation_id"] = "approved:request"
    return ["kind":"act", "binding":binding, "intent":[
      "observation":["binding":source, "target":["bundle_id":"com.faberline.PulseComputerFixture", "pid":42, "window_id":7],
        "process_launched_at":1, "title":"Owned fixture", "frame":["x":10.0,"y":20.0,"width":400.0,"height":300.0],
        "elements":[["id":"window.0", "role":"AXButton", "label":"Count once", "value":NSNull(), "can_press":true, "can_set_text":false],
          ["id":"window.1", "role":"AXTextField", "label":"Message", "value":"", "can_press":false, "can_set_text":true]],
        "truncated":false, "nonce":UUID().uuidString, "observed_at":now, "expires_at":now+120_000,
        "image":["digest":String(repeating:"a", count:64), "length":100, "width":1, "height":1]],
      "element_id":"window.0", "effect":["kind":"press"]]]
  }
  func decode(_ value: [String: Any]) throws -> ComputerRequest {
    try ComputerRequest.decode(JSONSerialization.data(withJSONObject:value))
  }
  func testFixedNativeActionHasExactPathFreeSourceAndPlainTextOnly() throws {
    let base = request()
    let parsed = try decode(base)
    XCTAssertEqual(parsed.kind,"act"); XCTAssertEqual(parsed.action?.effect.kind,"press")
    XCTAssertEqual(parsed.action?.observation.binding.operation_id,"read-1")
    XCTAssertEqual(parsed.binding?.operation_id,"approved:request")
    var value = base; var intent = value["intent"] as! [String: Any]
    intent["element_id"] = "window.1"
    for text in ["中文 test", ""] {
      intent["effect"] = ["kind":"set_text", "text":text]; value["intent"] = intent
      XCTAssertEqual(try decode(value).action?.effect.text,text)
    }
    for text in ["send\n", "\r", "\t", "\u{e007}", "\u{2028}", String(repeating:"文",count:342)] {
      intent["effect"] = ["kind":"set_text", "text":text]; value["intent"] = intent
      XCTAssertThrowsError(try decode(value))
    }
    for id in ["window.00", "window.-1", "window.128", "window/0", "window..0", "other.0", "window."+Array(repeating:"0",count:9).joined(separator:".")] {
      XCTAssertFalse(computerElementPath(id),id)
    }
    XCTAssertTrue(computerElementPath("window.0.127"))
  }
  func testNativeActionRejectsAlteredBindingsExpiredViewsSecureTargetsAndExtraAuthority() throws {
    for attack in ["task","revision","device","epoch","operation","expired","future","window","image","nonce","path","role","target_kind","text_kind","effect","grant","helper_path","image_path","control_path","source_grant"] {
      var value = request(); var binding = value["binding"] as! [String: Any]
      var intent = value["intent"] as! [String: Any]; var observation = intent["observation"] as! [String: Any]
      switch attack {
      case "task": binding["task_id"] = "other"
      case "revision": binding["revision"] = "2"
      case "device": binding["device"] = UUID().uuidString
      case "epoch": binding["epoch"] = "3"
      case "operation": binding["operation_id"] = "read-1"
      case "expired": observation["observed_at"] = 1; observation["expires_at"] = 120_001
      case "future": observation["observed_at"] = Int64.max-120_000; observation["expires_at"] = Int64.max
      case "window": var target = observation["target"] as! [String: Any];target["window_id"] = 0;observation["target"] = target
      case "image": var image = observation["image"] as! [String: Any];image["width"] = 8193;observation["image"] = image
      case "nonce": observation["nonce"] = "forged"
      case "path", "role", "control_path":
        var elements = observation["elements"] as! [[String: Any]]
        if attack == "path" {elements[0]["id"] = "window.00"}
        else if attack == "role" {elements[0]["role"] = "AXSecureTextField"}
        else {elements[0]["path"] = "/private/forged"}
        observation["elements"] = elements
      case "target_kind": intent["element_id"] = "window.1"
      case "text_kind": intent["effect"] = ["kind":"set_text","text":"replace a button"]
      case "effect": intent["effect"] = ["kind":"script","script":"forged"]
      case "grant": intent["grant"] = ["approved":true]
      case "helper_path": value["helper"] = "/private/forged"
      case "image_path": var image = observation["image"] as! [String: Any];image["path"] = "/private/forged";observation["image"] = image
      case "source_grant": var source = observation["binding"] as! [String: Any];source["grant"] = true;observation["binding"] = source
      default: XCTFail(attack)
      }
      intent["observation"] = observation; value["intent"] = intent; value["binding"] = binding
      XCTAssertThrowsError(try decode(value),attack)
    }
    XCTAssertThrowsError(try ComputerRequest.decode(Data(repeating:32,count:65_537)))
    XCTAssertThrowsError(try ComputerRequest.decode(Data(#"{"kind":"act"}"#.utf8)))
  }
}
