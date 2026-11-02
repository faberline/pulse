import XCTest
@testable import PulseDesktop

final class ComputerWindowListTests: XCTestCase {
  func testRustLocalCatalogKeepsRequestIdentityAndOldStatesRemainReadable() throws {
    var wire: [String: Any] = ["server":"https://pulse.example/", "connected":false,
      "stopped":false, "local_connected":true, "paired":true, "message":"", "pending":0,
      "tasks":[], "messages":[]]
    func decode() throws -> NativeState {
      try JSONDecoder().decode(NativeState.self, from: JSONSerialization.data(withJSONObject: wire))
    }
    XCTAssertNil(try decode().computer_windows)
    let id = UUID().uuidString
    wire["computer_windows"] = ["request_id":id, "loading":false, "failed":false,
      "windows":[["selection":["target":["bundle_id":"com.faberline.fixture", "pid":42,
        "window_id":7], "process_launched_at":1], "app_name":"Owned fixture", "title":"Fixture window"]]]
    let value = try XCTUnwrap(decode().computer_windows)
    XCTAssertEqual(value.request_id,id)
    XCTAssertEqual(value.windows.first?.appName,"Owned fixture")
    XCTAssertEqual(value.windows.first?.selection.target.window_id,7)
    XCTAssertFalse(value.loading); XCTAssertFalse(value.failed)
  }
  func testPermissionStatusComesFromRustAndFailedStatusDoesNotInventAGrant() throws {
    var wire: [String:Any] = ["server":"https://pulse.example/", "connected":false,
      "stopped":false,"local_connected":true,"paired":false,"message":"","pending":0,"tasks":[],"messages":[]]
    func decode() throws -> NativeState {
      try JSONDecoder().decode(NativeState.self,from:JSONSerialization.data(withJSONObject:wire))
    }
    XCTAssertNil(try decode().computer_permissions)
    let id=UUID().uuidString
    wire["computer_permissions"] = ["request_id":id,"loading":false,"failed":false,
      "permissions":["accessibility":false,"screen_capture":true]]
    let status=try XCTUnwrap(decode().computer_permissions)
    XCTAssertEqual(status.request_id,id)
    XCTAssertFalse(try XCTUnwrap(status.permissions).accessibility)
    XCTAssertTrue(try XCTUnwrap(status.permissions).screen_capture)
    wire["computer_permissions"] = ["request_id":id,"loading":false,"failed":true]
    XCTAssertNil(try decode().computer_permissions?.permissions)
  }
}
