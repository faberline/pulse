import Foundation
import XCTest
@testable import PulseDesktop

final class TextInputTests: XCTestCase {
  func testRustFileReferenceIsOptionalAndCarriesNoBodyOrPath() throws {
    var wire: [String:Any] = ["server":"https://pulse.example/","connected":false,"stopped":false,
      "local_connected":false,"paired":true,"message":"","pending":0,"tasks":[],"messages":[]]
    func decode() throws -> NativeState {
      try JSONDecoder().decode(NativeState.self,from:JSONSerialization.data(withJSONObject:wire))
    }
    XCTAssertNil(try decode().text_input)
    let id=UUID().uuidString
    wire["text_input"]=["id":id,"name":"report.md","digest":String(repeating:"a",count:64),"length":32768,"captured_at":1234]
    wire["text_input_request"]=["request_id":id,"loading":false,"failed":false,"cancelled":true]
    let state=try decode()
    XCTAssertEqual(state.text_input?.id,id)
    XCTAssertEqual(state.text_input?.name,"report.md")
    XCTAssertEqual(state.text_input?.length,32768)
    XCTAssertEqual(state.text_input?.captured_at,1234)
    XCTAssertEqual(state.text_input_request?.cancelled,true)
  }
  func testFileSubmissionKeepsOriginalIdsAndUsesOnlyTheRustSelectionReference() throws {
    let pending=PendingSubmit(sourceID:"source",taskID:"task",text:"摘要這份檔案",
      binding:["https://pulse.example/","/private/test-chat"],textInputID:"selection")
    let command=pending.command
    XCTAssertEqual(command["kind"] as? String,"submit_text_file")
    XCTAssertEqual(command["source_id"] as? String,"source")
    XCTAssertEqual(command["id"] as? String,"task")
    XCTAssertEqual(command["input_id"] as? String,"selection")
    XCTAssertEqual(command["goal"] as? String,"摘要這份檔案")
    XCTAssertNil(command["path"]); XCTAssertNil(command["input"])
    XCTAssertEqual(try JSONSerialization.data(withJSONObject:command,options:.sortedKeys),
      try JSONSerialization.data(withJSONObject:pending.command,options:.sortedKeys))
    var state=NativeState();state.server="https://pulse.example/"
    XCTAssertNil(pending.confirmedDraft("摘要這份檔案",by:state))
    state.messages=[ChatMessage(id:"source",body:"本機待送：摘要這份檔案")]
    XCTAssertEqual(pending.confirmedDraft("摘要這份檔案",by:state),"")
    XCTAssertEqual(pending.confirmedDraft("新的草稿",by:state),"新的草稿")
  }
}
