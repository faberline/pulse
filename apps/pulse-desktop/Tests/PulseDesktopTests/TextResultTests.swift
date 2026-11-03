import Foundation
import XCTest
@testable import PulseDesktop

final class TextResultTests: XCTestCase {
  func testExportCommandOnlyNamesTheOriginalMessageAndHumanDestination() throws {
    let command = TextResultExportCommand.make(requestID:"request",messageID:"model:run:1:0",path:"/private/test/result.md")
    XCTAssertEqual(command["kind"] as? String,"export_text_result")
    XCTAssertEqual(command["request_id"] as? String,"request")
    XCTAssertEqual(command["message_id"] as? String,"model:run:1:0")
    XCTAssertEqual(command["path"] as? String,"/private/test/result.md")
    XCTAssertNil(command["body"]); XCTAssertNil(command["goal"]); XCTAssertNil(command["task_id"])
    XCTAssertNil(command["digest"]); XCTAssertNil(command["grant"])
  }
  func testExportStatusIsSeparateFromTheExistingMessageContract() throws {
    let message = try JSONDecoder().decode(ChatMessage.self,from:Data(#"{"id":"model:run:1:0","body":"original reply"}"#.utf8))
    XCTAssertEqual(message.body,"original reply")
    let state = try JSONDecoder().decode(TextResultExport.self,from:Data(#"{"request_id":"request","status":"unknown","file_name":"result.md"}"#.utf8))
    XCTAssertEqual(state.request_id,"request")
    XCTAssertEqual(state.status,"unknown")
    XCTAssertEqual(state.file_name,"result.md")
    XCTAssertTrue(state.resultMessage?.contains("result.md") == true)
    XCTAssertTrue(state.resultMessage?.contains("不會自動重試") == true)
    XCTAssertNil(TextResultExport(request_id:"request",status:"working",file_name:nil).resultMessage)
    XCTAssertTrue(TextResultExport(request_id:"request",status:"saved",file_name:"saved.md").resultMessage?.contains("saved.md") == true)
  }
}
