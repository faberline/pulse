import XCTest
@testable import PulseDesktop

final class MemoryTests: XCTestCase {
  func testMemorySourcesAreBoundedAndOldHelperStatesRemainReadable() throws {
    let now: Int64 = 1_800_000_000_000
    let source = UUID().uuidString
    let memory = PulseMemory(id: UUID().uuidString, revision: 1, label: "回答語言", body: "繁體中文", source_id: source, source_at: now, expires_at: now + 90 * 24 * 60 * 60 * 1000)
    XCTAssertTrue(memory.live(at: now))
    XCTAssertEqual(try JSONDecoder().decode(String.self,from: Data(memory.controlName.utf8)),memory.label)
    XCTAssertFalse(memory.live(at: memory.expires_at))
    XCTAssertFalse(memory.live(at: now - 1))
    let bad = PulseMemory(id: memory.id, revision: 1, label: "x", body: "x", source_id: source, source_at: Int64.min, expires_at: Int64.max)
    XCTAssertFalse(bad.live(at: now))
    let old = Data("{\"server\":\"https://pulse.example\",\"connected\":false,\"stopped\":false,\"local_connected\":false,\"paired\":true,\"message\":\"\",\"pending\":0,\"tasks\":[],\"messages\":[]}".utf8)
    let state = try JSONDecoder().decode(NativeState.self, from: old)
    XCTAssertNil(state.memories)
    XCTAssertNil(state.input_failure)
  }
}
