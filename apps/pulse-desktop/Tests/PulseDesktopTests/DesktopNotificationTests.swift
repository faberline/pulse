import XCTest
@testable import PulseDesktop

@MainActor
private final class FakeNotificationPlatform: NotificationPlatform {
  var allowed = false
  var result = "submitted"
  var calls: [DesktopNotification] = []
  var delay: UInt64 = 0
  func authorize() async throws -> Bool { allowed }
  func submit(_ value: DesktopNotification) async -> String {
    if delay > 0 { try? await Task.sleep(nanoseconds: delay) }
    guard !Task.isCancelled else { return "failed" }
    calls.append(value); return result
  }
}
final class DesktopNotificationTests: XCTestCase {
  @MainActor
  func testRepeatedRustFramesCallPlatformOnceAndKeepExactContent() async {
    let platform = FakeNotificationPlatform()
    let presenter = DesktopNotificationPresenter(platform: platform)
    let notice = DesktopNotification(attempt_id: "attempt", identifier: "rust-bound-id", title: "提醒", body: "原文字\n與時區")
    let done = expectation(description: "one platform reply")
    for _ in 0..<10 { presenter.present(notice) { result in XCTAssertEqual(result,"submitted"); done.fulfill() } }
    await fulfillment(of: [done], timeout: 2)
    XCTAssertEqual(platform.calls,[notice])
  }
  @MainActor
  func testPlatformDenialIsReturnedWithoutAFrontendRetryOrSuccess() async {
    let platform = FakeNotificationPlatform(); platform.result = "denied"
    let presenter = DesktopNotificationPresenter(platform: platform)
    let allowed = await presenter.authorize(); XCTAssertFalse(allowed)
    let notice = DesktopNotification(attempt_id: "attempt", identifier: "id", title: "提醒", body: "內容")
    let done = expectation(description: "denial returned")
    presenter.present(notice) { result in XCTAssertEqual(result,"denied");done.fulfill() }
    await fulfillment(of: [done], timeout: 2)
    presenter.present(notice) { _ in XCTFail("frontend retried a durable offer") }
    XCTAssertEqual(platform.calls.count,1)
  }
  func testRustNotificationStateKeepsUnknownSeparateFromSystemSubmission() throws {
    let json = #"{"enabled":true,"current":{"attempt_id":"a","identifier":"i","title":"喝水","body":"時間與原文"},"uncertain":2,"failed":1}"#
    let value = try JSONDecoder().decode(DesktopNotifications.self, from: Data(json.utf8))
    XCTAssertTrue(value.enabled);XCTAssertEqual(value.current?.identifier,"i")
    XCTAssertEqual(value.uncertain,2);XCTAssertEqual(value.failed,1)
  }
  @MainActor
  func testAReplacedRustOfferCancelsTheOldPlatformCallBeforePresentation() async {
    let platform = FakeNotificationPlatform();platform.delay = 100_000_000
    let presenter = DesktopNotificationPresenter(platform: platform)
    let old = DesktopNotification(attempt_id:"old", identifier:"old", title:"舊", body:"原文")
    let current = DesktopNotification(attempt_id:"new", identifier:"new", title:"新", body:"原文")
    let first = expectation(description:"old offer stopped")
    let second = expectation(description:"current offer submitted")
    presenter.present(old) { result in XCTAssertEqual(result,"failed");first.fulfill() }
    presenter.present(current) { result in XCTAssertEqual(result,"submitted");second.fulfill() }
    await fulfillment(of:[first,second],timeout:2)
    XCTAssertEqual(platform.calls,[current])
  }
}
