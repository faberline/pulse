import Foundation
import UserNotifications

struct DesktopNotification: Decodable, Equatable, Sendable {
  let attempt_id: String
  let identifier: String
  let title: String
  let body: String
}
struct DesktopNotifications: Decodable, Sendable {
  var enabled = false
  var current: DesktopNotification?
  var uncertain = 0
  var failed = 0
}

/// The Rust journal chooses the content, identifier and attempt. This adapter
/// only calls the platform. It has no timer, durable queue or retry policy.
@MainActor
protocol NotificationPlatform {
  func authorize() async throws -> Bool
  func submit(_ value: DesktopNotification) async -> String
}
@MainActor
final class MacNotificationPlatform: NotificationPlatform {
  func authorize() async throws -> Bool {
    try await UNUserNotificationCenter.current().requestAuthorization(options: [.alert])
  }
  func submit(_ value: DesktopNotification) async -> String {
    let center = UNUserNotificationCenter.current()
    let settings = await center.notificationSettings()
    guard !Task.isCancelled else { return "failed" }
    guard settings.authorizationStatus == .authorized || settings.authorizationStatus == .provisional
    else { return "denied" }
    let content = UNMutableNotificationContent()
    content.title = value.title
    content.body = value.body
    content.threadIdentifier = "pulse-reminders"
    do {
      try await center.add(UNNotificationRequest(identifier: value.identifier, content: content, trigger: nil))
      return "submitted"
    } catch { return "failed" }
  }
}

@MainActor
final class DesktopNotificationPresenter {
  private let platform: any NotificationPlatform
  // Pipe snapshots repeat. This is only an in-process guard around one OS call.
  // Rust, not this set, owns the state after a process restart.
  private var started = Set<String>()
  private var pending: [String: Task<Void, Never>] = [:]
  init(platform: any NotificationPlatform = MacNotificationPlatform()) { self.platform = platform }
  func authorize() async -> Bool { (try? await platform.authorize()) == true }
  func present(_ value: DesktopNotification, reply: @escaping @MainActor (String) -> Void) {
    for (id, task) in pending where id != value.attempt_id { task.cancel() }
    guard started.insert(value.attempt_id).inserted else { return }
    pending[value.attempt_id] = Task {
      let result = await platform.submit(value)
      pending[value.attempt_id] = nil
      reply(result)
    }
  }
  func cancelPending() {
    for task in pending.values { task.cancel() }
  }
  func reset() { cancelPending(); started.removeAll() }
}
