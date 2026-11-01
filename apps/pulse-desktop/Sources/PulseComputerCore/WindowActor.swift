@preconcurrency import AppKit
import ApplicationServices

/// Fixed AX commands only. The Rust caller owns grant validation and durable one-use delivery.
@MainActor
public enum WindowActor {
  public static func act(target: ComputerTarget, binding: ComputerBinding, intent: ComputerActionIntent, cache: URL) async -> ComputerResponse {
    func response(_ state: ComputerActionState, observation: ComputerObservation? = nil, error: ComputerError? = nil) -> ComputerResponse {
      ComputerResponse(permissions: computerPermissions(), observation: observation, error: error,
        action: ComputerActionReceipt(binding: binding, source_nonce: intent.observation.nonce, state: state))
    }
    let element: AXUIElement
    do {
      try binding.validate()
      try intent.validate(now: Int64(Date().timeIntervalSince1970 * 1000))
      let original = intent.observation
      guard original.target == target, original.binding.task_id == binding.task_id,
        original.binding.revision == binding.revision, original.binding.device == binding.device,
        original.binding.epoch == binding.epoch, original.binding.operation_id != binding.operation_id
      else { throw ComputerError.invalidRequest }
      let fresh = try await WindowObserver.observe(target: target, binding: binding, cache: cache,
        expectedProcessLaunchedAt: original.process_launched_at)
      guard original.matches(fresh) else { throw ComputerError.targetChanged }
      let deadline = ContinuousClock.now.advanced(by: .seconds(2))
      let selected = try await WindowObserver.window(target)
      guard selected.title == original.title, try ComputerRect(selected.frame) == original.frame else { throw ComputerError.targetChanged }
      let catalog = AXCatalog(pid: target.pid, deadline: deadline)
      let window = try catalog.window(title: original.title, frame: original.frame)
      element = try catalog.resolve(window, id: intent.element_id)
      let (elements, truncated) = try catalog.collect(window)
      guard elements == original.elements, truncated == original.truncated else { throw ComputerError.targetChanged }
      let app = try WindowObserver.running(target)
      guard let launched = app.launchDate,
        Int64(launched.timeIntervalSince1970 * 1000) == original.process_launched_at,
        computerPermissions().canObserve, ContinuousClock.now < deadline,
        Int64(Date().timeIntervalSince1970 * 1000) < original.expires_at
      else { throw ComputerError.targetChanged }
    } catch let error as ComputerError { return response(.notExecuted, error: error) }
    catch { return response(.notExecuted, error: .targetChanged) }
    // An AX error can mean the app handled a command but did not answer. Never retry it.
    let error: AXError
    if intent.effect.kind == "press" {
      error = AXUIElementPerformAction(element, kAXPressAction as CFString)
    } else if intent.effect.kind == "set_text", let text = intent.effect.text {
      error = AXUIElementSetAttributeValue(element, kAXValueAttribute as CFString, text as CFString)
    } else { return response(.notExecuted, error: .invalidRequest) }
    guard error == .success else { return response(.unknown, error: .accessibilityUnavailable) }
    do {
      let after = try await WindowObserver.observe(target: target, binding: binding, cache: cache,
        expectedProcessLaunchedAt: intent.observation.process_launched_at)
      return response(.completed, observation: after)
    } catch { return response(.unknown, error: .captureFailed) }
  }
}
