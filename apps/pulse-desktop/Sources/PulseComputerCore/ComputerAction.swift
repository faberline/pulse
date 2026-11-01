import Foundation

/// Path-free original view. The Rust caller must verify the durable Human grant first.
public struct ComputerImageMetadata: Codable, Equatable, Sendable {
  public let digest: String
  public let length: Int
  public let width: Int
  public let height: Int
}
public struct ComputerExpectedView: Codable, Equatable, Sendable {
  public let binding: ComputerBinding
  public let target: ComputerTarget
  public let process_launched_at: Int64
  public let title: String
  public let frame: ComputerRect
  public let elements: [ComputerElement]
  public let truncated: Bool
  public let nonce: String
  public let observed_at: Int64
  public let expires_at: Int64
  public let image: ComputerImageMetadata
  public func validate(now: Int64) throws {
    try binding.validate()
    _ = try ComputerTarget(bundleID: target.bundle_id, pid: target.pid, windowID: target.window_id)
    _ = try ComputerRect(CGRect(x: frame.x, y: frame.y, width: frame.width, height: frame.height))
    let latest = now.addingReportingOverflow(1000)
    guard !latest.overflow, process_launched_at > 0, process_launched_at <= observed_at, observed_at > 0,
      observed_at <= latest.partialValue,
      observed_at.addingReportingOverflow(120_000).overflow == false,
      observed_at + 120_000 == expires_at, expires_at > now,
      UUID(uuidString: nonce) != nil, !title.isEmpty, title.utf8.count <= 512,
      image.digest.utf8.count == 64, image.digest.utf8.allSatisfy({ (48...57).contains($0) || (97...102).contains($0) }),
      (33...(4 * 1024 * 1024)).contains(image.length), (1...8192).contains(image.width), (1...8192).contains(image.height),
      image.width * image.height <= 16_777_216, elements.count <= 128,
      Set(elements.map(\.id)).count == elements.count,
      elements.filter({ $0.can_press || $0.can_set_text }).count <= 16,
      try JSONEncoder().encode(elements).count <= 32_768
    else { throw ComputerError.invalidRequest }
    for element in elements {
      guard computerElementPath(element.id), element.role.utf8.count <= 64, element.role != "AXSecureTextField",
        element.label.utf8.count <= 256, element.value.map({ $0.utf8.count <= 1024 }) ?? true,
        !element.can_press || element.role == "AXButton",
        !element.can_set_text || (["AXTextField", "AXTextArea"].contains(element.role) && element.value != nil)
      else { throw ComputerError.invalidRequest }
    }
  }
  func matches(_ current: ComputerObservation) -> Bool {
    target == current.target && process_launched_at == current.process_launched_at
      && title == current.title && frame == current.frame && elements == current.elements
      && truncated == current.truncated && image.digest == current.image.digest
      && image.length == current.image.length && image.width == current.image.width && image.height == current.image.height
  }
}
func computerElementPath(_ value: String) -> Bool {
  let parts = value.split(separator: ".", omittingEmptySubsequences: false)
  return parts.first == "window" && parts.count <= 9 && parts.dropFirst().allSatisfy { component in
    guard let index = UInt8(component), index < 128 else { return false }
    return String(index) == component
  }
}
func computerInputText(_ value: String) -> Bool {
  value.utf8.count <= 1024 && !value.unicodeScalars.contains {
    CharacterSet.controlCharacters.contains($0) || (0xe000...0xf8ff).contains($0.value)
      || (0xf0000...0xffffd).contains($0.value) || (0x100000...0x10fffd).contains($0.value)
      || $0.value == 0x2028 || $0.value == 0x2029
  }
}
public struct ComputerActionEffect: Codable, Equatable, Sendable {
  public let kind: String
  public let text: String?
}
public struct ComputerActionIntent: Codable, Sendable {
  public let observation: ComputerExpectedView
  public let element_id: String
  public let effect: ComputerActionEffect
  public func validate(now: Int64) throws {
    try observation.validate(now: now)
    guard let element = observation.elements.first(where: { $0.id == element_id }),
      (effect.kind == "press" && effect.text == nil && element.can_press)
        || (effect.kind == "set_text" && element.can_set_text && effect.text.map(computerInputText) == true)
    else { throw ComputerError.invalidRequest }
  }
  static func decode(_ value: [String: Any]) throws -> Self {
    func exact(_ value: Any?, _ keys: Set<String>) -> Bool {
      guard let object = value as? [String: Any] else { return false }
      return Set(object.keys) == keys
    }
    guard exact(value, ["observation", "element_id", "effect"]),
      let observation = value["observation"] as? [String: Any],
      exact(observation, ["binding", "target", "process_launched_at", "title", "frame", "elements", "truncated", "nonce", "observed_at", "expires_at", "image"]),
      exact(observation["binding"], ["operation_id", "task_id", "revision", "device", "epoch"]),
      exact(observation["target"], ["bundle_id", "pid", "window_id"]),
      exact(observation["frame"], ["x", "y", "width", "height"]),
      exact(observation["image"], ["digest", "length", "width", "height"]),
      let elements = observation["elements"] as? [[String: Any]], elements.count <= 128,
      elements.allSatisfy({ exact($0, ["id", "role", "label", "value", "can_press", "can_set_text"]) }),
      let effect = value["effect"] as? [String: Any],
      (effect["kind"] as? String == "press" && exact(effect, ["kind"]))
        || (effect["kind"] as? String == "set_text" && exact(effect, ["kind", "text"]))
    else { throw ComputerError.invalidRequest }
    let result = try JSONDecoder().decode(Self.self, from: JSONSerialization.data(withJSONObject: value))
    try result.validate(now: Int64(Date().timeIntervalSince1970 * 1000))
    return result
  }
}
public enum ComputerActionState: String, Codable, Sendable { case notExecuted = "not_executed", completed, unknown }
public struct ComputerActionReceipt: Codable, Sendable {
  public let binding: ComputerBinding
  public let source_nonce: String
  public let state: ComputerActionState
}
