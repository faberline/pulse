import Foundation

// Approval display data and local button guards only. Rust independently
// validates every command, source and grant before any native execution.
enum ComputerError: Error { case invalidRequest, evidenceLimit }


public struct ComputerTarget: Codable, Equatable, Sendable {
  public let bundle_id: String
  public let pid: Int32
  public let window_id: UInt32
  public init(bundleID: String, pid: Int32, windowID: UInt32) throws {
    let parts = bundleID.split(separator: ".", omittingEmptySubsequences: false)
    guard bundleID.utf8.count <= 255, parts.count >= 2, parts.allSatisfy({ !$0.isEmpty }),
      bundleID.utf8.allSatisfy({ (48...57).contains($0) || (65...90).contains($0) || (97...122).contains($0) || $0 == 45 || $0 == 46 }),
      pid > 0, windowID > 0 else { throw ComputerError.invalidRequest }
    bundle_id = bundleID; self.pid = pid; window_id = windowID
  }
}

public struct ComputerBinding: Codable, Equatable, Sendable {
  public let operation_id: String
  public let task_id: String
  public let revision: String
  public let device: String
  public let epoch: String
  public func validate() throws {
    func decimal(_ value: String) -> Bool { UInt64(value).map { $0 > 0 && String($0) == value } ?? false }
    guard !operation_id.isEmpty, operation_id.utf8.count <= 256,
      !task_id.isEmpty, task_id.utf8.count <= 128,
      !operation_id.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) }),
      !task_id.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) }),
      decimal(revision), decimal(epoch), UUID(uuidString: device) != nil
    else { throw ComputerError.invalidRequest }
  }
}

public struct ComputerRect: Codable, Equatable, Sendable {
  public let x: Double
  public let y: Double
  public let width: Double
  public let height: Double
  public init(_ rect: CGRect) throws {
    guard [rect.minX, rect.minY, rect.width, rect.height].allSatisfy({ $0.isFinite }),
      rect.width > 0, rect.height > 0, rect.width <= 8192, rect.height <= 8192
    else { throw ComputerError.evidenceLimit }
    x = rect.minX; y = rect.minY; width = rect.width; height = rect.height
  }
}

public struct ComputerElement: Codable, Equatable, Sendable {
  public let id: String
  public let role: String
  public let label: String
  public let value: String?
  public let can_press: Bool
  public let can_set_text: Bool
}


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

}

