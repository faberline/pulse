import Foundation

public enum ComputerError: String, Error, Codable, Sendable {
  case invalidRequest = "invalid_request"
  case permissionsRequired = "permissions_required"
  case targetUnavailable = "target_unavailable"
  case targetChanged = "target_changed"
  case unsupportedWindow = "unsupported_window"
  case accessibilityUnavailable = "accessibility_unavailable"
  case captureFailed = "capture_failed"
  case evidenceLimit = "evidence_limit"
  case privateStorageRequired = "private_storage_required"
}

public struct ComputerPermissions: Codable, Sendable {
  public let accessibility: Bool
  public let screen_capture: Bool
  public var canObserve: Bool { accessibility && screen_capture }
  public init(accessibility: Bool, screen_capture: Bool) {
    self.accessibility = accessibility; self.screen_capture = screen_capture
  }
}

/// These values are selected by the local operator. They are not model arguments.
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

public struct ComputerSelection: Codable, Equatable, Sendable {
  public let target: ComputerTarget
  public let process_launched_at: Int64
}

public struct ComputerRequest: Sendable {
  public let kind: String
  public let binding: ComputerBinding?
  public let expectedProcessLaunchedAt: Int64?
  public static func decode(_ data: Data) throws -> Self {
    do { return try decodeValue(data) }
    catch { throw ComputerError.invalidRequest }
  }
  private static func decodeValue(_ data: Data) throws -> Self {
    guard data.count <= 16_384,
      let value = try JSONSerialization.jsonObject(with: data) as? [String: Any],
      let kind = value["kind"] as? String else { throw ComputerError.invalidRequest }
    if kind == "status", Set(value.keys) == ["kind"] { return Self(kind: kind, binding: nil, expectedProcessLaunchedAt: nil) }
    let keys = Set(value.keys)
    if kind == "probe", keys == ["kind", "expected_process_launched_at"],
      let text = value["expected_process_launched_at"] as? String,
      let number = Int64(text), number > 0, String(number) == text {
      return Self(kind: kind, binding: nil, expectedProcessLaunchedAt: number)
    }
    guard kind == "observe", keys == ["kind", "binding"] || keys == ["kind", "binding", "expected_process_launched_at"],
      let binding = value["binding"] as? [String: Any],
      Set(binding.keys) == ["operation_id", "task_id", "revision", "device", "epoch"]
    else { throw ComputerError.invalidRequest }
    let decoded = try JSONDecoder().decode(ComputerBinding.self, from: JSONSerialization.data(withJSONObject: binding))
    try decoded.validate()
    var launched: Int64? = nil
    if keys.contains("expected_process_launched_at") {
      guard let text = value["expected_process_launched_at"] as? String,
        let number = Int64(text), number > 0, String(number) == text
      else { throw ComputerError.invalidRequest }
      launched = number
    }
    return Self(kind: kind, binding: decoded, expectedProcessLaunchedAt: launched)
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

public struct ComputerImage: Codable, Sendable {
  public let digest: String
  public let length: Int
  public let width: Int
  public let height: Int
  /// Local IPC only. The Rust integration must not forward this path to server/model.
  public let path: String
}

public struct ComputerObservation: Codable, Sendable {
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
  public let image: ComputerImage
}

public struct ComputerResponse: Codable, Sendable {
  public let version: String
  public let permissions: ComputerPermissions
  public let observation: ComputerObservation?
  public let error: ComputerError?
  public let selection: ComputerSelection?
  public init(permissions: ComputerPermissions, observation: ComputerObservation? = nil, error: ComputerError? = nil, selection: ComputerSelection? = nil) {
    version = "pulse-computer-observe-3"; self.permissions = permissions
    self.observation = observation; self.error = error
    self.selection = selection
  }
}

func boundedText(_ text: String, bytes: Int) -> String {
  guard text.utf8.count > bytes else { return text }
  var data = Data(text.utf8.prefix(bytes))
  while !data.isEmpty {
    if let value = String(data: data, encoding: .utf8) { return value }
    data.removeLast()
  }
  return ""
}
