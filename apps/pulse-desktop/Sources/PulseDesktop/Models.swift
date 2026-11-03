import Foundation

struct ChatMessage: Decodable, Identifiable, Sendable {
  let id: String
  let body: String
  let artifact: BrowserArtifact?
  let artifact_operation_id: String?
  let approval: BrowserApproval?
  var artifactID: String { artifact_operation_id ?? id }
  init(id: String, body: String, artifact: BrowserArtifact? = nil, artifact_operation_id: String? = nil, approval: BrowserApproval? = nil) {
    self.id = id; self.body = body; self.artifact = artifact; self.artifact_operation_id = artifact_operation_id; self.approval = approval
  }
  var isEvidence: Bool { body.hasPrefix("只讀工具結果") || body.hasPrefix("操作工具結果") }
  var isUser: Bool { ["已保存任務：", "已保存訊息：", "本機待送：", "雲端已收件："].contains { body.hasPrefix($0) } }
  var displayBody: String {
    var value = body
    for prefix in ["已保存任務：", "已保存訊息：", "本機待送：", "雲端已收件："] where value.hasPrefix(prefix) {
      value.removeFirst(prefix.count)
    }
    return value.replacingOccurrences(of: "\n\n模型尚未接入。可選任務後使用只讀工具。", with: "")
  }
  var label: String { body.hasPrefix("本機待送：") ? "你 · 待送" : isUser ? "你 · 已保存" : "Pulse" }
}
struct PulseTask: Decodable, Identifiable, Sendable {
  enum CodingKeys: String, CodingKey {
    case id, goal, revision, state, scope, reason
    case cancelRequested = "cancel_requested"
  }
  let id: String
  let goal: String
  let revision: String
  let state: String
  let scope: String?
  let reason: String?
  let cancelRequested: Bool
  var terminal: Bool { ["succeeded", "failed", "cancelled"].contains(state) }
  var canReselectWindow: Bool {
    let recoverable = ["queued", "waiting_device", "waiting_approval", "ready"].contains(state)
      || (state == "blocked" && reason == "computer_action_not_executed")
    return scope?.hasPrefix("computer:") == true && !cancelRequested && recoverable
  }
  var status: String {
    if reason == "browser_capability_wait" { return "等待本機瀏覽器；請核對安裝與網址範圍" }
    if reason == "computer_capability_wait" || reason == "computer_selection_wait" { return "等待選定視窗或電腦存取權限" }
    return [
      "queued": "排隊中", "planning": "規劃中", "dispatched": "已派工", "running": "執行中",
      "waiting_approval": "等待操作批准", "ready": "已批准，等待執行",
      "blocked": "等待下一步", "waiting_device": "等待本機", "cancelled": "已取消",
      "succeeded": "已完成", "failed": "未完成", "outcome_unknown": "結果待核對",
    ][state] ?? "等待處理"
  }
}
struct NativeState: Decodable, Sendable {
  enum CodingKeys: String, CodingKey {
    case server, connected, stopped, paired, message, pending, tasks, messages, artifacts, artifact_loading, artifact_failure, computer_selecting, computer_failure, computer_selections, memories, memory_synced_at, input_failure, computer_windows, computer_permissions, text_input, text_input_request
    case localConnected = "local_connected"
    case desktop_notifications
    case syncedAt = "synced_at"
  }
  var server = ""
  var connected = false
  var stopped = false
  var localConnected = false
  var paired = false
  var message = ""
  var pending = 0
  var syncedAt: Int64?
  var tasks: [PulseTask] = []
  var messages: [ChatMessage] = []
  var memories: [PulseMemory]?
  var memory_synced_at: Int64?
  var input_failure: String?
  var artifacts: [LocalBrowserArtifact]?
  var artifact_loading: String?
  var artifact_failure: String?
  var computer_selecting: String?
  var computer_failure: String?
  var computer_selections: [ComputerSelectionStatus]?
  var computer_windows: ComputerWindowList?
  var computer_permissions: ComputerPermissionState?
  var text_input: TextInputReference?
  var text_input_request: TextInputRequest?
  var desktop_notifications: DesktopNotifications?
}
struct TextInputReference: Decodable, Equatable, Sendable {
  let id: String
  let name: String
  let digest: String
  let length: UInt32
  let captured_at: Int64
}
struct TextInputRequest: Decodable, Sendable {
  let request_id: String
  let loading: Bool
  let failed: Bool
  let cancelled: Bool?
}
struct ComputerPermissionState: Decodable, Sendable {
  let request_id: String
  let loading: Bool
  let failed: Bool
  let permissions: ComputerPermissions?
}
struct ComputerPermissions: Decodable, Sendable {
  let accessibility: Bool
  let screen_capture: Bool
}
struct ComputerWindowList: Decodable, Sendable {
  let request_id: String
  let loading: Bool
  let failed: Bool
  let windows: [ComputerWindowChoice]
}
struct ComputerSelectionStatus: Decodable, Sendable {let source_id: String;let task_id: String;let revision: String;let state: String}
struct PulseMemory: Decodable, Identifiable, Sendable {
  let id: String
  let revision: UInt64
  let label: String
  let body: String
  let source_id: String
  let source_at: Int64
  let expires_at: Int64
  var controlName: String { (try? JSONEncoder().encode(label)).flatMap {String(data: $0, encoding: .utf8)} ?? "" }
  func live(at now: Int64) -> Bool {source_at >= 0 && revision > 0 && source_at <= now && expires_at > now && expires_at - source_at == 90 * 24 * 60 * 60 * 1000}
}
struct ConnectionSettings: Codable, Equatable {
  var server = ""
  var dataDir = FileManager.default.homeDirectoryForCurrentUser
    .appendingPathComponent("Library/Application Support/Pulse/chat").path
  var development = false
  // One launch only. Never saved in preferences or sent as a credential.
  var pairFile: String?
  enum CodingKeys: String, CodingKey { case server, dataDir, development }
  var arguments: [String] {
    ["--server", server, "--data-dir", dataDir] + (development ? ["--dev-loopback"] : [])
      + (pairFile.map { ["--pair-code-file", $0] } ?? [])
  }
  var binding: [String] { [serverBinding(server), dataDir] }
  var valid: Bool {
    guard let url = URLComponents(string: server), let host = url.host,
      server.utf8.count <= 2048, url.user == nil, url.password == nil,
      url.query == nil, url.fragment == nil, ["", "/"].contains(url.path), !dataDir.isEmpty
    else { return false }
    return url.scheme == "https"
      || (development && url.scheme == "http"
        && (host == "127.0.0.1" || host == "[::1]" || host == "::1"))
  }
  static func load(arguments: [String] = Array(CommandLine.arguments.dropFirst())) -> Self {
    if !arguments.isEmpty {
      var settings = Self()
      var index = 0
      while index < arguments.count {
        switch arguments[index] {
        case "--dev-loopback": settings.development = true
        case "--server", "--data-dir", "--pair-code-file":
          guard index + 1 < arguments.count else { return Self() }
          if arguments[index] == "--server" {
            settings.server = arguments[index + 1]
          } else if arguments[index] == "--data-dir" {
            settings.dataDir = arguments[index + 1]
          } else {
            settings.pairFile = arguments[index + 1]
          }
          index += 1
        default: return Self()
        }
        index += 1
      }
      return settings
    }
    // Only non-secret settings live in preferences. Credentials stay in Rust/Keychain.
    return UserDefaults.standard.data(forKey: "pulse.swift.connection")
      .flatMap { try? JSONDecoder().decode(Self.self, from: $0) } ?? Self()
  }
  func save() {
    UserDefaults.standard.set(try? JSONEncoder().encode(self), forKey: "pulse.swift.connection")
  }
}
struct PendingSubmit {
  let sourceID: String
  let taskID: String
  let text: String
  let binding: [String]
  var computer: ComputerArtifactSource.Selection? = nil
  var selectedAt: Int64? = nil
  var textInputID: String? = nil
  var command: [String: Any] {
    if let textInputID {
      return ["kind":"submit_text_file","source_id":sourceID,"id":taskID,"goal":text,"input_id":textInputID]
    }
    if let computer, let selectedAt {
      return ["kind":"submit_computer", "source_id":sourceID,"id":taskID,"goal":text,
        "selection":computer.wireValue,"selected_at":selectedAt]
    }
    return [
      "kind": "action", "source_id": sourceID,
      "action": ["kind": "submit", "id": taskID, "goal": text],
    ]
  }
  func confirmed(by state: NativeState) -> Bool {
    serverBinding(state.server) == binding[0]
      && (state.messages.contains { $0.id == sourceID } || state.tasks.contains { $0.id == taskID })
  }
  func confirmedDraft(_ draft: String, by state: NativeState) -> String? {
    confirmed(by: state) ? (draft == text ? "" : draft) : nil
  }
}
func serverBinding(_ value: String) -> String {
  guard var parts = URLComponents(string: value), let scheme = parts.scheme, let host = parts.host
  else { return value }
  parts.scheme = scheme.lowercased()
  parts.host = host.lowercased()
  parts.path = "/"
  if (parts.scheme == "https" && parts.port == 443) || (parts.scheme == "http" && parts.port == 80)
  {
    parts.port = nil
  }
  return parts.string ?? value
}

struct PendingWindowReselection {
  let sourceID: String
  let taskID: String
  let revision: String
  let selection: ComputerArtifactSource.Selection
  let selectedAt: Int64
  let binding: [String]
  var command: [String: Any] {
    ["kind":"rebind_computer","source_id":sourceID,"task_id":taskID,"revision":revision,
      "selection":selection.wireValue,"selected_at":selectedAt]
  }
}
func safeLink(_ url: URL) -> Bool {
  guard let value = URLComponents(url: url, resolvingAgainstBaseURL: false) else { return false }
  return value.scheme == "https" && value.host != nil && value.user == nil && value.password == nil
}

/// Incremental bounded JSON Lines. A partial frame never grows past 1 MiB.
struct FrameDecoder {
  static let limit = 1024 * 1024
  private var pending = Data()
  mutating func accept(_ bytes: Data) throws -> [Data] {
    var frames: [Data] = []
    var start = bytes.startIndex
    for index in bytes.indices where bytes[index] == 10 {
      guard pending.count + index - start <= Self.limit else { throw CocoaError(.fileReadTooLarge) }
      pending.append(bytes[start..<index])
      frames.append(pending)
      pending = Data()
      start = index + 1
    }
    guard pending.count + bytes.endIndex - start <= Self.limit else {
      throw CocoaError(.fileReadTooLarge)
    }
    pending.append(bytes[start..<bytes.endIndex])
    return frames
  }
}
