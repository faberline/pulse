import AppKit
import ImageIO
import SwiftUI

@MainActor
final class ChatModel: ObservableObject {
  @Published var settings: ConnectionSettings
  @Published var state = NativeState()
  @Published var draft = ""
  @Published var notice = ""
  @Published var pairCode = ""
  @Published var showSettings = false
  @Published var showProgress = false
  @Published var image: NSImage?
  @Published var imageName = ""
  @Published var selectedTask: String?
  @Published private(set) var pending: PendingSubmit?
  private var activeBinding: [String]?
  private var generation = UUID()
  private lazy var transport = ChatTransport(
    onState: { [weak self] id, state in
      Task { @MainActor in self?.receive(state, generation: id) }
    },
    onExit: { [weak self] id in
      Task { @MainActor in
        guard let self, self.generation == id else { return }
        self.state.connected = false
        self.notice = "對話連線已停止。草稿會保留，可在設定重新連線。"
      }
    })

  init(settings: ConnectionSettings = .load()) {
    self.settings = settings
    connect()
  }
  var canSend: Bool {
    !draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty && draft.utf8.count <= 4096
      && pending == nil
  }
  var task: PulseTask? { state.tasks.first { $0.id == selectedTask } }
  var connectionLabel: String {
    state.connected ? "已連線" : state.pending > 0 ? "離線 · 有訊息待送" : "尚未連線"
  }
  func connect(save: Bool = false) {
    guard settings.valid else {
      notice = "請在設定填入伺服器位址。你可以先寫好訊息。"
      return
    }
    let switched = activeBinding != nil && activeBinding != settings.binding
    if switched {
      pending = nil
      state = NativeState()
      selectedTask = nil
    }
    activeBinding = settings.binding
    generation = UUID()
    transport.start(arguments: settings.arguments, generation: generation)
    settings.pairFile = nil
    notice = switched ? "連線已切換。舊連線的待送訊息須另行確認，草稿會保留。" : "正在連線…"
    if save { settings.save() }
  }
  func stop() { transport.stop() }
  @discardableResult
  private func send(_ command: [String: Any]) -> Bool {
    guard transport.send(command, generation: generation) else {
      notice = "訊息尚未送出。請重新連線；草稿會保留。"
      return false
    }
    return true
  }
  func submit() {
    guard canSend, let binding = activeBinding else { return }
    let value = PendingSubmit(
      sourceID: UUID().uuidString, taskID: UUID().uuidString, text: draft, binding: binding)
    if send(value.command) {
      pending = value
      retriedGeneration = generation
    }
  }
  func receive(_ value: NativeState, generation id: UUID) {
    guard generation == id else { return }
    state = value
    if let pending {
      if let remaining = pending.confirmedDraft(draft, by: value) {
        draft = remaining
        self.pending = nil
      } else if pending.binding == activeBinding {
        // A restart retries the original IDs, never a second task.
        // The transport journal, not UI polling, handles ordinary retries.
        retryPendingIfNeeded()
      }
    }
    if value.connected { notice = "" } else if !value.paired { notice = "請到設定完成對話框配對。" }
  }
  private var retriedGeneration: UUID?
  private func retryPendingIfNeeded() {
    guard retriedGeneration != generation, let pending else { return }
    if send(pending.command) { retriedGeneration = generation }
  }
  func pair() {
    guard pairCode.trimmingCharacters(in: .whitespacesAndNewlines).utf8.count == 64 else {
      notice = "請核對一次性配對碼。"
      return
    }
    if send(["kind": "pair", "code": pairCode.trimmingCharacters(in: .whitespacesAndNewlines)]) {
      pairCode = ""
    }
  }
  func action(_ action: [String: Any]) {
    send(["kind": "action", "source_id": UUID().uuidString, "action": action])
  }
  func cancelTask() {
    guard let task, !task.terminal else { return }
    action(["kind": "cancel", "task_id": task.id, "revision": task.revision])
  }
  func read(_ operation: [String: Any]) {
    guard let task, !task.terminal else { return }
    action(["kind": "read", "task_id": task.id, "revision": task.revision, "operation": operation])
  }
  func toggleLocal() { send(["kind": "kill_switch", "stopped": !state.stopped]) }
  func pickDataDirectory() {
    let panel = NSOpenPanel()
    panel.canChooseDirectories = true
    panel.canChooseFiles = false
    if panel.runModal() == .OK, let url = panel.url { settings.dataDir = url.path }
  }
  func pickImage() {
    let panel = NSOpenPanel()
    panel.allowedContentTypes = [.png, .jpeg]
    guard panel.runModal() == .OK, let url = panel.url else { return }
    do {
      let resource = try url.resourceValues(forKeys: [
        .isRegularFileKey, .isSymbolicLinkKey, .fileSizeKey,
      ])
      guard resource.isRegularFile == true, resource.isSymbolicLink != true,
        let size = resource.fileSize, size <= 5 * 1024 * 1024
      else { throw CocoaError(.fileReadTooLarge) }
      let file = try FileHandle(forReadingFrom: url)
      defer { try? file.close() }
      let data = try file.read(upToCount: 5 * 1024 * 1024 + 1) ?? Data()
      guard data.count <= 5 * 1024 * 1024,
        let source = CGImageSourceCreateWithData(data as CFData, nil),
        let type = CGImageSourceGetType(source) as String?,
        ["public.png", "public.jpeg"].contains(type),
        let info = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any],
        let width = info[kCGImagePropertyPixelWidth] as? Int,
        let height = info[kCGImagePropertyPixelHeight] as? Int,
        width > 0, height > 0, width <= 4096, height <= 4096,
        width * height * 4 <= 64 * 1024 * 1024,
        let decoded = CGImageSourceCreateImageAtIndex(source, 0, nil)
      else { throw CocoaError(.fileReadCorruptFile) }
      image = NSImage(cgImage: decoded, size: NSSize(width: width, height: height))
      imageName = url.lastPathComponent
    } catch { notice = "圖片無法開啟。只接受 5 MiB 內、最多 4096×4096 的 PNG 或 JPEG。" }
  }
}
