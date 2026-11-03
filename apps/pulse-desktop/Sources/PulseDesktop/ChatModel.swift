import AppKit
import ImageIO
import SwiftUI
import UniformTypeIdentifiers

@MainActor
final class ChatModel: ObservableObject {
  @Published var settings: ConnectionSettings
  @Published var state = NativeState()
  @Published var draft = ""
  @Published var notice = ""
  @Published var pairCode = ""
  @Published var showSettings = false
  @Published var showProgress = false
  @Published var showComputerPicker = false
  @Published var selectedComputer: ComputerWindowChoice?
  @Published private(set) var textInputSelecting: String?
  @Published private(set) var textInputClearing: String?
  @Published private(set) var textResultExporting: String?
  @Published var reselectingTask: PulseTask?
  @Published private(set) var pendingWindowReselection: PendingWindowReselection?
  private var reselectionGeneration: UUID?
  @Published var image: NSImage?
  @Published var imageName = ""
  @Published var browserImages: [String: NSImage] = [:]
  @Published var browserImageDigests: [String: String] = [:]
  @Published var browserDecisionPending: [String: String] = [:]
  private var loadingBrowserImages = Set<String>()
  private var requestedBrowserImages = Set<String>()
  private var failedBrowserImages = Set<String>()
  private var imageRequestInFlight: String?
  @Published var selectedTask: String?
  @Published private(set) var pending: PendingSubmit?
  private var activeBinding: [String]?
  private var generation = UUID()
  private let notificationPresenter = DesktopNotificationPresenter()
  private var notificationAuthorization = UUID()
  @Published private(set) var authorizingNotifications = false
  @Published var notificationNotice = ""
  private lazy var transport = ChatTransport(
    onState: { [weak self] id, state in
      Task { @MainActor in self?.receive(state, generation: id) }
    },
    onExit: { [weak self] id in
      Task { @MainActor in
        guard let self, self.generation == id else { return }
        self.notificationPresenter.cancelPending()
        self.state.connected = false
        self.state.memories = []; self.state.memory_synced_at = nil
        self.state.text_input = nil
        self.textInputSelecting = nil; self.textInputClearing = nil
        self.notice = self.textResultExporting == nil ? "對話連線已停止。草稿會保留，可在設定重新連線。" : "對話連線已停止，匯出狀態未確認。請檢查你選的位置；程式不會自動重試。"
        self.textResultExporting = nil
      }
    })

  init(settings: ConnectionSettings = .load()) {
    self.settings = settings
    connect()
  }
  var canSend: Bool {
    !draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty && draft.utf8.count <= 4096
      && pending == nil && textInputSelecting == nil && textInputClearing == nil
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
      selectedComputer = nil
      reselectingTask = nil; pendingWindowReselection = nil
      browserImages = [:]; browserImageDigests = [:]; loadingBrowserImages = []; requestedBrowserImages = []; failedBrowserImages = []; imageRequestInFlight = nil
    }
    activeBinding = settings.binding
    notificationPresenter.reset()
    textInputSelecting = nil; textInputClearing = nil
    textResultExporting = nil
    notificationAuthorization = UUID()
    authorizingNotifications = false
    state.memories = []; state.memory_synced_at = nil
    generation = UUID()
    transport.start(arguments: settings.arguments, generation: generation)
    settings.pairFile = nil
    notice = switched ? "連線已切換。舊連線的待送訊息須另行確認，草稿會保留。" : "正在連線…"
    if save { settings.save() }
  }
  func stop() { notificationPresenter.cancelPending(); transport.stop() }
  func setDesktopNotifications(_ enabled: Bool) {
    let request = UUID()
    notificationAuthorization = request
    guard enabled else {
      authorizingNotifications = false
      notificationPresenter.cancelPending()
      send(["kind":"set_desktop_notifications", "enabled":false])
      return
    }
    let currentGeneration = generation
    authorizingNotifications = true
    Task {
      let allowed = await notificationPresenter.authorize()
      guard generation == currentGeneration, notificationAuthorization == request else { return }
      authorizingNotifications = false
      notificationNotice = allowed ? "" : "通知尚未啟用。請在 macOS 系統設定核對 Pulse 通知權限。"
      if allowed { send(["kind":"set_desktop_notifications", "enabled":true]) }
    }
  }
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
      sourceID: UUID().uuidString, taskID: UUID().uuidString, text: draft, binding: binding,
      computer: selectedComputer?.selection, selectedAt: selectedComputer == nil ? nil : Int64(Date().timeIntervalSince1970 * 1000),
      textInputID: selectedComputer == nil ? state.text_input?.id : nil)
    if send(value.command) {
      pending = value
      retriedGeneration = generation
    }
  }
  func chooseComputerWindow(_ window: ComputerWindowChoice) {
    guard let task = reselectingTask else {selectedComputer = window;clearTextInput();return}
    guard task.canReselectWindow, state.tasks.contains(where: { $0.id == task.id && $0.revision == task.revision && $0.canReselectWindow }),
      let binding = activeBinding, pendingWindowReselection == nil else {
      notice = "任務已變更。請更新進度後重新選擇。";return
    }
    let pending = PendingWindowReselection(sourceID: UUID().uuidString, taskID: task.id, revision: task.revision,
      selection: window.selection, selectedAt: Int64(Date().timeIntervalSince1970 * 1000), binding: binding)
    if send(pending.command) {pendingWindowReselection = pending;reselectionGeneration = generation}
    reselectingTask = nil
  }
  func requestComputerWindows() -> String? {
    let id = UUID().uuidString
    return send(["kind":"list_computer_windows", "request_id":id]) ? id : nil
  }
  func cancelComputerWindows(_ id: String) {
    send(["kind":"cancel_computer_windows", "request_id":id])
  }
  func requestComputerPermission(_ permission: String) -> String? {
    let id = UUID().uuidString
    return send(["kind":"computer_permissions", "request_id":id, "request":permission]) ? id : nil
  }
  func cancelComputerPermissions(_ id: String) {
    send(["kind":"cancel_computer_permissions", "request_id":id])
  }
  func receive(_ value: NativeState, generation id: UUID) {
    guard generation == id else { return }
    state = value
    if let request = textResultExporting, let result = value.text_result_export,
      result.request_id == request, !["working","cancelling"].contains(result.status) {
      textResultExporting = nil
      switch result.status {
      case "saved": notice = "成果已保存：\(result.file_name ?? "Markdown")。"
      case "not_saved": notice = "成果未保存。請核對連線、來源與新檔名；程式不會覆寫既有檔案。"
      default: notice = "匯出狀態未確認。請檢查你選的位置；程式不會自動重試。"
      }
    }
    if let request = textInputSelecting,
      let result = value.text_input_request, result.request_id == request, !result.loading {
      textInputSelecting = nil
      if result.failed { notice = value.message }
      else if result.cancelled == true { notice = "選檔已取消。" }
      else { selectedComputer = nil; image = nil; notice = "已選入檔案副本。按傳送後才會交給模型。" }
    }
    if let clearing = textInputClearing, value.text_input?.id != clearing { textInputClearing = nil }
    if let offer = value.desktop_notifications?.current {
      notificationPresenter.present(offer) { [weak self] outcome in
        guard let self, self.generation == id else { return }
        self.send(["kind":"desktop_notification_result", "attempt_id":offer.attempt_id, "outcome":outcome])
      }
    } else { notificationPresenter.cancelPending() }
    if value.connected && value.computer_failure == nil { notice = "" }
    else if !value.paired { notice = "請到設定完成對話框配對。" }
    browserDecisionPending = value.connected ? browserDecisionPending.filter { id, previous in value.messages.first(where: { $0.approval?.proposal.id == id })?.approval?.state == previous } : [:]
    let liveImages = Set(value.messages.filter { $0.artifact != nil }.map(\.artifactID))
    browserImages = browserImages.filter { liveImages.contains($0.key) }
    browserImageDigests = browserImageDigests.filter { liveImages.contains($0.key) }
    for message in value.messages where message.artifact != nil && browserImageDigests[message.artifactID] != nil && browserImageDigests[message.artifactID] != message.artifact?.digest {
      browserImages.removeValue(forKey: message.artifactID); browserImageDigests.removeValue(forKey: message.artifactID); requestedBrowserImages.remove(message.artifactID)
    }
    let cachedArtifacts = value.artifacts ?? []
    if let id = imageRequestInFlight, cachedArtifacts.contains(where: { $0.operation_id == id }) || value.artifact_failure == id {
      imageRequestInFlight = nil
    }
    for cached in cachedArtifacts where browserImages[cached.operation_id] == nil && !loadingBrowserImages.contains(cached.operation_id) && !failedBrowserImages.contains(cached.operation_id) {
      loadingBrowserImages.insert(cached.operation_id)
      let generation = generation
      let directory = settings.dataDir
      Task {
        let data = await Task.detached { verifiedBrowserImageData(cached, directory: directory) }.value
        guard self.generation == generation else { return }
        self.loadingBrowserImages.remove(cached.operation_id)
        if let data, let image = NSImage(data: data), self.state.messages.contains(where: { $0.artifactID == cached.operation_id && $0.artifact?.digest == cached.artifact.digest }) {
          self.browserImages[cached.operation_id] = image; self.browserImageDigests[cached.operation_id] = cached.artifact.digest
        }
        else { self.failedBrowserImages.insert(cached.operation_id) }
      }
    }
    if value.connected, imageRequestInFlight == nil, value.artifact_loading == nil,
      let message = value.messages.first(where: { message in message.artifact != nil && !requestedBrowserImages.contains(message.artifactID) && !cachedArtifacts.contains(where: { $0.operation_id == message.artifactID }) }) {
      loadBrowserArtifact(message)
    }
    if let pending {
      if let remaining = pending.confirmedDraft(draft, by: value) {
        draft = remaining
        if selectedComputer?.selection == pending.computer { selectedComputer = nil }
        if let file = pending.textInputID, value.text_input?.id == file { clearTextInput() }
        self.pending = nil
      } else if value.computer_failure == pending.sourceID || value.input_failure == pending.sourceID {
        self.pending = nil; notice = value.message
      } else if pending.binding == activeBinding {
        // A restart retries the original IDs, never a second task.
        // The transport journal, not UI polling, handles ordinary retries.
        retryPendingIfNeeded()
      }
    }
    if let pending = pendingWindowReselection {
      if let receipt = value.computer_selections?.first(where: { $0.source_id == pending.sourceID && $0.task_id == pending.taskID }) {
        pendingWindowReselection = nil
        notice = receipt.state == "invalidated" ? "任務已變更，這次視窗選擇未套用。" : "視窗選擇已保存，等待本機核對。"
      } else if value.computer_failure == pending.sourceID {pendingWindowReselection = nil;notice = value.message}
      else if pending.binding == activeBinding && reselectionGeneration != generation {
        if send(pending.command) {reselectionGeneration = generation}
      }
    }
    if value.computer_selecting != nil { notice = "正在核對選定的程式視窗…" }
    else if value.computer_failure != nil { notice = value.message }
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
  func loadBrowserArtifact(_ message: ChatMessage) {
    guard let artifact = message.artifact, imageRequestInFlight == nil else { return }
    failedBrowserImages.remove(message.artifactID)
    if send(["kind": "load_artifact", "operation_id": message.artifactID, "digest": artifact.digest]) {
      requestedBrowserImages.insert(message.artifactID); imageRequestInFlight = message.artifactID
    }
  }
  func decideBrowserOperation(_ request: BrowserApproval, approve: Bool) {
    guard browserDecisionPending[request.proposal.id] == nil else { return }
    let decision: [String: Any] = ["kind": "decide_operation", "binding": ["task_id": request.proposal.token.task_id, "expected_revision": request.proposal.token.revision, "request_id": request.proposal.id, "intent_digest": request.intent_digest], "approve": approve]
    if send(["kind": "action", "source_id": UUID().uuidString, "action": decision]) { browserDecisionPending[request.proposal.id] = request.state }
    else { notice = "操作決定尚未保存。請重新連線後再試。" }
  }
  func toggleLocal() { send(["kind": "kill_switch", "stopped": !state.stopped]) }
  func pickDataDirectory() {
    let panel = NSOpenPanel()
    panel.canChooseDirectories = true
    panel.canChooseFiles = false
    if panel.runModal() == .OK, let url = panel.url { settings.dataDir = url.path }
  }
  func pickImage() {
    guard pending == nil, textInputSelecting == nil, textInputClearing == nil else { return }
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
      clearTextInput()
      image = NSImage(cgImage: decoded, size: NSSize(width: width, height: height))
      imageName = url.lastPathComponent
    } catch { notice = "圖片無法開啟。只接受 5 MiB 內、最多 4096×4096 的 PNG 或 JPEG。" }
  }
  func pickTextFile() {
    guard pending == nil, textInputSelecting == nil, textInputClearing == nil else { return }
    let panel = NSOpenPanel()
    panel.title = "選擇 UTF-8 文字檔案（最多 32 KiB）"
    panel.canChooseDirectories = false
    panel.allowsMultipleSelection = false
    panel.allowedContentTypes = [.text, .json] + ["md","rs","toml","yaml","yml","csv"].compactMap { UTType(filenameExtension:$0) }
    guard panel.runModal() == .OK, let url = panel.url else { return }
    let id = UUID().uuidString
    if send(["kind":"select_text_file","request_id":id,"path":url.path]) { textInputSelecting = id }
  }
  func cancelTextFileSelection() {
    if let id = textInputSelecting { send(["kind":"cancel_text_file_selection","request_id":id]) }
  }
  func clearTextInput() {
    guard let file = state.text_input, textInputClearing == nil else { return }
    if send(["kind":"clear_text_file","input_id":file.id]) { textInputClearing = file.id }
  }
  func saveTextResult(_ message: ChatMessage) {
    guard state.connected, textResultExporting == nil else { return }
    let panel = NSSavePanel()
    panel.title = "保存分析草稿"
    panel.message = "只建立新檔案，不會覆寫。成果保留原任務與來源。"
    panel.nameFieldStringValue = "Pulse-\(UUID().uuidString.prefix(8)).md"
    panel.allowedContentTypes = [UTType(filenameExtension:"md") ?? .plainText]
    panel.canCreateDirectories = false
    guard panel.runModal() == .OK, let url = panel.url else { return }
    let id = UUID().uuidString
    if send(TextResultExportCommand.make(requestID:id,messageID:message.id,path:url.path)) { textResultExporting = id }
  }
  func cancelTextResultExport() {
    if let id = textResultExporting { send(["kind":"cancel_text_result_export","request_id":id]) }
  }
}
