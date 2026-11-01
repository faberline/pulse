import SwiftUI
import PulseComputerCore

struct BrowserApproval: Decodable, Sendable {
  struct Token: Decodable, Sendable { let task_id: String; let revision: String }
  struct Form: Decodable, Sendable { let url: String; let method: String; let fields: [[String]] }
  struct ControlKind: Decodable, Sendable { let kind: String; let value: String?; let form: Form? }
  struct Control: Decodable, Sendable { let id: String; let label: String; let kind: ControlKind }
  struct Observation: Decodable, Sendable {
    let url: String; let title: String; let png_digest: String; let elements: [Control]
    let observed_at: Int64; let expires_at: Int64
  }
  struct Effect: Decodable, Sendable { let kind: String; let text: String? }
  struct Intent: Decodable, Sendable {
    let source_operation_id: String; let observation: Observation; let element_id: String; let effect: Effect
    let computer: ComputerActionIntent?
    enum CodingKeys: String, CodingKey { case source_operation_id, observation, element_id, effect }
    enum SourceKeys: String, CodingKey { case binding }
    init(from decoder: Decoder) throws {
      let values = try decoder.container(keyedBy: CodingKeys.self)
      let source = try values.nestedContainer(keyedBy: SourceKeys.self, forKey: .observation)
      element_id = try values.decode(String.self, forKey: .element_id)
      effect = try values.decode(Effect.self, forKey: .effect)
      if source.contains(.binding) {
        let intent = try ComputerActionIntent(from: decoder); computer = intent
        let view = intent.observation
        source_operation_id = view.binding.operation_id
        observation = Observation(url: "", title: view.title, png_digest: view.image.digest,
          elements: view.elements.map { Control(id: $0.id, label: $0.label,
            kind: ControlKind(kind: $0.can_press ? "button" : "text", value: $0.value, form: nil)) },
          observed_at: view.observed_at, expires_at: view.expires_at)
      } else {
        computer = nil
        source_operation_id = try values.decode(String.self, forKey: .source_operation_id)
        observation = try values.decode(Observation.self, forKey: .observation)
      }
    }
  }
  struct Proposal: Decodable, Sendable { let id: String; let token: Token; let intent: Intent; let expires_at: Int64; let target: String? }
  let proposal: Proposal
  let intent_digest: String
  let state: String
  let reason: String?
  func canApprove(task: PulseTask?, connected: Bool, artifact: BrowserArtifact?, verifiedDigest: String?, hasImage: Bool, pending: Bool, now: Int64) -> Bool {
    guard let task else { return false }
    guard connected && !pending && hasImage && state == "pending"
      && task.id == proposal.token.task_id && task.revision == proposal.token.revision
      && !task.cancelRequested && !task.terminal && task.state == "waiting_approval"
      && now < proposal.expires_at && now < proposal.intent.observation.expires_at
      && artifact?.task_id == task.id && artifact?.revision == task.revision
      && artifact?.digest == proposal.intent.observation.png_digest
      && verifiedDigest == proposal.intent.observation.png_digest
    else { return false }
    if let native = proposal.intent.computer {
      guard let artifact, artifact.validSource, let source = artifact.computer,
        (try? native.validate(now: now)) != nil else { return false }
      let view = native.observation
      let target = view.target
      let scope = "computer:\(target.bundle_id):\(target.pid):\(target.window_id):\(view.process_launched_at)"
      return task.scope == scope && proposal.target == scope && artifact.url.isEmpty
        && source.binding.operation_id == view.binding.operation_id && source.binding.task_id == view.binding.task_id
        && source.binding.revision == view.binding.revision && source.binding.device == view.binding.device && source.binding.epoch == view.binding.epoch
        && view.binding.task_id == task.id && view.binding.revision == task.revision
        && source.selection.target.bundle_id == target.bundle_id && source.selection.target.pid == target.pid
        && source.selection.target.window_id == target.window_id && source.selection.process_launched_at == view.process_launched_at
        && artifact.title == view.title && artifact.observed_at == view.observed_at
        && artifact.length == view.image.length && artifact.width == view.image.width && artifact.height == view.image.height
    }
    return artifact?.computer == nil && artifact?.url == proposal.intent.observation.url
  }
  var control: Control? { proposal.intent.observation.elements.first { $0.id == proposal.intent.element_id } }
  var status: String {
    ["pending": "等待批准", "approved": "批准已保存，等待本機", "rejected": "已拒絕", "withdrawn": "已撤回",
     "expired": "畫面已過期", "invalidated": "任務或畫面已變更", "dispatched": "批准已使用，請看操作回報"][state] ?? "操作已停止"
  }
}

struct BrowserApprovalCard: View {
  @ObservedObject var model: ChatModel
  let message: ChatMessage
  let request: BrowserApproval
  var currentTask: Bool {
    model.state.tasks.contains { $0.id == request.proposal.token.task_id && $0.revision == request.proposal.token.revision && !$0.cancelRequested && !$0.terminal }
  }
  var imageReady: Bool {
    message.artifact?.digest == request.proposal.intent.observation.png_digest
      && message.artifact?.task_id == request.proposal.token.task_id
      && message.artifact?.revision == request.proposal.token.revision
      && model.browserImageDigests[message.artifactID] == request.proposal.intent.observation.png_digest
      && model.browserImages[message.artifactID] != nil
  }
  var canApprove: Bool {
    request.canApprove(task: model.state.tasks.first(where: { $0.id == request.proposal.token.task_id }),
      connected: model.state.connected, artifact: message.artifact,
      verifiedDigest: model.browserImageDigests[message.artifactID],
      hasImage: model.browserImages[message.artifactID] != nil,
      pending: model.browserDecisionPending[request.proposal.id] != nil,
      now: Int64(Date().timeIntervalSince1970 * 1000))
  }
  var body: some View {
    VStack(alignment: .leading, spacing: 12) {
      Text(request.status).font(.headline)
      if let control = request.control {
        if ["click", "press"].contains(request.proposal.intent.effect.kind) {
          Text("點擊「\(control.label.isEmpty ? "未命名按鈕" : control.label)」")
          if let form = control.kind.form {
            Text("這會送出表單。目標：\(form.url)")
            Text("方式：\(form.method.uppercased())").font(.caption)
            ForEach(Array(form.fields.enumerated()), id: \.offset) { _, pair in
              if pair.count == 2 { Text("\(pair[0])：\(pair[1])").textSelection(.enabled) }
            }
          }
        } else {
          Text("把「\(control.label.isEmpty ? "文字欄位" : control.label)」的內容改為：")
          Text(request.proposal.intent.effect.text ?? "").textSelection(.enabled)
          Text(request.proposal.intent.computer == nil ? "只改這個欄位，不送出表單。" : "只設定這個欄位的文字，不按 Enter。")
            .font(.caption).foregroundStyle(.secondary)
        }
      }
      if let artifact = message.artifact { BrowserScreenshot(model: model, message: message, artifact: artifact) }
      Text("有效至：\(Date(timeIntervalSince1970: Double(request.proposal.expires_at) / 1000).formatted())")
        .font(.caption).foregroundStyle(.secondary)
      if request.state == "pending" || request.state == "approved" {
        HStack {
          if request.state == "pending" {
            Button("批准並執行") { model.decideBrowserOperation(request, approve: true) }
              .buttonStyle(.borderedProminent).disabled(!canApprove)
          }
          Button(request.state == "approved" ? "撤回批准" : "拒絕") { model.decideBrowserOperation(request, approve: false) }
            .disabled(!currentTask || !model.state.connected || model.browserDecisionPending[request.proposal.id] != nil)
        }
        if !currentTask { Text("任務已變更，這份提議不能使用。").font(.caption) }
        else if !imageReady { Text("先取得並核對截圖，才可批准。").font(.caption) }
      }
    }
  }
}
