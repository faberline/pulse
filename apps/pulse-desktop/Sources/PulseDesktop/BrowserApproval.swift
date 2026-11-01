import SwiftUI

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
  }
  struct Proposal: Decodable, Sendable { let id: String; let token: Token; let intent: Intent; let expires_at: Int64 }
  let proposal: Proposal
  let intent_digest: String
  let state: String
  let reason: String?
  func canApprove(task: PulseTask?, connected: Bool, artifact: BrowserArtifact?, verifiedDigest: String?, hasImage: Bool, pending: Bool, now: Int64) -> Bool {
    guard let task else { return false }
    return connected && !pending && hasImage && state == "pending"
      && task.id == proposal.token.task_id && task.revision == proposal.token.revision
      && !task.cancelRequested && !task.terminal && task.state == "waiting_approval"
      && now < proposal.expires_at && now < proposal.intent.observation.expires_at
      && artifact?.task_id == task.id && artifact?.revision == task.revision
      && artifact?.digest == proposal.intent.observation.png_digest
      && verifiedDigest == proposal.intent.observation.png_digest
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
        if request.proposal.intent.effect.kind == "click" {
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
          Text("只改這個欄位，不送出表單。").font(.caption).foregroundStyle(.secondary)
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
