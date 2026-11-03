import SwiftUI

struct ComputerWindowChoice: Decodable, Identifiable, Equatable, Sendable {
  let selection: ComputerArtifactSource.Selection
  let app_name: String
  let title: String
  var appName: String { app_name }
  var id: UInt32 { selection.target.window_id }
}

/// Display only. Rust obtains the local list and validates the Human selection.
struct ComputerWindowPicker: View {
  @ObservedObject var model: ChatModel
  var taskGoal: String? = nil
  let choose: (ComputerWindowChoice) -> Void
  @Environment(\.dismiss) private var dismiss
  @State private var requestID: String?
  @State private var permissionID: String?
  @State private var started = false
  private var result: ComputerWindowList? {
    guard let requestID, model.state.computer_windows?.request_id == requestID else { return nil }
    return model.state.computer_windows
  }
  private var windows: [ComputerWindowChoice] { result?.windows ?? [] }
  private var access: ComputerPermissionState? {
    guard let access = model.state.computer_permissions,
      access.request_id == (permissionID ?? requestID) else { return nil }
    return access
  }
  private var busy: Bool {
    (requestID != nil && result == nil) || result?.loading == true
      || (permissionID != nil && access == nil) || access?.loading == true
  }
  private func requestPermission(_ permission: String) {
    guard !busy else { return }
    uiDiagnostic("permission_button")
    permissionID = model.requestComputerPermission(permission)
    uiDiagnostic(permissionID == nil ? "permission_not_queued" : "permission_queued")
  }
  private var message: String {
    if started && requestID == nil { return "本機對話連線尚未就緒。請重新連線。" }
    if let flags = access?.permissions, !flags.screen_capture || !flags.accessibility {
      return "請先完成所需授權，再按「重新檢查」。"
    }
    if result?.failed == true { return "視窗清單尚未取得。請核對本機連線與螢幕錄製權限，再重新選擇。" }
    if result == nil || result?.loading == true { return "正在讀取可選視窗…" }
    return "沒有可選的程式視窗。請先開啟要讀取的程式。"
  }
  var body: some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("選擇程式視窗").font(.title2)
      if let taskGoal { Text("替這個任務重新選擇：\(taskGoal)").lineLimit(2) }
      Text("清單只留在本機。選定視窗的文字會交給模型，原圖會顯示在對話中。這不會點擊或輸入。")
        .font(.callout).foregroundStyle(.secondary)
      if let status = access {
        if let flags = status.permissions {
          Text("輔助使用：\(flags.accessibility ? "已允許" : "未允許") · 螢幕錄製：\(flags.screen_capture ? "已允許" : "未允許")")
            .font(.callout)
          HStack {
            if !flags.accessibility {
              Button("要求輔助使用授權") { requestPermission("accessibility") }
            }
            if !flags.screen_capture {
              Button("要求螢幕錄製授權") { requestPermission("screen_capture") }
            }
          }.disabled(busy)
        }
        if status.loading { Text("正在處理權限要求…").font(.callout) }
        else if status.failed { Text("無法取得權限狀態。請核對地端執行器，再重新檢查。").font(.callout) }
      }
      if permissionID != nil {
        Text("系統授權由你決定。若提示或設定已開啟，完成後請按「重新檢查」。關閉此頁不會撤銷已開啟的系統提示。")
          .font(.callout).foregroundStyle(.secondary)
      }
      if windows.isEmpty { Text(message).padding(.vertical) }
      else {
        ScrollView {
          LazyVStack(alignment: .leading, spacing: 8) {
            ForEach(windows) { window in
              Button { choose(window); dismiss() } label: {
                VStack(alignment: .leading) {
                  Text(window.appName).font(.headline)
                  Text(window.title).lineLimit(2)
                }.frame(maxWidth: .infinity, alignment: .leading).padding(10)
              }.buttonStyle(.bordered).disabled(busy)
            }
          }
        }
      }
      Spacer(minLength: 0)
      HStack {
        Button("重新檢查") {
          guard !busy else { return }
          if let permissionID { model.cancelComputerPermissions(permissionID) }
          permissionID = nil
          requestID = model.requestComputerWindows()
        }.disabled(busy)
        Spacer(); Button("取消") { dismiss() }
      }
    }.padding(22).frame(width: 560, height: 540)
      .task {
        uiDiagnostic("window_picker_opened")
        started = true; requestID = model.requestComputerWindows()
      }
      .onDisappear {
        uiDiagnostic("window_picker_closed")
        if let requestID { model.cancelComputerWindows(requestID) }
        if let permissionID { model.cancelComputerPermissions(permissionID) }
      }
  }
}
