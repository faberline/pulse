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
  @State private var started = false
  private var result: ComputerWindowList? {
    guard let requestID, model.state.computer_windows?.request_id == requestID else { return nil }
    return model.state.computer_windows
  }
  private var windows: [ComputerWindowChoice] { result?.windows ?? [] }
  private var message: String {
    if started && requestID == nil { return "本機對話連線尚未就緒。請重新連線。" }
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
              }.buttonStyle(.bordered)
            }
          }
        }
      }
      Spacer(minLength: 0)
      HStack { Spacer(); Button("取消") { dismiss() } }
    }.padding(22).frame(width: 520, height: 440)
      .task { started = true; requestID = model.requestComputerWindows() }
      .onDisappear { if let requestID { model.cancelComputerWindows(requestID) } }
  }
}
