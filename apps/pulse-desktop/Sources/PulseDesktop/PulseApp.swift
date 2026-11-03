import AppKit
import SwiftUI

@main
struct PulseApp: App {
  @NSApplicationDelegateAdaptor(AppDelegate.self) private var delegate
  @StateObject private var model = ChatModel()
  var body: some Scene {
    WindowGroup("Pulse") {
      ChatView(model: model)
        .frame(minWidth: 640, minHeight: 480)
        .onAppear {
          delegate.model = model
          NSApp.setActivationPolicy(.regular)
          NSApp.activate(ignoringOtherApps: true)
        }
    }
    .defaultSize(width: 900, height: 740)
    .commands {
      CommandGroup(after: .newItem) {
        Button("對話設定") { model.showSettings = true }.keyboardShortcut(",")
        Button("選取圖片") { model.pickImage() }.keyboardShortcut("i")
      }
    }
  }
}
final class AppDelegate: NSObject, NSApplicationDelegate {
  weak var model: ChatModel?
  func applicationDidFinishLaunching(_ notification: Notification) { signal(SIGPIPE, SIG_IGN) }
  func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool { true }
  func applicationWillTerminate(_ notification: Notification) { model?.stop() }
}
struct ChatView: View {
  @ObservedObject var model: ChatModel
  var body: some View {
    VStack(spacing: 0) {
      HStack {
        Text("Pulse").font(.title2.weight(.semibold))
        Spacer()
        Text(model.connectionLabel).font(.caption).foregroundStyle(.secondary)
        Button("進度") { model.showProgress.toggle() }.disabled(model.state.tasks.isEmpty)
        Button("設定") { model.showSettings = true }
      }.padding()
      Divider()
      HStack(spacing: 0) {
        VStack(spacing: 0) {
          ScrollViewReader { reader in
            ScrollView {
              LazyVStack(alignment: .leading, spacing: 16) {
                if model.state.messages.isEmpty {
                  VStack(alignment: .leading, spacing: 8) {
                    Text("從一句話開始。").font(.title)
                    Text("訊息與回覆會顯示在這裡。").foregroundStyle(.secondary)
                  }.padding(.vertical, 90).frame(maxWidth: .infinity, alignment: .center)
                }
                ForEach(model.state.messages) { message in
                  HStack {
                    if message.isUser { Spacer(minLength: 40) }
                    VStack(alignment: .leading, spacing: 8) {
                      Text(message.label).font(.caption).foregroundStyle(.secondary)
                      if message.isEvidence {
                        DisclosureGroup(String(message.body.split(separator: "\n").first ?? "工具證據")) {
                          RichContent(source: message.displayBody)
                        }
                      } else {
                        RichContent(source: message.displayBody)
                      }
                      if let approval = message.approval {
                        BrowserApprovalCard(model: model, message: message, request: approval)
                      } else if let artifact = message.artifact {
                        BrowserScreenshot(model: model, message: message, artifact: artifact)
                      }
                    }
                    .padding(14).frame(maxWidth: .infinity, alignment: .leading)
                    .background(
                      message.isUser
                        ? Color.accentColor.opacity(0.10) : Color.secondary.opacity(0.06),
                      in: RoundedRectangle(cornerRadius: 12))
                  }.id(message.id)
                }
                if let memories = model.state.memories, model.state.memory_synced_at != nil {
                  VStack(alignment: .leading, spacing: 12) {
                    Text(model.state.connected ? "目前記憶" : "記憶快取・離線時可能已變更").font(.headline)
                    if memories.isEmpty {Text("目前沒有有效記憶。").foregroundStyle(.secondary)}
                    ForEach(memories.filter { $0.live(at: Int64(Date().timeIntervalSince1970 * 1000)) }) { memory in
                      VStack(alignment: .leading, spacing: 5) {
                        Text(memory.label).font(.headline)
                        Text(memory.body).textSelection(.enabled)
                        Text("來源：你的明確要求・\(Date(timeIntervalSince1970: Double(memory.source_at) / 1000).formatted(date: .abbreviated, time: .shortened))")
                          .font(.caption).foregroundStyle(.secondary)
                        Text("到期：\(Date(timeIntervalSince1970: Double(memory.expires_at) / 1000).formatted(date: .abbreviated, time: .omitted))")
                          .font(.caption).foregroundStyle(.secondary)
                        HStack {
                          Button("修正") {model.draft = "更正記憶 \(memory.controlName)："}
                          Button("忘記") {model.draft = "忘記記憶 \(memory.controlName)"}
                        }.disabled(!model.draft.isEmpty || model.pending != nil || memory.controlName.isEmpty)
                      }
                    }
                  }.padding(14).frame(maxWidth: .infinity, alignment: .leading)
                    .background(Color.secondary.opacity(0.06), in: RoundedRectangle(cornerRadius: 12))
                }
              }.padding()
            }.onChange(of: model.state.messages.last?.id) { _, id in
              if let id { reader.scrollTo(id, anchor: .bottom) }
            }
          }
          if let image = model.image {
            HStack {
              Image(nsImage: image).resizable().scaledToFit().frame(width: 100, height: 64)
              VStack(alignment: .leading) {
                Text(model.imageName)
                Text("本機預覽，尚未上傳").font(.caption).foregroundStyle(.secondary)
              }
              Spacer()
              Button("移除") { model.image = nil }
            }.padding(.horizontal)
          }
          VStack(spacing: 8) {
            if let file = model.state.text_input {
              HStack {
                Image(systemName:"doc.text")
                VStack(alignment:.leading) {
                  Text(file.name).lineLimit(1)
                  Text("\(file.length) 位元組 · 僅使用這份副本，傳送後可供模型使用")
                    .font(.caption).foregroundStyle(.secondary)
                }
                Spacer()
                Button("移除檔案") { model.clearTextInput() }
                  .disabled(model.pending != nil || model.textInputClearing != nil)
              }
            }
            if model.textInputSelecting != nil {
              HStack {Text("正在選入文字檔案…").font(.caption);Spacer();Button("取消選檔") {model.cancelTextFileSelection()} }
            }
            if let selected = model.selectedComputer {
              HStack {
                Image(systemName: "macwindow")
                VStack(alignment: .leading) { Text(selected.appName); Text(selected.title).font(.caption).lineLimit(1) }
                Spacer()
                Button("移除視窗") { model.selectedComputer = nil }.disabled(model.pending != nil)
              }
            }
            Composer(text: $model.draft, submit: model.submit)
              .frame(height: 104).clipShape(RoundedRectangle(cornerRadius: 8))
              .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.25)))
            HStack {
              Menu {
                Button("文字檔案") {model.pickTextFile()}
                Button("圖片（本機預覽）") {model.pickImage()}
              } label: {Label("附加",systemImage:"paperclip")}
                .disabled(model.pending != nil || model.textInputSelecting != nil || model.textInputClearing != nil)
              Button { model.reselectingTask = nil; model.showComputerPicker = true } label: { Label("程式視窗", systemImage: "macwindow") }
                .disabled(model.pending != nil || model.textInputSelecting != nil || model.textInputClearing != nil)
                .sheet(isPresented: $model.showComputerPicker) { ComputerWindowPicker(model: model, taskGoal: model.reselectingTask?.goal, choose: model.chooseComputerWindow) }
              Spacer()
              Text("⌘ Enter 傳送").font(.caption).foregroundStyle(.secondary)
              Button("傳送", action: model.submit).buttonStyle(.borderedProminent).disabled(
                !model.canSend)
            }
            HStack {
              Text(model.notice.isEmpty ? "訊息會先保存，回覆與進度會自動同步。" : model.notice).font(.caption)
                .foregroundStyle(.secondary)
              Spacer()
            }
          }.padding()
        }
        if model.showProgress {
          Divider()
          progress.frame(width: 250)
        }
      }
    }
    .sheet(isPresented: $model.showSettings) { SettingsView(model: model) }
    .environment(\.openURL, OpenURLAction { url in safeLink(url) ? .systemAction : .discarded })
  }
  var progress: some View {
    VStack(alignment: .leading, spacing: 12) {
      Text("進度").font(.headline)
      List(model.state.tasks, selection: $model.selectedTask) { task in
        VStack(alignment: .leading) {
          Text(task.goal).lineLimit(2)
          Text(task.status).font(.caption).foregroundStyle(.secondary)
        }.tag(task.id)
      }
      if let task = model.task {
        Text(task.status).font(.caption)
        Button("取消任務", action: model.cancelTask).disabled(task.terminal || task.cancelRequested)
        if task.scope?.hasPrefix("computer:") == true {
          Button("重新選擇視窗") { model.reselectingTask = task; model.showComputerPicker = true }
            .disabled(!task.canReselectWindow || model.pendingWindowReselection != nil || model.state.computer_selecting != nil)
        }
        if task.scope == nil || task.scope == "pulse:read" {
          DisclosureGroup("更多操作") {
            Button("讀取 README") { model.read(["kind": "text", "path": "README.md"]) }
            Button("Git 狀態") { model.read(["kind": "git_status"]) }
          }.disabled(task.terminal)
        }
      }
      Button("更新進度") { model.action(["kind": "status"]) }
      Button(model.state.stopped ? "恢復本機工具" : "暫停本機工具", action: model.toggleLocal)
      Text(model.state.localConnected ? "local executor 已連線" : "local executor 離線").font(.caption)
        .foregroundStyle(.secondary)
      if let timestamp = model.state.syncedAt {
        Text("最後同步：\(Date(timeIntervalSince1970: Double(timestamp) / 1000).formatted())").font(
          .caption2
        ).foregroundStyle(.secondary)
      }
    }.padding()
  }
}
struct SettingsView: View {
  @ObservedObject var model: ChatModel
  @Environment(\.dismiss) private var dismiss
  var body: some View {
    VStack(alignment: .leading, spacing: 18) {
      HStack {
        Text("設定").font(.title2)
        Spacer()
        Button("返回對話") {
          model.pairCode = ""
          dismiss()
        }.keyboardShortcut(.cancelAction)
      }
      TextField("HTTPS 伺服器位址", text: $model.settings.server).textFieldStyle(.roundedBorder)
      Button("儲存並連線") { model.connect(save: true) }.disabled(!model.settings.valid)
      Toggle("在這部 Mac 顯示提醒通知", isOn: Binding(
        get: { model.state.desktop_notifications?.enabled == true },
        set: { model.setDesktopNotifications($0) }
      )).disabled(model.authorizingNotifications)
      Text("只顯示啟用後到期、24 小時內的提醒。原文會保存在對話中。")
        .font(.caption).foregroundStyle(.secondary)
      if let notifications = model.state.desktop_notifications, notifications.uncertain > 0 || notifications.failed > 0 {
        Text("有 \(notifications.uncertain) 筆通知待核對、\(notifications.failed) 筆未確認送出。請回對話查看提醒。")
          .font(.caption).foregroundStyle(.secondary)
      }
      if !model.notificationNotice.isEmpty { Text(model.notificationNotice).font(.caption).foregroundStyle(.secondary) }
      Divider()
      Text("對話框配對").font(.headline)
      Text("配對碼只使用一次。密鑰由 Rust 傳輸程序存入 Keychain。").font(.caption).foregroundStyle(.secondary)
      HStack {
        SecureField("一次性配對碼", text: $model.pairCode).onSubmit(model.pair)
        Button("配對", action: model.pair)
      }
      DisclosureGroup("進階設定") {
        VStack(alignment: .leading, spacing: 10) {
          HStack {
            TextField("私有資料目錄", text: $model.settings.dataDir)
            Button("選擇", action: model.pickDataDirectory)
          }
          Toggle("只限本機 HTTP 測試", isOn: $model.settings.development)
          Button("重新連線") { model.connect() }
          Text("待送：\(model.state.pending)\n快取來源：\(model.state.server)\n\(model.state.message)")
            .font(.caption).foregroundStyle(.secondary)
        }.padding(.top, 8)
      }
      if !model.notice.isEmpty { Text(model.notice).font(.caption).foregroundStyle(.secondary) }
    }.padding(24).frame(width: 560)
  }
}
