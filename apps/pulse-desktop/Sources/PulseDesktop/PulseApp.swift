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
                    Text("模型尚未接入。訊息會先保存。").foregroundStyle(.secondary)
                  }.padding(.vertical, 90).frame(maxWidth: .infinity, alignment: .center)
                }
                ForEach(model.state.messages) { message in
                  HStack {
                    if message.isUser { Spacer(minLength: 40) }
                    VStack(alignment: .leading, spacing: 8) {
                      Text(message.label).font(.caption).foregroundStyle(.secondary)
                      RichContent(source: message.displayBody)
                    }
                    .padding(14).frame(maxWidth: .infinity, alignment: .leading)
                    .background(
                      message.isUser
                        ? Color.accentColor.opacity(0.10) : Color.secondary.opacity(0.06),
                      in: RoundedRectangle(cornerRadius: 12))
                  }.id(message.id)
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
            Composer(text: $model.draft, submit: model.submit)
              .frame(height: 104).clipShape(RoundedRectangle(cornerRadius: 8))
              .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.25)))
            HStack {
              Button {
                model.pickImage()
              } label: {
                Label("圖片", systemImage: "photo")
              }
              Spacer()
              Text("⌘ Enter 傳送").font(.caption).foregroundStyle(.secondary)
              Button("傳送", action: model.submit).buttonStyle(.borderedProminent).disabled(
                !model.canSend)
            }
            HStack {
              Text(model.notice.isEmpty ? "模型尚未接入，訊息會先保存。" : model.notice).font(.caption)
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
        DisclosureGroup("更多操作") {
          Button("讀取 README") { model.read(["kind": "text", "path": "README.md"]) }
          Button("Git 狀態") { model.read(["kind": "git_status"]) }
        }.disabled(task.terminal)
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
