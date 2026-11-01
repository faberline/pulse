import AppKit
import CoreGraphics
@preconcurrency import ScreenCaptureKit
import SwiftUI

struct ComputerWindowChoice: Identifiable, Equatable, Sendable {
  let selection: ComputerArtifactSource.Selection
  let appName: String
  let title: String
  var id: UInt32 { selection.target.window_id }
}

/// Opened only by the Human's composer button. No window inventory is sent to the server.
struct ComputerWindowPicker: View {
  let choose: (ComputerWindowChoice) -> Void
  @Environment(\.dismiss) private var dismiss
  @State private var windows: [ComputerWindowChoice] = []
  @State private var message = "正在讀取可選視窗…"
  var body: some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("選擇程式視窗").font(.title2)
      Text("只讀取你選的視窗。視窗文字會交給模型，原圖會顯示在對話中。這不會點擊或輸入。")
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
    }.padding(22).frame(width: 520, height: 440).task { await load() }
  }
  @MainActor
  private func load() async {
    guard CGPreflightScreenCaptureAccess() else {
      message = "尚未取得螢幕錄製權限。請在系統設定中授權 Pulse，再重新選擇。"
      return
    }
    do {
      let content = try await SCShareableContent.excludingDesktopWindows(true, onScreenWindowsOnly: true)
      var found: [ComputerWindowChoice] = []
      let limits = ProcessInfo.processInfo.environment
      for window in content.windows where window.isOnScreen && window.windowLayer == 0 {
        guard found.count < 64, let owner = window.owningApplication, owner.processID != getpid() else { continue }
        if let bundle = limits["PULSE_COMPUTER_BUNDLE_ID"], owner.bundleIdentifier != bundle { continue }
        if let pid = limits["PULSE_COMPUTER_PID"], String(owner.processID) != pid { continue }
        if let id = limits["PULSE_COMPUTER_WINDOW_ID"], String(window.windowID) != id { continue }
        guard
          let app = NSRunningApplication(processIdentifier: owner.processID), !app.isTerminated,
          app.activationPolicy == .regular, app.bundleIdentifier == owner.bundleIdentifier,
          let launched = app.launchDate, let title = window.title, !title.isEmpty, title.utf8.count <= 512,
          window.windowID > 0, owner.processID > 0, !owner.bundleIdentifier.isEmpty
        else { continue }
        let selection = ComputerArtifactSource.Selection(
          target: .init(bundle_id: owner.bundleIdentifier, pid: owner.processID, window_id: window.windowID),
          process_launched_at: Int64(launched.timeIntervalSince1970 * 1000))
        found.append(ComputerWindowChoice(selection: selection,
          appName: String((app.localizedName ?? owner.applicationName).prefix(128)), title: title))
      }
      windows = found.sorted { ($0.appName, $0.title) < ($1.appName, $1.title) }
      if windows.isEmpty { message = "沒有可選的程式視窗。請先開啟要讀取的程式。" }
    } catch { message = "視窗清單尚未取得。請核對螢幕錄製權限，再重新選擇。" }
  }
}
