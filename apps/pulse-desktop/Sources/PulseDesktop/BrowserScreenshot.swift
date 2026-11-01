import SwiftUI

struct BrowserScreenshot: View {
  @ObservedObject var model: ChatModel
  let message: ChatMessage
  let artifact: BrowserArtifact
  @State private var showImage = false
  var body: some View {
    VStack(alignment: .leading, spacing: 8) {
      Text("瀏覽器截圖 · \(artifact.title)").font(.headline)
      Text(artifact.url).textSelection(.enabled).font(.caption)
      Text("取得：\(Date(timeIntervalSince1970: Double(artifact.observed_at) / 1000).formatted())")
        .font(.caption).foregroundStyle(.secondary)
      if model.state.tasks.first(where: { $0.id == artifact.task_id })?.revision != artifact.revision {
        Text("這是舊修訂的證據，不能批准目前操作。").font(.caption)
      }
      if let image = model.browserImages[message.artifactID] {
        Button { showImage = true } label: {
          Image(nsImage: image).resizable().scaledToFit().frame(maxHeight: 320)
        }.buttonStyle(.plain).accessibilityLabel("放大瀏覽器截圖")
        Text("原始 PNG 已核對。點圖片放大。").font(.caption).foregroundStyle(.secondary)
      } else {
        Button("載入截圖") { model.loadBrowserArtifact(message) }
        Text("\(model.state.message.hasPrefix("截圖") ? model.state.message : "截圖尚未載入；文字證據仍可閱讀。")")
          .font(.caption).foregroundStyle(.secondary)
      }
    }.sheet(isPresented: $showImage) {
      VStack {
        if let image = model.browserImages[message.artifactID] {
          ScrollView([.horizontal, .vertical]) { Image(nsImage: image) }
        }
        Button("關閉") { showImage = false }
      }.padding().frame(minWidth: 700, minHeight: 550)
    }
  }
}
