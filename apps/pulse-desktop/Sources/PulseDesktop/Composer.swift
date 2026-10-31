import AppKit
import SwiftUI

struct Composer: NSViewRepresentable {
  @Binding var text: String
  var submit: () -> Void
  func makeCoordinator() -> Coordinator { Coordinator(self) }
  func makeNSView(context: Context) -> NSScrollView {
    let view = ChatTextView()
    view.isRichText = false
    view.allowsUndo = true
    view.isAutomaticQuoteSubstitutionEnabled = false
    view.isAutomaticDashSubstitutionEnabled = false
    view.font = .systemFont(ofSize: 15)
    view.textContainerInset = NSSize(width: 8, height: 8)
    view.autoresizingMask = [.width]
    view.isVerticallyResizable = true
    view.isHorizontallyResizable = false
    view.textContainer?.widthTracksTextView = true
    view.delegate = context.coordinator
    view.submit = submit
    view.setAccessibilityLabel("訊息給 Pulse")
    let scroll = NSScrollView()
    scroll.hasVerticalScroller = true
    scroll.documentView = view
    DispatchQueue.main.async { view.window?.makeFirstResponder(view) }
    return scroll
  }
  func updateNSView(_ scroll: NSScrollView, context: Context) {
    context.coordinator.parent = self
    guard let view = scroll.documentView as? ChatTextView else { return }
    view.submit = submit
    if view.string != text && !view.hasMarkedText() { view.string = text }
  }
  class Coordinator: NSObject, NSTextViewDelegate {
    var parent: Composer
    init(_ parent: Composer) { self.parent = parent }
    func textDidChange(_ notification: Notification) {
      if let view = notification.object as? NSTextView { parent.text = view.string }
    }
  }
}
final class ChatTextView: NSTextView {
  var submit: (() -> Void)?
  override func keyDown(with event: NSEvent) {
    if event.keyCode == 36 && event.modifierFlags.contains(.command) && !hasMarkedText() {
      submit?()
      return
    }
    super.keyDown(with: event)
  }
}
