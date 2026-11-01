import AppKit

@MainActor
final class FixtureDelegate: NSObject, NSApplicationDelegate {
  var window: NSWindow!
  let counter = NSTextField(labelWithString: "Counter: 0")
  let input = NSTextField(string: "")
  var count = 0
  func applicationDidFinishLaunching(_ notification: Notification) {
    window = NSWindow(contentRect: NSRect(x: 160, y: 200, width: 620, height: 360), styleMask: [.titled, .closable, .miniaturizable, .resizable], backing: .buffered, defer: false)
    window.title = "Pulse Computer Fixture"
    let marker = NSTextField(labelWithString: "Marker: " + UUID().uuidString)
    input.placeholderString = "Message"; input.setAccessibilityLabel("Message")
    let button = NSButton(title: "Count once", target: self, action: #selector(increment))
    let secret = NSSecureTextField(string: "fixture-secret-must-not-appear")
    secret.setAccessibilityLabel("Fixture password")
    let stack = NSStackView(views: [marker, counter, button, input, secret])
    stack.orientation = .vertical; stack.alignment = .leading; stack.spacing = 16
    stack.translatesAutoresizingMaskIntoConstraints = false
    window.contentView!.addSubview(stack)
    NSLayoutConstraint.activate([stack.leadingAnchor.constraint(equalTo: window.contentView!.leadingAnchor, constant: 24), stack.topAnchor.constraint(equalTo: window.contentView!.topAnchor, constant: 24), stack.trailingAnchor.constraint(equalTo: window.contentView!.trailingAnchor, constant: -24)])
    window.makeKeyAndOrderFront(nil)
    NSApp.activate(ignoringOtherApps: true)
    // Window identity only. No app or desktop inventory is exposed.
    let identity: [String: Any] = ["bundle_id": Bundle.main.bundleIdentifier ?? "", "pid": ProcessInfo.processInfo.processIdentifier, "window_id": window.windowNumber]
    if let data = try? JSONSerialization.data(withJSONObject: identity, options: .sortedKeys) {
      FileHandle.standardOutput.write(data); FileHandle.standardOutput.write(Data([10]))
    }
  }
  @objc func increment() { count += 1; counter.stringValue = "Counter: \(count)" }
  func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool { true }
}

@main
struct PulseComputerFixture {
  @MainActor static func main() {
    let delegate = FixtureDelegate()
    let app = NSApplication.shared; app.setActivationPolicy(.regular); app.delegate = delegate; app.run()
  }
}
