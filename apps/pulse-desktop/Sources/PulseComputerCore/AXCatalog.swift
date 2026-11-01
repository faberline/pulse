import ApplicationServices
import Foundation

@MainActor
struct AXCatalog {
  let pid: pid_t
  let deadline: ContinuousClock.Instant
  private func check() throws {
    guard ContinuousClock.now < deadline else { throw ComputerError.evidenceLimit }
  }
  func value(_ element: AXUIElement, _ attribute: String) throws -> CFTypeRef? {
    try check()
    var result: CFTypeRef?
    let error = AXUIElementCopyAttributeValue(element, attribute as CFString, &result)
    if error == .attributeUnsupported || error == .noValue { return nil }
    guard error == .success else { throw ComputerError.accessibilityUnavailable }
    return result
  }
  func children(_ element: AXUIElement, attribute: String, limit: Int) throws -> [AXUIElement] {
    try check()
    var count = 0
    let error = AXUIElementGetAttributeValueCount(element, attribute as CFString, &count)
    if error == .attributeUnsupported || error == .noValue { return [] }
    guard error == .success, count <= limit else { throw ComputerError.evidenceLimit }
    if count == 0 { return [] }
    var values: CFArray?
    guard AXUIElementCopyAttributeValues(element, attribute as CFString, 0, count, &values) == .success,
      let elements = values as? [AXUIElement], elements.count == count
    else { throw ComputerError.accessibilityUnavailable }
    return elements
  }
  func window(title: String, frame: ComputerRect) throws -> AXUIElement {
    let app = AXUIElementCreateApplication(pid)
    guard AXUIElementSetMessagingTimeout(AXUIElementCreateSystemWide(), 0.25) == .success else { throw ComputerError.accessibilityUnavailable }
    let windows = try children(app, attribute: kAXWindowsAttribute, limit: 32)
    let matches = try windows.filter { element in
      var owner: pid_t = 0
      guard AXUIElementGetPid(element, &owner) == .success, owner == pid else { throw ComputerError.targetChanged }
      guard try value(element, kAXTitleAttribute) as? String == title,
        let p = try value(element, kAXPositionAttribute), CFGetTypeID(p) == AXValueGetTypeID(),
        let s = try value(element, kAXSizeAttribute), CFGetTypeID(s) == AXValueGetTypeID()
      else { return false }
      var point = CGPoint.zero; var size = CGSize.zero
      guard AXValueGetValue(unsafeDowncast(p, to: AXValue.self), .cgPoint, &point),
        AXValueGetValue(unsafeDowncast(s, to: AXValue.self), .cgSize, &size)
      else { return false }
      return abs(point.x - frame.x) < 0.5 && abs(point.y - frame.y) < 0.5
        && abs(size.width - frame.width) < 0.5 && abs(size.height - frame.height) < 0.5
    }
    guard matches.count == 1 else { throw ComputerError.unsupportedWindow }
    return matches[0]
  }
  func collect(_ window: AXUIElement) throws -> ([ComputerElement], Bool) {
    var output: [ComputerElement] = []; var visited: [AXUIElement] = []
    var queue: [(AXUIElement, String, Int)] = [(window, "window", 0)]
    var truncated = false; var controls = 0
    while !queue.isEmpty {
      try check()
      let (element, path, depth) = queue.removeFirst()
      if visited.contains(where: { CFEqual($0, element) }) { continue }
      if visited.count >= 128 { truncated = true; break }
      visited.append(element)
      var owner: pid_t = 0
      guard AXUIElementGetPid(element, &owner) == .success, owner == pid else { throw ComputerError.targetChanged }
      let role = try value(element, kAXRoleAttribute) as? String ?? ""
      let subrole = try value(element, kAXSubroleAttribute) as? String ?? ""
      // Do not read secure values, labels, actions or descendants.
      if subrole == kAXSecureTextFieldSubrole { continue }
      let enabled = try value(element, kAXEnabledAttribute) as? Bool ?? false
      var label = try value(element, kAXTitleAttribute) as? String ?? ""
      if label.isEmpty { label = try value(element, kAXDescriptionAttribute) as? String ?? "" }
      var text: String?
      var press = false; var setText = false
      if role == kAXButtonRole && enabled && controls < 16 {
        var actions: CFArray?
        if AXUIElementCopyActionNames(element, &actions) == .success,
          let names = actions as? [String], names.count <= 16 { press = names.contains(kAXPressAction) }
      }
      if [kAXTextFieldRole, kAXTextAreaRole].contains(role) {
        // Long or uncounted text fields cannot become action targets.
        let count = try value(element, kAXNumberOfCharactersAttribute) as? Int
        if let count, (0...1024).contains(count), let string = try value(element, kAXValueAttribute) as? String,
          string.utf8.count <= 1024 {
          text = string
          var writable = DarwinBoolean(false)
          setText = enabled && controls < 16 && AXUIElementIsAttributeSettable(element, kAXValueAttribute as CFString, &writable) == .success && writable.boolValue
        }
      } else if role == kAXStaticTextRole {
        text = (try value(element, kAXValueAttribute) as? String).map { boundedText($0, bytes: 512) }
      }
      if press || setText { controls += 1 }
      if !label.isEmpty || text != nil || press || setText {
        output.append(ComputerElement(id: path, role: boundedText(role, bytes: 64), label: boundedText(label, bytes: 256), value: text, can_press: press, can_set_text: setText))
      }
      if depth >= 8 { truncated = true; continue }
      let next = try children(element, attribute: kAXChildrenAttribute, limit: 128)
      for (index, child) in next.enumerated() { queue.append((child, "\(path).\(index)", depth + 1)) }
    }
    guard try JSONEncoder().encode(output).count <= 32_768 else { throw ComputerError.evidenceLimit }
    return (output, truncated)
  }
}
