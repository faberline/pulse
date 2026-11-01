@preconcurrency import AppKit
import ApplicationServices
import CryptoKit
@preconcurrency import ScreenCaptureKit

@MainActor
public enum WindowObserver {
  static func running(_ target: ComputerTarget, launched: Date? = nil) throws -> NSRunningApplication {
    _ = try ComputerTarget(bundleID: target.bundle_id, pid: target.pid, windowID: target.window_id)
    guard let app = NSRunningApplication(processIdentifier: target.pid),
      app.bundleIdentifier == target.bundle_id, !app.isTerminated, let start = app.launchDate,
      launched == nil || start == launched
    else { throw ComputerError.targetUnavailable }
    return app
  }
  /// Exact selected background windows are readable. This never activates an app or captures pixels.
  public static func probe(target: ComputerTarget, expectedProcessLaunchedAt: Int64) async throws -> ComputerSelection {
    guard computerPermissions().canObserve else { throw ComputerError.permissionsRequired }
    let app = try running(target)
    guard let start = app.launchDate, expectedProcessLaunchedAt == Int64(start.timeIntervalSince1970 * 1000)
    else { throw ComputerError.targetChanged }
    let selected = try await window(target)
    guard let title = selected.title else { throw ComputerError.targetUnavailable }
    _ = try AXCatalog(pid: target.pid, deadline: ContinuousClock.now.advanced(by: .seconds(8)))
      .window(title: title, frame: ComputerRect(selected.frame))
    _ = try running(target, launched: start)
    return ComputerSelection(target: target, process_launched_at: expectedProcessLaunchedAt)
  }
  static func window(_ target: ComputerTarget) async throws -> SCWindow {
    let content = try await SCShareableContent.excludingDesktopWindows(true, onScreenWindowsOnly: true)
    let windows = content.windows.filter { $0.windowID == target.window_id }
    guard windows.count == 1, let window = windows.first,
      window.owningApplication?.processID == target.pid,
      window.owningApplication?.bundleIdentifier == target.bundle_id,
      window.isOnScreen, window.windowLayer == 0,
      let title = window.title, !title.isEmpty, title.utf8.count <= 512
    else { throw ComputerError.targetUnavailable }
    return window
  }
  public static func observe(target: ComputerTarget, binding: ComputerBinding, cache: URL, expectedProcessLaunchedAt: Int64? = nil) async throws -> ComputerObservation {
    try binding.validate()
    guard computerPermissions().canObserve else { throw ComputerError.permissionsRequired }
    let deadline = ContinuousClock.now.advanced(by: .seconds(8))
    let app = try running(target)
    guard let start = app.launchDate else { throw ComputerError.targetUnavailable }
    guard expectedProcessLaunchedAt == nil || expectedProcessLaunchedAt == Int64(start.timeIntervalSince1970 * 1000)
    else { throw ComputerError.targetChanged }
    let selected = try await window(target)
    _ = try running(target, launched: start)
    guard let title = selected.title else { throw ComputerError.targetUnavailable }
    let frame = try ComputerRect(selected.frame)
    let catalog = AXCatalog(pid: target.pid, deadline: deadline)
    let axWindow = try catalog.window(title: title, frame: frame)
    let (elements, truncated) = try catalog.collect(axWindow)
    let filter = SCContentFilter(desktopIndependentWindow: selected)
    let scale = Double(filter.pointPixelScale)
    guard scale.isFinite, (1...4).contains(scale) else { throw ComputerError.evidenceLimit }
    let width = Int(ceil(selected.frame.width * scale))
    let height = Int(ceil(selected.frame.height * scale))
    guard (1...8192).contains(width), (1...8192).contains(height), width * height <= 16_777_216
    else { throw ComputerError.evidenceLimit }
    let config = SCStreamConfiguration()
    config.width = width; config.height = height; config.showsCursor = false
    config.ignoreShadowsSingleWindow = true
    // Capture only the selected window. No display-wide or coordinate fallback.
    let observedAt = Int64(Date().timeIntervalSince1970 * 1000)
    let image = try await SCScreenshotManager.captureImage(contentFilter: filter, configuration: config)
    guard image.width == width, image.height == height else { throw ComputerError.captureFailed }
    _ = try running(target, launched: start)
    let current = try await window(target)
    guard current.title == title, try ComputerRect(current.frame) == frame,
      ContinuousClock.now < deadline, computerPermissions().canObserve
    else { throw ComputerError.targetChanged }
    let currentAX = try catalog.window(title: title, frame: frame)
    let (after, afterTruncated) = try catalog.collect(currentAX)
    guard elements == after, truncated == afterTruncated else { throw ComputerError.targetChanged }
    guard let png = NSBitmapImageRep(cgImage: image).representation(using: .png, properties: [:]),
      png.count <= 4 * 1024 * 1024 else { throw ComputerError.evidenceLimit }
    let path = try PrivatePNG.save(png, directory: cache)
    return ComputerObservation(binding: binding, target: target,
      process_launched_at: Int64(start.timeIntervalSince1970 * 1000), title: title, frame: frame,
      elements: elements, truncated: truncated, nonce: UUID().uuidString,
      observed_at: observedAt, expires_at: observedAt + 120_000,
      image: ComputerImage(digest: SHA256.hash(data: png).map { String(format: "%02x", $0) }.joined(),
        length: png.count, width: width, height: height, path: path))
  }
}
