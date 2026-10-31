import Foundation

/// This child is chat transport, not the independent local executor.
/// Process access is locked; pipe writes and process replacement are serialized.
final class ChatTransport: @unchecked Sendable {
  private let queue = DispatchQueue(label: "pulse.chat.writer")
  private let lock = NSLock()
  private var child: Process?
  private var input: FileHandle?
  private var generation: UUID?
  private var queued = 0
  private let onState: @Sendable (UUID, NativeState) -> Void
  private let onExit: @Sendable (UUID) -> Void
  private let executable: URL?

  init(
    executable: URL? = Bundle.main.executableURL?.deletingLastPathComponent()
      .appendingPathComponent("pulse-chat"),
    onState: @escaping @Sendable (UUID, NativeState) -> Void,
    onExit: @escaping @Sendable (UUID) -> Void
  ) {
    self.onState = onState
    self.onExit = onExit
    self.executable = executable
  }
  func start(arguments: [String], generation id: UUID) {
    stop()
    lock.withLock { generation = id }
    queue.async { [self] in
      let old = lock.withLock { child }
      if let old, old.isRunning { old.waitUntilExit() }
      guard lock.withLock({ generation == id }) else { return }
      let process = Process()
      let incoming = Pipe()
      let outgoing = Pipe()
      process.executableURL = executable
      process.arguments = arguments
      process.standardInput = incoming
      process.standardOutput = outgoing
      process.standardError = FileHandle.nullDevice
      do {
        try process.run()
        lock.withLock {
          child = process
          input = incoming.fileHandleForWriting
        }
        // A stop that raced process.run must still terminate this child.
        guard lock.withLock({ generation == id }) else {
          process.terminate()
          return
        }
        Thread.detachNewThread { [self] in
          var decoder = FrameDecoder()
          do {
            while true {
              // read(upToCount:) can wait for a full buffer on a pipe.
              // availableData delivers each short state immediately.
              let bytes = outgoing.fileHandleForReading.availableData
              if bytes.isEmpty { break }
              for frame in try decoder.accept(bytes) {
                onState(id, try JSONDecoder().decode(NativeState.self, from: frame))
              }
            }
          } catch { if process.isRunning { process.terminate() } }
          process.waitUntilExit()
          onExit(id)
        }
      } catch { onExit(id) }
    }
  }
  func send(_ command: [String: Any], generation id: UUID) -> Bool {
    guard var data = try? JSONSerialization.data(withJSONObject: command), data.count < 256 * 1024
    else { return false }
    data.append(10)
    let accepted = lock.withLock { () -> Bool in
      guard generation == id, queued < 32, child?.isRunning == true else { return false }
      queued += 1
      return true
    }
    guard accepted else { return false }
    let bytes = data
    queue.async { [self] in
      defer { lock.withLock { queued -= 1 } }
      guard let handle = lock.withLock({ generation == id ? input : nil }) else { return }
      do { try handle.write(contentsOf: bytes) } catch { onExit(id) }
    }
    return true
  }
  func stop() {
    let process = lock.withLock { () -> Process? in
      generation = nil
      return child
    }
    if let process, process.isRunning { process.terminate() }
  }
  deinit { stop() }
}
