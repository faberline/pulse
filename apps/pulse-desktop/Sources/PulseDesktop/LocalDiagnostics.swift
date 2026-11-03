import Foundation

/// Fixed local lifecycle events only. Never log commands, paths, or chat data.
func uiDiagnostic(_ event: StaticString, code: Int32? = nil) {
  let time = Int64(Date().timeIntervalSince1970 * 1000)
  let suffix = code.map { " code=\($0)" } ?? ""
  let line = "Pulse UI time=\(time) pid=\(ProcessInfo.processInfo.processIdentifier) event=\(event)\(suffix)\n"
  try? FileHandle.standardError.write(contentsOf: Data(line.utf8))
}
