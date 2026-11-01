import Foundation
import Darwin
import PulseComputerCore

@main
struct PulseComputerHelper {
  @MainActor
  static func main() async {
    // The parent must close stdin. A stalled input or OS request cannot keep this child alive.
    DispatchQueue.global().asyncAfter(deadline: .now() + 15) { _exit(74) }
    let permissions = computerPermissions()
    var response: ComputerResponse
    do {
      let arguments = Array(CommandLine.arguments.dropFirst())
      if arguments == ["--status"] {
        response = ComputerResponse(permissions: permissions)
      } else {
        guard arguments.count == 8, arguments[0] == "--bundle-id", arguments[2] == "--pid",
          arguments[4] == "--window-id", arguments[6] == "--cache",
          let pid = Int32(arguments[3]), let window = UInt32(arguments[5]), arguments[7].hasPrefix("/")
        else { throw ComputerError.invalidRequest }
        let target = try ComputerTarget(bundleID: arguments[1], pid: pid, windowID: window)
        var input = Data()
        while let chunk = try FileHandle.standardInput.read(upToCount: 1024), !chunk.isEmpty {
          input.append(chunk)
          guard input.count <= 65_536 else { throw ComputerError.invalidRequest }
        }
        let request = try ComputerRequest.decode(input)
        if request.kind == "status" { response = ComputerResponse(permissions: permissions) }
        else if request.kind == "probe", let launched = request.expectedProcessLaunchedAt {
          let selection = try await WindowObserver.probe(target: target, expectedProcessLaunchedAt: launched)
          response = ComputerResponse(permissions: computerPermissions(), selection: selection)
        }
        else if request.kind == "act", let binding = request.binding, let action = request.action {
          response = await WindowActor.act(target: target, binding: binding, intent: action, cache: URL(fileURLWithPath: arguments[7]))
        }
        else if let binding = request.binding {
          let observation = try await WindowObserver.observe(target: target, binding: binding, cache: URL(fileURLWithPath: arguments[7]), expectedProcessLaunchedAt: request.expectedProcessLaunchedAt)
          response = ComputerResponse(permissions: computerPermissions(), observation: observation)
        } else { throw ComputerError.invalidRequest }
      }
    } catch let error as ComputerError {
      response = ComputerResponse(permissions: permissions, error: error)
    } catch { response = ComputerResponse(permissions: permissions, error: .captureFailed) }
    let encoder = JSONEncoder(); encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
    guard let data = try? encoder.encode(response), data.count <= 65_536 else { _exit(75) }
    FileHandle.standardOutput.write(data); FileHandle.standardOutput.write(Data([10]))
  }
}
