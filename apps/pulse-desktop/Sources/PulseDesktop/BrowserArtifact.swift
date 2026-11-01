import AppKit
import CryptoKit
import ImageIO
import Darwin

struct BrowserArtifact: Decodable, Equatable, Sendable {
  let task_id: String
  let revision: String
  let digest: String
  let length: UInt32
  let url: String
  let title: String
  let observed_at: Int64
  let width: UInt32
  let height: UInt32
  var computer: ComputerArtifactSource? = nil
  var sourceLabel: String {
    if let source = computer { return "\(source.selection.target.bundle_id) · 視窗 \(source.selection.target.window_id)" }
    return url
  }
  var validSource: Bool {
    guard let computer else {
      guard let parsed = URLComponents(string: url) else { return false }
      return ["http", "https"].contains(parsed.scheme) && parsed.host != nil && parsed.user == nil && parsed.password == nil
    }
    let target = computer.selection.target
    let parts = target.bundle_id.split(separator: ".", omittingEmptySubsequences: false)
    let decimal: (String) -> Bool = { text in
      guard let value = UInt64(text), value > 0, value <= UInt64(Int64.max) else { return false }
      return String(value) == text
    }
    return url.isEmpty && computer.binding.task_id == task_id && computer.binding.revision == revision
      && !computer.binding.operation_id.isEmpty && computer.binding.operation_id.utf8.count <= 256
      && !computer.binding.operation_id.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) })
      && UUID(uuidString: computer.binding.device) != nil && decimal(revision) && decimal(computer.binding.epoch)
      && target.pid > 0 && target.window_id > 0 && target.bundle_id.utf8.count <= 255
      && parts.count >= 2 && parts.allSatisfy({ !$0.isEmpty })
      && target.bundle_id.utf8.allSatisfy({ (48...57).contains($0) || (65...90).contains($0) || (97...122).contains($0) || $0 == 45 || $0 == 46 })
      && computer.selection.process_launched_at > 0 && computer.selection.process_launched_at <= observed_at
      && !title.isEmpty && title.utf8.count <= 512
  }
}
struct ComputerArtifactSource: Decodable, Equatable, Sendable {
  struct Binding: Decodable, Equatable, Sendable {
    let operation_id: String; let task_id: String; let revision: String; let device: String; let epoch: String
  }
  struct Selection: Codable, Equatable, Sendable {
    struct Target: Codable, Equatable, Sendable { let bundle_id: String; let pid: Int32; let window_id: UInt32 }
    let target: Target; let process_launched_at: Int64
    var wireValue: [String: Any] { ["target":["bundle_id":target.bundle_id,"pid":target.pid,"window_id":target.window_id],"process_launched_at":process_launched_at] }
  }
  let binding: Binding; let selection: Selection
}
extension BrowserArtifact {
  enum CodingKeys: String, CodingKey { case task_id, revision, digest, length, url, title, observed_at, width, height, computer }
  init(from decoder: Decoder) throws {
    let values = try decoder.container(keyedBy: CodingKeys.self)
    task_id = try values.decode(String.self, forKey: .task_id)
    revision = try values.decode(String.self, forKey: .revision)
    digest = try values.decode(String.self, forKey: .digest)
    length = try values.decode(UInt32.self, forKey: .length)
    url = try values.decodeIfPresent(String.self, forKey: .url) ?? ""
    title = try values.decode(String.self, forKey: .title)
    observed_at = try values.decode(Int64.self, forKey: .observed_at)
    width = try values.decode(UInt32.self, forKey: .width)
    height = try values.decode(UInt32.self, forKey: .height)
    computer = try values.decodeIfPresent(ComputerArtifactSource.self, forKey: .computer)
  }
}
struct LocalBrowserArtifact: Decodable, Sendable {
  let operation_id: String
  let artifact: BrowserArtifact
  let path: String
}

// Only the Rust helper's private, content-addressed cache can supply an image.
func verifiedBrowserImageData(_ image: LocalBrowserArtifact, directory: String) -> Data? {
  let meta = image.artifact
  guard meta.validSource,
    meta.computer == nil || meta.computer?.binding.operation_id == image.operation_id,
    meta.digest.count == 64, meta.digest.allSatisfy({ $0.isHexDigit && !$0.isUppercase }),
    (33...4 * 1024 * 1024).contains(Int(meta.length)), meta.width > 0, meta.height > 0,
    meta.width <= 8192, meta.height <= 8192,
    UInt64(meta.width) * UInt64(meta.height) <= 16_777_216 else { return nil }
  let expected = URL(fileURLWithPath: directory).standardizedFileURL
    .appendingPathComponent("artifacts").appendingPathComponent(meta.digest + ".png")
  guard URL(fileURLWithPath: image.path).standardizedFileURL == expected else { return nil }
  let descriptor = open(expected.path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC)
  guard descriptor >= 0 else { return nil }
  let file = FileHandle(fileDescriptor: descriptor, closeOnDealloc: true)
  guard let data = try? file.read(upToCount: 4 * 1024 * 1024 + 1), data.count == Int(meta.length),
    SHA256.hash(data: data).map({ String(format: "%02x", $0) }).joined() == meta.digest,
    let source = CGImageSourceCreateWithData(data as CFData, nil),
    CGImageSourceGetCount(source) == 1,
    CGImageSourceGetType(source) as String? == "public.png",
    let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any],
    (properties[kCGImagePropertyPixelWidth] as? NSNumber)?.uint32Value == meta.width,
    (properties[kCGImagePropertyPixelHeight] as? NSNumber)?.uint32Value == meta.height
  else { return nil }
  return data
}
