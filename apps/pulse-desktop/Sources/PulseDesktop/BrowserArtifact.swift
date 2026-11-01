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
}
struct LocalBrowserArtifact: Decodable, Sendable {
  let operation_id: String
  let artifact: BrowserArtifact
  let path: String
}

// Only the Rust helper's private, content-addressed cache can supply an image.
func verifiedBrowserImageData(_ image: LocalBrowserArtifact, directory: String) -> Data? {
  let meta = image.artifact
  guard meta.digest.count == 64, meta.digest.allSatisfy({ $0.isHexDigit && !$0.isUppercase }),
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
