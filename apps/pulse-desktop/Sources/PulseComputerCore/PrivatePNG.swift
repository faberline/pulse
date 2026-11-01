import CryptoKit
import Darwin
import Foundation

enum PrivatePNG {
  private static func checkCached(_ data: Data, fd: Int32, name: String) throws {
    let cached = openat(fd, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK)
    guard cached >= 0 else { throw ComputerError.privateStorageRequired }
    let file = FileHandle(fileDescriptor: cached, closeOnDealloc: true)
    var metadata = stat()
    guard fstat(cached, &metadata) == 0, metadata.st_mode & S_IFMT == S_IFREG,
      metadata.st_mode & 0o777 == 0o600, metadata.st_uid == geteuid(), metadata.st_size == data.count,
      try file.read(upToCount: data.count + 1) == data else { throw ComputerError.privateStorageRequired }
  }
  static func save(_ data: Data, directory: URL) throws -> String {
    guard data.count <= 4 * 1024 * 1024,
      data.starts(with: [137, 80, 78, 71, 13, 10, 26, 10]),
      directory.path.hasPrefix("/") else { throw ComputerError.evidenceLimit }
    let fd = open(directory.path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
    guard fd >= 0 else { throw ComputerError.privateStorageRequired }
    defer { close(fd) }
    var info = stat()
    guard fstat(fd, &info) == 0, info.st_uid == geteuid(), info.st_mode & 0o777 == 0o700
    else { throw ComputerError.privateStorageRequired }
    let digest = SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    let name = digest + ".png"
    var existing = stat()
    if fstatat(fd, name, &existing, AT_SYMLINK_NOFOLLOW) == 0 {
      try checkCached(data, fd: fd, name: name)
    } else {
      guard errno == ENOENT else { throw ComputerError.privateStorageRequired }
      let staging = ".partial-" + UUID().uuidString
      let image = openat(fd, staging, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
      guard image >= 0 else { throw ComputerError.privateStorageRequired }
      defer { close(image); _ = unlinkat(fd, staging, 0) }
      guard fchmod(image, 0o600) == 0 else { throw ComputerError.privateStorageRequired }
      guard (try? FileManager.default.contentsOfDirectory(atPath: directory.path).count) ?? 129 <= 128
      else { throw ComputerError.evidenceLimit }
      try data.withUnsafeBytes { bytes in
        var offset = 0
        while offset < data.count {
          let n = write(image, bytes.baseAddress!.advanced(by: offset), data.count - offset)
          guard n > 0 else { throw ComputerError.privateStorageRequired }
          offset += n
        }
      }
      guard fsync(image) == 0 else { throw ComputerError.privateStorageRequired }
      // Publish complete bytes without replacing an existing artifact or following a link.
      if renameatx_np(fd, staging, fd, name, UInt32(RENAME_EXCL)) != 0 {
        guard errno == EEXIST else { throw ComputerError.privateStorageRequired }
        try checkCached(data, fd: fd, name: name)
      }
      guard fsync(fd) == 0 else { throw ComputerError.privateStorageRequired }
    }
    return directory.appendingPathComponent(name).path
  }
}
