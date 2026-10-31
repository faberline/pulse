// swift-tools-version: 6.0
import PackageDescription

let package = Package(
  name: "PulseDesktop",
  platforms: [.macOS(.v14)],
  products: [.executable(name: "pulse-desktop", targets: ["PulseDesktop"])],
  targets: [
    .executableTarget(name: "PulseDesktop"),
    .testTarget(name: "PulseDesktopTests", dependencies: ["PulseDesktop"]),
  ]
)
