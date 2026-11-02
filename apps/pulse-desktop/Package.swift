// swift-tools-version: 6.0
import PackageDescription

let package = Package(
  name: "PulseDesktop",
  platforms: [.macOS(.v14)],
  products: [
    .executable(name: "pulse-desktop", targets: ["PulseDesktop"]),
    .executable(name: "pulse-computer-fixture", targets: ["PulseComputerFixture"]),
  ],
  targets: [
    .executableTarget(name: "PulseDesktop"),
    .testTarget(name: "PulseDesktopTests", dependencies: ["PulseDesktop"]),
    .executableTarget(name: "PulseComputerFixture"),
  ]
)
