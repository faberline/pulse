// swift-tools-version: 6.0
import PackageDescription

let package = Package(
  name: "PulseDesktop",
  platforms: [.macOS(.v14)],
  products: [
    .executable(name: "pulse-desktop", targets: ["PulseDesktop"]),
    .executable(name: "pulse-computer", targets: ["PulseComputerHelper"]),
    .executable(name: "pulse-computer-fixture", targets: ["PulseComputerFixture"]),
  ],
  targets: [
    .executableTarget(name: "PulseDesktop", dependencies: ["PulseComputerCore"]),
    .testTarget(name: "PulseDesktopTests", dependencies: ["PulseDesktop"]),
    .target(name: "PulseComputerCore"),
    .executableTarget(name: "PulseComputerHelper", dependencies: ["PulseComputerCore"]),
    .executableTarget(name: "PulseComputerFixture"),
    .testTarget(name: "PulseComputerTests", dependencies: ["PulseComputerCore"]),
  ]
)
