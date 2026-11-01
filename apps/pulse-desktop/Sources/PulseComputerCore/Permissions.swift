import ApplicationServices
import CoreGraphics

public func computerPermissions() -> ComputerPermissions {
  // Preflight only. Do not call either permission-request API in a background helper.
  ComputerPermissions(accessibility: AXIsProcessTrusted(), screen_capture: CGPreflightScreenCaptureAccess())
}
