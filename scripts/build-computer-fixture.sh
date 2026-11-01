#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "$(uname -s)" != Darwin ]; then
  printf '%s\n' 'The computer fixture requires macOS.' >&2
  exit 1
fi
swift build --package-path apps/pulse-desktop --scratch-path target/swift-desktop --product pulse-computer-fixture --configuration debug
swift_bin=$(swift build --package-path apps/pulse-desktop --scratch-path target/swift-desktop --configuration debug --show-bin-path)
app=target/debug/PulseComputerFixture.app
mkdir -p "$app/Contents/MacOS"
cp "$swift_bin/pulse-computer-fixture" "$app/Contents/MacOS/pulse-computer-fixture"
cat > "$app/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>com.faberline.PulseComputerFixture</string>
<key>CFBundleName</key><string>Pulse Computer Fixture</string>
<key>CFBundleExecutable</key><string>pulse-computer-fixture</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleVersion</key><string>1</string>
<key>NSHighResolutionCapable</key><true/>
<key>LSMinimumSystemVersion</key><string>14.0</string>
</dict></plist>
PLIST
/usr/bin/codesign --force --sign - "$app"
/usr/bin/codesign --verify --deep --strict "$app"
printf '%s\n' "$app"
