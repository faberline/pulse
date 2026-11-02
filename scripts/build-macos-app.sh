#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "$(uname -s)" != Darwin ]; then
  printf '%s\n' 'The app bundle requires macOS.' >&2
  exit 1
fi
profile=release
case "${1-}" in
  --debug) profile=debug ;;
  '') ;;
  *) printf '%s\n' 'Use build-macos-app.sh [--debug]' >&2; exit 1 ;;
esac
app="${PULSE_APP_OUTPUT:-target/$profile/Pulse.app}"
if [ -n "${PULSE_APP_OUTPUT-}" ] && { [ -e "$app" ] || [ -L "$app" ]; }; then
  printf '%s\n' 'Use a new PULSE_APP_OUTPUT path for an isolated review bundle.' >&2
  exit 1
fi
if [ "$profile" = release ]; then
  cargo build --locked --release -p pulse-link -p pulse-macos --bins
else
  cargo build --locked -p pulse-link -p pulse-macos --bins
fi
swift build --package-path apps/pulse-desktop --scratch-path target/swift-desktop --configuration "$profile"
swift_bin=$(swift build --package-path apps/pulse-desktop --scratch-path target/swift-desktop --configuration "$profile" --show-bin-path)
mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources"
cp "$swift_bin/pulse-desktop" "$app/Contents/MacOS/pulse-desktop"
cp "target/$profile/pulse-computer" "$app/Contents/MacOS/pulse-computer"
cp "target/$profile/pulse-chat" "$app/Contents/MacOS/pulse-chat"
cp "target/$profile/pulse-executor" "$app/Contents/MacOS/pulse-executor"
rm -f "$app/Contents/MacOS/pulse-device"
cat > "$app/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>com.faberline.pulse</string>
<key>CFBundleName</key><string>Pulse</string>
<key>CFBundleDisplayName</key><string>Pulse</string>
<key>CFBundleExecutable</key><string>pulse-desktop</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleShortVersionString</key><string>0.3.0</string>
<key>CFBundleVersion</key><string>3</string>
<key>NSHighResolutionCapable</key><true/>
<key>LSMinimumSystemVersion</key><string>14.0</string>
</dict></plist>
PLIST
/usr/bin/codesign --force --sign - "$app/Contents/MacOS/pulse-executor"
/usr/bin/codesign --force --sign - "$app/Contents/MacOS/pulse-chat"
/usr/bin/codesign --force --sign - "$app/Contents/MacOS/pulse-computer"
/usr/bin/codesign --force --sign - "$app"
/usr/bin/codesign --verify --deep --strict "$app"
printf '%s\n' "$app"
