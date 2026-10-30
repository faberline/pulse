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
if [ "$profile" = release ]; then
  cargo build --locked --release -p pulse-desktop -p pulse-link --bins
else
  cargo build --locked -p pulse-desktop -p pulse-link --bins
fi
app="target/$profile/Pulse.app"
mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources"
cp "target/$profile/pulse-desktop" "$app/Contents/MacOS/pulse-desktop"
cp "target/$profile/pulse-device" "$app/Contents/MacOS/pulse-device"
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
/usr/bin/codesign --force --sign - "$app/Contents/MacOS/pulse-device"
/usr/bin/codesign --force --sign - "$app"
/usr/bin/codesign --verify --deep --strict "$app"
printf '%s\n' "$app"
