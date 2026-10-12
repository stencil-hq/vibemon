#!/usr/bin/env bash
# Renders the vibemon desktop branding into the template file context:
#   wallpaper.html -> ../files/wallpaper.png (2048×1536)
#   favicon.svg    -> ../files/novnc/{favicon.svg,favicon-32x32.png,apple-touch-icon.png}
# Needs headless Chrome/Chromium (`CHROME=...` to override) and rsvg-convert.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
files="$here/../files"
chrome="${CHROME:-}"
if [ -z "$chrome" ]; then
	for candidate in "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" google-chrome chromium chromium-browser; do
		if command -v "$candidate" >/dev/null 2>&1 || [ -x "$candidate" ]; then
			chrome="$candidate"
			break
		fi
	done
fi
[ -n "$chrome" ] || { echo "render.sh: no Chrome found; set CHROME" >&2; exit 1; }

shot() { # <url> <width> <height> <out>
	"$chrome" --headless=new --disable-gpu --hide-scrollbars --force-device-scale-factor=1 \
		--default-background-color=00000000 --window-size="$2,$3" --screenshot="$4" "$1" 2>/dev/null
}

mkdir -p "$files/novnc"
shot "file://$here/wallpaper.html" 2048 1536 "$files/wallpaper.png"
cp "$here/favicon.svg" "$files/novnc/favicon.svg"
rsvg-convert -w 32 -h 32 -o "$files/novnc/favicon-32x32.png" "$here/favicon.svg"
rsvg-convert -w 180 -h 180 -o "$files/novnc/apple-touch-icon.png" "$here/favicon.svg"
