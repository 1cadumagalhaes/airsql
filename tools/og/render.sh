#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

HTML_PATH="$SCRIPT_DIR/og.html"
OUT_PATH="$REPO_ROOT/docs/assets/brand/og-image.png"

mkdir -p "$(dirname "$OUT_PATH")"

google-chrome-stable \
  --headless \
  --disable-gpu \
  --hide-scrollbars \
  --window-size=1200,630 \
  --force-device-scale-factor=1 \
  --screenshot="$OUT_PATH" \
  "file://$HTML_PATH"

echo "Rendered $OUT_PATH"
