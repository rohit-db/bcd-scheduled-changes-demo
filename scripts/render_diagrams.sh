#!/usr/bin/env bash
# Render docs/architecture/src: *.mmd → SVG + 2x PNG (mermaid-cli), *.html → 2x PNG (Playwright).
# PLAYWRIGHT_PY: a python with playwright + chromium installed (default: python3).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root/docs/architecture"
mkdir -p svg png
for f in src/*.mmd; do
  name=$(basename "$f" .mmd)
  mmdc -q -i "$f" -o "svg/$name.svg" -c mermaid.config.json -b white
  mmdc -q -i "$f" -o "png/$name.png" -c mermaid.config.json -b white -s 2
  echo "rendered $name"
done
for f in src/*.html; do
  name=$(basename "$f" .html)
  "${PLAYWRIGHT_PY:-python3}" "$root/scripts/render_html_diagram.py" "$f" "png/$name.png"
  echo "rendered $name"
done
