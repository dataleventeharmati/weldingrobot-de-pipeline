#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Cleaning demo outputs (raw/staged/reports latest only)…"

rm -f data/raw/*.csv || true
rm -f data/staged/*.csv || true

# csak a latest-eket töröljük – a timestampelt history maradhat, ha akarod
rm -f data/reports/*_latest.json || true

echo "OK."
