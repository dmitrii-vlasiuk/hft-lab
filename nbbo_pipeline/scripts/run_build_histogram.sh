#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/build_histogram"

EVENTS_ROOT="$ROOT/data/research/events"
OUT_DIR="$ROOT/data/research/hist"
OUT="$OUT_DIR/SPY_histogram.json"
CFG="$ROOT/config/hist_bins_default.json"

mkdir -p "$OUT_DIR"

"$BIN" \
  --events-root "$EVENTS_ROOT" \
  --symbol "SPY" \
  --years "2018:2022" \
  --out "$OUT" \
  --alpha "1.0" \
  --bins-config "$CFG"
