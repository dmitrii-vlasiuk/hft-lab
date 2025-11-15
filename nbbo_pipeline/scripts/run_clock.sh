#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.."; pwd)"

IN_DIR="${IN_DIR:-/Volumes/T7/}"
CACHE_DIR="${CACHE_DIR:-$ROOT/data/cache}"
OUT_DIR="${OUT_DIR:-$ROOT/data/out}"
WORKERS="${WORKERS:-$(sysctl -n hw.ncpu 2>/dev/null || echo 8)}"

mkdir -p "$CACHE_DIR" "$OUT_DIR"

"$ROOT/build/nbbo_pipeline" \
  --in "$IN_DIR" \
  --cache "$CACHE_DIR" \
  --out "$OUT_DIR/SPY_1ms_clock.parquet" \
  --report "$OUT_DIR/SPY_clock_report.txt" \
  --clock --ffill --max-ffill-gap-ms 250 \
  --sym-root SPY --years 2018:2024 \
  --workers "$WORKERS" \
  --log-every-in 20000000 --log-every-out 5000000
