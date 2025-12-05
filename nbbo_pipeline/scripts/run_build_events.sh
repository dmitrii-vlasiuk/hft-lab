#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/build_events"

IN_DIR="$ROOT/data/out/event_clean"
OUT_DIR="$ROOT/data/research/events"
THRESHOLD_NEXT="1.0"

mkdir -p "$OUT_DIR"

declare -a YEARS=("2018" "2019" "2020" "2021" "2022" "2023")

for Y in "${YEARS[@]}"; do
  IN="$IN_DIR/SPY_${Y}.parquet"
  OUT="$OUT_DIR/SPY_${Y}_events.parquet"

  echo "[events] \"$IN\" -> \"$OUT\" threshold_next=\$$THRESHOLD_NEXT"
  "$BIN" \
      --in "$IN" \
      --out "$OUT" \
      --threshold-next "$THRESHOLD_NEXT"
done
