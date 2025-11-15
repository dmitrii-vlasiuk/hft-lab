#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/clean_mid_spikes"

IN_DIR="$ROOT/data/out/event"
OUT_DIR="$ROOT/data/out/event_clean"
THR="100"

mkdir -p "$OUT_DIR"

declare -a YEARS=("2018" "2019" "2020" "2021" "2022" "2023")

for Y in "${YEARS[@]}"; do
  IN="$IN_DIR/SPY_${Y}.parquet"
  OUT="$OUT_DIR/SPY_${Y}.parquet"
  echo "[denoise] \"$IN\" -> \"$OUT\" threshold=\$$THR"
  "$BIN" --in "$IN" --out "$OUT" --thr "$THR" --progress 10000000
done
