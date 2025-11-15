#!/usr/bin/env bash
set -euo pipefail

# From repo root
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mkdir -p "$ROOT/build"
cmake -S "$ROOT" -B "$ROOT/build" -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build "$ROOT/build" -j

echo "Built: $ROOT/build/clean_mid_spikes"
