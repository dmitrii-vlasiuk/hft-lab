#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.."; pwd)"

# Hint Homebrew Arrow/Parquet if needed
export CMAKE_PREFIX_PATH="${CMAKE_PREFIX_PATH:-/opt/homebrew:/opt/homebrew/opt/arrow}"

cmake -S "$ROOT" -B "$ROOT/build" -G Ninja
cmake --build "$ROOT/build" -j
