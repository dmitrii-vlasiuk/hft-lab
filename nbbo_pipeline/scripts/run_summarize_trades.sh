#!/usr/bin/env bash
set -euo pipefail

# Directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Repo root is parent of scripts/
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BUILD_DIR="${REPO_ROOT}/build"
BIN="${BUILD_DIR}/summarize_trades"

TRADES_DIR="${REPO_ROOT}/data/research/trades"
OUT_FILE="${REPO_ROOT}/data/research/summary/yearly_pnl.txt"

# Default years
DEFAULT_YEARS="2018-2023"

# If the user passes args, use those as years; otherwise use default.
if [ "$#" -gt 0 ]; then
  YEARS="$*"
else
  YEARS="${DEFAULT_YEARS}"
fi

# Create output directory if needed
mkdir -p "$(dirname "${OUT_FILE}")"

echo "Running summarize_trades..."
echo "  trades_dir = ${TRADES_DIR}"
echo "  out_file   = ${OUT_FILE}"
echo "  years      = ${YEARS}"

# Pass years as separate args (so ranges/lists just work)
"${BIN}" "${TRADES_DIR}" "${OUT_FILE}" ${YEARS}
