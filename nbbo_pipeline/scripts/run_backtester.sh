#!/usr/bin/env bash
set -euo pipefail

# nbbo_pipeline/scripts/run_backtester.sh
#
# Run backtester over a range of years for SPY.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${ROOT_DIR}/nbbo_pipeline/build/run_backtester"

EVENTS_DIR="${ROOT_DIR}/data/research/events"
HIST_PATH="${ROOT_DIR}/data/research/hist/SPY_histogram.json"
CFG_PATH="${ROOT_DIR}/nbbo_pipeline/config/strategy_params.json"

START_YEAR="${1:-2018}"
END_YEAR="${2:-2023}"

echo "Using:"
echo "  BIN        = ${BIN}"
echo "  EVENTS_DIR = ${EVENTS_DIR}"
echo "  HIST_PATH  = ${HIST_PATH}"
echo "  CFG_PATH   = ${CFG_PATH}"
echo "  YEARS      = ${START_YEAR}..${END_YEAR}"

"${BIN}" "${EVENTS_DIR}" "${HIST_PATH}" "${CFG_PATH}" "${START_YEAR}" "${END_YEAR}"
