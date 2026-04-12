#!/bin/bash
# Phase 7.5 — Full V5 Validation Pipeline (Jesse 1.x Python API version)
# Usage: ./scripts/run_full_validation.sh <StrategyName>
# Runs: full backtest → WF → regime split → sanity check → V5 report
# Exit code: 0 = PASS, 1 = FAIL or CRITICAL warning

set -euo pipefail

STRATEGY="${1:?Usage: $0 <StrategyName>}"
FULL_START="2023-04-01"
FULL_END="2026-04-01"
RESULT_DIR="/home/justant/Data/Bit-Mania/.result/backtest/v5"
MAIN_JSON="storage/results/${STRATEGY}_main.json"

echo "========================================================"
echo " V5 Full Validation Pipeline: ${STRATEGY}"
echo " $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "========================================================"

mkdir -p storage/results storage/walk_forward storage/regime_split

# ─── Step 1: Full-period backtest ──────────────────────────────────────────────
echo ""
echo "[1/5] Full backtest: ${FULL_START} → ${FULL_END}"
docker compose run --rm jesse python scripts/run_backtest.py \
  --strategy "${STRATEGY}" \
  --start "${FULL_START}" --end "${FULL_END}" \
  --output "${MAIN_JSON}" \
  || { echo "ERROR: Backtest failed"; exit 1; }
echo "  Done: ${MAIN_JSON}"

# ─── Step 2: Walk-Forward ──────────────────────────────────────────────────────
echo ""
echo "[2/5] Walk-Forward (train=365d, test=180d, slide=90d)"
docker compose run --rm jesse python scripts/walk_forward.py \
  --strategy "${STRATEGY}" \
  --start "${FULL_START}" --end "${FULL_END}" \
  --train-days 365 --test-days 180 --slide-days 90 \
  || { echo "ERROR: Walk-Forward failed"; exit 1; }

# ─── Step 3: Regime split ──────────────────────────────────────────────────────
echo ""
echo "[3/5] Regime split analysis (bull / transition / compressed)"
docker compose run --rm jesse python scripts/regime_split_analysis.py \
  --strategy "${STRATEGY}" \
  --regimes "2023-04-01:2023-12-31:bull,2024-01-01:2024-12-31:transition,2025-01-01:2026-04-01:compressed" \
  || { echo "ERROR: Regime split failed"; exit 1; }

# ─── Step 4: Sanity check ──────────────────────────────────────────────────────
echo ""
echo "[4/5] Sanity check"
docker compose run --rm jesse python scripts/sanity_check.py \
  --result-json "${MAIN_JSON}" \
  --strategy "${STRATEGY}"
SANITY_EXIT=$?
if [ $SANITY_EXIT -ne 0 ]; then
  echo ""
  echo "  SANITY CHECK FAILED — CRITICAL warnings detected."
fi

# ─── Step 5: Generate V5 report ────────────────────────────────────────────────
echo ""
echo "[5/5] Generating V5 report"
docker compose run --rm jesse python scripts/generate_v5_report.py \
  --strategy "${STRATEGY}" \
  --output "${RESULT_DIR}/${STRATEGY}_v5_report.md" \
  || { echo "ERROR: Report generation failed"; exit 1; }

echo ""
echo "========================================================"
echo " COMPLETE: ${RESULT_DIR}/${STRATEGY}_v5_report.md"
echo "========================================================"

exit $SANITY_EXIT
