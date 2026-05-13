#!/usr/bin/env bash
# Pre-2021 backfill orchestration
# Steps:
#   0. Fetch Binance API funding data (2019-09 ~ 2019-12)
#   3. Apply realistic costs (after backfill completes)
#   4. Run sanity checks
#   5. Regenerate champion dashboard
#
# NOTE: Step 2 (actual backtest jobs) must run INSIDE the Jesse Docker container.
#       Run that separately: docker exec jesse_backtest python /app/scripts/sweep/pre21_backfill.py --workers 6

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKTEST_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Pre-2021 Backfill Orchestration ==="
echo "Backtest root: $BACKTEST_ROOT"
echo ""

echo "[0] Fetching Binance API funding data (2019-09 ~ 2019-12)..."
python3 "$BACKTEST_ROOT/scripts/data/fetch_binance_funding_api.py"
echo ""

echo "[3] Applying realistic costs..."
python3 "$BACKTEST_ROOT/scripts/analysis/apply_realistic_costs_pre21.py"
echo ""

echo "[4] Running sanity checks..."
python3 "$BACKTEST_ROOT/scripts/validation/c3_pre21_sanity.py"
STATUS=$?
if [ $STATUS -ne 0 ]; then
    echo "ERROR: Sanity checks failed (exit $STATUS). Check backtest/results/validation_pre21_sanity.json"
    exit $STATUS
fi
echo ""

echo "[5] Regenerating champion dashboard..."
python3 "$BACKTEST_ROOT/scripts/reports/build_champion_dashboard.py"
echo ""

echo "=== Done ==="
