#!/bin/bash
# Run from project root or any location — script auto-detects cryptoengine/ dir.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"  # up to backtest/
cd "$BT_DIR"

echo "=== Batch 1 V2 Backtest Execution ==="
echo "Output directory: results/batch_1/"

# Test configurations
TESTS=(
  "BBPBStrategy:bidirectional:results/batch_1/bbpb/bidirectional"
  "BBPBStrategy:long_only:results/batch_1/bbpb/long_only"
  "BBWPStrategy:bidirectional:results/batch_1/bbwp/bidirectional"
  "BBWPStrategy:long_only:results/batch_1/bbwp/long_only"
  "StochStrategy:bidirectional:results/batch_1/stoch/bidirectional"
  "StochStrategy:long_only:results/batch_1/stoch/long_only"
)

for test_config in "${TESTS[@]}"; do
  IFS=':' read -r strategy variant output_dir <<< "$test_config"

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "Running: $strategy ($variant)"
  echo "Output: $output_dir"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  docker compose -f docker/docker-compose.yml --profile backtest run --rm backtester \
    python /app/scripts/runners/run_external_backtest.py \
      --strategy "$strategy" --variant "$variant" \
      --balance 10000 --leverage 1 \
      --start 2021-04-01 --end 2025-12-31 \
      --output "$output_dir" 2>&1

  if [ $? -eq 0 ]; then
    echo "✓ $strategy ($variant) completed successfully"
  else
    echo "✗ $strategy ($variant) failed - continuing..."
  fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "All backtests completed"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
