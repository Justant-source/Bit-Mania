# Database Migration Scripts

PostgreSQL migration scripts for loading strategy backtesting results into the Jesse backtester database.

## Files

- `schema.sql` - PostgreSQL schema (3 tables: st_sweeps, st_combos, st_window_results)
- `_common.py` - Shared utilities (DB connection, CSV parsing, upsert functions)
- `migrate_v4.py` - Load v4 grid (216 combos) from v4_all_combos.csv
- `migrate_v5.py` - Load v5 grid (324 combos) from v5_all_combos.csv
- `migrate_v5_2.py` - Load v5_2 dense grid (1296 combos) from v5_2_all_combos.csv
- `migrate_v6.py` - Load v6 asymmetric tp/sl (225 combos) from v6_all_combos.csv
- `migrate_v7.py` - Load v7 leverage validation (3 combos at 3x leverage) from v7_results.csv
- `verify.py` - Verify migration results (counts, ordering, field values)
- `migrate_all.py` - Orchestrator script (runs all migrations + schema + verification)

## Usage

From the backtester container:

```bash
# Run all migrations at once (recommended)
docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester \
  python3 /app/scripts/db/migrate_all.py

# Or run individual migrations
docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester \
  python3 /app/scripts/db/migrate_v4.py

# Run verification only
docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester \
  python3 /app/scripts/db/verify.py
```

From the host (assuming backtest-postgres is running on port 5433):

```bash
psql -h localhost -p 5433 -U jesse -d jesse_db -f backtest/scripts/db/schema.sql
```

## Database Configuration

- Host: `backtest-postgres` (in container) / `localhost` (from host)
- Port: `5432` (in container) / `5433` (from host)
- Database: `jesse_db`
- User: `jesse`
- Password: `***REMOVED***`

## Schema Overview

### st_sweeps
Metadata for each sweep (v4, v5, v5_2, v6, v7). Fields: sweep_id (PK), description, leverage, variant, grid_json, n_combos, created_at, source_csv.

### st_combos
Strategy parameters and aggregate results for each combo. Fields:
- sweep_id + combo_id (unique constraint)
- Parameters: st_factor, st_period, ema lengths, atr_mult, tp_atr_mult, sl_atr_mult, sl_margin_pct
- Results: mean_cagr, std_cagr, worst_mdd, mean_mdd, total_trades, liquidated, recent variants
- Tiers: tier1-4/tier_pass (v4/v5/v5_2), tier_a/b/c/final_tier (v6)
- Quality: plateau_quality, plateau_score, sweet_spot_score, safety_score
- xref_json: cross-version references and metadata
- raw_json: entire original CSV row as JSON

### st_window_results
Per-window performance metrics. Foreign key to st_combos. Fields:
- window: window identifier (W1-W8 for v4/v5/v6, 'full'/'recent' for v7)
- complete, cagr_raw, mdd_raw, cagr_adj, mdd_adj, sharpe, trades_count, liquidated, finishing_balance

## Idempotency

All scripts use ON CONFLICT clauses for idempotent upserts. Running migrations multiple times is safe — data will be updated but not duplicated.

## Progress Tracking

Each migration prints progress every 100 rows and a final count at completion.

## Verification

The `verify.py` script checks:
1. Sweep metadata (sweep_id, n_combos)
2. Combo counts (v4=216, v5=324, v5_2=1296, v6=225, v7=3)
3. Window results counts (v4/v5/v6=n*8, v5_2=1296*8, v7=3*2)
4. v5_2 top-10 sweet_spot_score ordering matches CSV
5. Random v5_2 combos: raw_json field values match CSV

## Error Handling

- Missing/empty CSV values are converted to Python None → NULL in PostgreSQL
- Boolean fields handle 'True'/'False'/'1'/'0' strings
- window_stats_json is parsed with graceful fallback to empty list on error
- Errors during migration halt the script and print traceback
