# Jesse Backtesting Framework Integration

This directory contains the Jesse backtesting framework integration for CryptoEngine.

## Overview

**Jesse** is a crypto-native open-source backtesting framework (~6.6k GitHub stars):
- **Bybit native** support (official integration)
- **Perpetuals-capable** (futures trading with leverage)
- **Built-in Monte Carlo simulation** for robustness testing
- **PostgreSQL candle storage** for persistent historical data
- **Free** for backtesting (no licensing fees)

Jesse replaces our custom backtesting engine because the custom engine has confirmed bugs in:
- Multi-symbol position tracking
- Funding rate income calculation
- Leverage validation at edge cases

Jesse is production-tested and widely used in professional quant trading.

---

## Directory Structure

```
jesse_project/
├── config.py                          # Jesse configuration (DB, caching)
├── strategies/
│   ├── __init__.py
│   ├── FundingArb.py                  # Funding arbitrage (delta-neutral short)
│   └── MultiFundingRotation.py        # Multi-symbol funding rotation
└── README.md                          # This file
```

---

## Installation

### 1. Install Jesse

```bash
# Inside the backtester Docker container (or your Python env)
pip install jesse>=0.41.0
```

Jesse is already added to `cryptoengine/services/backtester/requirements.txt`.

### 2. Create Jesse Project Folder

```bash
cd cryptoengine/services/backtester/

# If not already done:
jesse create-project  # Interactive setup (optional)
# Or use the provided jesse_project/ directory directly
```

### 3. Set Up Database

Jesse requires a separate PostgreSQL database to store candles and backtest results.

```bash
# Inside container or on host
psql -U cryptoengine -d postgres -c "CREATE DATABASE jesse_db;"

# Verify connection
psql -U cryptoengine -d jesse_db -c "SELECT version();"
```

**Database name:** `jesse_db` (configured in `config.py`)
- Separate from main `cryptoengine` database to avoid conflicts
- Connection: `postgresql://cryptoengine:CryptoEngine2026!@localhost:5432/jesse_db`

---

## Quick Start

### Import Candle Data

Jesse provides built-in data import for major exchanges. For Bybit data:

#### Option 1: Jesse's Official Bybit Importer

```bash
jesse import-candles Bybit BTCUSDT 1h 2019-01-01 2025-01-01
```

This uses Jesse's official Bybit connector (if available in your version).

#### Option 2: Import from Parquet Files (Binance Vision)

If you have Binance parquet data, convert and import:

```bash
# Convert parquet to CSV
python -c "
import pandas as pd
df = pd.read_parquet('BTCUSDT-1h-2019-01.parquet')
df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
df.to_csv('btcusdt_1h.csv', index=False)
"

# Import to Jesse
jesse import-candles --csv btcusdt_1h.csv Bybit BTCUSDT 1h
```

#### Option 3: Use CryptoEngine's OHLCV Pipeline

Our `services/backtester/scripts/` directory has data collectors that store OHLCV in PostgreSQL.
You can export this and import to Jesse:

```bash
# Export from CryptoEngine DB to CSV
psql -U cryptoengine -d cryptoengine -c \
  "COPY (SELECT timestamp, open, high, low, close, volume FROM ohlcv_history 
         WHERE symbol='BTCUSDT' AND timeframe='1h' ORDER BY timestamp) 
   TO STDOUT WITH CSV HEADER" > btcusdt_1h.csv

# Import to Jesse
jesse import-candles --csv btcusdt_1h.csv Bybit BTCUSDT 1h
```

### Run a Backtest

#### Using FundingArb Strategy

```bash
# Standard backtest
jesse backtest FundingArb Bybit BTCUSDT 1h 2020-01-01 2025-12-31

# With custom config
jesse backtest FundingArb Bybit BTCUSDT 1h 2020-01-01 2025-12-31 \
  --fee 0.055 \
  --margin 5
```

#### Docker Execution

```bash
# Build backtester image
docker compose build backtester

# Run backtest
docker compose --profile backtest run --rm backtester \
  bash -c "cd jesse_project && jesse backtest FundingArb Bybit BTCUSDT 1h 2020-01-01 2025-12-31"
```

#### Python API (Programmatic)

```python
from jesse.config import config
from jesse.modes import backtest_mode

# Set up config
config['env']['databases']['postgres'] = {
    'driver': 'postgres',
    'host': 'localhost',
    'port': 5432,
    'name': 'jesse_db',
    'username': 'cryptoengine',
    'password': 'CryptoEngine2026!',
}

# Run backtest
results = backtest_mode.run(
    strategy_name='FundingArb',
    exchange='Bybit',
    symbol='BTCUSDT',
    timeframe='1h',
    start_date='2020-01-01',
    end_date='2025-12-31',
    leverage=5,
    fee_rate=0.055,
)

print(f"Total Profit: {results['total_profit']}")
print(f"Sharpe Ratio: {results['sharpe_ratio']}")
print(f"Max Drawdown: {results['max_drawdown']}")
```

---

## Strategy Implementation

### FundingArb Strategy

**Location:** `strategies/FundingArb.py`

**Domain Logic:**
- Entry: Perpetual short when funding_rate >= 0.0001 (0.01%/8h)
- Exit: Close position when funding_rate <= 0.00005 (0.005%/8h)
- Funding collection: At every 8h settlement (UTC 00:00, 08:00, 16:00)
- Position sizing: Fixed notional (e.g., $150 per position)
- Leverage: Up to 5x per Phase 5 constraints

**Hyperparameters (can be tuned in backtest):**

```python
hp = {
    'min_funding_rate': 0.0001,   # Entry threshold
    'exit_funding_rate': 0.00005, # Exit threshold
    'leverage': 5.0,               # Max 5x
    'size_pct': 0.80,             # Deploy 80% of equity
}
```

**Key Methods:**

| Method | Purpose |
|--------|---------|
| `current_funding_rate` | Get funding rate for current symbol (from shared_vars) |
| `is_settlement_time()` | Check if we're at 00:00, 08:00, or 16:00 UTC |
| `should_short()` | Entry signal (funding_rate >= threshold) |
| `should_exit_short()` | Exit signal (funding_rate <= threshold) |
| `update_position()` | Accumulate funding income at settlements |

**Important Notes:**

1. **Funding Rate Data:**
   - Jesse's OHLCV candles don't include funding rate by default
   - Strategy looks for funding rate in `self.shared_vars['funding_rate']`
   - **TODO:** Integrate with Coinalyze API or CryptoEngine's funding rate pipeline
   - Until integrated, strategy won't generate trades (funding_rate = 0)

2. **Fee Handling:**
   - Bybit taker fee: 0.055% per side
   - Entry fee: 0.00065 (spot 0.01% + futures 0.055%)
   - Exit fee: 0.00065 (spot 0.01% + futures 0.055%)
   - Jesse deducts fees automatically from PnL

3. **Leverage Limits:**
   - Max leverage: 5x per CLAUDE.md Phase 5 constraints
   - Jesse enforces margin validation per trade

### MultiFundingRotation Strategy

**Location:** `strategies/MultiFundingRotation.py`

**Domain Logic:**
- Track funding rates for 4 symbols: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT
- Every 8h, pick top 3 symbols by funding rate
- Hold short positions in top 3, rotate out of lower-ranked symbols
- Fixed position sizing: 20% equity per symbol (max 3 = 60% total)

**Note:** This strategy requires multi-symbol support in Jesse. Jesse backtests typically run single symbol per session. For multi-symbol, you would:

1. Run separate backtests per symbol
2. Aggregate results with capital allocation logic
3. Or use Jesse's custom indicator API to pull cross-symbol data

---

## Funding Rate Data Integration

### Current Status

Strategies load funding rate data from `self.shared_vars['funding_rate']`:

```python
@property
def current_funding_rate(self) -> float:
    if hasattr(self, 'shared_vars') and isinstance(self.shared_vars, dict):
        return self.shared_vars.get('funding_rate', {}).get(self.time, 0.0)
    return 0.0
```

### Integration Points

1. **CryptoEngine Funding Pipeline:**
   - `services/backtester/scripts/fetch_coinalyze_funding.py` collects funding rates
   - Data stored in PostgreSQL `funding_rate_history` table
   - Jesse data loader should query this table

2. **Data Loader Hook:**
   - Create a custom Jesse data loader that:
     - Fetches OHLCV from Jesse's native storage
     - Fetches funding rates from `cryptoengine` DB
     - Merges them into OHLCV + funding_rate tuples
     - Passes funding_rate via `shared_vars`

3. **Example Implementation:**

```python
# jesse_project/loaders.py (new file)
import asyncpg
from datetime import datetime

async def load_ohlcv_with_funding(
    symbol: str, 
    timeframe: str, 
    start_date: str, 
    end_date: str
) -> dict:
    """Load OHLCV + funding rates from databases."""
    
    # Connect to CryptoEngine DB
    pool = await asyncpg.create_pool(
        "postgresql://cryptoengine:CryptoEngine2026!@localhost/cryptoengine"
    )
    
    # Fetch funding rates
    query = """
    SELECT timestamp, funding_rate 
    FROM funding_rate_history 
    WHERE symbol = $1 AND timestamp >= $2 AND timestamp < $3
    ORDER BY timestamp
    """
    
    funding = await pool.fetch(
        query, symbol, start_date, end_date
    )
    
    # Convert to dict for Jesse's shared_vars
    funding_dict = {
        int(row['timestamp'].timestamp() * 1000): row['funding_rate']
        for row in funding
    }
    
    return {
        'funding_rate': funding_dict
    }
```

Then modify strategy to use this loader.

---

## Running Backtests via Docker

### Single Backtest

```bash
docker compose --profile backtest run --rm backtester \
  bash -c "cd jesse_project && jesse backtest FundingArb Bybit BTCUSDT 1h 2020-01-01 2025-12-31"
```

### Parameter Sweep (Grid Search)

Create a helper script `jesse_project/run_grid_search.py`:

```python
#!/usr/bin/env python
"""Grid search over FundingArb hyperparameters."""

import subprocess
import json
from itertools import product

MIN_FUNDING_RATES = [0.00005, 0.0001, 0.00015]
EXIT_RATES = [0.000025, 0.00005, 0.0001]
LEVERAGES = [3, 5, 7]

results = []

for min_fr, exit_fr, lev in product(MIN_FUNDING_RATES, EXIT_RATES, LEVERAGES):
    print(f"\nTesting: min_fr={min_fr}, exit_fr={exit_fr}, lev={lev}")
    
    cmd = [
        "jesse", "backtest", "FundingArb", "Bybit", "BTCUSDT", "1h",
        "2020-01-01", "2025-12-31",
        f"--min-funding-rate={min_fr}",
        f"--exit-funding-rate={exit_fr}",
        f"--leverage={lev}",
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    results.append({
        'params': {'min_fr': min_fr, 'exit_fr': exit_fr, 'lev': lev},
        'output': result.stdout
    })

# Save results
with open('grid_search_results.json', 'w') as f:
    json.dump(results, f, indent=2)
```

Run via Docker:

```bash
docker compose --profile backtest run --rm backtester \
  python jesse_project/run_grid_search.py
```

---

## Reporting and Analysis

### Generate Reports

Jesse outputs results to the console and optionally to JSON:

```bash
jesse backtest FundingArb Bybit BTCUSDT 1h 2020-01-01 2025-12-31 --json
```

### Custom Report Script

Create `jesse_project/generate_report.py`:

```python
import json
import pandas as pd

# Load Jesse's output
with open('backtest_result.json') as f:
    result = json.load(f)

print(f"Strategy: {result['strategy_name']}")
print(f"Total Profit: ${result['total_profit']:.2f}")
print(f"Sharpe Ratio: {result['sharpe_ratio']:.3f}")
print(f"Max Drawdown: {result['max_drawdown']:.2%}")
print(f"Win Rate: {result['win_rate']:.2%}")
print(f"Total Trades: {result['total_trades']}")

# Compare with CryptoEngine backtest results
# (save to DB or CSV for analysis)
```

---

## Troubleshooting

### Issue: "Module jesse not found"

**Solution:** Install jesse in your environment
```bash
pip install jesse>=0.41.0
```

### Issue: "Database jesse_db does not exist"

**Solution:** Create the database
```bash
psql -U cryptoengine -d postgres -c "CREATE DATABASE jesse_db;"
```

### Issue: "No candles found for symbol"

**Solution:** Import candles first
```bash
jesse import-candles Bybit BTCUSDT 1h 2019-01-01 2025-01-01
```

(Requires Jesse's Bybit integration and API credentials, or import from CSV)

### Issue: "Strategy produces no trades"

**Likely cause:** Funding rate data not available

**Check:**
1. Is `shared_vars['funding_rate']` populated?
2. Are there any log messages indicating zero funding rates?
3. Test manually:
   ```python
   strategy.current_funding_rate  # Should return > 0
   ```

**Fix:**
- Implement funding rate data loader (see Integration Points above)
- Or mock funding rates for testing:
   ```python
   self.shared_vars['funding_rate'] = {
       self.time: 0.0001,  # Mock 0.01%/8h
   }
   ```

---

## API Documentation

### Jesse Strategy Hooks

| Hook | Called | Purpose |
|------|--------|---------|
| `should_long()` | Every candle | Entry signal for long |
| `should_short()` | Every candle | Entry signal for short |
| `go_long()` | After should_long() returns True | Execute long entry |
| `go_short()` | After should_short() returns True | Execute short entry |
| `should_exit_long()` | Every candle while long | Exit signal |
| `should_exit_short()` | Every candle while short | Exit signal |
| `should_cancel_entry()` | While pending order | Cancel order |
| `update_position()` | Every candle while open | Update position logic |
| `on_open_position()` | After order fill | Position opened |
| `on_close_position()` | After exit order fill | Position closed |

### Jesse Position API

```python
self.position.qty          # Position quantity (positive=long, negative=short)
self.position.pnl          # Unrealized PnL in %
self.position.pnl_dollar   # Unrealized PnL in $
self.position.is_open      # True if position exists
self.position.entry_price  # Entry price

self.balance               # Current account balance
self.equity                # Current equity (balance + pnl)
self.close                 # Current candle close price
self.time                  # Current candle timestamp (ms)
```

### Order Placement

```python
self.buy_at_market(qty)              # Market buy order
self.sell_at_market(qty)             # Market sell order
self.buy_at_limit(qty, price)        # Limit buy order
self.sell_at_limit(qty, price)       # Limit sell order
```

For shorts, use negative qty:
```python
self.sell_at_market(-qty)            # Open short
self.buy_at_market(abs(qty))         # Close short
```

---

## Next Steps

1. **Integrate Funding Rate Data:**
   - Implement funding rate loader from CryptoEngine DB
   - Update FundingArb strategy to use real funding rates
   - Test with historical Bybit funding data

2. **Backtest FundingArb:**
   - Run 6-year backtest (2019-2025)
   - Compare results with custom engine (should be similar or better)
   - Document any divergences

3. **Multi-Symbol Support:**
   - Implement data loader for 4+ symbols
   - Extend MultiFundingRotation to run with real multi-symbol data
   - Test rotation logic

4. **Production Integration:**
   - Export optimized parameters from Jesse backtests
   - Update live trading config (`config/strategies/funding-arb.yaml`)
   - Deploy to Phase 5

---

## References

- **Jesse Documentation:** https://jesse.trade/docs
- **Bybit Trading API:** https://bybit-exchange.github.io/docs
- **CryptoEngine Backtester Skill Index:** `tests/backtest/README.md`
- **Phase 5 Phase Gates:** `CLAUDE.md` (PHASE5_MODE, Kill Switch, fixed_notional)

---

## Support

For issues or questions:
1. Check Jesse's official docs: https://jesse.trade
2. Review CryptoEngine CLAUDE.md for Phase 5 constraints
3. See `tests/backtest/core/` for shared utilities
4. Consult existing backtests in `tests/backtest/fa/` for examples

---

**Created:** 2026-04-11  
**Framework Version:** Jesse >= 0.41.0  
**CryptoEngine Phase:** 4 (Forward Testing)  
**Status:** Integration Template (Funding Rate Data Loading Pending)
