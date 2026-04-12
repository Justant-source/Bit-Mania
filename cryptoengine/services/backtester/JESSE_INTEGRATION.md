# Jesse Framework Integration Guide

**Date:** 2026-04-11  
**Status:** Complete - Template Ready for Data Integration  
**Phase:** 4 (Forward Testing)

---

## Summary

Jesse backtesting framework has been integrated into CryptoEngine at:
```
cryptoengine/services/backtester/jesse_project/
```

This replaces the custom backtester engine which had confirmed bugs in multi-symbol position tracking, funding rate income calculation, and leverage validation.

Jesse is production-tested, Bybit-native, and free for backtesting.

---

## What Was Created

### 1. Updated Dependencies

**File:** `services/backtester/requirements.txt`

Added:
```
jesse>=0.41.0
```

### 2. Jesse Project Structure

**Directory:** `services/backtester/jesse_project/`

```
jesse_project/
├── config.py                              # DB & cache configuration
├── strategies/
│   ├── __init__.py
│   ├── FundingArb.py                      # Funding arbitrage (primary)
│   └── MultiFundingRotation.py            # Multi-symbol rotation (secondary)
└── README.md                              # Full documentation
```

### 3. Strategies Implemented

#### **FundingArb.py** (Primary Strategy)
- **Purpose:** Collect positive funding rates with delta-neutral short positions
- **Entry:** Perpetual short when funding_rate >= 0.0001 (0.01%/8h)
- **Exit:** Close when funding_rate <= 0.00005 (0.005%/8h)
- **Settlement:** Every 8h (UTC 00:00, 08:00, 16:00)
- **Position Sizing:** Fixed notional with configurable leverage (max 5x per Phase 5)
- **Fee Handling:** Bybit 0.055% taker fee per side

**Hyperparameters (optimizable):**
```python
hp = {
    'min_funding_rate': 0.0001,   # Entry threshold
    'exit_funding_rate': 0.00005, # Exit threshold
    'leverage': 5.0,               # Max leverage
    'size_pct': 0.80,             # Deploy 80% of equity
}
```

**Included Variants:**
- `FundingArbConservative` — Tighter thresholds, smaller positions, lower leverage (3x)

#### **MultiFundingRotation.py** (Secondary Strategy)
- **Purpose:** Rotate capital between top funding rate symbols
- **Symbols:** BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT
- **Logic:** Every 8h, pick top 3 by funding rate
- **Position Sizing:** 20% equity per symbol (3 symbols = 60% total)
- **Note:** Requires multi-symbol data loader (template provided)

**Included Variants:**
- `SimpleFundingRotation` — Simplified 2-symbol rotation (BTC + ETH only)

---

## Quick Start

### 1. Install Jesse

```bash
# Already in requirements.txt, installs with:
docker compose build backtester

# Or manually:
pip install jesse>=0.41.0
```

### 2. Create jesse_db Database

```bash
psql -U cryptoengine -d postgres -c "CREATE DATABASE jesse_db;"

# Verify
psql -U cryptoengine -d jesse_db -c "SELECT version();"
```

### 3. Import Candle Data

For Bybit BTCUSDT from 2019-2025:

```bash
# Option A: Jesse's native Bybit importer
jesse import-candles Bybit BTCUSDT 1h 2019-01-01 2025-01-01

# Option B: From CryptoEngine's OHLCV table
psql -U cryptoengine -d cryptoengine -c \
  "COPY (SELECT timestamp, open, high, low, close, volume FROM ohlcv_history 
         WHERE symbol='BTCUSDT' AND timeframe='1h' ORDER BY timestamp) 
   TO STDOUT WITH CSV HEADER" > btcusdt_1h.csv

jesse import-candles --csv btcusdt_1h.csv Bybit BTCUSDT 1h
```

### 4. Run Backtest

```bash
# Docker
docker compose --profile backtest run --rm backtester \
  bash -c "cd jesse_project && jesse backtest FundingArb Bybit BTCUSDT 1h 2019-01-01 2025-12-31"

# Or directly (if Jesse is installed)
cd jesse_project
jesse backtest FundingArb Bybit BTCUSDT 1h 2019-01-01 2025-12-31
```

**Expected output:**
```
Strategy: FundingArb
Trades: [count]
Sharpe Ratio: [value]
Max Drawdown: [%]
Total Profit: $[amount]
```

---

## Current Status

### ✅ Complete

- [x] Jesse installed in requirements.txt
- [x] Project directory structure created
- [x] Configuration file (config.py) with DB settings
- [x] FundingArb strategy fully implemented
  - [x] Entry/exit signals based on funding rate
  - [x] Position sizing logic (fixed_notional compatible)
  - [x] Leverage enforcement (max 5x)
  - [x] Funding settlement tracking (8h)
  - [x] Fee deduction (0.055% taker)
  - [x] Conservative variant for stress testing
- [x] MultiFundingRotation strategy (template)
  - [x] Multi-symbol tracking
  - [x] Top-N funding rate selection
  - [x] Position rotation logic
  - [x] Simple variant (2-symbol)
- [x] Comprehensive README with:
  - [x] Installation instructions
  - [x] Data import guide
  - [x] Strategy documentation
  - [x] API reference
  - [x] Troubleshooting
  - [x] Integration points

### ⏳ Pending (Data Integration)

- [ ] **Funding Rate Data Loader**
  - Fetch from `cryptoengine.funding_rate_history` table
  - Merge with OHLCV candles
  - Pass via `shared_vars['funding_rate']` to strategy
  - **Impact:** Until implemented, strategies will have no trades (funding_rate = 0)
  - **Effort:** ~2-3 hours (create data loader, wire into Jesse)

- [ ] **Backtest Historical Data Collection** (optional)
  - Import 6 years of Bybit OHLCV (2019-2025)
  - Test against both engines (custom vs Jesse)
  - Validate parameter parity

- [ ] **Docker Integration** (optional)
  - Add `jesse-backtester` service profile to docker-compose.yml
  - Expose Jesse results to dashboard

---

## Integration: Funding Rate Data

### Problem

Strategies reference `self.current_funding_rate`, but Jesse's OHLCV candles don't include funding rates by default.

### Current Workaround

Strategies check for funding rate in `shared_vars`:

```python
@property
def current_funding_rate(self) -> float:
    if hasattr(self, 'shared_vars') and isinstance(self.shared_vars, dict):
        return self.shared_vars.get('funding_rate', {}).get(self.time, 0.0)
    return 0.0
```

Returns 0.0 until data is supplied → no trades yet.

### Solution (To Be Implemented)

Create a data loader in `jesse_project/loaders.py`:

```python
# Fetch funding rates from CryptoEngine DB
# Merge with Jesse's OHLCV
# Expose via shared_vars to strategy
```

**Steps:**
1. Create `jesse_project/loaders.py` with `load_ohlcv_with_funding()` function
2. Query `cryptoengine.funding_rate_history` table
3. Convert to dict: `{timestamp_ms: funding_rate}`
4. Pass to strategy via `self.shared_vars['funding_rate']`
5. Strategy now generates trades based on real funding rates

**Estimated effort:** 2-3 hours

---

## Validation Checklist

### Before Phase 5 Transition

Use this checklist to validate Jesse integration:

- [ ] Jesse 0.41.0+ installs without errors
- [ ] jesse_db database created and accessible
- [ ] 6-year OHLCV candles imported for BTCUSDT
- [ ] FundingArb backtest runs without errors
  - [ ] 2019-2025 full backtest completes
  - [ ] Results show trades > 0 (after funding rate integration)
  - [ ] Sharpe, CAGR, MDD metrics are reasonable
- [ ] FundingArb results match custom engine results
  - [ ] Total profit within ±5%
  - [ ] Trade count similar (same/similar entry/exit signals)
  - [ ] Any divergences documented and understood
- [ ] MultiFundingRotation template works (even without trades)
- [ ] Docker build succeeds: `docker compose build backtester`
- [ ] Can run in Docker: `docker compose --profile backtest run --rm backtester ...`

---

## File Locations

**Core Integration:**
- `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/jesse_project/` ← Main directory
- `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/requirements.txt` ← Added jesse
- `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/JESSE_INTEGRATION.md` ← This file

**Strategies:**
- `jesse_project/strategies/FundingArb.py` (400+ lines, fully documented)
- `jesse_project/strategies/MultiFundingRotation.py` (300+ lines, template)

**Configuration:**
- `jesse_project/config.py` (Jesse DB settings)

**Documentation:**
- `jesse_project/README.md` (Comprehensive guide, 400+ lines)

---

## Known Limitations

1. **Funding Rate Data**
   - Not in Jesse OHLCV by default
   - Strategy returns 0.0 until data loader is implemented
   - Will generate no trades until funding rate data is integrated

2. **Multi-Symbol Backtesting**
   - Jesse runs single symbol per session
   - MultiFundingRotation works as template but needs:
     - Separate backtest per symbol
     - Post-processing to aggregate with capital allocation

3. **Market Hours**
   - Jesse assumes 24/7 trading (crypto)
   - Bybit 24/7 trading matches Jesse default
   - No special handling needed

4. **Slippage**
   - Jesse defaults to zero slippage in backtest
   - Bybit high liquidity justifies this for large symbols
   - Can add slippage model if needed: `--slippage 0.001` (0.1%)

---

## Performance Notes

**Expected Results** (after funding rate integration):

Based on CryptoEngine's Phase 3 backtest with fa80_lev5_r30 parameters:
- **CAGR:** +34.87%
- **Sharpe Ratio:** 3.583
- **Max Drawdown:** -4.52%
- **Liquidations:** 0 in 6 years
- **Trades:** ~600-800 per year (entry on high funding, exit on low funding)

Jesse should reproduce similar or better results (fewer bugs in position tracking).

---

## Next Steps (Recommended Order)

1. **Immediate (today):**
   - Review `jesse_project/README.md` and `strategies/FundingArb.py`
   - Verify files are in place
   - Test Docker build: `docker compose build backtester`

2. **This week:**
   - Create `jesse_project/loaders.py` to load funding rates from CryptoEngine DB
   - Test with mock funding rate data
   - Run single backtest to verify framework works

3. **Next week:**
   - Import full 6-year OHLCV for BTCUSDT
   - Run full backtest FundingArb 2019-2025
   - Compare results with custom engine

4. **Before Phase 5:**
   - Validate parameter parity between engines
   - Document any differences
   - Confirm kill switch logic works in Jesse

---

## References

**Jesse Documentation:**
- Official Docs: https://jesse.trade/docs
- GitHub: https://github.com/jesse-ai/jesse
- Bybit Integration: https://jesse.trade/docs/exchanges/bybit

**CryptoEngine:**
- Phase 5 Gates: `CLAUDE.md` (PHASE5_MODE, fixed_notional, Kill Switch)
- Backtest Index: `services/backtester/tests/backtest/README.md`
- Shared Utilities: `services/backtester/tests/backtest/core/`

**Bybit API:**
- Trading Docs: https://bybit-exchange.github.io/docs
- Funding Rate Settlement: UTC 00:00, 08:00, 16:00 (every 8h)
- Taker Fee: 0.055% per side (perpetuals)

---

## Support & Troubleshooting

See `jesse_project/README.md` for detailed troubleshooting, including:
- Module import errors
- Database connection issues
- Missing candle data
- No trades generated
- Strategy logic validation

**Quick Command Reference:**

```bash
# Install Jesse
pip install jesse>=0.41.0

# Create database
psql -U cryptoengine -d postgres -c "CREATE DATABASE jesse_db;"

# List available strategies
jesse list-strategies

# Run backtest
jesse backtest FundingArb Bybit BTCUSDT 1h 2019-01-01 2025-12-31

# Docker backtest
docker compose --profile backtest run --rm backtester \
  bash -c "cd jesse_project && jesse backtest FundingArb Bybit BTCUSDT 1h 2019-01-01 2025-12-31"
```

---

**Integration Complete ✓**  
**Status:** Template Ready (Awaiting Funding Rate Data Integration)  
**Contact:** See CLAUDE.md for phase leads
