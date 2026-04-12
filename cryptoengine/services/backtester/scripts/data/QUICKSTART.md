# Phase 4 Data Pipeline - Quick Start

## 30-Second Setup

```bash
cd /home/justant/Data/Bit-Mania

# Run all scripts in sequence
python cryptoengine/services/backtester/scripts/data/fetch_binance_vision.py
python cryptoengine/services/backtester/scripts/data/fetch_coinalyze.py
python cryptoengine/services/backtester/scripts/data/fetch_fear_greed.py

# For FRED (macro data) - need API key first
export FRED_API_KEY=$(cat .env | grep FRED_API_KEY | cut -d= -f2)
python cryptoengine/services/backtester/scripts/data/fetch_fred.py

# Validate all data
python cryptoengine/services/backtester/scripts/data/validate_data_integrity.py

# Check report
cat cryptoengine/services/backtester/docs/data_quality_report.md
```

## Via Docker

```bash
docker compose --profile backtest run --rm backtester \
  python scripts/data/fetch_binance_vision.py
```

## What Each Script Does

| Script | Downloads | Time | Key? |
|--------|-----------|------|------|
| `fetch_binance_vision.py` | BTCUSDT OHLCV (1m/1h/1d) + funding rates from Binance | 30m | No |
| `fetch_coinalyze.py` | BTC Open Interest + Liquidations | 5m | No |
| `fetch_fear_greed.py` | Crypto sentiment index | 1s | No |
| `fetch_fred.py` | Fed rates, treasury yields, balance sheet, CPI | 10s | Yes* |
| `validate_data_integrity.py` | QA check + report generation | 1m | No |

*Free FRED API key: https://fred.stlouisfed.org/docs/api/api_key.html (instant)

## Data Locations

```
cryptoengine/data/
├── binance_vision/klines/BTCUSDT/{1m,1h,1d}/*.parquet
├── binance_vision/funding/BTCUSDT/*.parquet
├── coinalyze/open_interest/*.parquet
├── coinalyze/liquidations/*.parquet
├── fear_greed/fear_greed_index.parquet
└── fred/*.parquet
```

## Get FRED API Key (2 Minutes)

1. Go to: https://fred.stlouisfed.org/docs/api/api_key.html
2. Click "Request API Key"
3. Sign up (free email)
4. Copy key
5. Set env: `export FRED_API_KEY=your_key_here`
6. Or add to `.env`: `FRED_API_KEY=your_key_here`

## What the Validation Report Shows

```
✅ OHLCV data: 1m/1h/1d coverage, gap count, price anomalies
✅ Funding rates: row count, rate range (min/max/mean)
✅ Fear & Greed: value range, date coverage
✅ FRED macro: series availability
```

Check report at: `cryptoengine/services/backtester/docs/data_quality_report.md`

## Troubleshooting

**"ModuleNotFoundError: No module named 'httpx'"**
- Inside Docker: auto-installed in backtester image
- Locally: `pip install httpx pandas pyarrow`

**"HTTP 404" on old Binance data**
- Normal - some very old files don't exist on Binance Vision
- Script skips automatically, no action needed

**FRED rate limit error**
- Free tier: 1 call/sec limit
- Script auto-sleeps between calls
- Just retry in 60 seconds if you hit limits

**No data downloaded**
- Check internet connection
- Verify source APIs are online (https://data.binance.vision, https://coinalyze.net)
- Try with `--verbose` flag if available

## Next: Jesse Integration

After data download + validation:

1. Import OHLCV into Jesse: `jesse_project/README.md`
2. Load funding rates into strategy
3. Run Jesse backtests
4. Apply bug fixes (Track C)
5. Remove synthetic data fallbacks (Track D)

---

Full documentation: `README.md` in this directory
Detailed report: `.result/backtest/v4/phase4_scripts_report.md`
