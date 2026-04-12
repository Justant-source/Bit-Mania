# Phase 4 Data Pipeline Scripts

Real market data collection for backtest v2 rebuild.

## Quick Start

```bash
# Download all data (run in order)
python fetch_binance_vision.py    # OHLCV + funding (~30 mins)
python fetch_coinalyze.py         # OI + liquidations (~5 mins)
python fetch_fear_greed.py        # Sentiment (~1 sec)
export FRED_API_KEY=your_key && python fetch_fred.py  # Macro (~10 secs)
python validate_data_integrity.py # Verify all data

# Or via Docker
docker compose --profile backtest run --rm backtester \
  python scripts/data/fetch_binance_vision.py
```

## Scripts

| Script | Purpose | Source | Time | Key? |
|--------|---------|--------|------|------|
| `fetch_binance_vision.py` | OHLCV 1m/1h/1d + funding history | Binance Vision | 30m | No |
| `fetch_coinalyze.py` | Open Interest + Liquidations | Coinalyze API | 5m | No |
| `fetch_fear_greed.py` | Crypto sentiment index | alternative.me | 1s | No |
| `fetch_fred.py` | Fed funds rate, treasury, balance sheet, CPI | FRED API | 10s | Yes* |
| `validate_data_integrity.py` | Check for gaps, anomalies, nulls | Local | 1m | No |

*FRED key is free and instant from https://fred.stlouisfed.org/docs/api/api_key.html

## Data Output

```
cryptoengine/data/
├── binance_vision/
│   ├── klines/BTCUSDT/{1m,1h,1d}/{year}/{month}.parquet
│   └── funding/BTCUSDT/{year}-{month}.parquet
├── coinalyze/
│   ├── open_interest/BTCUSDT_{daily,hourly}.parquet
│   └── liquidations/BTCUSDT_hourly.parquet
├── fear_greed/
│   └── fear_greed_index.parquet
└── fred/
    └── {DFF,DGS10,WALCL,CPIAUCSL}.parquet
```

All files: Parquet format, ZSTD compression, sorted by timestamp.

## Validation

After running all scripts, check `docs/data_quality_report.md`:
- Timestamp gaps (no missing candles)
- Price jump anomalies
- Missing values count
- Data coverage (date ranges)

## Integration

Data feeds:
- **Jesse backtests**: `jesse_project/strategies/FundingArb.py` (OHLCV + funding)
- **Liquidation cascade**: `tests/backtest/stress/bt_liquidation_cascade.py` (Coinalyze)
- **Calendar spread**: `tests/backtest/fa/bt_calendar_spread.py` (FRED rates)
- **Sentiment filters**: Combined strategies (Fear & Greed)

## Troubleshooting

**"No such file or directory"**: Ensure you're in `cryptoengine/` directory when running.

**HTTP 404 on older months**: Some Binance Vision files don't exist for very old dates. Scripts skip automatically.

**FRED rate limit**: Scripts respect 1 req/sec. If errors occur, retry in 60 seconds.

**Empty Fear & Greed**: Alternative.me API occasionally times out. Retry in a few minutes.

## Dependencies

```
httpx        # HTTP requests
pandas       # Data processing
pyarrow      # Parquet I/O
```

All included in `backtester` Docker image.
