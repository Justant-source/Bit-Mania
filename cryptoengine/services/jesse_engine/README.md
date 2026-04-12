# Jesse Engine — CryptoEngine V5

Phase 7-8 백테스트 환경. 자체 엔진(V1-V4)을 대체하는 Jesse 기반 검증 프레임워크.

## 빠른 시작

```bash
# 1. Jesse 컨테이너 기동
cd cryptoengine/services/jesse_engine
docker compose up -d

# 2. 컨테이너 상태 확인
docker compose logs jesse

# 3. 데이터 임포트 (Binance Vision Parquet → Jesse DB)
docker compose exec jesse python /jesse/scripts/jesse_import.py \
  --symbol BTCUSDT --timeframe 1h --start 2020-01-01 --end 2026-04-01

# 4. Sanity Check (BTC Buy-and-Hold 2024)
docker compose exec jesse jesse backtest \
  '2024-01-01' '2024-12-31' \
  --route 'Bybit Perpetual:BTCUSDT:1h:BtcBuyAndHold'

# 예상 결과: CAGR ~120%, MDD ~-25%, Sharpe 1.5-2.0
```

## 전략 목록

| 파일 | 전략 | 목적 |
|------|------|------|
| `strategies/sanity_check.py` | BtcBuyAndHold | Phase 7.2 엔진 검증용 |
| `strategies/intraday_seasonality.py` | IntradaySeasonality | Phase 8.1 일중 시즈널리티 |
| `strategies/macro_event.py` | MacroEvent | Phase 8.2 FOMC/CPI |
| `strategies/contrarian_sentiment.py` | ContrarianSentimentStandalone, SeasonalityWithFGFilter | Phase 8.3 F&G |

## 검증 파이프라인

```bash
# 단일 명령으로 전체 검증 (WF + MC + Sanity + 리포트)
./scripts/run_full_validation.sh <StrategyName>
```

## 선행 조건

1. `download_binance_vision.py` — BTCUSDT OHLCV Parquet 필요
2. `fetch_coinalyze_funding.py` — 펀딩비 Parquet 필요 (FundingTracker)
3. Jesse PostgreSQL DB가 cryptoengine 네트워크에 연결되어야 함
