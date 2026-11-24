---
title: Backtest — Jesse 스택 + 리플레이
last_updated: 2026-11-22
---

# Backtest

백테스트 전용 영역. 운영 코드(`cryptoengine/`)와 분리됨. 전략(Supertrend 4h 3x long-only, combo #7908)은 확정이며 파라미터 탐색은 종료됐다.

- 정본 지표: 198 trades, win 42.4%, CAGR +219.06%, MDD −66.70%, PF 1.507, Sharpe 1.667 (Binance **현물** 4h·00계열, 2017-08-17 ~ 2026-04-30, ATR 손절만).
- 라이브는 Bybit USDT 무기한. 백테스트 시세는 Binance 현물이며 펀딩비 미반영.

## 구조

| 경로 | 설명 |
|---|---|
| `docker/` | Jesse 백테스트 스택(`docker-compose.yml`, `Dockerfile`): `backtester`, `backtest-postgres` |
| `strategies/`, `scripts/strategies/` | Jesse 전략 코드 (Supertrend 등) |
| `scripts/runners/` | `run_external_backtest.py`, `run_intrabar_backtest.py`, `verify_backtest_consistency.py` |
| `scripts/data/` | Binance Vision 1m 적재, Bybit 펀딩 수집, OHLCV 게이트 SQL |
| `scripts/db/` | `jesse_db` 스키마와 공용 헬퍼 |
| `docs/` | 전략 R&D 히스토리 |

## Jesse 백테스트

```bash
cd backtest/docker
docker compose --profile backtest up -d backtest-postgres
docker compose --profile backtest run --rm backtester \
  python /app/scripts/runners/run_external_backtest.py --strat SupertrendStrategy --tf 4h --leverage 3
```

compose는 `../data`(읽기 전용), `../results`, `../strategies`, `../scripts`를 마운트한다.

## 리플레이 (정본 경량 백테스트)

엔진: `cryptoengine/tests/fixtures/_replay_supertrend.py` (라이브 지표 `bot/indicators.py` 사용, 수수료 0.055%/편, 종가 체결, 95%×3x). 인자 없이 돌리면 DB의 최신 데이터 전체로 실행한다.

입력 데이터는 Postgres 테이블이다 (봇이 4h봉마다 갱신).

| 테이블 | 내용 |
|---|---|
| `replay_ohlcv_4h` | Binance 현물 BTCUSDT 4h (ts ms, OHLCV) |
| `replay_reference_trades` | `trades_7908.csv` 기준 거래 목록 (진입 타이밍 대조용) |

시드: `btc_4h_extended.csv`(2017-08-17 ~ 2026-08-30)가 `btc_4h.csv`(~2026-04-30)의 상위집합이며 겹치는 19,057봉은 완전히 동일하다. 이후 구간은 `sync`가 Binance에서 채운다. 두 CSV의 일부 봉(93개, 주로 2017)은 open을 직전 종가로 채워 둔 값이라 Binance 원본과 다르다. 정본 지표가 이 데이터 기준이라 그대로 시드하며 `sync`는 새 봉만 추가하고 기존 봉은 덮어쓰지 않는다.

```bash
cd cryptoengine
export DB_HOST=127.0.0.1 DB_PORT=5432 DB_NAME=cryptoengine DB_USER=cryptoengine DB_PASSWORD=...

# 데이터 최초 적재 / 갱신
python3 -m bot.replay_data import-csv tests/fixtures/btc_4h_extended.csv
python3 -m bot.replay_data import-trades tests/fixtures/trades_7908.csv
python3 -m bot.replay_data sync            # 마지막 ts 이후 닫힌 봉만 Binance에서 upsert
python3 -m bot.replay_data status
python3 -m bot.replay_data export-csv /tmp/btc_4h.csv

# 리플레이
python3 tests/fixtures/_replay_supertrend.py --until 2026-04-30   # 동결 정본(198 / 219.06% / −66.70%)
python3 tests/fixtures/_replay_supertrend.py                      # DB 최신 데이터 전체 (다른 구간)
python3 tests/fixtures/_replay_supertrend.py --csv tests/fixtures/btc_4h.csv
```

`--until`은 `YYYY-MM-DD`(그 UTC 일 전체 포함) 또는 epoch ms. 최신 데이터 실행 결과는 정본과 다른 구간의 수치다.

## 문서

→ `backtest/docs/README.md`
