---
title: Backtest CODE_MAP
category: backtest-docs
last_updated: 2026-05-13
---

# Backtest 코드 ↔ 문서 역인덱스

## 전략 코드

| 코드 경로 | 관련 문서 |
|---|---|
| `backtest/strategies/funding_arbitrage.py` | `docs/strategies/001_funding_arb.md` |
| `backtest/strategies/external/SupertrendStrategy.py` | `docs/runs/supertrend.md` |
| `backtest/strategies/external/MomentumMAStrategy.py` | `docs/runs/momentum_ma.md` |
| `backtest/strategies/external/StochStrategy.py` | `docs/runs/stoch.md` |
| `backtest/strategies/external/SupertrendTrendTypeStrategy.py` | `docs/runs/supertrend_trendtype.md` |
| `backtest/strategies/external/TradeIQPsarHaStrategy.py` | `docs/runs/tradeiq_psar_ha.md` |
| `backtest/strategies/external/TradeIQCciCeStrategy.py` | `docs/runs/tradeiq_cci_ce.md` |
| `backtest/strategies/external/TrendTypeStrategy.py` | `docs/runs/trendtype.md` |

## 스크립트

| 코드 경로 | 관련 문서 |
|---|---|
| `backtest/scripts/runners/run_external_backtest.py` | `docs/methodology/backtest-skillset.md` |
| `backtest/scripts/runners/v4_run_all.py` | `docs/methodology/backtest-skillset.md` |
| `backtest/scripts/sweep/param_sweep_v2.py` | `docs/optimization/param_sweep_v2.md`, `docs/runs/*.md` |
| `backtest/scripts/sweep/param_sweep_v3.py` | `docs/optimization/param_sweep_v3.md`, `docs/runs/*.md` |
| `backtest/scripts/analysis/rolling_window_analysis.py` | `docs/optimization/rolling_window_test.md` |
| `backtest/scripts/analysis/walk_forward.py` | `docs/methodology/jesse-engine.md` |
| `backtest/scripts/reports/build_v4_dashboard.py` | `docs/runs/*.md` |
| `backtest/scripts/reports/build_champion_dashboard.py` | `docs/runs/*.md` |
| `backtest/scripts/data/download_binance_vision.py` | `docs/methodology/backtest-skillset.md` |
| `backtest/scripts/data/jesse_import.py` | `docs/methodology/jesse-engine.md` |

## 비용 보정 & 펀딩 데이터 파이프라인

실거래 비용(Taker fee 델타 + 펀딩비)을 백테스트 결과에 사후 적용하는 파이프라인.
펀딩비는 8h 이벤트 단위로 정확하게 계산 (월별 평균 아님).

### 실행 순서

```
1. fetch_binance_funding_api.py   → BTCUSDT_2019.parquet (Binance 2019-09~2020-03)
2. merge_funding_sources.py       → BTCUSDT_8h.parquet 갱신 (Bybit + Binance 병합)
3. build_monthly_funding_estimates.py → BTCUSDT_monthly_estimates.csv (레거시 스크립트용)
4. apply_realistic_costs_7strategies.py → all_adjusted_results_7s.json
5. build_v4_dashboard.py          → dashboard.html (8h funding series 임베드)
```

### 펀딩비 모델 (per-event, 2026-05-13 이후)

- 데이터: `backtest/data/funding/BTCUSDT_8h.parquet` (8856 rows, 2018-04 ~ 2026-04)
  - Bybit 실데이터: 2020-03-25 → 현재
  - Binance 실데이터: 2019-09-08 → 2020-03-25 (gap fill)
  - zero-fill (fallback=0.000124): 2018-04 → 2019-09-07 (pre-launch)
- 계산: `Σ rate_i × notional × fund_sign` (포지션 보유 중 지나간 8h 이벤트 합산)
- notional = `qty × entry_price` (leverage 이미 반영됨)
- fund_sign: long_only=+1 (비용), short_only=−1 (수익), bidirectional=0

### 스크립트 인덱스

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/scripts/data/fetch_binance_funding_api.py` | Binance BTCUSDT 펀딩비 API 수집 (2019-09 ~ 2020-03) | Binance fapi | `data/funding/binance_api/BTCUSDT_2019.parquet` |
| `backtest/scripts/data/merge_funding_sources.py` | Bybit parquet의 zero-fill 행을 Binance로 치환 | Bybit parquet + Binance parquet | `data/funding/BTCUSDT_8h.parquet` (갱신) |
| `backtest/scripts/data/build_monthly_funding_estimates.py` | 월별 평균 펀딩율 CSV 생성 (레거시용) | `BTCUSDT_8h.parquet` | `data/funding/BTCUSDT_monthly_estimates.csv` |
| `backtest/scripts/analysis/apply_realistic_costs_7strategies.py` | 7-strategies 결과에 per-event 비용 보정 적용 | `results/7-strategies/*/trades.csv` + `BTCUSDT_8h.parquet` | `results/adjusted_costs_7strategies/all_adjusted_results_7s.json` |
| `backtest/scripts/analysis/apply_realistic_costs.py` | 219 survivors (v2/v3) 비용 보정 (월별 추정치 기반) | `results/param_sweep/*/summary.json` + monthly CSV | `results/adjusted_costs/*/adjusted_stats.json` |
| `backtest/scripts/analysis/apply_realistic_costs_pre21.py` | pre-2021 결과 비용 보정 (월별 추정치 기반) | sweep 결과 + monthly CSV | `results/adjusted_costs_pre2021/` |

## Jesse 엔진 인프라 (backtest/docker/)

| 코드 경로 | 관련 문서 |
|---|---|
| `backtest/strategies/` | `docs/methodology/jesse-strategies.md` |
| `backtest/scripts/shell/run_full_validation.sh` | `docs/methodology/jesse-engine.md` |
| `backtest/scripts/analysis/walk_forward.py` | `docs/methodology/jesse-engine.md` |
| `backtest/docker/docker-compose.yml` | `docs/methodology/jesse-engine.md` |

