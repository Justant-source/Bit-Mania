---
title: Backtest CODE_MAP
category: backtest-docs
last_updated: 2026-05-17
---

# Backtest 코드 ↔ 문서 역인덱스

## 전략 코드

| 코드 경로 | 관련 문서 |
|---|---|
| `backtest/strategies/funding_arbitrage.py` | `docs/strategies/001_funding_arb.md` |
| `backtest/strategies/external/SupertrendStrategy.py` | `docs/runs/supertrend.md` |
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

## 최적화 파이프라인

### Phase 1 (p1_): Grid 생성 & 병렬 실행

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/scripts/optimization/p1_generate_grid.py` | 파라미터 그리드 생성 | 파라미터 범위 | SQLite 큐 (parameter_queue.db) |
| `backtest/scripts/optimization/p1_worker.py` | SQLite claim-and-run 워커 (다중 프로세스) | SQLite 큐 | 백테스트 결과 |
| `backtest/scripts/optimization/p1_master.py` | 멀티프로세싱 마스터 | SQLite 큐 | 통합 결과 |

### Phase 2 (p2_): 결과 분석 & 파레토

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/scripts/optimization/p2_aggregate.py` | 결과 수집 & 통계 계산 | 백테스트 결과 | aggregated metrics |
| `backtest/scripts/optimization/p2_pareto_plot.py` | 파레토 프론티어 분석 | aggregated metrics | pareto chart |

### Phase 3 (p3_): Walk-Forward & 검증

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/scripts/optimization/p3_walk_forward.py` | Walk-forward validation | 최적 파라미터 | walk-forward 결과 |
| `backtest/scripts/optimization/p3_yearly_sanity.py` | 연도별 건전성 검증 | 최적 파라미터 | 연도별 통계 |

### Phase 3 (v3_): 고도 최적화 파이프라인 (Fine-Grid)

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/scripts/optimization/v3_generate_grid.py` | 1,620 fine-grid SQLite 큐 생성 (sl=-25 고정, 3×5×4×3×3×3) | 파라미터 범위 | SQLite 큐 |
| `backtest/scripts/optimization/v3_worker.py` | SQLite claim-and-run 워커 (v2 p1_worker 패턴 재사용) | SQLite 큐 | 백테스트 결과 |
| `backtest/scripts/optimization/v3_master.py` | 6-worker ProcessPoolExecutor 마스터 + 30초 모니터링 | SQLite 큐 | 통합 결과 |
| `backtest/scripts/optimization/v3_aggregate.py` | 분석적 10구간 슬라이스 + 펀딩비 차감 + composite 점수 | 백테스트 결과 | aggregated metrics |
| `backtest/scripts/optimization/v3_walk_forward.py` | top-50 독립 10-fold + robustness + plateau detection | 최적 파라미터 | walk-forward 결과 |
| `backtest/scripts/optimization/v3_baseline.py` | combo_1390 baseline 11 백테스트 (H-0 게이트) | combo_1390 설정 | baseline 검증 결과 |

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

## PostgreSQL 통합 파이프라인 (PG-native, 2026-05)

Supertrend v4~v7 sweep 메타데이터 및 결과를 PostgreSQL로 중앙화. 향후 신규 sweep은 combo_X_W 디렉토리를 생성하지 않음.

### 마이그레이션 & 검증

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/scripts/db/schema_st.sql` | Supertrend sweep 테이블 스키마 (st_sweeps, st_combos, st_window_results) | — | PostgreSQL 테이블 |
| `backtest/scripts/db/migrate_v4.py` | v4 CSV 결과 적재 | `results/v4_optimization/summary.csv` + stats | `st_combos` + `st_window_results` (v4) |
| `backtest/scripts/db/migrate_v5.py` | v5 CSV 결과 적재 | v5 summary + window stats | st_combos + st_window_results (v5) |
| `backtest/scripts/db/migrate_v5_2.py` | v5_2 (dense grid) 결과 적재 | v5_2 parquet/CSV | st_combos + st_window_results (v5_2) |
| `backtest/scripts/db/migrate_v6.py` | v6 결과 적재 | v6 summary | st_combos + st_window_results (v6) |
| `backtest/scripts/db/migrate_v7.py` | v7 top-3 leverage sweep 결과 적재 | v7 결과 (1x/2x/3x) | st_combos + st_window_results (v7) |
| `backtest/scripts/db/migrate_all.py` | 전체 마이그레이션 오케스트레이터 (schema → v4~v7 순차) | 모든 버전 | 완전 적재 |
| `backtest/scripts/db/verify.py` | 테이블 무결성 검증 (row counts, 통계, 범위 체크) | PostgreSQL | 검증 리포트 |

### 신규 Sweep (PG-native)

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/scripts/optimization/pg_generate_grid.py` | sweep명 기반 파라미터 그리드 생성 및 PG 삽입 | 파라미터 범위 정의 | `st_combos` (sweep 레코드) |
| `backtest/scripts/optimization/pg_worker.py` | PG 큐 claim-and-run 워커 (한 번에 1 combo 처리) | `st_combos` (pending) | 백테스트 결과 |
| `backtest/scripts/optimization/pg_master.py` | 다중 워커 오케스트레이션 (6 workers, 임시 디렉토리 사용 후 자동 삭제) | 그리드 생성 후 st_combos | `st_combos` (completed), `st_window_results` (created) |
| `backtest/scripts/optimization/pg_aggregate.py` | 통계 계산 및 정렬 (CAGR/MDD/Sharpe, 파레토 마킹) | 백테스트 결과 | 분석 메트릭 (PG 저장) |

### 대시보드 & 리포팅

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/scripts/reports/build_dashboard.py` | PG 기반 통합 대시보드 생성 (v4~v8+) | `st_sweeps`, `st_combos`, `st_window_results` + `dashboard_template.html` | `results/supertrend_x3_long_only/dashboard_v2.html` (self-contained) |

### 결과 저장소

| 경로 | 설명 |
|---|---|
| `results/supertrend_x3_long_only/` | 3x leverage 통합 결과 (v4~v8+, 2288 combos) |
| `results/supertrend_x3_long_only/README.md` | 아카이브 정보 + 사용 가이드 |
| `results/supertrend_x3_long_only/dashboard_template.html` | 대시보드 UI 셸 (커밋됨, CSS/HTML/JS 구조) |
| `results/supertrend_x3_long_only/dashboard_v2.html` | 생성된 대시보드 (gitignored, 빌드 시 재생성) |
| `results/supertrend_x3_long_only/scripts/run_sweep.py` | 신규 sweet spot 탐색 실행 스크립트 (pg_* 파이프라인 래퍼) |
| `results/supertrend_x3_long_only/docs/sweeps/` | v4~v8 sweep별 verdict + summary 문서 |

## Jesse 엔진 인프라 (backtest/docker/)

| 코드 경로 | 관련 문서 |
|---|---|
| `backtest/strategies/` | `docs/methodology/jesse-strategies.md` |
| `backtest/scripts/shell/run_full_validation.sh` | `docs/methodology/jesse-engine.md` |
| `backtest/scripts/analysis/walk_forward.py` | `docs/methodology/jesse-engine.md` |
| `backtest/docker/docker-compose.yml` | `docs/methodology/jesse-engine.md` |

