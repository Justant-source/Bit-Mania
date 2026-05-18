---
title: Backtest CODE_MAP
category: backtest-docs
last_updated: 2026-05-18
---

# Backtest 코드 ↔ 문서 역인덱스

## 전략 코드

| 코드 경로 | 관련 문서 |
|---|---|
| `backtest/strategies/external/SupertrendStrategy.py` | `backtest/docs/strategies/017_supertrend.md` |
| `backtest/strategies/external/StochStrategy.py` | `backtest/docs/strategies/016_stoch.md` |
| `backtest/strategies/external/SupertrendTrendTypeStrategy.py` | `backtest/docs/strategies/020_supertrend_trendtype.md` |
| `backtest/strategies/external/TradeIQPsarHaStrategy.py` | `backtest/docs/strategies/018_tradeiq_psar_ha.md` |
| `backtest/strategies/external/TradeIQCciCeStrategy.py` | `backtest/docs/strategies/021_tradeiq_cci_ce.md` |
| `backtest/strategies/external/TrendTypeStrategy.py` | `backtest/docs/strategies/019_trendtype.md` |

## 스크립트

| 코드 경로 | 관련 문서 |
|---|---|
| `backtest/scripts/runners/run_external_backtest.py` | `backtest/docs/methodology/backtest-skillset.md` |
| `backtest/scripts/runners/run_stt_only.py` | `backtest/docs/strategies/020_supertrend_trendtype.md` |
| `backtest/scripts/runners/run_supertrend_only.py` | `backtest/docs/strategies/017_supertrend.md` |
| `backtest/scripts/analysis/rolling_window_analysis.py` | `backtest/docs/methodology/backtest-skillset.md` |
| `backtest/scripts/analysis/walk_forward.py` | `backtest/docs/methodology/jesse-engine.md` |
| `backtest/scripts/analysis/apply_realistic_costs_7strategies.py` | `backtest/docs/strategies/` |
| `backtest/dashboards/script/build_strategy_dashboard.py` | `backtest/docs/strategies/` |
| `backtest/dashboards/script/build_supertrend_dashboard.py` | `backtest/results/supertrend_x3_long_only/README.md` |
| `backtest/scripts/reports/build_champion_dashboard.py` | `backtest/docs/strategies/` |
| `backtest/scripts/reports/sync_7strategies_docs.py` | `backtest/docs/strategies/` |
| `backtest/scripts/data/download_binance_vision.py` | `backtest/docs/methodology/backtest-skillset.md` |
| `backtest/scripts/data/jesse_import.py` | `backtest/docs/methodology/jesse-engine.md` |
| `backtest/scripts/data/fetch_bybit_funding_history.py` | `backtest/docs/methodology/backtest-skillset.md` |
| `backtest/scripts/data/resample_4h_from_1h.py` | `backtest/docs/methodology/backtest-skillset.md` |
| `backtest/scripts/data/validate_data_quality.py` | `backtest/docs/methodology/backtest-skillset.md` |

## PG-native 최적화 파이프라인 (현행)

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/scripts/optimization/pg_generate_grid.py` | sweep명 기반 파라미터 그리드 생성 및 PG 삽입 | 파라미터 범위 정의 | `st_combos` (sweep 레코드) |
| `backtest/scripts/optimization/pg_worker.py` | PG 큐 claim-and-run 워커 (한 번에 1 combo 처리) | `st_combos` (pending) | 백테스트 결과 |
| `backtest/scripts/optimization/pg_master.py` | 다중 워커 오케스트레이션 (6 workers, 임시 디렉토리 사용 후 자동 삭제) | 그리드 생성 후 st_combos | `st_combos` (completed), `st_window_results` (created) |
| `backtest/scripts/optimization/pg_aggregate.py` | 통계 계산 및 정렬 (CAGR/MDD/Sharpe, 파레토 마킹) | 백테스트 결과 | 분석 메트릭 (PG 저장) |
| `backtest/results/supertrend_x3_long_only/scripts/run_sweep.py` | 신규 sweet spot 탐색 실행 래퍼 (pg_* 파이프라인 오케스트레이션) | --grid-json, --sweep 인자 | PG + 대시보드 재빌드 |

## DB 스키마 & 검증

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/scripts/db/schema_st.sql` | Supertrend sweep 테이블 스키마 (st_sweeps, st_combos, st_window_results) | — | PostgreSQL 테이블 |
| `backtest/scripts/db/verify.py` | 테이블 무결성 검증 (row counts, 통계, 범위 체크) | PostgreSQL | 검증 리포트 |

## 대시보드 & 리포팅

| 코드 경로 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `backtest/dashboards/script/build_strategy_dashboard.py` | 7-strategies 대시보드 생성 | `results/7-strategies/` + `BTCUSDT_8h.parquet` | `dashboards/strategy_dashboard.html` |
| `backtest/dashboards/script/build_supertrend_dashboard.py` | Supertrend sweep 대시보드 생성 (v4~v9+) | `st_sweeps`, `st_combos`, `st_window_results` + 템플릿 | `dashboards/supertrend_sweep_dashboard.html` |
| `backtest/dashboards/script/run_supertrend_dashboard.py` | 대시보드 단독 재빌드 래퍼 | — | `dashboards/supertrend_sweep_dashboard.html` |
| `backtest/dashboards/script/supertrend_dashboard_template.html` | 대시보드 UI 셸 (커밋됨) | — | — |
| `backtest/scripts/audit/audit_v4_dashboard.py` | 7-strategies 대시보드 데이터 무결성 검사 | 대시보드 HTML | 검사 리포트 |

## Jesse 엔진 인프라

| 코드 경로 | 관련 문서 |
|---|---|
| `backtest/jesse_engine/` | Jesse 1.x 런타임 환경 (스토리지 + .env) |
| `backtest/strategies/` | `backtest/docs/methodology/jesse-strategies.md` |
| `backtest/scripts/shell/run_full_validation.sh` | `backtest/docs/methodology/jesse-engine.md` |
| `backtest/scripts/analysis/walk_forward.py` | `backtest/docs/methodology/jesse-engine.md` |
| `backtest/docker/docker-compose.yml` | `backtest/docs/methodology/jesse-engine.md` |

## 결과 저장소

| 경로 | 설명 |
|---|---|
| `backtest/results/supertrend_x3_long_only/` | 3x leverage 통합 결과 + sweep 문서 (v4~v9+) |
| `backtest/results/7-strategies/` | 7-strategies 비교 결과 (rolling window + per-strategy) |
| `backtest/dashboards/` | 대시보드 HTML 출력물 (gitignored, 빌드 시 재생성) |
