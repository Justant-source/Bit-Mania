---
title: Backtest — 전략 R&D 통합 트리
last_updated: 2026-08-20
---

# Backtest

전략 R&D 전용 영역. 운영 코드(`cryptoengine/`)와 완전히 격리됨.

**메인넷 현활 전략**: Supertrend 4h 3x long-only (combo #7908)  
- CAGR: +219.06%, Sharpe: 1.667, MDD: -66.70% (Bybit 네이티브 4h 정본, ATR 손절만, 2026-08-20)
- 구 Binance 스윕 값(151.56%/1.37/-84.28%)은 폐기됨
- 배포: 2026-05-18~ (Phase 5)

## 구조

| 디렉토리 | 설명 |
|---|---|
| `data/` | OHLCV/funding 원본 (Parquet, .gitignore) |
| `results/` | 백테스트 산출물 (Parquet) |
| `scripts/` | 실행 스크립트 (카테고리별: runners, analysis, optimization, reports, db, data) |
| `strategies/` | 전략 코드 (외부: Supertrend, Stoch 등; 폐기됨: FA, DCA, TradeIQ, TrendType, BBPB 등) |
| `dashboards/` | 대시보드 HTML 산출물 |
| `docker/` | 백테스트 인프라 (backtester, wf-scheduler, backtest-postgres) |
| `docs/` | 문서 단일 소스 (strategies 001-021 R&D 히스토리 + methodology) |

## PostgreSQL 통합 (2026-05)

모든 v4~v9+ sweep 결과가 `backtest-postgres` (`jesse_db`)의 `st_sweeps/st_combos/st_window_results` 테이블로 통합됨.

**현활 대시보드**:
- **`build_supertrend_dashboard.py`**: Supertrend sweep 파라미터 공간 탐색 (v4~v9+) → `dashboards/supertrend_sweep_dashboard.html`
- **`build_strategy_dashboard.py`**: 7-strategies 비교 (Supertrend/Stoch/etc, 롤링윈도우 분석) → `dashboards/strategy_dashboard.html`

**삭제된 스크립트** (2026-05-18):
- `run_supertrend_only.py` (대체: `run_external_backtest.py --strat SupertrendStrategy`)
- `run_stt_only.py` (대체: `run_external_backtest.py --strat SupertrendTrendTypeStrategy`)
- `run_plateau_backtests.py` (PG-native `pg_master.py`로 통합)
- `run_remaining_backtests.py` (PG-native `pg_master.py`로 통합)

**삭제된 전략** (2026-05-18):
- Funding Arbitrage (v1-v3) — FA 차익거래
- DCA — 분할 매수
- Stochastic (Stoch) — 확률론적 오실레이터
- TradeIQ (PSAR, HA, CCI, CE) — 거래소신호
- TrendType — 트렌드 분류기
- BBPB, BBWP, Asymmetric, WithSL — Bollinger Band 파생

## 백테스트 스크립트 (보존)

| 스크립트 | 목적 |
|---|---|
| `run_external_backtest.py` | 단일 전략 백테스트 (포괄적 러너) |
| `run_backtest.py` | Jesse 엔진 백테스트 (아카이브용) |
| `run_intrabar_backtest.py` | 일중 다중 타임프레임 분석 |
| `rolling_window_analysis.py` | 롤링윈도우 강건성 검증 |
| `walk_forward.py` | Walk-Forward 분석 |
| `regime_split_analysis.py` | 시장 환경별 성과 분석 |
| `wf_generate_report.py` | Walk-Forward 리포트 생성 |

**대시보드 빌더**:
- `build_supertrend_dashboard.py` — Supertrend 정렬 및 히트맵 시각화
- `build_strategy_dashboard.py` — 7-strategies 비교 (historical)

**최적화 (PG-native)**:
- `pg_generate_grid.py` — 파라미터 그리드 생성 및 PG 삽입
- `pg_clone_sweep.py` — 기존 sweep combo 파라미터만 새 sweep_id로 복사 (window 결과는 복사하지 않음)
- `pg_worker.py` — 개별 combo 처리 워커
- `pg_master.py` — 다중 워커 오케스트레이션
- `pg_aggregate.py` — 통계 계산 및 파레토 마킹
- `sweep_scheduler.py` — KST 00–06 6워커 / 그 외 2워커, cpuset·nice로 운영 Docker 양보

**ATR-SL 재스윕 (v10_notp, 2026-08-20~)**: v7_st 15,000 combo를 현재 SupertrendStrategy(익절 없음)로 8윈도우 재실행. 기존 v6_st/v7_st 행은 덮어쓰지 않음. 완료 후 `pg_aggregate` + `build_supertrend_dashboard.py --sweeps v10_notp`.

**데이터 + 유틸**:
- `download_binance_vision.py`, `fetch_bybit_funding_history.py`, `resample_4h_from_1h.py`, `validate_data_quality.py`
- `schema_st.sql`, `verify.py` (DB 스키마 + 검증)

## 빠른 시작

```bash
# 인프라 기동
cd backtest/docker
docker compose --profile backtest up -d backtest-postgres

# Supertrend 백테스트 실행
docker compose --profile backtest run --rm backtester \
  python /app/scripts/runners/run_external_backtest.py --strat SupertrendStrategy --tf 4h --leverage 3

# 대시보드 재빌드
docker compose --profile backtest run --rm backtester \
  python /app/dashboards/script/build_supertrend_dashboard.py
docker compose --profile backtest run --rm backtester \
  python /app/dashboards/script/build_strategy_dashboard.py
```

## 결과 저장소

| 경로 | 설명 |
|---|---|
| `results/supertrend_x3_long_only/` | Supertrend 3x sweep 결과 + 문서 (v4~v9+) |
| `results/7-strategies/` | 7-strategies 비교 (rolling window) |

## 문서

→ `backtest/docs/README.md` (진입점)

## 경로 헬퍼

모든 스크립트는 `from scripts._paths import BT_ROOT, DATA_ROOT, RESULTS_ROOT, RUNTIME_ROOT`를 사용.
