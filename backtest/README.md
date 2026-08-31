---
title: Backtest — 전략 R&D 통합 트리
last_updated: 2026-08-31
---

# Backtest

전략 R&D 전용 영역. 운영 코드(`cryptoengine/`)와 완전히 격리됨.

**메인넷 현활 전략**: Supertrend 4h 3x long-only (combo #7908)  
- CAGR: +219.06%, Sharpe: 1.667, MDD: -66.70% (Binance **현물** 4h·00계열 격자 정본, ATR 손절만, 2026-08-20)
  - ⚠️ 라이브는 Bybit USDT 무기한 거래. 정본 백테스트 시세는 Binance 현물이며 종가 기준 평균 +0.05% 차이·펀딩비 미반영. 상세: `results/2026-08-31/csv_ohlcv_drift.md`
- 구 Binance 스윕 값(151.56%/1.37/-84.28%)은 폐기됨
- 배포: 2026-05-18~ (Phase 5)
- **파라미터 탐색은 2026-08-31 종료 권고**(v11/v12 3중 독립 검증 — 설계구간 우열이 홀드아웃과 반상관, ρ=−0.508). 상세: `results/2026-08-31/holdout_reverification.md`. 현재 R&D는 탐색이 아니라 **엣지 모니터링**(트립와이어) + **체결 품질 실측**(슬리피지) 단계.

## 구조

| 디렉토리 | 설명 |
|---|---|
| `data/` | OHLCV/funding 원본 (Parquet, .gitignore) |
| `results/` | 백테스트 산출물 (Parquet) |
| `scripts/` | 실행 스크립트 (카테고리별: runners, analysis, optimization, reports, db, data) |
| `strategies/` | 전략 코드 (외부: Supertrend, Stoch 등; 폐기됨: FA, DCA, TradeIQ, TrendType, BBPB 등) |
| `dashboards/` | 대시보드 HTML 산출물 |
| `docker/` | 백테스트 인프라 (backtester, backtest-postgres) |
| `docs/` | 문서 단일 소스 (strategies 001-021 R&D 히스토리 + methodology) |

## PostgreSQL 통합 (2026-05)

모든 v4~v9+ sweep 결과가 `backtest-postgres` (`jesse_db`)의 `st_sweeps/st_combos/st_window_results` 테이블로 통합됨.

**현행 대시보드**:
- **`build_supertrend_dashboard.py`**: Supertrend sweep 파라미터 공간 탐색 (v4~v9+) → `dashboards/supertrend_sweep_dashboard.html`

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
| `run_intrabar_backtest.py` | 일중 다중 타임프레임 분석 |
| `regime_split_analysis.py` | 시장 환경별 성과 분석 |

**대시보드 빌더**:
- `build_supertrend_dashboard.py` — Supertrend 정렬 및 히트맵 시각화

**최적화 (PG-native)**:
- `pg_generate_grid.py` — 파라미터 그리드 생성 및 PG 삽입
- `pg_clone_sweep.py` — 기존 sweep combo 파라미터만 새 sweep_id로 복사 (window 결과는 복사하지 않음)
- `pg_worker.py` — 개별 combo 처리 워커
- `pg_master.py` — 다중 워커 오케스트레이션
- `pg_aggregate.py` — 통계 계산 및 파레토 마킹
- `sweep_scheduler.py` — KST 00–06 6워커 / 그 외 2워커, cpuset·nice로 운영 Docker 양보

**ATR-SL 재스윕 (v10_notp)**: v7_st 15,000 combo를 현재 SupertrendStrategy(익절 없음)로 8윈도우 재실행하는 재스윕. **중단됨** (2026-08-29 기준) — `backtester` 컨테이너가 3개월간 미가동 상태이며, 15,000개 combo는 이미 Postgres에 적재된 채 재개 대기 중. 재개 시 기존 v6_st/v7_st 행은 덮어쓰지 않음. 완료 후 `pg_aggregate` + `build_supertrend_dashboard.py --sweeps v10_notp`.

## 리플레이 기반 R&D (v11~, 2026-08-30~) — 현행 도구

Jesse PG-sweep(위)과 별개의 경로. Jesse 8윈도우 선별 지표가 라이브 정합 성과와 사실상 무상관(ρ≈0.06)임이 v11에서 드러나면서, `#7908` 진입/청산 로직을 그대로 포팅한 **경량 Python 리플레이 엔진**(`cryptoengine/tests/fixtures/_replay_supertrend.py`)으로 이전했다. combo 1개 = 9년치 4h ~0.5초(구 Jesse 대비 ~150배). `backtest/scripts/analysis/v12/replay_lib.py`가 실행가정(수수료·워밍업 420봉·인트라바 안전스탑)을 고정한 얇은 래퍼다.

| 스크립트 | 목적 |
|---|---|
| `scripts/analysis/v12/replay_lib.py` | 공유 라이브러리 — 블록·격자·점수함수·이웃정의(FROZEN, 사전등록 이후 수정 금지) |
| `scripts/analysis/v12/run_grid.py`, `apply_plateau.py` | v12 1,536-combo 격자 채점 + plateau 판정 |
| `scripts/analysis/holdout_report.py`, `v11a_vs_7908.py` | v11 홀드아웃 리플레이(S7) |
| `scripts/analysis/live_slippage.py` | 실 체결 슬리피지 실측 — `orders` 테이블 vs 신호봉 종가 대조, 홀드아웃 재실행. 신규 체결 10건 누적 또는 분기 1회 재실행 (`docs/70-policy/operations.md` §3) |
| `scripts/analysis/tripwire_check.py` | 엣지 소멸 트립와이어 — `extend-csv`(jesse_db→CSV 연장)·`check`(T1 월간워닝/T2 블록게이트). 매월 1일 실행. 규칙: `results/tripwire/PREREGISTRATION_TRIPWIRE.md` |

**결론 계보 (전부 `#7908` 유지로 수렴)**:

| 단계 | 결론 | 문서 |
|---|---|---|
| v11 (Jesse 8윈도우 선별) | 채택 후보 5개 전원 홀드아웃 탈락 | `.temp/improve_strategy/RESULT.md` |
| v12 (사전등록 1,536격자 plateau 판정) | NO_PLATEAU — 후보 자체 없음 | `results/v12/VERDICT.md` |
| 최종 재검증 (원본 재계산 + 전이검정) | 설계구간 우열 ↔ 홀드아웃 성과 ρ=−0.508 (역상관) → **탐색 종료 권고** | `results/2026-08-31/holdout_reverification.md` |

**현행 우선순위(파라미터 탐색 아님)**: (A) 실 체결 슬리피지 실측, (B) 엣지 소멸 트립와이어 — 둘 다 위 표의 스크립트. 근거: `results/2026-08-31/holdout_reverification.md` §3.3.

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
```

## 결과 저장소

| 경로 | 설명 |
|---|---|
| `results/supertrend_x3_long_only/` | Supertrend 3x sweep 결과 + 문서 (v4~v9+) |
| `results/v12/` | v12 사전등록·1,536격자 채점·plateau 판정·최종 VERDICT |
| `results/2026-08-31/` | 홀드아웃 최종 재검증, 실 체결 슬리피지 실측, CSV/DB 데이터 출처 조사(Binance Spot vs Bybit Perpetual) |
| `results/tripwire/` | 엣지 소멸 트립와이어 사전등록 + 판정 로그(`log.md`, 매월 append) |

## 문서

→ `backtest/docs/README.md` (진입점)

## 경로 헬퍼

모든 스크립트는 `from scripts._paths import BT_ROOT, DATA_ROOT, RESULTS_ROOT, RUNTIME_ROOT`를 사용.
