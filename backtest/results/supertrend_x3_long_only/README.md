# Supertrend 4h Long-Only 3x — 파라미터 탐색 결과

## 개요

BTC/USDT 4h Supertrend 전략 6-파라미터 백테스트 전체 결과 저장소.
v4→v5→v5_2→v6→v7 검증 체인 완료 (2026-05). **결론: 3x archive 확정, 실거래 = fa80 funding-arb 단독.**

## 3x-Only 정책

모든 백테스트는 **leverage=3x** 기준. 1x/2x는 v7에서 검증 완료 (avg_mdd -36~-87%, 1x만 MARGINAL, 2x이상 FAIL). v7 결과는 PG에 sweep='v7'로 통합됨.

## MDD 게이트: -80%

`run_intrabar_backtest.py PASS_MDD_PCT = -80.0`. 3x leverage에서 모든 1,296 combos가 full period -80% 미만 — recent (W7+W8) 기준 필터 사용 권장.

## PostgreSQL 통합

**SoT(Source of Truth): `backtest-postgres` 컨테이너 (`jesse_db`)**

| 테이블 | 내용 |
|---|---|
| `st_sweeps` | v4/v5/v5_2/v6/v7 sweep 메타데이터 |
| `st_combos` | 전체 2,064 combos (v4=216, v5=324, v5_2=1296, v6=225, v7=3) |
| `st_window_results` | window별 CAGR/MDD/Sharpe (W1~W8, v7=full/recent) |

### 마이그레이션 (일회성)
```bash
DC="docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester"
$DC python3 /app/scripts/db/migrate_all.py   # schema 적용 + v4~v7 CSV 적재
$DC python3 /app/scripts/db/verify.py        # row counts + 통계 검증
```

### 대시보드 빌드
```bash
$DC python3 /app/scripts/reports/build_dashboard.py \
  --out /result/supertrend_x3_long_only/dashboard.html
```
→ `dashboard.html` 열기 (브라우저, offline, self-contained)

## 향후 신규 Sweep (PG-native, combo_X_W 디렉토리 미생성)
```bash
# 1. 그리드 생성
$DC python3 /app/scripts/optimization/pg_generate_grid.py --sweep my_sweep
# 2. 백테스트 실행 (6 worker 병렬, 임시 디렉토리 사용 후 자동 삭제)
$DC python3 /app/scripts/optimization/pg_master.py --sweep my_sweep --workers 6
# 3. 대시보드 재빌드
$DC python3 /app/scripts/reports/build_dashboard.py
```

## 결과 디렉토리 정책 (2026-05-17 갱신)

- `v4~v6_optimization/`, `v7_leverage_test/`: **combo_X_W 디렉토리 + queue.sqlite3 삭제됨**. 원본 `*_all_combos.csv`/`v7_results.csv`는 provenance로 보존. PG가 SoT — raw_json+window_results로 CSV 전체 복원 가능.
- 옛 SQLite 기반 옵티마이저 스크립트(v4~v7): `scripts/legacy_optimizers/`로 이동 (이력 참조용). 현역은 `backtest/scripts/optimization/pg_*`.

## 파일 목록

| 파일 | 설명 |
|---|---|
| `dashboard.html` | PG 기반 생성 통합 대시보드 (v4~v7+) |
| `dashboard_v2.html` | UI 레퍼런스 (v5_2 전용, hand-authored) |
| `scripts/legacy_optimizers/` | 옛 v4~v7 SQLite 기반 스크립트 (deprecated) |
| `README.md` | 이 파일 |

## Sweep 결과 요약

| sweep | combos | grid | 특징 |
|---|---|---|---|
| v4 | 216 | 4×3×3×3×3×1(SL=0) | 기준선 확립. combo_18 baseline (144.99%/149.17) |
| v5 | 324 | +atr_mult 3단계 | ROBUST 23개, PLATEAU 0개. v4 drift=0% |
| v5_2 | 1296 | dense grid (사이값+확장) | PLATEAU 159개, ROBUST 476개. top carrier: 173/176/164 |
| v6 | 225 | 비대칭 TP/SL (top-25×9) | 모든 worst_mdd -90%대, 비대칭 무효 확인 |
| v7 | 3 | top-3 carrier × 1x~3x lev | 1x MARGINAL (-36%), 2x+ FAIL. 3x archive 최종 확정 |
| v8 | 224 | dir_ema 확장 (250→400) | sweet-spot 코어 고정, direction_ema_len 7단계 탐색 |

## 파라미터 정의

| param | 범위 (v5_2) | 설명 |
|---|---|---|
| st_factor | 2.4~2.7 | Supertrend ATR 배수 |
| st_period | 6~9 | Supertrend ATR 기간 |
| fast_ema_len | 7~9 | 진입 신호 빠른 EMA |
| slow_ema_len | 25~30 | 진입 신호 느린 EMA |
| direction_ema_len | 230~270 | 방향 필터 EMA (200 EMA 계열) |
| atr_mult | 3.0~3.2 | ATR stop-loss 배수 |
