# Supertrend 4h Long-Only 3x — 파라미터 탐색 결과

## 개요

BTC/USDT 4h Supertrend 전략 6-파라미터 백테스트 전체 결과 저장소.
v4→v5→v5_2→v6→v7→v8 검증 체인 완료 (2026-05). **결론: 3x archive 확정, 실거래 = fa80 funding-arb 단독.**

## 3x-Only 정책

모든 백테스트는 **leverage=3x** 기준. 1x/2x는 v7에서 검증 완료 (avg_mdd -36~-87%, 1x만 MARGINAL, 2x이상 FAIL). v7 결과는 PG에 sweep='v7'로 통합됨.

## MDD 게이트: -80%

`run_intrabar_backtest.py PASS_MDD_PCT = -80.0`. 3x leverage에서 모든 1,296 combos가 full period -80% 미만 — recent (W7+W8) 기준 필터 사용 권장.

## PostgreSQL 통합

**SoT(Source of Truth): `backtest-postgres` 컨테이너 (`jesse_db`)**

| 테이블 | 내용 |
|---|---|
| `st_sweeps` | v4/v5/v5_2/v6/v7/v8 sweep 메타데이터 |
| `st_combos` | 전체 2,288 combos (v4=216, v5=324, v5_2=1296, v6=225, v7=3, v8=224) |
| `st_window_results` | window별 CAGR/MDD/Sharpe (W1~W8) |

### 마이그레이션 (일회성)
```bash
DC="docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester"
$DC python3 /app/scripts/db/migrate_all.py   # schema 적용 + v4~v7 CSV 적재
$DC python3 /app/scripts/db/verify.py        # row counts + 통계 검증
```

### 대시보드 빌드
```bash
$DC python3 /app/scripts/reports/build_dashboard.py
```
→ `dashboard_v2.html` 생성 (브라우저, offline, self-contained)
→ 소스 템플릿: `dashboard_template.html` (커밋됨, UI 셸)

## 추가 Sweet Spot 탐색 (신규 Sweep)

`scripts/run_sweep.py` 사용:
```bash
DC="docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester"

# 사전 확인 (조합 수만 출력)
$DC python3 /result/supertrend_x3_long_only/scripts/run_sweep.py \
    --sweep v9 \
    --grid-json '{"st_factor":[2.4,2.5],"st_period":[8,9],"fast_ema_len":[7,8],"slow_ema_len":[25,27],"direction_ema_len":[330,360,400,450],"atr_mult":[3.1,3.2]}' \
    --dry

# 실행 (그리드 생성 → 백테스트 → aggregate → 대시보드 재빌드)
$DC python3 /result/supertrend_x3_long_only/scripts/run_sweep.py \
    --sweep v9 \
    --grid-json '...' \
    --workers 6
```

### v8 신호 기반 다음 탐색 방향
- `direction_ema_len` 추가 확장: 400 이상 (v8 top이 방향 EMA 400 근방)
- `st_factor` 하단 확장: 2.3 추가 (v5_2에서 경계값)
- `atr_mult` 상단 확장: 3.3~3.5 추가

## 결과 디렉토리 정책 (2026-05-17 정리 완료)

모든 CSV/결과 디렉토리 → **PG 적재 확인 후 완전 삭제됨.** PG가 유일한 SoT.

| 삭제된 디렉토리 | 내용 | PG 상태 |
|---|---|---|
| `v4_optimization/` | v4_all_combos.csv + 216 combos | v4: 216 combos, 1728 window_results ✓ |
| `v5_optimization/` | v5_all_combos.csv + 324 combos | v5: 324 combos, 2592 window_results ✓ |
| `v5_2_optimization/` | v5_2_all_combos.csv + 1296 combos | v5_2: 1296 combos, 10368 window_results ✓ |
| `v6_optimization/` | v6_all_combos.csv + 225 combos | v6: 225 combos, 1800 window_results ✓ |
| `v7_leverage_test/` | v7_results.csv + c*_lev* 디렉토리 | v7: 3 combos, 6 window_results ✓ |
| `combo_0_W1~combo_223_W8` | v8 결과 1792개 | v8: 224 combos, 1792 window_results ✓ |

verdict/summary 문서는 `docs/sweeps/`로 이동. SQLite 기반 레거시 스크립트 삭제됨.

## 파일 목록

| 파일/디렉토리 | 설명 |
|---|---|
| `dashboard_template.html` | 대시보드 UI 셸 (커밋됨, CSS/HTML/JS 구조) |
| `dashboard_v2.html` | 생성된 대시보드 (빌드 시 생성, gitignored) |
| `docs/sweeps/` | sweep별 verdict + summary 문서 아카이브 |
| `docs/sweeps/v4_verdict.md` | v4 최종 판정 |
| `docs/sweeps/v4_summary.md` | v4 sweep 결과 요약 |
| `docs/sweeps/v5_verdict.md` | v5 최종 판정 |
| `docs/sweeps/v5_summary.md` | v5 sweep 결과 요약 |
| `docs/sweeps/v5_2_verdict.md` | v5_2 최종 판정 |
| `docs/sweeps/v5_2_summary.md` | v5_2 sweep 결과 요약 |
| `docs/sweeps/v6_verdict.md` | v6 최종 판정 |
| `docs/sweeps/v6_summary.md` | v6 sweep 결과 요약 |
| `docs/sweeps/v7_verdict.md` | v7 레버리지 테스트 판정 |
| `docs/sweeps/v8_direction_ema_sweep.md` | v8 sweep 결과 + 핵심 발견 |
| `scripts/run_sweep.py` | 신규 sweet spot 탐색 실행 스크립트 |
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
