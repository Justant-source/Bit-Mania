---
title: param_sweep_v3 — Champion 인근 세밀 격자 + stoch/momentum_ma 재탐색
category: test
related_code:
  - cryptoengine/services/jesse_engine/scripts/param_sweep_v3.py
last_updated: 2026-05-12
status: PARTIAL_COMPLETE
when_to_update: 나머지 5개 전략(supertrend_trendtype, trendtype, tradeiq_psar_ha, stoch, momentum_ma) 전체 24-combo 실행 시
---

# param_sweep_v3 — Champion 인근 세밀 격자 + stoch/momentum_ma 재탐색

## 개요

param_sweep_v2(2,688 backtests) 완료 후, v2 champion 인근의 파라미터 공간을 추가 탐색하고  
v2에서 전 조합 FAIL한 stoch/momentum_ma의 근본 원인을 수정·재탐색하는 v3 스윕.

**v2 대비 주요 변경사항**:
1. TF: 4h/1D만 (1h 제외) → 불필요한 탐색 제거
2. stoch: `use_direction_ema=False` → `True` 수정 (방향 필터 활성화)
3. momentum_ma: `val_ma_len=100` 고정 → `[50, 100, 200]` 3번째 파라미터로 추가
4. 각 전략 24 combo × 2 TF × 2 variant = 96 jobs per strategy
5. 출력 경로: `backtest-results/data/param_sweep_v3/`

---

## 평가 기준 (v2와 동일)

| 기간 | 시작 | 종료 | 의미 |
|------|------|------|------|
| P1 | 2021-04-01 | 2026-04-30 | 고점 매수 → 현재 (최악 진입) |
| P2 | 2022-12-01 | 2026-04-30 | 저점 매수 → 현재 (최선 진입) |
| P3 | 2021-04-01 | 2025-09-30 | 고점 → 다음 고점 (전체 사이클) |
| P4 | 2022-12-01 | 2025-09-30 | 저점 → 고점 (Bull run) |

**점수식**: `mean(P1~P4 CAGR)` if ALL periods `MDD ≥ -35%` AND `trades ≥ 5` else `-999`

---

## 실행 현황 (2026-05-12 기준)

| 전략 | 계획 combos | 실행 combos | 유효 combos | 상태 |
|------|------------|------------|------------|------|
| supertrend | 96 | 96 | 48 (4h 24 + 1D 24) | ✅ 전체 완료 |
| tradeiq_cci_ce | 96 | 13 (4h/bidir) | 6 | ⚠️ 부분 완료 |
| supertrend_trendtype | 96 | 13 (4h/bidir) | 0 | ⚠️ 부분 완료 |
| trendtype | 96 | 13 (4h/bidir) | 0 | ⚠️ 부분 완료 |
| tradeiq_psar_ha | 96 | 13 (4h/bidir) | 0 | ⚠️ 부분 완료 |
| stoch | 96 | 50 (4h×2var 24+2 + 1D×2var) | 0 | ⚠️ 부분 완료 |
| momentum_ma | 96 | 12 | 0 | ⚠️ 부분 완료 |

> 스윕 중단 이유: 충분한 신규 champion 발견(supertrend 4h/1D, tradeiq_cci_ce 4h/bidir)으로  
> 나머지 전략은 v2 champion을 유지. 필요시 개별 전략 재실행 가능.

---

## v3 Champion 결과 (v2 대비 개선된 조합)

| 전략 | TF | Variant | Combo | 주요 파라미터 | Score | P1 CAGR | P4 CAGR | vs v2 |
|------|----|---------|-------|------------|-------|---------|---------|-------|
| supertrend | 4h | long_only | v3 c3 | factor=2.5, period=12 | **+38.92** | +26.3% | +55.6% | +0.53↑ |
| supertrend | 1D | long_only | v3 c19 | factor=2.8, period=7 | **+31.71** | +23.9% | +42.3% | +1.23↑ |
| tradeiq_cci_ce | 4h | bidir | v3 c2 | cci_period=18, ce_mult=3.0 | **+29.38** | +19.6% | +48.2% | +12.11↑ |

### 개선 없는 전략 (v2 champion 유지)

| 전략 | TF | v2 Champion | v3 best | 판정 |
|------|----|------------|---------|------|
| supertrend_trendtype | 4h | factor=2.0, atr_len=10, +31.78 | 부분 탐색, 유효 없음 | v2 유지 |
| trendtype | 4h/1D | atr_len=10, di_len=10, +29.33/+23.74 | 부분 탐색, 유효 없음 | v2 유지 |
| tradeiq_psar_ha | 1D | rsi_len=18, atr_mult=3.0, +22.12 | 부분 탐색, 유효 없음 | v2 유지 |
| stoch | — | FAIL | direction_ema=True에서도 전 조합 FAIL | 여전히 투입 불가 |
| momentum_ma | — | FAIL | val_ma_len 다양화에도 FAIL | 여전히 투입 불가 |

---

## v2+v3 통합 최종 Champion (대시보드 반영 기준)

Champion run: v2+v3 summary 통합 후 (strat, tf, variant)별 최고 score combo 선정  
→ `backtest-results/data/7-strategies/` 전체기간 재실행 완료

| 전략 | TF | Variant | 파라미터 | Score | 출처 |
|------|----|---------|---------|-------|------|
| supertrend | 4h | long_only | factor=2.5, period=12 | +38.92 | v3 c3 |
| supertrend | 4h | bidir | factor=3.0, period=9 | +14.90 | v3 c9 |
| supertrend | 1D | long_only | factor=2.8, period=7 | +31.71 | v3 c19 |
| supertrend | 1h | long_only | factor=5.0, period=7 | +10.66 | v2 c11 |
| supertrend_trendtype | 4h | long_only | factor=2.0, atr_len=10 | +31.78 | v2 c6 |
| supertrend_trendtype | 1D | long_only | factor=2.5, atr_len=12 | +27.64 | v2 c10 |
| supertrend_trendtype | 1h | long_only | factor=5.0, atr_len=14 | +13.61 | v2 c11 |
| trendtype | 4h | long_only | atr_len=10, di_len=10 | +29.33 | v2 c6 |
| trendtype | 1D | long_only | atr_len=10, di_len=10 | +23.74 | v2 c6 |
| tradeiq_psar_ha | 1D | long_only | rsi_len=18, atr_mult=3.0 | +22.12 | v2 c3 |
| tradeiq_psar_ha | 4h | long_only | rsi_len=21, atr_mult=4.0 | +11.67 | v2 c9 |
| tradeiq_cci_ce | 4h | bidir | cci_period=18, ce_mult=3.0 | +29.38 | v3 c2 |
| tradeiq_cci_ce | 1h | bidir | cci_period=26, ce_mult=3.0 | +7.91 | v2 c3 |

> stoch/momentum_ma: 전 조합 FAIL — 대시보드 optimal_params에 미포함

---

## 각 전략별 24-Combo 파라미터 계획

### 1. SupertrendStrategy (st_factor × st_period)

| # | st_factor | st_period | 비고 |
|---|-----------|-----------|------|
| 1 | 2.5 | 9 | period 확장 |
| 2 | 2.5 | 11 | period 확장 |
| 3 | 2.5 | 12 | period 확장 |
| 4 | 2.5 | 14 | period 확장 |
| 5 | 2.3 | 7 | factor 세밀 |
| 6 | 2.7 | 7 | factor 세밀 |
| 7 | 2.3 | 10 | 2차원 세밀 |
| 8 | 2.7 | 10 | 2차원 세밀 |
| 9 | 3.0 | 9 | community optimal 인접 |
| 10 | 3.0 | 11 | community optimal 인접 |
| 11 | 3.0 | 12 | community optimal 인접 |
| 12 | 3.0 | 14 | community optimal (3.0/14) |
| 13 | 2.0 | 9 | 낮은 factor × 확장 period |
| 14 | 2.0 | 11 | 낮은 factor × 확장 period |
| 15 | 2.0 | 12 | 낮은 factor × 확장 period |
| 16 | 2.0 | 14 | 낮은 factor × 확장 period |
| 17 | 2.5 | 3 | 초단기 period |
| 18 | 2.5 | 6 | 단기 period |
| 19 | 2.8 | 7 | factor 세밀 ← **4h/1D 모두 best** |
| 20 | 2.8 | 10 | factor 세밀 |
| 21 | 3.2 | 7 | factor 세밀 |
| 22 | 3.2 | 10 | factor 세밀 |
| 23 | 1.8 | 7 | 매우 낮은 factor |
| 24 | 1.8 | 10 | 매우 낮은 factor |

### 2. TradeIQCciCeStrategy (cci_period × ce_mult) — 부분 실행

| # | cci_period | ce_mult | 비고 | 실행 여부 |
|---|------------|---------|------|----------|
| 1 | 16 | 3.0 | cci 세밀 | ✅ |
| 2 | 18 | 3.0 | cci 세밀 ← **4h/bidir best** | ✅ |
| 3 | 22 | 3.0 | cci 세밀 | ✅ |
| 4 | 24 | 3.0 | cci 세밀 | ✅ |
| 5 | 28 | 3.0 | cci 확장 | ✅ |
| 6 | 26 | 4.0 | ce_mult 확장 | ✅ |
| 7 | 26 | 4.5 | ce_mult 확장 | ✅ |
| 8 | 26 | 5.0 | ce_mult 와이드 | ✅ |
| 9 | 26 | 2.0 | ce_mult 타이트 | ✅ |
| 10 | 20 | 4.0 | | ✅ |
| 11 | 20 | 4.5 | | ✅ |
| 12 | 20 | 5.0 | | ✅ |
| 13 | 14 | 4.0 | | ✅ |
| 14~24 | — | — | 미실행 | ⬜ |

> 13 combo (4h/bidirectional만) 실행 후 중단. v2 champion(+17.27) 대비 v3 c2(+29.38)로 크게 개선.

### 3~7. 나머지 전략 — 미실행 (v2 champion 유지)

supertrend_trendtype, trendtype, tradeiq_psar_ha: 13 combo씩 부분 실행했으나 유효 조합 없음  
stoch, momentum_ma: 부분 실행, 여전히 전 조합 FAIL

---

## 실행 방법

```bash
# supertrend 전체 (96 jobs)
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v3.py \
  --strategies supertrend --workers 1

# 나머지 전략 (필요시)
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v3.py \
  --strategies supertrend_trendtype trendtype tradeiq_psar_ha --workers 2

docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v3.py \
  --strategies tradeiq_cci_ce stoch momentum_ma --workers 2

# v2+v3 통합 Champion run + 대시보드 재빌드
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v3.py --champion-run

python3 cryptoengine/services/jesse_engine/scripts/build_v4_dashboard.py
```

---

## 관련 문서

- [param_sweep_v2.md](param_sweep_v2.md) — v2 스윕 상세
- [rolling_window_results_v2.md](rolling_window_results_v2.md) — 롤링 윈도우 교차 검증
- [strategies/backtest/](strategies/backtest/) — 전략별 v2+v3 전체 파라미터 기록
- `cryptoengine/services/jesse_engine/scripts/param_sweep_v3.py` — 스윕 스크립트
