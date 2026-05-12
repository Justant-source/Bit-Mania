---
title: param_sweep_v2 — TF별 최적 파라미터 발굴
category: test
related_code:
  - cryptoengine/services/jesse_engine/scripts/param_sweep_v2.py
  - cryptoengine/services/jesse_engine/scripts/param_sweep.py
last_updated: 2026-05-12
status: COMPLETED
when_to_update: v3 이후 추가 스윕 실행 시
---

# param_sweep_v2 — TF별 최적 파라미터 발굴

## 세션 완료 현황 (2026-05-11~12)

| 작업 | 상태 | 커밋 |
|------|------|------|
| param_sweep.py (4h/1D, 6 combos, 168 backtests) | ✅ 완료 | `432a88b` |
| 2h 타임프레임 전체 제거 (코드 + docs) | ✅ 완료 | `dc34438` |
| 7개 전략 MD 파일 생성 (v1 결과 기록) | ✅ 완료 | `432a88b` |
| param_sweep_v2.py 스크립트 작성 | ✅ 완료 | `fa636ed` |
| **v2 스윕 실행 (2,688 backtests)** | ✅ 완료 | `7b040c2` |
| v2 결과 → MD 파일 업데이트 | ✅ 완료 | `7b040c2` |
| Champion run + 대시보드 재빌드 | ✅ 완료 | `7b040c2` |
| param_sweep_v3.py 스크립트 작성 | ✅ 완료 | (본 커밋) |
| **v3 스윕 실행 (supertrend 96 + tradeiq_cci_ce 13 combo)** | ✅ 완료 | (본 커밋) |
| v2+v3 Champion run + 대시보드 재빌드 | ✅ 완료 | (본 커밋) |

---

## v2 스윕 개요

### 평가 기준 — 4-Period

| 기간 | 시작 | 종료 | 의미 |
|------|------|------|------|
| P1 | 2021-04-01 | 2026-04-30 | 고점 매수 → 현재 (최악 진입) |
| P2 | 2022-12-01 | 2026-04-30 | 저점 매수 → 현재 (최선 진입) |
| P3 | 2021-04-01 | 2025-09-30 | 고점 → 다음 고점 (전체 사이클) |
| P4 | 2022-12-01 | 2025-09-30 | 저점 → 고점 (Bull run) |

**점수식**: `mean(P1~P4 CAGR)`  if ALL periods `MDD ≥ -35%` AND `trades ≥ 5`  else `-999`

### 규모

```
7 strategies × 16 combos × 3 TF × 2 variants × 4 periods = 2,688 backtests
```

- 기존 6 combos (v1과 동일, idempotent SKIP) + 신규 10 combos
- TF: 1h / 4h / 1D
- Variants: bidirectional / long_only

### 출력

```
backtest-results/data/param_sweep_v2/{strat}/{tf}/{variant}/combo_{n}/
  p1/mini_stats.json   # CAGR, Sharpe, MDD, trades, WR, PF
  p2/mini_stats.json
  p3/mini_stats.json
  p4/mini_stats.json
  summary.json         # score, all-period stats, hp ← idempotency key
backtest-results/data/param_sweep_v2/param_sweep_v2_report.md
```

---

## v1 스윕 결과 요약 (param_sweep.py, 4h/1D, 6 combos)

| 전략 | 최적 파라미터 | TF | CAGR | Sharpe | MDD |
|------|-------------|-----|------|--------|-----|
| supertrend | st_factor=**2.0**, st_period=7 | 4h | +36.29% | 1.121 | -27.63% |
| tradeiq_psar_ha | rsi_len=**18**, atr_mult=3.0 | 1D | +29.94% | 1.080 | -27.34% |
| trendtype | atr_len=14, di_len=**18** | 1D | +25.83% | 1.040 | -24.83% |
| supertrend_trendtype | st_factor=**2.0**, atr_len=**10** | 4h | +33.44% | 1.121 | -29.05% |
| tradeiq_cci_ce | baseline 유지 | — | — | — | — |
| stoch | **전체 FAIL** (MDD -54~-89%) | — | — | — | — |
| momentum_ma | **전체 FAIL** (MDD -54~-89%) | — | — | — | — |

> 상세 내용: `docs/test/strategies/backtest/{strategy}.md`

---

## 실행 방법

### 사전 확인 (dry-run)

```bash
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py --dry-run
```

### 6-Container 병렬 실행 (overnight)

```bash
# Terminal 1 — stoch
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py \
  --strategies stoch --workers 1

# Terminal 2 — momentum_ma
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py \
  --strategies momentum_ma --workers 1

# Terminal 3 — supertrend
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py \
  --strategies supertrend --workers 1

# Terminal 4 — tradeiq_psar_ha
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py \
  --strategies tradeiq_psar_ha --workers 1

# Terminal 5 — trendtype
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py \
  --strategies trendtype --workers 1

# Terminal 6 — supertrend_trendtype + tradeiq_cci_ce (2 strategies)
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py \
  --strategies supertrend_trendtype tradeiq_cci_ce --workers 2
```

예상 소요 시간: **5~6시간** (8코어 기준, 각 backtest ~40초)

### 결과 확인

```bash
cat backtest-results/data/param_sweep_v2/param_sweep_v2_report.md
```

### Champion run + 대시보드 재빌드

```bash
# Docker 내부 — 최적 파라미터로 7-strategies 전체기간 재실행
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py --champion-run

# HOST — 대시보드 재빌드 (반드시 호스트에서 실행)
python3 cryptoengine/services/jesse_engine/scripts/build_v4_dashboard.py
```

---

## 전략별 16-Combo 파라미터 세트

| 전략 | param_A | param_B | v1 combos (1-6) | v2 신규 (7-16) |
|------|---------|---------|----------------|---------------|
| stoch | stoch_k_period | atr_mult | 10/14/18×3.0, 14×2.0/4.0, 10×2.0 | 18×2.0/4.0, 10×4.0, 7/21×{3,2,4}, 14×1.5 |
| momentum_ma | lin_len | atr_mult | 14/20/30×3.0, 20×2.0/4.0, 14×2.0 | 30×2.0/4.0, 14×4.0, 10/40×{3,2,4}, 25×2.5 |
| supertrend | st_factor | st_period | 2/3/4×7, 3×5/10, 2×5 | 4×5/10, 2×10, 1.5/5×{7,5,10}, 2.5×7 |
| tradeiq_psar_ha | rsi_len | atr_mult | 10/14/18×3.0, 14×2.0/4.0, 10×2.0 | 18×2.0/4.0, 10×4.0, 7/21×{3,2,4}, 14×1.5 |
| trendtype | atr_len | di_len | 10/14/18×14, 14×10/18, 10×10 | 10×18, 18×10, 18×18, 7/21×14, 14×7/21, 7×7, 21×21, 10×7 |
| supertrend_trendtype | st_factor | atr_len | 2/3/4×14, 3×10/18, 2×10 | 4×10/18, 2×18, 1.5/5×{14,10,18}, 2.5×12 |
| tradeiq_cci_ce | cci_period | ce_mult | 14/20/26×3.0, 20×2.5/3.5, 14×2.5 | 26×2.5/3.5, 14×3.5, 10/30×{3,2.5,3.5}, 20×2.0 |

---

## 스윕 완료 후 절차

```
1. 결과 리포트 확인
   cat backtest-results/data/param_sweep_v2/param_sweep_v2_report.md

2. 7개 전략 MD 파일 업데이트 (docs/test/strategies/backtest/)
   - TF별 최적 파라미터 테이블 추가
   - 16-combo 전체 결과 테이블 추가

3. Champion run (Docker)
   python /jesse-project/scripts/param_sweep_v2.py --champion-run

4. 대시보드 재빌드 (HOST)
   python3 scripts/build_v4_dashboard.py

5. 커밋
   git add .
   git commit -m "feat(7-strategies): v2 param sweep — TF별 최적 파라미터 발굴"
```

## v2 스윕 최종 결과 요약 (2026-05-11 실행)

### TF별 Champion (유효 조합만 — score > -999)

| 전략 | TF | Variant | Combo | 주요 파라미터 | Score | P1 CAGR | P1 MDD |
|------|----|---------|-------|------------|-------|---------|--------|
| supertrend | 4h | long_only | c16 | factor=2.5, period=7 | +38.39 | +27.3% | -27.4% |
| supertrend | 1D | long_only | c11 | factor=5.0, period=7 | +30.48 | +24.4% | -21.8% |
| supertrend | 1h | long_only | c11 | factor=5.0, period=7 | +10.66 | +4.6% | -33.1% |
| supertrend_trendtype | 4h | long_only | c6 | factor=2.0, atr_len=10 | +31.78 | +23.3% | -28.7% |
| supertrend_trendtype | 1D | long_only | c10 | factor=2.5, atr_len=14 | +27.64 | +19.8% | -26.2% |
| supertrend_trendtype | 1h | long_only | c11 | factor=5.0, atr_len=14 | +13.61 | +6.3% | -33.2% |
| trendtype | 4h | long_only | c6 | atr_len=10, di_len=10 | +29.33 | +21.0% | -22.2% |
| trendtype | 1D | long_only | c6 | atr_len=10, di_len=10 | +23.74 | +18.2% | -19.5% |
| tradeiq_psar_ha | 1D | long_only | c3 | rsi_len=18, atr_mult=3.0 | +22.12 | +20.7% | -24.9% |
| tradeiq_psar_ha | 4h | long_only | c9 | rsi_len=21, atr_mult=3.0 | +11.67 | +9.3% | -31.3% |
| tradeiq_cci_ce | 4h | bidir | c3 | cci_period=26, ce_mult=3.0 | +17.27 | +10.0% | -28.1% |
| stoch | — | — | — | 전 조합 FAIL (MDD > -35%) | -999 | — | — |
| momentum_ma | — | — | — | 전 조합 FAIL (MDD > -35%) | -999 | — | — |

> stoch: `use_direction_ema=False` 고정이 원인 → v3에서 `True`로 재탐색  
> momentum_ma: `val_ma_len=100` 고정이 원인 → v3에서 3rd param으로 추가 탐색

상세 내용: [strategies/backtest/](strategies/backtest/) 각 전략 MD 참조

---

## 관련 문서

- [param_sweep_v3.md](param_sweep_v3.md) — v3 추가 탐색 (champion 인근 세밀 격자 + stoch/momentum_ma 재탐색)
- [rolling_window_test.md](rolling_window_test.md) — 전략 생존 검증 방법론
- [strategies/backtest/](strategies/backtest/) — 전략별 파라미터 레퍼런스
- `cryptoengine/services/jesse_engine/scripts/param_sweep_v2.py` — 스윕 스크립트
