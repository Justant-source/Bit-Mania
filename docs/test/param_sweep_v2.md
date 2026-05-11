---
title: param_sweep_v2 — TF별 최적 파라미터 발굴
category: test
related_code:
  - cryptoengine/services/jesse_engine/scripts/param_sweep_v2.py
  - cryptoengine/services/jesse_engine/scripts/param_sweep.py
last_updated: 2026-05-11
status: SCRIPT_READY / SWEEP_PENDING
when_to_update: 스윕 실행 완료 후 status + 결과 섹션 업데이트
---

# param_sweep_v2 — TF별 최적 파라미터 발굴

## 세션 완료 현황 (2026-05-11)

| 작업 | 상태 | 커밋 |
|------|------|------|
| param_sweep.py (4h/1D, 6 combos, 168 backtests) | ✅ 완료 | `432a88b` |
| 2h 타임프레임 전체 제거 (코드 + docs) | ✅ 완료 | `dc34438` |
| 7개 전략 MD 파일 생성 (v1 결과 기록) | ✅ 완료 | `432a88b` |
| param_sweep_v2.py 스크립트 작성 | ✅ 완료 | (미커밋) |
| **v2 스윕 실행 (2,688 backtests)** | ⏳ 미실행 | — |
| v2 결과 → MD 파일 업데이트 | ⏳ 스윕 후 | — |
| Champion run + 대시보드 재빌드 | ⏳ 스윕 후 | — |

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
| tradeiq_220320 | rsi_len=**18**, atr_mult=3.0 | 1D | +29.94% | 1.080 | -27.34% |
| trendtype | atr_len=14, di_len=**18** | 1D | +25.83% | 1.040 | -24.83% |
| supertrend_trendtype | st_factor=**2.0**, atr_len=**10** | 4h | +33.44% | 1.121 | -29.05% |
| tradeiq_220323 | baseline 유지 | — | — | — | — |
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

# Terminal 4 — tradeiq_220320
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py \
  --strategies tradeiq_220320 --workers 1

# Terminal 5 — trendtype
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py \
  --strategies trendtype --workers 1

# Terminal 6 — supertrend_trendtype + tradeiq_220323 (2 strategies)
docker compose --profile backtest run --rm jesse_engine \
  python /jesse-project/scripts/param_sweep_v2.py \
  --strategies supertrend_trendtype tradeiq_220323 --workers 2
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
| tradeiq_220320 | rsi_len | atr_mult | 10/14/18×3.0, 14×2.0/4.0, 10×2.0 | 18×2.0/4.0, 10×4.0, 7/21×{3,2,4}, 14×1.5 |
| trendtype | atr_len | di_len | 10/14/18×14, 14×10/18, 10×10 | 10×18, 18×10, 18×18, 7/21×14, 14×7/21, 7×7, 21×21, 10×7 |
| supertrend_trendtype | st_factor | atr_len | 2/3/4×14, 3×10/18, 2×10 | 4×10/18, 2×18, 1.5/5×{14,10,18}, 2.5×12 |
| tradeiq_220323 | cci_period | ce_mult | 14/20/26×3.0, 20×2.5/3.5, 14×2.5 | 26×2.5/3.5, 14×3.5, 10/30×{3,2.5,3.5}, 20×2.0 |

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

## 관련 문서

- [rolling_window_test.md](rolling_window_test.md) — 전략 생존 검증 방법론
- [strategies/backtest/](strategies/backtest/) — 전략별 파라미터 레퍼런스
- `cryptoengine/services/jesse_engine/scripts/param_sweep_v2.py` — 스윕 스크립트
