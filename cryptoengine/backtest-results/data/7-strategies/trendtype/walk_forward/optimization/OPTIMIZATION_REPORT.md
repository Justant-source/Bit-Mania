# TrendType skopt 최적화 보고서 (자동 생성)

**생성 시각**: 2026-05-09T08:59:06.757053+00:00
**생성 방식**: `wf_optimize_skopt.py` (LLM 직접 작성 금지)

## 설정
- IS (in-sample): 2020-01-01 → 2022-12-31
- OOS (out-of-sample): 2023-01-01 → 2025-12-31
- 2026 Q1 (paper): 2026-01-01 → 2026-03-31
- n_calls: 40 per variant
- 탐색 공간: atr_len∈[7,21], di_len∈[7,21], atr_mult∈[1.5,5.0]
- 고정 HP: atr_ma_len=20, smooth=1, adx_len=di_len
- 목적 함수: -(Sharpe × clamp(trades/30, 0, 1)) | MDD<-30% → 패널티

## 기준선 (Default HP, 전체기간)
| 항목 | 값 |
|------|-----|
| BnH CAGR | 46.86% |
| BnH Sharpe | 0.8399 |
| V3 bidi (default HP) CAGR | 38.19% |
| V3 bidi (default HP) Sharpe | 0.7758 |
| V3 long (default HP) CAGR | 39.19% |
| V3 long (default HP) Sharpe | 0.7888 |
| Default HP | {"atr_len": 14, "atr_ma_len": 20, "di_len": 14, "adx_len": 14, "smooth": 1, "atr_mult": 3.0} |

## BIDIRECTIONAL

**최적 HP**: `{"atr_len": 20, "atr_ma_len": 20, "di_len": 7, "adx_len": 7, "smooth": 1, "atr_mult": 4.973}`

### IS / OOS / Q1 성과
| 구간 | CAGR | Sharpe | MDD | 거래 |
|------|------|--------|-----|------|
| IS  2020-01-01→2022-12-31 | +66.36% | 1.061 | -19.51% | 30 |
| OOS 2023-01-01→2025-12-31 | +14.00% | 0.518 | -23.77% | 53 |
| Q1  2026-01-01→2026-03-31 | +78.09% | 1.405 | -1.86% | 2 |

### 평가
| 항목 | 결과 |
|------|------|
| OOS 통과 | ❌ NO |
| OOS Sharpe 열화 | +51.2% (IS→OOS) |
| Default HP 대비 IS Sharpe 개선 | +36.8% |

### IS 최고 성과 상위 5건
| atr_len | di_len | atr_mult | Sharpe | Trades | MDD | score |
|---------|--------|----------|--------|--------|-----|-------|
| 20 | 7 | 4.973 | 1.061 | 30 | -19.51% | -1.0614 |
| 21 | 20 | 1.872 | 1.058 | 35 | -7.02% | -1.0584 |
| 21 | 18 | 1.542 | 1.005 | 39 | -16.45% | -1.0046 |
| 16 | 16 | 1.525 | 0.995 | 35 | -25.68% | -0.9953 |
| 20 | 16 | 1.579 | 0.995 | 36 | -25.68% | -0.9952 |

## LONG_ONLY

**최적 HP**: `{"atr_len": 21, "atr_ma_len": 20, "di_len": 18, "adx_len": 18, "smooth": 1, "atr_mult": 1.5}`

### IS / OOS / Q1 성과
| 구간 | CAGR | Sharpe | MDD | 거래 |
|------|------|--------|-----|------|
| IS  2020-01-01→2022-12-31 | +71.92% | 1.018 | -4.04% | 28 |
| OOS 2023-01-01→2025-12-31 | +21.78% | 0.935 | -8.07% | 28 |
| Q1  2026-01-01→2026-03-31 | +0.00% | 0.000 | 0.00% | 0 |

### 평가
| 항목 | 결과 |
|------|------|
| OOS 통과 | ✅ YES |
| OOS Sharpe 열화 | +8.1% (IS→OOS) |
| Default HP 대비 IS Sharpe 개선 | +29.0% |

### IS 최고 성과 상위 5건
| atr_len | di_len | atr_mult | Sharpe | Trades | MDD | score |
|---------|--------|----------|--------|--------|-----|-------|
| 21 | 18 | 1.5 | 1.018 | 28 | -4.04% | -0.9497 |
| 21 | 16 | 1.5 | 1.013 | 27 | -10.56% | -0.9119 |
| 21 | 7 | 1.5 | 1.042 | 26 | -14.77% | -0.9032 |
| 15 | 18 | 1.532 | 1.019 | 25 | -4.11% | -0.8488 |
| 21 | 21 | 1.5 | 0.836 | 29 | -6.64% | -0.8077 |

## 종합 결론

**bidirectional**: ⚠️ OOS 미통과 — default HP 유지 권고 (열화 과다)
- 최적 HP: `{"atr_len": 20, "atr_ma_len": 20, "di_len": 7, "adx_len": 7, "smooth": 1, "atr_mult": 4.973}`
- 2026 Q1 paper: CAGR +78.09%, Sharpe 1.405, Trades 2

**long_only**: ✅ OOS 통과 — Phase 5 후보 적합
- 최적 HP: `{"atr_len": 21, "atr_ma_len": 20, "di_len": 18, "adx_len": 18, "smooth": 1, "atr_mult": 1.5}`
- 2026 Q1 paper: CAGR +0.00%, Sharpe 0.000, Trades 0

