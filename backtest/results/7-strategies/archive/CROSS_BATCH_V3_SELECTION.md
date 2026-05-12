# V3 Cross-Batch Selection Report (자동 생성)

**생성 시각**: 2026-05-09T07:29:57.935494+00:00
**생성 방식**: `v3_generate_report.py` (LLM 직접 작성 금지)
**입력 검증**: `v3_verify_markers.py` 통과 후에만 생성됨

## BnH 벤치마크 (실측)
- CAGR: **46.86%**
- Sharpe: **0.8399**
- MDD: **-70.46%**
- Tier A 기준 Sharpe: **≥ 0.5879** (BnH × 70%)

## 18개 결과 종합

| Batch | 전략 | Variant | Status | CAGR | Sharpe | MDD | Trades | WR | PF | Tier |
|-------|------|---------|--------|------|--------|-----|--------|-----|-----|------|
| batch_1 | bbpb | bidirectional | SUCCESS | -14.32% | -0.218 | -77.05% | 35 | 28.6% | 0.65 | **C** |
| batch_1 | bbpb | long_only | SUCCESS | 3.54% | 0.258 | -53.80% | 18 | 33.3% | 1.16 | **C** |
| batch_1 | bbwp | bidirectional | SUCCESS | -21.92% | -0.336 | -89.64% | 17 | 41.2% | 0.62 | **C** |
| batch_1 | bbwp | long_only | SUCCESS | -1.74% | 0.087 | -60.53% | 10 | 60.0% | 0.90 | **C** |
| batch_1 | stoch | bidirectional | SUCCESS | 58.45% | 0.925 | -59.16% | 29 | 79.3% | 2.88 | **C** |
| batch_1 | stoch | long_only | SUCCESS | 46.89% | 0.839 | -72.66% | 26 | 73.1% | 1.91 | **C** |
| batch_2 | momentum_ma | bidirectional | SUCCESS | 25.26% | 0.605 | -63.85% | 61 | 47.5% | 1.13 | **C** |
| batch_2 | momentum_ma | long_only | SUCCESS | 58.55% | 0.944 | -64.40% | 32 | 62.5% | 1.71 | **C** |
| batch_2 | supertrend | bidirectional | SUCCESS | 8.07% | 0.353 | -50.32% | 25 | 48.0% | 1.39 | **C** |
| batch_2 | supertrend | long_only | SUCCESS | 13.04% | 0.482 | -26.36% | 17 | 47.1% | 2.10 | **C** |
| batch_2 | tradeiq_psar_ha | bidirectional | SUCCESS | -4.83% | 0.035 | -65.37% | 47 | 31.9% | 0.84 | **C** |
| batch_2 | tradeiq_psar_ha | long_only | SUCCESS | 7.15% | 0.349 | -30.41% | 32 | 40.6% | 1.37 | **C** |
| batch_3 | trendtype | bidirectional | SUCCESS | 38.19% | 0.776 | -24.60% | 61 | 50.8% | 1.89 | **A** |
| batch_3 | trendtype | long_only | SUCCESS | 39.19% | 0.789 | -20.33% | 33 | 60.6% | 2.67 | **A** |
| batch_3 | supertrend_trendtype | bidirectional | SUCCESS | 8.90% | 0.382 | -35.91% | 17 | 58.8% | 1.75 | **C** |
| batch_3 | supertrend_trendtype | long_only | SUCCESS | 10.71% | 0.437 | -22.98% | 12 | 66.7% | 2.54 | **C** |
| batch_3 | tradeiq_cci_ce | bidirectional | SUCCESS | -1.66% | 0.130 | -55.62% | 10 | 60.0% | 0.92 | **C** |
| batch_3 | tradeiq_cci_ce | long_only | SUCCESS | 0.84% | 0.185 | -39.82% | 8 | 62.5% | 1.05 | **C** |

## Tier 분포
- Tier A: **2**
- Tier B: **0**
- Tier C: **16**
- Failed: **0**

## 종합
⭐ **2개 전략이 Tier A 도달** — walk-forward 진행 권고
