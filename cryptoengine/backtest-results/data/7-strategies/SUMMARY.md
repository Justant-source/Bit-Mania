# V4 종합 요약 (자동 생성)

**생성 시각**: 2026-05-10T07:39:53.356542+00:00
**생성 방식**: `v4_generate_report.py` (LLM 직접 작성 금지)

## Tier 분포

| Tier | 건수 | 기준 |
|------|------|------|
| A | 12 | Sharpe≥BnH×0.7, CAGR≥5%, MDD≥-30%, Trades≥30 |
| B | 15 | Sharpe≥0.3, CAGR≥0%, MDD≥-40% |
| C | 45 | B 미달 |
| FAILED | 0 | 백테스트 실패 |

## BnH 벤치마크 (TF별)

| TF | Sharpe | CAGR | $10,000→ |
|----|--------|------|----------|
| 1D | 0.000 | +16.54% | $22,596 |

## Top 10 — 최고 누적 잔고 (전략, 변형, TF) · x1 기준

| 순위 | 전략 | 변형 | TF | $10,000→ | Sharpe | MDD | Tier |
|------|------|-----|----|----------|--------|-----|------|
| 1 | supertrend | long_only | 4h | $50,110 | 1.006 | -19.4% | A |
| 2 | trendtype | bidirectional | 1D | $47,312 | 1.077 | -22.7% | A |
| 3 | stoch | bidirectional | 1h | $46,350 | 1.481 | -67.0% | C |
| 4 | supertrend | long_only | 1D | $44,462 | 1.345 | -21.2% | A |
| 5 | trendtype | bidirectional | 1h | $38,601 | 1.107 | -41.0% | C |
| 6 | supertrend_trendtype | long_only | 4h | $37,499 | 0.996 | -26.2% | A |
| 7 | trendtype | long_only | 1h | $33,931 | 1.492 | -36.3% | B |
| 8 | tradeiq_220320 | long_only | 1D | $32,576 | 0.906 | -21.2% | A |
| 9 | supertrend_trendtype | long_only | 1D | $32,398 | 1.532 | -25.5% | A |
| 10 | trendtype | long_only | 1D | $32,112 | 1.349 | -20.7% | A |

## Tier A 전략 목록 (12건)

| 전략 | 변형 | TF | CAGR | Sharpe | MDD | Trades | $10,000→ |
|------|-----|----|------|--------|-----|--------|----------|
| supertrend | long_only | 1D | +32.34% | 1.345 | -21.2% | 41 | $44,462 |
| supertrend | long_only | 4h | +35.34% | 1.006 | -19.4% | 245 | $50,110 |
| supertrend_trendtype | long_only | 1D | +24.70% | 1.532 | -25.5% | 33 | $32,398 |
| supertrend_trendtype | long_only | 4h | +28.17% | 0.996 | -26.2% | 192 | $37,499 |
| tradeiq_220320 | bidirectional | 1h | +15.37% | 0.882 | -27.8% | 43 | $21,412 |
| tradeiq_220320 | long_only | 1D | +24.83% | 0.906 | -21.2% | 87 | $32,576 |
| tradeiq_220320 | long_only | 2h | +14.09% | 0.661 | -28.2% | 851 | $20,173 |
| tradeiq_220320 | long_only | 4h | +15.44% | 0.684 | -22.9% | 421 | $21,477 |
| tradeiq_220323 | bidirectional | 4h | +15.10% | 0.725 | -27.8% | 108 | $21,143 |
| trendtype | bidirectional | 1D | +33.89% | 1.077 | -22.7% | 131 | $47,312 |
| trendtype | long_only | 1D | +24.49% | 1.349 | -20.7% | 61 | $32,112 |
| trendtype | long_only | 2h | +17.57% | 0.850 | -18.8% | 823 | $23,677 |

## Leveraged Top 10 (x2/x3) — 1x 대비 잔고 비교

| 순위 | 전략 | 변형 | TF | x1 $10,000→ | x2 $10,000→ | x3 $10,000→ |
|------|------|-----|----|------------|------------|------------|
| 1 | supertrend | long_only | 4h | $50,110 | $152,392 | $290,279 |
| 2 | trendtype | bidirectional | 1D | $47,312 | $114,994 | $147,774 |
| 3 | stoch | bidirectional | 1h | $46,350 | 💀$0 | 💀$0 |
| 4 | supertrend | long_only | 1D | $44,462 | $120,858 | $217,162 |
| 5 | trendtype | bidirectional | 1h | $38,601 | $68,537 | $15,431 |
| 6 | supertrend_trendtype | long_only | 4h | $37,499 | $94,449 | $164,052 |
| 7 | trendtype | long_only | 1h | $33,931 | $66,298 | $21,347 |
| 8 | tradeiq_220320 | long_only | 1D | $32,576 | $67,207 | $95,012 |
| 9 | supertrend_trendtype | long_only | 1D | $32,398 | $70,975 | $111,699 |
| 10 | trendtype | long_only | 1D | $32,112 | $71,564 | $113,398 |
