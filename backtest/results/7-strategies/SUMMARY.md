# V4 종합 요약 (자동 생성)

**생성 시각**: 2026-05-12T05:25:54.803472+00:00
**생성 방식**: `v4_generate_report.py` (LLM 직접 작성 금지)

## Tier 분포

| Tier | 건수 | 기준 |
|------|------|------|
| A | 1 | Sharpe≥BnH×0.7, CAGR≥5%, MDD≥-30%, Trades≥30 |
| B | 12 | Sharpe≥0.3, CAGR≥0%, MDD≥-40% |
| C | 29 | B 미달 |
| FAILED | 0 | 백테스트 실패 |

## BnH 벤치마크 (TF별)

| TF | Sharpe | CAGR | $10,000→ |
|----|--------|------|----------|
| 1D | 0.692 | +24.21% | $65,908 |

## Top 10 — 최고 누적 잔고 (전략, 변형, TF) · x1 기준

| 순위 | 전략 | 변형 | TF | $10,000→ | Sharpe | MDD | Tier |
|------|------|-----|----|----------|--------|-----|------|
| 1 | supertrend | long_only | 4h | $296,496 | 1.310 | -32.2% | B |
| 2 | stoch | long_only | 4h | 💀$0 | 0.960 | -67.5% | C |
| 3 | trendtype | bidirectional | 1D | $143,824 | 1.021 | -39.4% | B |
| 4 | supertrend | long_only | 1D | $133,754 | 1.143 | -30.4% | B |
| 5 | supertrend | bidirectional | 4h | $126,104 | 0.930 | -46.6% | C |
| 6 | tradeiq_220320 | long_only | 4h | $116,967 | 1.235 | -31.5% | B |
| 7 | supertrend_trendtype | long_only | 4h | $105,444 | 1.103 | -31.7% | B |
| 8 | supertrend_trendtype | long_only | 1D | $96,118 | 1.131 | -33.0% | B |
| 9 | trendtype | long_only | 1D | $72,611 | 1.099 | -27.1% | A |
| 10 | supertrend_trendtype | bidirectional | 4h | $72,227 | 0.855 | -47.3% | C |

## Tier A 전략 목록 (1건)

| 전략 | 변형 | TF | CAGR | Sharpe | MDD | Trades | $10,000→ |
|------|-----|----|------|--------|-----|--------|----------|
| trendtype | long_only | 1D | +28.55% | 1.099 | -27.1% | 88 | $72,611 |

## Leveraged Top 10 (x2/x3) — 1x 대비 잔고 비교

| 순위 | 전략 | 변형 | TF | x1 $10,000→ | x2 $10,000→ | x3 $10,000→ |
|------|------|-----|----|------------|------------|------------|
| 1 | supertrend | long_only | 4h | $296,496 | $141,777 | $259,516 |
| 2 | stoch | long_only | 4h | 💀$0 | N/A | N/A |
| 3 | trendtype | bidirectional | 1D | $143,824 | $110,292 | $137,448 |
| 4 | supertrend | long_only | 1D | $133,754 | $119,457 | $213,366 |
| 5 | supertrend | bidirectional | 4h | $126,104 | N/A | N/A |
| 6 | tradeiq_220320 | long_only | 4h | $116,967 | N/A | N/A |
| 7 | supertrend_trendtype | long_only | 4h | $105,444 | $88,092 | $146,436 |
| 8 | supertrend_trendtype | long_only | 1D | $96,118 | $70,100 | $109,353 |
| 9 | trendtype | long_only | 1D | $72,611 | $70,057 | $109,616 |
| 10 | supertrend_trendtype | bidirectional | 4h | $72,227 | N/A | N/A |
