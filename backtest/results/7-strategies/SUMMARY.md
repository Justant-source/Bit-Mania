# V4 종합 요약 (자동 생성)

**생성 시각**: 2026-05-18T01:07:07.594378+00:00
**생성 방식**: `v4_generate_report.py` (LLM 직접 작성 금지)

## Tier 분포

| Tier | 건수 | 기준 |
|------|------|------|
| A | 4 | Sharpe≥BnH×0.7, CAGR≥5%, MDD≥-30%, Trades≥30 |
| B | 9 | Sharpe≥0.3, CAGR≥0%, MDD≥-40% |
| C | 23 | B 미달 |
| FAILED | 0 | 백테스트 실패 |

## BnH 벤치마크 (TF별)

| TF | Sharpe | CAGR | $10,000→ |
|----|--------|------|----------|
| 1D | 0.692 | +24.21% | $65,908 |

## Top 10 — 최고 누적 잔고 (전략, 변형, TF) · x1 기준

| 순위 | 전략 | 변형 | TF | $10,000→ | Sharpe | MDD | Tier |
|------|------|-----|----|----------|--------|-----|------|
| 1 | supertrend | long_only | 4h | $193,169 | 1.246 | -30.4% | B |
| 2 | supertrend | long_only | 1D | $182,080 | 1.156 | -57.9% | C |
| 3 | supertrend_trendtype | bidirectional | 1D | $129,429 | 0.937 | -56.5% | C |
| 4 | supertrend | bidirectional | 1D | $114,335 | 0.856 | -53.0% | C |
| 5 | supertrend_trendtype | long_only | 1D | $71,929 | 0.964 | -39.3% | B |
| 6 | supertrend_trendtype | long_only | 4h | $66,926 | 0.997 | -30.4% | B |
| 7 | supertrend | bidirectional | 4h | $66,080 | 0.815 | -38.4% | B |
| 8 | stoch | bidirectional | 1D | $56,973 | 0.870 | -69.7% | C |
| 9 | trendtype | bidirectional | 1D | $32,563 | 0.784 | -33.2% | B |
| 10 | tradeiq_psar_ha | long_only | 1D | $29,374 | 0.864 | -26.1% | A |

## Tier A 전략 목록 (4건)

| 전략 | 변형 | TF | CAGR | Sharpe | MDD | Trades | $10,000→ |
|------|-----|----|------|--------|-----|--------|----------|
| tradeiq_psar_ha | long_only | 1D | +22.43% | 0.864 | -26.1% | 86 | $29,374 |
| tradeiq_psar_ha | long_only | 4h | +12.94% | 0.586 | -30.0% | 377 | $19,113 |
| trendtype | long_only | 1D | +21.54% | 0.919 | -27.5% | 85 | $28,261 |
| trendtype | long_only | 1h | +17.66% | 0.752 | -27.7% | 1380 | $23,771 |

## Leveraged Top 10 (x2/x3) — 1x 대비 잔고 비교

| 순위 | 전략 | 변형 | TF | x1 $10,000→ | x2 $10,000→ | x3 $10,000→ |
|------|------|-----|----|------------|------------|------------|
| 1 | supertrend | long_only | 4h | $193,169 | $1,928,863 | $6,037,295 |
| 2 | supertrend | long_only | 1D | $182,080 | $1,479,989 | $4,308,179 |
| 3 | supertrend_trendtype | bidirectional | 1D | $129,429 | N/A | N/A |
| 4 | supertrend | bidirectional | 1D | $114,335 | $234,793 | $19,434 |
| 5 | supertrend_trendtype | long_only | 1D | $71,929 | $370,170 | 💀$0 |
| 6 | supertrend_trendtype | long_only | 4h | $66,926 | $323,830 | $653,427 |
| 7 | supertrend | bidirectional | 4h | $66,080 | $40,294 | 💀$0 |
| 8 | stoch | bidirectional | 1D | $56,973 | N/A | N/A |
| 9 | trendtype | bidirectional | 1D | $32,563 | $110,292 | $137,448 |
| 10 | tradeiq_psar_ha | long_only | 1D | $29,374 | $65,334 | $90,988 |
