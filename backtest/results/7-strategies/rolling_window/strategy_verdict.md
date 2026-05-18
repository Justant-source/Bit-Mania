# Rolling Window 분석 결과

- 데이터 범위: 2021-01 ~ 2026-04
- Window sizes: [3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57, 60, 63] 개월 (월별 시프트)
- 대상 조합: 42 개
- 실패 기준: `fail_strict` = 구간 누적 PnL ≤ $0  |  `fail_meaningful` = < $-100

## Tier 1 — Strategy 종합 판정

| Strategy | 판정 | 조합 수 | 양수 구간 有 | 양수 구간 無 | avg fail% | 최악 조합 | 최선 조합 |
|---|---|---|---|---|---|---|---|
| `stoch` | ✅ KEEP | 6 | 6 | 0 | 28.9% | bidirectional/1D | bidirectional/1h |
| `supertrend` | ✅ KEEP | 6 | 6 | 0 | 22.8% | bidirectional/1h | long_only/4h |
| `tradeiq_psar_ha` | ✅ KEEP | 6 | 6 | 0 | 44.7% | bidirectional/1h | long_only/1D |
| `trendtype` | ✅ KEEP | 6 | 6 | 0 | 23.2% | bidirectional/1h | long_only/1D |
| `supertrend_trendtype` | ✅ KEEP | 6 | 6 | 0 | 24.0% | bidirectional/1h | long_only/4h |
| `tradeiq_cci_ce` | ✅ KEEP | 6 | 6 | 0 | 32.3% | bidirectional/1D | bidirectional/4h |

## Tier 2 — (variant, TF) 조합별 "양수 구간 존재" 매트릭스

`✓` = 어느 구간에서든 최소 1회 양수 수익  `✗` = 모든 구간에서 손실 또는 0

| Strategy | bidirectional/1D | bidirectional/1h | bidirectional/4h | long_only/1D | long_only/1h | long_only/4h |
|---|---|---|---|---|---|---|
| `stoch` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `supertrend` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `supertrend_trendtype` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `tradeiq_psar_ha` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `tradeiq_cci_ce` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `trendtype` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

## Tier 3 — Fail 발생 조합 상세

| Strategy | Variant | TF | Fail% | Meaningful fail% | 최악 구간 | 최악 PnL | 최선 PnL | 전체기간 PnL |
|---|---|---|---|---|---|---|---|---|
| `stoch` | bidirectional | 1D | 38.5% | 36.4% | 2024-04(6m) | $-6,011 | $24,670 | $6,221 |
| `stoch` | long_only | 1h | 31.2% | 22.5% | 2021-10(15m) | $-6,723 | $49,858 | $7,635 |
| `stoch` | bidirectional | 4h | 30.7% | 30.3% | 2024-04(24m) | $-5,694 | $23,117 | $3,361 |
| `stoch` | long_only | 1D | 28.1% | 26.0% | 2021-04(21m) | $-7,334 | $46,967 | $8,943 |
| `stoch` | long_only | 4h | 25.1% | 24.7% | 2021-04(21m) | $-6,250 | $46,426 | $13,172 |
| `stoch` | bidirectional | 1h | 19.5% | 11.3% | 2021-01(15m) | $-5,169 | $85,740 | $36,250 |
| `supertrend` | bidirectional | 1h | 50.6% | 50.2% | 2025-01(12m) | $-4,553 | $8,511 | $-957 |
| `supertrend` | bidirectional | 1D | 27.3% | 26.8% | 2021-04(27m) | $-4,213 | $13,730 | $8,988 |
| `supertrend` | long_only | 1h | 18.6% | 16.9% | 2025-01(15m) | $-2,306 | $12,540 | $6,985 |
| `supertrend` | long_only | 1D | 16.0% | 11.7% | 2022-01(15m) | $-1,397 | $40,319 | $36,646 |
| `supertrend` | bidirectional | 4h | 13.9% | 13.4% | 2025-01(9m) | $-3,532 | $30,171 | $17,343 |
| `supertrend` | long_only | 4h | 10.4% | 10.0% | 2022-04(9m) | $-1,683 | $53,430 | $46,152 |
| `supertrend_trendtype` | bidirectional | 1h | 54.5% | 52.4% | 2023-04(33m) | $-4,844 | $5,021 | $-2,699 |
| `supertrend_trendtype` | bidirectional | 1D | 26.4% | 26.0% | 2022-01(3m) | $-3,275 | $12,324 | $7,343 |
| `supertrend_trendtype` | bidirectional | 4h | 23.4% | 22.9% | 2025-01(9m) | $-2,762 | $24,072 | $16,694 |
| `supertrend_trendtype` | long_only | 1D | 15.6% | 9.5% | 2025-01(12m) | $-2,188 | $36,426 | $26,269 |
| `supertrend_trendtype` | long_only | 1h | 14.7% | 14.3% | 2025-01(12m) | $-2,440 | $16,469 | $10,703 |
| `supertrend_trendtype` | long_only | 4h | 9.1% | 8.7% | 2025-01(3m) | $-1,871 | $44,265 | $36,478 |
| `tradeiq_psar_ha` | bidirectional | 1h | 93.1% | 92.2% | 2021-10(54m) | $-7,363 | $2,775 | $-7,129 |
| `tradeiq_psar_ha` | bidirectional | 4h | 55.8% | 55.0% | 2022-07(42m) | $-4,210 | $6,858 | $-616 |
| `tradeiq_psar_ha` | long_only | 1h | 50.2% | 49.4% | 2021-10(12m) | $-4,426 | $7,434 | $-997 |
| `tradeiq_psar_ha` | bidirectional | 1D | 29.4% | 27.7% | 2021-04(30m) | $-3,897 | $9,130 | $9,267 |
| `tradeiq_psar_ha` | long_only | 4h | 20.8% | 18.2% | 2022-04(18m) | $-2,594 | $10,907 | $7,965 |
| `tradeiq_psar_ha` | long_only | 1D | 19.1% | 12.1% | 2024-04(6m) | $-1,889 | $30,993 | $30,452 |
| `tradeiq_cci_ce` | bidirectional | 1D | 88.3% | 84.4% | 2021-01(54m) | $-6,375 | $2,925 | $-5,670 |
| `tradeiq_cci_ce` | long_only | 1D | 39.8% | 19.5% | 2021-01(6m) | $-3,169 | $4,037 | $-1,175 |
| `tradeiq_cci_ce` | long_only | 1h | 23.4% | 21.6% | 2025-01(15m) | $-2,028 | $7,226 | $4,054 |
| `tradeiq_cci_ce` | long_only | 4h | 17.8% | 16.5% | 2022-01(9m) | $-2,629 | $10,991 | $7,642 |
| `tradeiq_cci_ce` | bidirectional | 1h | 14.3% | 12.6% | 2022-01(3m) | $-1,611 | $8,003 | $4,931 |
| `tradeiq_cci_ce` | bidirectional | 4h | 10.4% | 9.5% | 2025-07(6m) | $-1,579 | $15,026 | $10,714 |
| `trendtype` | bidirectional | 1h | 41.1% | 41.1% | 2024-10(12m) | $-6,030 | $19,521 | $1,349 |
| `trendtype` | bidirectional | 4h | 40.7% | 39.4% | 2023-07(33m) | $-6,012 | $15,602 | $-42 |
| `trendtype` | long_only | 4h | 20.8% | 19.5% | 2025-01(15m) | $-2,973 | $21,421 | $11,896 |
| `trendtype` | long_only | 1h | 13.0% | 11.3% | 2023-04(6m) | $-1,444 | $14,671 | $13,771 |
| `trendtype` | bidirectional | 1D | 12.1% | 9.5% | 2025-01(9m) | $-1,940 | $24,897 | $21,259 |
| `trendtype` | long_only | 1D | 11.7% | 10.8% | 2025-07(9m) | $-1,616 | $25,780 | $20,000 |
