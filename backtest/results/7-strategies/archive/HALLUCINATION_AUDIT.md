# V3 환각 점검표 (자동 생성)

**점검 시각**: 2026-05-09T07:30:01.547862+00:00
**생성 방식**: `v3_audit.py` (LLM 직접 작성 금지)

## 모든 백테스트 산출물 검증

| 경로 | stats.json | SUCCESS marker | SHA256 | 크기 | mtime (UTC) |
|------|-----------|---------------|--------|------|------------|
| v3/buy_and_hold | ✓ | ✓ | ✓ | 2264B | 07:25:38 |
| v3/batch_1/bbpb/bidirectional | ✓ | ✓ | ✓ | 2428B | 07:27:00 |
| v3/batch_1/bbpb/long_only | ✓ | ✓ | ✓ | 2366B | 07:27:05 |
| v3/batch_1/bbwp/bidirectional | ✓ | ✓ | ✓ | 2415B | 07:27:10 |
| v3/batch_1/bbwp/long_only | ✓ | ✓ | ✓ | 2331B | 07:27:14 |
| v3/batch_1/stoch/bidirectional | ✓ | ✓ | ✓ | 2408B | 07:27:19 |
| v3/batch_1/stoch/long_only | ✓ | ✓ | ✓ | 2377B | 07:27:24 |
| v3/batch_2/supertrend/bidirectional | ✓ | ✓ | ✓ | 2358B | 07:27:29 |
| v3/batch_2/supertrend/long_only | ✓ | ✓ | ✓ | 2356B | 07:27:37 |
| v3/batch_2/tradeiq_psar_ha/bidirectional | ✓ | ✓ | ✓ | 2411B | 07:27:46 |
| v3/batch_2/tradeiq_psar_ha/long_only | ✓ | ✓ | ✓ | 2346B | 07:27:55 |
| v3/batch_3/trendtype/bidirectional | ✓ | ✓ | ✓ | 2414B | 07:27:35 |
| v3/batch_3/trendtype/long_only | ✓ | ✓ | ✓ | 2358B | 07:27:42 |
| v3/batch_3/supertrend_trendtype/bidirectional | ✓ | ✓ | ✓ | 2414B | 07:27:52 |
| v3/batch_3/supertrend_trendtype/long_only | ✓ | ✓ | ✓ | 2355B | 07:28:00 |
| v3/batch_3/tradeiq_cci_ce/bidirectional | ✓ | ✓ | ✓ | 2331B | 07:28:09 |
| v3/batch_3/tradeiq_cci_ce/long_only | ✓ | ✓ | ✓ | 2323B | 07:28:17 |

## 점검 통계
- 총 산출물 경로: 19 (예상: 19 = 18 variants + 1 BnH)
- SHA256 검증 통과: **19**
- 검증 실패/미완료: **0**

## 환각 의심 점검
- mtime 분산: 159.6초 (모든 stats.json mtime의 max−min)
  ✓ 정상 (각 백테스트가 다른 시각에 완료됨)
- CROSS_BATCH_V3_SELECTION.md: 자동 생성 ✓
