# V4 대시보드 표시 정확성 검증 리포트

**생성 시각**: 2026-05-10T23:13:33.207781+00:00
**스크립트**: `audit_v4_dashboard.py`

## 요약

| 카테고리 | 검증 건수 | 실패 | 경고 | 결과 |
|---------|----------|------|------|------|
| 2A_equity_len | 97 | 0 | 0 | ✅ PASS |
| 2A_finishing | 97 | 0 | 0 | ✅ PASS |
| 2A_leverage | 97 | 0 | 0 | ✅ PASS |
| 2A_trade_count | 97 | 0 | 0 | ✅ PASS |
| 2A_trade_field | 3080 | 0 | 0 | ✅ PASS |
| 2B_full_finishing | 97 | 0 | 0 | ✅ PASS |
| 2B_intentional_liq | 0 | 0 | 2 | ⚠️ WARN |
| 2C_bnh_1d_missing | 1 | 0 | 0 | ✅ PASS |
| 2C_bnh_slice | 4 | 0 | 0 | ✅ PASS |
| 2D_slice_count_mismatch | 10 | 0 | 0 | ✅ PASS |
| 2D_slice_pnl_mismatch | 10 | 0 | 0 | ✅ PASS |
| 2E_first_point_not_10k | 97 | 0 | 0 | ✅ PASS |
| 2E_last_point_vs_payload_finishing | 97 | 0 | 0 | ✅ PASS |
| 2F_marker_strict_filter | 62 | 0 | 0 | ✅ PASS |
| 2H_cache_clear_missing | 1 | 0 | 0 | ✅ PASS |

## ⚠️ 2B_intentional_liq — 2 warnings

- `label`=stoch/1h/bidirectional_x2, `payload_finishing`=10304.72, `payload_mdd`=-98.9012
- `label`=stoch/1h/bidirectional_x3, `payload_finishing`=791.73, `payload_mdd`=-99.9797

