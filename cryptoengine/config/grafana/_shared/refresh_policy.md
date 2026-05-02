---
last_updated: 2026-05-02
---

# 새로고침 주기 정책

## 대시보드별 정책
| 대시보드 | 주기 | 기본 시간범위 | 사유 |
|---|---|---|---|
| Overview | 30s | 24h | 청산/Kill Switch 즉시 감지 |
| Performance & Risk | 1m | 7d | 시계열 위주, 빠른 갱신 불필요 |
| Strategies & Positions | 30s | 7d | 오픈 포지션 실시간성 |
| Operations | 30s | 6h | 장애 즉시 감지 |
| LLM Intelligence | 5m | 7d (정확도 30d) | LLM 호출 빈도와 정합 |
| Mobile Overview | 30s | 24h | 긴급 대응용 |

## 시간범위 변수
모든 대시보드에 `$time_range` 변수 추가:
- 옵션: `1h`, `6h`, `24h`, `7d`, `30d`, `90d`
- 기본값: 위 표의 기본 시간범위
