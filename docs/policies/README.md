---
title: 정책 문서
category: policies
last_updated: 2026-06-14
---

# 정책

정책, 운영 매뉴얼, 전략 사양을 관리하는 카테고리. 

## 핵심 정책

| 문서 | 설명 |
|------|------|
| [btc-only.md](btc-only.md) | BTC 단일 운영 정책 (핵심) |
| [kill-switch.md](kill-switch.md) | Kill Switch 4단계 계층 |
| [emergency-manual-close.md](emergency-manual-close.md) | 비상 수동 청산 SOP |
| [leverage-limits.md](leverage-limits.md) | 레버리지 제한 정책 |
| [deployment-position.md](deployment-position.md) | 배포 시 포지션 보호 원칙 |

## 카테고리

### 운영 매뉴얼

자세한 운영 절차, 인시던트 대응, 모니터링 설정은 [operations/](operations/) 참조.

| 문서 | 설명 |
|------|------|
| [operations/README.md](operations/README.md) | 운영 매뉴얼 인덱스 |
| [operations/runbook.md](operations/runbook.md) | 시스템 운영 매뉴얼 |
| [operations/pubsub-channels.md](operations/pubsub-channels.md) | Redis Pub/Sub 채널 카탈로그 |
| [operations/deployment-procedure.md](operations/deployment-procedure.md) | Docker 배포 절차 |
| [operations/mainnet-switch.md](operations/mainnet-switch.md) | 메인넷 전환 절차 |
| [operations/monitoring.md](operations/monitoring.md) | 모니터링 및 알림 설정 |

### 거래 전략

거래 전략 사양 및 상세 규칙은 [strategies/](strategies/) 참조.

| 문서 | 설명 |
|------|------|
| [strategies/README.md](strategies/README.md) | 전략 인덱스 |
| [strategies/supertrend.md](strategies/supertrend.md) | Supertrend 4h Long-Only 3x 전략 (현재 운영) |

---

## 문서 업데이트 주기

- **정책 문서**: 정책/한도 변경 시 → 즉시 업데이트
- **운영 매뉴얼**: 절차 변경 시 → 즉시 업데이트
- **전략 사양**: 파라미터/백테스트 결과 변경 시 → 1주 내 업데이트

---

## 빠른 링크

- **긴급 상황**: [emergency-manual-close.md](emergency-manual-close.md) 참조
- **일상 운영**: [operations/runbook.md](operations/runbook.md) 참조
- **배포 전**: [deployment-position.md](deployment-position.md) + [operations/deployment-procedure.md](operations/deployment-procedure.md) 참조
- **메인넷 전환**: [operations/mainnet-switch.md](operations/mainnet-switch.md) 참조
