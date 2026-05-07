---
title: 운영 매뉴얼
category: policies/operations
last_updated: 2026-05-01
---

# 운영 매뉴얼

일상적인 시스템 운영, 인시던트 대응, 배포 절차, 모니터링 설정을 다루는 카테고리.

## 핵심 문서

| 문서 | 설명 |
|------|------|
| [runbook.md](runbook.md) | 시스템 운영 매뉴얼 (시작/중지/문제해결) |
| [pubsub-channels.md](pubsub-channels.md) | Redis Pub/Sub 채널 카탈로그 |
| [deployment-procedure.md](deployment-procedure.md) | Docker 배포 및 빌드 절차 |
| [mainnet-switch.md](mainnet-switch.md) | 메인넷 전환 절차 (Phase 5) |
| [monitoring.md](monitoring.md) | Grafana/Telegram 모니터링 설정 |

---

## 빠른 참조

### 일상 운영

- 매일 확인: [runbook.md#매일 확인 사항](runbook.md#매일-확인-사항)
- 주간 작업: [runbook.md#주간 확인 사항](runbook.md#주간-확인-사항)
- 월간 작업: [runbook.md#월간 확인 사항](runbook.md#월간-확인-사항)

### 문제 해결

- 서비스 다운: [runbook.md#서비스가 시작되지 않는 경우](runbook.md#서비스가-시작되지-않는-경우)
- 주문 체결 안 됨: [runbook.md#주문이 체결되지 않는 경우](runbook.md#주문이-체결되지-않는-경우)
- DB 연결 실패: [runbook.md#데이터베이스 연결 실패](runbook.md#데이터베이스-연결-실패)

### 배포 전

1. [deployment-procedure.md](deployment-procedure.md) — 배포 절차 확인
2. [../deployment-position.md](../deployment-position.md) — 포지션 보호 원칙 확인

### 메인넷 전환

1. [mainnet-switch.md](mainnet-switch.md) — 메인넷 전환 절차 (9단계)
2. [monitoring.md](monitoring.md) — 강화 모니터링 설정

### 서비스 간 통신

[pubsub-channels.md](pubsub-channels.md) — Redis 채널, 메시지 포맷, REST API

---

## 관련 정책 문서

- [../kill-switch.md](../kill-switch.md) — Kill Switch 정책
- [../emergency-manual-close.md](../emergency-manual-close.md) — 비상 청산 SOP
- [../deployment-position.md](../deployment-position.md) — 배포 시 포지션 보호
