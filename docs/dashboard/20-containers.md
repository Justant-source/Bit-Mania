---
title: L2 Containers — Dashboard 스택
last_updated: 2026-08-29
---

# L2 Containers — Dashboard 스택

## 4. Dashboard 스택

<!-- last-verified: 2026-08-29 -->
<!-- code-ref: /dashboard/docker-compose.yml -->

```mermaid
flowchart TB
  subgraph dash_stack["Dashboard — bitmania-dashboard"]
    dashboard_svc["<b>dashboard</b><br/>Express<br/>:3000<br/>CPU: 0.25 · Memory: 128M"]
  end

  ce_infra["CryptoEngine Infrastructure<br/>(별도 compose)"]
  ce_pg["postgres :5432<br/>cryptoengine DB"]
  ce_redis["redis :6379<br/>Pub/Sub channels"]
  ce_prometheus["prometheus :9090<br/>metrics"]

  dashboard_svc -->|"읽기 트래픽"| ce_pg
  dashboard_svc -->|"구독: 실시간 신호"| ce_redis
  dashboard_svc -.->|"선택 메트릭"| ce_prometheus

  client["User Browser<br/>http://0.0.0.0:3000"]
  client -->|"HTTP REST"| dashboard_svc
  client -->|"WebSocket"| dashboard_svc
```

### 구조 설명

**네트워크**:
- `cryptoengine_default` (external)에 연결
- 운영 postgres, redis, prometheus 접근 가능

**역할**:
- 실시간 포지션, 자산, 신호 시각화
- Telegram 이외 웹 기반 모니터링 인터페이스

**포트**:
- 호스트 바인딩: `0.0.0.0:3000:3000` (운영 네트워크에서 접근)

---

## 5. 포트 맵
| 포트 | 서비스 | 호스트 바인딩 | 용도 | 접근성 |
|------|--------|-------------|------|--------|
| **3000** | dashboard (bitmania-dashboard, §9) | 0.0.0.0:3000 | 실시간 매매 대시보드 | 호스트 접근 가능 |

## 11. 상태 게이트 (Health Checks)
| 서비스 | 검사 방식 | 시작 대기 | Retries |
|--------|---------|---------|---------|
| dashboard | wget /health | 15s | 3 |
5. telegram-bot, dashboard (인터페이스)

## 참고 문서

- `docs/shared/20-containers.md` — 네트워크 경계
- `docs/dashboard/50-api.md` — Dashboard REST
