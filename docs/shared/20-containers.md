---
title: L2 Containers — 세 서브시스템 토폴로지 · 네트워크 경계
last_updated: 2026-08-29
---

# L2 Containers — 세 서브시스템 관계 · 네트워크 경계

운영(`cryptoengine/`), 백테스트(`backtest/`), 대시보드(`dashboard/`)는 **서로 다른 Compose 파일**로 기동한다. 서비스표·포트·볼륨·환경변수는 각 대분류 `20-containers.md`에 있다.

| 서브시스템 | Compose | 네트워크 | 역할 |
|---|---|---|---|
| CryptoEngine | `cryptoengine/docker-compose.yml` | `cryptoengine_default` (bridge) | 메인넷 실거래 |
| Backtest | `backtest/docker/docker-compose.yml` | `cryptoengine-backtest-net` + (optional) external `cryptoengine_default` | 파라미터 R&D |
| Dashboard | `dashboard/docker-compose.yml` | external `cryptoengine_default` | 운영 PG/Redis 읽기 관측 |

Backtest → CryptoEngine 반영은 자동화 없이 `cryptoengine/config/strategies/supertrend.yaml` 수동 PR이다.

---

## 네트워크 토폴로지

### Production (`cryptoengine_default`)

```
┌─ cryptoengine/docker-compose.yml
│  └─ networks:
│     └─ cryptoengine_default (bridge, implicit)
│        ├─ postgres :5432
│        ├─ redis :6379
│        ├─ prometheus :9090
│        ├─ market-data
│        ├─ strategy-orchestrator
│        ├─ execution-engine
│        ├─ supertrend
│        ├─ telegram-bot
│        ├─ pg-backup
│        ├─ log-retention
│        ├─ ohlcv-retention
│        ├─ node-exporter
│        └─ redis-exporter
```

### Backtest (`cryptoengine-backtest-net` + external `cryptoengine_default`)

```
┌─ backtest/docker/docker-compose.yml
│  └─ networks:
│     ├─ cryptoengine-backtest-net (isolated)
│     │  ├─ backtest-postgres :5433
│     │  └─ backtester
│     └─ cryptoengine_default (external, optional)
│        └─ 운영 postgres에 read-only 접근 가능
```

### Service-to-Service Communication

**Production:**
- 모든 서비스 → postgres: TCP 5432 (heartbeat, schema)
- 모든 서비스 → redis: TCP 6379 (Pub/Sub)
- prometheus → node-exporter: TCP 9100 (scrape)
- prometheus → redis-exporter: TCP 9121 (scrape)

**External:**
- market-data, execution-engine → Bybit: HTTPS (API), WSS (WebSocket)
- telegram-bot → Telegram: HTTPS (Bot API)
- dashboard → 운영 postgres/redis (읽기)

---

## 참고 문서

- `docs/cryptoengine/20-containers.md` — 운영 서비스표 · 포트 · 볼륨 · env
- `docs/backtest/20-containers.md` — 백테스트 Compose
- `docs/dashboard/20-containers.md` — 대시보드 Compose
- `docs/shared/10-context.md` — L1 외부 액터 · 서브시스템 경계
- `docs/shared/70-policy.md` — 운영 Runbook
