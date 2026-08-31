---
title: L1 System Context — CryptoEngine 모노레포
last_updated: 2026-08-29
---

# L1 System Context

<!-- last-verified: 2026-08-29 -->
<!-- code-ref: cryptoengine/services/market-data/collector.py:37-38, cryptoengine/services/telegram-bot/main.py:20, backtest/docker/docker-compose.yml, dashboard/docker-compose.yml -->

```mermaid
flowchart TB
    subgraph actors["외부 액터"]
        operator["👤 운영자<br/>Telegram /kill · /positions"]
        bybit_mainnet["☁ Bybit Mainnet<br/>REST + WebSocket"]
        bybit_testnet["☁ Bybit Testnet<br/>개발·검증 전용"]
        telegram_api["☁ Telegram Bot API<br/>알림 + 명령 채널"]
        coinglass["☁ CoinGlass API<br/>펀딩 비교 (키 있을 때만)"]
        grafana_am["☁ Grafana AlertManager<br/>ce:alerts:grafana webhook"]
    end

    subgraph repo["CryptoEngine 모노레포"]
        subgraph cryptoengine["CryptoEngine (운영)<br/>Supertrend 4h Long-Only 3x · BTC/USDT · Phase 5 메인넷 · Bybit 단독"]
            ce_core["Core Services<br/>market-data · orchestrator · execution-engine"]
            ce_strat["supertrend<br/>strategy-01"]
            ce_bot["telegram-bot"]
        end
        subgraph backtest["Backtest R&D<br/>Jesse 2.1.2 · profiles:backtest · port 5433"]
            bt_runner["backtester"]
        end
        subgraph dashboard["Dashboard (관측)<br/>Vite + Express · port 3000/3001"]
            dash_web["supertrend-dashboard<br/>monitor-dashboard"]
        end
    end

    operator -->|"Telegram 명령"| telegram_api
    telegram_api -->|"Bot API"| ce_bot
    bybit_mainnet -->|"WebSocket OHLCV<br/>REST 주문 실행"| ce_core
    bybit_testnet -.->|"BYBIT_TESTNET=true 시만"| ce_core
    coinglass -.->|"COINGLASS_API_KEY 있을 때만<br/>펀딩 비교 REST"| ce_core
    grafana_am -->|"Alert webhook"| ce_bot
    ce_core -->|"주문 REST"| bybit_mainnet
    ce_core -->|"알림"| telegram_api

    backtest -.->|"파라미터 산출 (수동 PR)"| cryptoengine
    dashboard -->|"PostgreSQL · Redis 읽기"| cryptoengine
```

> **Track-C 폐지 · 2026-08-29 운영 창**: Binance·OKX 수집기는 삭제. 같은 날 분기물 파이프라인·레거시 테이블 DROP·자격증명 fail-closed·git 히스토리 재작성은 [ADR-0010](90-adr/0010-ops-cleanup-20260829.md). Bybit 단독 + Supertrend 4h.

---

## 외부 시스템 요약

| 시스템 | 방향 | 프로토콜 | 비고 |
|---|---|---|---|
| Bybit Mainnet | 양방향 | WebSocket + REST | `BYBIT_TESTNET=false` (Phase 5) |
| Bybit Testnet | 양방향 | WebSocket + REST | `BYBIT_TESTNET=true` 시만 사용 |
| Telegram Bot API | 양방향 | HTTPS Long-poll | 알림 발송 + /kill · /positions 수신 |
| CoinGlass API | 인바운드 (선택) | REST | `COINGLASS_API_KEY` 없으면 비교 폴링 비활성. Track-C 삭제와 별개 |
| Grafana AlertManager | 인바운드 | HTTP webhook | `ce:alerts:grafana` 채널 경유 |

---

## 서브시스템 경계

| 서브시스템 | 역할 | 격리 수준 | 정보 흐름 |
|---|---|---|---|
| **CryptoEngine** (운영) | Phase 5 메인넷 BTC 선물 자동매매 | 운영 PG(5432) + Redis(6379) | Bybit ↔ 전략 ↔ Telegram |
| **Backtest** (R&D) | 전략 파라미터 검증 · Walk-Forward | 별도 PG(5433, jesse_db) · 별도 compose | 운영 data read-only 마운트(../../data:/data:ro) → 파라미터 수동 PR |
| **Dashboard** (관측) | 실시간 차트 · 모니터링 UI | 별도 compose · Node/Express | 운영 PG + Redis 읽기 전용 |

> Backtest → CryptoEngine 인터페이스는 `cryptoengine/config/strategies/supertrend.yaml` 파라미터 업데이트 PR (자동화 없음).
