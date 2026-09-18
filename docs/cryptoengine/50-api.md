---
title: L5 API — Redis Pub/Sub 채널 카탈로그
last_updated: 2026-09-19
---

# L5 API — Redis Pub/Sub 채널 카탈로그

> CryptoEngine 마이크로서비스 간 비동기 통신 규약 .

---

## §1. 채널 카탈로그 (Diagram J)

★ **중요**: 기존 문서의 `order:update`는 **코드 드리프트**입니다.
코드 실제 채널: `order:result` + `order:result:{strategy_id}` (execution/engine.py:257,262,312,314)

<!-- last-verified: 2026-09-19 -->
<!-- code-ref: cryptoengine/shared/kill_switch.py:32-35, cryptoengine/services/execution/engine.py, cryptoengine/services/orchestrator/core.py, cryptoengine/services/market-data/funding_monitor.py, cryptoengine/services/strategies/supertrend/strategy.py, cryptoengine/services/telegram-bot/main.py:72-80 -->

```mermaid
sequenceDiagram
    participant MD as market-data
    participant ST as supertrend
    participant ORCH as strategy-orchestrator
    participant ENG as execution-engine
    participant TG as telegram-bot

    Note over MD,ENG: 시장 데이터 수집 → 전략 실행 흐름
    MD->>ST: market:ohlcv:bybit:BTCUSDT:4h (4h 확정 봉)
    MD->>ORCH: market:ohlcv:bybit:BTCUSDT:4h

    Note over ORCH,ST: 전략 명령
    ORCH->>ST: strategy:command:supertrend-01 (자본 배분)

    Note over ST,ENG: 주문 라이프사이클 (★order:update 아님)
    ST->>ENG: order:request (지정가 Post-only)
    ENG->>ST: order:result (filled/cancelled/rejected)
    ENG->>ST: order:result:supertrend-01 (전략 전용)

    Note over ENG,TG: 체결 알림 (2026-09-19: 발행부 신설 — 이전엔 구독자만 있고 발행자가 없어 매수/매도 체결이 텔레그램에 전혀 뜨지 않았다)
    ENG->>TG: ce:alerts:entry (매수 체결)
    ENG->>TG: ce:alerts:exit (매도 체결, 근사 실현손익 포함)

    Note over ORCH,TG: Kill Switch (2026-08-29: 오케스트레이터가 ce:kill_switch 구독)
    TG->>ORCH: ce:kill_switch (Telegram /emergency_close)
    Note over ORCH: _listen_external_kill → trigger_manual
    ORCH->>ST: strategy:command:supertrend-01 stop
    ORCH->>ENG: ce:kill_switch:active (신규 주문 차단)
    ENG->>TG: ce:alerts:anomaly (ERROR/CRITICAL 로그 브리지)
    ORCH->>TG: ce:alerts:kill_switch (2026-09-19: 전용 리치 알림 발행 신설 — 이전엔 anomaly 브리지로만 간접 도달)
    MD->>TG: ce:alerts:anomaly (2026-09-19: 펀딩 요율 임계값 경고 — 예전 "alerts:funding"(접두사 누락, 구독자 없음)에서 재라우팅)

    Note over TG: ce:alerts:daily_report는 발행자가 없다 — 실제 일일 리포트는<br/>telegram-bot 자체 스케줄러(_scheduled_report_task)가 pub/sub을 거치지 않고 직접 전송한다
```

---

## §2. 채널 상수 정의 위치

| 채널 | 상수 정의 | 파일 |
|---|---|---|
| `ce:kill_switch` | orchestrator `_listen_external_kill` (구독자 ≥1), 발행: telegram / emergency 스크립트 / KS 콜백 | `cryptoengine/shared/kill_switch.py:32` |
| `ce:kill_switch:ack` | `KILL_SWITCH_ACK_CHANNEL` | `cryptoengine/shared/kill_switch.py:34` |
| `ce:alerts:anomaly` | `_ERROR_ALERT_CHANNEL` (ERROR/CRITICAL 로그 전체) + funding_monitor 펀딩 요율 임계값 경고(2026-09-19 재라우팅) | `cryptoengine/shared/logging_config.py:154`, `cryptoengine/services/market-data/funding_monitor.py:_check_alerts` |
| `ce:alerts:entry` / `ce:alerts:exit` | `TRADE_ALERT_CHANNELS` (2026-09-19 신설 — 이전엔 구독자만 있고 발행자가 없던 죽은 채널) | `cryptoengine/services/execution/engine.py:_publish_trade_alert` |
| `ce:alerts:kill_switch` | `_on_kill_switch_trigger` (2026-09-19 신설 — 이전엔 `ce:alerts:anomaly` 경유로만 간접 도달, Level/Action/DD 상세 없이 표시됨) | `cryptoengine/services/orchestrator/core.py:_on_kill_switch_trigger` |
| `position:reconcile_event` | `RECONCILE_CHANNEL` | `cryptoengine/services/execution/position_tracker.py:37` |
| `market:ohlcv:bybit:BTCUSDT:4h` | 하드코딩 | `cryptoengine/services/strategies/supertrend/strategy.py:926` |

---

## §3. f-string 채널 패턴

| 패턴 | 발행자 | 구독자 |
|---|---|---|
| `market:ohlcv:{exchange}:{symbol}:{tf}` | market-data | supertrend, orchestrator |
| `market:funding:{exchange}:{symbol}` | market-data, funding_monitor | orchestrator |
| `market:open_interest:...` | market-data | orchestrator |
| `market:liquidations:...` | market-data | orchestrator |
| `order:request` | supertrend | execution-engine |
| `order:result` | execution-engine | supertrend |
| `order:result:{strategy_id}` | execution-engine | supertrend (전략 전용) |
| `strategy:command:{strategy_id}` | orchestrator | supertrend (`tick_interval` 60s drain) |
| `ce:strategy:command` | telegram-bot | orchestrator |
| `ce:alerts:{type}` | 전 서비스 | telegram-bot |
| `ce:alerts:daily_report` | ⚠️ **DEAD** — 발행자 없음 (일일 리포트는 telegram-bot 자체 스케줄러가 pub/sub 없이 직접 전송) | — |
| `ce:alerts:funding` | ⚠️ **미사용** — `_format_funding_alert`/`FundingAccumulator`는 "펀딩 결제(payment)" 스키마 전용으로 예약돼 있으나 그런 이벤트를 발행하는 코드가 아직 없다. 펀딩 요율 임계값 경고는 `ce:alerts:anomaly`로 간다(위 §2 참고) — 다시 이 채널로 되돌리지 말 것 | — |
| `llm:advisory` | ⚠️ **DEAD** — 발행자 없음 | — |

> `llm:advisory`: `llm-advisor` 서비스와 orchestrator의 `_subscribe_llm_advisory()` 구독 로직이 2026-08-29 전량 삭제되었다. 채널 상수만 코드에 잔존할 수 있으나 발행자가 없어 죽은 채널이다.

> 분기물 Redis 채널(`market:ohlcv:{exchange}:{quarterly}:{tf}`, `market:ticker:{exchange}:{quarterly}`)과 `quarterly_lifecycle.py`는 2026-08-29 D2에서 제거됨. 라이브 구독은 Bybit **BTCUSDT 4h**만. Track-C(Binance/OKX)도 2026-08-29 전량 삭제.

---

## §3.1 Redis KV 키 (잔고 · 하트비트)

| 키 | TTL | Writer | 용도 |
|---|---|---|---|
| `ce:phase5:equity_baseline` | 없음 | execution-engine | Phase 5 잔고 게이트 기준선 (전원 장애 복구). JSON: `equity`, `updated_at`, `source` |
| `cache:wallet_balance` | 300s | execution-engine | orchestrator 포트폴리오 equity 조회 |
| `cache:balance:{exchange}` | 300s | execution-engine | 거래소별 잔고 캐시 |
| `heartbeat:execution-engine` | 300s | execution-engine | Dead Man's Switch 감시 |

---

