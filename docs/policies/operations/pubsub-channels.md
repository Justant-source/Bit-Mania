---
title: Redis Pub/Sub 채널 카탈로그
category: policies/operations
related_code:
  - cryptoengine/shared/redis_client.py
  - cryptoengine/services/*/
last_updated: 2026-05-01
when_to_update: |
  - 채널 추가/삭제 시
  - 메시지 포맷 변경 시
---

# Redis Pub/Sub 채널 카탈로그

---

## Redis Pub/Sub 채널

```mermaid
graph LR
    subgraph pub["Publishers"]
        MD["market-data"]
        FA["funding-arb"]
        DCA["adaptive-dca"]
        ORC["strategy-orchestrator"]
        ENG["execution-engine"]
    end

    subgraph ch["Redis Channels"]
        C1["market:ohlcv\n:{exchange}:{symbol}:{tf}"]
        C2["market:regime"]
        C3["market:funding:{symbol}"]
        C4["order:request"]
        C5["order:update"]
        C6["strategy:command:{id}"]
        C7["ce:kill_switch"]
        C8["system:service_health"]
        C9["ce:kill_switch:ack"]
    end

    subgraph sub["Subscribers"]
        FA2["funding-arb"]
        DCA2["adaptive-dca"]
        ORC2["strategy-orchestrator"]
        ENG2["execution-engine"]
        TG["telegram-bot"]
    end

    MD --> C1
    MD --> C2
    MD --> C3
    FA --> C4
    DCA --> C4
    ORC --> C6
    ORC --> C7
    ORC --> C8
    ENG --> C5
    ENG --> C9

    C1 --> FA2
    C1 --> DCA2
    C1 --> ORC2
    C2 --> ORC2
    C3 --> FA2
    C4 --> ENG2
    C5 --> FA2
    C5 --> DCA2
    C6 --> FA2
    C6 --> DCA2
    C7 --> ENG2
    C7 --> TG
    C9 --> ORC2

    style C7 fill:#ff4444,color:#fff
    style C4 fill:#4caf50,color:#fff
    style C2 fill:#2196f3,color:#fff
```

### 시장 데이터 채널

#### `market:ohlcv:{exchange}:{symbol}:{timeframe}`

OHLCV 캔들 데이터 배포. Market Data Collector → 전략 서비스

**메시지 예시**:

```json
{
  "exchange": "bybit",
  "symbol": "BTCUSDT",
  "timeframe": "5m",
  "open": 65000.0,
  "high": 65100.0,
  "low": 64950.0,
  "close": 65050.0,
  "volume": 1234.56,
  "ts": 1712100000000,
  "confirmed": true
}
```

**수신자**:
- funding-arb 전략
- adaptive-dca 전략
- orchestrator

---

#### `market:regime`

시장 레짐 분류 결과 → Strategy Orchestrator가 수신하여 가중치 조정

**메시지 예시**:

```json
{
  "regime": "ranging",
  "confidence": 0.75,
  "adx": 18.5,
  "volatility": 120.3,
  "bb_width": 0.035,
  "detected_at": "2026-04-03T12:00:00Z"
}
```

**레짐 값**:
- `trending_up`: 상승 추세
- `trending_down`: 하락 추세
- `ranging`: 횡보 (박스권)
- `volatile`: 고변동성

**전략별 대응**:
- `trending_up/down` → 펀딩비 차익거래 유지
- `volatile` → 전체 전략 축소

---

#### `market:funding:{symbol}`

펀딩레이트 업데이트 → Funding Arb 전략의 핵심 입력

**메시지 예시**:

```json
{
  "exchange": "bybit",
  "symbol": "BTCUSDT",
  "rate": 0.0001,
  "predicted_rate": 0.00008,
  "next_funding_time": "2026-04-03T16:00:00Z"
}
```

**수신자**: funding-arb 전략 (진입/청산 판정)

---

### 주문 채널

#### `order:request`

전략 → Execution Engine 주문 요청

**메시지 예시**:

```json
{
  "request_id": "abc123def456",
  "strategy_id": "funding_arb_01",
  "exchange": "bybit",
  "symbol": "BTC/USDT:USDT",
  "side": "buy",
  "order_type": "limit",
  "quantity": 0.1,
  "price": 65000.0,
  "post_only": true,
  "reduce_only": false,
  "stop_loss": null,
  "take_profit": null
}
```

**발행자**: 
- funding-arb (현물/선물 동시 주문)
- adaptive-dca (매수 주문)

---

#### `order:result`

Execution Engine → 전략 주문 결과

**메시지 예시**:

```json
{
  "request_id": "abc123def456",
  "order_id": "bybit-ord-789",
  "status": "filled",
  "filled_qty": 0.1,
  "filled_price": 65000.0,
  "fee": 0.039,
  "fee_currency": "USDT",
  "timestamp": "2026-04-03T12:00:00Z"
}
```

**status 값**:
- `new` — 주문 접수
- `partially_filled` — 부분 체결
- `filled` — 완전 체결
- `cancelled` — 취소됨
- `rejected` — 거부됨
- `expired` — 만료됨

**수신자**: 주문을 낸 전략

---

#### `order:result:{strategy_id}`

특정 전략 전용 결과 채널.

---

### 전략 명령 채널

#### `strategy:{strategy_id}:command`

Strategy Orchestrator → 전략 자본 배분 명령

**메시지 예시**:

```json
{
  "strategy_id": "funding_arb",
  "allocated_capital": 2500.0,
  "weight": 0.25,
  "regime": "ranging",
  "max_drawdown": 5.0,
  "timestamp": "2026-04-03T12:00:00Z"
}
```

**수신자**:
- funding-arb (자본 배분 조정)
- adaptive-dca (자본 배분 조정)

---

### 시스템 채널

#### `system:kill_switch`

Kill Switch 발동 이벤트 → 모든 서비스 수신 (즉시 청산)

**메시지 예시**:

```json
{
  "triggered": true,
  "level": 2,
  "reason": "Daily drawdown -5.2% >= -5.0%",
  "timestamp": "2026-04-03T12:00:00Z",
  "cooldown_minutes": 60
}
```

**수신자**:
- execution-engine (포지션 청산)
- funding-arb (포지션 상태 저장 후 종료)
- adaptive-dca (포지션 청산)
- telegram-bot (알림 전송)

**대응 절차**: [../kill-switch.md](../kill-switch.md) 참조

---

#### `llm:advisory`

LLM 어드바이저 가중치 조정 제안

**메시지 예시**:

```json
{
  "rating": "buy",
  "confidence": 0.72,
  "weight_adjustments": {
    "funding_arb": 0.05,
    "adaptive_dca": 0.03,
    "cash": -0.08
  },
  "reasoning": "시장 저점 구간으로 판단...",
  "regime_assessment": "ranging_to_trending_up"
}
```

**수신자**: strategy-orchestrator (선택적 가중치 조정)

---

#### `llm:request`

LLM 분석 요청 (온디맨드)

**메시지 예시**:

```json
{
  "trigger": "on_demand",
  "context": {}
}
```

---

## Redis 캐시 키

| 키 | 타입 | TTL | 설명 |
|----|------|-----|------|
| `market:regime:current` | String(JSON) | 600s | 현재 레짐 |
| `cache:regime` | Hash | - | 레짐 상세 정보 |
| `cache:portfolio_state` | String(JSON) | 300s | 포트폴리오 상태 |
| `features:latest` | String(JSON) | 300s | ML 특성 벡터 |
| `market:ticker:{symbol}` | String(JSON) | 60s | 최신 시세 |
| `market:funding:{symbol}` | String(JSON) | 600s | 펀딩레이트 |
| `orchestrator:state` | String(JSON) | 600s | 오케스트레이터 상태 |
| `orchestrator:kill_switch` | String(JSON) | 7200s | Kill Switch 상태 |
| `llm:latest_advisory` | String(JSON) | 28800s | 최신 LLM 어드바이저리 |
| `strategy:saved_state:funding_arb` | String(JSON) | 3600s | Funding Arb 포지션 복구용 |

---

## Dashboard REST API

### 내부 API (포트 3000)

#### `GET /api/internal/portfolio`

현재 포트폴리오 상태 조회

```bash
curl http://localhost:3000/api/internal/portfolio
```

**응답**:

```json
{
  "total_equity": 10000.0,
  "unrealized_pnl": -50.0,
  "realized_pnl_today": 120.0,
  "daily_drawdown": -0.005,
  "strategies": [
    {
      "strategy_id": "funding_arb_01",
      "allocated_capital": 2500.0,
      "current_pnl": 80.0,
      "position_count": 1
    }
  ]
}
```

---

#### `GET /api/internal/positions`

열린 포지션 목록

```bash
curl http://localhost:3000/api/internal/positions
```

---

#### `GET /api/internal/trades?limit=50&strategy=funding_arb`

거래 이력 조회

```bash
curl http://localhost:3000/api/internal/trades?limit=50
```

---

#### `GET /api/internal/regime`

현재 시장 레짐

```bash
curl http://localhost:3000/api/internal/regime
```

---

#### `POST /api/internal/kill-switch`

Kill Switch 수동 발동

```bash
curl -X POST http://localhost:3000/api/internal/kill-switch \
  -H "Content-Type: application/json" \
  -d '{"reason": "Manual trigger from dashboard"}'
```

**대응 절차**: [../kill-switch.md](../kill-switch.md) 참조

---

#### `POST /api/internal/resume`

Kill Switch 해제 (수동 재개)

```bash
curl -X POST http://localhost:3000/api/internal/resume
```

---

### 외부 API (포트 3001)

#### `GET /api/public/status`

시스템 상태 (인증 불필요, 제한된 정보)

```bash
curl http://localhost:3001/api/public/status
```

**응답**:

```json
{
  "status": "running",
  "regime": "ranging",
  "uptime_hours": 72.5
}
```

---

#### `GET /api/public/performance`

성과 요약 (제한된 정보)

```bash
curl http://localhost:3001/api/public/performance
```

---

## 도메인 모델 (Pydantic v2)

모든 모델은 `shared/models/` 에 정의됨.

### OrderRequest

```python
class OrderRequest(BaseModel):
    strategy_id: str
    exchange: str
    symbol: str
    side: Literal["buy", "sell"]
    order_type: Literal["limit", "market", "stop_limit", "stop_market"]
    quantity: float  # > 0
    price: float | None = None
    post_only: bool = True
    reduce_only: bool = False
    stop_loss: float | None = None
    take_profit: float | None = None
    request_id: str  # 자동 생성 UUID
```

---

### OrderResult

```python
class OrderResult(BaseModel):
    request_id: str
    order_id: str
    status: Literal["new", "partially_filled", "filled", "cancelled", "rejected", "expired"]
    filled_qty: float = 0.0
    filled_price: float | None = None
    fee: float = 0.0
    fee_currency: str = "USDT"
    timestamp: datetime
```

---

### PortfolioState

```python
class PortfolioState(BaseModel):
    total_equity: float
    unrealized_pnl: float = 0.0
    realized_pnl_today: float = 0.0
    daily_drawdown: float = 0.0
    weekly_drawdown: float = 0.0
    strategies: list[StrategySnapshot]
    kill_switch_triggered: bool = False
```

---

### MarketRegime

```python
class MarketRegime(BaseModel):
    regime: Literal["trending_up", "trending_down", "ranging", "volatile"]
    confidence: float  # 0.0 ~ 1.0
    adx: float | None = None
    volatility: float | None = None
    bb_width: float | None = None
```

---

## 에러 코드

| 코드 | 설명 | 대응 |
|------|------|------|
| `safety_check_internal_error` | 안전성 검사 내부 오류 | 로그 확인 |
| `execution_failed_after_3_retries` | 3회 재시도 후 실행 실패 | 거래소 상태 확인 |
| `order_timeout` | 주문 타임아웃 (30초) | 네트워크 확인 |
| `order_rejected` | 거래소에서 주문 거부 | 잔고/마진 확인 |

---

## 관련 문서

- [runbook.md](runbook.md) — 운영 매뉴얼
- [../kill-switch.md](../kill-switch.md) — Kill Switch 정책
- [../strategies/funding-arb.md](../strategies/funding-arb.md) — Funding Arb 전략
