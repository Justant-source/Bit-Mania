---
title: CryptoEngine 내부 API 문서
tags:
  - api
  - redis
  - pubsub
  - rest
  - messaging
aliases:
  - API
  - 내부 API
  - Redis 채널
related:
  - "[[architecture]]"
  - "[[runbook]]"
  - "[[strategies/funding_arb]]"
  - "[[strategies/adaptive_dca]]"
---

# CryptoEngine 내부 API 문서

> [!abstract] 개요
> 서비스 간 통신은 Redis Pub/Sub 채널과 REST API (Dashboard)로 구성됩니다.
> 시스템 전체 구조는 [[architecture|아키텍처 문서]] 참조.

---

## Redis Pub/Sub 채널

### 시장 데이터 채널

#### `market:ohlcv:{exchange}:{symbol}:{timeframe}`
OHLCV 캔들 데이터 배포. [[architecture#1. Market Data Collector|Market Data Collector]] → 전략 서비스

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

#### `market:regime`
시장 레짐 분류 결과. → [[architecture#2. Strategy Orchestrator|오케스트레이터]]가 수신하여 가중치 조정

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

> [!note] 레짐별 전략 활성화
> - `trending_up/down` → [[strategies/funding_arb|펀딩비 차익거래]] 유지
> - `volatile` → 전체 전략 축소

#### `market:funding:{symbol}`
펀딩레이트 업데이트. → [[strategies/funding_arb|펀딩비 전략]]의 핵심 입력

```json
{
  "exchange": "bybit",
  "symbol": "BTCUSDT",
  "rate": 0.0001,
  "predicted_rate": 0.00008,
  "next_funding_time": "2026-04-03T16:00:00Z"
}
```

### 주문 채널

#### `order:request`
전략 → [[architecture#3. Execution Engine|실행 엔진]] 주문 요청.

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

> [!tip] 주문을 사용하는 전략
> - [[strategies/funding_arb|펀딩비]]: 현물+선물 동시 주문
> - [[strategies/adaptive_dca|DCA]]: 시장가/지정가 매수 주문

#### `order:result`
실행 엔진 → 전략 주문 결과.

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

#### `order:result:{strategy_id}`
특정 전략 전용 결과 채널.

### 전략 명령 채널

#### `strategy:{strategy_id}:command`
[[architecture#2. Strategy Orchestrator|오케스트레이터]] → 전략 자본 배분 명령.

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

### 시스템 채널

#### `system:kill_switch`
[[architecture#Kill Switch 4단계|Kill Switch]] 발동 이벤트. 대응 절차: [[runbook#Kill Switch 대응]]

```json
{
  "triggered": true,
  "reason": "Daily drawdown -5.2% >= -5.0%",
  "timestamp": "2026-04-03T12:00:00Z",
  "cooldown_minutes": 60
}
```

#### `llm:advisory`
[[architecture#5. LLM Advisor|LLM 어드바이저]] 가중치 조정 제안.

```json
{
  "rating": "buy",
  "confidence": 0.72,
  "weight_adjustments": {
    "funding_arb": 0.05,
    "grid": -0.05,
    "dca": 0.03,
    "cash": -0.03
  },
  "reasoning": "시장 저점 구간으로 판단...",
  "regime_assessment": "ranging_to_trending_up"
}
```

#### `llm:request`
LLM 분석 요청 (온디맨드).

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

---

## Dashboard REST API

### 내부 API (포트 3000)

#### `GET /api/internal/portfolio`
현재 포트폴리오 상태 조회.

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

#### `GET /api/internal/positions`
열린 포지션 목록.

#### `GET /api/internal/trades?limit=50&strategy=funding_arb`
거래 이력 조회.

#### `GET /api/internal/regime`
현재 시장 레짐.

#### `POST /api/internal/kill-switch`
Kill Switch 수동 발동. [[runbook#Kill Switch 대응|대응 절차 참조]]

```json
{
  "reason": "Manual trigger from dashboard"
}
```

#### `POST /api/internal/resume`
Kill Switch 해제.

### 외부 API (포트 3001)

#### `GET /api/public/status`
시스템 상태 (인증 불필요, 제한된 정보).

```json
{
  "status": "running",
  "regime": "ranging",
  "uptime_hours": 72.5
}
```

#### `GET /api/public/performance`
성과 요약 (제한된 정보).

---

## 도메인 모델 (Pydantic v2)

> [!note] 모델 위치
> `shared/models/` 디렉토리에 정의. [[architecture#디렉토리 구조|디렉토리 구조]] 참조.

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

> [!tip] 문제 해결
> 에러 발생 시 [[runbook#문제 해결|운영 매뉴얼 문제 해결]] 섹션 참조

> [!seealso] 관련 문서
> - [[architecture|시스템 아키텍처]] — 서비스 구조 및 역할
> - [[runbook|운영 매뉴얼]] — 인시던트 대응 및 문제 해결
> - [[strategies/funding_arb|펀딩비 차익거래]] — 핵심 전략
> - [[strategies/adaptive_dca|적응형 DCA]] — 보조 전략
> - [[changelog|변경 이력]] — 버전별 변경사항
