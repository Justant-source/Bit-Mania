# 비트코인 선물 자동매매 시스템 설계서 — Part 1: 아키텍처

> **프로젝트명**: CryptoEngine
> **목표**: 극도로 안정적인 수익을 24/7 생성하고, 투명한 대시보드로 신뢰를 구축하는 자동매매 시스템
> **1차 거래소**: Bybit / **중장기**: Binance, OKX, Bitget 확장
> **환경**: Windows PC + WSL Ubuntu + Docker + Claude Code Max

---

## 1. 시스템 전체 아키텍처

```
┌──────────────────────────────────────────────────────────────────────┐
│                        DOCKER COMPOSE STACK                         │
│                    (WSL Ubuntu, 24/7 운영)                           │
│                                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  ┌───────────┐ │
│  │  Strategy    │  │  Execution   │  │  Market Data │  │  LLM      │ │
│  │  Orchestrator│←→│  Engine      │←→│  Collector   │  │  Advisor  │ │
│  │  (전략 조율) │  │  (주문 실행) │  │  (데이터 수집)│  │  (분석)   │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └─────┬─────┘ │
│         │                 │                 │                │       │
│  ┌──────▼─────────────────▼─────────────────▼────────────────▼─────┐ │
│  │                     Redis (메시지 브로커 + 캐시)                 │ │
│  └──────┬──────────────────────────────────────────────────────────┘ │
│         │                                                            │
│  ┌──────▼──────┐  ┌──────────────┐  ┌──────────────┐               │
│  │  PostgreSQL  │  │  Grafana     │  │  Telegram    │               │
│  │  (트레이드DB)│  │  Dashboard   │  │  Bot         │               │
│  └─────────────┘  │  + Public Web│  │  (알림)      │               │
│                    └──────────────┘  └──────────────┘               │
└──────────────────────────────────────────────────────────────────────┘
         ↕ WebSocket / REST API
┌──────────────────────────────────────────────────────────────────────┐
│  Bybit API  │  (향후) Binance  │  OKX  │  Bitget  │  CoinGlass     │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. 핵심 설계 원칙

### 2.1 생존 우선 (Survival First)

모든 컴포넌트는 "장애가 나면 포지션을 보호한다"를 제1원칙으로 한다.

```
[Kill Switch 계층]

Level 1 — 전략 레벨: 개별 전략의 손절 로직
Level 2 — 포트폴리오 레벨: 일일 -1%, 주간 -3% 도달 시 전략 자동 정지
Level 3 — 시스템 레벨: Docker 컨테이너 헬스체크 실패 시 모든 오픈 포지션 마켓 청산
Level 4 — 수동 비상: Telegram "/emergency_close" 명령으로 즉시 전체 청산
```

### 2.2 전략 독립성 (Strategy Isolation)

각 전략은 독립 프로세스로 실행되며, 하나가 죽어도 다른 전략에 영향 없음. Redis Pub/Sub으로 Orchestrator와 통신.

### 2.3 Docker 기반 격리

Windows PC를 개인 용도로 사용하면서 트레이딩 프로세스는 Docker 안에서 24/7 운영. WSL이 꺼져도 Docker Desktop의 자동 재시작으로 복구.

---

## 3. Docker Compose 구성

```yaml
# docker-compose.yml 설계
version: "3.9"

services:
  # ──────────────────── 인프라 ────────────────────
  postgres:
    image: postgres:16-alpine
    volumes:
      - pgdata:/var/lib/postgresql/data
    environment:
      POSTGRES_DB: cryptoengine
      POSTGRES_USER: engine
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    restart: always
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U engine"]
      interval: 10s
      retries: 5

  redis:
    image: redis:7-alpine
    restart: always
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s

  # ──────────────────── 핵심 서비스 ────────────────────
  market-data:
    build: ./services/market-data
    depends_on: [redis, postgres]
    restart: always
    environment:
      - BYBIT_API_KEY=${BYBIT_API_KEY}
      - BYBIT_API_SECRET=${BYBIT_API_SECRET}
      - COINGLASS_API_KEY=${COINGLASS_API_KEY}

  strategy-orchestrator:
    build: ./services/orchestrator
    depends_on: [redis, postgres, market-data]
    restart: always

  execution-engine:
    build: ./services/execution
    depends_on: [redis, postgres]
    restart: always

  funding-arb:
    build: ./services/strategies/funding-arb
    depends_on: [redis, postgres, market-data]
    restart: always

  grid-trading:
    build: ./services/strategies/grid-trading
    depends_on: [redis, postgres, market-data]
    restart: always

  adaptive-dca:
    build: ./services/strategies/adaptive-dca
    depends_on: [redis, postgres, market-data]
    restart: always

  # ──────────────────── LLM & 분석 ────────────────────
  llm-advisor:
    build: ./services/llm-advisor
    depends_on: [redis]
    volumes:
      - /tmp/claude-code:/tmp/claude-code  # Claude Code 소켓 마운트

  # ──────────────────── 모니터링 & 알림 ────────────────────
  telegram-bot:
    build: ./services/telegram-bot
    depends_on: [redis, postgres]
    restart: always

  dashboard:
    build: ./services/dashboard
    depends_on: [postgres, redis]
    ports:
      - "3000:3000"   # 내부 대시보드
      - "3001:3001"   # 공개 퍼포먼스 페이지
    restart: always

  grafana:
    image: grafana/grafana:latest
    volumes:
      - grafana-data:/var/lib/grafana
      - ./config/grafana:/etc/grafana/provisioning
    ports:
      - "3002:3000"
    depends_on: [postgres]
    restart: always

  # ──────────────────── 백테스트 (온디맨드) ────────────────────
  backtester:
    build: ./services/backtester
    depends_on: [postgres]
    profiles: ["backtest"]  # docker compose --profile backtest up

volumes:
  pgdata:
  grafana-data:
```

---

## 4. 서비스별 상세 설계

### 4.1 Market Data Collector

거래소 데이터를 실시간 수집하여 Redis에 발행하고, PostgreSQL에 영구 저장한다.

```
[데이터 수집 채널]

실시간 (WebSocket):
├── Bybit: BTC/USDT 무기한 선물
│   ├── 오더북 (depth 25, 100ms 갱신)
│   ├── 체결 데이터 (trades)
│   ├── 캔들 (1m, 5m, 15m, 1h, 4h)
│   └── 펀딩비 (실시간 예측값 + 8h 확정값)
├── (향후) Binance, OKX, Bitget 동일 구조
└── CoinGlass: 멀티 거래소 펀딩비 비교

주기적 (REST, 1분~5분 간격):
├── 미체결약정 (Open Interest)
├── 롱/숏 비율
├── 청산 데이터
└── Fear & Greed Index (Alternative.me, 1일 1회)
```

```python
# 핵심 데이터 모델 (services/market-data/models.py)

class FundingRate:
    exchange: str        # "bybit"
    symbol: str          # "BTCUSDT"
    rate: float          # 0.0001 (0.01%)
    predicted_rate: float
    next_funding_time: datetime
    collected_at: datetime

class OHLCV:
    exchange: str
    symbol: str
    timeframe: str       # "1m", "5m", "1h", "4h"
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime

class MarketRegime:
    regime: str          # "trending_up", "trending_down", "ranging", "volatile"
    confidence: float    # 0.0 ~ 1.0
    adx: float
    volatility: float    # ATR 기반
    bb_width: float      # 볼린저 밴드 폭
    detected_at: datetime
```

### 4.2 Strategy Orchestrator

모든 전략의 가동/정지/가중치를 제어하는 두뇌. 시장 레짐에 따라 자본 배분을 동적으로 조절한다.

```
[Orchestrator 핵심 로직]

매 5분마다:
1. Market Data로부터 현재 시장 레짐 수신
2. 레짐에 따라 전략별 가중치 조절
3. 포트폴리오 전체 리스크 평가
4. Kill Switch 조건 체크
5. 전략별 자본 배분 명령 발행 (Redis)

시장 레짐별 가중치 매트릭스:
┌─────────────┬──────────┬──────────┬──────────┬──────────┐
│ 전략 \ 레짐  │ 횡보     │ 상승추세 │ 하락추세 │ 고변동성 │
├─────────────┼──────────┼──────────┼──────────┼──────────┤
│ 펀딩비 차익  │ 50%      │ 55%      │ 55%      │ 40%      │
│ 그리드       │ 35%      │ 15%      │ 15%      │ 10%      │
│ 적응형 DCA   │ 5%       │ 15%      │ 20%      │ 15%      │
│ 현금 예비금  │ 10%      │ 15%      │ 10%      │ 35%      │
└─────────────┴──────────┴──────────┴──────────┴──────────┘

레짐 판별 기준:
- ADX < 20 & BB Width < 중간값     → 횡보
- ADX > 25 & 가격 > EMA20          → 상승추세
- ADX > 25 & 가격 < EMA20          → 하락추세
- ATR > 평균ATR × 2.0              → 고변동성
```

```python
# 핵심 인터페이스 (services/orchestrator/core.py)

class StrategyCommand:
    strategy_id: str          # "funding_arb", "grid", "adaptive_dca"
    action: str               # "start", "stop", "adjust_weight"
    allocated_capital: float   # USDT 기준 배분 자본
    max_drawdown: float        # 해당 전략 최대 허용 손실
    params: dict               # 전략 파라미터 오버라이드

class PortfolioState:
    total_equity: float
    unrealized_pnl: float
    realized_pnl_today: float
    daily_drawdown: float       # 오늘 최대 낙폭
    weekly_drawdown: float
    strategies: dict[str, StrategyState]
    kill_switch_triggered: bool
```

### 4.3 Execution Engine

모든 전략의 주문을 중앙에서 처리한다. 전략은 직접 거래소 API를 호출하지 않으며, 반드시 Execution Engine을 통한다. 이렇게 해야 주문 충돌 방지, 포지션 통합 관리, 로깅이 가능하다.

```
[주문 흐름]

전략 → Redis (order_request 채널) → Execution Engine → 거래소 API
                                          ↓
                                    Redis (order_result 채널) → 전략
                                          ↓
                                    PostgreSQL (trade_log 테이블)
```

```python
# 주문 요청/응답 모델 (services/execution/models.py)

class OrderRequest:
    strategy_id: str
    exchange: str              # "bybit"
    symbol: str                # "BTCUSDT"
    side: str                  # "buy", "sell"
    order_type: str            # "limit", "market"
    quantity: float
    price: float | None        # limit일 때만
    post_only: bool = True     # Maker 보장
    reduce_only: bool = False
    stop_loss: float | None
    take_profit: float | None
    request_id: str            # UUID, 멱등성 보장

class OrderResult:
    request_id: str
    order_id: str
    status: str                # "filled", "partial", "rejected", "cancelled"
    filled_qty: float
    filled_price: float
    fee: float
    fee_currency: str
    timestamp: datetime

class Position:
    exchange: str
    symbol: str
    side: str                  # "long", "short", "none"
    size: float
    entry_price: float
    unrealized_pnl: float
    leverage: float
    liquidation_price: float
    margin_used: float
```

### 4.4 전략 서비스: Funding Rate Arbitrage (1순위)

```
[펀딩비 차익거래 — Phase 1: 단일 거래소 (Bybit)]

Bybit 내에서 현물 매수 + 무기한 선물 숏으로 델타 뉴트럴 포지션 구성.
가격 변동과 무관하게 8시간마다 펀딩비 수취.

진입 조건:
1. 현재 예측 펀딩비 > 0.005% (연 5.5% 이상 환산)
2. BTC/USDT 현물-선물 괴리율 < 0.3%
3. 포트폴리오에 배분된 자본 내에서 실행

진입 실행:
1. 현물 BTC 매수 (지정가, Post-Only)
2. 무기한 선물 BTC 동일 수량 숏 (지정가, Post-Only)
3. 두 주문 모두 체결 확인 후 포지션 활성화
4. 한쪽만 체결 시 3분 대기 → 미체결 취소 → 체결된 쪽 정리

유지 관리 (매 1시간):
- 현물/선물 수량 일치 확인 (±0.1% 이내)
- 마진 비율 확인 (유지 마진 대비 3배 이상 여유)
- 선물 미체결 PnL 확인 → 현물 PnL과 상쇄 확인

청산 조건:
- 펀딩비가 -0.005% 이하로 전환 (역방향)
- Orchestrator의 stop 명령
- Kill Switch 발동
- 현물-선물 괴리율 > 1% (비정상)

청산 실행:
1. 선물 숏 마켓 청산
2. 현물 BTC 마켓 매도
3. 순서 중요: 선물 먼저 (청산 리스크가 더 높음)
```

```
[펀딩비 차익거래 — Phase 2: 거래소 간 (중장기)]

거래소 A에서 BTC 선물 롱 (낮은 펀딩비)
거래소 B에서 BTC 선물 숏 (높은 펀딩비)
→ 펀딩비 차이만큼 수익

필요 조건:
- 각 거래소에 자본 배치 (자금 이동 불필요)
- CoinGlass API로 거래소 간 펀딩비 실시간 비교
- 진입 기준: 거래소 간 펀딩비 차이 > 0.02%

Phase 2는 Bybit 안정 운영 확인 후 (최소 3개월) 추가 거래소 API 연동.
Binance, OKX 순서로 확장. 코드 구조는 Phase 1부터 멀티 거래소를 고려해 설계.
```

### 4.5 전략 서비스: Grid Trading

```
[그리드 트레이딩 — 횡보장 전용]

가동 조건 (Orchestrator가 판단):
- 시장 레짐 = "ranging"
- ADX < 20
- 볼린저 밴드 폭이 최근 20일 평균 이하

설정:
- 가격 범위: 현재가 ± ATR(14) × 3 (동적)
- 그리드 수: 20~40개
- 각 그리드 주문량: 배분 자본 / 그리드 수 / 현재가
- 모든 주문 Post-Only (Maker 수수료)

안전장치:
- 가격이 그리드 범위 ±5% 이탈 시 자동 정지
- 추세장 진입 감지 (ADX > 25) 시 자동 정지
- 정지 시: 현재 보유 포지션만 청산, 미체결 주문 전체 취소
```

### 4.6 전략 서비스: Adaptive DCA

```
[공포지수 기반 적응형 DCA]

데이터:
- Alternative.me Fear & Greed Index (1일 1회)
- Bybit BTC 펀딩비 (음수 = 하락 심리)
- 주간 RSI(14)

매수 로직 (주 1회, 월요일 UTC 00:00):
- F&G 0~10:  기본금 × 3.0
- F&G 11~25: 기본금 × 2.0
- F&G 26~50: 기본금 × 1.0
- F&G 51~75: 매수 중단
- F&G 76~100: 보유분 20% 매도

기본금: 배분 자본의 2% (주간)
매수 방식: 현물 BTC 지정가 (현재가 -0.1%)
별도 지갑에서 관리 (장기 보유 목적)
```

### 4.7 LLM Advisor

Claude Code Max를 로컬에서 활용하여 시장 분석, 전략 파라미터 제안, 리스크 평가를 수행한다. API 비용 없이 LLM 능력을 활용하는 핵심 차별점.

```
[LLM Advisor 아키텍처]

┌─────────────────────────────────┐
│  LLM Advisor Service (Docker)   │
│                                 │
│  ┌─────────────┐               │
│  │ Task Queue   │ ← Redis Sub  │
│  │ (분석 요청)  │               │
│  └──────┬───────┘               │
│         ▼                       │
│  ┌─────────────┐               │
│  │ Prompt       │               │
│  │ Builder      │               │
│  │ (컨텍스트    │               │
│  │  + 데이터    │               │
│  │  조합)       │               │
│  └──────┬───────┘               │
│         ▼                       │
│  ┌─────────────┐               │
│  │ Claude Code  │ ← 로컬 실행  │
│  │ CLI Bridge   │   (Max 토큰) │
│  └──────┬───────┘               │
│         ▼                       │
│  ┌─────────────┐               │
│  │ Response     │ → Redis Pub  │
│  │ Parser       │   (구조화)   │
│  └─────────────┘               │
└─────────────────────────────────┘

호출 시점 (비용 없으므로 적극 활용):
1. 매 4시간: 시장 환경 종합 분석
2. 매 8시간: 펀딩비 추세 + 진입/청산 판단 보조
3. Orchestrator 레짐 전환 시: 전략 가중치 조정 의견
4. 비정상 상황 감지 시: 긴급 분석

Claude Code Max는 토큰 제한이 넉넉하므로 별도 로컬 모델 없이
모든 LLM 작업을 Claude Code에서 처리한다.
```

```python
# Claude Code 브릿지 (services/llm-advisor/claude_bridge.py)

import subprocess
import json

class ClaudeCodeBridge:
    """
    Claude Code CLI를 subprocess로 호출하여 LLM 분석 수행.
    Max 플랜이므로 토큰 제한 걱정 없이 상세 분석 가능.
    """

    def analyze(self, prompt: str, context: dict) -> dict:
        full_prompt = self._build_prompt(prompt, context)

        # Claude Code CLI 호출 (--print 모드로 비대화형 실행)
        result = subprocess.run(
            ["claude", "-p", full_prompt, "--output-format", "json"],
            capture_output=True,
            text=True,
            timeout=120
        )

        return self._parse_response(result.stdout)

    def _build_prompt(self, task: str, context: dict) -> str:
        return f"""
        당신은 비트코인 선물 트레이딩 전문가입니다.
        아래 시장 데이터를 분석하고 JSON으로 응답하세요.

        ## 현재 시장 데이터
        {json.dumps(context, indent=2, default=str)}

        ## 분석 요청
        {task}

        ## 응답 형식 (반드시 JSON)
        {{
            "regime": "ranging|trending_up|trending_down|volatile",
            "confidence": 0.0~1.0,
            "funding_arb_signal": "enter|hold|exit",
            "grid_signal": "start|continue|stop",
            "risk_level": "low|medium|high|critical",
            "reasoning": "분석 근거 요약",
            "suggested_params": {{}}
        }}
        """
```

### 4.8 Dashboard & Public Performance

```
[대시보드 구조]

내부 대시보드 (localhost:3000 — 본인 전용):
├── 실시간 포지션 현황
├── 전략별 PnL (실시간)
├── 시장 레짐 표시
├── Kill Switch 상태
├── 시스템 헬스 (Docker 컨테이너, API 연결)
└── LLM Advisor 최근 분석

공개 퍼포먼스 페이지 (포트 3001 — 유튜브/커뮤니티 공유):
├── 일별/주별/월별 수익률 차트 (10분 딜레이)
├── 누적 수익 곡선
├── 최대 낙폭 (Max Drawdown)
├── Sharpe Ratio (30일 롤링)
├── 전략별 수익 기여도 (이름만, 상세 파라미터는 비공개)
├── 현재 가동 중인 전략 수
└── 시스템 업타임

Grafana (포트 3002):
├── PostgreSQL 데이터소스 연결
├── 거래 실행 지표 (체결률, 슬리피지)
├── 시스템 메트릭 (CPU, RAM, 네트워크)
└── 펀딩비 수취 히스토리
```

```
[신뢰 구축을 위한 투명성 설계]

1. 수익 데이터: PostgreSQL에 모든 거래 기록, 대시보드에서 조작 불가능한 구조
2. 10분 딜레이: 실시간 포지션 노출 방지 (프론트러닝 방어)
3. 손실 표시: 손실일도 숨기지 않고 표시 (이것이 진짜 신뢰)
4. 검증 가능: Bybit 거래 내역 CSV 엑스포트와 대시보드 데이터 일치 증빙
5. 월간 리포트: 자동 생성되는 월간 퍼포먼스 리포트 (LLM이 작성)
```

### 4.9 Telegram Bot

```
[명령어 체계]

관리 명령 (본인만):
/status          — 전체 포지션 + PnL 요약
/emergency_close — 모든 포지션 즉시 청산
/stop [전략명]   — 특정 전략 정지
/start [전략명]  — 특정 전략 시작
/weight [전략명] [%] — 가중치 수동 조절
/report          — 오늘의 수익 리포트 생성

자동 알림:
- 전략 진입/청산 시
- 펀딩비 수취 시 (8시간마다)
- Kill Switch 발동 시 (즉시)
- 시스템 이상 감지 시 (즉시)
- 일일 마감 수익 리포트 (매일 UTC 00:00)
```

---

## 5. 데이터베이스 스키마

```sql
-- 핵심 테이블

CREATE TABLE trades (
    id              BIGSERIAL PRIMARY KEY,
    strategy_id     VARCHAR(50) NOT NULL,
    exchange        VARCHAR(20) NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    side            VARCHAR(10) NOT NULL,      -- buy, sell
    order_type      VARCHAR(10) NOT NULL,      -- limit, market
    quantity        DECIMAL(20, 8) NOT NULL,
    price           DECIMAL(20, 2) NOT NULL,
    fee             DECIMAL(20, 8),
    fee_currency    VARCHAR(10),
    pnl             DECIMAL(20, 8),            -- 실현 손익 (청산 시)
    order_id        VARCHAR(100),
    request_id      VARCHAR(100) UNIQUE,
    status          VARCHAR(20) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    filled_at       TIMESTAMPTZ
);

CREATE TABLE positions (
    id              BIGSERIAL PRIMARY KEY,
    strategy_id     VARCHAR(50) NOT NULL,
    exchange        VARCHAR(20) NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    side            VARCHAR(10) NOT NULL,
    size            DECIMAL(20, 8) NOT NULL,
    entry_price     DECIMAL(20, 2) NOT NULL,
    current_price   DECIMAL(20, 2),
    unrealized_pnl  DECIMAL(20, 8),
    leverage        DECIMAL(5, 2) DEFAULT 1.0,
    opened_at       TIMESTAMPTZ DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    close_reason    VARCHAR(50)                -- "signal", "stop_loss", "kill_switch"
);

CREATE TABLE funding_payments (
    id              BIGSERIAL PRIMARY KEY,
    exchange        VARCHAR(20) NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    funding_rate    DECIMAL(10, 6) NOT NULL,
    payment         DECIMAL(20, 8) NOT NULL,   -- 수취한 펀딩비 (USDT)
    position_size   DECIMAL(20, 8) NOT NULL,
    collected_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE portfolio_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    total_equity    DECIMAL(20, 2) NOT NULL,
    unrealized_pnl  DECIMAL(20, 8),
    realized_pnl    DECIMAL(20, 8),
    drawdown        DECIMAL(10, 6),
    sharpe_30d      DECIMAL(10, 4),
    strategy_weights JSONB,                    -- {"funding_arb": 0.50, ...}
    market_regime   VARCHAR(20),
    snapshot_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE daily_reports (
    id              BIGSERIAL PRIMARY KEY,
    date            DATE UNIQUE NOT NULL,
    starting_equity DECIMAL(20, 2),
    ending_equity   DECIMAL(20, 2),
    daily_pnl       DECIMAL(20, 8),
    daily_return    DECIMAL(10, 6),            -- 일일 수익률 %
    trade_count     INTEGER,
    funding_income  DECIMAL(20, 8),
    grid_income     DECIMAL(20, 8),
    dca_value       DECIMAL(20, 8),
    max_drawdown    DECIMAL(10, 6),
    llm_summary     TEXT                       -- LLM이 생성한 일일 요약
);

-- 인덱스
CREATE INDEX idx_trades_strategy ON trades(strategy_id, created_at);
CREATE INDEX idx_trades_filled ON trades(filled_at);
CREATE INDEX idx_funding_collected ON funding_payments(collected_at);
CREATE INDEX idx_snapshots_time ON portfolio_snapshots(snapshot_at);
```

---

## 6. Redis 채널 설계

```
[Pub/Sub 채널]

market:ohlcv:{exchange}:{symbol}:{tf}     — OHLCV 데이터
market:funding:{exchange}:{symbol}        — 펀딩비 업데이트
market:orderbook:{exchange}:{symbol}      — 오더북 스냅샷
market:regime                              — 시장 레짐 변경

strategy:command:{strategy_id}            — Orchestrator → 전략 명령
strategy:status:{strategy_id}             — 전략 → Orchestrator 상태

order:request                              — 전략 → Execution Engine
order:result:{strategy_id}                — Execution Engine → 전략

alert:telegram                             — 알림 메시지
alert:kill_switch                          — Kill Switch 발동

llm:request                                — LLM 분석 요청
llm:response                               — LLM 분석 결과

[Key-Value 캐시]

cache:position:{exchange}:{symbol}        — 현재 포지션 (JSON)
cache:portfolio_state                      — 포트폴리오 전체 상태
cache:market_regime                        — 현재 시장 레짐
cache:funding_rates                        — 최신 펀딩비 맵
```

---

## 7. 오픈소스 활용 전략

### 7.1 Freqtrade — 백테스트 & 전략 검증

직접 라이브에 사용하지 않고, **백테스트 전용 도구**로 활용한다. Freqtrade의 FreqAI 모듈로 ML 기반 신호를 학습시키고, 검증된 파라미터를 CryptoEngine에 이식한다.

```
활용 방식:
1. Freqtrade로 그리드/평균회귀 전략 백테스트
2. FreqAI로 시장 레짐 분류 모델 훈련 (scikit-learn → 모델 export)
3. 훈련된 모델(.pkl)을 CryptoEngine의 Orchestrator에 로드
4. Walk-Forward 검증으로 과적합 방지
```

### 7.2 Hummingbot — 펀딩비 전략 참고

Hummingbot의 `funding_rate_arb` 전략 코드를 참고하되, 직접 사용하지 않는다. Bybit 연동 부분과 델타 뉴트럴 포지션 관리 로직을 연구하여 자체 구현에 반영.

### 7.3 TradingAgents — LLM 멀티 에이전트 구조 차용

TradingAgents의 멀티 에이전트 토론 구조를 LLM Advisor에 적용한다.

```
[적용할 에이전트 역할]

Technical Analyst: 기술 지표 분석 → 시장 레짐 판별
Sentiment Analyst: 펀딩비 + 롱숏비율 → 시장 심리 판단
Risk Manager: 포트폴리오 리스크 평가 → 가중치 조정 의견

이 3개 역할을 Claude Code에 순차적으로 질의하고
응답을 종합하여 최종 판단을 내린다.
TradingAgents처럼 동시 실행이 아닌 파이프라인 방식 (비용 절약).
```

### 7.4 LLM_trader — RAG 메모리 구조 차용

LLM_trader의 ChromaDB 기반 과거 트레이딩 히스토리 검색을 차용한다.

```
[적용 방식]

1. 매 거래 결과를 ChromaDB에 임베딩하여 저장
2. 새로운 시장 상황 발생 시 유사한 과거 상황 검색
3. 과거에 어떤 전략이 효과적이었는지를 LLM 컨텍스트에 포함
4. 시간이 지날수록 시스템이 자체 경험에서 학습하는 효과

임베딩 모델: sentence-transformers (Claude Code 환경에서 실행)
저장 내용: {시장_지표, 전략_파라미터, 결과_PnL, 시장_레짐}
```

---

## 8. 오픈소스 심층 분석 — 반드시 차용해야 할 검증된 기술

> **2026-04-02 업데이트**: 4개 오픈소스 프로젝트를 코드 레벨까지 분석하여
> CryptoEngine에 누락된 핵심 기능을 식별하고 적용 방안을 추가합니다.

### 8.1 Freqtrade/FreqAI — 차용할 6가지 핵심 기술

#### ① 자기적응형 재학습 (Self-Adaptive Retraining)

FreqAI는 라이브 운영 중에도 백그라운드 스레드에서 모델을 지속적으로 재학습한다. 이것이 없으면 시장 레짐이 변할 때 모델이 과거 패턴에 갇힌다.

```
[CryptoEngine 적용]

현재 설계: Orchestrator가 고정 ADX 임계값으로 레짐 판별
개선: FreqAI 방식의 슬라이딩 윈도우 재학습 도입

구현:
- regime_ml_model.py에 LightGBM 분류기 탑재
- 매 6시간마다 최근 30일 데이터로 재학습 (백그라운드 스레드)
- 학습 완료 시 즉시 Orchestrator에 새 모델 로드
- LightGBM은 CPU에서도 충분히 빠름 (학습 수 초, 추론 ms 단위)
```

#### ② 대규모 피처 엔지니어링 파이프라인

FreqAI는 단순 지표 몇 개가 아니라, 멀티 타임프레임 × 상관 페어 × 시프트 캔들 조합으로 **10,000+ 피처**를 자동 생성한다. 적은 피처로는 시장의 복잡한 상태를 포착할 수 없다.

```
[CryptoEngine 적용]

FreqAI 피처 확장 공식:
피처 수 = 타임프레임 수 × 기본지표 수 × 상관페어 수 × 시프트캔들 수 × 지표기간 수

예시 설정:
- 타임프레임: [5m, 15m, 1h, 4h] = 4
- 기본지표: [RSI, ADX, BB_width, ATR, EMA_ratio, volume_ratio] = 6
- 상관페어: [ETH/USDT, SOL/USDT] = 2 (BTC 외 상관 자산)
- 시프트캔들: [1, 2, 3] = 3 (과거 3캔들 포함)
- 지표기간: [7, 14, 21] = 3

총 피처: 4 × 6 × 2 × 3 × 3 = 432 피처
→ 레짐 분류 모델의 정확도를 크게 향상

적용 위치: services/market-data/feature_engine.py (신규 파일)
```

#### ③ 이상치 탐지 & 비유사성 지수 (Dissimilarity Index)

FreqAI는 현재 시장 상황이 학습 데이터와 너무 다르면 예측을 차단하는 DI(Dissimilarity Index)를 제공한다. 이것이 없으면 모델이 본 적 없는 상황에서 잘못된 신호를 낼 수 있다.

```
[CryptoEngine 적용]

구현:
1. 학습 데이터의 피처 분포 저장 (평균, 표준편차)
2. 새 데이터가 학습 분포에서 N 표준편차 이상 벗어나면 DI 경고
3. DI > 임계값 → Orchestrator에 "uncertain" 레짐 전달
4. "uncertain" 레짐 → 현금 비중 확대, 신규 진입 차단

적용 위치: services/orchestrator/regime_ml_model.py
```

#### ④ 강화학습 모듈 (Reinforcement Learning)

FreqAI는 stable-baselines3 기반 RL 에이전트를 제공한다. 에이전트가 hold/long/short 중 하나를 선택하고, 커스텀 보상 함수로 학습한다.

```
[CryptoEngine 적용 — Phase 3 이후]

목적: 그리드 트레이딩 파라미터(그리드 간격, 범위) 자동 최적화
환경: OpenAI Gym 커스텀 환경 (BTC 1시간봉 데이터)
에이전트: PPO (Proximal Policy Optimization)
보상 함수: Sharpe Ratio 기반 (단순 수익이 아닌 위험조정 수익)
학습: CPU에서 오프라인 학습 → 최적 파라미터를 config에 반영

지금 당장은 불필요. 수동 파라미터로 3개월 운영 후 RL로 자동 최적화 전환.
```

#### ⑤ Walk-Forward 백테스트 자동화

FreqAI의 백테스트는 슬라이딩 윈도우로 학습/검증을 반복하여 과적합을 방지한다.

```
[CryptoEngine 적용]

기존 backtester 서비스에 Walk-Forward 모듈 추가:

for window in sliding_windows(2023-01 ~ 2026-03, train=180일, test=90일):
    model = train(window.train_data)
    result = test(model, window.test_data)
    results.append(result)

aggregate_results = combine(results)
if aggregate_results.sharpe < 1.5:
    alert("전략 과적합 의심 — 파라미터 검토 필요")
```

#### ⑥ LSTM/PyTorch 가격 예측 모듈

FreqAI-LSTM 프로젝트는 LSTM 네트워크로 가격 방향을 예측하고, 동적 가중치 시스템으로 신호 강도를 조절한다.

```
[CryptoEngine 적용 — Phase 2 이후]

목적: 그리드 전략의 가격 범위 설정 보조
LSTM 입력: [BTC가격, ETH가격, S&P500, 달러인덱스, VIX, 금가격, 금리]
LSTM 출력: 향후 24시간 가격 방향 (상승/하락/횡보) + 신뢰도
활용: LSTM 예측이 "강한 추세"면 그리드 정지, "횡보"면 그리드 가동

학습: CPU/Cloud에서 오프라인 학습 (LightGBM 대비 무거우므로 필요 시 클라우드 활용)
적용 위치: services/strategies/grid-trading/lstm_predictor.py (신규)
```

---

### 8.2 Hummingbot — 차용할 4가지 핵심 기술

#### ① Basis Spread 2단계 상태 머신

Hummingbot의 spot_perpetual_arbitrage는 단순 펀딩비만 보지 않는다. 현물-선물 간 가격 괴리(basis spread)의 divergence/convergence를 2단계로 관리한다.

```
[CryptoEngine 적용 — 펀딩비 전략 강화]

기존: 펀딩비 > 0.005% → 진입, 펀딩비 < -0.005% → 청산
개선: 2단계 상태 머신

State 1 (Closed):
  basis_spread > min_divergence(0.3%) → 진입 → State 2

State 2 (Opened):
  8시간마다 펀딩비 수취 (핵심 수익)
  basis_spread < min_convergence(0.1%) → 청산 기회 판단
  basis_spread > max_divergence(1.0%) → 위험 청산

이 방식의 장점:
- 펀딩비뿐 아니라 basis spread도 수익원으로 활용
- 진입/청산 타이밍이 더 정교해짐
- Hummingbot이 실전에서 검증한 로직

적용 위치: services/strategies/funding-arb/basis_spread_sm.py (신규)
```

#### ② 슬리피지 버퍼 시스템

Hummingbot은 현물/선물 양쪽에 별도의 슬리피지 버퍼를 설정한다. CryptoEngine 현재 설계에는 이 개념이 없다.

```
[CryptoEngine 적용]

config/strategies/funding_arb.yaml에 추가:
  slippage:
    spot_buffer_pct: 0.001      # 현물 주문 시 0.1% 버퍼
    perp_buffer_pct: 0.001      # 선물 주문 시 0.1% 버퍼
    max_acceptable_slippage: 0.005  # 0.5% 이상 슬리피지 시 주문 취소

Execution Engine의 safety.py에 슬리피지 체크 로직 추가
```

#### ③ 펀딩 지급 시간 인지 (Funding Payment Timing Awareness)

Hummingbot은 펀딩비 지급 시간을 인지하여, 지급 전에 포지션을 청산하지 않도록 보호한다.

```
[CryptoEngine 적용]

펀딩비 수취 스케줄 (Bybit: 매일 00:00, 08:00, 16:00 UTC):
- 수취 30분 전: 신규 청산 명령 차단 (펀딩비 받고 나서 청산)
- 수취 5분 전: 포지션 크기 확인 (최대 크기로 펀딩비 극대화)
- 수취 직후: 정상 운영 복귀

이것을 무시하면 펀딩비 수취 직전에 청산되어 수익 누락 발생.
적용 위치: services/strategies/funding-arb/funding_tracker.py
```

#### ④ StrategyV2 컴포저블 컨트롤러 패턴

Hummingbot V2는 전략을 작은 컨트롤러 단위로 분해한다. 이 패턴은 CryptoEngine의 BaseStrategy를 더 유연하게 만든다.

```
[CryptoEngine 적용]

BaseStrategy를 컨트롤러 조합으로 확장:

class FundingArbStrategy(BaseStrategy):
    controllers = [
        FundingRateMonitor,       # 펀딩비 감시
        BasisSpreadTracker,       # 괴리율 추적
        DeltaNeutralManager,      # 델타 뉴트럴 유지
        FundingPaymentScheduler,  # 펀딩 시간 관리
    ]

각 컨트롤러는 독립 테스트 가능, 재사용 가능.
새 전략 추가 시 기존 컨트롤러 조합으로 빠르게 구성.
```

---

### 8.3 TradingAgents — 차용할 5가지 핵심 기술

#### ① Bull/Bear 토론 구조 (Dialectical Debate)

TradingAgents의 가장 강력한 차별점. 단순 분석이 아니라 강세/약세 두 관점이 구조화된 토론을 벌여 편향을 제거한다.

```
[CryptoEngine 적용 — LLM Advisor 강화]

기존: Technical → Sentiment → Risk 순차 호출
개선: 분석 → 토론 → 판단 3단계

Stage 1 — 분석 (병렬):
  Technical Analyst: "BTC RSI 72, 볼린저 상단 터치, 과매수"
  Sentiment Analyst: "펀딩비 0.03%, 롱숏비 70:30, 과열"

Stage 2 — 토론 (2라운드):
  Bull Researcher: "기관 유입 지속, ETF 자금 유입 강세"
  Bear Researcher: "과매수 지표 집중, 2025년 Q4 패턴 반복 가능"
  → 2라운드 재반론 후 합의점 도출

Stage 3 — 판단:
  Risk Manager: 토론 결과 + 포트폴리오 상태 → 최종 추천
  5단계 등급: Strong Buy / Buy / Hold / Sell / Strong Sell

Claude Code Max에서 실행 (비용 무료).
매 4시간 1회 → 하루 6회 토론 = 전략 가중치 미세 조정에 활용.
```

#### ② LangGraph 상태 그래프 기반 에이전트 오케스트레이션

TradingAgents는 LangGraph의 StateGraph로 에이전트 간 흐름을 제어한다.

```python
# LLM Advisor에 LangGraph 도입 (services/llm-advisor/agent_graph.py)

from langgraph.graph import StateGraph

class TradingAnalysisState(TypedDict):
    market_data: dict
    technical_report: str
    sentiment_report: str
    bull_argument: str
    bear_argument: str
    debate_conclusion: str
    risk_assessment: str
    final_decision: dict

workflow = StateGraph(TradingAnalysisState)
workflow.add_node("technical_analysis", technical_analyst)
workflow.add_node("sentiment_analysis", sentiment_analyst)
workflow.add_node("bull_research", bull_researcher)
workflow.add_node("bear_research", bear_researcher)
workflow.add_node("debate", debate_moderator)
workflow.add_node("risk_check", risk_manager)
workflow.add_node("decide", final_decision)

# 분석은 병렬, 토론은 순차
workflow.add_edge("technical_analysis", "bull_research")
workflow.add_edge("sentiment_analysis", "bear_research")
workflow.add_edge(["bull_research", "bear_research"], "debate")
workflow.add_edge("debate", "risk_check")
workflow.add_edge("risk_check", "decide")
```

#### ③ 5단계 등급 시스템 + 신뢰도

TradingAgents v0.2.2에서 도입한 5단계 등급(Strong Buy ~ Strong Sell)과 신뢰도 점수.

```
[CryptoEngine 적용]

LLM Advisor 응답 형식 개선:
{
  "rating": "hold",           // strong_buy, buy, hold, sell, strong_sell
  "confidence": 0.78,         // 0.0 ~ 1.0
  "regime": "ranging",
  "funding_arb_action": "maintain",
  "grid_action": "continue",
  "weight_adjustment": {
    "funding_arb": +0.05,     // 현재 대비 조정값
    "grid": -0.03
  },
  "bull_summary": "...",
  "bear_summary": "...",
  "risk_flags": ["high_leverage_market", "funding_rate_declining"]
}

Orchestrator는 confidence < 0.5 일 때 가중치 변경을 무시 (확신 없는 판단 차단).
```

#### ④ Reflection & Memory Update (회고 루프)

TradingAgents는 거래 결과를 회고하여 에이전트의 판단 정확도를 추적하고 개선한다.

```
[CryptoEngine 적용]

매일 UTC 00:00 자동 실행:
1. 오늘 LLM Advisor가 내린 모든 판단을 수집
2. 실제 시장 결과와 비교 (판단 정확도 계산)
3. 정확했던 판단의 컨텍스트를 ChromaDB에 "성공 사례"로 저장
4. 틀렸던 판단의 컨텍스트를 "실패 사례"로 저장 + 이유 분석
5. 다음 판단 시 유사 상황의 성공/실패 사례를 컨텍스트에 포함

→ 시간이 지날수록 LLM의 판단 정확도가 자가 개선
적용 위치: services/llm-advisor/reflection.py (신규)
```

#### ⑤ Claude Code Max 단일 LLM 전략

TradingAgents는 멀티 LLM 프로바이더를 지원하지만, CryptoEngine은 Claude Code Max 하나로 통일한다. 비용이 무료이고 품질이 최고 수준이므로 별도 로컬 모델이 불필요하다.

```
[CryptoEngine 적용]

LLM 처리: Claude Code Max 단일 사용
- 모든 시장 분석, 토론, 회고, 리포트 생성을 Claude Code에서 처리
- Max 플랜의 넉넉한 토큰으로 하루 6회 심층 분석 가능
- 비전(이미지) 분석도 Claude Code에서 직접 처리

에러 대응:
- Claude Code 일시 불가 시 → 분석 스킵, 기존 가중치 유지
- 전략 실행 자체는 LLM 없이도 독립 동작 (LLM은 보조 판단)
- LLM 의존도를 낮게 설계하여 LLM 장애가 트레이딩 중단으로 이어지지 않음
```

---

### 8.4 LLM_trader — 차용할 5가지 핵심 기술

#### ① 시간 감쇠 적응형 메모리 (Temporal Decay Memory)

LLM_trader의 메모리 시스템은 오래된 거래 기록의 가중치를 시간에 따라 감쇠시킨다. 6개월 전의 횡보장 경험이 현재 추세장에서 동일한 가중치를 가지면 안 된다.

```python
# services/llm-advisor/memory/temporal_decay.py (신규)

import math
from datetime import datetime, timedelta

class TemporalDecayMemory:
    """LLM_trader의 시간 감쇠 메모리 시스템 차용"""

    def __init__(self, half_life_days: int = 30):
        self.half_life = half_life_days

    def decay_weight(self, memory_timestamp: datetime) -> float:
        """오래된 메모리일수록 가중치 감소 (반감기 방식)"""
        age_days = (datetime.utcnow() - memory_timestamp).days
        return math.exp(-0.693 * age_days / self.half_life)

    def retrieve_with_decay(self, query_embedding, top_k=10):
        """유사도 × 시간가중치 = 최종 관련성 점수"""
        results = self.chromadb.query(query_embedding, n_results=top_k * 3)
        scored = []
        for r in results:
            similarity = r.similarity
            time_weight = self.decay_weight(r.metadata["timestamp"])
            final_score = similarity * time_weight
            scored.append((r, final_score))
        return sorted(scored, key=lambda x: x[1], reverse=True)[:top_k]
```

#### ② 시맨틱 규칙 자동 생성 (Semantic Rules via Reflection)

LLM_trader는 반복되는 성공/실패 패턴에서 자동으로 규칙을 추출하여 영구 저장한다.

```
[CryptoEngine 적용]

예시 자동 생성 규칙:
- "펀딩비가 0.05% 이상이면서 OI가 급증할 때 진입하면 80% 성공"
- "ADX > 30인 상태에서 그리드를 가동하면 70% 실패"
- "F&G 지수 15 이하에서 DCA 매수한 포지션은 평균 +45% 수익"

구현:
1. 주간 회고 시 ChromaDB에서 최근 50건 거래 검색
2. LLM에게 "반복되는 패턴을 규칙으로 추출하라" 요청
3. 추출된 규칙을 rules.yaml에 저장
4. Orchestrator가 의사결정 시 규칙을 추가 필터로 사용

적용 위치: services/llm-advisor/memory/semantic_rules.py (신규)
```

#### ③ 비전 AI 차트 분석

LLM_trader는 차트 이미지를 LLM의 비전 기능으로 직접 분석한다. 패턴 인식(헤드앤숄더, 쐐기형 등)을 코드가 아닌 시각적으로 수행.

```
[CryptoEngine 적용]

구현:
1. matplotlib으로 BTC 4시간봉 차트 이미지 자동 생성 (캔들 + 볼린저 + 거래량)
2. Claude Code Max의 비전 기능으로 차트 분석 요청
3. "이 차트에서 어떤 패턴이 보이는가? 지지/저항선은 어디인가?"
4. LLM 응답을 구조화 → Orchestrator에 참고 정보로 전달

Claude Code Max는 이미지 입력을 지원하므로 추가 비용 없이 가능.
매 4시간 분석 시 차트 이미지를 같이 전달.
적용 위치: services/llm-advisor/vision_chart.py (신규)
```

#### ④ 하이브리드 검색 (Cosine + BM25 + FAISS)

LLM_trader는 단순 코사인 유사도가 아니라 BM25 키워드 매칭과 결합한 하이브리드 검색을 사용한다. "펀딩비 역전"같은 키워드가 정확히 매칭되어야 하는 경우에 유사도만으로는 부족하다.

```
[CryptoEngine 적용]

검색 파이프라인:
1. Dense 검색: ChromaDB 코사인 유사도 (시맨틱 매칭)
2. Sparse 검색: BM25 키워드 매칭 (정확한 용어 매칭)
3. 최종 점수 = 0.6 × dense_score + 0.4 × bm25_score

이 방식이 단일 검색보다 검색 품질(recall)이 15~25% 높다.
적용 위치: services/llm-advisor/memory/hybrid_retrieval.py (신규)
```

#### ⑤ Brain.py — PnL 기반 유사 상황 검색

LLM_trader의 brain.py는 단순히 "비슷한 시장 상황"을 찾는 것이 아니라, "비슷한 상황에서 수익/손실이 어떠했는지"를 함께 검색한다.

```
[CryptoEngine 적용]

ChromaDB 저장 스키마 확장:
{
  "market_features": [ADX, RSI, BB_width, funding_rate, ...],
  "strategy_used": "funding_arb",
  "action_taken": "enter",
  "pnl_result": +0.015,          // +1.5% 수익
  "success": true,
  "regime": "ranging",
  "timestamp": "2026-03-15T08:00:00Z",
  "context_text": "펀딩비 0.02%, ADX 18, 횡보장 3일째..."
}

검색 시:
→ "현재와 유사한 상황"을 찾되
→ success=true인 사례와 success=false인 사례를 분리
→ LLM에게 "과거 유사 상황에서 성공 6건, 실패 2건" 정보 전달
→ 더 정교한 판단 가능

적용 위치: services/llm-advisor/memory/trade_memory.py (기존 파일 확장)
```

---

### 8.5 통합 차용 맵 — 어디서 무엇을 가져오는가

```
┌─────────────────┬─────────────────────────┬──────────────────────────────┐
│ 오픈소스         │ 차용 기술               │ CryptoEngine 적용 위치       │
├─────────────────┼─────────────────────────┼──────────────────────────────┤
│ Freqtrade       │ 자기적응형 재학습       │ orchestrator/regime_ml_model │
│                 │ 대규모 피처 엔지니어링  │ market-data/feature_engine   │
│                 │ 비유사성 지수 (DI)      │ orchestrator/regime_ml_model │
│                 │ Walk-Forward 백테스트   │ backtester/walk_forward      │
│                 │ RL 모듈 (Phase 3)      │ strategies/grid/rl_optimizer │
│                 │ LSTM 예측 (Phase 2)    │ strategies/grid/lstm_predict │
├─────────────────┼─────────────────────────┼──────────────────────────────┤
│ Hummingbot      │ Basis Spread 상태 머신  │ funding-arb/basis_spread_sm  │
│                 │ 슬리피지 버퍼           │ execution/safety             │
│                 │ 펀딩 시간 인지          │ funding-arb/funding_tracker  │
│                 │ 컴포저블 컨트롤러       │ strategies/base_strategy     │
├─────────────────┼─────────────────────────┼──────────────────────────────┤
│ TradingAgents   │ Bull/Bear 토론 구조     │ llm-advisor/agents/debate    │
│                 │ LangGraph 상태 그래프   │ llm-advisor/agent_graph      │
│                 │ 5단계 등급 + 신뢰도    │ llm-advisor/prompt_templates │
│                 │ 회고 루프 (Reflection)  │ llm-advisor/reflection       │
│                 │ Claude Code 단일 전략   │ llm-advisor/claude_bridge    │
├─────────────────┼─────────────────────────┼──────────────────────────────┤
│ LLM_trader      │ 시간 감쇠 메모리       │ llm-advisor/memory/decay     │
│                 │ 시맨틱 규칙 자동 생성   │ llm-advisor/memory/rules     │
│                 │ 비전 AI 차트 분석       │ llm-advisor/vision_chart     │
│                 │ 하이브리드 검색         │ llm-advisor/memory/hybrid    │
│                 │ PnL 기반 유사상황 검색  │ llm-advisor/memory/trade_mem │
└─────────────────┴─────────────────────────┴──────────────────────────────┘
```

---

> **Part 2에서 계속**: 디렉토리 구조 (업데이트), 개발 순서, 테스트 계획, 배포 체크리스트
