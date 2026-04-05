# CryptoEngine 데이터 흐름 아키텍처

> 비트코인 선물 자동매매 시스템의 전체 데이터 파이프라인 명세.
> Bybit 테스트넷 기반, Docker Compose 환경에서 24/7 무중단 운영.

---

## 1. 시장 데이터 파이프라인 (Market Data Pipeline)

### 1.1 개요

`market-data` 서비스가 Bybit WebSocket V5 Public API로부터 실시간 데이터를 수집하고,
Redis Pub/Sub로 즉시 브로드캐스트하며 PostgreSQL에 영구 저장한다.

```
Bybit WebSocket (V5 Public Linear)
        |
   market-data 서비스
    /         \
Redis Pub/Sub   PostgreSQL
(실시간 전파)    (영구 저장)
```

### 1.2 WebSocket 스트림

| 스트림 | Bybit 토픽 | 갱신 주기 | 설명 |
|--------|-----------|-----------|------|
| Orderbook L1 | `orderbook.1.BTCUSDT` | ~100ms | 호가창 최우선 1호가 (bid/ask) |
| 공개 체결 | `publicTrade.BTCUSDT` | 실시간 | 개별 체결 틱 데이터 |
| Kline 1분봉 | `kline.1.BTCUSDT` | 실시간 | 1분 OHLCV 캔들 |
| Kline 5분봉 | `kline.5.BTCUSDT` | 실시간 | 5분 OHLCV 캔들 |
| Kline 15분봉 | `kline.15.BTCUSDT` | 실시간 | 15분 OHLCV 캔들 |
| Kline 1시간봉 | `kline.60.BTCUSDT` | 실시간 | 1시간 OHLCV 캔들 |
| Kline 4시간봉 | `kline.240.BTCUSDT` | 실시간 | 4시간 OHLCV 캔들 |
| 티커 | `tickers.BTCUSDT` | 실시간 | 마크가, 인덱스가, 펀딩비, OI |

**자동 재접속**: 연결 끊김 시 지수 백오프(1s ~ 120s)로 자동 재접속.

### 1.3 REST 폴링

| 데이터 | 엔드포인트 | 주기 | 설명 |
|--------|-----------|------|------|
| 미결제약정 (OI) | `/v5/market/open-interest` | 60초 | 5분 단위 OI 데이터 |
| 롱/숏 비율 | `/v5/market/account-ratio` | 300초 | 글로벌 매수/매도 비율 |
| 청산 데이터 | `/v5/market/recent-trade` | 120초 | 블록 트레이드(청산) 필터링 |

### 1.4 OHLCV 저장 (ohlcv 테이블)

확정(confirmed)된 캔들만 PostgreSQL `ohlcv` 테이블에 UPSERT 한다.

```sql
CREATE TABLE ohlcv (
    id        BIGSERIAL PRIMARY KEY,
    exchange  TEXT NOT NULL,          -- 'bybit'
    symbol    TEXT NOT NULL,          -- 'BTCUSDT'
    timeframe TEXT NOT NULL,          -- '1m', '5m', '15m', '1h', '4h'
    ts        TIMESTAMPTZ NOT NULL,   -- 캔들 시작 시각 (UTC)
    open      DOUBLE PRECISION NOT NULL,
    high      DOUBLE PRECISION NOT NULL,
    low       DOUBLE PRECISION NOT NULL,
    close     DOUBLE PRECISION NOT NULL,
    volume    DOUBLE PRECISION NOT NULL,
    UNIQUE (exchange, symbol, timeframe, ts)
);
```

- **미확정 캔들**: Redis에만 발행 (실시간 화면 갱신용)
- **확정 캔들**: Redis 발행 + PostgreSQL UPSERT + Redis Hash 캐시 갱신
- **타임프레임**: 1m / 5m / 15m / 1h / 4h (WS 구독), 1d (미포함 -- 백테스트 스크립트에서 별도 수집)

### 1.5 펀딩비 저장 (funding_rates 테이블)

8시간 간격 펀딩비를 `funding_rates` 테이블에 저장한다.
`FundingMonitor`가 Bybit REST API를 60초 간격으로 폴링하고,
CoinGlass API로 다중 거래소 비교 데이터를 300초 간격으로 수집한다.

```sql
CREATE TABLE funding_rates (
    id                BIGSERIAL PRIMARY KEY,
    exchange          TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    rate              DOUBLE PRECISION NOT NULL,
    predicted_rate    DOUBLE PRECISION,
    next_funding_time TIMESTAMPTZ NOT NULL,
    collected_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (exchange, symbol, next_funding_time)
);
```

- **경고 임계값**: 0.05%/8h (~21.9% APR) -- 펀딩비 차익 기회 감지
- **위험 임계값**: 0.10%/8h (~43.8% APR) -- 극단적 펀딩비 경보

### 1.6 Orderbook L2 스냅샷

`orderbook.1.BTCUSDT` 토픽으로 최우선 1호가 스냅샷/델타를 수신한다.
(테스트넷은 depth-25를 지원하지 않아 depth-1 사용)

- Redis 채널: `market:orderbook:bybit:BTCUSDT`
- 페이로드: `{ type: "snapshot"|"delta", bids: [[price, qty]], asks: [[price, qty]], ts }`
- DB 저장 없음 -- 실시간 전파 전용

### 1.7 레짐 감지 (Regime Detection)

`RegimeDetector`가 확정된 5분봉 캔들을 구독하여 시장 레짐을 분류한다.
최소 50개 캔들이 누적되어야 감지가 시작된다.

**사용 지표:**

| 지표 | 설정값 | 용도 |
|------|--------|------|
| ADX (Average Directional Index) | 14기간 | 추세 강도 측정 |
| ATR (Average True Range) | 14기간 | 변동성 측정 |
| BB (Bollinger Bands) | 20기간, 2.0 표준편차 | 밴드 폭으로 횡보/확산 판단 |
| EMA (Exponential Moving Average) | 20기간 | 가격 방향성 판단 |

**분류 로직:**

| 레짐 | 조건 | 설명 |
|------|------|------|
| `volatile` | ATR > 평균ATR x 2.0 | 극단적 변동성 -- 방어 모드 |
| `trending_up` | ADX > 25 AND 종가 > EMA20 | 상승 추세 |
| `trending_down` | ADX > 25 AND 종가 < EMA20 | 하락 추세 |
| `uncertain` | DI(Dissimilarity Index) 초과 | ML 모델 불확실 -- 현금 비중 확대 |

**저장:**
- Redis 발행: `market:regime` 채널
- Redis 캐시: `cache:regime` (Hash)
- PostgreSQL: `market_regimes` 테이블 (regime, confidence, adx, volatility, bb_width)

---

## 2. Redis Pub/Sub 채널 명세

### 2.1 시장 데이터 채널

| 채널 | 발행자 | 구독자 | 페이로드 |
|------|--------|--------|----------|
| `market:ohlcv:{exchange}:{symbol}:{tf}` | market-data | regime-detector | `{ exchange, symbol, timeframe, open, high, low, close, volume, ts, confirmed }` |
| `market:orderbook:{exchange}:{symbol}` | market-data | (실시간 소비자) | `{ exchange, symbol, type, bids, asks, ts }` |
| `market:trades:{exchange}:{symbol}` | market-data | (실시간 소비자) | `{ exchange, symbol, price, quantity, side, ts }` |
| `market:ticker:{exchange}:{symbol}` | market-data | (실시간 소비자) | `{ exchange, symbol, last_price, mark_price, index_price, funding_rate, next_funding_time, open_interest, volume_24h }` |
| `market:funding:{exchange}:{symbol}` | market-data | funding-arb, orchestrator | `{ exchange, symbol, rate, predicted_rate, next_funding_time }` |
| `market:open_interest:{exchange}:{symbol}` | market-data | (분석용) | `{ exchange, symbol, open_interest, ts }` |
| `market:long_short_ratio:{exchange}:{symbol}` | market-data | (분석용) | `{ exchange, symbol, buy_ratio, sell_ratio, ts }` |
| `market:liquidations:{exchange}:{symbol}` | market-data | (분석용) | `{ exchange, symbol, price, qty, side, ts }` |
| `market:regime` | market-data (RegimeDetector) | orchestrator | `{ regime, confidence, adx, volatility, bb_width, detected_at }` |

### 2.2 전략 조율 채널

| 채널 | 발행자 | 구독자 | 페이로드 |
|------|--------|--------|----------|
| `strategy:funding_arb:command` | orchestrator | funding-arb | `AllocationCommand { strategy_id, allocated_capital, weight, regime, max_drawdown, timestamp }` |
| `strategy:dca:command` | orchestrator | adaptive-dca | `AllocationCommand` (동일 구조) |

### 2.3 주문 실행 채널

| 채널 | 발행자 | 구독자 | 페이로드 |
|------|--------|--------|----------|
| `order:request` | 각 전략 (BaseStrategy) | execution-engine | `OrderRequest { request_id, symbol, side, order_type, quantity, price, strategy_id, post_only, reduce_only, leverage }` |
| `order:result` | execution-engine | 모든 전략 | `OrderResult { request_id, order_id, status, filled_qty, filled_price, fee, fee_currency, reason }` |
| `order:result:{strategy_id}` | execution-engine | 해당 전략 | `OrderResult` (전략별 필터링 채널) |

### 2.4 시스템 채널

| 채널 | 발행자 | 구독자 | 페이로드 |
|------|--------|--------|----------|
| `system:kill_switch` | orchestrator | execution-engine, telegram-bot | `{ triggered, reason, timestamp, cooldown_minutes }` |
| `llm:advisory` | llm-advisor | orchestrator | `{ confidence, weight_adjustments, analysis, timestamp }` |
| `system:service_health` | orchestrator (watchdog) | (모니터링 대시보드) | `{ status: "healthy"/"degraded", dead_services: [...], timestamp }` |
| `system:config_reload` | orchestrator (config watcher) | (감사 로그) | `{ section: "kill_switch", changed_keys: [...], new_values: {...}, old_values: {...}, timestamp }` |
| `telegram:notification` | orchestrator (watchdog) | telegram-bot | `{ level: "critical", message: "...", timestamp }` — Dead man's switch 발동 알림 |

### 2.5 Redis 캐시 키 (Pub/Sub 외)

| 키 | 갱신 주기 | TTL | 설명 |
|-----|-----------|-----|------|
| `cache:ohlcv:{exchange}:{symbol}:{tf}` | 캔들마다 | 없음 (Hash) | 최신 OHLCV 값 |
| `cache:funding:{exchange}:{symbol}` | 티커마다 | 없음 (Hash) | 현재 펀딩비 |
| `cache:oi:{exchange}:{symbol}` | 60초 | 없음 (Hash) | 미결제약정 |
| `cache:regime` | 5분봉마다 | 없음 (Hash) | 현재 레짐 |
| `cache:wallet_balance` | 60초 | 300초 | 지갑 잔고 (JSON) |
| `orchestrator:state` | 5분 | 600초 | 오케스트레이터 상태 |
| `orchestrator:kill_switch` | 이벤트 시 | 7200초 | Kill Switch 상태 |
| `heartbeat:execution-engine` | 30초 | 300초 | 실행 엔진 하트비트 (JSON) |
| `heartbeat:market-data` | 30초 | 300초 | 시장 데이터 서비스 하트비트 (JSON) |
| `strategy:status:{strategy_id}` | 매 틱 (5초) | 90초 | 전략 상태 스냅샷 (하트비트 겸용) |
| `system:service_health` | 60초 | 120초 | 전체 서비스 헬스 상태 |
| `strategy:command_last:{strategy_id}` | 명령 수신 시 | - | 마지막 orchestrator 명령 (재연결 시 상태 복구용) |

---

## 3. 주문 실행 흐름 (Order Execution Flow)

### 3.1 전체 흐름

```
전략 (funding-arb / grid / dca)
    |
    | OrderRequest (JSON, Redis Pub/Sub)
    v
order:request 채널
    |
    v
execution-engine (구독 대기)
    |
    | 1. 멱등성 검사 (request_id 중복 확인)
    | 2. 안전 검증 (SafetyGuard.check_order)
    | 3. 주문 실행 (OrderManager.place_order)
    | 4. 결과 발행 + DB 저장
    v
order:result 채널 → 전략으로 반환
order:result:{strategy_id} 채널 → 해당 전략 전용
```

### 3.2 멱등성 보장

- 모든 주문 요청에 고유한 `request_id` 포함 필수
- `ExecutionEngine`이 처리 완료된 `request_id`를 메모리 Set에 보관 (최대 10,000개, 초과 시 최근 5,000개만 유지)
- 중복 `request_id`는 무시하고 `order_duplicate_skipped` 로그 기록
- DB `orders` 테이블의 `request_id` 컬럼에 UNIQUE 제약조건

### 3.3 재시도 로직

| 항목 | 값 |
|------|-----|
| 최대 재시도 | 3회 |
| 백오프 방식 | 지수 백오프 (1s, 2s, 3s) |
| 주문 타임아웃 | 30초/주문 |
| 동시 주문 제한 | Semaphore(5) |

재시도 대상 오류:
- 네트워크 타임아웃 (`asyncio.TimeoutError`)
- 거래소 API 일시 오류 (일반 `Exception`)

### 3.4 안전 검증 (SafetyGuard)

주문 실행 전 7단계 순차 검증을 통과해야 한다:

| 순서 | 검증 항목 | 기본 임계값 | 설명 |
|------|----------|-------------|------|
| 0 | Redis 연결 상태 (fail-closed) | 3회 연속 실패 시 차단 | Redis 불건강 시 모든 주문 차단. 로컬 캐시(TTL 60s)로 임시 폴백 |
| 1 | 최대 주문 크기 | $100,000 명목가 | 단일 주문 명목가 제한 |
| 2 | 레버리지 제한 | 10배 (설정), 2배 (운영 원칙) | 명시적 + 암묵적 레버리지 검사 |
| 3 | 여유 마진 확인 | $50 이상 | 가용 마진 부족 시 차단 |
| 4 | 슬리피지 검증 | 0.1% 경고, 0.5% 차단 | post_only 주문은 검사 제외 |
| 5 | 네트워크 상태 | 30초 이내 응답 | 마지막 API 응답 이후 경과 시간 |
| 6 | API 속도 제한 | 분당 120회, 90%에서 차단 | 거래소 Rate Limit 보호 |

### 3.5 결과 발행 및 저장

- 체결 결과: `order:result` 채널 + `order:result:{strategy_id}` 채널 동시 발행
- DB 업데이트: `orders` 테이블의 `status`, `filled_qty`, `filled_price`, `fee` 갱신
- 거부된 주문: `status = 'rejected'`로 기록, 거부 사유 포함
- 포지션 갱신: 체결 시 `PositionTracker.on_order_fill()` 호출

---

## 4. 포트폴리오 상태 흐름 (Portfolio State Flow)

```
execution-engine
    |
    | 60초마다 지갑 잔고 조회 (Bybit REST API)
    v
Redis: cache:wallet_balance (TTL 300s)
    |
    v
orchestrator (PortfolioMonitor)
    |
    | 5분마다 오케스트레이션 사이클
    | 1. 레짐 조회 (Redis cache:regime 또는 ML 모델)
    | 2. 레짐 기반 가중치 산출 (WeightManager)
    | 3. LLM Advisory 반영 (max 15% 조정)
    | 4. 포트폴리오 평가 (PortfolioMonitor.evaluate)
    | 5. Kill Switch 조건 검사
    | 6. 자본 배분 명령 발행
    v
strategy:*:command 채널 → 각 전략으로 배분
    |
    v
전략들: 배분 자본에 맞춰 포지션 조정
```

### 4.1 Kill Switch 4단계

| 레벨 | 조건 | 동작 |
|------|------|------|
| 일간 | 일일 낙폭 >= 5% | 전 전략 정지, 100% 현금 |
| 주간 | 주간 낙폭 >= 10% | 전 전략 정지, 100% 현금 |
| 월간 | 월간 낙폭 >= 15% | 전 전략 정지, 100% 현금 |
| 쿨다운 | 발동 후 60분 | 모든 거래 중단, 자동 복구 대기 |

Kill Switch 발동 시:
1. 모든 전략에 `weight=0, allocated_capital=0` 전송
2. `system:kill_switch` 채널로 알림 발행
3. `orchestrator:kill_switch` Redis 키에 상태 저장 (TTL 2시간)
4. 쿨다운 타이머 시작

### 4.2 Dead Man's Switch 흐름

```
execution-engine / market-data 서비스
    |
    | 30초마다 Redis setex (TTL=5분)
    v
heartbeat:{service} 키 (e.g., heartbeat:execution-engine)
    |
orchestrator 워치독 (60초마다)
    |
    +-- 키 존재 → 정상 → system:service_health = "healthy"
    |
    +-- 키 없음 → 서비스 다운 감지
         |
         +-- 핵심 서비스(execution-engine) → Kill Switch 발동
         |    → _execute_kill_switch() → 전략 정지 + 포지션 청산
         |    → telegram:notification 채널 발행
         |
         +-- 비핵심 서비스 → system:service_health = "degraded" 발행
```

각 서비스는 하트비트 발행 시 `/tmp/heartbeat_ok` 파일도 touch하며,
Docker healthcheck가 이 파일 존재 여부로 컨테이너 상태를 판단한다.

---

## 5. 모니터링 흐름 (Monitoring Flow)

### 5.1 Prometheus 메트릭

```
각 서비스 (/metrics 엔드포인트)
        |
        v
   Prometheus (스크래핑)
        |
        v
    Grafana (시각화, 포트 3002)
```

- **node-exporter**: 시스템 리소스 (CPU, 메모리, 디스크, 네트워크)
- **redis-exporter**: Redis 메트릭 (연결 수, 메모리 사용량, 키 수, Pub/Sub 채널)

### 5.2 PostgreSQL 직접 연결

Grafana가 PostgreSQL에 직접 쿼리하여 다음 데이터를 시각화:

| 테이블 | 대시보드 용도 |
|--------|-------------|
| `ohlcv` | 가격 차트, 캔들스틱 |
| `funding_rates` | 펀딩비 히스토리 차트 |
| `market_regimes` | 레짐 변화 타임라인 |
| `orders` | 주문 실행 기록 |
| `positions` | 포지션 현황 |
| `portfolio_snapshots` | 자산 추이, 수익률 곡선 |
| `daily_reports` | 일별 PnL 리포트 |
| `kill_switch_events` | Kill Switch 발동 이력 |

### 5.3 알림 파이프라인

```
Redis Pub/Sub (system:kill_switch, market:funding 등)
        |
        v
   telegram-bot (구독)
        |
        | 알림 포맷팅 + 전송
        v
   Telegram 채팅 (사용자 알림)
```

---

## 6. 데이터베이스 스키마 요약

### market-data 서비스 생성 테이블
- `ohlcv` -- OHLCV 캔들 (exchange, symbol, timeframe, ts 복합 유니크)
- `trades` -- 공개 체결 기록
- `funding_rates` -- 펀딩비 (exchange, symbol, next_funding_time 복합 유니크)
- `market_regimes` -- 레짐 감지 결과

### execution-engine 서비스 생성 테이블
- `orders` -- 주문 기록 (request_id 유니크, 인덱스: request_id, status, strategy_id)
- `positions` -- 포지션 (exchange, symbol, side 복합 유니크)

---

## 7. 접속 정보 (개발/테스트)

| 항목 | 주소 |
|------|------|
| Bybit WebSocket (테스트넷) | `wss://stream-testnet.bybit.com/v5/public/linear` |
| Bybit REST (테스트넷) | `https://api-testnet.bybit.com` |
| PostgreSQL | `localhost:5432` (DB: cryptoengine) |
| Redis | `localhost:6379` |
| Grafana | `http://localhost:3002` |
