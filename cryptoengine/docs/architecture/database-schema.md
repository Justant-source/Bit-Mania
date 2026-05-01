---
title: CryptoEngine 데이터베이스 스키마
category: architecture
related_code:
  - cryptoengine/shared/db/
  - cryptoengine/shared/db/migrations/
last_updated: 2026-05-01
when_to_update: |
  - 새로운 테이블 추가 시
  - 테이블 컬럼 변경 시
  - 인덱스 전략 수정 시
  - 마이그레이션 파일 생성 시
---

# CryptoEngine 데이터베이스 스키마 문서

## 1. 데이터베이스 개요

| 항목 | 내용 |
|------|------|
| DBMS | PostgreSQL 16 (Alpine 이미지) |
| 데이터베이스명 | `cryptoengine` |
| 사용자 | `cryptoengine` |
| 연결 방식 | asyncpg 비동기 커넥션 풀 |
| 풀 설정 | min_size=2, max_size=10, command_timeout=30s |
| 마이그레이션 | Alembic (`shared/db/migrations/`) |
| 저장소 패턴 | Repository 패턴 (`shared/db/repository.py`) |

### 아키텍처 구성

- **connection.py**: 싱글턴 asyncpg 풀 관리 (`create_pool()`, `get_pool()`, `close_pool()`)
- **repository.py**: `_BaseRepo` 기반 비동기 Repository 패턴 — `_fetchrow()`, `_fetch()`, `_execute()` 등 공통 메서드 제공
- **init_schema.sql**: DDL 스크립트 (Docker 초기화 시 실행)
- **migrations/**: Alembic 버전 관리 (001_initial_schema, 002_llm_reports, 003_service_logs, 004_regime_dashboard)

### DSN 구성

```
postgresql://cryptoengine:<DB_PASSWORD>@postgres:5432/cryptoengine
```

환경 변수 `DATABASE_URL`이 설정되지 않으면 기본값 사용.

---

## 2. 테이블 상세

### 2.1 trades (체결 기록)

거래 전략이 생성한 모든 주문의 체결 기록.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `strategy_id` | VARCHAR(50) | NOT NULL | 전략 식별자 (funding_arb, grid 등) |
| `exchange` | VARCHAR(20) | NOT NULL | 거래소 (bybit) |
| `symbol` | VARCHAR(20) | NOT NULL | 심볼 (BTCUSDT) |
| `side` | VARCHAR(10) | NOT NULL | buy / sell |
| `order_type` | VARCHAR(10) | NOT NULL | limit / market |
| `quantity` | DECIMAL(20,8) | NOT NULL | 주문 수량 |
| `price` | DECIMAL(20,2) | NOT NULL | 체결 가격 |
| `fee` | DECIMAL(20,8) | | 수수료 |
| `fee_currency` | VARCHAR(10) | | 수수료 통화 |
| `pnl` | DECIMAL(20,8) | | 실현 손익 (청산 시) |
| `order_id` | VARCHAR(100) | | 거래소 주문 ID |
| `request_id` | VARCHAR(100) | **UNIQUE** | 내부 요청 ID (멱등성 보장) |
| `status` | VARCHAR(20) | NOT NULL | 주문 상태 |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | 생성 시각 |
| `filled_at` | TIMESTAMPTZ | | 체결 시각 |

**인덱스:**
- `idx_trades_strategy` — (strategy_id, created_at)
- `idx_trades_filled` — (filled_at)
- `idx_trades_request_id` — (request_id)

---

### 2.2 positions (포지션)

현재 보유 중이거나 청산 완료된 포지션.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `strategy_id` | VARCHAR(50) | NOT NULL | 전략 식별자 |
| `exchange` | VARCHAR(20) | NOT NULL | 거래소 |
| `symbol` | VARCHAR(20) | NOT NULL | 심볼 |
| `side` | VARCHAR(10) | NOT NULL | long / short |
| `size` | DECIMAL(20,8) | NOT NULL | 포지션 크기 |
| `entry_price` | DECIMAL(20,2) | NOT NULL | 진입 가격 |
| `current_price` | DECIMAL(20,2) | | 현재 가격 |
| `unrealized_pnl` | DECIMAL(20,8) | | 미실현 손익 |
| `leverage` | DECIMAL(5,2) | DEFAULT 1.0 | 레버리지 배수 |
| `opened_at` | TIMESTAMPTZ | DEFAULT NOW() | 진입 시각 |
| `closed_at` | TIMESTAMPTZ | | 청산 시각 (NULL=보유중) |
| `close_reason` | VARCHAR(50) | | 청산 사유 (signal, stop_loss, kill_switch) |

**인덱스:**
- `idx_positions_strategy` — (strategy_id, opened_at)
- `idx_positions_open` — (strategy_id) WHERE closed_at IS NULL (부분 인덱스)

---

### 2.3 funding_payments (펀딩비 수취 기록)

펀딩비 차익거래 전략이 수취한 펀딩비 내역.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `exchange` | VARCHAR(20) | NOT NULL | 거래소 |
| `symbol` | VARCHAR(20) | NOT NULL | 심볼 |
| `funding_rate` | DECIMAL(10,6) | NOT NULL | 펀딩비율 |
| `payment` | DECIMAL(20,8) | NOT NULL | 수취 금액 (USDT) |
| `position_size` | DECIMAL(20,8) | NOT NULL | 포지션 크기 |
| `collected_at` | TIMESTAMPTZ | NOT NULL | 수취 시각 |

**인덱스:**
- `idx_funding_collected` — (collected_at)
- `idx_funding_exchange_symbol` — (exchange, symbol, collected_at)

---

### 2.4 funding_rate_history (펀딩비 시계열)

거래소에서 수집한 펀딩비율 히스토리.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `exchange` | VARCHAR(20) | NOT NULL | 거래소 |
| `symbol` | VARCHAR(20) | NOT NULL | 심볼 |
| `rate` | DECIMAL(10,6) | NOT NULL | 실제 펀딩비율 |
| `predicted_rate` | DECIMAL(10,6) | | 예측 펀딩비율 |
| `timestamp` | TIMESTAMPTZ | NOT NULL | 타임스탬프 |

**인덱스:**
- `idx_funding_rate_lookup` — (exchange, symbol, timestamp) **UNIQUE** — 중복 삽입 방지

---

### 2.5 ohlcv_history (OHLCV 캔들 데이터)

시장 데이터 서비스가 수집한 캔들스틱 데이터. 백테스트 및 기술적 분석에 사용.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `exchange` | VARCHAR(20) | NOT NULL | 거래소 |
| `symbol` | VARCHAR(20) | NOT NULL | 심볼 |
| `timeframe` | VARCHAR(5) | NOT NULL | 시간 프레임 (1m, 5m, 1h, 1d 등) |
| `open` | DECIMAL(20,2) | NOT NULL | 시가 |
| `high` | DECIMAL(20,2) | NOT NULL | 고가 |
| `low` | DECIMAL(20,2) | NOT NULL | 저가 |
| `close` | DECIMAL(20,2) | NOT NULL | 종가 |
| `volume` | DECIMAL(20,8) | NOT NULL | 거래량 |
| `timestamp` | TIMESTAMPTZ | NOT NULL | 캔들 시작 시각 |

**인덱스:**
- `idx_ohlcv_lookup` — (exchange, symbol, timeframe, timestamp) **UNIQUE** — 중복 캔들 방지

---

### 2.6 portfolio_snapshots (포트폴리오 스냅샷)

주기적으로 기록되는 포트폴리오 상태 스냅샷. Grafana 대시보드의 자산 추이 차트 소스.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `total_equity` | DECIMAL(20,2) | NOT NULL | 총 자산 (USDT) |
| `unrealized_pnl` | DECIMAL(20,8) | | 미실현 손익 |
| `realized_pnl` | DECIMAL(20,8) | | 실현 손익 |
| `drawdown` | DECIMAL(10,6) | | 현재 드로다운 비율 |
| `sharpe_30d` | DECIMAL(10,4) | | 30일 샤프 비율 |
| `strategy_weights` | JSONB | | 전략별 자본 배분 비율 |
| `market_regime` | VARCHAR(20) | | 현재 시장 레짐 |
| `snapshot_at` | TIMESTAMPTZ | DEFAULT NOW() | 스냅샷 시각 |

**인덱스:**
- `idx_snapshots_time` — (snapshot_at)

---

### 2.7 market_regime_history (시장 레짐 이력)

시장 데이터 서비스가 감지한 시장 레짐 (추세/횡보/변동) 변화 이력.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `symbol` | VARCHAR(20) | NOT NULL, DEFAULT 'BTCUSDT' | 심볼 |
| `regime` | VARCHAR(20) | NOT NULL | trending / ranging / volatile |
| `confidence` | DECIMAL(5,3) | | 감지 신뢰도 (0~1) |
| `indicators` | JSONB | | 판단 근거 지표 |
| `detected_at` | TIMESTAMPTZ | DEFAULT NOW() | 감지 시각 |

**인덱스:**
- `idx_regime_history_time` — (detected_at)
- `idx_regime_history_symbol` — (symbol, detected_at)

---

### 2.8 daily_reports (일별 리포트)

일별 수익/지표 집계. 텔레그램 일일 리포트 및 Grafana 일별 차트 소스.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `date` | DATE | NOT NULL, **UNIQUE** | 리포트 날짜 |
| `starting_equity` | DECIMAL(20,2) | | 시작 자산 |
| `ending_equity` | DECIMAL(20,2) | | 종료 자산 |
| `daily_pnl` | DECIMAL(20,8) | | 일일 손익 |
| `daily_return` | DECIMAL(10,6) | | 일일 수익률 (%) |
| `trade_count` | INTEGER | | 거래 건수 |
| `funding_income` | DECIMAL(20,8) | | 펀딩비 수입 |
| `grid_income` | DECIMAL(20,8) | | 그리드 수입 |
| `dca_value` | DECIMAL(20,8) | | DCA 매입 가치 |
| `max_drawdown` | DECIMAL(10,6) | | 최대 드로다운 |
| `llm_summary` | TEXT | | LLM 일일 요약 |

**인덱스:**
- `idx_daily_reports_date` — (date)

---

### 2.9 strategy_states (전략 상태)

각 전략의 현재 실행 상태 및 자본 배분.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `strategy_id` | VARCHAR(50) | NOT NULL, **UNIQUE** | 전략 식별자 |
| `is_running` | BOOLEAN | DEFAULT FALSE | 실행 중 여부 |
| `allocated_capital` | DECIMAL(20,2) | | 배분 자본 (USDT) |
| `current_pnl` | DECIMAL(20,8) | | 현재 누적 손익 |
| `position_count` | INTEGER | DEFAULT 0 | 보유 포지션 수 |
| `config_override` | JSONB | | 설정 오버라이드 |
| `updated_at` | TIMESTAMPTZ | DEFAULT NOW() | 마지막 갱신 |

**인덱스:**
- strategy_id UNIQUE 제약 조건이 자동 인덱스 생성

---

### 2.10 kill_switch_events (킬 스위치 이벤트)

Kill Switch 발동 이력. 4단계 계층 (strategy / portfolio / system / manual) 기록.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `level` | INTEGER | NOT NULL | 레벨 (1: strategy, 2: portfolio, 3: system, 4: manual) |
| `reason` | VARCHAR(200) | NOT NULL | 발동 사유 |
| `positions_closed` | INTEGER | | 청산된 포지션 수 |
| `pnl_at_trigger` | DECIMAL(20,8) | | 발동 시점 손익 |
| `details` | JSONB | | 상세 정보 |
| `triggered_at` | TIMESTAMPTZ | DEFAULT NOW() | 발동 시각 |
| `resolved_at` | TIMESTAMPTZ | | 해제 시각 |

**인덱스:**
- `idx_kill_switch_triggered` — (triggered_at)

---

### 2.11 llm_judgments (LLM 판단 기록)

LLM Advisor가 생성한 시장 판단 기록. 정확도 추적용.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `rating` | VARCHAR(20) | NOT NULL | strong_buy / buy / hold / sell / strong_sell |
| `confidence` | DECIMAL(5,3) | | 신뢰도 (0~1) |
| `regime` | VARCHAR(20) | | 시장 레짐 |
| `reasoning` | TEXT | | 판단 근거 |
| `weight_adjustment` | JSONB | | 전략 가중치 조정 |
| `bull_summary` | TEXT | | 강세 논거 요약 |
| `bear_summary` | TEXT | | 약세 논거 요약 |
| `risk_flags` | JSONB | | 리스크 플래그 |
| `actual_outcome` | VARCHAR(20) | | 실제 결과 (회고용) |
| `accuracy_score` | DECIMAL(5,3) | | 정확도 점수 |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | 생성 시각 |
| `evaluated_at` | TIMESTAMPTZ | | 평가 시각 |

**인덱스:**
- `idx_llm_judgments_created` — (created_at)

---

### 2.12 llm_reports (LLM 분석 리포트)

LLM Advisor의 전체 분석 리포트. 대시보드에서 리스트 및 상세 조회에 사용.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `title` | VARCHAR(200) | NOT NULL | 리포트 제목 |
| `trigger` | VARCHAR(30) | NOT NULL, DEFAULT 'scheduled' | 트리거 유형 (scheduled / on_demand) |
| `rating` | VARCHAR(20) | NOT NULL | 시장 판단 |
| `confidence` | DECIMAL(5,3) | | 신뢰도 |
| `regime` | VARCHAR(20) | | 시장 레짐 |
| `symbol` | VARCHAR(20) | DEFAULT 'BTCUSDT' | 심볼 |
| `btc_price` | DECIMAL(20,2) | | 분석 시점 BTC 가격 |
| `technical_summary` | TEXT | | 기술적 분석 요약 |
| `sentiment_summary` | TEXT | | 시장 심리 요약 |
| `bull_summary` | TEXT | | 강세 논거 |
| `bear_summary` | TEXT | | 약세 논거 |
| `debate_conclusion` | TEXT | | 토론 결론 |
| `risk_assessment` | TEXT | | 리스크 평가 |
| `reasoning` | TEXT | | 종합 판단 근거 |
| `weight_adjustments` | JSONB | | 전략 가중치 조정 추천 |
| `risk_flags` | JSONB | | 리스크 플래그 |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | 생성 시각 |

**인덱스:**
- `idx_llm_reports_created` — (created_at DESC)
- `idx_llm_reports_symbol` — (symbol, created_at DESC)

---

### 2.13 grid_orders (그리드 전략 주문)

그리드 트레이딩 전략이 생성한 개별 그리드 레벨 주문.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `strategy_id` | VARCHAR(50) | NOT NULL | 전략 식별자 |
| `grid_level` | INTEGER | NOT NULL | 그리드 레벨 번호 |
| `side` | VARCHAR(10) | NOT NULL | buy / sell |
| `price` | DECIMAL(20,2) | NOT NULL | 주문 가격 |
| `quantity` | DECIMAL(20,8) | NOT NULL | 주문 수량 |
| `order_id` | VARCHAR(100) | | 거래소 주문 ID |
| `status` | VARCHAR(20) | NOT NULL, DEFAULT 'pending' | 주문 상태 |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | 생성 시각 |
| `filled_at` | TIMESTAMPTZ | | 체결 시각 |

**인덱스:**
- `idx_grid_orders_strategy` — (strategy_id, status)

---

### 2.13b service_logs (서비스 구조화 로그) — migration 003

모든 서비스의 구조화 이벤트 로그. Grafana Service Logs 대시보드 소스.

```sql
CREATE TABLE IF NOT EXISTS service_logs (
    id          BIGSERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service     VARCHAR(50) NOT NULL,
    level       VARCHAR(10) NOT NULL,
    level_no    SMALLINT NOT NULL,          -- 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL
    event       VARCHAR(500) NOT NULL,      -- 이벤트 코드 (SERVICE_STARTED, ORDER_RECEIVED 등)
    message     TEXT,                       -- 사람이 읽을 수 있는 메시지
    context     JSONB,                      -- 추가 컨텍스트 데이터
    trace_id    VARCHAR(36),                -- 분산 추적 ID
    error_type  VARCHAR(200),               -- 예외 타입
    error_stack TEXT                        -- 스택 트레이스
);

CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON service_logs (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_service_level ON service_logs (service, level_no, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_event ON service_logs (event, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_trace ON service_logs (trace_id) WHERE trace_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_logs_errors ON service_logs (service, timestamp DESC) WHERE level_no >= 40;
```

**데이터 보존 정책** (log_retention 서비스가 매일 03:00 KST 실행):
- DEBUG (level_no=10): 7일
- INFO (level_no=20): 30일
- WARNING (level_no=30): 90일
- ERROR/CRITICAL (level_no>=40): 365일

---

### 2.13c regime_raw_log (원시 레짐 로그) — migration 004

market-data 서비스가 매 5분 캔들마다 기록하는 원시 레짐 감지 결과.
분석 목적으로 모든 신호(확정 전)를 저장하여 레짐 감지 알고리즘 성능 평가용.

```sql
CREATE TABLE IF NOT EXISTS regime_raw_log (
    id                BIGSERIAL PRIMARY KEY,
    symbol            VARCHAR(20) NOT NULL,           -- BTCUSDT
    raw_regime        VARCHAR(20) NOT NULL,           -- trending_up/trending_down/ranging/volatile
    confidence        DECIMAL(5,3),                   -- 0.0 ~ 1.0 신뢰도
    adx               DECIMAL(10,4),                  -- ADX 값 (14기간)
    atr               DECIMAL(10,4),                  -- ATR 값 (14기간)
    bb_width          DECIMAL(10,6),                  -- Bollinger Band 폭 (20기간, 2σ)
    is_confirmed      BOOLEAN DEFAULT FALSE,          -- 확정 여부 (3회 연속 동일 신호)
    consecutive_count SMALLINT,                       -- 연속 신호 횟수
    candle_time       TIMESTAMPTZ NOT NULL,           -- 해당 5분 캔들 시작 시각
    recorded_at       TIMESTAMPTZ DEFAULT NOW(),      -- DB 기록 시각
    UNIQUE(symbol, candle_time)
);

CREATE INDEX IF NOT EXISTS idx_regime_raw_symbol_time ON regime_raw_log(symbol, candle_time DESC);
CREATE INDEX IF NOT EXISTS idx_regime_raw_confidence ON regime_raw_log(confidence DESC);
```

---

### 2.13d regime_transitions (확정 레짐 전환) — migration 004

레짐이 3회 연속 동일 신호로 확정 변경될 때만 기록되는 이벤트 로그.

```sql
CREATE TABLE IF NOT EXISTS regime_transitions (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL,             -- BTCUSDT
    previous_regime VARCHAR(20),                      -- 이전 확정 레짐 (NULL: 초기)
    new_regime      VARCHAR(20) NOT NULL,             -- 새로운 확정 레짐
    transition_type VARCHAR(20) DEFAULT 'confirmed',  -- 전환 방식
    confirmed       BOOLEAN DEFAULT TRUE,             -- 항상 TRUE (확정만 저장)
    confidence      DECIMAL(5,3),                     -- 전환 신뢰도
    confirmed_at    TIMESTAMPTZ DEFAULT NOW()         -- 확정 시각
);

CREATE INDEX IF NOT EXISTS idx_regime_transitions_symbol_time ON regime_transitions(symbol, confirmed_at DESC);
CREATE INDEX IF NOT EXISTS idx_regime_transitions_from_to ON regime_transitions(previous_regime, new_regime);
```

**알고리즘 상세** (from regime_detector.py):

레짐 감지는 다음 2단계 프로세스:

1. **원시 신호 (매 5분 캔들)** — `regime_raw_log`에 저장
   - ADX 기반: ADX > 25 → trending_up/down, ADX < 20 + BB_width 좁음 → ranging
   - ATR 기반: ATR > 평균ATR × 2.0 → volatile
   - MIN_CANDLES = 50 (초기 학습 기간)

2. **확정 신호 (3회 연속)** — `regime_transitions`에 저장
   - 동일 신호가 3회 연속 나타나면 확정
   - 확정된 레짐이 변경되면 `regime_transitions`에 이벤트 기록
   - Orchestrator가 이를 구독하여 전략 가중치 조정

---

### 2.14 dca_purchases (DCA 매입 기록)

Fear & Greed 지수 기반 적응형 DCA 전략의 매입 기록.

| 컬럼 | 타입 | 제약 조건 | 설명 |
|------|------|-----------|------|
| `id` | BIGSERIAL | **PK** | 자동 증가 ID |
| `fear_greed_index` | INTEGER | NOT NULL | Fear & Greed 지수 (0~100) |
| `multiplier` | DECIMAL(5,2) | NOT NULL | 매입 배수 |
| `amount_usdt` | DECIMAL(20,2) | NOT NULL | 매입 금액 (USDT) |
| `btc_quantity` | DECIMAL(20,8) | NOT NULL | 매입 BTC 수량 |
| `btc_price` | DECIMAL(20,2) | NOT NULL | 매입 시 BTC 가격 |
| `avg_cost_basis` | DECIMAL(20,2) | | 평균 매입 단가 |
| `total_btc_held` | DECIMAL(20,8) | | 누적 BTC 보유량 |
| `purchased_at` | TIMESTAMPTZ | DEFAULT NOW() | 매입 시각 |

**인덱스:**
- `idx_dca_purchased` — (purchased_at)

---

### 2.15 etf_flow_history (BTC ETF 흐름) — migration 005

BTC 현물 ETF의 일일 유입/유출 데이터. 모멘텀 전략용 매크로 지표.

```sql
CREATE TABLE IF NOT EXISTS etf_flow_history (
    id                  BIGSERIAL PRIMARY KEY,
    date                DATE NOT NULL UNIQUE,         -- 유입 날짜
    total_flow_usd      DECIMAL(20,2) NOT NULL,       -- 총 유입 (USDT)
    ibit_flow_usd       DECIMAL(20,2),                -- iShares Bitcoin Trust 유입
    fbtc_flow_usd       DECIMAL(20,2),                -- Fidelity Bitcoin Trust 유입
    other_flow_usd      DECIMAL(20,2),                -- 기타 ETF 유입
    cumulative_flow_usd DECIMAL(20,2),                -- 누적 유입
    source              VARCHAR(20) NOT NULL,         -- 데이터 출처 (coinglass, glassnode 등)
    collected_at        TIMESTAMPTZ DEFAULT NOW()     -- 수집 시각
);

CREATE INDEX IF NOT EXISTS idx_etf_flow_date ON etf_flow_history(date DESC);
CREATE INDEX IF NOT EXISTS idx_etf_flow_source ON etf_flow_history(source);
```

---

### 2.16 macro_events (거시 경제 이벤트) — migration 005

FOMC, CPI, 고용지표 등 주요 경제 지표 발표 일정.

```sql
CREATE TABLE IF NOT EXISTS macro_events (
    id          BIGSERIAL PRIMARY KEY,
    event_date  DATE NOT NULL,
    event_type  VARCHAR(20) NOT NULL,                 -- FOMC, CPI, NFP, PPI 등
    impact_level INTEGER DEFAULT 3,                   -- 1: low, 2: medium, 3: high
    UNIQUE(event_date, event_type)
);

CREATE INDEX IF NOT EXISTS idx_macro_events_date ON macro_events(event_date DESC);
CREATE INDEX IF NOT EXISTS idx_macro_events_type ON macro_events(event_type);
```

---

### 2.17 quarterly_futures_history (분기물 선물 데이터) — migration 007

BTC 분기물 선물(BTCQUART, BTC_USDTPerp 등) OHLCV 데이터.
선물 스프레드 전략(캘린더 스프레드) 개발용.

```sql
CREATE TABLE IF NOT EXISTS quarterly_futures_history (
    id              BIGSERIAL PRIMARY KEY,
    exchange        VARCHAR(20) NOT NULL,             -- bybit
    symbol          VARCHAR(30) NOT NULL,             -- BTCQUART, BTCDEC25 등
    underlying      VARCHAR(20) NOT NULL,             -- BTC
    expiry_date     DATE NOT NULL,                    -- 만기일
    timestamp       TIMESTAMPTZ NOT NULL,             -- 데이터 시점
    open            DECIMAL(20,2),
    high            DECIMAL(20,2),
    low             DECIMAL(20,2),
    close           DECIMAL(20,2) NOT NULL,
    volume          DECIMAL(20,8),                    -- 거래량 (계약 수)
    open_interest   DECIMAL(20,2),                    -- 미결제약정 (USDT)
    UNIQUE(exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_qf_symbol_time ON quarterly_futures_history(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_qf_expiry ON quarterly_futures_history(expiry_date);
CREATE INDEX IF NOT EXISTS idx_qf_underlying ON quarterly_futures_history(underlying, expiry_date);
```

---

### 2.18 liquidation_history (청산 캐스케이드 데이터) — migration 008

시장 청산 데이터. 청산 캐스케이드 전략 및 시장 공포 지표용.

```sql
CREATE TABLE IF NOT EXISTS liquidation_history (
    id                      BIGSERIAL PRIMARY KEY,
    exchange                VARCHAR(20) NOT NULL,     -- bybit
    symbol                  VARCHAR(20) NOT NULL,     -- BTCUSDT
    timestamp               TIMESTAMPTZ NOT NULL,     -- 청산 발생 시각
    long_liquidations_usd   DECIMAL(20,2),            -- 롱 청산액 (USDT)
    short_liquidations_usd  DECIMAL(20,2),            -- 숏 청산액 (USDT)
    total_liquidations_usd  DECIMAL(20,2),            -- 총 청산액
    oi_change_pct           DECIMAL(10,4),            -- OI 변화율 (%)
    source                  VARCHAR(20) NOT NULL,     -- 데이터 출처
    UNIQUE(exchange, symbol, timestamp, source)
);

CREATE INDEX IF NOT EXISTS idx_liquidation_time ON liquidation_history(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_liquidation_total ON liquidation_history(total_liquidations_usd DESC);
CREATE INDEX IF NOT EXISTS idx_liquidation_symbol ON liquidation_history(symbol, timestamp DESC);
```

---

### 2.19 onchain_metrics (온체인 메트릭) — migration 009

온체인 지표 (MVRV, ASOPR, 거래소 순유입 등) 및 공포탐욕 지수.

```sql
CREATE TABLE IF NOT EXISTS onchain_metrics (
    id                    BIGSERIAL PRIMARY KEY,
    asset                 VARCHAR(10) NOT NULL DEFAULT 'BTC',
    date                  DATE NOT NULL,
    price_usd             DECIMAL(20,2),                -- BTC 가격
    market_cap_usd        DECIMAL(30,2),                -- 시가총액
    realized_cap_usd      DECIMAL(30,2),                -- 실현 시가총액
    mvrv                  DECIMAL(10,4),                -- Market Value / Realized Value
    mvrv_zscore           DECIMAL(10,4),                -- MVRV Z-Score (-2 ~ +2 범위)
    asopr                 DECIMAL(10,4),                -- ASPR (Average Spent Output Price Ratio)
    exchange_netflow_usd  DECIMAL(20,2),                -- 거래소 순유입 (USDT)
    exchange_balance_btc  DECIMAL(20,8),                -- 거래소 총 BTC 보유량
    active_supply_180d    DECIMAL(20,8),                -- 180일 활동 공급량 (BTC)
    source                VARCHAR(20) NOT NULL DEFAULT 'coinmetrics',
    collected_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(asset, date)
);

CREATE INDEX IF NOT EXISTS idx_onchain_date ON onchain_metrics(date DESC);
CREATE INDEX IF NOT EXISTS idx_onchain_asset_date ON onchain_metrics(asset, date DESC);
```

---

### 2.20 fear_greed_history (공포탐욕 지수) — migration 009

Alternative.me / CryptoQuant 등 출처의 일일 Fear & Greed Index.
DCA 전략의 매입 신호 및 분석용.

```sql
CREATE TABLE IF NOT EXISTS fear_greed_history (
    id              BIGSERIAL PRIMARY KEY,
    date            DATE NOT NULL UNIQUE,
    value           INTEGER NOT NULL,                 -- 0~100 점수
    classification  VARCHAR(30),                      -- Extreme Fear, Fear, Neutral, Greed, Extreme Greed
    collected_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fg_date ON fear_greed_history(date DESC);
```

---

### 2.21 macro_indicators (거시경제 지표) — migration 010

FRED API로 수집한 일일 거시경제 지표 (DXY, VIX, US10Y 금리 등).

```sql
CREATE TABLE IF NOT EXISTS macro_indicators (
    id          BIGSERIAL PRIMARY KEY,
    date        DATE NOT NULL,
    indicator   VARCHAR(30) NOT NULL,                 -- DXY, VIX, US10Y, TNX 등
    value       DECIMAL(20,6),                        -- 지표 값
    source      VARCHAR(20) DEFAULT 'fred',
    collected_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(date, indicator)
);

CREATE INDEX IF NOT EXISTS idx_macro_date ON macro_indicators(date DESC, indicator);
CREATE INDEX IF NOT EXISTS idx_macro_indicator ON macro_indicators(indicator, date DESC);
```

---

### 2.22 ohlcv_1m_longterm (장기 1분봉) — migration 011

1분봉의 장기 보존 테이블. 일반 ohlcv_history는 30일 보존이므로,
장기 피쳐 엔지니어링(3년 이상 데이터)을 위해 별도 테이블 운영.

```sql
CREATE TABLE IF NOT EXISTS ohlcv_1m_longterm (
    id          BIGSERIAL PRIMARY KEY,
    exchange    VARCHAR(20) NOT NULL DEFAULT 'bybit',
    symbol      VARCHAR(20) NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,                 -- 1분 캔들 시작 시각
    open        DECIMAL(20,2),
    high        DECIMAL(20,2),
    low         DECIMAL(20,2),
    close       DECIMAL(20,2) NOT NULL,
    volume      DECIMAL(20,8),                        -- 거래량 (BTC)
    UNIQUE(exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_1m_lt_symbol_time ON ohlcv_1m_longterm(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_1m_lt_time ON ohlcv_1m_longterm(timestamp DESC);
```

**보존 정책**: 제한 없음 (3년 이상 축적)

---

## 3. 테이블 관계도 (논리적)

스키마에 명시적 FK는 정의되어 있지 않으나, `strategy_id`를 통한 논리적 관계가 존재:

```
strategy_states.strategy_id
    ├── trades.strategy_id
    ├── positions.strategy_id
    └── grid_orders.strategy_id

portfolio_snapshots.market_regime ←→ market_regime_history.regime

llm_judgments ←→ llm_reports (동일 분석의 요약/상세 버전)

daily_reports.funding_income ← funding_payments 집계
daily_reports.grid_income ← grid_orders 집계
daily_reports.dca_value ← dca_purchases 집계
```

---

## 4. ER 다이어그램

```mermaid
erDiagram
    TRADES ||--o{ POSITIONS : "strategy_id"
    TRADES ||--o{ DAILY_REPORTS : "date"
    POSITIONS ||--o{ STRATEGY_STATES : "strategy_id"
    FUNDING_PAYMENTS ||--o{ DAILY_REPORTS : "date"
    GRID_ORDERS ||--o{ DAILY_REPORTS : "date"
    DCA_PURCHASES ||--o{ DAILY_REPORTS : "date"
    MARKET_REGIME_HISTORY ||--o{ PORTFOLIO_SNAPSHOTS : "regime"
    OHLCV_HISTORY ||--o{ MARKET_REGIME_HISTORY : "symbol"
    FUNDING_RATE_HISTORY ||--o{ FUNDING_PAYMENTS : "rate"
    KILL_SWITCH_EVENTS ||--o{ STRATEGY_STATES : "strategy_id"
    LLM_JUDGMENTS ||--o{ LLM_REPORTS : "similar"

    TRADES {
        bigserial id PK
        varchar strategy_id FK
        varchar exchange
        varchar symbol
        varchar side
        varchar order_type
        decimal quantity
        decimal price
        decimal fee
        varchar request_id UK
        varchar status
        timestamptz created_at
        timestamptz filled_at
    }

    POSITIONS {
        bigserial id PK
        varchar strategy_id FK
        varchar exchange
        varchar symbol
        varchar side
        decimal size
        decimal entry_price
        decimal current_price
        decimal unrealized_pnl
        decimal leverage
        timestamptz opened_at
        timestamptz closed_at
        varchar close_reason
    }

    STRATEGY_STATES {
        bigserial id PK
        varchar strategy_id UK
        boolean is_running
        decimal allocated_capital
        decimal current_pnl
        integer position_count
        jsonb config_override
        timestamptz updated_at
    }

    FUNDING_PAYMENTS {
        bigserial id PK
        varchar exchange
        varchar symbol
        decimal funding_rate
        decimal payment
        decimal position_size
        timestamptz collected_at
    }

    FUNDING_RATE_HISTORY {
        bigserial id PK
        varchar exchange
        varchar symbol
        decimal rate
        decimal predicted_rate
        timestamptz timestamp UK
    }

    OHLCV_HISTORY {
        bigserial id PK
        varchar exchange
        varchar symbol
        varchar timeframe
        decimal open
        decimal high
        decimal low
        decimal close
        decimal volume
        timestamptz timestamp UK
    }

    MARKET_REGIME_HISTORY {
        bigserial id PK
        varchar symbol
        varchar regime
        decimal confidence
        jsonb indicators
        timestamptz detected_at
    }

    PORTFOLIO_SNAPSHOTS {
        bigserial id PK
        decimal total_equity
        decimal unrealized_pnl
        decimal realized_pnl
        decimal drawdown
        decimal sharpe_30d
        jsonb strategy_weights
        varchar market_regime FK
        timestamptz snapshot_at
    }

    DAILY_REPORTS {
        bigserial id PK
        date date UK
        decimal starting_equity
        decimal ending_equity
        decimal daily_pnl
        decimal daily_return
        integer trade_count
        decimal funding_income
        decimal grid_income
        decimal dca_value
        decimal max_drawdown
        text llm_summary
    }

    KILL_SWITCH_EVENTS {
        bigserial id PK
        integer level
        varchar reason
        integer positions_closed
        decimal pnl_at_trigger
        jsonb details
        timestamptz triggered_at
        timestamptz resolved_at
    }

    LLM_JUDGMENTS {
        bigserial id PK
        varchar rating
        decimal confidence
        varchar regime
        text reasoning
        jsonb weight_adjustment
        text bull_summary
        text bear_summary
        jsonb risk_flags
        varchar actual_outcome
        decimal accuracy_score
        timestamptz created_at
        timestamptz evaluated_at
    }

    LLM_REPORTS {
        bigserial id PK
        varchar title
        varchar trigger
        varchar rating
        decimal confidence
        varchar regime
        varchar symbol
        decimal btc_price
        text technical_summary
        text sentiment_summary
        text bull_summary
        text bear_summary
        text debate_conclusion
        text risk_assessment
        text reasoning
        jsonb weight_adjustments
        jsonb risk_flags
        timestamptz created_at
    }

    GRID_ORDERS {
        bigserial id PK
        varchar strategy_id FK
        integer grid_level
        varchar side
        decimal price
        decimal quantity
        varchar order_id
        varchar status
        timestamptz created_at
        timestamptz filled_at
    }

    DCA_PURCHASES {
        bigserial id PK
        integer fear_greed_index
        decimal multiplier
        decimal amount_usdt
        decimal btc_quantity
        decimal btc_price
        decimal avg_cost_basis
        decimal total_btc_held
        timestamptz purchased_at
    }

    SERVICE_LOGS {
        bigserial id PK
        varchar service
        varchar level
        varchar event
        text message
        jsonb data
        timestamptz created_at
    }

    REGIME_RAW_LOG {
        bigserial id PK
        varchar symbol
        varchar raw_regime
        decimal confidence
        jsonb indicators
        timestamptz candle_time
        timestamptz recorded_at
    }

    REGIME_TRANSITIONS {
        bigserial id PK
        varchar symbol
        varchar from_regime
        varchar to_regime
        decimal confidence
        timestamptz transitioned_at
    }
```

---

## 5. 데이터 보존 및 유지보수

### 현재 상태

- 데이터 보존 정책: `scripts/ohlcv_retention.py` — 타임프레임별 자동 삭제 (1m→30일, 5m→90일, 15m→180일, 1h→365일, 4h→730일)
- 파티셔닝: 미적용
- 아카이빙: 미적용
- 백업: `pg-backup` 서비스 — 일일 `pg_dump` (02:00 KST), 7일 보존, `pg-backups` Docker 볼륨

### 테이블별 데이터 증가 예상

| 테이블 | 주기 | 일일 예상 레코드 | 월간 누적 |
|--------|------|-----------------|-----------|
| ohlcv_history | 1분 캔들 기준 | ~1,440 | ~43,200 |
| funding_rate_history | 8시간마다 | ~3 | ~90 |
| portfolio_snapshots | 매시간 | ~24 | ~720 |
| market_regime_history | 레짐 변경 시 | ~5~20 | ~150~600 |
| trades | 전략 활동 시 | 변동적 | 변동적 |
| llm_reports | 6시간마다 | ~4 | ~120 |

---

## 6. 개선 제안

### 6.1 시계열 테이블 파티셔닝

`ohlcv_history`와 `funding_rate_history`는 시간이 지남에 따라 대량 데이터가 누적됨.
PostgreSQL 네이티브 파티셔닝 적용 권장:

```sql
-- 예시: ohlcv_history 월별 파티셔닝
CREATE TABLE ohlcv_history (
    ...
) PARTITION BY RANGE (timestamp);

CREATE TABLE ohlcv_history_2026_01 PARTITION OF ohlcv_history
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

### 6.2 TimescaleDB 확장 검토

시계열 데이터(ohlcv_history, funding_rate_history, portfolio_snapshots)에 대해
TimescaleDB 하이퍼테이블 전환 시 이점:

- 자동 청크 관리 (파티션 수동 생성 불필요)
- 연속 집계 (continuous aggregates) — 실시간 OHLCV 리샘플링
- 데이터 보존 정책 자동화 (`add_retention_policy`)
- 압축 정책으로 스토리지 절감 (10배 이상)

```sql
-- TimescaleDB 적용 예시
SELECT create_hypertable('ohlcv_history', 'timestamp');
SELECT add_retention_policy('ohlcv_history', INTERVAL '1 year');
SELECT add_compression_policy('ohlcv_history', INTERVAL '7 days');
```

### 6.3 아카이빙 전략

| 테이블 | 보존 기간 | 아카이빙 방법 |
|--------|-----------|---------------|
| ohlcv_history | 6개월 (실시간) | 오래된 데이터 → Parquet/CSV 내보내기 |
| funding_rate_history | 1년 | 동일 |
| trades | 영구 | 청산 완료된 오래된 거래 → 아카이브 테이블 |
| portfolio_snapshots | 3개월 (원본) | 오래된 데이터 → 일별 집계로 압축 |
| llm_judgments | 6개월 | 정확도 분석 후 요약만 보존 |

### 6.4 커넥션 풀 사이징 권장

현재 설정: min=2, max=10

서비스별 풀 사용 현황을 고려한 조정:

| 서비스 | 동시 쿼리 | 권장 max_size |
|--------|-----------|---------------|
| market-data | 높음 (OHLCV 삽입) | 8~10 |
| execution-engine | 중간 (체결 기록) | 5~8 |
| funding-arb | 낮음 | 3~5 |
| strategy-orchestrator | 낮음 | 3~5 |
| llm-advisor | 낮음 | 2~3 |
| dashboard | 중간 (읽기 전용) | 5~8 |

총 커넥션 수 상한: PostgreSQL `max_connections` 기본값(100)으로 충분하나,
서비스 수 증가 시 PgBouncer 도입 검토.

### 6.5 FK 제약 조건 추가 검토

현재 스키마에 외래 키가 없어 데이터 정합성을 애플리케이션 레벨에서 관리 중.
`strategy_id` 기반 FK 추가 시 참조 무결성 보장 가능하나,
고빈도 INSERT 성능에 영향을 줄 수 있으므로 트레이드오프 고려 필요.

### 6.6 추가 인덱스 권장

```sql
-- 미청산 포지션의 빠른 전략별 조회 (이미 부분 인덱스 있음 — 양호)
-- trades 테이블 심볼별 조회 (백테스트/분석)
CREATE INDEX idx_trades_symbol ON trades(symbol, filled_at);

-- portfolio_snapshots 시장 레짐별 필터링
CREATE INDEX idx_snapshots_regime ON portfolio_snapshots(market_regime, snapshot_at);
```
