---
title: CryptoEngine 시스템 아키텍처
tags:
  - architecture
  - system
  - microservice
  - docker
aliases:
  - 아키텍처
  - 시스템 구조
related:
  - "[[api]]"
  - "[[runbook]]"
  - "[[strategies/funding_arb]]"
  - "[[strategies/adaptive_dca]]"
---

# CryptoEngine 시스템 아키텍처

> [!abstract] 개요
> 비트코인 선물 자동매매를 위한 마이크로서비스 기반 시스템.
> 시장 레짐 감지, 전략 오케스트레이션, 리스크 관리, LLM 어드바이저를 통합하여 24/7 무인 운영.

## 시스템 구성도

```
                    ┌─────────────────┐
                    │   Bybit API     │
                    │  (REST + WS)    │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
    ┌─────────▼──────┐ ┌─────▼──────┐ ┌────▼──────────┐
    │  Market Data   │ │ Execution  │ │  Dashboard    │
    │  Collector     │ │  Engine    │ │  (Express.js) │
    └──────┬─────────┘ └─────┬──────┘ └───────────────┘
           │                 │
    ┌──────▼─────────────────▼──────┐
    │          Redis Pub/Sub        │
    │    (이벤트 버스 + 캐시)         │
    └──────┬─────────────────┬──────┘
           │                 │
    ┌──────▼──────┐   ┌──────▼──────┐
    │ Orchestrator│   │  Strategies │
    │  (Core)     │   │  ├ Funding  │
    │  ├ Weight   │   │  └ DCA      │
    │  ├ Regime   │   └─────────────┘
    │  └ Kill SW  │
    └──────┬──────┘
           │
    ┌──────▼──────┐   ┌─────────────┐
    │ LLM Advisor │   │  Telegram   │
    │ (Claude)    │   │  Bot        │
    └─────────────┘   └─────────────┘
           │
    ┌──────▼──────────────────┐
    │     PostgreSQL 16       │
    │  (TimescaleDB 호환)      │
    └─────────────────────────┘
```

> [!note] 서비스 간 통신
> 모든 서비스 간 메시지는 [[api|내부 API 문서]]에 상세 정의되어 있습니다.

## 핵심 서비스

### 1. Market Data Collector (`services/market-data/`)
- **역할**: 실시간 OHLCV, 오더북, 펀딩레이트 수집
- **기능**:
  - WebSocket 스트림으로 실시간 가격 수신
  - 기술적 지표 계산 (ADX, ATR, BB, EMA)
  - 시장 레짐 분류 (ranging, trending_up, trending_down, volatile)
  - Feature Engine: ML 모델용 특성 벡터 생성
- **출력**: Redis pub/sub 채널로 데이터 배포
  - [[api#`market:ohlcv:{exchange}:{symbol}:{timeframe}`|market:ohlcv]] — 캔들 데이터
  - [[api#`market:regime`|market:regime]] — 레짐 분류
  - [[api#`market:funding:{symbol}`|market:funding]] — 펀딩레이트

### 2. Strategy Orchestrator (`services/orchestrator/`)
- **역할**: 전략 가중치 관리 및 자본 배분
- **주기**: 5분마다 평가 사이클 실행
- **기능**:
  - 레짐 기반 가중치 매트릭스 적용
  - EMA 스무딩으로 점진적 가중치 전환
  - 포트폴리오 리스크 평가
  - Kill Switch 조건 검사
  - LLM 어드바이저 가중치 조정 통합

> [!important] 전략 가중치 배분
> 오케스트레이터는 레짐에 따라 3개 전략의 자본 비중을 동적 조정합니다:
> - `volatile` → 현금 비중 증가, 전체 전략 축소

### 3. Execution Engine (`services/execution/`)
- **역할**: 주문 수신, 검증, 실행, 결과 배포
- **기능**:
  - Redis [[api#`order:request`|order:request]] 채널 구독
  - 안전성 검사 (Safety Guard)
  - 동시 주문 제한 (세마포어)
  - 재시도 로직 (최대 3회, 지수 백오프)
  - 멱등성 보장 (request_id 기반)
- **출력**: [[api#`order:result`|order:result]] 채널로 결과 배포, DB 저장

### 4. Trading Strategies

#### 4a. [[strategies/funding_arb|Funding Rate Arbitrage]] (`services/strategies/funding-arb/`)
- 델타 중립 포지션: 현물 매수 + 무기한 선물 매도 (**핵심 전략**)
- **현재 설정**: fa80_lev5_r30 (FA 80% + 레버리지 5x + 재투자 30%)
  - CAGR +34.87% | Sharpe 3.583 | MDD -4.52% (6년 백테스트, Test 12 Stage D2)
- Basis Spread State Machine으로 진입/청산 결정
- 한쪽 체결 복구 로직 (1분 대기, v1.5.0에서 3분 → 1분 단축)
- 재투자: 펀딩비 수익의 30%를 현물 BTC 매수로 자동 재투자

#### 4b. [[strategies/adaptive_dca|Adaptive DCA]] (`services/strategies/adaptive-dca/`)
- Fear & Greed Index 기반 매수량 조절 (**보조 전략**)
- RSI, 이동평균 이탈도, 변동성 기반 멀티플라이어
- 이익 실현 래더 (15%/30%/50%/100% 수준)

### 5. LLM Advisor (`services/llm-advisor/`)
- **역할**: Claude를 활용한 시장 분석 및 가중치 조정 제안
- **주기**: 4시간마다 정기 분석 + 온디맨드 요청
- **기능**:
  - 다단계 분석 그래프 (Agent Graph)
  - 차트 비전 분석
  - 일일 회고 (Daily Reflection)
  - 신뢰도 기반 가중치 조정
- **채널**: [[api#`llm:advisory`|llm:advisory]], [[api#`llm:request`|llm:request]]

### 6. Telegram Bot (`services/telegram-bot/`)
- 실시간 알림 (포지션 변경, Kill Switch, 일일 리포트)
- 수동 명령 (`/status`, `/kill`, `/resume`, `/report`)
- 자세한 명령은 [[runbook#Kill Switch 대응|운영 매뉴얼]] 참조

### 7. Dashboard (`services/dashboard/`)
- Express.js 기반 REST API
- 내부용 (포트 3000) + 외부용 (포트 3001) 분리
- Grafana 연동 (포트 3002)
- API 엔드포인트: [[api#Dashboard REST API|Dashboard API]] 참조

## 데이터 레이어

### PostgreSQL 16
주요 테이블:
- `trades` — 거래 이력
- `positions` — 포지션 상태
- `funding_payments` — 펀딩비 수취 기록 ([[strategies/funding_arb|펀딩비 전략]] 전용)
- `portfolio_snapshots` — 포트폴리오 스냅샷
- `daily_reports` — 일일 리포트 (LLM 요약 포함)
- `ohlcv_history` — OHLCV 캔들 (타임프레임별 보존 정책: 1m 30일 / 5m 90일 / 15m 180일 / 1h 365일 / 4h 730일)
- `funding_rate_history` — 펀딩레이트 히스토리
- `kill_switch_events` — Kill Switch 이벤트
- `llm_judgments` — LLM 판단 기록
- `llm_reports` — LLM 리포트 (migration 002)
- `service_logs` — 전 서비스 구조화 이벤트 로그 (migration 003, DEBUG 7일 / INFO 30일 / WARNING 90일 / ERROR 365일 보존)
- `regime_raw_log` — 5분 캔들별 원시 레짐 감지 결과 (migration 004)
- `regime_transitions` — 확정 레짐 전환 이벤트 (migration 004)
- `strategy_states` — 전략 상태 스냅샷
- `dca_purchases` — DCA 매입 이력

> [!tip] DB 관리
> 백업, 마이그레이션, 성능 최적화 절차는 [[runbook#데이터베이스 관리|운영 매뉴얼]] 참조

### Redis 7
- **Pub/Sub**: 서비스 간 이벤트 전달
  - `market:ohlcv:*`, `market:regime`, `order:request`, `order:result`
- **캐시**: 최신 시세, 레짐, 포트폴리오 상태
- **키-값**: 전략 설정, Kill Switch 상태
- 전체 채널/키 목록: [[api#Redis Pub/Sub 채널|API 문서]] 및 [[api#Redis 캐시 키|캐시 키 목록]]

## 리스크 관리

### Kill Switch 4단계

| 레벨 | 조건 | 동작 |
|------|------|------|
| L1 Strategy | 개별 전략 낙폭 초과 | 해당 전략 중지 |
| L2 Portfolio | 일일 **-5%**, 주간 **-10%**, 월간 **-15%** (`orchestrator.yaml` 기준). Phase 5: 퍼센트 AND 절대값 USD AND 조건 ($10/$20/$30) | 전체 포지션 청산 |
| L3 System | API 오류, 인프라 장애 | 시장가 전량 청산 |
| L4 Manual | 텔레그램 `/kill` 명령 | 즉시 전량 청산, 수동 복구만 허용 |

> [!warning] Kill Switch 대응
> 각 레벨별 대응 절차는 [[runbook#Kill Switch 대응|운영 매뉴얼]]에 상세히 기술되어 있습니다.

### 쿨다운
- Kill Switch 발동 후 기본 4시간 쿨다운
- L4 (수동)는 자동 복구 불가, 운영자 직접 해제 필요
- Kill Switch 이벤트 채널: [[api#`system:kill_switch`|system:kill_switch]]

## 백테스트 시스템 (`services/backtester/`)
- FreqtradeBridge: Freqtrade CLI 또는 내부 이벤트 기반 엔진
- Walk-Forward Analysis: 슬라이딩 윈도우 (훈련 180일 / 테스트 90일)
- Monte Carlo 시뮬레이션: 수익/Sharpe/낙폭 신뢰구간
- HTML/Markdown 리포트 자동 생성
- 스킬셋 29개 (`tests/backtest/`: fa/, regime/, combined/, trend/, stress/, analysis/, optimization/)

## 자동화 서비스
- **pg-backup**: 매일 02:00 KST PostgreSQL 자동 백업 (`pg_dump`), 7일 보존
- **log-retention**: 매일 03:00 KST `service_logs` 보존 정책 실행 (레벨별 자동 삭제)
- **wf-scheduler**: 매월 1일 02:00 KST Walk-Forward 분석 자동 실행, 결과 Telegram 전송

## 배포

### Docker Compose
- 개발: `docker-compose.dev.yml`
- 프로덕션: `docker-compose.yml`
- 모든 서비스는 `restart: always` 설정
- Health check 기반 의존성 관리

> [!tip] 운영 절차
> 시스템 시작/중지, 서비스 재시작 절차는 [[runbook#시스템 시작/중지|운영 매뉴얼]] 참조

### 환경변수
- `.env.example` 참조
- 거래소 API 키, DB 비밀번호, 텔레그램 토큰 등

## 디렉토리 구조

```
cryptoengine/
├── config/                    # YAML 설정 파일
│   ├── strategies/            # 전략별 설정
│   ├── exchanges/             # 거래소 설정
│   └── orchestrator.yaml      # 오케스트레이터 설정
├── shared/                    # 공용 라이브러리
│   ├── models/                # Pydantic 도메인 모델
│   ├── exchange/              # 거래소 커넥터 (ABC + 구현)
│   ├── db/                    # asyncpg 연결 + 리포지토리
│   ├── risk.py                # 리스크 계산 유틸
│   ├── kill_switch.py         # 4단계 Kill Switch (Phase 5: 절대값 AND 조건)
│   ├── redis_client.py        # Redis 클라이언트
│   ├── config_loader.py       # YAML 로더
│   ├── log_events.py          # 이벤트 코드 정의 (95개)
│   ├── log_writer.py          # 비동기 DB 로그 라이터
│   ├── timezone_utils.py      # KST 타임존 유틸리티
│   └── logging_config.py      # structlog 설정 (KST 타임스탬프)
├── services/                  # 마이크로서비스
│   ├── market-data/
│   ├── orchestrator/
│   ├── execution/
│   ├── strategies/
│   ├── llm-advisor/
│   ├── telegram-bot/
│   ├── dashboard/
│   └── backtester/
├── scripts/                   # 운영 스크립트
│   ├── phase5_preflight.py    # Phase 5 진입 전 점검
│   ├── switch_to_mainnet.py   # 메인넷 전환 (9단계)
│   └── switch_to_testnet.py   # 테스트넷 롤백 (6단계)
├── tests/                     # 테스트 스위트
│   ├── unit/
│   ├── integration/
│   └── backtest/
└── docs/                      # 운영 문서
    └── EMERGENCY_MANUAL_CLOSE.md  # 비상 수동 청산 SOP
```

> [!seealso] 관련 문서
> - [[api|내부 API]] — Redis 채널, 메시지 포맷, REST 엔드포인트
> - [[runbook|운영 매뉴얼]] — 시작/중지, 인시던트 대응, Kill Switch
> - [[strategies/funding_arb|펀딩비 차익거래]] — 핵심 전략 상세
> - [[strategies/adaptive_dca|적응형 DCA]] — 보조 전략 상세
> - [[changelog|변경 이력]] — 버전별 변경사항
