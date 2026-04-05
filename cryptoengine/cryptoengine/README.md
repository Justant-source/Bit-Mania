# CryptoEngine

Bybit 테스트넷 기반 비트코인 선물 자동매매 시스템

- **핵심 전략**: 델타 뉴트럴 펀딩비 차익거래 (선물 숏 + 현물 롱)
- **보조 전략**: 그리드 트레이딩, 적응형 DCA
- **인프라**: Docker Compose 기반 12개 마이크로서비스

> **현재 상태**: Phase 4 테스트넷 포워드 테스트 진행 중  
> **목표**: 테스트넷 검증 → 소액($500) 실전 → 공개 퍼포먼스 대시보드

---

## 시스템 아키텍처

```
┌──────────────────────────────────────────────────────────────────┐
│                      DOCKER COMPOSE STACK                        │
│                                                                  │
│  market-data ──→ Redis ──→ strategy-orchestrator                 │
│       ↓           ↕              ↓                               │
│   PostgreSQL   pub/sub    execution-engine ──→ Bybit API         │
│       ↑           ↕              ↑                               │
│   grafana     llm-advisor   funding-arb                          │
│   dashboard   telegram-bot  grid-trading                         │
│                             adaptive-dca                         │
└──────────────────────────────────────────────────────────────────┘
```

### 서비스 구성

```
postgres, redis, grafana        — 인프라
market-data                     — WebSocket 데이터 수집 + 레짐 감지
execution-engine                — 주문 실행 + 포지션 관리
strategy-orchestrator           — 전략 조율 + 자본 배분
funding-arb                     — 핵심: 델타 뉴트럴 펀딩비 차익
grid-trading                    — 보조: EMA 필터 그리드
adaptive-dca                    — 보조: Fear&Greed DCA
llm-advisor                     — Claude 기반 시장 분석
telegram-bot                    — 알림 + 비상 명령
dashboard                       — 내부/공개 대시보드
backtester                      — 백테스트 엔진
```

| 서비스 | 역할 | 포트 |
|--------|------|------|
| `market-data` | Bybit WebSocket 데이터 수집, 레짐 감지, 펀딩비 모니터링 | — |
| `strategy-orchestrator` | 시장 레짐 기반 전략 가중치 조율, Kill Switch 관리 | — |
| `execution-engine` | 주문 실행, 포지션 추적, 안전 검증 | — |
| `funding-arb` | 핵심 전략: 델타 뉴트럴 + 펀딩비 수취 | — |
| `grid-trading` | 보조 전략: EMA 필터 그리드 | — |
| `adaptive-dca` | 보조 전략: Fear & Greed 기반 BTC 적립 | — |
| `llm-advisor` | Claude 기반 시장 분석 및 일일 회고 | — |
| `telegram-bot` | 알림 발송, 비상 청산 명령 수신 | — |
| `dashboard` | 내부 관리 + 공개 퍼포먼스 페이지 | 3000/3001 |
| `grafana` | 실시간 모니터링 대시보드 | 3002 |
| `postgres` | 트레이딩 데이터 영구 저장 | 5432 |
| `redis` | 서비스 간 메시지 브로커 + 캐시 | 6379 |

---

## 핵심 전략: 펀딩비 차익거래 (Funding Rate Arbitrage)

### 현재 설정 (fa80_lev5_r30)
- **FA 자본 비율**: 80% | **레버리지**: 5x | **재투자 비율**: 30%
- **백테스트** (2020-04-01 ~ 2026-03-31): CAGR **+34.87%** | Sharpe **3.583** | MDD **-4.52%** | 청산 **0회**

### 개념
- BTC 선물 포지션을 델타 뉴트럴로 유지하면서 8시간마다 발생하는 펀딩비를 수취
- 현물 매수 + 선물 숏 5x 레버리지로 방향성 리스크 제거 + 펀딩비 수입 극대화
- 연환산 수익률 목표: **30~35%** (fa80_lev5_r30 기준)

### 진입 조건
- 펀딩비 > 0.01% (연환산 ~11%)
- 베이시스 스프레드 허용 범위 내
- 포트폴리오 손실 한도 미도달

### 리스크 관리
```
Level 1: 개별 전략 손절 (전략 자체 로직)
Level 2: 일일 -1%, 주간 -3% → 해당 전략 자동 정지
Level 3: 헬스체크 실패 → 전체 마켓 청산
Level 4: Telegram /emergency_close → 즉시 전체 청산
```

---

## 빠른 시작

### 사전 요구사항
- Docker Engine 28+, docker compose plugin
- WSL Ubuntu 24.04 (Windows 환경)
- Bybit 테스트넷 계정 + API 키

### 1. 환경 설정

```bash
cp .env.example .env
# .env 편집: API 키, DB 비밀번호 입력
```

### 2. 인프라 기동

```bash
docker compose up -d postgres redis grafana
```

### 3. 전체 서비스 기동

```bash
docker compose up -d --build
```

### 4. 상태 확인

```bash
docker compose ps
```

### 5. 백테스트 실행

```bash
docker compose --profile backtest build backtester
docker compose --profile backtest run --rm backtester \
  python scripts/seed_historical.py --symbol BTCUSDT --timeframes 1m,5m,15m,1h,4h --start 2025-10-01 --end 2026-04-01
docker compose --profile backtest run --rm backtester \
  python tests/backtest/bt_funding_arb.py --start 2025-10-01 --end 2026-04-01
```

---

## 백테스트 결과 (2025-10-01 ~ 2026-04-01)

| 전략 | 수익률 | Sharpe | MDD |
|------|--------|--------|-----|
| 펀딩비 차익 (델타 뉴트럴) | +40.9% | 54.81 | -0.14% |
| 그리드 (EMA 필터) | +12,986% | 8.60 | -23.87% |
| 복합 전략 | +327.7% | 9.63 | -6.2% |

> 테스트넷 데이터 기반. 실전 성능은 다를 수 있음.

---

## 모니터링

- **Grafana**: http://localhost:3002
- **Dashboard**: http://localhost:3000 (내부), http://localhost:3001 (공개)
- **Telegram 봇**: `/status`, `/report`, `/emergency_close`

---

## Phase 진행 상태

- [x] Phase 0: 환경 설정
- [x] Phase 1: API 키 발급
- [x] Phase 2: 서비스 기동 + 연결 테스트
- [x] Phase 3: 백테스트
- [x] Phase 4: 테스트넷 포워드 테스트 (진행 중)
- [ ] Phase 5: 소액 실전
- [ ] Phase 6: 공개

---

## 안전 장치

- **Kill Switch 4단계** (전략 → Orchestrator → Execution → Telegram)
- **BYBIT_TESTNET=true** (Phase 5 전까지 절대 변경 금지)
- **max_leverage: 5** (하드 리밋 — fa80_lev5_r30)
- **출금 권한 없는 API 키**

---

## 주요 명령어

```bash
# 전체 재시작
docker compose down && docker compose up -d

# 특정 서비스만 재빌드
docker compose up -d --build --no-deps funding-arb

# DB 직접 쿼리
docker compose exec postgres psql -U cryptoengine -d cryptoengine

# 최근 거래 확인
# SELECT * FROM trades ORDER BY created_at DESC LIMIT 10;

# 포지션 확인
# SELECT * FROM positions WHERE closed_at IS NULL;

# Kill Switch 이력
# SELECT * FROM kill_switch_events ORDER BY triggered_at DESC LIMIT 5;

# 비상 청산
make emergency
```

---

## 디렉토리 구조

```
cryptoengine/
├── config/           # 전략 파라미터, 거래소 설정, Grafana 프로비저닝
├── shared/           # 공유 라이브러리 (models, exchange, db, redis)
├── services/         # 마이크로서비스 (각 서비스 독립 Docker 이미지)
│   ├── market-data/
│   ├── orchestrator/
│   ├── execution/
│   ├── strategies/   # funding-arb, grid-trading, adaptive-dca
│   ├── llm-advisor/
│   ├── telegram-bot/
│   ├── dashboard/
│   └── backtester/
├── tests/            # unit / integration / backtest
├── scripts/          # DB 초기화, 데이터 시딩, 리포트 생성
└── docs/             # 아키텍처 문서, 전략 설명, 운영 매뉴얼
```

---

## 주의사항

- `.env` 파일은 절대 git에 커밋하지 않음
- `BYBIT_TESTNET=true` — Phase 4 완료 전까지 절대 변경 금지
- API 키에 **출금 권한 없음** (의도적 설계)
- 선물 레버리지 **5배 초과 금지** (현재 설정: fa80_lev5_r30)
- Kill Switch 로직(`shared/kill_switch.py`) 절대 약화 금지

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 언어 | Python 3.12, TypeScript (dashboard) |
| 거래소 연동 | CCXT 4.x, Bybit WebSocket |
| 데이터 | PostgreSQL 16, Redis 7, asyncpg |
| 기술 지표 | TA-Lib, pandas, numpy, scikit-learn |
| 비동기 | asyncio, aiohttp |
| 로깅 | structlog (JSON 구조화) |
| 컨테이너 | Docker Compose, python:3.12-slim |
| 모니터링 | Grafana, PostgreSQL datasource |
| LLM | Claude Code (claude-sonnet-4-6) |
