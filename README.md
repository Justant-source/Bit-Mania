# CryptoEngine

비트코인 선물 자동매매 시스템 — 펀딩비 차익거래 중심의 안정적 수익 추구

> **현재 상태**: Bybit 테스트넷 연동 완료, Phase 3 (백테스트) 진행 예정  
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

| 서비스 | 역할 | 포트 |
|--------|------|------|
| `market-data` | Bybit WebSocket 데이터 수집, 레짐 감지, 펀딩비 모니터링 | — |
| `strategy-orchestrator` | 시장 레짐 기반 전략 가중치 조율, Kill Switch 관리 | — |
| `execution-engine` | 주문 실행, 포지션 추적, 안전 검증 | — |
| `funding-arb` | 핵심 전략: 델타 뉴트럴 + 펀딩비 수취 | — |
| `grid-trading` | 보조 전략: 횡보 구간 그리드 | — |
| `adaptive-dca` | 보조 전략: Fear & Greed 기반 BTC 적립 | — |
| `llm-advisor` | Claude Code 기반 시장 분석 및 일일 회고 | — |
| `telegram-bot` | 알림 발송, 비상 청산 명령 수신 | — |
| `dashboard` | 내부 관리 + 공개 퍼포먼스 페이지 | 3000/3001 |
| `grafana` | 실시간 모니터링 대시보드 | 3002 |
| `postgres` | 트레이딩 데이터 영구 저장 | 5432 |
| `redis` | 서비스 간 메시지 브로커 + 캐시 | 6379 |

---

## 핵심 전략: 펀딩비 차익거래 (Funding Rate Arbitrage)

### 개념
- BTC 선물 포지션을 델타 뉴트럴로 유지하면서 8시간마다 발생하는 펀딩비를 수취
- 현물 매수 + 선물 숏 (또는 반대) 으로 방향성 리스크 제거
- 연환산 수익률 목표: 15~30% (펀딩비만으로)

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
cd ~/Data/Bit-Mania/cryptoengine
cp .env.example .env
# .env 편집: DB_PASSWORD, GRAFANA_ADMIN_PASSWORD, BYBIT_API_KEY, BYBIT_API_SECRET
```

### 2. 인프라 기동

```bash
docker compose up -d postgres redis grafana

# DB 스키마 초기화
docker compose exec -T postgres psql -U cryptoengine -d cryptoengine \
  -f /dev/stdin < shared/db/init_schema.sql

# Grafana: http://localhost:3002 (admin / 설정한 비밀번호)
```

### 3. 서비스 빌드 및 기동

```bash
# 전체 빌드
docker compose build

# 핵심 서비스 기동
docker compose up -d market-data execution-engine funding-arb strategy-orchestrator

# 상태 확인
docker compose ps
docker compose logs -f funding-arb
```

### 4. 연결 확인

```bash
# 데이터 수신 확인
docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "SELECT count(*) FROM ohlcv_history;"

# 펀딩비 확인
docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "SELECT * FROM funding_rate_history ORDER BY recorded_at DESC LIMIT 5;"
```

---

## 개발 로드맵

```
Phase 0 ✅  환경 설정 (Docker, DB, 인프라)
Phase 1 ✅  API 키 발급 (Bybit 테스트넷 10,000 USDT)
Phase 2 🔄  서비스 기동 + 연결 검증
Phase 3 ⏳  백테스트 (6개월 데이터, Sharpe ≥ 2.0 목표)
Phase 4 ⏳  테스트넷 포워드 테스트 (2주, 7개 시나리오)
Phase 5 ⏳  소액 실전 ($500, BYBIT_TESTNET=false)
Phase 6 ⏳  공개 대시보드 + 유튜브
```

### Phase 3 백테스트 기준

| 지표 | 통과 기준 |
|------|-----------|
| Sharpe Ratio | ≥ 2.0 |
| Max Drawdown | ≤ 5% |
| 펀딩비 수취 승률 | ≥ 80% |
| 백테스트 vs 실전 괴리 | ≤ 10% |

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
- 선물 레버리지 **2배 초과 금지**
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
