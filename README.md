# CryptoEngine

**비트코인 선물 자동매매 시스템** -- 펀딩비 차익거래 중심, 델타 뉴트럴 전략으로 안정적 수익 추구

> **현재 상태**: Phase 4 (테스트넷 포워드 테스트) 진행 중 | FA 단독 운영 + 현금 50%  
> **목표**: 테스트넷 검증 -> 소액 실전($500) -> 공개 퍼포먼스 대시보드

---

## 시스템 개요

17개 마이크로서비스가 Docker Compose로 구성된 24/7 무중단 트레이딩 시스템.
Bybit 선물 시장에서 펀딩비 차익거래를 핵심으로, 그리드/DCA를 보조 전략으로 운영합니다.

```
                        ┌─────────────┐
                        │  Bybit API  │
                        └──────┬──────┘
                               │
              ┌────────────────┼────────────────┐
              v                v                v
        ┌───────────┐  ┌──────────────┐  ┌───────────┐
        │market-data│  │  execution   │  │ telegram  │
        │ WebSocket │  │   engine     │  │   bot     │
        └─────┬─────┘  └──────┬───────┘  └───────────┘
              │               │
              v               v
        ┌─────────────────────────────┐
        │        Redis Pub/Sub        │
        └──────┬──────────┬───────────┘
               │          │
     ┌─────────┴──┐  ┌────┴──────────────────────────┐
     │orchestrator│  │          strategies            │
     │  (가중치)  │  │ funding-arb | grid | dca       │
     └────────────┘  └───────────────────────────────-┘
               │          │
        ┌──────┴──────────┴──────┐
        │      PostgreSQL        │──── Grafana / Dashboard
        └────────────────────────┘
```

---

## 아키텍처 문서

상세 설계 문서는 [`arch/`](arch/) 디렉토리를 참조하세요.

| 문서 | 내용 |
|------|------|
| [`system-overview.md`](arch/overview/system-overview.md) | 전체 시스템 아키텍처, 서비스 간 관계 |
| [`data-flow.md`](arch/overview/data-flow.md) | 데이터 흐름: WebSocket -> Redis -> DB |
| [`trading-strategies.md`](arch/overview/trading-strategies.md) | 3개 전략 상세 (펀딩비, 그리드, DCA) |
| [`risk-management.md`](arch/overview/risk-management.md) | Kill Switch 4단계, 레버리지 정책 |
| [`database-schema.md`](arch/overview/database-schema.md) | PostgreSQL 테이블 설계 및 관계도 |
| [`deployment.md`](arch/overview/deployment.md) | Docker 배포, 환경 변수, 운영 가이드 |

---

## 서비스 구성 (16개)

### 트레이딩 핵심

| 서비스 | 설명 |
|--------|------|
| `market-data` | Bybit WebSocket으로 OHLCV, 호가, 펀딩비 실시간 수집. 시장 레짐(trending/ranging/volatile) 감지 |
| `strategy-orchestrator` | 레짐 기반 전략 가중치 동적 조절, 자본 배분, Kill Switch 총괄 |
| `execution-engine` | CCXT 주문 실행, 포지션 추적, 슬리피지/수수료 검증, 레버리지 안전장치 |
| `funding-arb` | **핵심 전략** -- 델타 뉴트럴 포지션으로 8시간 주기 펀딩비 수취 (연 15-30% 목표) |
| `adaptive-dca` | 보조 전략 -- Fear & Greed Index 기반 적응형 BTC 적립 |

### 분석 및 알림

| 서비스 | 설명 |
|--------|------|
| `llm-advisor` | Anthropic SDK + LangGraph 기반 시장 분석, 일일 회고 리포트 생성 |
| `telegram-bot` | 트레이딩 알림 발송, `/emergency_close` 비상 청산 명령 수신 |

### 모니터링 및 인프라

| 서비스 | 포트 | 설명 |
|--------|------|------|
| `dashboard` | 3000 / 3001 | 내부 관리 대시보드(3000) + 공개 퍼포먼스 페이지(3001) |
| `grafana` | 3002 | 실시간 모니터링 대시보드 (Prometheus + PostgreSQL 데이터소스) |
| `prometheus` | 9090 | 메트릭 수집 및 시계열 저장 |
| `node-exporter` | -- | 호스트 시스템 메트릭 (CPU, 메모리, 디스크) |
| `redis-exporter` | -- | Redis 메트릭 (연결 수, 메모리, 명령 처리량) |
| `postgres` | 5432 | 트레이딩 데이터 영구 저장 |
| `redis` | 6379 | 서비스 간 메시지 브로커(Pub/Sub) + 캐시 |
| `backtester` | -- | 히스토리 데이터 기반 전략 백테스트 (프로필 활성화 시 실행) |
| `pg-backup` | -- | PostgreSQL 일일 자동 백업 (02:00 KST, 7일 보존) |

---

## 기술 스택

| 영역 | 기술 | 버전 |
|------|------|------|
| 언어 | Python, TypeScript (dashboard) | 3.12, ES2022 |
| 거래소 | CCXT, Bybit WebSocket | 4.x |
| 데이터베이스 | PostgreSQL, asyncpg | 16, -- |
| 캐시/메시징 | Redis (Pub/Sub) | 7 |
| 비동기 | asyncio, aiohttp | -- |
| 기술 지표 | TA-Lib, pandas, numpy, scikit-learn | -- |
| LLM | Anthropic SDK, LangGraph | claude-sonnet-4-6 |
| 로깅 | structlog (JSON 구조화) | -- |
| 컨테이너 | Docker Compose, python:3.12-slim | 28+ |
| 모니터링 | Grafana, Prometheus | -- |

---

## 빠른 시작

### 사전 요구사항

- Docker Engine 28+ / docker compose plugin
- WSL Ubuntu 24.04 (Windows) 또는 Linux
- Bybit 테스트넷 계정 + API 키 (출금 권한 제외)

### 1. 환경 설정

```bash
cd ~/Data/Bit-Mania/cryptoengine
cp .env.example .env
# .env 편집: BYBIT_API_KEY, BYBIT_API_SECRET, DB_PASSWORD, GRAFANA_ADMIN_PASSWORD
```

### 2. 인프라 기동

```bash
# 인프라 서비스 시작
docker compose up -d postgres redis grafana prometheus

# DB 스키마 초기화
docker compose exec -T postgres psql -U cryptoengine -d cryptoengine \
  -f /dev/stdin < shared/db/init_schema.sql
```

### 3. 트레이딩 서비스 기동

```bash
# 전체 빌드 + 기동
make up

# 또는 핵심 서비스만
docker compose up -d market-data execution-engine funding-arb strategy-orchestrator
```

### 4. 동작 확인

```bash
# 서비스 상태 + 리소스 사용량
make status

# 데이터 수신 확인
docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "SELECT count(*) FROM ohlcv_history;"

# 펀딩비 확인
docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "SELECT * FROM funding_rate_history ORDER BY recorded_at DESC LIMIT 5;"

# 실시간 로그
make logs-funding-arb
```

### 접속 URL

| 서비스 | URL |
|--------|-----|
| 내부 대시보드 | http://localhost:3000 |
| 공개 대시보드 | http://localhost:3001 |
| Grafana | http://localhost:3002 |
| Prometheus | http://localhost:9090 |

---

## Makefile 명령어

`cryptoengine/` 디렉토리에서 실행합니다.

| 명령 | 설명 |
|------|------|
| `make up` | 전체 서비스 빌드 + 기동 |
| `make up-dev` | 개발 모드 (hot reload) 기동 |
| `make down` | 전체 서비스 중지 |
| `make down-clean` | 전체 중지 + 볼륨 삭제 (**데이터 삭제됨**) |
| `make restart` | 전체 서비스 재시작 |
| `make logs` | 전체 서비스 로그 tail |
| `make logs-<서비스명>` | 특정 서비스 로그 (예: `make logs-funding-arb`) |
| `make status` | 컨테이너 상태 + 리소스 사용량 |
| `make test` | 전체 테스트 스위트 실행 |
| `make test-unit` | 유닛 테스트만 실행 |
| `make backtest` | 백테스터 실행 (히스토리 데이터 기반) |
| `make migrate` | DB 마이그레이션 (Alembic) |
| `make monthly-report` | 월간 성과 리포트 생성 |
| `make backup` | PostgreSQL 수동 백업 실행 |
| `make backup-list` | 백업 파일 목록 확인 |
| `make backup-restore` | 백업으로 DB 복원 |
| `make resilience-test` | 전체 복원력 테스트 (kill+restart 자동화) |
| `make resilience-test-market-data` | market-data 복원력 테스트 |
| `make resilience-test-execution` | execution-engine 복원력 테스트 |
| `make emergency` | **비상 청산** -- 전 포지션 즉시 청산 + 전략 정지 |

---

## 리스크 관리: Kill Switch 4단계

```
Level 1  개별 전략 손절      전략 자체 로직으로 포지션 축소
Level 2  일일/주간 한도      일 -1% 또는 주 -3% 도달 시 해당 전략 자동 정지
Level 3  헬스체크 실패       서비스 무응답 시 전체 마켓 포지션 청산
Level 4  수동 비상 정지      Telegram /emergency_close 또는 make emergency
```

상세 내용: [`arch/risk-management.md`](arch/overview/risk-management.md)

---

## 개발 로드맵

| Phase | 상태 | 내용 |
|-------|------|------|
| 0 | 완료 | 환경 설정 -- Docker, PostgreSQL, Redis, Grafana |
| 1 | 완료 | Bybit 테스트넷 API 키 발급 (10,000 USDT) |
| 2 | 완료 | 서비스 기동 + 연결 검증, Redis Pub/Sub 데이터 흐름 확인 |
| 3 | 완료 | 백테스트 -- 6년 히스토리 데이터 (2020-2026), FA Sharpe 1.49 달성 |
| 4 | **진행 중** | 테스트넷 포워드 테스트 -- FA 단독 운영, 현금 50% 버퍼 |
| 5 | 예정 | 소액 실전 ($500, BYBIT_TESTNET=false 전환) |
| 6 | 예정 | 공개 퍼포먼스 대시보드 + 유튜브 |

### Phase 3 백테스트 결과 (6년, 2020-2026)

| 전략 | Sharpe | 수익률 | MDD | 결과 |
|------|--------|--------|-----|------|
| FA (short_hold, max_hold=168h) | **1.494** | +59.24% | 0.94% | ✅ 채택 |
| Grid Trading | 전 변형 음수 | — | — | ❌ 가중치 0, 비활성 |
| DCA (graduated) | — | — | 42% | ❌ 6년 WFO 실패, 재설계 필요 |

**FA Walk-Forward**: 22 윈도우, OOS 평균 Sharpe 1.348  
**채택 파라미터**: `min_rate=0.0001`, `consecutive=3`, `max_hold=168h`

---

## 디렉토리 구조

```
cryptoengine/
├── docker-compose.yml        # 17개 서비스 정의
├── Makefile                  # 운영 명령어
├── .env                      # 환경 변수 (git 제외)
├── config/
│   ├── strategies/           # 전략별 파라미터 YAML
│   ├── orchestrator.yaml     # 레짐별 가중치, Kill Switch 임계값
│   └── grafana/              # Grafana 프로비저닝
├── shared/                   # 전 서비스 공유 라이브러리
│   ├── models/               # 도메인 모델 (Order, Position, Strategy)
│   ├── exchange/             # Bybit CCXT 래퍼
│   ├── db/                   # asyncpg 풀, Repository 패턴
│   ├── redis_client.py       # Redis Pub/Sub 헬퍼
│   ├── config_loader.py      # YAML 설정 로더
│   └── kill_switch.py        # Kill Switch 공통 로직
├── services/
│   ├── market-data/          # 시장 데이터 수집
│   ├── orchestrator/         # 전략 조율
│   ├── execution/            # 주문 실행
│   ├── strategies/           # funding-arb, adaptive-dca
│   ├── llm-advisor/          # LLM 시장 분석
│   ├── telegram-bot/         # 알림 + 비상 명령
│   ├── dashboard/            # 웹 대시보드 (TypeScript)
│   └── backtester/           # 백테스트 엔진
├── arch/                     # 아키텍처 설계 문서
├── tests/                    # 테스트
└── scripts/                  # DB 초기화, 데이터 시딩
```

---

## 안전 수칙

- **`.env` 파일은 절대 git에 커밋하지 않음**
- **`BYBIT_TESTNET=true`** -- Phase 4 완료 전까지 절대 변경 금지
- **API 키에 출금 권한 없음** (의도적 설계)
- **선물 레버리지 5배 초과 금지** -- 현재 설정 `fa80_lev5_r30` (Test 12 Stage D2 최적값)
- **Kill Switch 로직(`shared/kill_switch.py`)을 절대 약화시키지 않음**
- `shared/` 변경 시 모든 서비스 이미지 재빌드 필요 (`make up`)
