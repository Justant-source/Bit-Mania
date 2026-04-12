# CryptoEngine

**비트코인 선물 자동매매 시스템** -- 펀딩비 차익거래 중심, 델타 뉴트럴 전략으로 안정적 수익 추구

> **현재 상태**: Phase 4 (테스트넷 포워드 테스트) 진행 중 | FA 단독 운영 + 현금 50%  
> **목표**: 테스트넷 검증 -> 소액 실전($500) -> 공개 퍼포먼스 대시보드

---

## 시스템 개요

19개 마이크로서비스가 Docker Compose로 구성된 24/7 무중단 트레이딩 시스템.
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
     │  (가중치)  │  │  funding-arb | adaptive-dca    │
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

## 서비스 구성 (19개)

### 트레이딩 핵심

| 서비스 | 설명 |
|--------|------|
| `market-data` | Bybit WebSocket으로 OHLCV, 호가, 펀딩비 실시간 수집. 시장 레짐(trending/ranging/volatile) 감지 |
| `strategy-orchestrator` | 레짐 기반 전략 가중치 동적 조절, 자본 배분, Kill Switch 총괄 |
| `execution-engine` | CCXT 주문 실행, 포지션 추적, 슬리피지/수수료 검증, 레버리지 안전장치. 스탑로스 주문 자동 배치/취소/복구 (`stoploss_manager.py`) |
| `funding-arb` | **핵심 전략** -- 델타 뉴트럴 포지션으로 8시간 주기 펀딩비 수취 (연 15-30% 목표) |
| `adaptive-dca` | 보조 전략 -- Fear & Greed Index 기반 적응형 BTC 적립 |

### 분석 및 알림

| 서비스 | 설명 |
|--------|------|
| `llm-advisor` | Anthropic SDK + LangGraph 기반 시장 분석, 일일 회고 리포트 생성 |
| `telegram-bot` | 트레이딩 알림 발송 (AlertDispatcher: 배치·레이트리밋·dedup), `/emergency_close` 비상 청산 명령 수신 + ACK 확인 |

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
| `backtester` | -- | 히스토리 데이터 기반 전략 백테스트 (프로필 활성화 시 실행). **v2 재건 완료**: Jesse 프레임워크, 실데이터 파이프라인, 버그 수정 |
| `pg-backup` | -- | PostgreSQL 일일 자동 백업 (02:00 KST, 7일 보존) |
| `log-retention` | -- | 매일 03:00 KST service_logs 보존 정책 자동 실행 (DEBUG 7일, INFO 30일, WARNING 90일, ERROR 365일) |
| `wf-scheduler` | -- | 매월 1일 02:00 KST Walk-Forward 분석 자동 실행 + Telegram 결과 전송 |

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
Level 2  일일/주간 한도      일 -5% 도달 시 해당 전략 자동 정지
                            Phase 5 모드: 퍼센트 AND 절대값 USD 둘 다 초과해야 발동
                            (노이즈 오발동 방지 — 일 $10 / 주 $20 / 월 $30 절대 임계값)
Level 3  헬스체크 실패       서비스 무응답 시 전체 마켓 포지션 청산
Level 4  수동 비상 정지      Telegram /emergency_close 또는 make emergency
                            └─ ACK 확인: 오케스트레이터 처리 후 5초 내 확인 메시지 전송
                               미수신 시 ⚠️ 경고 + 최대 3회 재전송
```

상세 내용: [`arch/risk-management.md`](arch/overview/risk-management.md)

---

## 개발 로드맵

| Phase | 상태 | 내용 |
|-------|------|------|
| 0 | 완료 | 환경 설정 -- Docker, PostgreSQL, Redis, Grafana |
| 1 | 완료 | Bybit 테스트넷 API 키 발급 (10,000 USDT) |
| 2 | 완료 | 서비스 기동 + 연결 검증, Redis Pub/Sub 데이터 흐름 확인 |
| 3 | 완료 | 백테스트 -- 6년 히스토리 데이터 (2020-2026), fa80_lev5_r30 채택. **v2 재건 완료** (2026-04-11) |
| 4 | **진행 중** | 테스트넷 포워드 테스트 -- FA 단독 운영, 현금 50% 버퍼 |
| 5 | **준비 완료** | 소액 실전 ($200 USDT, `switch_to_mainnet.py` → BYBIT_TESTNET=false 전환) |
| 6 | 예정 | 공개 퍼포먼스 대시보드 + 유튜브 |

### Phase 3 백테스트 결과 (6년, 2020-2026)

| 전략 | 설정 | Sharpe | CAGR | MDD | 결과 |
|------|------|--------|------|-----|------|
| FA (fa80_lev5_r30) | Lev=5x, FA=80%, Reinv=30% | **3.583** | +34.87% | -4.52% | ✅ 채택 (Test 12 Stage D2, 6년 청산 0회) |
| Grid Trading | — | 전 변형 음수 | — | — | ❌ 가중치 0, 비활성 |
| DCA (graduated) | — | — | — | 42% | ❌ 6년 WFO 실패, 재설계 필요 |

**후보 설정** (보수적 차선책):
- `fa80_lev4_r30`: CAGR +28.56%, Sharpe 3.556
- `fa80_lev5_r50`: CAGR +33.54%, Sharpe 1.867

### 백테스트 v2 재건 (2026-04-11)

10전 10패 근본 원인 진단 후 4-Track 병렬 재건 완료.

| 수정 항목 | 내용 |
|----------|------|
| 실데이터 파이프라인 | Binance Vision·Coinalyze·Fear&Greed·FRED → Parquet |
| Jesse 프레임워크 | `jesse_project/` — FundingArb·MultiFundingRotation 전략 |
| 버그 3개 수정 | 멀티심볼 진입 차단 / HMM+LLM 수수료 오기재 / abs() 부호 제거 |
| 합성 데이터 제거 | 4개 collector의 무음 폴백 → RuntimeError로 대체 |

상세 내용: `services/backtester/jesse_project/README.md`, `tests/backtest/README.md`

---

## 디렉토리 구조

```
cryptoengine/
├── docker-compose.yml        # 19개 서비스 정의
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
│   ├── kill_switch.py        # Kill Switch 공통 로직
│   ├── log_events.py         # 이벤트 코드 정의 (95개)
│   ├── log_writer.py         # 비동기 DB 로그 라이터 (큐 기반)
│   ├── logging_config.py     # structlog 표준 설정 (KST 타임스탬프)
│   ├── timezone_utils.py     # KST 타임존 유틸리티
│   └── risk.py               # 레버리지 검증, 포지션 크기 계산
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
├── scripts/                  # 운영 스크립트
│   ├── phase5_preflight.py   # Phase 5 진입 전 8개 항목 점검
│   ├── switch_to_mainnet.py  # 메인넷 전환 (9단계, 이중 확인)
│   ├── switch_to_testnet.py  # 테스트넷 롤백 (6단계, 백업 복원)
│   └── ...                   # DB 초기화, 데이터 시딩 등
└── docs/                     # 운영 문서
    └── EMERGENCY_MANUAL_CLOSE.md  # 비상 수동 청산 SOP
```

---

## 배포 절차 (포지션 유지)

배포·재시작 시 `funding-arb`는 포지션을 청산하지 않고 Redis에 상태를 저장한 뒤 재시작 후 자동 복구합니다.
**dev/stage 없이 prod 직접 배포 구조이므로 아래 절차를 반드시 따릅니다.**

### 단일 서비스 재빌드

```bash
# 포지션 유지하며 재배포 (service_shutdown → Redis 저장 → 복구)
docker compose up -d --build --no-deps funding-arb

# 복구 확인
docker compose logs --tail=20 funding-arb | grep -E "복구|recovered"
```

### shared/ 변경 시 (모든 서비스 재빌드)

```bash
docker compose build market-data execution-engine funding-arb strategy-orchestrator telegram-bot
docker compose up -d --no-deps market-data execution-engine funding-arb strategy-orchestrator telegram-bot
```

### 주의

| 상황 | 결과 |
|------|------|
| 재시작 (1시간 이내) | 포지션 자동 복구 ✅ |
| 재시작 (1시간 초과) | 복구 불가 → 신규 시작. 거래소 잔여 포지션 수동 확인 필요 |
| `make emergency` | Kill Switch 발동 → 즉시 청산 (의도적) |

---

## 안전 수칙

- **`.env` 파일은 절대 git에 커밋하지 않음**
- **`BYBIT_TESTNET=true`** -- Phase 4 완료 전까지 절대 변경 금지
- **API 키에 출금 권한 없음** (의도적 설계)
- **선물 레버리지 5배 초과 금지** -- 현재 설정 `fa80_lev5_r30` (Test 12 Stage D2 최적값)
- **Kill Switch 로직(`shared/kill_switch.py`)을 절대 약화시키지 않음**
- `shared/` 변경 시 모든 서비스 이미지 재빌드 필요 (위 배포 절차 참조)
