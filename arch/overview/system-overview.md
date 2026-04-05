# CryptoEngine 시스템 아키텍처 개요

## 1. 시스템 소개

CryptoEngine은 Bybit 비트코인 선물 시장을 대상으로 하는 **자동매매 시스템**이다.
핵심 전략은 **펀딩비 차익거래(Funding Rate Arbitrage)**이며, 그리드 트레이딩과 적응형 DCA를 보조 전략으로 운영한다.

전체 시스템은 **Docker Compose** 기반의 마이크로서비스 아키텍처로 구성되어 있으며,
WSL Ubuntu 환경에서 24/7 무중단 운영을 목표로 설계되었다.

---

## 2. 서비스 레이어 구성

총 **16개 Docker Compose 서비스**가 6개 레이어로 분류된다.

### 2.1 Infrastructure (인프라)

| 서비스 | 이미지 | 역할 | 포트 |
|--------|--------|------|------|
| **postgres** | `postgres:16-alpine` | 주 데이터 저장소. 거래 기록, 포지션, 펀딩비 히스토리, OHLCV 등 영구 데이터 보관 | 5432 |
| **redis** | `redis:7-alpine` | 메시지 브로커(Pub/Sub) + 캐시. AOF 영속화, 256MB 메모리 제한 | 6379 |
| **prometheus** | `prom/prometheus:v2.51.0` | 메트릭 수집 및 시계열 저장. 30일 보존 | 9090 |
| **node-exporter** | `prom/node-exporter:v1.8.0` | 호스트 시스템 메트릭(CPU, 메모리, 디스크) 수집 | 9100 (내부) |
| **redis-exporter** | `oliver006/redis_exporter` | Redis 메트릭을 Prometheus 형식으로 노출 | 9121 (내부) |

### 2.2 Core (핵심 서비스)

| 서비스 | 역할 |
|--------|------|
| **market-data** | Bybit WebSocket으로 실시간 시세, 펀딩비, 오더북 수신. OHLCV 캔들 저장. 시장 레짐(trending/ranging/volatile) 감지 후 Redis 발행 |
| **strategy-orchestrator** | 시장 레짐에 따라 전략별 자본 배분 가중치 조정. Kill Switch 4단계 계층 관리. 전략 시작/정지 명령 발행 |
| **execution-engine** | 주문 요청 수신 후 Bybit API로 실행. 포지션 추적, 안전 검증(레버리지 2배 제한), 체결/취소 알림 발행 |

### 2.3 Strategy (전략)

| 서비스 | 유형 | 역할 |
|--------|------|------|
| **funding-arb** | 핵심 전략 | 델타 뉴트럴 포지션 구성 + 펀딩비 수취. 양/음 펀딩비 방향에 따라 롱/숏 진입 |
| **grid-trading** | 보조 전략 | 횡보(ranging) 레짐에서 그리드 주문 배치. 가격 범위 내 자동 매수/매도 |
| **adaptive-dca** | 보조 전략 | Fear & Greed 지수 기반 적응형 분할매수. 시장 공포 시 공격적 매수 |

### 2.4 Intelligence (지능)

| 서비스 | 역할 |
|--------|------|
| **llm-advisor** | Anthropic SDK + LangGraph 기반 시장 분석. 기술적/펀더멘털 데이터를 종합하여 판단 리포트 생성. `llm_reports` 테이블에 결과 저장 |

### 2.5 Interface (인터페이스)

| 서비스 | 역할 | 포트 |
|--------|------|------|
| **telegram-bot** | 실시간 알림 전송 + 비상 명령 수신(`/kill`, `/status`, `/pause_all`, `/resume_all`). Kill Switch 발동 시 즉시 알림. 30분 간격 시스템 하트비트(자본 + 포지션 수) 전송 | - |
| **dashboard** | Express.js 기반 웹 대시보드. 내부용(상세 지표)과 공개용(요약) 분리 | 3000 (내부), 3001 (공개) |
| **grafana** | Grafana 10.4 기반 모니터링 대시보드. PostgreSQL + Prometheus 데이터소스. 공개 대시보드 기능 활성화 | 3002 |

### 2.6 Analysis (분석)

| 서비스 | 역할 |
|--------|------|
| **backtester** | Freqtrade 브릿지 기반 백테스팅 엔진. `backtest` 프로필로 온디맨드 실행. 결과는 `./backtest-results`에 저장 |

---

## 3. 공유 라이브러리 (shared/)

모든 Python 서비스가 공통으로 사용하는 라이브러리. Dockerfile에서 `/app/shared`로 복사하고 `PYTHONPATH=/app` 설정으로 접근한다.

| 모듈 | 역할 |
|------|------|
| `models/` | 도메인 모델 정의 (Order, Position, Strategy 등) |
| `exchange/` | Bybit CCXT 래퍼. 테스트넷/메인넷 전환, API 호출 추상화 |
| `db/` | asyncpg 커넥션 풀 관리, Repository 패턴 구현 |
| `redis_client.py` | Redis 싱글턴 연결 관리 (`get_redis()` / `close_redis()`), Pub/Sub 헬퍼. 자동 재연결 (`ensure_connected()`, 최대 3회, 지수 백오프), `get/set/publish`의 ConnectionError 시 1회 자동 재시도 |
| `config_loader.py` | YAML 설정 파일 로더. 절대경로 지원, 환경변수 치환 |
| `kill_switch.py` | Kill Switch 공통 로직. 4단계 계층 (경고 → 축소 → 청산 → 전면중지) |
| `logging_config.py` | structlog 기반 구조화 로깅 설정 |
| `risk.py` | 리스크 관리 유틸리티 (레버리지 검증, 포지션 크기 계산 등) |

---

## 4. 통신 패턴 (Redis Pub/Sub)

서비스 간 통신은 Redis Pub/Sub 채널을 통해 이루어진다. 느슨한 결합(loose coupling)으로 서비스 독립성을 보장한다.

| 채널 | 발행자 | 구독자 | 메시지 내용 |
|------|--------|--------|------------|
| `market:funding_rate` | market-data | funding-arb, orchestrator | 현재 펀딩비율, 다음 정산 시각 |
| `market:regime` | market-data | orchestrator | 시장 레짐 (trending / ranging / volatile) |
| `strategy:command:{id}` | orchestrator | 각 전략 서비스 | 자본 배분 비율, 시작/정지/파라미터 변경 명령 |
| `order:request` | 각 전략 서비스 | execution-engine | 주문 요청 (종목, 방향, 수량, 가격, 유형) |
| `order:update` | execution-engine | 각 전략 서비스 | 체결/부분체결/취소/거부 알림 |
| `kill_switch` | orchestrator | execution-engine | 긴급 청산 명령 (레벨 1~4) |
| `system:service_health` | orchestrator (watchdog) | (모니터링) | 서비스 헬스 상태 { status, dead_services, timestamp } |
| `system:config_reload` | orchestrator (config watcher) | (감사 로그) | kill_switch 설정 변경 이력 { section, changed_keys, old/new_values, timestamp } |
| `telegram:notification` | orchestrator (watchdog) | telegram-bot | Dead man's switch 알림 |

---

## 5. 데이터 흐름

```
Bybit WebSocket ──→ market-data ──→ Redis Pub/Sub ──→ 전략 서비스들
                         │                                  │
                         ▼                                  ▼
                    PostgreSQL                        order:request
                   (OHLCV, 펀딩비)                         │
                                                           ▼
                                                   execution-engine
                                                           │
                                                           ▼
                                                     Bybit REST API
                                                     (주문 실행)
                                                           │
                                                           ▼
                                                     order:update
                                                     (체결 알림)
```

**상세 흐름:**

1. **시세 수집**: market-data가 Bybit WebSocket에서 실시간 가격, 펀딩비, 오더북 데이터를 수신
2. **데이터 저장 및 발행**: OHLCV는 PostgreSQL에 저장하고, 펀딩비와 레짐 정보는 Redis로 발행
3. **전략 조율**: strategy-orchestrator가 레짐 정보를 수신하여 전략별 자본 배분 가중치를 계산하고 명령 발행
4. **전략 실행**: 각 전략 서비스가 시장 데이터와 오케스트레이터 명령을 수신하여 매매 판단 후 주문 요청
5. **주문 처리**: execution-engine이 안전 검증(레버리지, 포지션 크기) 후 Bybit API로 주문 실행
6. **결과 통보**: 체결 결과를 Redis로 발행하고, PostgreSQL에 거래 기록 저장
7. **모니터링**: Grafana + Prometheus가 전체 시스템 메트릭을 시각화, 이상 시 Telegram 알림

---

## 6. 설정 관리

### YAML 설정 파일 (`config/`)

```
config/
├── strategies/
│   ├── funding-arb.yaml      # 펀딩비 전략 파라미터 (진입 임계값, 포지션 크기 등)
│   ├── grid-trading.yaml     # 그리드 전략 파라미터 (가격 범위, 그리드 수 등)
│   └── adaptive-dca.yaml     # DCA 전략 파라미터 (분할 횟수, Fear&Greed 임계값 등)
├── orchestrator.yaml         # 레짐별 가중치, Kill Switch 임계값
├── prometheus/
│   └── prometheus.yml        # Prometheus 스크래핑 대상 설정
└── grafana/
    ├── datasources/          # PostgreSQL, Prometheus, Redis 데이터소스
    ├── dashboards/           # 프로비저닝 대시보드 JSON
    └── alerting/             # 알림 규칙
```

### 환경변수 (`.env`)

민감한 정보(API 키, DB 비밀번호)는 `.env` 파일로 관리하며 Git에서 제외된다.
`docker-compose.yml`의 `x-common-env` 앵커로 공통 환경변수를 모든 서비스에 주입한다.

---

## 7. 안전 장치

- **Kill Switch 4단계**: 경고 → 포지션 축소 → 전체 청산 → 시스템 전면 중지
- **테스트넷 강제**: `BYBIT_TESTNET=true` 기본값. Phase 5 전까지 변경 금지
- **레버리지 제한**: 선물 포지션 최대 2배 레버리지 (`bybit.py MAX_LEVERAGE=2` 상수 + `SafetyGuard` 이중 강제)
- **출금 불가**: API 키에 Withdraw 권한 미부여
- **헬스체크**: PostgreSQL, Redis에 Docker 헬스체크 설정. 의존 서비스 시작 순서 보장
- **Dead Man's Switch**: execution-engine/market-data가 30초마다 Redis에 하트비트 발행. 오케스트레이터 워치독이 60초마다 확인, execution-engine 하트비트 5분 이상 미수신 시 Kill Switch 자동 발동
- **Redis 보안**: `requirepass` 인증 활성화, 포트 127.0.0.1에만 바인딩 (외부 접근 차단)
- **주문 Rate Limiting**: 전략별 초당 2회 / 분당 30회 제한 (기본값, 설정 가능)
- **Redis Fail-Closed**: Redis 3회 연속 연결 실패 시 신규 주문 전면 차단. 로컬 메모리 캐시(TTL 60초)로 일시적 단절 완충
- **설정 핫 리로드**: `orchestrator.yaml`의 kill_switch 임계값을 서비스 재시작 없이 변경 가능 (최대 30초 반영)
