# CryptoEngine — Claude Code 작업 가이드

## 프로젝트 개요

Bybit 테스트넷 → 소액 실전을 목표로 하는 비트코인 선물 자동매매 시스템.
**펀딩비 차익거래**를 핵심 전략으로, 그리드/DCA를 보조 전략으로 운영.
Docker Compose 기반, WSL Ubuntu, 24/7 무중단 운영.

## 현재 진행 상태 (2026-04-03 기준)

- Phase 0 완료: Docker, PostgreSQL, Redis, Grafana 기동
- Phase 1 완료: Bybit 테스트넷 API 키 설정 (10,000 USDT)
- Phase 2 완료: 12개 서비스 전체 기동 성공 (dashboard 포함)
- Phase 3 완료: 백테스트 완료 (펀딩비 Sharpe 54.81, MDD -0.14%)
- Phase 4 진행 중: 테스트넷 포워드 테스트 시작 (2026-04-04~)

## 핵심 원칙

1. **생존 우선**: 수익보다 포지션 보호. Kill Switch 4단계 계층 유지
2. **테스트넷 우선**: `BYBIT_TESTNET=true` 확인 후 작업
3. **단계별 검증**: Phase 3 → 4 → 5 순서, 절대 건너뛰지 않음

## 프로젝트 구조

```
cryptoengine/
├── docker-compose.yml          # 전체 스택 (12개 서비스)
├── .env                        # API 키, DB 비밀번호 (git 제외)
├── config/
│   ├── strategies/
│   │   ├── funding-arb.yaml    # 펀딩비 전략 파라미터
│   │   ├── grid-trading.yaml   # 그리드 전략 파라미터
│   │   └── adaptive-dca.yaml   # DCA 전략 파라미터
│   └── orchestrator.yaml       # 레짐별 가중치, Kill Switch 임계값
├── shared/                     # 모든 서비스 공유 라이브러리
│   ├── models/                 # 도메인 모델 (Order, Position, Strategy)
│   ├── exchange/               # Bybit CCXT 래퍼
│   ├── db/                     # asyncpg 풀, Repository 패턴
│   ├── redis_client.py         # Redis Pub/Sub 헬퍼
│   ├── config_loader.py        # YAML 설정 로더 (절대경로 지원)
│   └── kill_switch.py          # Kill Switch 공통 로직
└── services/
    ├── market-data/            # WebSocket 데이터 수집, 레짐 감지
    ├── orchestrator/           # 전략 조율, 자본 배분, 레짐 기반 가중치
    ├── execution/              # 주문 실행, 포지션 추적, 안전 검증
    ├── strategies/
    │   ├── base_strategy.py    # BaseStrategy ABC (모든 전략 상속)
    │   ├── funding-arb/        # 핵심 전략: 델타 뉴트럴 + 펀딩비 수취
    │   ├── grid-trading/       # 보조: 횡보 구간 그리드
    │   └── adaptive-dca/       # 보조: Fear&Greed 기반 적응형 DCA
    ├── llm-advisor/            # Claude Code 기반 시장 분석
    ├── telegram-bot/           # 알림 + 비상 명령
    ├── dashboard/              # 내부(3000) + 공개(3001) 대시보드
    ├── backtester/             # 백테스트 엔진
    └── grafana (이미지)        # 모니터링 대시보드 (포트 3002)
```

## Docker 작업 규칙

### 빌드 컨텍스트
모든 서비스의 build context는 프로젝트 루트(`.`)로 설정되어 있음.
Dockerfile 내 COPY 경로는 반드시 프로젝트 루트 기준으로 작성:
```dockerfile
# 올바른 예
COPY shared /app/shared
COPY services/strategies/funding-arb /app/strategy
COPY services/strategies/base_strategy.py /app/

# 잘못된 예 (빌드 실패)
COPY ../../shared /app/shared
```

### 자주 쓰는 명령
```bash
# 인프라만 기동
docker compose up -d postgres redis grafana

# 핵심 서비스 기동
docker compose up -d market-data execution-engine funding-arb strategy-orchestrator

# 특정 서비스 재빌드 후 재시작
docker compose up -d --build --no-deps <service>

# 로그 확인
docker compose logs -f funding-arb
docker compose logs --tail=50 market-data

# 전체 상태
docker compose ps

# DB 직접 접속
docker compose exec postgres psql -U cryptoengine -d cryptoengine

# 비상 정지 (포지션 보호)
make emergency
```

## 알려진 문제 및 해결법

### 1. config 파일 경로
`load_config()`에 절대 경로 전달 시 직접 파일을 열도록 `config_loader.py` 수정 완료.
전략 config 파일은 두 이름 모두 존재:
- `config/strategies/funding_arb.yaml` (원본)
- `config/strategies/funding-arb.yaml` (하이픈 버전, 복사본)

### 2. shared 모듈 접근
서비스에서 `from shared.xxx import yyy` 사용 시 Dockerfile에서
`COPY shared /app/shared` 와 `ENV PYTHONPATH=/app` 필수.

### 3. structlog 레벨 상수
`structlog.INFO` 없음 → `logging.INFO` 사용. `import logging` 추가 필요.

### 4. base_strategy 위치
`services/strategies/base_strategy.py` — 전략 서비스 Dockerfile에서 명시적 복사 필요:
```dockerfile
COPY services/strategies/base_strategy.py /app/
```

### 5. Redis close_redis
`shared/redis_client.py`에 모듈 레벨 싱글턴 `get_redis()` / `close_redis()` 추가 완료.

### 6. trades 테이블 스키마
market-data의 원시 틱 데이터는 Redis Pub/Sub로만 전달.
`trades` 테이블은 execution-engine의 전략 체결 전용.
market-data collector.py의 `_on_trades()`에서 DB INSERT 제거됨.

### 7. Grafana 대시보드
두 개의 대시보드가 프로비저닝됨:
- `backtest_results.json` — 백테스트 성과, 펀딩비 히스토리, BTC 가격
- `live_performance.json` — 라이브 자산 곡선, 일별 수익률, 전략별 기여도

### 8. 백테스터 스크립트
- `scripts/seed_historical.py` — OHLCV + 펀딩비 히스토리 다운로드
- `tests/backtest/bt_funding_arb.py` — 델타 뉴트럴 펀딩비 차익 백테스트
- `tests/backtest/bt_grid.py` — EMA 필터 그리드 백테스트
- `tests/backtest/bt_combined.py` — 복합 전략 (레짐 기반 가중치)
- `scripts/phase4_health_check.py` — Phase 4 헬스체크 (8항목)

### 9. backtest_results 테이블
백테스트 결과 저장 전용 테이블 (daily_reports와 별도):
(id, run_at, strategy, symbol, start_date, end_date, initial_capital, final_equity, total_return, sharpe_ratio, max_drawdown, win_rate, total_trades, metadata)

## 서비스 간 통신 (Redis Pub/Sub)

| 채널 | 발행자 | 구독자 | 내용 |
|------|--------|--------|------|
| `market:funding_rate` | market-data | funding-arb, orchestrator | 현재 펀딩비 |
| `market:regime` | market-data | orchestrator | 시장 레짐 (trending/ranging/volatile) |
| `strategy:command:{id}` | orchestrator | 각 전략 | 자본 배분, 시작/정지 명령 |
| `order:request` | 각 전략 | execution-engine | 주문 요청 |
| `order:update` | execution-engine | 각 전략 | 체결/취소 알림 |
| `kill_switch` | orchestrator | execution-engine | 긴급 청산 |

### Redis `cache:` 키 패턴

| 키 | 설명 |
|---|---|
| `cache:ohlcv:{exchange}:{symbol}:{tf}` | 최신 OHLCV 해시 |
| `cache:funding:{exchange}:{symbol}` | 현재 펀딩비 + 다음 수취 시간 |
| `cache:wallet_balance` | 지갑 잔고 |
| `cache:portfolio_state` | 포트폴리오 상태 |

## 데이터베이스 (PostgreSQL)

주요 테이블:
- `trades` — 전략 체결 기록 (execution-engine 전용)
- `positions` — 현재/과거 포지션
- `funding_payments` — 펀딩비 수취 기록
- `funding_rate_history` — 펀딩비 히스토리
- `ohlcv_history` — OHLCV 캔들 데이터
- `portfolio_snapshots` — 시간별 포트폴리오 스냅샷
- `daily_reports` — 일별 수익/지표 집계
- `kill_switch_events` — Kill Switch 발동 이력
- `strategy_states` — 전략 상태 스냅샷
- `llm_judgments` — LLM 분석 결과
- `backtest_results` — 백테스트 결과 저장

## 환경 변수 (.env)

```bash
# Bybit (테스트넷)
BYBIT_API_KEY=UhwLgPG4wqEwt9cX0x
BYBIT_TESTNET=true          # 절대 false로 바꾸지 않음 (Phase 5 전까지)

# DB
DB_PASSWORD=CryptoEngine2026!

# Grafana: http://localhost:3002
# 로그인: admin / GrafanaAdmin2026!
```

## 코드 작업 시 주의사항

1. **실전 전환 금지**: `BYBIT_TESTNET=false` 변경은 Phase 4 완료 후 명시적 승인 필요
2. **출금 권한 없음**: API 키에 Withdraw 권한 없음 (의도적)
3. **Kill Switch 유지**: `shared/kill_switch.py` 로직 절대 약화시키지 않음
4. **레버리지 제한**: 선물 포지션 레버리지 2배 초과 금지
5. **공유 라이브러리 수정 시**: `shared/` 변경은 모든 서비스 이미지 재빌드 필요

## 다음 작업 (Phase 4 진행 중)

1. 테스트넷 포워드 테스트 7개 시나리오 검증
2. 1주일 이상 안정 가동 확인
3. 백테스트 대비 결과 괴리율 10% 이내 확인
4. PHASE4_MONITORING.md 가이드 따라 일일/주간 체크

## 헬스체크 실행
```bash
docker compose --profile backtest run --rm backtester python scripts/phase4_health_check.py
```
