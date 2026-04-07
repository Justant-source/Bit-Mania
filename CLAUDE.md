# CryptoEngine — Claude Code 작업 가이드

## 프로젝트 개요

Bybit 테스트넷 → 소액 실전을 목표로 하는 비트코인 선물 자동매매 시스템.
**펀딩비 차익거래**를 핵심 전략으로, DCA를 보조 전략으로 운영.
Docker Compose 기반, WSL Ubuntu, 24/7 무중단 운영.

## 현재 진행 상태 (2026-04-07 기준)

- Phase 0 완료: Docker, PostgreSQL, Redis, Grafana 기동
- Phase 1 완료: Bybit 테스트넷 API 키 설정 (10,000 USDT)
- Phase 2 완료: 서비스 기동 + 연결 검증, Redis Pub/Sub 데이터 흐름 확인
- Phase 3 완료: 백테스트 (6년 히스토리 데이터, fa80_lev5_r30 채택, CAGR +34.87% Sharpe 3.583)
- Phase 4 진행 중: 테스트넷 포워드 테스트, 안전장치 구축 중
  - 완료: 통합 구조화 로깅, KST 타임스탬프, OHLCV 보존 정책, 자동 백업 (pg-backup)
  - 완료: startup gap recovery, 복원력 테스트 자동화, 포지션 정합성 체크 (reconciliation, 3분 간격)
  - 완료: 레짐 모니터링 대시보드, Telegram 파일 관리 + 인라인 키보드
  - 완료: Phase 5 preflight 스크립트 (8개 항목 점검)
  - 완료: stoploss_on_exchange (진입가 ±2% StopMarket, 자동 배치/취소/복구)
  - 완료: AlertDispatcher (배치·레이트리밋·dedup), Grafana→Telegram 단일 경로, Kill Switch ACK 확인
  - 완료: log-retention 서비스 (보존 정책 자동화), wf-scheduler 서비스 (월간 WF 자동화)
  - 완료: SafetyGuard 유닛 테스트 27개, Telegram 포매터 테스트 59개
  - 완료: 배포 재시작 시 포지션 자동 복구 (service_shutdown → Redis 저장 → 재시작 후 복원, 불필요한 청산 수수료 제거)
  - 완료: Phase 5 포지션 사이징 재설계 (`fixed_notional` 모드, $150 고정 명목가, 소액 최소 주문 대응)
  - 완료: Kill Switch 절대값 임계값 (퍼센트 AND 절대값 USD 둘 다 초과해야 발동, 수수료 노이즈 오발동 방지)
  - 완료: 진입 조건 강화 (연 25% / 4회 연속 / NetProfitabilityCheck BEP 2회)
  - 완료: 메인넷 전환 스크립트 (`scripts/switch_to_mainnet.py` 9단계, `scripts/switch_to_testnet.py` 6단계 롤백)
  - 완료: STRICT_MONITORING 모드 (24시간 강화 모니터링, 1시간 강제 리포트, 마진비율 경고)
  - 완료: 비상 수동 청산 SOP 문서 (`docs/EMERGENCY_MANUAL_CLOSE.md`, 휴대폰 저장용 5단계 요약 포함)
  - 완료: 잔고 동기화 검증 (`EXPECTED_INITIAL_BALANCE_USD` 기반, 5% 이상 차이 시 시작 거부)
  - 완료: orchestrator/core.py Phase 5 Kill Switch 연결 (`_build_kill_switch()`, `equity_at_open` 전달)

## 핵심 원칙

1. **생존 우선**: 수익보다 포지션 보호. Kill Switch 4단계 계층 유지
2. **테스트넷 우선**: `BYBIT_TESTNET=true` 확인 후 작업
3. **단계별 검증**: Phase 3 → 4 → 5 순서, 절대 건너뛰지 않음

## 프로젝트 구조

```
cryptoengine/
├── docker-compose.yml          # 전체 스택 (19개 서비스)
├── .env                        # API 키, DB 비밀번호 (git 제외)
├── config/
│   ├── strategies/
│   │   ├── funding-arb.yaml    # 펀딩비 전략 파라미터
│   │   └── adaptive-dca.yaml   # DCA 전략 파라미터
│   └── orchestrator.yaml       # 레짐별 가중치, Kill Switch 임계값
├── shared/                     # 모든 서비스 공유 라이브러리
│   ├── models/                 # 도메인 모델 (Order, Position, Strategy)
│   ├── exchange/               # Bybit CCXT 래퍼
│   ├── db/                     # asyncpg 풀, Repository 패턴
│   ├── redis_client.py         # Redis Pub/Sub 헬퍼
│   ├── config_loader.py        # YAML 설정 로더 (절대경로 지원)
│   ├── kill_switch.py          # Kill Switch 공통 로직 (Phase 5: 절대값 AND 조건 지원)
│   ├── log_events.py           # 이벤트 코드 정의 (95개)
│   ├── log_writer.py           # 비동기 DB 로그 라이터 (큐 기반)
│   ├── logging_config.py       # structlog 표준 설정 (KST 타임스탬프)
│   ├── timezone_utils.py       # KST 타임존 유틸리티
│   └── risk.py                 # 레버리지 검증, 포지션 크기 계산
├── docs/
│   └── EMERGENCY_MANUAL_CLOSE.md  # 비상 수동 청산 SOP (휴대폰 저장용 5단계 포함)
├── scripts/
│   ├── phase5_preflight.py     # Phase 5 진입 전 8개 항목 점검
│   ├── switch_to_mainnet.py    # 메인넷 전환 스크립트 (9단계, 이중 확인)
│   ├── switch_to_testnet.py    # 테스트넷 롤백 스크립트 (6단계, 백업 복원)
│   └── ...                     # 기타 운영 스크립트
└── services/
    ├── market-data/            # WebSocket 데이터 수집, 레짐 감지
    ├── orchestrator/           # 전략 조율, 자본 배분, 레짐 기반 가중치
    ├── execution/              # 주문 실행, 포지션 추적, 안전 검증, stoploss_manager.py
    ├── strategies/
    │   ├── base_strategy.py    # BaseStrategy ABC (모든 전략 상속)
    │   ├── funding-arb/        # 핵심 전략: 델타 뉴트럴 + 펀딩비 수취
    │   └── adaptive-dca/       # 보조: Fear&Greed 기반 적응형 DCA
    ├── llm-advisor/            # Claude Code 기반 시장 분석
    ├── telegram-bot/           # 알림 (AlertDispatcher) + 비상 명령 + ACK 확인
    ├── dashboard/              # 내부(3000) + 공개(3001) 대시보드
    ├── backtester/             # 백테스트 엔진 + 스킬셋
    │   ├── main.py             # 진입점
    │   ├── freqtrade_bridge.py # Freqtrade 연동 어댑터
    │   ├── walk_forward.py     # 워크포워드 분석기
    │   ├── report_generator.py # HTML/MD 리포트 생성
    │   ├── weight_optimizer.py # 레짐별 가중치 최적화
    │   ├── regime_accuracy.py  # 레짐 감지 정확도 평가
    │   ├── scripts/            # 데이터 수집·시드·헬스체크
    │   │   └── monthly_wf_runner.py  # 월간 WF 분석 파이프라인
    │   ├── tests/backtest/     # ★ 백테스트 스킬셋 (아래 참조)
    │   └── tests/unit/         # 유닛 테스트
    │       └── test_phase5.py  # Phase 5 유닛 테스트 16개
    ├── log-retention/          # service_logs 보존 정책 자동 실행 (매일 03:00 KST)
    ├── wf-scheduler/           # 월간 WF 자동 실행 (매월 1일 02:00 KST)
    └── grafana (이미지)        # 모니터링 대시보드 (포트 3002)
```

## 배포 시 포지션 관리 ★ 중요

### 원칙: 배포(재시작)는 포지션을 청산하지 않는다

`funding-arb` 전략은 `service_shutdown` 사유로 종료될 때 포지션을 청산하지 않고
Redis에 상태를 저장한 뒤, 재시작 후 자동으로 복구한다.
**dev/stage 환경 없이 prod에 직접 배포하는 구조이므로 이 원칙이 수수료 낭비를 막는 핵심이다.**

### 종료 사유별 동작

| 종료 사유 | 포지션 | 비고 |
|---------|--------|------|
| `service_shutdown` (배포·재시작) | **유지** — Redis에 상태 저장 | TTL 1시간 내 재시작 시 복구 |
| `kill_switch` | **즉시 청산** | 긴급 상황 |
| `funding_reversal` | **즉시 청산** | 펀딩비 음수 전환 |
| `basis_divergence_risk` | **즉시 청산** | 스프레드 과도 확대 |
| `basis_convergence` | **청산** | 수익 실현 |

### 안전한 서비스 배포 절차

```bash
# 1. 포지션 확인 (선택)
docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "SELECT * FROM positions WHERE size > 0;"

# 2. 코드 수정 후 재빌드 (포지션 유지됨)
docker compose up -d --build --no-deps funding-arb

# 3. 복구 확인 — 로그에서 "포지션 상태 복구 완료" 확인
docker compose logs --tail=20 funding-arb | grep -E "복구|recovered"
```

### shared/ 수정 시 (모든 서비스 재빌드)

```bash
# shared/ 변경은 이 순서로 재빌드
docker compose build market-data execution-engine funding-arb strategy-orchestrator telegram-bot
docker compose up -d --no-deps market-data execution-engine funding-arb strategy-orchestrator telegram-bot
```

### 주의사항
- Redis TTL은 **1시간**. 1시간 초과 중단 후 재시작 시 포지션이 남아있어도 복구 안 됨 → 신규 시작
- 이 경우 거래소에 포지션이 남아있으면 **수동 청산 필요** (Telegram `/emergency_close` 또는 Bybit UI)
- `make emergency`는 kill_switch 사유로 종료 → 여전히 청산됨 (의도적)

## Docker 작업 규칙

### 빌드 컨텍스트
모든 서비스의 build context는 프로젝트 루트(`.`)로 설정되어 있음.
Dockerfile 내 COPY 경로는 반드시 프로젝트 루트 기준으로 작성:

```dockerfile
# 올바른 예
COPY cryptoengine/shared /app/shared
COPY cryptoengine/services/strategies/funding-arb /app/strategy
COPY cryptoengine/services/strategies/base_strategy.py /app/

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

## 서비스 간 통신 (Redis Pub/Sub)

| 채널 | 발행자 | 구독자 | 내용 |
|------|--------|--------|------|
| `market:funding_rate` | market-data | funding-arb, orchestrator | 현재 펀딩비 |
| `market:regime` | market-data | orchestrator | 시장 레짐 (trending/ranging/volatile) |
| `strategy:command:{id}` | orchestrator | 각 전략 | 자본 배분, 시작/정지 명령 |
| `order:request` | 각 전략 | execution-engine | 주문 요청 |
| `order:update` | execution-engine | 각 전략 | 체결/취소 알림 |
| `kill_switch` | orchestrator | execution-engine | 긴급 청산 |

## 데이터베이스 (PostgreSQL)

주요 테이블:
- `trades` — 모든 체결 기록
- `positions` — 현재/과거 포지션
- `funding_payments` — 펀딩비 수취 기록
- `funding_rate_history` — 펀딩비 히스토리
- `ohlcv_history` — OHLCV 캔들 데이터 (보존 정책: 타임프레임별 자동 삭제)
- `portfolio_snapshots` — 시간별 포트폴리오 스냅샷
- `daily_reports` — 일별 수익/지표 집계
- `kill_switch_events` — Kill Switch 발동 이력
- `strategy_states` — 전략 상태 스냅샷
- `llm_judgments` — LLM 분석 결과
- `service_logs` — 전 서비스 구조화 이벤트 로그 (migration 003)
- `regime_raw_log` — 5분 캔들별 원시 레짐 감지 결과 (migration 004)
- `regime_transitions` — 확정 레짐 전환 이벤트 (migration 004)

## 환경 변수 (.env)

```bash
# Bybit (테스트넷)
BYBIT_API_KEY=<set in .env, do not commit>
BYBIT_TESTNET=true          # 절대 false로 바꾸지 않음 (Phase 5 전까지)

# DB
DB_PASSWORD=CryptoEngine2026!

# Grafana: http://localhost:3002
# 로그인: admin / GrafanaAdmin2026!
```

## 백테스트 스킬셋 규칙 ★ 중요

백테스트 스크립트는 **스킬셋**으로 관리한다. 새 스크립트 작성 전 반드시 아래 절차를 따른다.

### 1. 기존 스킬 확인 (필수)
```
services/backtester/tests/backtest/README.md
```
이 파일이 스킬 인덱스다. 새 스크립트를 작성하기 전에 **반드시 이 파일을 읽어**
기존에 동일하거나 유사한 스크립트가 있는지 확인한다.

### 2. 새 스크립트 위치 (필수)
모든 백테스트 Python 파일은 `services/backtester/tests/backtest/<카테고리>/` 에 생성한다.
**루트 레벨(`services/backtester/*.py`)에 테스트 스크립트를 절대 생성하지 않는다.**

| 목적 | 카테고리 디렉토리 |
|------|----------------|
| FA 단독 성과·파라미터·레버리지·재투자 | `fa/` |
| 레짐 감지 로직 개선·비교 | `regime/` |
| 복수 전략 조합·자본배분 | `combined/` |
| 추세추종 전략 | `trend/` |
| 극단 시나리오 검증 | `stress/` |
| 데이터 탐색·수수료 분석 | `analysis/` |
| 파라미터 그리드서치·최적화 | `optimization/` |

### 3. 공유 유틸리티 사용 (필수)
새 스크립트에서 아래 중복 구현을 금지한다. 반드시 `core/`를 임포트한다:
```python
from tests.backtest.core import (
    load_ohlcv, load_funding,   # DB 데이터 로드
    sharpe, mdd, cagr, safe_float, monthly_returns,  # 지표 계산
    make_pool, save_result,     # DB 연결·저장
    FAEngine, SimpleBacktester, # 엔진
)
```

### 4. README 업데이트 (필수)
`tests/backtest/README.md`를 반드시 업데이트해야 하는 시점:
- 새 스크립트 **추가** → 해당 카테고리 테이블에 행 추가
- 기존 스크립트 **수정** (파라미터·목적 변경) → 해당 행 업데이트
- 스크립트 **삭제** → 해당 행 제거
- **실행 이력 요약** 테이블에 중요 결과 추가

### 5. Docker 실행 명령
```bash
# 단일 스크립트 실행
docker compose --profile backtest run --rm backtester \
  python tests/backtest/<카테고리>/<스크립트>.py

# 이미지 재빌드 후 실행 (새 파일 추가 시)
docker compose --profile backtest build --no-cache backtester && \
docker compose --profile backtest run --rm backtester \
  python tests/backtest/<카테고리>/<스크립트>.py
```

## 코드 작업 시 주의사항

1. **실전 전환 금지**: `BYBIT_TESTNET=false` 변경은 Phase 4 완료 후 명시적 승인 필요
2. **출금 권한 없음**: API 키에 Withdraw 권한 없음 (의도적)
3. **Kill Switch 유지**: `shared/kill_switch.py` 로직 절대 약화시키지 않음
4. **레버리지 제한**: 선물 포지션 레버리지 5배 초과 금지
   - **현재 적용 설정**: `fa80_lev5_r30` (FA 80% + 레버리지 5x + 재투자 30%)
     - CAGR +34.87% | Sharpe 3.583 | MDD -4.52% | 6년 청산 0회 (Test 12 Stage D2)
   - **후보 설정** (변경 시 `.result/12.` 리포트 참조):
     - `fa80_lev4_r30`: FA=80% Lev=4x Reinv=30% → CAGR +28.56% Sharpe 3.556 (보수적 차선책)
     - `fa80_lev5_r50`: FA=80% Lev=5x Reinv=50% → CAGR +33.54% Sharpe 1.867 (재투자 확대)
5. **공유 라이브러리 수정 시**: `shared/` 변경은 모든 서비스 이미지 재빌드 필요

## 다음 작업 (Phase 4 진행 중)

1. 7개 시나리오 체크리스트 완료 (`arch/PHASE4_MONITORING.md` 참조)
2. 7일 이상 무중단 운영 확인 (Restarting 없이 Running 유지)
3. `scripts/phase5_preflight.py` 모든 항목 PASS 확인
4. `make resilience-test`로 복원력 검증
5. Telegram 모든 알림 유형 수신 확인 (Kill Switch ACK 포함)
6. stoploss_on_exchange 정상 동작 확인 (진입/청산/재시작 시나리오)
7. Walk-Forward 월간 파이프라인 1회 이상 정상 완료 확인
8. **Phase 5 진입 준비 완료** — `scripts/switch_to_mainnet.py` 실행 후 `BYBIT_TESTNET=false` 전환
   - `EXPECTED_INITIAL_BALANCE_USD=200` 설정 필수 (잔고 검증)
   - `STRICT_MONITORING_HOURS=24` 설정 (첫 24시간 강화 모니터링)
   - `PHASE5_MODE=true` 설정 (fixed_notional 사이징, 절대값 Kill Switch 활성화)
