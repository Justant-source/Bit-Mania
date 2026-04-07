---
title: 변경 이력
tags:
  - changelog
  - release
  - version
aliases:
  - Changelog
  - 릴리스 노트
related:
  - "[[architecture]]"
  - "[[strategies/funding_arb]]"
  - "[[strategies/adaptive_dca]]"
---

# 변경 이력 (Changelog)

이 문서는 CryptoEngine의 주요 변경 사항을 기록합니다.

형식: [시맨틱 버전](https://semver.org/lang/ko/) 기준

---

## [1.6.1] - 2026-04-07

### 추가 (Added)

#### orchestrator/core.py: Phase 5 Kill Switch 연결
- `_build_kill_switch()` 헬퍼 메서드 신규 추가 — `PHASE5_MODE` / `BYBIT_TESTNET` 감지 후 적절한 KillSwitch 인스턴스 생성
- Phase 5 감지 시 `orchestrator.yaml`의 `kill_switch.phase5` 섹션에서 완화된 퍼센트 임계값과 절대값 USD 임계값을 읽어 KillSwitch 초기화 (AND 조건)
- `_orchestration_cycle()`: `kill_switch.check()` 호출에 `equity_at_open=portfolio_state.total_equity` 전달 (절대값 계산 기준)
- `_reload_kill_switch_config()`: 핫 리로드 시 KillSwitch 인스턴스 재생성으로 설정 변경 즉시 반영

---

## [1.6.0] - 2026-04-07

### 추가 (Added)

#### Phase 5 소액 실전 전환 ($200 USDT) 사전 개발

##### 1.1 포지션 사이징 재설계 (`services/strategies/funding-arb/strategy.py`)
- `fixed_notional` 사이징 모드 추가: `$150` 고정 명목가 (= $200 × 75% 안전 버퍼)
- `min_viable` 사이징 모드 추가: `min_position_usd` 기반 최소 수량
- Phase 5 자동 활성화: `PHASE5_MODE=true` 또는 `BYBIT_TESTNET=false` 시 `phase5` config 섹션 적용
  - `max_concurrent_positions: 1` (5 → 1, 소액 리스크 관리)
  - `min_position_usd: 50` (100 → 50, Bybit 최소 주문 대응)

##### 1.2 Kill Switch 절대값 임계값 (`shared/kill_switch.py`)
- `daily_loss_abs_usd`, `weekly_loss_abs_usd`, `monthly_loss_abs_usd` 파라미터 추가
- AND 조건: 퍼센트 임계값 AND 절대값 USD 임계값 둘 다 초과해야 발동 (노이즈 오발동 방지)
- `None`(기본값) 시 기존 퍼센트 전용 동작 유지 (하위 호환)
- `check()` 메서드에 `equity_at_open` 키워드 파라미터 추가

##### 1.3 진입 조건 강화 (`services/strategies/funding-arb/funding_tracker.py`)
- `estimate_net_profit_per_cycle()`: 왕복 수수료/슬리피지 고려 후 순수익, BEP 사이클 수 계산
- `is_entry_net_profitable()`: BEP 사이클 수 <= 기준(기본 2회) 여부 반환
- `config/strategies/funding-arb.yaml` phase5 진입 조건: `min_funding_rate_annualized: 25.0`, `consecutive_intervals: 4`

##### 1.4 메인넷 전환 스크립트 (`scripts/`)
- `switch_to_mainnet.py` (273줄, 9단계): `.env` 백업 → API 키 검증 → 오픈 포지션 확인 → 메인넷 잔고 조회 → 이중 확인(`yes I am sure` + `MAINNET`) → `BYBIT_TESTNET=false` 설정 → Redis 캐시 클리어
- `switch_to_testnet.py` (184줄, 6단계): 현재 `.env` 백업 → 테스트넷 백업 자동 탐색 → `rollback` 확인 → 복원 + `BYBIT_TESTNET=true` 강제 → Redis 캐시 클리어

##### 1.5 STRICT_MONITORING 모드 (`services/telegram-bot/main.py`)
- 활성화 조건: `BYBIT_TESTNET=false AND STRICT_MONITORING_HOURS=24`
- `AlertDispatcher batch_window_seconds=0` → 모든 알림 즉시 전송
- 매 1시간마다 강제 상태 리포트 (자산, PnL, 포지션, 마진비율)
- 마진비율 < `STRICT_MARGIN_WARN_THRESHOLD` (기본 20x) → 경고
- `EXPECTED_INITIAL_BALANCE_USD` 대비 변동 표시
- `STRICT_MONITORING_HOURS` 경과 시 자동 해제 알림

##### 1.6 비상 수동 청산 SOP (`docs/EMERGENCY_MANUAL_CLOSE.md`)
- 사용 시점 판단 기준, Bybit 앱/웹 청산 절차 (모바일 + PC)
- 스탑마켓 미체결 주문 취소, 봇 복구 후 DB/Redis 정리 명령
- **휴대폰 저장용 5단계 빠른 참조 블록**

##### 1.7 잔고 동기화 검증 (`services/execution/main.py`)
- `EXPECTED_INITIAL_BALANCE_USD > 0 AND BYBIT_TESTNET=false` 시 시작 시 잔고 검증
- 실제 잔고 ↔ 기대 잔고 차이 > 5% → `RuntimeError` (시작 거부) + Redis Telegram 알림 발행

#### 테스트
- `services/backtester/tests/unit/test_phase5.py` 신규 (407줄, 16개 테스트)
  - `TestPositionSizing` (8개): fixed_notional, min_viable, pct_equity, 레버리지 비교
  - `TestFundingTrackerNetProfit` (6개): 순수익 계산, BEP, 진입 판단
  - `TestKillSwitchAbsoluteThreshold` (5개): AND 조건 4케이스 + 레거시 동작

#### 설정
- `config/strategies/funding-arb.yaml`: `phase5:` 섹션 추가 (+28줄)
- `config/orchestrator.yaml`: `kill_switch.phase5:` 서브섹션 추가 (+15줄)

---

## [1.5.1] - 2026-04-07

### 수정 (Fixed)

#### phase5_preflight.py / bybit.py: `sandbox` 속성 안전 접근
- `connector._exchange.sandbox` → `getattr(connector._exchange, "sandbox", False)`
- CCXT 버전에 따라 `sandbox` 속성이 없을 경우 AttributeError 방지

#### backtester Dockerfile: 스크립트 경로 누락 수정
- `COPY scripts /app/scripts` 추가 — backtester 컨테이너에서 `scripts/phase5_preflight.py` 등 최상위 스크립트 접근 가능

---

## [1.5.0] - 2026-04-07

### 추가 (Added)

#### execution-engine: 거래소 스탑로스 주문 자동 관리 (`stoploss_manager.py`)
- 포지션 진입 시 거래소에 StopMarket 스탑로스 주문 자동 배치 (진입가 ±2%, Bybit StopMarket)
- 포지션 청산 시 연관된 스탑로스 주문 자동 취소
- 봇 재시작 시 열린 포지션의 스탑로스 자동 복구 (Redis 캐시 기반)
- 캐시 키: `cache:stoploss:{exchange}:{symbol}` (TTL 24h)

#### telegram-bot: AlertDispatcher 알림 시스템
- 동일 타입 알림을 `batch_window_seconds=5` 동안 배치 후 단일 메시지로 전송
- `max_messages_per_minute=30` 레이트 리밋 적용
- `kill_switch` / `anomaly` 타입은 즉시 전송 바이패스
- `min_trade_size_usd` 미만 entry/exit 알림 자동 드롭
- 일일 리포트 자동 전송 (08:00, 20:00 UTC) — asyncio 스케줄러
- 펀딩비 수취: 정상 수취는 일일 누적 후 리포트 통합, 이상(음수/예상의 50% 이하)만 즉시 알림
- `/status` 응답에 Sharpe(30일 연환산), 월간 최대 드로다운 추가

#### Grafana 알림 Telegram 중복 제거
- 이전: Grafana Bot Token 직접 전송 + 봇 Redis 구독 → 이중 전송
- 현재: Grafana → Webhook(dashboard:3000/grafana-webhook) → Redis `ce:alerts:grafana` → 봇 단일 경로
- `kill_switch`, `anomaly` 타입은 fingerprint 기반 120초 dedup 적용

#### Kill Switch ACK 확인 메커니즘
- Telegram `/emergency_close` → Redis PUBLISH 후 5초 대기
- 오케스트레이터가 처리 즉시 `ce:kill_switch:ack` PUBLISH
- ACK 수신 시 "✅ Kill Switch 수신 확인" 전송
- ACK 미수신 시 "⚠️ ACK 미수신" 경고 + 최대 3회 재전송

#### 신규 Docker 서비스
- `log-retention`: 매일 03:00 KST service_logs 보존 정책 자동 실행 (DEBUG 7일, INFO 30일, WARNING 90일, ERROR 365일)
- `wf-scheduler`: 매월 1일 02:00 KST Walk-Forward 분석 자동 실행 + Telegram 결과 전송 (`services/backtester/scripts/monthly_wf_runner.py`)

#### 테스트
- `tests/unit/test_safety_guard.py`: 27개 유닛 테스트 (Kill Switch 체크, 레버리지, 슬리피지, Redis fail-closed 등)
- `services/telegram-bot/tests/test_telegram_formatters.py`: 59개 유닛 테스트

### 변경 (Changed)

#### 수치 설정 변경
- `ONE_SIDE_FILL_TIMEOUT`: 3분 → **1분** (delta-neutral gap 최소화)
- `RECONCILE_INTERVAL`: 10분 → **3분** (불일치 감지 시간 단축)
- LLM `max_adjustment`: 15%p → **5%p** (AI 환각 시 자본 배분 오차 축소)

#### 로그 시스템 개선
- `shared/log_events.py`: `EVENT_LEVELS` dict 추가 (95개 이벤트별 권장 로그 레벨)
- `shared/log_writer.py`: `dropped_count` 카운터 + 드롭 시 DB warning 기록
- `shared/logging_config.py`: aioredis, asyncpg, telegram 등 라이브러리 로그 억제 추가

### 수정 (Fixed)

#### SQL 인젝션 방지
- `public.ts`: 템플릿 리터럴 `${PUBLIC_DELAY_MINUTES}` → `$1 * INTERVAL '1 minute'` 파라미터 바인딩

#### cross_exchange.py
- `CROSS_EXCHANGE_ENABLED` env var 명시적 가드 추가 (미설정 시 비활성화)

---

## [1.4.0] - 2026-04-06

### 추가 (Added)

#### 레짐 모니터링 대시보드 (`173c8f3`)
- DB: `regime_raw_log` (5분 캔들별 원시 레짐), `regime_transitions` (확정 레짐 변경) 테이블 추가 (migration 004)
- market-data: 매 5분 캔들마다 `regime_raw_log` 저장, 확정 변경 시 `regime_transitions` 기록
- orchestrator: 가중치 전환 진행률(5-cycle EMA) 추적, `orchestrator:weight_transition` Redis 채널 발행
- dashboard: `/regime` 엔드포인트 4개 + 6-섹션 레짐 대시보드 (http://localhost:3000/regime, 5초 폴링, Chart.js)

#### Telegram 파일 관리 + 인라인 키보드 UI (`b93beb6`)
- `.md` 파일 업로드 → 자동으로 `.request/` 디렉토리에 저장
- 새 명령어: `/requests` (요청 파일 목록), `/results` (결과 리포트 목록), `/get <파일명>` (파일 다운로드)
- `/help` 명령어에 인라인 키보드(한 번 탭으로 명령 실행) 추가
- `/results` — 다운로드 버튼이 달린 결과 리포트 목록 제공

#### startup gap recovery + 복원력 자동화 테스트 (`66dda3f`)
- `collector.py`: 기동 시 `backfill_ohlcv_gaps()` — OHLCV 공백 감지 후 Bybit REST로 최대 48h 복구 (5개 타임프레임)
- `funding_monitor.py`: 기동 시 `backfill_funding_gaps()` — 펀딩비율 공백 감지 후 최대 3일 복구
- `scripts/resilience_test.py`: market-data, execution-engine, redis kill+restart 자동화 테스트 (데이터 연속성 및 포지션 보존 검증)
- Makefile: `resilience-test`, `resilience-test-market-data`, `resilience-test-execution` 타겟 추가

#### Phase 5 실전 전환 안전장치 (`1348a12`)
- `position_tracker.py`: `reconcile_positions()` 10분마다 실행 — 내부 상태와 거래소 실제 포지션 비교, 불일치 시 `position:reconcile_event` 채널로 발행
- `bybit.py`: `get_trading_fees()` (VIP 등급별), `get_min_order_sizes()` 추가
- `log_events.py`: `POSITION_RECONCILE_OK/MISMATCH/FIXED`, `FEE_TIER_UPDATED/MISMATCH` 이벤트 추가
- `scripts/phase5_preflight.py`: 8개 항목 점검 스크립트 (환경변수, API 연결, 잔고, 수수료, 최소 주문 크기, 레버리지, 오픈 포지션, DB)

#### PostgreSQL 자동 백업 + 컨테이너 리소스 제한 + 인프라 Grafana 알림 (`1940aa7`)
- `pg-backup` 서비스: crond 기반 일일 `pg_dump` (02:00 KST), 7일 보존, `pg-backups` 볼륨
- `scripts/pg_backup.sh`, `pg_backup_entrypoint.sh`: 백업 로직 및 cron 엔트리포인트
- Makefile: `backup`, `backup-list`, `backup-restore` 명령 추가
- `docker-compose.yml`: 전체 서비스 `deploy.resources.limits` 적용 (~3.8GB 총 예산)
- `alert_rules.yaml`: 인프라 경고 4개 추가 (CPU >85%, 메모리 <15%, 디스크 <10%, Redis >80%)

#### 통합 구조화 로깅 시스템 (`5cfb66e`)
- `shared/log_events.py`: 전략 이벤트 코드 정의 (95개 이벤트)
- `shared/log_writer.py`: 비동기 DB 로그 라이터 (배치 처리, 큐 기반)
- `shared/logging_config.py`: structlog 기반 표준 로깅 설정 (JSON 형식 + KST 타임스탬프)
- DB migration `003_service_logs.sql`: `service_logs` 테이블 추가
- 모든 서비스에 구조화 DB 로깅 통합

### 변경 (Changed)

#### KST 타임존 유틸리티 + 로그 타임스탬프 KST 전환 (`d11e8e4`)
- `shared/timezone_utils.py`: KST 변환 유틸리티 추가 (152줄, `to_kst()`, `now_kst()` 등)
- 모든 서비스 로그 타임스탬프 UTC → KST 표시로 전환 (43개 파일)

#### OHLCV 보존 정책 스크립트 + 오케스트레이터 수정 (`4171d8f`)
- `scripts/ohlcv_retention.py`: 타임프레임별 보존 기한 초과 데이터 자동 삭제 + VACUUM ANALYZE
  - 1m→30일, 5m→90일, 15m→180일, 1h→365일, 4h→730일
- `orchestrator/core.py`: `portfolio_snapshot_interval_seconds` 하드코딩(900s) → config 파일에서 읽도록 수정

### 수정 (Fixed)

#### Redis 인증 오류 수정 (`dbae0a4`)
- `docker-compose.yml`: `REDIS_URL` fallback에 `${REDIS_PASSWORD}` 포함 (`.env` 없이도 인증 가능)
- `redis-exporter` `REDIS_ADDR` 하드코딩 제거 → 패스워드 포함 URL 사용
- `.env.example`: `REDIS_PASSWORD` 항목 추가 및 `REDIS_URL` 예시 업데이트

#### Service Logs Grafana 대시보드 필터 오류 수정 (`1c2f75a`)
- `service-logs.json`: service, level, event 템플릿 변수에 `allValue: "All"` 추가 → "All" 선택 시 `No data` 오류 해결

---

## [1.1.0] - 2026-04-05

### 변경 (Changed)

#### FA 전략 파라미터 — fa80_lev5_r30 적용
- `config/strategies/funding-arb.yaml`: 레버리지 2x → **5x** (max_leverage: 5)
- `fa_capital_ratio: 0.80` 추가 (전체 포트폴리오의 80%를 FA 전략에 배분)
- `reinvest_ratio: 0.30` 추가 (펀딩비 수익의 30% 현물 BTC 재투자)
- `risk.max_portfolio_allocation_pct`: 25% → 80%

#### 전략 코드 변경 (`services/strategies/funding-arb/strategy.py`)
- `leverage` 기본값: 1.0 → 5.0
- `_calculate_position_size()`: 레버리지 팩터 반영 (qty = capital * 0.95 / (price * (1 + 1/L)))
- `_process_funding_payment()`: 재투자 로직 추가 — 양수 펀딩비의 30%를 현물 BTC 시장가 매수
- `on_start()`: 설정값 로깅 강화 (leverage, fa_capital_ratio, reinvest_ratio)

#### 선택 근거
- Test 12 Stage D2 백테스트 결과 기반 (2020-04-01 ~ 2026-03-31, 6년)
- 전체 18개 조합 중 연수익 1위, Sharpe 1위
- 6년간 청산 0회, 최소 마진비율 36.5x (충분히 안전)
- 후보 설정 (CLAUDE.md 및 config 주석 참조):
  - `fa80_lev4_r30`: CAGR +28.56% Sharpe 3.556 (보수적)
  - `fa80_lev5_r50`: CAGR +33.54% Sharpe 1.867 (재투자 확대)

---

## [1.0.0] - 2026-04-03

### 추가 (Added)

#### 핵심 시스템
- [[architecture#PostgreSQL 16|PostgreSQL 16]] 기반 데이터 저장소 (asyncpg 연결 풀)
- [[architecture#Redis 7|Redis 7]] 기반 이벤트 버스 및 캐시 레이어
- Docker Compose 기반 마이크로서비스 배포
- structlog 기반 구조화된 JSON 로깅 (correlation ID 포함)
- YAML 설정 로더 (환경변수 치환 지원)
- Alembic 데이터베이스 마이그레이션

#### 거래소 연동
- Bybit 선물 커넥터 (ccxt.pro 기반, REST + WebSocket)
- Binance 커넥터 (기본 구조)
- 거래소 팩토리 패턴 (`exchange_factory`)

#### 전략
- **[[strategies/funding_arb|펀딩 레이트 차익거래]]**: 델타 중립 포지션 관리, Basis Spread State Machine, 펀딩비 추적
- **[[strategies/adaptive_dca|적응형 DCA]]**: Fear & Greed 멀티플라이어, RSI/이동평균/변동성 기반 적응, 이익 실현 래더

#### 오케스트레이터
- [[architecture#2. Strategy Orchestrator|시장 레짐 기반 가중치 매트릭스]]
- EMA 스무딩 가중치 전환
- 포트폴리오 모니터링
- ML 레짐 분류 모델 통합
- 비유사도 지수 (Dissimilarity Index)
- LLM 어드바이저 가중치 조정 통합

#### 실행 엔진
- [[architecture#3. Execution Engine|Redis pub/sub 기반 주문 수신]]
- 안전성 검사 (Safety Guard)
- 동시 주문 제한 (세마포어)
- 재시도 로직 (지수 백오프, 최대 3회)
- 멱등성 보장 (request_id 중복 검사)

#### 리스크 관리
- [[architecture#Kill Switch 4단계|4단계 Kill Switch]] (전략/포트폴리오/시스템/수동)
- 쿨다운 및 자동 복구 메커니즘
- Sharpe/Sortino 비율 계산
- 최대 낙폭 추적
- 포지션 크기 계산기 (고정 분수법)
- 레버리지 검증

#### 시장 데이터
- [[architecture#1. Market Data Collector|실시간 OHLCV 수집기]]
- 기술적 지표 계산 (ADX, ATR, BB, EMA)
- 시장 레짐 분류기 (ranging/trending/volatile)
- Feature Engine (ML 모델용)
- 펀딩레이트 모니터

#### LLM 어드바이저
- [[architecture#5. LLM Advisor|Claude Code CLI 기반 분석 엔진]]
- 다단계 분석 그래프
- 차트 비전 분석
- 일일 회고 (Daily Reflection)
- 신뢰도 기반 가중치 조정

#### 백테스트 시스템
- [[architecture#백테스트 시스템|FreqtradeBridge]] (CLI + 내부 이벤트 엔진)
- Walk-Forward Analysis (슬라이딩 윈도우)
- Monte Carlo 시뮬레이션 (신뢰구간)
- HTML/Markdown 리포트 생성기

#### 운영 도구
- [[architecture#6. Telegram Bot|Telegram Bot]] (알림 + 수동 명령)
- [[architecture#7. Dashboard|Express.js Dashboard]] (내부/외부 API)
- Grafana 연동
- 데이터베이스 초기화 스크립트 (`init_db.py`)
- 히스토리 데이터 시드 스크립트 (`seed_historical.py`)
- 거래 내역 CSV 내보내기 (`export_trades.py`)
- 시스템 상태 점검 스크립트 (`health_check.py`)
- 월간 리포트 생성 스크립트 (`generate_monthly_report.py`)

#### 테스트
- pytest 기반 테스트 스위트
- 통합 테스트: Bybit 테스트넷, 실행 파이프라인, 펀딩 차익 E2E, 오케스트레이터 레짐

#### 문서
- [[architecture|시스템 아키텍처 문서]]
- [[runbook|운영 매뉴얼 (Runbook)]]
- [[api|내부 API 문서]]
- 변경 이력 (이 문서)

---

## [미정] - 향후 계획

### 계획 (Planned)
- OKX 거래소 커넥터 추가
- 옵션 전략 (커버드 콜) 구현
- 웹 기반 대시보드 프론트엔드 (React)
- Prometheus + AlertManager 모니터링 통합
- CI/CD 파이프라인 (GitHub Actions)
- 실시간 성과 알림 고도화
- 다중 계정 지원
