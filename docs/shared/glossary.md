---
title: 프로젝트 용어집
last_updated: 2026-08-31
---

# 프로젝트 용어집

60개 이상의 현재 활성 거래, 기술, 운영 용어를 정의합니다. 용어는 카테고리별로 정렬되어 있습니다.

## 거래 & 전략 용어

### Supertrend (슈퍼트렌드)

**메인 전략** (Phase 5 실전 중). ATR(Average True Range) 기반 추세추종 지표를 활용한 BTC 4시간 장기 추세 추종 전략.

**작동 원리**:
- 상승 추세: Supertrend 상승선 + EMA(7) > EMA(29)에서 롱 진입 (3x 레버리지)
- 하락 추세: 추세선 붕괴 또는 EMA 하강 교차 시 청산
- 장기 필터: EMA(240) > Price (상승 구간만 거래)
- 손절: ATR 기반 하방 거리 초과 (`가격 ≤ 진입가 − ATR×3.3`) 또는 EMA 데드크로스. ATR 익절 없음 (2026-08-20~).

**백테스트 성과** (Bybit 네이티브 4h, 2017-08-17~2026-04-30):
- CAGR: +219.06% (연환산)
- Sharpe: 1.667 (위험 조정 효율)
- MDD: -66.70% (⚠️ 고위험, 사용자 승인)
- 거래 수: 198회 (충분한 샘플)

**파라미터**: `supertrend_4h_x3_7908` (combo #7908)
- 기간: 4시간 봉
- 방향: Long-only (롱 포지션만, 숏 없음)
- 레버리지: 3x (하드 리밋)
- Supertrend: period=9, multiplier=2.6
- EMA: 빠른=7, 느린=29, 추세=240

### ATR (Average True Range)

변동성 지표로, 일정 기간 내 실제 변동폭의 평균. Supertrend 계산의 핵심 요소로 사용되어 추세선의 높이를 동적으로 조정.

- **높은 ATR**: 변동성 커짐 → 추세선 거리 증가 (손절 여유)
- **낮은 ATR**: 변동성 작음 → 추세선 거리 감소 (신호 민감)

### combo #7908

Supertrend 4h 3x long-only 전략의 파라미터 조합 번호. 매개변수 스윕(sweep) 분석에서 최종 선정된 최적 조합.
- Supertrend period=9, multiplier=2.6
- EMA 기간들 (7, 29, 240)
- ATR 배수 3.3

### sweet_spot_score (스윗스팟 스코어)

매개변수 스윕 분석에서 여러 후보 조합을 평가하는 복합 점수. CAGR, Sharpe, MDD, 거래 수 등을 종합하여 최적 균형을 찾는 메트릭.

- 점수 계산: 수익성(CAGR) + 효율성(Sharpe) + 안정성(MDD 페널티) 조합
- combo #7908이 최고 점수 획득 (따라서 실전 채택)

### long-only (롱 전용)

롱 포지션(매수)만 거래하고, 숏 포지션(매도)은 전혀 거래하지 않는 운영 모드.
- Supertrend: 상승 추세에서만 진입, 하락 추세는 회피 (포지션 0)
- 장점: 구조적 상승 편향, 청산 비용 낮음 (1회 진입/청산)
- 단점: 약세장에서 수익 불가 (현금 대기)

## 설정 / 파라미터 (Configuration)

### supertrend_4h_x3_7908

현재 채택된 메인 전략 설정의 약자:
- **Supertrend**: ATR 기반 추세추종 지표
- **4h**: 4시간 봉 기반 거래
- **3x**: 3배 레버리지 (하드 리밋)
- **7908**: 매개변수 스윕 분석 combo #7908 선정

**백테스트 성과** (Bybit 네이티브 4h, 2026-08-20 ATR 익절 제거):
- CAGR +219.06%, Sharpe 1.667, MDD -66.70% (198 trades)
- 사용자 승인: MDD -66.70% 고위험 수용

### PHASE5_MODE

Phase 5 실전 모드 활성화 플래그. `true`일 때:
- 절대값 AND Kill Switch 활성화
- 잔고 게이트(Redis baseline / `EXPECTED_INITIAL_BALANCE_USD`) 적용
- 사이징은 전략 yaml의 **pct_equity 95% × 레버리지 3x** (구 문서의 fixed_notional $150은 폐기)

### EXPECTED_INITIAL_BALANCE_USD

Phase 5 잔고 게이트의 **폴백** 기준 자본. 메인넷 기동 시 실잔고와 대비 검증(허용 5%).

기동 시 우선순위:
1. Redis `ce:phase5:equity_baseline` (execution-engine이 운영 중 자동 갱신)
2. 이 환경 변수 (콜드스타트 / Redis wipe)

현재 운영값: **$238.88 USDT** (2026-08-29 청산 후 `.env` 현행화, gitignore).  
역사: Phase 5 개시 2026-05-18 $185.31 → 중간 스냅샷 $181.99(2026-08-04 무렵) → 당일 익절 청산 후 $238.88.

### ce:phase5:equity_baseline

전원 장애 후 자동 복구용 Redis 영속 키. JSON `{"equity", "updated_at", "source"}`.
TTL 없음. `source`는 `runtime` / `startup_ok`.

### pct_equity (Phase 5 사이징)

**정본**: 오케스트레이터가 배분한 `allocated_capital`의 **95%** 를 레버리지 3x와 곱해 명목 수량을 만든다 (`supertrend.yaml` `sizing.pct_equity`). 최소 명목 $65.

구 서술 `fixed_notional $150`은 Phase 5 초안 잔재이며 **사용하지 않는다**.

### BYBIT_TESTNET (환경 변수)

Bybit 거래소 모드 플래그:
- `true`: 테스트넷 모드 (모의 거래, 실전 자금 무관, Phase 4)
- `false`: 메인넷 모드 (실전 거래, Phase 5+)

**절대 규칙**: Phase 4 완료 전까지는 반드시 `true`

**변경 절차**: `docs/shared/70-policy.md` (메인넷 전환 절)

### STRICT_MONITORING_HOURS

Phase 5 강화 모니터링 시간. 설정값만큼 매시간 상태 리포트 강제 발송.

기본값: 24시간

## 시장 & 데이터 (Market & Data)

### ADX (Average Directional Index)

추세의 강도를 측정하는 기술 지표 (0-100).

- **> 25**: 강한 추세 (Trending)
- **20-25**: 중간 추세
- **< 20**: 약한 추세 (Ranging)

### OHLCV

시장 캔들 데이터의 기본 단위:
- **O**pen: 개시가
- **H**igh: 최고가
- **L**ow: 최저가
- **C**lose: 종가
- **V**olume: 거래량

**저장 위치**: PostgreSQL `ohlcv_history` 테이블 (라이브, Bybit USDT 무기한)
**보존 정책**: 운영 수집은 **4h만 영구**. 기타 타임프레임 잔여는 retention 잡이 삭제. `timestamp`는 봉 **시작 시각**(UTC). 17:00 KST에 마감된 4h 봉 = `2026-08-29 04:00:00+00` 형태의 행.

⚠️ **백테스트 정본(`backtest/`, `jesse_db.ohlcv_4h`)은 이 테이블과 출처가 다르다** — Binance **현물**이다. 격자(00계열 4h)는 동일하지만 종가는 평균 +0.05% 차이가 나고 무기한 펀딩비가 없다. 2026-08-31 이전에는 `jesse_db.exchange` 컬럼이 `'Bybit Perpetual'`로 잘못 라벨링돼 있었다(현재 `'Binance Spot'`으로 정정). 상세: `backtest/results/2026-08-31/csv_ohlcv_drift.md`.

### TWAP (Time-Weighted Average Price)

시간 가중 평균 가격. 대형 주문($5,000 이상)을 여러 소주문으로 분할하여 평균 체결가 계산.

**목적**: 시장 임팩트 최소화 (슬리피지 방지)

### 펀딩레이트 (Funding Rate)

선물 시장에서 8시간마다 지급/수취되는 이자. 롱과 숏 사이의 자금 이전으로, 시장 수급을 조절.

**예**: 0.01% per 8h = 연 27% (기준)
- 양수: 롱이 숏에게 지급 (매수 압력 높음)
- 음수: 숏이 롱에게 지급 (매도 압력 높음)

**주의**: Supertrend는 펀딩레이트 무관, 추세 신호 기반 거래 — 진입/청산 신호에 펀딩레이트를 쓰지 않는다는 뜻이며, **비용으로도 무관하다는 뜻은 아니다**. 정본 백테스트 시세가 Binance 현물이라 캔들 자체에 펀딩비가 존재하지 않아, `#7908`의 정본 성과(CAGR +219.06% 등)는 **무기한 보유 펀딩비를 구조적으로 0으로 가정**한다. 라이브는 실제로 펀딩비를 지불한다(롱 보유가 일반적으로 지불 쪽). 미정량 — `backtest/results/2026-08-31/csv_ohlcv_drift.md` §7.

## 안전 & 리스크 (Safety & Risk)

### Kill Switch (킬스위치)

4단계 계층 손실 방어 시스템. 손실이 임계값을 초과하면 자동으로 포지션을 강제 청산.

**4단계 구조**:

| 레벨 | 이름 | 트리거 | 동작 | 복구 |
|------|------|--------|------|------|
| 1 | STRATEGY | 개별 전략 손절 (일 -5%, 주 -10%, 월 -15%) | 해당 전략만 중지 + 포지션 청산 | 60분 cooldown |
| 2 | PORTFOLIO | 포트폴리오 손실 (일 -5% AND $10, 주 -10% AND $20, 월 -15% AND $30) | **모든 전략 중지** + **전체 포지션 청산** | 60분 cooldown |
| 3 | SYSTEM | API 연결 실패, DB/Redis 다운, EE 하트비트 5분 미수신 | 시장가 청산 시도 | 자동 불가 (수동/재시작) |
| 4 | MANUAL | `ce:kill_switch` 외부 발행 / Telegram `/emergency_close` | 즉시 **모든 포지션 청산** | auto-resume 불가 |

**Phase 5 특수**: Level 2는 퍼센트 AND 절대값 USD 둘 다 조건 (예: -5% AND $10 손실)

**임계값 확인**: `redis-cli GET ce:kill_switch:active`

### KillLevel (IntEnum)

Kill Switch 레벨을 나타내는 정수형 열거형:
- NONE = 0
- STRATEGY = 1
- PORTFOLIO = 2
- SYSTEM = 3
- MANUAL = 4

### 포지션 보호 (Position Protection)

배포·재시작 시 `supertrend` 전략의 포지션 보호 메커니즘:

1. **service_shutdown 사유**: 포지션 청산 안 함
2. **Redis 저장**: 포지션 상태, 진입가, P&L 저장 (TTL 1시간)
3. **자동 복구**: 재시작 후 1시간 내 자동으로 포지션 복구

**효과**: 불필요한 청산 수수료 절감 (매회 0.05% × 2), 거래 연속성 유지

**TTL 초과 시**: 1시간 초과 중단 후 재시작하면 Redis 데이터 만료 → 신규 시작 (거래소 포지션은 수동 청산 필요)

### Dead Man's Switch (데드맨 스위치)

하트비트 기반 자동 장애 감지:
- **하트비트 간격**: 30초 (Redis 키 갱신)
- **Watchdog 체크**: 60초마다 마지막 하트비트 확인
- **미수신 판정**: 5분(300초) 미수신 → KillSwitch L3 자동 발동
- **목표**: 서비스 다운 시에도 자동으로 포지션 보호

### 엣지 소멸 트립와이어 (Edge-Decay Tripwire, 2026-08-31~)

Kill Switch가 **손실 회로차단기**(얼마나 잃었는가)인 것과 달리, 전략의 **신호 자체가 통계적으로 살아있는지**(엣지 검정)를 재는 별도 사전등록 장치. `backtest/scripts/analysis/tripwire_check.py`.

- **참조 분포**: `#7908` 설계구간(2017-10~2025-01) 15블록 로그성장의 분위수(최솟값 −0.578, P25 −0.016, 중앙값 +0.324) — `backtest/results/v12/PREREGISTRATION.md` §4에서 고정, 이후 재계산 금지
- **T1 (월간 워닝)**: 직전 182일 트레일링 로그성장 < P25 → 로그 기록 + 수동 알림. **자동 행동 없음** (트레일링 윈도우라 통계적 독립시행 아님)
- **T2 (블록 검토 게이트)**: 클린(비오염) 완결 6개월 블록에서 (a) P25 미만 2연속 또는 (b) 설계 최솟값 미만 1회 → **사람 검토 게이트** 발동. 기본 권고 = 신규 진입 중단(기존 포지션은 정상 청산까지 유지)
- **오염 고지**: 2026-08-31까지 관측된 데이터는 판정에 쓰지 않는다(사후 임계값 조정과 구분 불가하므로). 첫 클린 T2 판정 가능 시점은 **2027-04-01**
- **자동화 아님**: CLAUDE.md 불변규칙 #7(항상 95%×3x 고정배분)과 충돌하지 않도록, T2 발동 시에도 시스템이 배분을 자동으로 바꾸지 않는다 — 사전등록된 해석 + 사람 결정

상세: `backtest/results/tripwire/PREREGISTRATION_TRIPWIRE.md`, 판정 로그 `backtest/results/tripwire/log.md`.

### ACK (Acknowledge)

Kill Switch Level 2 발동 후 실행 확인 응답. Telegram에서 `/acknowledge` 또는 `/ack` 입력.

**타임아웃**: 5초
**재시도**: 최대 3회
**필요 이유**: 사용자가 상황을 인지했는지 확인하고, 추가 조치 방향 결정

## 운영 & Phase (Operations & Phases)

### Phase 4 (테스트넷 포워드 테스트) — 완료

**목표**: 실전과 동일한 환경에서 7일 이상 무중단 운영 검증

**특징** (과거):
- Bybit 테스트넷 (`BYBIT_TESTNET=true`)
- 현금 50% 버퍼 (포지션 축소 용이)
- pct_equity 포지션 사이징
- 상대값 Kill Switch (절대값 무관)

**완료**: 2026-05-18 (Supertrend 채택 후 Phase 5로 즉시 전환)

### Phase 5 (소액 실전 운영 중) — 활성

**목표**: $200 USDT로 메인넷 소액 실전 운영 (Supertrend 4h 3x)

**현황**:
- Bybit 메인넷 (`BYBIT_TESTNET=false`)
- `PHASE5_MODE=true`
- **pct_equity 95% × 3x** (고정 $150 명목 아님)
- 절대값 AND Kill Switch
- 레버리지 3x
- 시작: 2026-05-18 ($185.31). **2026-08-29 청산 후 지갑 ≈ $238.88**

**진입 조건**: Phase 4 완료 + phase5_preflight.py 8개 항목 PASS + 사용자 승인

### Walk-Forward (WF / 월간 포워드 테스트)

월간 자동 파라미터 재최적화 및 검증 프로세스. 구동 서비스 `wf-scheduler`는 2026-08-29 전량 삭제됨 (FA 시대 잔재) — 현재 자동화 없음, 수동 검토로 대체.

**이전 동작** (아카이브됨):
- **일시**: 매월 1일 02:00 KST
- **데이터**: 최근 6개월 (IS 3개월 + OOS 3개월)
- **최적화**: IS에서 파라미터 재최적화
- **검증**: OOS에서 성과 검증
- **결과**: Telegram 자동 전송

**목표**: 과최적화 방지, 파라미터 드리프트 감지

### OOS / IS (Out-of-Sample / In-Sample)

**WF 분석에서의 학습/검증 구간**:
- **IS (In-Sample, 학습 구간)**: 처음 3개월 데이터로 파라미터 최적화
- **OOS (Out-of-Sample, 검증 구간)**: 다음 3개월 데이터로 독립적 성과 검증

**과최적화 방지**: OOS 성과가 IS보다 30% 이상 악화되면 경고

## 거래소 & API (Exchange & API)

### CCXT (Cryptocurrency eXchange Trading Library)

크립토 거래소 통일 API 라이브러리. Bybit, Binance, OKX 등 150+ 거래소를 단일 인터페이스로 제공.

**사용**: `shared/exchange/bybit.py` (Bybit 래퍼)

### WebSocket (WS)

실시간 양방향 통신 프로토콜. Bybit에서 다음 데이터를 실시간 푸시:
- OHLCV (캔들 데이터)
- 호가 (Order Book)
- 펀딩레이트 (Funding Rate)
- 거래 (Trade)

**이점**: HTTP 폴링(지연)보다 저레이턴시 (< 100ms)

### API Rate Limit

거래소 API 호출 제한. Bybit 기본:
- **일반 엔드포인트**: 분당 최대 600회
- **주문 엔드포인트**: 분당 최대 100회

**대응**: 요청 배치, 재시도 로직 (`retry_backoff`)

### fail-closed env (`require_env`)

2026-08-29부터 서비스·스크립트는 `DB_PASSWORD` / `REDIS_URL`(또는 `REDIS_PASSWORD`)이 없으면 **기동 거부**. compose는 `${DB_PASSWORD:?...}`. 소스·문서에 실비밀번호를 넣지 않는다. 구현: `cryptoengine/shared/required_env.py`. 로그 URL은 `redact_url()`.

### 018 / D9 (2026-08-29)

- **018**: `018_drop_legacy_tables.sql` — 레거시·분기물 테이블 DROP. 라이브 DB ~306MB.
- **D9**: `git filter-repo`로 히스토리 평문 자격증명 제거. 모든 커밋 해시 변경. 정본 `origin/main` = `9f5b116f`. 태그 `legacy-archive-2026-08-29` = `2ee11756`.

---

### PostgreSQL

영구 데이터 저장소. 거래, 포지션, 이벤트 로그 등 모든 이력 기록.

**핵심 테이블** (018 DROP 이후, ~306MB):
- `trades` — 체결 로그 (텔레그램 `/report`가 SELECT. DROP 금지)
- `positions` / `orders` / `supertrend_signals`
- `ohlcv_history` — 4h 정본 캔들
- `funding_rate_history` / `funding_payments`
- `kill_switch_events` / `service_logs` / `portfolio_snapshots` / `strategy_states`
- `llm_judgments` / `llm_reports` (대시보드 API. 행이 비어도 DROP 금지)
- `daily_reports` — 컬럼 `daily_pnl` 있음. **`daily_pnl` 테이블은 없음** (`GET /api/pnl` 500은 기존 버그)
- `open_interest_history` — 빈 껍데기일 수 있음 (OI는 Redis 위주)

분기·레짐·멀티거래소 테이블은 2026-08-29 `018`으로 DROP. 상세 ADR-0006 · ADR-0010.

### Redis

인메모리 고속 캐시 및 메시지 브로커.

**역할**:
- **Pub/Sub**: 서비스 간 실시간 통신 (시장 데이터, 주문, 명령)
- **캐시**: 포지션 상태, 포트폴리오 스냅샷
- **포지션 복구**: service_shutdown 시 상태 저장 (TTL 1시간)

**중요 키**:
- `strategy:saved_state:supertrend-01` — Supertrend 포지션 상태 복구용
- `ce:kill_switch:active` — Kill Switch 활성 상태

### structlog

구조화 로깅 라이브러리. 각 이벤트를 JSON 형식으로 기록하여 검색 및 분석 용이.

**이점**: 시간순 추적, 이벤트 필터링, Grafana 시각화 가능

## 인프라 (Infrastructure)

### Docker Compose

컨테이너로 서비스·DB·모니터링 도구를 정의 및 협주 (2026-08-29 기준 3개 compose 프로젝트: 운영 13종 + 백테스트 2종 + 대시보드 1종 = 16개 컨테이너). Track-C(멀티거래소) 2종은 2026-08-29 전량 삭제.

**핵심 명령**:
```bash
docker compose up -d               # 전체 시작
docker compose down                # 전체 정지
docker compose up -d --build <svc> # 특정 서비스 재빌드 + 재시작
docker compose logs -f <svc>       # 실시간 로그
```

### Grafana

시계열 데이터 시각화 및 실시간 모니터링 대시보드.

**데이터소스**:
- PostgreSQL: trades, positions, kill_switch_events
- Prometheus: CPU, 메모리, Redis, API 레이턴시
- Redis: 실시간 메트릭

**포트**: http://localhost:3002 (admin / `$GRAFANA_ADMIN_PASSWORD`)

### Prometheus

메트릭 수집 및 시계열 저장 시스템.

**수집 대상**:
- node_exporter: CPU, 메모리, 디스크, 네트워크
- redis_exporter: Redis 메모리, 키 수, 명령 수
- 애플리케이션: 주문 건수, 에러율, 레이턴시

**데이터 보존**: 30일

## 성과 지표 (Performance Metrics)

### CAGR (Compound Annual Growth Rate)

연평균 복합 수익률. 초기 자본이 매년 평균 몇 %씩 불어났는지 나타내는 지표.

**계산**: ((최종값 / 초기값) ^ (1/년수)) - 1

**Supertrend 4h 3x (Phase 5)**: +219.06% (Bybit 네이티브 4h, ATR 손절만, 2017-08-17~2026-04-30)

**목표**: > 15% (연환산 기본 기준)

### Sharpe Ratio

위험 조정 수익률 지표. (평균 수익률 - 무위험 수익률) / 표준편차

**해석**:
- > 3.0: 우수 (위험 대비 매우 효율적)
- 2.0-3.0: 양호
- 1.0-2.0: 보통 (적절)
- < 1.0: 미흡

**Supertrend 4h 3x (Phase 5)**: 1.667

### MDD (Maximum Drawdown)

포트폴리오가 최고점에서 최저점까지 내려간 최대 낙폭.

**계산**: (최저점 - 최고점) / 최고점 × 100%

**Supertrend 4h 3x (Phase 5 실전)**: -66.70% (⚠️ **고위험**)
- 사용자 승인: 고수익성(+219.06% CAGR)과 고위험(-66.70% MDD)의 트레이드오프 명시 수용
- 포지션 보호: Kill Switch 4단계 + 강화 모니터링으로 실제 손실 제한

### Win Rate (승률)

수익 거래 / 전체 거래 × 100%

**현재 설정**: > 48% (Supertrend combo #7908)

**해석**: 거래 빈도 대비 수익률 안정성 지표

### Sortino Ratio

Sharpe과 유사하나, 하락 변동성만 고려 (상승 변동성 무시).

**특징**: 변동성 자체보다 손실 리스크에 중점
