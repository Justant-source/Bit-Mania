---
title: 프로젝트 용어집
last_updated: 2026-05-01
---

# 프로젝트 용어집

95개 이상의 거래, 기술, 운영 용어를 정의합니다. 용어는 카테고리별로 정렬되어 있습니다.

## 거래 & 전략 용어

### FA (Funding Arb / 펀딩비 차익거래)

**핵심 전략**. Bybit 선물 시장에서 **델타 뉴트럴 포지션**(현물 롱 + 무기한 선물 숏)을 유지하며 8시간 주기(3회/일)로 지급되는 펀딩비를 수취하는 전략.

**작동 원리**:
- 현물 시장에서 BTC 매수 (예: 0.1 BTC @ $65,000)
- 동시에 선물 시장에서 동량 숏 (0.1 BTC)
- 가격 변동 위험 = 0 (Long + Short = Delta Neutral)
- 8시간마다 펀딩비 수취 (기준 0.01% per 8h = 연 27%)

**현재 설정**: `fa80_lev5_r30` (자본 80% 배분, 5x 레버리지, 수익 30% 재투자)

**백테스트 검증** (6년 데이터, 2019-2025):
- CAGR: +34.87% (연환산)
- Sharpe: 3.583 (수익률 vs 리스크 효율)
- MDD: -4.52% (최대 낙폭)
- 청산: 0회 (6년 무중단)

```mermaid
graph LR
    SPOT["현물 BTC 매수\n(Long)"] --> NEUTRAL["델타 뉴트럴\n가격 노출 0"]
    PERP["선물 BTC 매도\n(Short × 5x)"] --> NEUTRAL
    NEUTRAL --> INCOME["펀딩비 수취\n8h × 3회/일"]
    INCOME --> RATE["연환산 30~35%\n(fa80_lev5_r30 기준)"]

    style NEUTRAL fill:#e8f5e9
    style INCOME fill:#fff3e0,stroke:#ff9800
    style RATE fill:#4caf50,color:#fff
```

### DCA (Dollar Cost Averaging / 적응형 적립식)

**보조 전략**. Fear & Greed Index 기반으로 시간 간격을 두고 정기적으로 BTC를 매수하는 장기 적립식 투자. 현재 Phase 4에서 가중치 0% (비활성).

### 델타 뉴트럴 (Delta Neutral)

롱 포지션과 숏 포지션의 수량이 동일하여 기초자산 가격 변동에 대한 순수익 위험도가 0인 상태.

**예시**: BTC 1롱 + BTC 1숏 @ $65,000
- 가격 → $70,000: 롱 +$5,000, 숏 -$5,000 = 0
- 펀딩비 수익: 순이익 (가격 무관)

### BEP (Break-Even Price)

포지션의 손익분기점. 이 가격에서 청산하면 손익이 0이 되는 지점. FA에서는 현물과 선물의 기저차(basis) 고려.

### 기저차 (Basis)

선물 가격 - 현물 가격. FA 전략에서는 수익/손실 판정 기준.

- 양의 기저차(Contango): 선물이 현물보다 비쌈 (수익 기대)
- 음의 기저차(Backwardation): 선물이 현물보다 쌈 (손실 우려, 청산)

## 설정 / 파라미터 (Configuration)

### fa80_lev5_r30

현재 채택된 포트폴리오 설정의 약자:
- **FA 80%**: 자본의 80%를 Funding Arb에 배분
- **Lev 5x**: 선물 포지션 레버리지 5배
- **Reinv 30%**: 실현 수익의 30%를 BTC 현물 재투자

**대체 설정**:
- `fa80_lev4_r30`: CAGR +28.56%, Sharpe 3.556 (더 보수적)
- `fa80_lev5_r50`: CAGR +33.54%, Sharpe 1.867 (더 공격적)

### fa_capital_ratio

FA 전략에 배분하는 전체 포트폴리오 비율. 기본값: 0.80 (80%).

Phase 5에서는 0.75 (75%)로 조정 (소액 리스크 관리).

### reinvest_ratio

실현 수익 중 BTC 현물 재투자 비율. 기본값: 0.30 (30%).

Phase 5에서는 0.0 (비활성, 소액에서 무의미).

### fixed_notional vs pct_equity

**Phase 4 (테스트넷)**: `pct_equity` — 전체 자본의 일정 비율로 포지션 사이징

**Phase 5 (메인넷)**: `fixed_notional` — 고정 명목가($150)로 사이징
- 자본 규모 변화에 불감
- 매 거래 크기 예측 용이
- 소액($200) 운영에 최적화

### BYBIT_TESTNET (환경 변수)

Bybit 거래소 모드 플래그:
- `true`: 테스트넷 모드 (모의 거래, 실전 자금 무관, Phase 4)
- `false`: 메인넷 모드 (실전 거래, Phase 5+)

**절대 규칙**: Phase 4 완료 전까지는 반드시 `true`

**변경 절차**: [policies/operations/mainnet-switch.md](policies/operations/mainnet-switch.md) 참조

### PHASE5_MODE

Phase 5 실전 모드 활성화 플래그. `true`일 때:
- fixed_notional 사이징 활성화
- 절대값 AND Kill Switch 활성화
- STRICT_MONITORING_HOURS 강제 모니터링

### EXPECTED_INITIAL_BALANCE_USD

Phase 5 초기 자본 잔고. 메인넷 진입 시 실제 잔고와 대비하여 검증.

기본값: $200 USDT

### STRICT_MONITORING_HOURS

Phase 5 강화 모니터링 시간. 설정값만큼 매시간 상태 리포트 강제 발송.

기본값: 24시간

### consecutive_intervals

FA 진입 조건: 연속으로 펀딩비 임계값을 초과한 횟수.

- **Phase 4**: 3회 (기본)
- **Phase 5**: 4회 (더 보수적)

## 시장 & 데이터 (Market & Data)

### 레짐 (Regime)

시장 상황을 5분 캔들 기반 HMM(Hidden Markov Model)으로 분류한 상태.

**4가지 레짐**:
1. **Trending Up** — 일정한 상향 추세장
2. **Trending Down** — 일정한 하향 추세장
3. **Ranging** — 상하한 사이를 오르락내리락하는 박스권
4. **Volatile** — 급변동하는 변동성장

**감지 메커니즘**: ADX(Average Directional Index), 볼린저밴드 폭, 변동성 지표 조합

**전략별 대응** (config/orchestrator.yaml):
- Trending (up/down): FA 진입 유리 (기저차 확대), 가중치 유지
- Ranging: FA 최적 (회귀 거래), 가중치 +10%
- Volatile: 위험 증가, FA 가중치 -20% (현금 확대)

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

**저장 위치**: PostgreSQL `ohlcv_history` 테이블
**보존 정책**: 타임프레임별 자동 삭제 (90일 이상 자동 정리)

### TWAP (Time-Weighted Average Price)

시간 가중 평균 가격. 대형 주문($5,000 이상)을 여러 소주문으로 분할하여 평균 체결가 계산.

**목적**: 시장 임팩트 최소화 (슬리피지 방지)

### 펀딩레이트 (Funding Rate)

선물 시장에서 8시간마다 지급/수취되는 이자. 롱과 숏 사이의 자금 이전으로, 시장 수급을 조절.

**예**: 0.01% per 8h = 연 27% (기준)
- 양수: 롱이 숏에게 지급 (매수 압력 높음)
- 음수: 숏이 롱에게 지급 (매도 압력 높음)

**FA 진입 기준**: 펀딩레이트 > 0.01%, consecutive_intervals 조건 만족
**청산 기준**: 펀딩레이트 < 0 (음수 반전) 또는 수익 실현

## 안전 & 리스크 (Safety & Risk)

### Kill Switch (킬스위치)

4단계 계층 손실 방어 시스템. 손실이 임계값을 초과하면 자동으로 포지션을 강제 청산.

**4단계 구조**:

| 레벨 | 이름 | 트리거 | 동작 | 복구 |
|------|------|--------|------|------|
| 1 | STRATEGY | 개별 전략 손절 (일 -3%, 주 -7%, 월 -12%) | 해당 전략만 중지 + 포지션 청산 | 4시간 쿨다운 후 자동 재개 |
| 2 | PORTFOLIO | 포트폴리오 손실 (일 -5%, 주 -10%, 월 -15%) | **모든 전략 중지** + **전체 포지션 청산** | 1시간 쿨다운 후 재개 |
| 3 | SYSTEM | API 연결 실패, DB/Redis 다운 | 시장가 청산 시도 → 실패 시 수동 개입 대기 | 자동 불가 (수동) |
| 4 | MANUAL | Telegram 명령 또는 SSH | 즉시 **모든 포지션 청산** | 수동 `/resume` |

**Phase 5 특수**: Level 2는 퍼센트 AND 절대값 USD 둘 다 조건 (예: -5% AND $50 손실)

**임계값 확인**: `redis-cli GET ce:kill_switch:active`

```mermaid
graph TD
    KS["KillSwitch\nshared/kill_switch.py"] --> L1["L1 STRATEGY\n전략 단위 정지"]
    KS --> L2["L2 PORTFOLIO\n전체 포트폴리오 정지"]
    KS --> L3["L3 SYSTEM\n시스템 장애 감지"]
    KS --> L4["L4 MANUAL\n운영자 수동 발동"]

    L1 -->|"4h 쿨다운"| AUTO["자동 재개"]
    L2 -->|"4h 쿨다운"| AUTO
    L3 -->|"수동 reset"| MANUAL_R["운영자 /reset"]
    L4 -->|"수동 reset만"| MANUAL_R

    style L4 fill:#b71c1c,color:#fff
    style L3 fill:#e53935,color:#fff
    style L2 fill:#ef6c00,color:#fff
    style L1 fill:#f9a825,color:#000
```

### KillLevel (IntEnum)

Kill Switch 레벨을 나타내는 정수형 열거형:
- NONE = 0
- STRATEGY = 1
- PORTFOLIO = 2
- SYSTEM = 3
- MANUAL = 4

### 포지션 보호 (Position Protection)

배포·재시작 시 `funding-arb` 전략의 포지션 보호 메커니즘:

1. **service_shutdown 사유**: 포지션 청산 안 함
2. **Redis 저장**: 포지션 상태, 진입가, 펀딩비 누적액 저장 (TTL 1시간)
3. **자동 복구**: 재시작 후 1시간 내 자동으로 포지션 복구

**효과**: 불필요한 청산 수수료 절감 (매회 0.05% × 2)

**TTL 초과 시**: 1시간 초과 중단 후 재시작하면 Redis 데이터 만료 → 신규 시작 (거래소 포지션은 수동 청산 필요)

### Dead Man's Switch (데드맨 스위치)

하트비트 기반 자동 장애 감지:
- **하트비트 간격**: 30초 (Redis 키 갱신)
- **Watchdog 체크**: 60초마다 마지막 하트비트 확인
- **미수신 판정**: 5분(300초) 미수신 → KillSwitch L3 자동 발동
- **목표**: 서비스 다운 시에도 자동으로 포지션 보호

### ACK (Acknowledge)

Kill Switch Level 2 발동 후 실행 확인 응답. Telegram에서 `/acknowledge` 또는 `/ack` 입력.

**타임아웃**: 5초
**재시도**: 최대 3회
**필요 이유**: 사용자가 상황을 인지했는지 확인하고, 추가 조치 방향 결정

## 운영 & Phase (Operations & Phases)

### Phase 4 (테스트넷 포워드 테스트)

**목표**: 실전과 동일한 환경에서 7일 이상 무중단 운영 검증

**특징**:
- Bybit 테스트넷 (`BYBIT_TESTNET=true`)
- FA 단독 운영 (DCA 비활성)
- 현금 50% 버퍼 (포지션 축소 용이)
- pct_equity 포지션 사이징
- 상대값 Kill Switch (절대값 무관)

**완료 기준** (7개 시나리오):
1. 무중단 운영 7일 이상
2. Kill Switch 4단계 모두 동작 확인
3. Telegram 알림 모든 유형 수신
4. 포지션 진입/청산/재시작 정상 동작
5. 스톱로스 온 익스체인지 동작 확인
6. Walk-Forward 월간 파이프라인 1회 이상 성공
7. phase5_preflight.py 8개 항목 PASS

### Phase 5 (소액 실전)

**목표**: $200 USDT로 메인넷 소액 실전 운영

**특징**:
- Bybit 메인넷 (`BYBIT_TESTNET=false`)
- `PHASE5_MODE=true` (고급 설정)
- fixed_notional 포지션 사이징 ($150)
- 절대값 AND Kill Switch (상대값 + USD 둘 다)
- STRICT_MONITORING_HOURS=24 (첫 24시간 강화)
- 레버리지 5x 이하 강제

**전환 절차**: [policies/operations/mainnet-switch.md](policies/operations/mainnet-switch.md)

### Walk-Forward (WF / 월간 포워드 테스트)

월간 자동 파라미터 재최적화 및 검증 프로세스.

**동작**:
- **일시**: 매월 1일 02:00 KST
- **데이터**: 최근 6개월 (IS 3개월 + OOS 3개월)
- **최적화**: IS(In-Sample)에서 fa80_lev5_r30 파라미터 최적화
- **검증**: OOS(Out-of-Sample)에서 성과 검증
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

## 데이터베이스 & 로깅 (Database & Logging)

### PostgreSQL

영구 데이터 저장소. 거래, 포지션, 펀딩비, 이벤트 로그 등 모든 이력 기록.

**핵심 테이블**:
- `trades` — 체결 거래 이력
- `positions` — 포지션 상태 스냅샷
- `funding_payments` — 펀딩비 수령 기록
- `funding_rate_history` — 펀딩레이트 히스토리
- `ohlcv_history` — OHLCV 캔들 (90일 보존)
- `kill_switch_events` — Kill Switch 발동 이력
- `service_logs` — 구조화 이벤트 로그 (모든 서비스)
- `regime_transitions` — 레짐 전환 이벤트

### Redis

인메모리 고속 캐시 및 메시지 브로커.

**역할**:
- **Pub/Sub**: 서비스 간 실시간 통신 (시장 데이터, 주문, 명령)
- **캐시**: 포지션 상태, 포트폴리오 스냅샷, 레짐 정보
- **포지션 복구**: service_shutdown 시 상태 저장 (TTL 1시간)

**중요 키**:
- `market:regime:current` — 현재 시장 레짐
- `strategy:saved_state:funding_arb` — FA 포지션 상태 복구용
- `ce:kill_switch:active` — Kill Switch 활성 상태

### structlog

구조화 로깅 라이브러리. 각 이벤트를 JSON 형식으로 기록하여 검색 및 분석 용이.

**이점**: 시간순 추적, 이벤트 필터링, Grafana 시각화 가능

## 인프라 (Infrastructure)

### Docker Compose

19개 마이크로서비스, DB, 모니터링 도구를 컨테이너로 정의 및 협주.

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

**포트**: http://localhost:3002 (admin / ***REMOVED***)

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

**현재 설정**: +34.87% (fa80_lev5_r30, 6년 백테스트)

**목표**: > 15% (연환산)

### Sharpe Ratio

위험 조정 수익률 지표. (평균 수익률 - 무위험 수익률) / 표준편차

**해석**:
- > 3.0: 우수 (위험 대비 매우 효율적)
- 2.0-3.0: 양호
- 1.0-2.0: 보통
- < 1.0: 미흡

**현재 설정**: 3.583 (매우 우수)

### MDD (Maximum Drawdown)

포트폴리오가 최고점에서 최저점까지 내려간 최대 낙폭.

**계산**: (최저점 - 최고점) / 최고점 × 100%

**현재 설정**: -4.52% (매우 안정적)

**목표**: > -5% (손실 제한)

### Win Rate (승률)

수익 거래 / 전체 거래 × 100%

**현재 설정**: > 60% (예상)

**해석**: 거래 빈도 대비 수익률 안정성 지표

### Sortino Ratio

Sharpe과 유사하나, 하락 변동성만 고려 (상승 변동성 무시).

**특징**: 변동성 자체보다 손실 리스크에 중점

**현재 설정**: 더 높은 값 (MDD 작으므로)
