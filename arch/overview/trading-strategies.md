# CryptoEngine 트레이딩 전략 아키텍처

## 개요

CryptoEngine은 비트코인 선물 자동매매 시스템으로, 1개의 핵심 전략과 2개의 보조 전략을 Strategy Orchestrator가 시장 레짐에 따라 조율한다.

| 구분 | 전략 | 핵심 개념 |
|------|------|-----------|
| CORE | 펀딩비 차익거래 | 델타 뉴트럴 포지션으로 8시간 펀딩비 수취 |
| AUX | 그리드 트레이딩 | 횡보장에서 N x N 지정가 그리드 |
| AUX | 적응형 DCA | Fear & Greed 기반 BTC 적립 |

---

## 1. BaseStrategy ABC

**파일**: `services/strategies/base_strategy.py`

모든 전략이 상속하는 추상 기반 클래스. Hummingbot V2 스타일의 조합형(Composable) 컨트롤러 패턴을 채택한다.

### 추상 메서드

| 메서드 | 설명 |
|--------|------|
| `tick()` | 매 틱 간격(`tick_interval`, 기본 5초)마다 호출. 전략의 핵심 로직 |
| `on_start(capital, params)` | Orchestrator가 전략을 시작할 때 리소스 초기화 |
| `on_stop(reason)` | 전략 정지 시 포지션 정리 / 주문 취소 |
| `get_status()` | 현재 상태 스냅샷(`StrategyStatus`) 반환 |
| `_rebalance(new_capital)` | 할당 자본 변경 시 포지션 조정 |
| `_atr_position_size(atr, entry_price, risk_pct)` | ATR 기반 포지션 사이징 — 변동성 조정 사이징 |

### run() 메인 루프

```
시작
  -> Redis 연결 + DB Pool 생성
  -> strategy:command:{strategy_id} 채널 구독
  -> 무한 루프:
       1. 명령 큐 소진 (start/stop/pause/resume/reconfigure)
          → Redis 연결 오류 시 ensure_connected() + 상태 재동기화 (_reconnect_and_sync)
       2. is_running && !is_paused 이면 tick() 호출
       3. 하트비트 발행 (_publish_status, Redis TTL 90초)
       4. tick_interval 만큼 sleep
  -> 종료 시 구독 해제, Redis/DB 정리
```

### Composable Controllers (Hummingbot V2 스타일)

서브클래싱 없이 런타임에 기능을 확장하는 패턴:

```python
self.register_controller("risk", RiskController(...))
self.register_controller("signal", SignalController(...))

# 사용 시
risk = self.get_controller("risk")
```

컨트롤러는 `_controllers: dict[str, Any]`에 저장되며, 리스크 관리, 신호 생성, 주문 실행 등을 독립 모듈로 분리할 수 있다.

### submit_order()

`OrderRequest` 객체를 Redis `order:request` 채널에 JSON으로 발행하여 execution-engine에 주문을 위임한다.

```python
await self.submit_order(OrderRequest(
    symbol="BTCUSDT",
    side="buy",
    quantity=0.001,
    price=65000.0,
    ...
))
```

#### Rate Limiting

`submit_order()` 호출 시 슬라이딩 윈도우 rate limiter가 자동 동작한다:

| 파라미터 | 기본값 | config 키 |
|----------|--------|-----------|
| 초당 최대 주문 수 | 2 | `max_orders_per_second` |
| 분당 최대 주문 수 | 30 | `max_orders_per_minute` |

한도 초과 시 `OrderSubmitRateLimitError` 발생 — 주문 거부, warning 로그 기록.

### 명령 처리

Orchestrator가 `strategy:command:{id}` 채널로 보내는 `StrategyCommand`:

| 명령 | 동작 |
|------|------|
| `start` | `on_start()` 호출, 자본 할당, 실행 시작 |
| `stop` | `on_stop()` 호출, 실행 중지 |
| `pause` | tick 일시 정지 (포지션 유지) |
| `resume` | tick 재개 |
| `reconfigure` | 자본 재배분 + 파라미터 갱신 |

### 상태 발행

매 틱마다 Redis `strategy:status:{strategy_id}` 키에 상태를 저장하고 (TTL **90초**, 워치독의 60초 체크 주기와 호환되도록 변경), DB `strategy_states` 테이블에 UPSERT한다. 이 키는 오케스트레이터 Dead Man's Switch 워치독의 하트비트 역할도 겸한다.

---

## 2. 펀딩비 차익거래 (CORE 전략)

**디렉터리**: `services/strategies/funding-arb/`
**설정**: `config/strategies/funding-arb.yaml`

### 개념

무기한 선물(Perpetual Futures)은 8시간마다 펀딩비(Funding Rate)를 교환한다. 펀딩비가 양수(+)이면 롱 포지션 보유자가 숏 포지션 보유자에게 지불한다.

**전략**: 현물 롱 + 선물 숏의 델타 뉴트럴 포지션을 구성하여 가격 변동 리스크를 제거하고, 8시간마다 펀딩비만 수취한다.

**목표 수익률**: 펀딩비만으로 연 15-30% 수익

### 진입 조건

| 파라미터 | 값 | 설명 |
|----------|-----|------|
| `min_funding_rate_annualized` | ~10.95% (0.0001/8h 기준) | 최소 연환산 펀딩비 (`min_funding_rate=0.0001`, 8h 기준 0.01%) |
| `consecutive_intervals` | 3회 | 연속 3회 초과 필요 |
| `funding_interval_hours` | 8시간 | Bybit 펀딩 주기 |
| `require_predicted_alignment` | true | 예측 펀딩비 방향 일치 필요 |
| `min_open_interest_usd` | 500만 USD | 최소 미결제약정 |
| `max_entry_spread_pct` | 0.05% | 최대 진입 스프레드 |

**거래 쌍**: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, XRPUSDT

### 포지션 사이징

| 파라미터 | 값 |
|----------|-----|
| 사이징 모드 | `pct_equity` (자기자본 비율) |
| 포지션당 비율 | 5% |
| 포지션당 상한 | 10,000 USD |
| 최소 포지션 | 100 USD |
| 최대 레버리지 | 2배 |
| 동시 포지션 수 | 최대 5개 |
| 헤지 비율 | 1.0 (완전 헤지) |
| 헤지 드리프트 허용 | 2% |

### 청산 조건

| 조건 | 값 | 설명 |
|------|-----|------|
| 펀딩비 하한 | 연 5% 미만 | 수익성 소실 시 청산 |
| 최대 보유 | **168시간 (7일)** — 6년 백테스트 최적값 | 장기 보유 방지 |
| 분할 청산 | 50%→75%→100% 보유 시점에 50%→30%→20% 청산 | 리스크 점진적 감소 |
| 이익실현 | 3% | 누적 펀딩 수익 기준 |
| 손절 | 2% | 미실현 손실 기준 |
| 펀딩비 반전 | 즉시 청산 | 방향 전환 시 |
| 재진입 쿨다운 | 60분 | 동일 쌍 재진입 대기 |

> **on_start() 초기화**: `set_margin_mode("isolated")` + `set_leverage(2)` 자동 설정 후 거래 시작.

### 리스크 관리

- **포트폴리오 최대 배분**: 25%
- **최대 낙폭(MDD)**: 5% (7일 윈도우)
- **전략 상관계수 제한**: 0.7
- **서킷 브레이커**: 연속 3회 손실 -> 360분 쿨다운

### 컴포넌트 구조

```
funding-arb/
├── main.py             # 엔트리포인트, 전략 인스턴스 생성 + run()
├── strategy.py         # FundingArbStrategy(BaseStrategy) 구현
├── funding_tracker.py  # 펀딩비 추적, 연환산 계산, 연속 간격 검증
├── basis_spread_sm.py  # 베이시스 스프레드 상태 머신 (진입/보유/청산 상태 전이)
├── delta_neutral.py    # 델타 뉴트럴 포지션 관리, 헤지 비율 모니터링
└── cross_exchange.py   # 교차 거래소 차익거래 (현재 비활성)
```

**basis_spread_sm.py 상태 머신**:
```
IDLE -> MONITORING -> ENTRY_READY -> POSITION_OPEN -> EXIT_PENDING -> IDLE
```

### 수수료 구조

| 구분 | 수수료율 |
|------|----------|
| 현물 테이커 | 0.01% |
| 무기한 테이커 | 0.055% |

### 슬리피지 및 실행

- 예상 진입/청산 슬리피지: 0.03%
- 최대 허용 슬리피지: 0.10%
- 기본 주문 유형: 지정가 (limit)
- 지정가 타임아웃: 30초 (초과 시 시장가 전환)
- TWAP 임계값: 5,000 USD 초과 시 활성화 (300초 실행)

---

## 3. 그리드 트레이딩 (보조 전략)

**디렉터리**: `services/strategies/grid-trading/`
**설정**: `config/strategies/grid-trading.yaml`

### 개념

횡보장(Ranging Market)에서 현재가 위아래로 N개의 지정가 주문을 배치하여 평균 회귀(Mean Reversion)로 수익을 창출한다.

### 활성화 조건

| 조건 | 값 |
|------|-----|
| 필수 레짐 | `ranging` |
| 최소 레짐 신뢰도 | 0.6 |
| ATR 기간 | 14 |
| 볼린저 밴드 폭 상한 | 0.06 |
| 최소 24시간 거래량 | 5천만 USD |
| 브레이크아웃 시 자동 비활성화 | true (1% 버퍼) |

### 그리드 파라미터

| 파라미터 | 값 |
|----------|-----|
| 그리드 유형 | 기하급수(geometric) |
| 상단 레벨 수 | 10 |
| 하단 레벨 수 | 10 |
| 레벨 간격 | 0.5% |
| 간격 범위 | 0.2% ~ 2.0% |
| 레벨당 주문 크기 | 200 USD |
| 거래 쌍 | BTCUSDT, ETHUSDT |
| 그리드 재배치 주기 | 60분 |
| 재배치 트리거 | 가격이 전체 범위의 30% 이동 시 |

### 안전 메커니즘

| 메커니즘 | 값 |
|----------|-----|
| 최대 미체결 주문 | 40개 |
| 최대 동시 그리드 | 2개 |
| 긴급 취소 | 미실현 손실 -500 USD |
| 최대 스프레드 | 0.15% (초과 시 일시정지) |
| 최소 체결 간격 | 5초 |
| 체결 가격 이탈 한도 | 0.5% |
| 미체결 주문 타임아웃 | 120분 (취소 후 재배치) |

### 리스크 관리

- **포트폴리오 최대 배분**: 30%
- **최대 레버리지**: 2배
- **최대 낙폭**: 4% (7일 윈도우)
- **일일 손실 한도**: 300 USD
- **사이클당 이익 목표**: 500 USD (달성 시 그리드 리셋)
- **서킷 브레이커**: 연속 5회 실패 -> 240분 쿨다운

### 컴포넌트 구조

```
grid-trading/
├── main.py             # 엔트리포인트
├── strategy.py         # GridTradingStrategy(BaseStrategy) 구현
├── grid_calculator.py  # 그리드 레벨 계산 (기하/산술), 간격 조정
└── grid_state.py       # 그리드 상태 관리 (활성 주문, 체결 추적)
```

---

## 4. 적응형 DCA (보조 전략)

**디렉터리**: `services/strategies/adaptive-dca/`
**설정**: `config/strategies/adaptive-dca.yaml`

### 개념

Fear & Greed Index, 기술적 지표, 변동성 등 시장 상황에 따라 매수 금액과 간격을 동적으로 조정하는 적립식 투자(DCA) 전략. 공포가 클수록 더 많이 매수한다.

### 기본 파라미터

| 파라미터 | 값 |
|----------|-----|
| 기본 투자금 | 100 USD / 24시간 |
| 거래 쌍 | BTCUSDT, ETHUSDT |
| 포지션 방향 | 롱 전용 |
| 상품 유형 | 선물 |
| 레버리지 | 1배 |

### 적응형 승수 (Adaptive Multipliers)

모든 승수는 독립적으로 적용된 후 곱셈 방식으로 결합된다. 최종 승수는 0.1x ~ 5.0x로 제한한다.

**Fear & Greed 승수**:

| 구간 | 지수 범위 | 승수 |
|------|----------|------|
| 극도의 공포 | 0 - 10 | 2.5x |
| 공포 | 11 - 25 | 1.8x |
| 중립 | 26 - 50 | 1.0x |
| 탐욕 | 51 - 75 | 0.5x |
| 극도의 탐욕 | 76 - 100 | 0.2x |

**이동평균 이탈 승수** (EMA 200 기준):

| 조건 | 승수 |
|------|------|
| MA 대비 -5% | 1.3x |
| MA 대비 -10% | 1.8x |
| MA 대비 -20% | 2.5x |
| MA 대비 -30% | 3.0x |
| MA 대비 +0% 이상 | 0.7x |
| MA 대비 +20% 이상 | 0.3x |

**RSI 승수**:

| 조건 | 승수 |
|------|------|
| RSI < 30 (과매도) | 1.8x |
| RSI > 70 (과매수) | 0.4x |

**변동성 승수** (ATR 14 기준):

| 조건 | 금액 승수 | 간격 승수 |
|------|----------|----------|
| 고변동성 | 1.3x | 1.5x (간격 확대) |
| 저변동성 | 0.8x | 0.7x (간격 축소) |

### 이익실현 사다리 (Take Profit Ladder)

| 수익률 | 매도 비율 |
|--------|----------|
| +15% | 10% 매도 |
| +30% | 15% 매도 |
| +50% | 20% 매도 |
| +100% | 25% 매도 |

이익실현 후 48시간 쿨다운 뒤 DCA 재개.

### 리스크 관리

- **최대 투입 자본**: 50,000 USD
- **최대 포트폴리오 배분**: 40%
- **낙폭 일시정지**: 25% (15% 회복 시 재개)
- **단일 매수 상한**: 1,000 USD
- **일일 한도**: 500 USD
- **주간 한도**: 2,000 USD
- **서킷 브레이커**: 연속 3회 매수 실패 -> 120분 쿨다운

### 컴포넌트 구조

```
adaptive-dca/
├── main.py          # 엔트리포인트
├── strategy.py      # AdaptiveDCAStrategy(BaseStrategy) 구현
├── fear_greed.py    # Fear & Greed Index 조회 (외부 API)
└── scheduler.py     # DCA 간격 스케줄링, 적응형 타이밍 조정
```

---

## 5. Strategy Orchestrator

**디렉터리**: `services/orchestrator/`
**설정**: `config/orchestrator.yaml`

### 역할

시장 레짐(Regime)을 감지하고, 레짐에 따라 전략별 자본 배분 가중치를 결정하며, Kill Switch를 통해 비상 상황을 관리한다.

### 레짐 감지

**사용 지표**:
- ADX(14): 추세 강도 (25 이상 = 추세, 40 이상 = 강한 추세)
- 볼린저 밴드 폭(20, 2SD): 변동성 (0.03 미만 = 횡보, 0.08 초과 = 고변동)
- EMA(20/50/200) 정렬: 추세 방향
- ATR(14): 변동성 수준 (25/75 백분위)
- 거래량 프로파일: 20일 기준, 2배 = 급등

**분류 기준**:
- 1차 타임프레임: 4시간봉
- 2차 타임프레임: 1시간봉, 일봉
- 최소 신뢰도: 0.5
- 확인 기간: 2회 연속 동일 신호

### 가중치 매트릭스 (Weight Matrix)

4개 레짐 x 3개 전략 + 현금 준비금. 각 행의 합 = 1.0.

| 레짐 | 펀딩 차익 | 그리드 | DCA | 현금 |
|------|----------|--------|-----|------|
| **횡보 (Ranging)** | **0.50** | 0.00 | 0.00 | **0.50** |
| **상승 추세 (Trending Up)** | **0.20** | 0.00 | 0.00 | **0.80** |
| **하락 추세 (Trending Down)** | **0.10** | 0.00 | 0.00 | **0.90** |
| **고변동 (Volatile)** | **0.40** | 0.00 | 0.00 | **0.60** |

> **2026-04-05 기준**: 6년 백테스트 결과 Grid / DCA 가중치 0으로 조정. FA 단독 운영.

**핵심 원칙**:
- 횡보장: FA 최대 가동 (0.50), 현금 50% 안전 버퍼
- 상승/하락장: 현금 비중 극대화 (0.80/0.90)
- 고변동: FA 0.40, 현금 0.60 균형

### 가중치 전환 (Weight Transition)

레짐 변경 시 급격한 자본 재배분을 방지하기 위해 EMA 스무딩을 적용한다.

| 파라미터 | 값 |
|----------|-----|
| 전환 단계 | 5 스텝 |
| 스텝 간격 | 60초 |
| 스텝당 최대 변동 | 10%p |
| EMA 알파 | 0.3 |
| 최소 변동 임계값 | 2%p |

### ML 레짐 감지기 (LightGBM)

지표 기반 규칙 분류를 보완하기 위해 LightGBM 모델을 사용한다.

| 파라미터 | 값 |
|----------|-----|
| 재학습 주기 | 6시간 |
| 학습 데이터 | 최근 30일 |
| 최소 학습 샘플 | 500개 |
| 이상치 감지 | Dissimilarity Index (2.5 임계값, 2 SD) |

**입력 피처** (10개):
`adx_14`, `rsi_14`, `bb_width_20`, `atr_14`, `volume_sma_ratio`, `close_sma_ratio_20`, `close_sma_ratio_50`, `macd_histogram`, `obv_slope`, `funding_rate`

### Kill Switch 통합

`orchestrator/core.py`는 `shared/kill_switch.py`에 완전히 위임한다 (중복 KillSwitchState 제거, 단일 소스). 4단계 계층적 방어:

| 트리거 | 임계값 | 동작 |
|--------|--------|------|
| 일일 낙폭 | 5% | 전략 정지 + 포지션 청산 |
| 주간 낙폭 | 10% | 전략 정지 + 포지션 청산 |
| 월간 낙폭 | 15% | 전략 정지 + 포지션 청산 |
| 일일 손실 | 1,000 USD | 전략 정지 + 포지션 청산 |
| BTC 급락 | 24시간 내 -15% | 전체 시스템 정지 |
| API 오류 폭증 | 10분 내 50건 | 전체 시스템 정지 |
| 실행 지연 | 5,000ms 초과 | 전체 시스템 정지 |

**Kill Switch 발동 시**:
1. 모든 포지션 청산
2. 모든 주문 취소
3. Telegram 알림 발송
4. 크리티컬 로그 기록
5. **수동 리셋 필요** (60분 쿨다운)

### 낙폭 기반 사이징 (`_get_drawdown_size_multiplier`)

Kill Switch와 독립적으로 동작하는 점진적 주문 크기 축소 메커니즘:

| 포트폴리오 낙폭 | 주문 크기 승수 |
|----------------|---------------|
| 20% 미만 | 1.0x (정상) |
| 20% 이상 | 0.5x |
| 30% 이상 | 0.1x |
| 50% 이상 | 0.0x (신규 주문 차단) |

### 평가 주기

| 항목 | 간격 |
|------|------|
| 레짐 평가 + 리밸런싱 | 300초 (5분) |
| 포트폴리오 스냅샷 | 900초 (15분) |
| 최소 리밸런싱 간격 | 900초 (빈번한 변경 방지) |

### 컴포넌트 구조

```
orchestrator/
├── main.py                 # 엔트리포인트
├── core.py                 # StrategyOrchestrator 핵심 로직
├── weight_manager.py       # 가중치 매트릭스 관리, EMA 전환
├── regime_ml_model.py      # LightGBM 레짐 분류 모델
├── dissimilarity_index.py  # 이상치 감지 (학습 데이터 품질 관리)
└── portfolio_monitor.py    # 포트폴리오 모니터링, 스냅샷 저장
```

### LLM Advisor 통합

Claude API 기반 시장 분석 결과를 가중치 조정에 반영한다.

- 최소 신뢰도: 0.5
- 최대 가중치 조정: +/-15%p
- Redis 채널: `llm:advisory`

---

## 6. 개선 제안

### 6.1 가중치 매트릭스 동적 최적화

현재 가중치 매트릭스는 YAML에 정적으로 정의되어 있다. 개선 방안:

- **베이지안 최적화**: 실제 운용 데이터를 기반으로 Sharpe Ratio를 목적 함수로 하여 가중치를 자동 탐색
- **온라인 학습**: 일정 기간마다 최근 성과를 반영하여 가중치를 미세 조정
- **주의점**: 과적합 위험이 있으므로 최소 3개월 이상의 실전 데이터 확보 후 적용 권장. 가중치 변동 범위에 상한/하한을 설정하여 급격한 변화 방지 필요

### 6.2 교차 거래소 차익거래 활성화 시점

`cross_exchange.enabled: false`로 비활성화되어 있다. 활성화 조건:

- **전제 조건**: Bybit 테스트넷 Phase 완료 후 실전 운용 안정화 (최소 Phase 5)
- **자금 요건**: Binance + Bybit 양쪽에 충분한 마진 확보 (최소 각 5,000 USD)
- **기술 요건**: 교차 거래소 자금 이동 자동화, 출금 API 권한 필요
- **리스크**: 출금 지연(30분 버퍼), 거래소 점검/장애 시 한쪽 포지션만 노출
- **권장 시점**: 단일 거래소 펀딩 차익이 목표 수익률(연 15%)에 미달할 때

### 6.3 ML 레짐 감지기 학습 데이터 요건

현재 설정:

- 최소 학습 샘플: 500개 (4시간봉 기준 약 83일)
- 학습 데이터 윈도우: 30일 (4시간봉 180개)
- **다중 타임프레임 확인(멀티 TF 확인) ✅ 적용됨**: 1차(4h) + 2차(1h, 일봉) 타임프레임 피처 결합으로 단일 타임프레임 의존도 감소

**문제점**: 30일 x 6봉/일 = 180개 < 최소 500개. 학습 데이터가 부족할 수 있다.

**개선 방안**:
- `training_lookback_days`를 90일 이상으로 확대
- 또는 `primary_timeframe`을 1시간봉으로 변경하여 샘플 수 증가 (30일 x 24봉 = 720개)
- Phase 3 완료: 6년(2020-2026) 히스토리 데이터 활용하여 모델 사전 학습 완료

### 6.4 기타 고려사항

- **몬테카를로 시뮬레이션 1,000회 ✅ 구현됨**: 백테스터에서 100회에서 1,000회로 확대하여 신뢰구간 정밀도 향상
- **FA 분할 청산(Tiered Exit) ✅ 구현됨**: 보유 기간 50%/75%/100% 시점에 포지션 50%/30%/20% 순차 청산 (`strategy.py` 적용)
- **펀딩비 전략 현물 레그**: 현재 Bybit 내에서 현물+선물을 동시 운용하나, 현물 유동성이 낮은 쌍(SOL, XRP)은 슬리피지 리스크가 높다. 쌍별 유동성 모니터링 추가 필요
- **그리드 트레이딩 트레일링**: `trailing_enabled: false`로 설정되어 있다. (현재 Grid 가중치 0, Phase 5 이후 재검토)
- **DCA 재설계**: 6년 WFO에서 MDD 42% 기록. 졸업형 DCA 전략 재설계 후 백테스트 재수행 필요
