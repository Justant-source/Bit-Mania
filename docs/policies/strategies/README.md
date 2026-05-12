---
title: 거래 전략
category: policies/strategies
last_updated: 2026-05-01
when_to_update: |
  - 새 전략 추가 시
  - orchestrator.yaml 가중치 변경 시
  - BaseStrategy ABC 수정 시
---

# 거래 전략

CryptoEngine의 거래 전략 포트폴리오 및 운영 규칙. 모든 전략은 BaseStrategy ABC를 상속하며, Strategy Orchestrator가 시장 레짐에 따라 자본을 동적 배분합니다.

---

## 전략 포트폴리오 (현황)

| 전략 | 상태 | CAGR | Sharpe | 역할 |
|------|------|------|--------|------|
| [Funding Arb](funding-arb.md) | ✅ **활성** | +34.87% | 3.583 | 핵심 (메인 수익원) |
| [Adaptive DCA](adaptive-dca.md) | ⚠️ **비활성** | N/A | N/A | 보조 (재활성화 검토 중) |

---

## Funding Arb (핵심 전략, 활성)

### 개요

**무기한 선물의 펀딩레이트를 수취하기 위한 델타 중립(Delta-Neutral) 전략**

- **진입**: 현물 BTC 매수 + 선물 BTC 매도 (동시)
- **수익**: 방향성 없는 펀딩비 캐리 수익 (8시간마다 정산)
- **리스크**: 펀딩비 반전 시 즉시 청산
- **레버리지**: 5x (하드 리밋)

### 백테스트 성과 (fa80_lev5_r30)

**기간**: 2020-04-01 ~ 2026-03-31 (6년, Test 12 Stage D2)

| 지표 | 값 |
|------|-----|
| **CAGR** | +34.87% |
| **Sharpe Ratio** | 3.583 |
| **Maximum Drawdown** | -4.52% |
| **Liquidations** | 0회 (마진 안전성 최우수) |

### 핵심 파라미터

```yaml
# 자본 배분
fa_capital_ratio: 0.80                   # 포트폴리오의 80%
leverage: 5.0

# 진입 조건
entry:
  min_funding_rate_annualized: 15.0      # 연 15% 이상
  consecutive_intervals: 3                # 3회 연속 양수
  max_entry_spread_pct: 0.05             # Spread < 0.05%
  min_open_interest_usd: 5_000_000       # 유동성 $5M 이상

# 청산 조건
exit:
  min_funding_rate_annualized: 5.0       # 수익성 상실 시 청산
  exit_on_rate_flip: true                # 음수 전환 즉시 청산
  max_holding_hours: 720                 # 30일 강제 청산
  stop_loss_pct: 2.0
  take_profit_pct: 3.0
```

### Phase 5 (메인넷 소액 실전) 오버라이드

`BYBIT_TESTNET=false` 또는 `PHASE5_MODE=true` 시:

```yaml
phase5:
  sizing_mode: fixed_notional
  fixed_notional_usd: 150                # $200 × 75% (안전 버퍼)
  max_concurrent_positions: 1            # 5 → 1 (소액 리스크 관리)
  entry:
    min_funding_rate_annualized: 25.0    # 15% → 25% (수수료 현실화)
    consecutive_intervals: 4             # 3 → 4 (더 보수적)
```

[상세 문서 →](funding-arb.md)

---

## Adaptive DCA (보조 전략, 현재 비활성)

### 상태

**비활성화** (orchestrator.yaml에서 weight = 0.0)

**사유**: 6년 Walk-Forward 백테스트에서 consistency 0.409 (기준 미달) 및 2022년 MDD -42% 기록

**다음 결정**: Phase 4 이후 4주 테스트넷 운영 결과에 따라 재활성화 여부 재결정

### 설계 개요

시장 심리와 기술 지표에 기반한 **적응형 장기 축적 전략**

- **기본 매수**: $100 per 24 hours (조건부 조정)
- **멀티플라이어 4가지**: Fear & Greed, EMA200 편차, RSI, ATR
- **분할 진입**: Test C 결과 +34.3%p 수익률 개선
- **이익 실현 래더**: +15%, +30%, +50%, +100% 수익 목표

### 구조 (참조용)

```yaml
base:
  base_amount_usd: 100
  base_interval_hours: 24
  pairs: [BTCUSDT]
  leverage: 1                             # 레버리지 없음

adaptive:
  fear_greed:
    extreme_fear_multiplier: 2.5          # FGI 0-10
    extreme_greed_multiplier: 0.2         # FGI 76-100
  
  price_deviation:                        # EMA200 대비
    below_30_pct_multiplier: 3.0
  
  rsi:
    oversold_multiplier: 1.8              # RSI < 30
    overbought_multiplier: 0.4            # RSI > 70
  
  volatility:                             # ATR 기반
    high_vol_amount_multiplier: 1.3
  
  combination_method: multiply
  min_combined_multiplier: 0.1
  max_combined_multiplier: 5.0

risk:
  max_total_deployed_usd: 50_000          # 누적 상한
  max_portfolio_allocation_pct: 40.0
  daily_cap_usd: 500
  weekly_cap_usd: 2_000
```

[상세 문서 →](adaptive-dca.md)

---

## BaseStrategy ABC (전략 구현 기본)

모든 전략(funding-arb, adaptive-dca)은 `BaseStrategy` 추상 기본 클래스를 상속합니다.

**파일**: `cryptoengine/services/strategies/base_strategy.py`

### 핵심 구조

```python
class BaseStrategy(ABC):
    """Composable strategy base (Hummingbot V2 style)."""
    
    def __init__(self, strategy_id: str, config: dict[str, Any]) -> None:
        # 초기화: 자본, 설정, 상태
        self.allocated_capital: float = 0.0
        self.is_running: bool = False
        self.is_paused: bool = False
    
    async def run(self) -> None:
        """Main loop: Redis 명령 구독 + tick 실행."""
        # 1. Redis 연결
        # 2. DB pool 초기화
        # 3. 명령 채널 구독: strategy:command:{strategy_id}
        # 4. 메인 루프:
        #    - 명령 수신 및 처리
        #    - tick() 호출 (전략 로직)
        #    - 상태 발행 (heartbeat)
    
    # ── 추상 메서드 (모든 전략이 구현해야 함) ────
    
    @abstractmethod
    async def tick(self) -> None:
        """Called every tick interval while running."""
        # 전략 실행 로직 (주문 발행, 포지션 관리 등)
    
    @abstractmethod
    async def on_start(self, capital: float, params: dict) -> None:
        """Initialize when started by orchestrator."""
        # 자본 배분받고 포지션 초기화
    
    @abstractmethod
    async def on_stop(self, reason: str) -> None:
        """Teardown positions / cancel orders on stop."""
        # 포지션 청산, 주문 취소
    
    @abstractmethod
    async def get_status(self) -> StrategyStatus:
        """Return current status snapshot."""
        # 현재 상태 (P&L, 포지션 수, 실행 여부)
    
    @abstractmethod
    async def _rebalance(self, new_capital: float) -> None:
        """Adjust positions when capital changes."""
        # 자본 재배분 시 포지션 조정

    # ── 유틸리티 메서드 ────
    
    async def submit_order(self, order: OrderRequest) -> None:
        """Publish order to Redis order:request channel."""
        # Rate limit 확인 후 주문 발행
    
    async def register_controller(self, name: str, controller: Any) -> None:
        """Attach composable controller."""
        # Hummingbot V2 style 컨트롤러 추가 (risk, signal, executor)
```

### 통신 플로우

```
Orchestrator                Strategy (BaseStrategy)
    │                             │
    ├──────────────────────────────► strategy:command:{id}
    │     (start/stop/pause/     │    ┌─ on_start() 호출
    │      resume/reconfigure)    │    ├─ on_stop() 호출
    │                             │    ├─ allocated_capital 업데이트
    │                             │    └─ _rebalance() 호출
    │                             │
    │                             ├──► order:request (Redis Pub)
    │                             │    └─ Execution Engine 수신
    │                             │
    │◄──────────────────────────────
    │  strategy:status:{id}       │
    │  (heartbeat + P&L)          │
```

### 실행 라이프사이클

1. **Orchestrator 시작**
   - Strategy Orchestrator 서비스 부팅
   - market-data와 regime 감지 시작

2. **전략 시작 신호**
   ```
   Orchestrator 판단: 펀딩레이트 > 15% → FA 활성화 필요
   → StrategyCommand(action="start", allocated_capital=$8,000) 발행
   ```

3. **전략 초기화**
   ```python
   # funding-arb strategy.py
   async def on_start(self, capital: float, params: dict) -> None:
       self.allocated_capital = capital
       # 포지션 사이징 계산
       # Redis 청취 시작 (펀딩레이트 채널)
       # 진입 조건 모니터링 활성화
   ```

4. **주기적 실행**
   ```python
   async def tick(self) -> None:
       # 1초마다 호출 (configurable)
       # 펀딩레이트 수신 → 진입 조건 검사
       # 포지션 있으면: 청산 조건 검사
       # 주문 발행 (Redis order:request)
   ```

5. **상태 보고**
   ```python
   await self._publish_status()  # 60초마다
   → Redis strategy:status:{id}
   → PostgreSQL strategy_states 업데이트
   ```

6. **전략 중지 신호**
   ```
   Orchestrator: 펀딩비 반전 또는 Kill Switch 발동
   → StrategyCommand(action="stop", reason="funding_reversal") 발행
   ```

7. **정리**
   ```python
   async def on_stop(self, reason: str) -> None:
       # 청산 또는 포지션 유지 (reason에 따라)
       # 주문 취소
       # 최종 P&L 기록
   ```

### 컨트롤러 패턴 (선택적)

Hummingbot V2 스타일 Composable Controllers:

```python
# 전략이 시작할 때 컨트롤러 등록
strategy.register_controller("risk", RiskController(...))
strategy.register_controller("signal", SignalGenerator(...))
strategy.register_controller("executor", OrderExecutor(...))

# tick 중에 컨트롤러 활용
risk_ctrl = self.get_controller("risk")
if risk_ctrl.should_stop():
    await self.on_stop("risk_limit_exceeded")
```

---

## 레짐별 가중치 (orchestrator.yaml)

Strategy Orchestrator가 **시장 레짐**에 따라 자본을 동적 배분:

```yaml
weights:
  ranging:           # 횡보 시장 (Sharpe 2.72)
    funding_arb: 0.50
    adaptive_dca: 0.00  # DCA 비활성
    cash_reserve: 0.50
  
  trending_up:       # 상승추세
    funding_arb: 0.20
    adaptive_dca: 0.00  # DCA 비활성
    cash_reserve: 0.80
  
  trending_down:     # 하락추세 (리스크 최소)
    funding_arb: 0.10
    adaptive_dca: 0.00  # DCA 비활성
    cash_reserve: 0.90
  
  volatile:          # 고변동성 (Sharpe 2.63)
    funding_arb: 0.40
    adaptive_dca: 0.00  # DCA 비활성
    cash_reserve: 0.60
```

**해석**:
- **Ranging**: FA 수익 극대화 환경 → FA 50% + 현금 50%
- **Trending Up**: 상승장 진행 중 → 현금 80% 확보 (하락 시 매수)
- **Trending Down**: 하락 위험 높음 → 현금 90% 방어 (FA 축소)
- **Volatile**: 변동성 높음 → FA 40% 유지 (스프레드 위험 고려)

**주의**: DCA는 현재 모든 레짐에서 0.0 (비활성)

---

## Kill Switch와 전략별 동작

포트폴리오 레벨 Kill Switch 발동 시:

| Level | 조건 | Funding Arb | Adaptive DCA |
|-------|------|------------|-------------|
| **L1** | 전략 손실 > 3% | 포지션 청산 | 매수 중지 |
| **L2** | 일일 손실 > 5% | 포지션 청산 | 포지션 청산 |
| **L3** | 시스템 장애 | 시장가 청산 | 시장가 청산 |
| **L4** | 수동 비상 정지 | 즉시 청산 | 즉시 청산 |

[상세 정책 →](../kill-switch.md)

---

## 백테스트 스킬셋 규칙

새 전략 또는 파라미터 변경 시 백테스트 절차:

### 1. 기존 스크립트 확인 (필수)

```bash
ls -la backtest/scripts/
```

동일/유사 스크립트 존재 여부 확인 후 재사용 검토.

### 2. 새 스크립트 작성

```bash
# 경로: backtest/scripts/{runners,sweep,analysis,reports,audit,data}/
# 예: backtest/scripts/runners/test_fa80_lev5_r30.py
```

### 3. README 업데이트

`backtest/docs/CODE_MAP.md`에 다음 시점에 반드시 업데이트:

- **스크립트 추가** → 테이블에 행 추가
- **파라미터 변경** → 해당 행 업데이트
- **스크립트 삭제** → 해당 행 제거

### 4. Docker 실행

```bash
# 이미지 재빌드 (새 파일 포함)
docker compose -f backtest/docker/docker-compose.yml --profile backtest build --no-cache backtester

# Jesse 백테스트 실행
docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester \
  python scripts/runners/test_fa80_lev5_r30.py
```

---

## 관련 정책 문서

- [../btc-only.md](../btc-only.md) — **BTC 단일 운영** (다른 심볼 금지)
- [../leverage-limits.md](../leverage-limits.md) — **레버리지 제한** (5x 하드 캡)
- [../kill-switch.md](../kill-switch.md) — **Kill Switch 정책** (자동 보호 4단계)
- [../deployment-position.md](../deployment-position.md) — **배포 시 포지션 유지**
- [../operations/runbook.md](../operations/runbook.md) — **일상 운영 및 모니터링**

---

## 참고: 서비스 구현

### Funding Arb 서비스

**파일**: `cryptoengine/services/strategies/funding-arb/`

| 모듈 | 역할 |
|------|------|
| `strategy.py` | 메인 전략 로직 (BaseStrategy 상속) |
| `funding_tracker.py` | 펀딩비 추적 + NetProfitabilityCheck |
| `delta_neutral.py` | 현물-선물 헷지 관리 |
| `basis_spread_sm.py` | Basis spread 상태 머신 |
| `cross_exchange.py` | Cross-exchange 차익거래 (미사용) |

### Adaptive DCA 서비스

**파일**: `cryptoengine/services/strategies/adaptive-dca/`

| 모듈 | 역할 |
|------|------|
| `strategy.py` | 메인 전략 로직 (BaseStrategy 상속) |
| `fear_greed.py` | Fear & Greed Index 멀티플라이어 |
| `scheduler.py` | 매수 스케줄 + 분할 진입 관리 |
