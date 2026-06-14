---
title: Supertrend 4h Long-Only 3x 전략 사양
category: policies/strategies
related_code:
  - cryptoengine/services/strategies/supertrend/
  - cryptoengine/config/strategies/supertrend.yaml
  - cryptoengine/config/orchestrator.yaml
last_updated: 2026-06-14
when_to_update: |
  - supertrend.yaml 파라미터 변경 시
  - 백테스트 결과 업데이트 시
  - orchestrator.yaml 자본 배분 변경 시
  - 주문 확정/재시도/동기화 동작 변경 시 (strategy.py)
  - 지표 계산/lookback 변경 시 (indicators.py, CANDLE_LOOKBACK)
---

# Supertrend 4h Long-Only 3x (combo #7908)

## 전략 개요

**CryptoEngine 메인 전략**으로, Supertrend 기술적 지표와 EMA 조합을 활용한 **BTC 4시간 추세 추종(Trend-Following) 전략**입니다. Long-only(매수 포지션만)로 운영하며, 상승 추세 확인 후 진입하고 하강 신호에서 즉시 청산합니다.

### 기본 구조

```
Supertrend 상승 신호 감지 (Close > ST선)
        +
빠른 EMA > 느린 EMA (모멘텀 확인)
        +
현재가 > 방향 필터 EMA (추세 확인)
        =
Long 진입 (3x 레버리지 적용)
```

### 수익 메커니즘

- **진입**: BTC 가격이 Supertrend선 위로 올라가고 EMA 교차 확인 시 매수
- **청산 조건**: 
  - EMA 하강 교차 → 즉시 청산 (추세 반전)
  - ATR 기반 손절/익절 → 자동 청산
  - 방향 필터 EMA 이탈 → 추세 약화 청산
- **레버리지**: 3x (선물 명목가 증폭, 자본 효율성)
- **리스크**: MDD −84.28% (극단 시나리오 대비 고위험, 사용자 승인 필수)

---

## 파라미터

### 지표 설정

| 항목 | 값 | 설명 |
|---|---|---|
| **타임프레임** | 4h | 4시간 캔들 기준 |
| **방향** | Long-only | 매수 포지션만 (숏 금지) |
| **레버리지** | 3x | 3배 선물 계약 |
| **Supertrend 기간** | 9 | ATR 계산 기간 |
| **Supertrend 배수** | 2.6 | ATR 상수배 |
| **빠른 EMA** | 7 | 단기 모멘텀 필터 |
| **느린 EMA** | 29 | 중기 방향 필터 |
| **방향 EMA** | 240 | 장기 추세 필터 |
| **ATR 청산 배수** | 3.3 | 손절/익절 배수 (ATR × 3.3) |
| **봉 lookback** | 1000 | 지표 계산용 보유 봉 — dir_ema(240) 시드 정합 (2026-06-14 300→1000) |

> **지표 구현 정합 (2026-06-14)**: 라이브 EMA·ATR·Supertrend는 백테스트 엔진(Jesse 2.1.2 = combo
> #7908)과 수치 일치하도록 구현됨 — EMA는 `close[0]` 시드 재귀(≡ `jesse_rust.ema`), ATR은 Wilder
> `_atr_jesse`(≡ `jesse_rust.atr`), Supertrend는 정본 포팅. **TA-Lib 미사용** (SMA 시드 차이로
> 진입필터가 경계 봉에서 발산하던 것을 수정). 검증: `cryptoengine/tests/unit/test_supertrend_parity.py` (5/5).
> 백테스트와 라이브는 거래소·격자가 다름(#7908 Binance 1h→4h 02계열, 라이브 Bybit 네이티브 4h 00계열) → `.result/supertrend_7908_parity_audit.md`.

### 구성 파일

```yaml
# cryptoengine/config/strategies/supertrend.yaml

supertrend:
  timeframe: 4h
  direction: long_only
  leverage: 3
  
  indicators:
    supertrend:
      period: 9
      multiplier: 2.6
    
    ema_fast: 7
    ema_slow: 29
    ema_trend: 240
    
    atr_exit_multiplier: 3.3

position:
  sizing_mode: pct_equity          # 비율 기반 배분 (고정액 아님)
  pct_equity: 95.0                 # 할당자본 95% × 3x
  max_concurrent_positions: 1
  min_position_usd: 65
```

---

## 진입 조건 (3중 확인)

포지션을 열기 위한 필수 조건들:

### 1. Supertrend 상승 신호

```
Current Close > Supertrend 선 (ST line)
```

- ST선이 가격 아래에 있고 상승 추세 진행 중

### 2. EMA 모멘텀 확인

```
EMA(7) > EMA(29)
```

- 단기 추세가 중기 추세보다 강함
- 최근 가격 모멘텀 양수

### 3. EMA 추세 필터

```
Current Close > EMA(240)
```

- 장기 추세(240 EMA)보다 위에 있음
- 큰 틀의 상승 추세 확인

**모든 3가지 조건이 동시에 만족해야 진입**

---

## 청산 조건

포지션을 종료하기 위한 트리거들:

### 1. EMA 하강 교차 (최우선)

```
EMA(7) < EMA(29)
→ 즉시 청산 (타임프레임 무관)
```

- 추세 반전 신호
- 가장 높은 우선순위 (손실 제한)

### 2. ATR 기반 손절/익절

```
진입가 - ATR(14) × 3.3 ≤ 현재가 ≤ 진입가 + ATR(14) × 3.3
→ 자동 청산 + 4h 진입 금지
```

- 손절: 진입가 이하 3.3×ATR 위치에서 자동 청산
- 익절: 진입가 이상 3.3×ATR 위치에서 자동 청산
- ATR 청산 후 1봉(4h) 동안 신규 진입 차단

### 3. 추세 필터 이탈

```
Current Close ≤ EMA(240)
→ 방향 필터 조건 미충족 — 신규 진입 억제 (청산 조건은 EMA cross만)
```

---

## 백테스트 성과

### 현재 설정: **supertrend_4h_x3_7908** (채택 설정)

**Backtest Period**: 2017-01-01 ~ 2026-05-18 (9년)  
**Data Quality**: 전체 역사 데이터 검증

| 지표 | 값 | 평가 |
|------|-----|------|
| **CAGR** | +151.56% | ✅ 매우 우수 |
| **Sharpe Ratio** | 1.37 | ⚠️ 1.0 이상 (적절) |
| **Maximum Drawdown** | -84.28% | ⚠️ **프로젝트 기준 FAIL** |
| **총 거래 수** | 354회 | ✅ 충분한 샘플 |
| **승률** | 46.83% | ✅ 양수 기대값 |

### ⚠️ 위험 수준 평가

**MDD −84.28% → 프로젝트 기준 FAIL (MDD ≤ −80%, PF ≥ 1.2)**

이 전략은 매우 높은 드로우다운 위험을 가지고 있습니다:

- **극단 시나리오**: 2022년 BTC 약세장 중 한 번의 거대한 drawdown 이벤트 발생
- **복구 기간**: 6개월 이상 소요 가능
- **사용자 심리**: 80%+ 손실 시 정신적 스트레스 극대

**그럼에도 불구하고 채택된 이유**:
1. CAGR +151.56% — 파워풀한 장기 수익성
2. 추세 추종 특성 — BTC 상승장에서 극대 이익 창출
3. 사용자 승인 — 위험 인지 후 명시적 동의

---

## 운영 상태

### Phase 5 (메인넷 실전)

```
Strategy ID: supertrend-01
Redis Channel: strategy:command:supertrend-01
Config File: cryptoengine/config/strategies/supertrend.yaml
Service: cryptoengine/services/strategies/supertrend/
```

### Phase 5 오버라이드 (메인넷 소액 실전 $200 USDT)

```yaml
phase5:
  sizing_mode: fixed_notional
  fixed_notional_usd: 150              # $200 × 75% (25% 안전 버퍼)
  max_concurrent_positions: 1          # 동시 1개만
  min_position_usd: 65                 # Bybit 최소 주문
```

### 주문 실행 방식 (2026-05-21~)

| 구분 | 방식 |
|------|------|
| 진입 (entry) | **Post-only 지정가** — bar close 가격으로 initial peg; order_manager가 best-bid로 즉시 re-peg |
| 청산 (exit) | **Post-only 지정가** — bar close 가격으로 initial peg; best-ask로 re-peg |
| 긴급 청산 (on_stop) | 시장가 유지 (셧다운 즉각 청산 보장) |
| Re-peg 정책 | 10초마다 새 best-bid/ask로 재발행, 최대 20회 (200초). **부분체결 누적 추적 — 잔량만 재발주** (2026-06-13: 전량 재발주로 인한 과체결 위험 수정) |
| 폴백 | 20회 미체결 시 시장가 fallback (`LIMIT_FALLBACK_TO_MARKET` 로그) — 잔량만 발주, 결과는 누적 합산 보고 |
| 수수료 기준 | Maker 0.020% (백테스트 combo #7908 가정과 정합) |
| SafetyGuard leverage_limit | 3.0 (`SAFETY_LEVERAGE_LIMIT=3.0` env) |
| reduce_only 마진 체크 | SafetyGuard 마진 잔고 검사 면제 — 청산 주문은 마진 부족으로 차단되지 않음 |
| reduce_only 레버리지 체크 | SafetyGuard 레버리지 검사 면제 — 청산 주문은 implied leverage 계산에서 제외 (2026-05-27 버그픽스: sell exit가 잘못 차단되던 문제) |
| 진입 사이징 자본 | `min(할당자본, 실시간 equity)` — equity(`cache:balance:bybit`)가 낮으면 축소 진입해 implied leverage 차단(2.85x vs 한도 3.0x, 여유 5%) 예방 (2026-06-13) |

### 주문 확정·상태 동기화 (2026-06-13~, 미체결 사고 재발 방지)

2026-05-27 사고(청산 신호 safety 차단 → 전략이 모른 채 발산 → 수동 개입)의 구조적 재발 방지를 위해 전략의 상태 관리를 "낙관적 갱신"에서 "확정 기반"으로 전환했다.

| 구분 | 동작 |
|------|------|
| 진실 동기화 | 매 봉 신호 판단 **전** `get_position`으로 내부 상태(`_has_position` 등)를 거래소 실포지션으로 교정. 발산 감지 시 `position_state_divergence` ERROR (→ Telegram) |
| 주문 확정 | 제출 시 낙관적 상태 변경 없이 pending으로 추적. `order:result:{strategy_id}` 구독 수신 또는 포지션 폴링(20초 주기 백스톱)으로만 확정 |
| 확정 시한 | 450초 (engine ORDER_TIMEOUT 420초 + 여유). 초과 시 `pending_order_unresolved` ERROR + 재동기화 |
| exit 거부 | ERROR 알림 + 재동기화 + **60초 후 1회 자동 재시도** (일시적 차단 자동 회복; 재시도도 거부되면 수동 대응) |
| entry 거부 | ERROR 알림 + 재동기화만 — **재시도 없음** (진입 스킵이 안전한 방향) |
| 쿨다운 | `_last_liquidation_ts`/`_atr_cooldown_until`은 청산 **확정** 시에만 설정 (거부된 청산이 쿨다운을 남기지 않음) |
| 봉 누락 복구 | 확정 봉 메시지 미수신 시 워치독이 마감+10분 후 REST 백필(`bar_feed_stall` ERROR). 갭/중복 봉은 수신 시점에 백필/교체. 재시작 중 놓친 봉은 `supertrend_signals MAX(bar_ts)` 대비로 감지해 즉시 처리 |
| 실체결가 채택 | 진입 확정 시 entry_price를 주문가가 아닌 실체결가로 기록 (ATR exit 정확도 개선) |

### Equity Stop (청산위험 분석)

Equity Stop (−70%/−75%/−80%) 검증 결과:

- 0건 발동 (MDD 개선 효과 없음)
- Drawdown은 개별 거래가 아닌 **equity curve 전체 손실 구간**에서 발생
- 진입/청산 신호만으로 risk management 충분

---

## 백테스트 플로우

```mermaid
flowchart TD
    A["4h 캔들 신규 생성<br>(00:00, 04:00, 08:00 ...)"] --> B["Supertrend 계산<br>ST_upper, ST_lower"]
    B --> C["EMA(7), EMA(29), EMA(240) 계산"]
    C --> D{["진입 조건 체크\n3가지 모두?"]}
    
    D -->|"No"| Z["신호 대기"]
    D -->|"Yes"| E{["기존 포지션\n있음?"]}
    
    E -->|"Yes"| F["EMA(7) < EMA(29)<br>확인"]
    E -->|"No"| G["신규 Long 진입<br>3x 레버리지"]
    
    F -->|"Yes"| H["즉시 청산<br>시장가"]
    F -->|"No"| I["포지션 유지<br>ATR 손절/익절 모니터"]
    
    G --> J["ATR 손절/익절<br>설정"]
    J --> I
    
    I --> K{["청산 신호<br>발생?"]}
    K -->|"ATR 터치<br>또는 EMA"| H
    K -->|"No"| Z
    
    H --> L["포지션 청산<br>P&L 기록"]
    L --> M["60분 cooldown<br>신규 진입 차단"]
    M --> Z
    
    style G fill:#4caf50,color:#fff
    style H fill:#f44336,color:#fff
```

---

## 자본 배분 (단일 전략 고정)

Strategy Orchestrator는 단일 전략 모델로, **Supertrend에 항상 보유 자본의 100%를 배분**한다 (레짐 기반 동적 배분은 2026-05-25 제거). 전략 내부에서 배분 자본의 95% × 3배 레버리지로 포지션을 구성한다.

```python
# WeightManager.FIXED_WEIGHTS
{"supertrend": 1.0, "cash": 0.0}
```

- 배분: 전체 잔고의 100% → 전략이 95% × 3x 사용
- 현금 예비 없음 (항상 완전 배분)
- 진입/청산은 Supertrend 4h 신호로만 결정 (시장 레짐 무관)

---

## 극단 시나리오 분석

### Scenario 1: BTC +30% 급등

```
초기:
  - 자본: $200 USDT
  - Supertrend 배분: $150 (고정액)
  - BTC 가격: $60,000
  - 포지션: 0.005 BTC (3x)

상승: BTC → $78,000 (+30%)
  - Long 이익: 0.005 × (78,000 - 60,000) × 3 = $2,700
  - 수수료: -$20
  - 순 P&L: +$2,680
  - 최종 equity: $202,680
```

### Scenario 2: BTC -50% 극단 하락

```
초기:
  - 자본: $200 USDT
  - Supertrend 배분: $150
  - BTC 가격: $60,000

하락: BTC → $30,000 (-50%)
  - Long 손실: 0.005 × (30,000 - 60,000) × 3 = -$450
  - 수수료: -$20
  - 순 P&L: -$470
  - 최종 equity: -$270 (마진 콜)
  
→ Kill Switch 발동 (절대값 기준 -$50 이상)
→ 모든 포지션 즉시 청산
```

### Scenario 3: 극도의 분할 손실 (2022년 시나리오)

```
연속된 약세:
  - 1월: -10% (100회 소규모 손실)
  - 2월: -15% (100회)
  - 3월: -20% (100회)
  - MDD 누적: -84.28% (극단점)

→ 자본: $200 → $26 (마진 콜)
→ Kill Switch 자동 발동
```

---

## Kill Switch와 Supertrend

포트폴리오 레벨 Kill Switch 발동 시:

| Level | 조건 | 동작 |
|-------|------|------|
| **L1** | 전략 손실 > 3% | 포지션 청산 |
| **L2** | 일일 손실 > 5% | 포지션 청산 |
| **L3** | 시스템 장애 | 시장가 청산 |
| **L4** | 수동 비상 정지 | 즉시 청산 |

**Phase 5 절대값 AND**:

```
if (drawdown_pct <= -5.0) AND (drawdown_usd >= $50):
    trigger_kill_switch_l2()
```

---

## 관련 정책 및 문서

- [../btc-only.md](../btc-only.md) — BTC 단일 운영 정책
- [../leverage-limits.md](../leverage-limits.md) — 3배 레버리지 한정
- [../kill-switch.md](../kill-switch.md) — Kill Switch 4단계 정책
- [../deployment-position.md](../deployment-position.md) — 배포 시 포지션 유지
- [../operations/runbook.md](../operations/runbook.md) — 일상 운영 및 모니터링
- [funding-arb.md](funding-arb.md) — 이전 전략 (폐기됨, 히스토리 참조)

---

## 주의사항

⚠️ **이 전략은 극한의 위험을 수반합니다**:
1. MDD −84.28% → 최악의 경우 자본 손실 90%
2. 추세 반전 시 급격한 손실 가능
3. 메인넷 $200 USDT는 마진 콜 위험 높음
4. 24시간 강화 모니터링 필수

**이를 충분히 이해하고 수용할 수 있을 때만 운영하십시오.**
