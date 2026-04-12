# FA (Funding Arbitrage) 전략 사양서

## 1. FA 핵심 가설

**현물-선물 기저(Basis) 차익거래의 펀딩비 수취 모델**

- **역할**: 현물 BTC를 매수 → 선물 BTC를 매도(숏)하는 델타 뉴트럴 포지션
- **수익원**: 현물-선물 간 기저(Basis)에 포함된 펀딩비를 정기 정산으로 수취
- **정산 주기**: Bybit 기준 8시간 (00:00, 08:00, 16:00 UTC)
- **수익 특성**: 모멘텀이나 방향성과 무관한 "캐리(Carry)" 수익
- **핵심 가정**: 
  - Basis > 0 (현물이 선물보다 비쌈) 상태가 지속됨
  - 펀딩비 > 0일 때 숏 포지션에서 자금 조달자가 롱 보유자에게 지급
  - 거래소의 안정적인 청산가 유지 (극단 변동 시나리오 제외)

## 2. 진입 조건 (Entry Logic)

### 2.1 트리거 조건 (모두 만족 필요)

1. **펀딩비 임계값**: `min_funding_rate = 0.0001` (≥ 0.01% per 8h)
   - 양수 펀딩비가 충분히 높아야 함
   - 0.0001 이상이면 펀딩비 수익 > 거래 수수료 가능

2. **연속 판독 횟수**: `consecutive_intervals = 3`
   - 펀딩비가 3회 연속 정산 사이클(24시간)에서 임계값 이상이어야 진입
   - 이는 일시적 스파이크가 아닌 지속적 양수 펀딩비 환경 확인

3. **정산 타이밍**: 
   - UTC 기준 00:00, 08:00, 16:00 (또는 이에 해당하는 1h 캔들)
   - 정산 직후 새 펀딩비가 나올 때만 진입 판정 수행
   - 시간 기반 확인: `ts_dt.hour % 8 == 0 and ts_dt.minute == 0`

4. **포지션 상태**: 기존 포지션 없어야 함 (`self._position is None`)

### 2.2 진입 신호

```
If min_funding_rate > 0:
  → "Sell" (Select): 숏 포지션 (선물 매도, 현물 미보유)
  → 펀딩비 음수로 전환 시 반대 신호 발생

If min_funding_rate < 0 (음수 펀딩비):
  → "Buy" (Long): 롱 포지션 (선물 매수)
  → 펀딩비 양수로 전환 시 반대 신호 발생
```

**주의**: 실제 현물-선물 차익거래에서는 양쪽 모두 필요하나, 백테스트에서는 숏(펀딩비 양수) 시나리오만 주로 검증함.

## 3. 청산 조건 (Exit Logic)

포지션이 진입된 상태에서 다음 중 **하나라도 만족**하면 청산:

### 3.1 펀딩비 반전 (exit_on_flip)

```
If funding_direction == 1 (진입 시 양수):
  AND funding_rate < 0 (이제 음수):
    → reverse_count += 1
  ELSE:
    → reverse_count = 0

If reverse_count >= 3:
  → CLOSE 포지션
```

- 반전이 3회 이상 지속되면 청산
- 기저 조건이 악화됐음을 나타냄

### 3.2 보유 기간 만료

```
If bars_held >= max_hold_bars:
  → CLOSE 포지션
```

- `max_hold_bars = 168` (168 시간 = 7일)
- 극단 손실 방지 + 자본 회전 촉진

### 3.3 기타 종료 사유

- **Basis Divergence (기저 확산)**: 스프레드가 최대 임계값 초과
- **Liquidation Risk**: 마진 비율이 안전 수준 미만
- **Kill Switch**: 일일 손실액이나 최대낙폭 임계값 도달

## 4. 포지션 사이징 공식

### 4.1 기본 사이징

```python
equity = 현재 자산
fa_capital_ratio = 0.80  # 자본의 80%를 FA에 할당

position_capital = equity × fa_capital_ratio
entry_price = 현재 BTC 종가
size = position_capital / entry_price
```

### 4.2 레버리지 적용

```python
leverage = 5.0  # 5배 레버리지

notional = position_capital × leverage
# 명목가: 실제 교환소에 담보로 제시되는 포지션 가치

# 예시:
equity = 10,000 USDT
fa_ratio = 0.80 → 자본 8,000 USDT
leverage = 5x → 명목 40,000 USDT
BTC 가격 = 50,000 USDT
→ 수량 = 40,000 / 50,000 = 0.8 BTC
```

### 4.3 수수료 선납

```python
fee_entry = entry_price × size × TAKER_FEE
fee_entry = 50,000 × 0.8 × 0.00055 = 22 USDT

equity -= fee_entry  # 진입 당시 자산에서 차감
```

## 5. 레버리지 적용 방식: fa80_lev5_r30 의미

현재 백테스트 권장 설정: **fa80_lev5_r30**

| 파라미터 | 값 | 의미 |
|---------|-----|------|
| `fa_capital_ratio` | 0.80 | 초기 자본의 80%를 FA 전략에 할당 |
| `leverage` | 5.0 | 선물 포지션의 명목가에 5배 레버리지 적용 |
| `reinvest_ratio` | 0.30 | 수익 거래의 30%를 BTC 현물 매수로 재투자 |

### 성과 기준 (6년 백테스트, 2020-04 ~ 2026-03):
- **CAGR**: +34.87% (연 환산 수익률)
- **Sharpe 비율**: 3.583 (우수한 위험 조정 수익)
- **최대낙폭(MDD)**: -4.52% (낮은 손실)
- **청산 횟수**: 0회 (마진 안전성 우수)

### 대안 설정:
- **fa80_lev4_r30**: 더 보수적 (CAGR +28.56%, Sharpe 3.556)
- **fa80_lev5_r50**: 더 공격적 (CAGR +33.54%, Sharpe 1.867, 재투자 50%)

## 6. 수수료 모델

### 6.1 수수료율

| 항목 | 값 | 적용 시점 |
|------|-----|---------|
| Taker Fee (선물) | 0.00055 (0.055%) | 진입 및 청산 |
| Maker Fee (선물) | 0.0002 (0.02%) | 제한가 주문 (적용 안 함, Taker만 사용) |
| 슬리피지 | 거래량 기반 | 극단 변동 시 별도 모델 |

### 6.2 진입/청산 비용 (왕복)

```
진입 수수료 = 명목 × Taker Fee × 1회
청산 수수료 = 명목 × Taker Fee × 1회
왕복 총 수수료 = 명목 × 0.00055 × 2 = 명목의 0.11%

예:
명목 40,000 USDT → 44 USDT 수수료 (진입 22 + 청산 22)
```

### 6.3 재투자 수수료

재투자 시 BTC 현물 매수 → 별도 현물 거래 수수료 적용 (시뮬레이션에서는 간소화)

## 7. 펀딩비 P&L 계산 공식

### 7.1 8시간 정산 단위 수익

```python
# 정산 시점(ts)에서:
direction = 1 if side == "sell" else -1  # 1: 숏, -1: 롱
position_value = size × entry_price      # 명목가
funding_rate = 현재 펀딩비                  # Bybit 기준

net_funding_pnl = position_value × funding_rate × direction

# 예:
size = 0.8 BTC, entry = 50,000
position_value = 40,000 USDT
direction = 1 (숏)
funding_rate = 0.0001 (0.01% per 8h)

net_funding = 40,000 × 0.0001 × 1 = 4 USDT (8시간마다)
```

### 7.2 누적 펀딩 수익

```python
funding_accumulated = 0
for each settlement in holding_period:
    funding_accumulated += net_funding_pnl
    equity += net_funding_pnl

# 예: 7일 보유 (21회 정산)
21회 × 4 USDT = 84 USDT 펀딩비 수익
```

### 7.3 최종 P&L

```python
gross_pnl = funding_accumulated - fee_entry - fee_exit
net_equity = initial_equity + gross_pnl

# 예:
초기 자산 = 10,000 USDT
펀딩 수익 = 84 USDT
진입 수수료 = 22 USDT
청산 수수료 = 22 USDT
최종 P&L = 84 - 22 - 22 = 40 USDT
최종 자산 = 10,040 USDT
```

## 8. 재투자 로직

### 8.1 수익 거래 감지

```python
profitable_trades = [t for t in all_trades if t["pnl"] > 0]
```

### 8.2 재투자 자금 산정

```python
reinvest_ratio = 0.30  # 30%

for trade in profitable_trades:
    if trade["pnl"] > 0:
        reinvest_amount = trade["pnl"] × reinvest_ratio
```

### 8.3 BTC 현물 매수

```python
# 거래 종료 시점의 BTC 가격 조회
btc_price_at_close = ohlcv.loc[close_timestamp, "close"]

btc_qty = reinvest_amount / btc_price_at_close
cumulative_spot_btc += btc_qty
total_reinvested += reinvest_amount
```

### 8.4 최종 현물 포트폴리오 가치

```python
final_btc_price = ohlcv.loc[end_date, "close"]
spot_value = cumulative_spot_btc × final_btc_price
spot_return_pct = (spot_value - total_reinvested) / total_reinvested × 100
```

### 8.5 재투자 효과

- 초기 자본 대비 추가 수익 창출
- 2022년 약세장에서 BTC 현물 손실 발생 가능 (극도 동적 리스크)
- 포트폴리오 total return = FA 수익 + 현물 수익 - 재투자원금

## 9. 데이터 갭 경고 ⚠️

### 9.1 현황

**데이터베이스의 펀딩비 이력 범위: 2023-04-01 ~ 현재**

- Bybit에서 수집 가능한 최초 펀딩비 데이터: 2023년 4월
- 2020~2022년 데이터는 거래소에서 제공되지 않음

### 9.2 기존 6년 백테스트의 데이터 구성

백테스트 기간: 2020-04-01 ~ 2026-03-31

**데이터 소스:**
- 2020-04 ~ 2023-03 (36개월): 합성 폴백(Synthetic Fallback)
  - **펀딩비 = 고정 0.0001 (0.01% per 8h)**
  - 실제 펀딩비 데이터 없음 → 상수 사용
  
- 2023-04 ~ 2026-03 (36개월): 실제 Bybit 펀딩비 데이터

### 9.3 성과 신뢰성 평가

**⚠️ 주의: 결과 해석 필요**

| 기간 | 데이터 | 신뢰성 | 비고 |
|------|--------|---------|------|
| 2020-2022 (36개월) | 합성 0.0001 고정 | **낮음** | 극도 낙관적 가정 |
| 2023-2026 (36개월) | 실제 Bybit | **높음** | 검증된 데이터 |
| **6년 평균** | **혼합** | **중간-낮음** | **과장될 가능성 HIGH** |

### 9.4 영향 분석

**2020-2022 합성 폴백의 문제점:**

1. **극도로 낙관적**: 
   - 실제 2020-2022년 비트코인 약세장(Bear Market)에서 펀딩비는 음수 또는 매우 낮았음
   - 고정 0.0001로 가정하면 이 기간의 손실을 은폐

2. **결과 왜곡**:
   - 6년 CAGR +34.87%는 2020-2022 가상 수익에 크게 의존
   - 실제 로직: (2020-2022 가상 수익 + 2023-2026 실제 수익) / 6년
   - 가상 폴백 비중 감소 필요

3. **권장 검증 방법**:
   - 2023-2026 실데이터만으로 CAGR 재계산 (3년 기준 연율화)
   - 2020-2022 극단 시나리오(음수 펀딩비) 별도 스트레스 테스트
   - 현재 매개변수가 실제 펀딩비 환경에서도 작동하는지 확인

### 9.5 Jesse 백테스트 로드맵

**실데이터 기반 검증 필수:**

1. Bybit Historical API 또는 Binance Vision에서 실제 펀딩비 수집
2. 2020-04 ~ 2023-03 구간 합성 데이터 대체
3. Jesse 프레임워크에서 동일 로직으로 재시뮬레이션
4. 기존 6년 결과와 비교:
   - **기대값**: CAGR 감소 (가상 폴백 제거)
   - **위험**: Sharpe, MDD 악화 (실제 마이너스 펀딩비 포함)

## 참고: FA 전략 변형 비교

### A. Short Hold (현재 채택)
- `exit_on_flip=True`: 펀딩비 반전 시 3회 연속 → 청산
- 보유 기간: 최대 168시간 (7일)
- 성과: CAGR +34.87%, Sharpe 3.583 (데이터 갭 경고 주의)

### B. Long Hold
- `exit_on_flip=False`: 펀딩비 반전 무시
- 연속 반전 임계값: `negative_hours_before_exit` (예: 48시간 = 6회 정산)
- 성과: 수익 극대화, 단 드롤다운 증가

### C. Dynamic Exit
- 펀딩비 임계값 변동
- 기저 스프레드 추적 기반 동적 청산
- 미검증 (과거 실험 중단)

---

**최종 검증:** 메인넷 배포 전 Jesse 프레임워크에서 2023-2026 실데이터 기반 재시뮬레이션 필수
