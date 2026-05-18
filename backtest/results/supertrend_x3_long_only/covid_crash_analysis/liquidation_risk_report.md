# Liquidation Risk Analysis — SupertrendStrategy 4h 3x Long-Only

**분석 기간**: 2019-01-01 ~ 2026-04-30
**Liq 임계**: 진입가 대비 −67.17%  (liq_price = entry × 0.3283에서 = entry × 0.6717)
**레버리지**: 3x isolated (Bybit Perp, maint 0.5%)

> **검사 방법**: 각 long 포지션의 보유 기간 중 모든 4h 봉에 대해
> `candle.low ≤ liq_price` 조건을 전수 검사 (단순 -34% 봉 탐지 아님)

---

## combo_173  ✅ `ZERO_RISK`

- 전체 long trades: 324건
- Liq 위험 이벤트: **0건** (단 1건이라도 있으면 HAS_RISK)

**분석 결과: 전 구간에서 liq_price에 도달한 캔들 없음 → 완전 안전**


## combo_176  ✅ `ZERO_RISK`

- 전체 long trades: 316건
- Liq 위험 이벤트: **0건** (단 1건이라도 있으면 HAS_RISK)

**분석 결과: 전 구간에서 liq_price에 도달한 캔들 없음 → 완전 안전**


## combo_164  ✅ `ZERO_RISK`

- 전체 long trades: 322건
- Liq 위험 이벤트: **0건** (단 1건이라도 있으면 HAS_RISK)

**분석 결과: 전 구간에서 liq_price에 도달한 캔들 없음 → 완전 안전**


---

## 종합 결론

**모든 콤보에서 ZERO_RISK — 전 구간 어떤 포지션에서도 liq 위험 없음**

→ equity stop 안전장치 없이도 이론적으로 청산 위험이 없음.
  (단, Jesse 시뮬레이션은 bar-close 기준이므로 실거래 intrabar 위험은 별도 확인 필요)

*생성: 2026-05-17T23:02:35Z*

