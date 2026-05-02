# 전략 테스트 기록 (Strategy Test Log)

> 지금까지 수행한 모든 백테스트 결과를 시간순으로 정리한 디렉토리.
> 각 파일은 한 전략의 전체 테스트 이력을 담는다.

## 현황 요약

| # | 파일 | 전략 | 판정 | 상태 |
|---|------|------|------|------|
| 001 | [001_funding_arb.md](001_funding_arb.md) | 펀딩비 차익거래 (FA) | ✅ PASS | **ACTIVE** |
| 002 | [002_grid_trading.md](002_grid_trading.md) | 그리드 트레이딩 | ❌ FAIL | ARCHIVED |
| 003 | [003_adaptive_dca.md](003_adaptive_dca.md) | 적응형 DCA | ❌ FAIL | ARCHIVED |
| 004 | [004_trend_following.md](004_trend_following.md) | 추세추종 (FA 결합 포함) | ❌ FAIL | ARCHIVED |
| 005 | [005_etf_flow_momentum.md](005_etf_flow_momentum.md) | ETF 플로우 모멘텀 | ❌ FAIL | ARCHIVED |
| 006 | [006_volatility_squeeze_hmm.md](006_volatility_squeeze_hmm.md) | 변동성 스퀴즈 + HMM | ❌ FAIL | ARCHIVED |
| 007 | [007_funding_extreme_reversal.md](007_funding_extreme_reversal.md) | 펀딩비 극단치 역발상 | ❌ FAIL | ARCHIVED |
| 008 | [008_calendar_spread.md](008_calendar_spread.md) | 캘린더 스프레드 | ❌ FAIL | ARCHIVED |
| 009 | [009_liquidation_cascade.md](009_liquidation_cascade.md) | 청산 캐스케이드 역발상 | ❌ FAIL | ARCHIVED |
| 010 | [010_onchain_macro.md](010_onchain_macro.md) | 온체인 매크로 복합신호 | ❌ FAIL | ARCHIVED |
| 011 | [011_xgboost_ensemble.md](011_xgboost_ensemble.md) | XGBoost 멀티피처 앙상블 | ❌ FAIL | ARCHIVED |
| 012 | [012_hmm_llm_meta.md](012_hmm_llm_meta.md) | HMM + LLM 메타전략 | ❌ FAIL | ARCHIVED |
| 013 | [013_intraday_seasonality.md](013_intraday_seasonality.md) | 일중 시즈널리티 | ❌ FAIL | ARCHIVED |
| 014 | [014_macro_event.md](014_macro_event.md) | FOMC/CPI 매크로 이벤트 | ❌ FAIL | ARCHIVED |
| 015 | [015_contrarian_sentiment.md](015_contrarian_sentiment.md) | 역행 센티먼트 (F&G) | ❌ FAIL | ARCHIVED |

**요약**: 15개 전략 중 1개(FA) PASS, 14개 FAIL/ARCHIVED

---

## 테스트 계보 (시간순)

```
Phase 3 (2026-04-05)
  └─ 001 FA 초기 백테스트 (3년/6년)
  └─ 002 그리드 트레이딩 — FAIL
  └─ 003 적응형 DCA — FAIL
  └─ 004 추세추종 결합 — FAIL

backtest_v2 (2026-04-10~11)
  └─ 005 ETF 플로우 모멘텀 — FAIL (합성 ETF 데이터)
  └─ 006 변동성 스퀴즈 + HMM — FAIL (연 2.3회 거래)
  └─ 007 펀딩비 극단치 역발상 — FAIL (수수료 > 수익)
  └─ 008 캘린더 스프레드 — FAIL (실 분기물 데이터 없음)
  └─ 009 청산 캐스케이드 — FAIL (데이터 50% 프록시)
  └─ 010 온체인 매크로 — FAIL (7지표 동시 조건 = 0 신호)
  └─ 011 XGBoost 앙상블 — FAIL (MDD -37%, 불안정)
  └─ 012 HMM+LLM 메타 — FAIL (엔진 버그, Sharpe 0.000)

backtest_v3 (2026-04-11, v2 일부 재검증)
  └─ 005/006/008/009/010/011/012 재검증 → 동일하게 FAIL

Phase 9~10 (2026-04-12)
  └─ 001 FA 재검증 — 3년 실데이터 CAGR +13.11%
  └─ 001 FA 오버레이 비교 — 원본 유지 결정

Phase 7+8 / backtest_v5 (2026-04-12, Jesse 엔진)
  └─ 001 FA Jesse 이식 검증 — CAGR +18.37% ✅
  └─ 013 일중 시즈널리티 — FAIL (ETF 이후 패턴 소멸)
  └─ 014 매크로 이벤트 — FAIL (연 26회, 거래수 부족)
  └─ 015 역행 센티먼트 — FAIL (극단값 희소)

Phase 5 실운영 (2026-04-07 ~ 2026-05-01)
  └─ 001 FA 실운영 24일 — 거래 0건 (임계값 25% APR → 8% APR 조정 중)
```

---

## FA 최종 채택 설정

| 파라미터 | 값 |
|---------|-----|
| 전략명 | `fa80_lev5_r30` |
| FA 자본 비율 | 80% |
| 레버리지 | 5x |
| 수익 재투자 | 30% → BTC 현물 매수 |
| 진입 조건 | 8h 펀딩비 ≥ 0.00007 (7.66% APR), 2회 연속 |
| 청산 조건 | 3회 연속 역전 OR 보유 168h 초과 |
| 3년 실데이터 성과 | CAGR +13.11%, Sharpe 3.25, MDD -1.14% |

---

## 파일 규칙

- 번호는 테스트 최초 수행 시점 기준 시간순
- 동일 전략의 재검증 결과는 해당 번호 파일에 Round를 추가
- 새 전략 추가 시 다음 번호를 부여하고 이 README 표를 업데이트
