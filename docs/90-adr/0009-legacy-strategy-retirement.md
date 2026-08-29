---
title: 레거시 전략 계보 일괄 폐기
adr_number: "0009"
status: Accepted
date: 2026-08-29
related_code:
  - cryptoengine/services/strategies/supertrend/
  - cryptoengine/config/orchestrator.yaml
supersedes: []
related_adr:
  - 0002
  - 0004
  - 0005
---

# ADR-0009. 레거시 전략 계보 일괄 폐기 + Track-C(멀티거래소) 폐기

## Status

Accepted (2026-08-29)

## Context

2026년 한 해 동안 R&D 트랙에서 총 **21개 전략**이 백테스트로 검증되었다. 이 중 실제로
메인넷 라이브 운영에 채택된 것은 단 하나, **017 Supertrend Triple Confirmation**
(현재 `combo #7908`, `docs/70-policy/strategy.md` SSOT)뿐이다.

나머지 20개(001~016, 018~021)는 각각 `backtest/docs/strategies/<번호>_*.md` 형태의
R&D 문서와, 일부는 대응 전략 코드(`backtest/strategies/*.py`)를 저장소에 남기고 있었다.
001(Funding Arbitrage)과 003(Adaptive DCA)은 개별 ADR(0004, 0005)로 이미 폐기가
기록되었으나, 나머지 18개 전략에는 개별 ADR이 없어 폐기 결정이 문서화되지 않은 채
코드와 R&D 문서만 저장소에 누적되고 있었다.

이 상태는 다음 문제를 낳았다:

1. **컨텍스트 오염**: 21개 전략의 R&D 산출물(백테스트 결과, 스크립트, 문서)이 저장소
   전반에 흩어져 있어, 코드베이스를 탐색하는 사람/에이전트가 현재 운영 전략(017)을
   식별하기 어려움.
2. **자원 낭비**: `backtest/results/7-strategies/`(약 1,071 파일, 22MB), 레거시 대시보드,
   비운영 전략의 데이터 수집 스크립트 등이 디스크·git 이력에 계속 누적.
2026-08-29 레거시 정리 작업(`.request/legacy-cleanup-plan-20260829.md`, Q3·Q4·Q7·Q8)에서
이 20개 전략 + Track-C(멀티거래소) 인프라를 일괄 정리 대상으로 확정했다.

### 검증 방법론 주의사항

이 ADR의 표는 아래 소스로 채워졌다:
- 기존 ADR 0001, 0002, 0004, 0005의 서술
- `backtest/docs/strategies/README.md` (삭제 직전 원본을 확보해 인용)
- **A3 에이전트가 20개 R&D 문서를 삭제하기 전 추출한 상세 백테스트 지표** (본 문서
  작성 도중 병렬 작업 결과로 전달받음, 아래 표에 전부 반영함)

A3가 원문에서 직접 추출한 값만 표에 반영했고, 그 외 지어낸 수치는 없다. 남은 공란은
"—"로 표기했다. 원문 전체는 삭제 이후에도 git 이력, 커밋 `8d6f1b79`
(태그 예정: `legacy-archive-2026-08-29`) 기준으로 복구 가능하다.

**번호 체계 주의사항 (A3 확인)**: 018~021 문서는 파일 자체의 내부 헤딩·상호 참조에
기존부터 존재하던 off-by-one 오류가 있었다(문서 내부 표기 번호가 실제 파일명보다
하나 낮고, 존재하지 않는 파일명을 상호 참조). 아래 표의 번호는 **실제 파일명 기준**이며
(예: `018_tradeiq_psar_ha.md` → 018), 문서 내부 서술 번호와는 다를 수 있다. 향후 git
이력에서 원문을 복구해 대조할 때 이 차이를 혼동하지 않도록 남긴다.

**추가 확인 (A3)**: `001_funding_arb.md`의 frontmatter는 삭제 시점까지도 `ACTIVE`로
표기되어 있었다 — ADR-0004(2026-05-18, FA 완전 폐기)와 3개월 이상 모순된 상태로
방치된 것이다. 이 mislabel은 오늘 문서 삭제로 함께 사라졌다.

## Decision

**001~016, 018~021 (20개 전략) 및 Track-C(Binance/OKX 멀티거래소 인프라)를 전량 폐기,
관련 R&D 문서·코드를 저장소에서 삭제한다.** 017 Supertrend만 유일한 운영 전략으로 남는다.

### 폐기 전략 목록

| # | 이름 | TF/대상 | 핵심 결과 | 판정 | 사유 |
|---|------|---------|-----------|------|------|
| 001 | 펀딩비 차익거래 (FA) | 8h funding, BTC 현물+무기한 델타중립 | Jesse 3년: CAGR +18.37%, Sharpe 3.268, MDD -5.15%. 실운영 24일: 거래 0건 (BEP-gate 버그) | CONDITIONAL_PASS → 폐기(ADR-0004) | Supertrend로 대체; 2025년+ 압축장 CAGR 1.64%로 알파 소멸; BEP-gate 버그로 실거래 0건 |
| 002 | 그리드 트레이딩 | 1h BTC 선물 | 6년 Sharpe -3.09, Return -87.77%, MDD -88.05%, 22/22 WF 윈도우 손실 | FAIL | 추세장 BTC에서 편측 누적 포지션; 거래 1,982건이 수수료에 잠식 |
| 003 | 적응형 DCA | 1h BTC | 3년 Sharpe -0.0069, MDD -59.26%; 최적 변형 6년 WF Sharpe 0.044, 일관성 0.409(기준 ≥0.50 미달), MDD 42.12%(기준 <40% 미달) | FAIL | 레짐 의존적(불장 +23% / 2025-26 -43%); EMA 필터 지연, 2022년 -71% 폭락 무방비 |
| 004 | 추세추종 (EMA/Donchian/ADX/MACD/BB, FA 결합 포함) | 멀티지표 BTC 선물 6년 | 최선(Donchian) Sharpe -0.230, CAGR -1.84%, MDD 16.11%; FA+TF 결합 WF Sharpe 0.777 | FAIL | 0.055% 선물 수수료 × 고빈도 거래가 알파 전량 잠식; 2022년 -70% 폭락에서도 숏 무수익 |
| 005 | ETF 플로우 모멘텀 | 1d BTC 2024-2026 | v3 CAGR +9.56%(목표 25%), Sharpe 1.26, MDD -2.86%, OOS/IS 일관성 -0.31(기준 ≥0.6 미달) | FAIL | 플로우 데이터 약 80% 합성(API 차단); 2024 불장 과적합, 2025 OOS Sharpe -1.29 |
| 006 | 변동성 스퀴즈 + HMM | 6h/1d BTC 3년 | 3년간 거래 7~8건뿐; CAGR +0.17~0.52%; Sharpe 12.9/22.7은 통계적으로 무의미 | FAIL | AND 결합 필터로 신호 빈도가 사실상 0으로 붕괴 |
| 007 | 펀딩비 극단치 역발상 | funding 기반 6년 | CAGR -0.90%, Sharpe 1.22(기준 ≥1.5 미달), MDD -61.32%, funding/fee 비율 0.05x(기준 2.0x 미달) | FAIL | 펀딩비 극단치는 후행 지표(추세 지속, 회귀 없음); 델타중립 헤지 부재 |
| 008 | 캘린더 스프레드 | 분기물 vs 무기한 3년 | CAGR -0.06%, Sharpe -1.01, 거래 9건 — 100% 합성 베이시스 데이터 기반 | **ARCHIVED (데이터 부재, 가설 반증 아님)** | Bybit/Binance 실 분기물 이력 데이터 없이 검증 불가 |
| 009 | 청산 캐스케이드 역발상 | 1h OHLCV 프록시 3년 | CAGR -3.93%, Sharpe -0.49, MDD -12.59%, 승률 36.84%, 거래 19건 | **ARCHIVED (L2 오더북 필요, 가설 반증 아님)** | 반등 가설 자체가 틀림(가격 지속 하락); OHLCV 프록시는 신뢰도 50%뿐 |
| 010 | 온체인 매크로 복합신호 (7지표) | 1d BTC 3년 | 7지표 AND 전체 결합 시 거래 0건; MVRV 단독: CAGR +8.24%, Sharpe 0.45, 17건 | FAIL | 저확률 필터 7개 AND 결합 → 결합확률 ~0 (설계 결함); 데이터 약 90% 합성 |
| 011 | XGBoost 멀티피처 앙상블 | 6h ML, 3년, 30-fold WF | OOS Sharpe 0.945(기준 ≥1.0 미달), CAGR +13.59%(기준 25% 미달), MDD -36.87%(기준 ≤20% 미달), fold간 변동계수 66.5%(기준 <30% 미달) | FAIL | 극심한 레짐 의존성 + fold간 심각한 불안정성 |
| 012 | HMM 레짐게이트 + LLM 메타 | 6h 3년 | v2 CAGR -0.20%, Sharpe 0.000 — 엔진 버그 3건 확인(잘못된 수수료, 확신도 필터 부재, 최소보유 부재); 보정된 v3 재실행 미완료 | FAIL/ARCHIVED (미해결) | 엔진 버그로 v2 결과 해석 불가; 수정된 v3 백테스트 끝내 미실행 |
| 013 | 일중 시즈널리티 (UTC 21-23시) | Jesse 3년 | CAGR -4.61%(기준 ≥10% 미달), Sharpe -2.192, 모든 레짐에서 음수 | FAIL | ETF 상장(2024+) 이후 구조가 2015-2022 학술 패턴을 역전시킴 |
| 014 | FOMC/CPI 매크로 이벤트 | 이벤트 기반 3년 | CAGR +0.27%(기준 ≥10% 미달), Sharpe 0.145, 거래 78건 = 연 26건(기준 ≥30건/년 미달) | FAIL | 연간 매크로 이벤트 수 부족; "뉴스에 팔기" 가설이 Range 레짐에서만 작동 |
| 015 | 역행 센티먼트 (Fear & Greed) | 3년 | CAGR -3.54%, Sharpe -0.517, 3년간 21건 = 연 7건(기준 ≥30건/년 미달); 불장 Sharpe -1.446 | FAIL | 2023년 이후 F&G 극단값 희소; 불장에서 "극단 탐욕" 숏 = 추세지속 함정 |
| 016 | Stochastic + EMA (Stoch/HA) | 멀티TF 2017-2026 | 최선(1D 양방향) CAGR 38.65%, Sharpe 0.870, MDD -69.75%; 스윕 40개 조합 전부 -999점, MDD -54~89% | FAIL/ARCHIVED | MDD가 Stoch 평균회귀+ATR스탑 구조 자체의 문제 — 추세장 BTC에서 튜닝으로 해결 불가 |
| 017 | Supertrend Triple Confirmation | 4h BTC Long-only | 운영 SSOT는 `docs/70-policy/strategy.md` 참조(combo #7908) | PASS | **유일 생존 — 운영 중** |
| 018 | TradeIQ PSAR-HA (PSAR+EMA200+RSI+HA) | 멀티TF 2017-2026 | 1D long_only 합격: CAGR 22.43%, Sharpe 0.864, MDD -26.09%; 4h는 CAGR 12.94%, MDD -29.96%로 ProfitFactor 기준 미달 | **CANDIDATE (1D 합격, 미배포)** | 라이브 Supertrend와 중복; Supertrend 성과 저하 시 폴백 후보로 보류 |
| 019 | TrendType (ADX/DMI) | 멀티TF 2017-2026 | 1D long_only 합격: CAGR 21.54%, Sharpe 0.919, MDD -27.54%; 4h는 CAGR 15.59%, MDD -33.81%로 미달 | **CANDIDATE (1D 합격, 미배포)** | Supertrend 대비 CAGR 낮음; MDD 헤지/분산 후보로 보류 |
| 020 | Supertrend + TrendType 하이브리드 (5-factor) | 멀티TF 2017-2026 | 4h long_only 합격: CAGR 29.51%, Sharpe 0.997, MDD -30.41%(후보 3종 중 최선) vs Supertrend 단독 CAGR 44.69%, Sharpe 1.246, 동일 MDD | **CANDIDATE (합격, 미배포)** | TrendType 필터 추가가 MDD 개선 없이 거래수·CAGR만 깎음(-15pp) — Supertrend 단독이 우위 |
| 021 | TradeIQ CCI + Chandelier Exit | 멀티TF 2017-2026 | 7개 변형 중 최하위; 4h long_only CAGR 4.29%, Sharpe 0.298; 1D는 거래 5건뿐 | FAIL/ARCHIVED | CCI 교차 신호가 지나치게 희소(4h 5년간 80건); 전 TF·방향에서 낮은 Sharpe |

**판정 표기 원칙**: 018/019/020은 FAIL이 아니다. 세 전략 모두 자체 백테스트 합격
기준(1D 또는 4h+1D 조건부)을 통과했으나, 017 Supertrend가 먼저 채택되어 병행 운영을
하지 않기로 한 정책(단일 전략 원칙, ADR-0003)에 따라 R&D 트랙이 종료된 CANDIDATE다.
이를 실패로 기록하는 것은 사실 왜곡이며, 향후 017 성과 저하 시 재검토 후보로서
가치가 있다는 점을 이 표에 정직하게 남긴다.

**008/009는 "가설 반증"이 아니라 "데이터 부재"로 ARCHIVED됨**: 두 전략 모두 명확한
음성 결과(negative alpha)가 아니라, 검증에 필요한 실데이터(캘린더 스프레드용 실 분기물
이력, 청산 캐스케이드용 L2 오더북)가 당시 확보 불가능해 프록시/합성 데이터로만 테스트된
채 보류되었다. 향후 해당 데이터가 확보되면 **재검토 가치가 있는 카테고리**로, 나머지
FAIL 전략들(가설 자체가 데이터로 반증됨)과 구분해서 읽어야 한다.

**020의 결과는 라이브 전략에 직접적으로 유효한 부정적 증거다**: Supertrend(017) 단독에
TrendType 필터를 추가한 하이브리드는 MDD 개선 없이 CAGR만 -15%p 깎았다(29.51% vs
44.69%, 동일 MDD -30%대). 즉 **"Supertrend에 TrendType을 얹는 방향은 다시 시도할
가치가 없다"**는 것이 백테스트로 확인된 결론이며, 향후 017 개선 R&D에서 이 조합을
재시도하기 전에 반드시 참고해야 한다.

### Track-C (Binance/OKX 멀티거래소) 폐기

Track-C는 Bybit 외 거래소(Binance, OKX)로 시세 수집·차익거래를 확장하려던 인프라
트랙이다. `.request/legacy-cleanup-plan-20260829.md` Q6에 따라 다음을 오늘 전량 삭제한다:

- `cryptoengine/services/market-data/binance_collector.py`, `okx_collector.py`
- `cryptoengine/shared/exchange/binance.py` (+ exchange factory 등록 해제)
- `cryptoengine/config/exchanges/binance.yaml`, `okx.yaml`
- `docker-compose.yml`의 `market-data-binance`, `market-data-okx` 서비스
- Docker 이미지 `cryptoengine-market-data-binance`(870MB), `cryptoengine-market-data-okx`(870MB)

**상태**: 미구현 완료(unimplemented) 상태였음 — 실거래 코드가 실제로 완성되어 운영된
적이 없다. Bybit 단독 운영(ADR-0001 BTC 단일 운영 정책과 결이 같은 단일 거래소 원칙)을
유지하며, **향후 거래소 이중화/마이그레이션이 실제로 필요해지면 그 시점의 요구사항에
맞춰 처음부터 새로 구현한다.** 오늘 삭제되는 코드를 베이스로 재사용하지 않는다 —
당시 설계가 현재 아키텍처(Kill Switch, PortfolioState 등)와 정합성 검증이 안 된 상태이기
때문이다.

## 삭제 범위 (카테고리별)

| 카테고리 | 대상 | 비고 |
|---|---|---|
| R&D 문서 | `backtest/docs/strategies/001~016.md`, `018~021.md` (20개) | 017만 존치 |
| 전략 코드 | 각 전략에 대응하는 `backtest/strategies/*.py` (FA/DCA 관련은 이미 이전 커밋에서 삭제됨, 나머지는 오늘 삭제) | A1~A3 작업 범위 |
| 백테스트 산출물 | `backtest/results/7-strategies/` 전체(약 1,071 파일/22MB), `backtest/results/adjusted_costs_7strategies/`(244KB) | supertrend 하위 결과 포함 전량 |
| 대시보드/스크립트 | `backtest/dashboards/strategy_dashboard.html`(22MB), `script/build_strategy_dashboard.py`, `run_top10_backtests.py`, `walk_forward.py` 외 분석 스크립트 다수 | 상세는 정리 계획 Phase 1 참조 |
| Track-C 코드 | 위 "Track-C 폐기" 절 목록 | market-data-binance/okx, exchange/binance.py |
| Track-C Docker 이미지 | `cryptoengine-market-data-binance`, `cryptoengine-market-data-okx` | Opus가 직접 삭제 (Q1 범위: 저장소 소유 이미지만, 전역 빌드 캐시 제외) |

**복구 참조**: 위 전체 삭제분의 유일한 영구 복구 지점은 git 태그
`legacy-archive-2026-08-29`(커밋 `8d6f1b79`)다. 부가적으로 `git bundle`과 논리 DB
백업이 `cryptoengine_pg-backups` 볼륨에 생성되었으나 **7일 후(2026-09-05경) 자동
만료**된다. 7일 경과 후 영구 복구 수단은 git 태그 하나뿐이다.

## Consequences

### 긍정 (Positive)

- 저장소에서 탐색 가능한 전략이 017 하나로 좁혀져, 신규 컨텍스트 진입(에이전트/사람)
  시 혼란 제거.
- 디스크·git 이력 부담 경감 (results/7-strategies 22MB + 대시보드 22MB + 관련 스크립트).
- Track-C 미구현 잔재 제거로 `docs/20-containers/containers.md`, `docs/30-components/components.md`
  (A8 담당)의 서비스 목록이 실제 운영 상태와 일치하게 됨.

### 부정 (Negative) — 잃는 것

- **20개 전략의 상세 R&D 서술(백테스트 파라미터 스윕, 실패 원인 분석, 재검증 라운드
  기록)이 저장소에서 더는 브라우징 불가능**. 예: 006(변동성 스퀴즈+HMM)의 정확한 파라미터
  조합별 결과, 011(XGBoost)의 피처 중요도 분석 등은 이 ADR의 표에 옮겨지지 않았다.
- 018/019/020 CANDIDATE 3종의 코드가 사라짐에 따라, 향후 017 성과 저하 시 즉시
  재가동할 수 있는 대안이 저장소에 남아있지 않게 됨 — git 이력에서 되살려야 함.
- Track-C 코드 삭제로 향후 멀티거래소 확장 시 처음부터 재설계·재구현이 필요.

### 복구 방법

```bash
# 개별 파일 복구 예시 (특정 전략 R&D 문서)
git show 8d6f1b79:backtest/docs/strategies/006_volatility_squeeze_hmm.md

# 삭제된 디렉토리 전체를 별도 워크트리로 복구
git worktree add /tmp/legacy-recovery 8d6f1b79 --detach
# 또는 태그 생성 후
git checkout legacy-archive-2026-08-29 -- backtest/docs/strategies/
```

## Rejected Alternatives

### 대안 A: 20개 문서를 `docs/90-adr/`나 별도 아카이브 디렉토리로 이동 보존

```
이유:
- Q3/Q8 결정: "ADR에 기록만 존치" — 전문 이동이 아니라 요약 흡수로 확정
- 21개 문서 전문을 보존해도 실제로 다시 읽히지 않을 가능성이 높음(017 채택 이후
  실질적 참조 빈도 0에 수렴)
- git 이력이 이미 완전한 아카이브 역할을 하므로 in-tree 중복 보존은 컨텍스트
  오염 문제를 해결하지 못함
```

### 대안 B: 018/019/020 CANDIDATE 코드만 별도 보존 (재도입 대비)

```
이유:
- Q7에서 "CANDIDATE 3종 폐기 확정"으로 명시적으로 결정됨
- 재도입 필요 시점의 시장 환경·아키텍처가 이미 달라졌을 것이므로 당시 코드를
  그대로 재사용하기보다 재검증 후 재구현이 안전
- git 이력으로 복구 가능해 실질적 손실 없음
```

## References

- `.request/legacy-cleanup-plan-20260829.md` (Q3, Q4, Q6, Q7, Q8)
- ADR-0001 (BTC 단일 운영 정책), ADR-0002 (Multi-symbol FA 검토, Superseded by 0004),
  ADR-0003 (Supertrend 단일 전략 채택), ADR-0004 (FA 폐기), ADR-0005 (Adaptive DCA 폐기)
- `docs/70-policy/strategy.md` — 017 Supertrend SSOT (combo #7908)
- 복구 지점: git 태그 `legacy-archive-2026-08-29` = 커밋 `8d6f1b79`

---

**작성일**: 2026-08-29
**상태**: Accepted
**시행일**: 2026-08-29 (R&D 문서·코드 삭제는 병렬 에이전트 A1~A3 작업으로 동시 진행)
