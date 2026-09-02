---
title: 펀딩비 실측 반영 — #7908 비용 모델 정교화 결과
date: 2026-09-02
plan: .temp/2026-09-02_funding_cost_model_plan.md
basis: backtest/results/2026-08-31/csv_ohlcv_drift.md §7 (미해결 갭) · docs/cryptoengine/70-policy/strategy.md "최종 판정"
scope: 라이브 코드·yaml·Kill Switch·레버리지·배분 정책·`BYBIT_TESTNET` 무변경. 비용 모델 opt-in 추가만.
---

# 결론

**펀딩비를 반영해도 홀드아웃(2025-01~2026-08) net은 여전히 크게 양수다: +38.20% → +29.23% (−8.96%p).** `strategy.md`가 열어둔 taker+15bps 비관 시나리오(net −1.6%)에는 근처도 가지 않는다 — 펀딩비는 부호를 뒤집는 요인이 아니라 CAGR을 21.48%→16.68%로 깎는 정도다.

라이브 yaml·Kill Switch·레버리지·`BYBIT_TESTNET`·배분 규칙은 변경하지 않았다. `_replay_supertrend.py`의 펀딩 모델은 **opt-in**이며 인자 없는 기본 실행은 기존과 바이트 동일하다 — 정본 지표(전기간 CAGR 219.06%·MDD −66.70%·Sharpe 1.667)는 이 작업으로 바뀌지 않는다.

---

# 1. 데이터

Bybit 공개 REST(`/v5/market/funding/history`, category=linear, symbol=BTCUSDT — 라이브와 동일 상품)에서 직접 수집. 7,055개 정산 레코드, **2020-03-25 16:00 UTC ~ 2026-09-02 00:00 UTC**, 8시간 간격 예외 없음(갭 0건). 이전 구간은 API가 데이터를 주지 않는다(에러 아님 — 그 무렵 상품 출시로 추정). 검증 5개 항목(첫 timestamp·행수 정합·재실행 멱등성·부분fetch 이어붙임·API 재조회 대조) 전부 PASS. 상세: `backtest/results/funding/PROVENANCE.md`.

기존 `jesse_db.funding_8h`(8,856행, 출처 미검증)는 사용하지 않았다 — `csv_ohlcv_drift.md` §7 선례와 동일 원칙으로, 미검증 소스에 기대는 대신 처음부터 검증된 소스로 새로 받았다.

---

# 2. 엔진 변경

`cryptoengine/tests/fixtures/_replay_supertrend.py`의 `run_backtest()`에 `funding`/`funding_log` opt-in 파라미터를 추가했다. 인자를 안 주면(`None`) 기존과 완전히 동일 — 회귀 검증(`trades=198, CAGR=219.06450168817332%, MDD=-66.69712567226549%, Sharpe=1.6673376400212279`) 바이트 동일 PASS. 반환 시그니처(4-튜플)는 바뀌지 않아 기존 호출부(`main()`, `holdout_report.py`, `replay_lib.py`) 무수정.

**정산 시점**: Bybit 정산 시각(00:00/08:00/16:00 UTC)과 정확히 일치하는 4h 봉에서, **이전 봉부터 이미 열려 있던** 포지션에만 정산가 = 그 봉의 종가(별도 index/mark price 시계열 없음, 기존 미실현손익 계산과 같은 근사 수준)로 `cost = size * price * rate`를 부과한다. `size`는 이미 `자본 × 95% × 3배레버리지`로 계산된 BTC 수량이므로, 이 비용 산식은 **3배 레버리지 명목가에 대해 자동으로 이미 3배 반영**돼 있다(사용자 확인 완료 — 추가로 3을 곱하면 이중계상).

합성 데이터 단위검증(부호·정산액 계산·커버리지-밖 0처리·`funding_log=None` 무영향) 전부 PASS.

---

# 3. 결과

## 전기간 (bar[420] ~ 2026-08-28)

| | 펀딩 없음(기존) | 펀딩 반영 | 델타 |
|---|---|---|---|
| trades | 205 | 205 | — |
| CAGR | 197.67% | 159.22% | −38.45%p |
| MDD | −66.70% | −69.02% | −2.32%p |
| Sharpe | 1.613 | 1.464 | −0.149 |
| 펀딩 총액 | — | $11,745,171.94 | 최종 equity 대비 25.61% |

커버리지 밖(2020-03-25 이전, 0 가정) 보유시간 비율 **24.7%**, 205거래 중 **46건**이 그 이전에 진입(2017~2020 구간은 필연적으로 0 가정 — §0의 지시대로).

## 홀드아웃 (2025-01-01 ~ 2026-08-28)

| | 펀딩 없음(기존) | 펀딩 반영 | 델타 |
|---|---|---|---|
| trades | 41 | 41 | — |
| CAGR | 21.48% | 16.68% | −4.80%p |
| MDD | −52.06% | −52.46% | −0.40%p |
| Sharpe | 0.617 | 0.551 | −0.066 |
| **net** | **+38.20%** | **+29.23%** | **−8.96%p** |
| 펀딩 총액 | — | $831.05 | 최종 equity 대비 6.43% |

홀드아웃 41거래는 **전부(0/41) 커버리지 안** — 실측치로만 계산됐고 0-가정이 섞이지 않았다. 보유구간 평균 펀딩률 4.16e-05(0.00416%/8h) — naive 3x 연환산 ~13.7%/yr, 실측 시간가중(보유비율 ~34%) 반영 시 CAGR 델타 ~4.65%p로 실측 델타(4.80%p)와 정합(sanity check 통과). 펀딩 총액 부호는 양방향 모두 양수(롱이 지불 우세) — 크립토 강세장 펀딩의 통상적 방향과 일치.

---

# 4. 판정

> 실측 펀딩비 반영 시 홀드아웃(2025-01~2026-08) net은 **+29.23%**이며, 이는 `strategy.md` "최종 판정"이 taker+15bps 시나리오로 열어둔 비관 범위(−1.6%)와 **불일치한다(펀딩비 단독으로는 그 수준까지 떨어지지 않는다)**. 커버리지는 홀드아웃 보유시간의 100%(전기간은 24.7%가 2020-03-25 이전으로 0 가정).

이전 Part A(실 체결 슬리피지 실측)가 "체결 품질" 축의 비관 시나리오를 기각했다면, 이번 작업은 "비용 모델" 축의 마지막 미지수(펀딩비)를 닫는다. 두 축 모두 실측 결과 홀드아웃 net이 견고하게 양수임을 확인했다.

---

# 5. 산출물

| 경로 | 내용 |
|---|---|
| `backtest/scripts/data/fetch_bybit_funding.py` | 펀딩비 fetch (신규) |
| `backtest/results/funding/bybit_btcusdt_funding_8h.csv` | 펀딩비 캐시, 7,055행 (신규) |
| `backtest/results/funding/PROVENANCE.md` | 출처·검증 (신규) |
| `cryptoengine/tests/fixtures/_replay_supertrend.py` | `funding`/`funding_log` opt-in 파라미터 (수정, 회귀 계약 유지) |
| `backtest/scripts/analysis/funding_replay.py` | 비교 스크립트 (신규) |
| 이 문서 | 최종 비교표·판정 |

# 6. 범위 밖 (하지 않음)

라이브 yaml·Kill Switch·레버리지 3x·`BYBIT_TESTNET`·배분 규칙 변경. `jesse_db.funding_8h` 재라벨링. T1/T2 트립와이어 참조분포 재계산(별개 축, 다중검정 오염 방지). 파라미터 재탐색(`#7908` 유지는 이미 확정).
