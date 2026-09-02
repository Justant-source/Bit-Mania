---
title: bybit_btcusdt_funding_8h.csv — 출처·검증
date: 2026-09-02
plan: .temp/2026-09-02_funding_cost_model_plan.md §2.1
---

# 출처

`GET https://api.bybit.com/v5/market/funding/history?category=linear&symbol=BTCUSDT`
— 공개 REST, 인증 불필요. 상품: **USDT 무기한(linear) BTCUSDT** — 라이브(`strategy.md` §6)와
정확히 동일 상품.

fetch 스크립트: `backtest/scripts/data/fetch_bybit_funding.py`
재실행: `python backtest/scripts/data/fetch_bybit_funding.py` (기존 CSV의 마지막
timestamp+1ms부터 재개, 인자 없이 전체 재현 가능)

## 사용하지 않은 소스

`jesse_db.funding_8h`(8,856행, `schema.sql`)는 **사용하지 않았다.** 출처가 미검증이고
적재 스크립트가 트리에 없다(`backtest/results/2026-08-31/csv_ohlcv_drift.md` §7).
이번 CSV는 그 테이블을 손대지 않고 Bybit REST에서 처음부터 새로 받았다.

# 가용 구간 (실측)

| 항목 | 값 |
|---|---|
| 첫 레코드 | **2020-03-25 16:00:00 UTC** |
| 마지막 레코드 (이번 fetch 시점) | 2026-09-02 00:00:00 UTC |
| 그 이전(2020-03-25 이전) | API가 빈 배열을 반환(에러 아님) — Bybit BTCUSDT USDT 무기한 상품이 그 무렵 출시된 것으로 보임. 이진탐색으로 2019-10~2020-01-10 구간 0건, 2020-01 분기부터 존재 확인(계획 수립 세션) 후 이번 fetch에서 정확한 첫 timestamp 확정 |
| 정산 간격 | 8시간 (00:00 / 08:00 / 16:00 UTC) — 전 구간 예외 없이 정확히 8h 간격, 갭 0건 확인 |

fetch 일시: **2026-09-02 00:52 UTC**

# 검증 결과 (계획서 §2.1 5개 항목)

| # | 항목 | 결과 |
|---|---|---|
| 1 | 첫 timestamp가 2020-03-25 부근 | **PASS** — 2020-03-25 16:00:00 UTC |
| 2 | 총 행 수가 예상 범위(±수%) | **PASS** — 실제 7,055행, `(마지막-첫)/8h + 1` 계산값도 정확히 7,055 (오차 0%). 8h 등간격, 갭 0건 |
| 3 | 재실행 시 멱등성 (신규 0건) | **PASS** — 즉시 재실행 시 `newly fetched this run: 0`, 행 수·범위 불변 |
| 4 | `--end`로 부분 실행 후 재실행 이어붙임 | **PASS** — `--end 2020-06-01`로 203행만 받은 뒤 인자 없이 재실행 → 정본 전체 fetch와 완전히 동일한 7,055행(집합 diff 0) |
| 5 | 임의 5개 timestamp API 재조회 대조 | **PASS** — 2025-01-04·2021-01-23·2020-06-01·2025-10-10·2022-04-15 각 시각 재조회, `funding_rate` 5건 전부 CSV와 일치 |

# 파일 형식

```
timestamp_ms,funding_rate
1585152000000,3.059e-05
...
```

`funding_rate`는 Bybit 원 단위(예: `0.0001` = 0.01%/8h). 양수 = 롱이 숏에게 지불.

# 재실행 권고 주기

분기 1회, 또는 `docs/70-policy/strategy.md` "Part A 실 체결 슬리피지" 재실측과 동시 진행
(같은 라이브-검증 갱신 사이클에 묶는다). 재실행은 항상 안전 — 기존 행을 다시 쓰지 않고
마지막 timestamp 이후만 이어붙인다.
