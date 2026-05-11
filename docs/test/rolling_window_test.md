---
title: 롤링 윈도우 테스트 방법론
category: test
related_code:
  - cryptoengine/services/jesse_engine/scripts/rolling_window_analysis.py
  - cryptoengine/backtest-results/data/7-strategies/rolling_window/
last_updated: 2026-05-11
when_to_update: |
  - 백테스트 데이터 재생성 후 재실행 시
  - 판정 기준(threshold) 변경 시
  - 전략 추가/삭제로 대상 조합 변경 시
---

# 롤링 윈도우 테스트 방법론

## 개요

특정 기간 백테스트 전체 성과만 보면 "평균이 좋아 보이지만 특정 시기에는 항상 손실"인 전략을 발견하지 못한다.
**롤링 윈도우 테스트**는 2021-01 ~ 2026-04 전체 기간을 다양한 길이의 창(window)으로 슬라이딩하며
각 구간별 누적 수익을 측정하여, **어떤 시기에도 양의 수익을 내지 못하는 전략**을 식별한다.

```
전체 기간: [2021-01 ─────────────────────────────── 2026-04]

 3개월 창:  [21-01~03] [21-04~06] [21-07~09] ... (21 windows)
 6개월 창:  [21-01~06] [21-04~09] [21-07~12] ... (20 windows)
 9개월 창:  [21-01~09] [21-04~12] ...          (19 windows)
12개월 창:  [21-01~12] [21-04~15] ...          (18 windows)
  ⋮
63개월 창:  [21-01~26-03]                      ( 1 window )
총 231 windows × 조합 수  (3개월 분기 시프트)
```

## 파라미터

| 항목 | 기본값 | 설명 |
|---|---|---|
| 데이터 범위 | `2021-01 ~ 2026-04` | 전체 백테스트 기간 |
| Window sizes | `3~63개월 (3개월 단위, 21가지)` | 슬라이딩 창 크기 |
| 시프트 단위 | **3개월 (분기 단위)** | 창을 3개월씩 이동 |
| 대상 | 7 strategies × {long_only, bidirectional} × {1h, 2h, 4h, 1D} | x2/x3 레버리지 제외 |
| Starting balance | $10,000 | 각 윈도우 시작 시점에 새로 투입 (가상) |
| Margin cap | 0.95 | 거래당 잔고의 최대 95%까지 margin 사용 (`--cap`) |
| Liquidation | 0.05 | 잔고가 starting의 5% 이하로 떨어지면 청산 |

### 잔고 시뮬레이션 (Balance-bounded simulation)

각 윈도우는 **$10,000을 새로 투입한 것처럼** 시뮬레이션된다. raw 백테스트 결과를
그대로 합산하면 시작 시점의 누적 잔고(예: 2024-01-01에 $212)가 영향을 미쳐 의미가
없으므로, 다음 절차로 재계산한다:

```
virtual_equity = $10,000  (윈도우 시작)
window strict trades (opened_at >= start AND closed_at <= end) 시간순:
  raw_size_ratio = raw_margin / raw_equity_at_open
                  (= "raw 백테스트에서 잔고의 몇 %를 margin으로 썼는가")
  capped_ratio   = min(raw_size_ratio, 0.95)
  v_margin       = virtual_equity × capped_ratio
  v_pnl          = v_margin × (raw_pnl / raw_margin)   # ROI% 유지
  virtual_equity += v_pnl
  
  if virtual_equity ≤ $500 (5%): 청산, 이후 거래 무시
net_pnl = virtual_equity - $10,000
```

이 방식은 dashboard의 KPI/equity 차트/trade table 모두에서 동일하게 사용되어 결과가 일관된다.

## 판정 기준

### 조합(combo) 단위
| 지표 | 정의 |
|---|---|
| `fail_strict` | 구간 누적 Net PnL ≤ $0 |
| `fail_meaningful` | 구간 누적 Net PnL < -$100 (1% 손실 이상) |
| `fail_strict_ratio` | 전체 윈도우 중 fail_strict 비율 |
| `any_positive_window` | 어느 구간에서든 양수 수익 존재 여부 |

### 전략(strategy) 단위
| Verdict | 조건 | 의미 |
|---|---|---|
| `DELETE` | 8개 조합 모두 `any_positive_window = False` | 어떤 TF/variant에서도 양수 구간 없음 → 삭제 |
| `WEAK` | 일부 조합은 never positive, 일부는 OK | 특정 TF/variant 조합만 정리 가능 |
| `KEEP` | 모든 조합에서 양수 구간 최소 1회 존재 | 유지 |

## 스크립트

```
cryptoengine/services/jesse_engine/scripts/rolling_window_analysis.py
```

### 사전 조건

백테스트 데이터가 최신 상태여야 한다:
1. Jesse 백테스트 완료 (`EXECUTION_SUCCESS.marker` 존재)
2. `apply_maker_fee.py` 실행 완료 (fee rate 0.02% 반영)
3. `audit_v4_data.py` 모두 PASS 확인

```bash
# 사전 검증
python3 cryptoengine/services/jesse_engine/scripts/audit_v4_data.py
# → 모든 체크 PASS 확인 후 진행
```

### 기본 실행 (heatmap 포함)

```bash
# 프로젝트 루트에서 실행
python3 cryptoengine/services/jesse_engine/scripts/rolling_window_analysis.py
```

### 빠른 실행 (heatmap 제외)

```bash
python3 cryptoengine/services/jesse_engine/scripts/rolling_window_analysis.py --skip-plots
```

### 옵션 전체

```bash
python3 cryptoengine/services/jesse_engine/scripts/rolling_window_analysis.py \
    --variants long_only bidirectional \   # 분석 대상 variant (기본: 1x 둘 다)
    --timeframes 1h 2h 4h 1D \            # 분석 대상 TF (기본: 전체)
    --window-sizes 3 6 12 24 36 \         # 월 단위 window (기본: 5가지)
    --start 2021-01 --end 2026-04 \       # 기간 (기본: 전체 백테스트 범위)
    --cap 0.95 \                          # 거래당 잔고 사용 최대 비율 (기본: 0.95)
    --threshold-meaningful -100 \         # fail_meaningful 기준 ($)
    --skip-plots \                        # heatmap 생성 안 함
    --heatmap-all                         # fail 여부 무관 전체 heatmap
```

### 특정 기간만 검증 (재실행 시)

```bash
# 예: 2023년 이후만 빠르게 재검증
python3 cryptoengine/services/jesse_engine/scripts/rolling_window_analysis.py \
    --start 2023-01 --end 2026-04 --skip-plots
```

## 출력 파일

`cryptoengine/backtest-results/data/7-strategies/rolling_window/`

| 파일 | 내용 |
|---|---|
| `windows_detail.csv` | 모든 (조합 × 윈도우) 평가 결과 — 핵심 원본 데이터 |
| `combo_summary.csv` | 56 조합별 요약 (fail 비율, worst/best 구간, 전체기간 PnL) |
| `strategy_verdict.csv` | 7 전략 단위 판정 (DELETE / WEAK / KEEP) |
| `strategy_verdict.md` | 사람이 읽는 리포트 (Tier 1~3 테이블 + 판정) |
| `heatmap_*.png` | 구간별 PnL 히트맵 (x축=시작월, y축=window크기, 색=수익) |

### windows_detail.csv 컬럼

| 컬럼 | 설명 |
|---|---|
| `strategy, variant, timeframe` | 조합 식별자 |
| `window_size_months` | 창 크기 (3/6/9/.../63, 3개월 단위 21가지) |
| `start_month, end_month` | 구간 (YYYY-MM) |
| `net_pnl` | 잔고 시뮬레이션 결과 누적 PnL ($, $10k 기준) |
| `final_equity` | 윈도우 종료 시점 가상 잔고 ($10,000 + net_pnl) |
| `n_trades` | strict filter로 추출된 거래 수 |
| `n_trades_used` | 실제 시뮬레이션에 반영된 거래 수 (raw_eq_open ≤ 0인 거래 제외) |
| `liquidated` | 윈도우 내 청산 발생 여부 (잔고 ≤ $500) |
| `fail_strict` | `net_pnl <= 0` |
| `fail_meaningful` | `net_pnl < -100` |

## 결과 해석 가이드

### Heatmap 읽는 법

```
y축 = window 크기 (3m/6m/9m/.../63m)
x축 = window 시작 월 (2021-01 → 2026-01)
색  = net_pnl  파란색=수익  흰색=0  빨간색=손실
```

- **왼쪽 전체가 빨간색**: 2021년 초 bull run 시기에 특히 취약
- **특정 y행 전체가 빨간색**: 해당 기간 길이에서 항상 손실 (예: 36m 행)
- **오른쪽으로 갈수록 파란색**: 최근 시장에서만 수익

### 삭제 결정 기준 (권장)

엄격한 DELETE (모든 구간 손실)는 잘 발생하지 않는다. 실전에서는 아래 기준을 조합한다:

| 기준 | 해석 |
|---|---|
| `fail_strict_ratio > 0.80` | 10개 중 8개 구간에서 손실 — 실전 사용 불가 |
| `full_period_pnl < -3,000` | 전체기간 손실 $3k 이상 ($10k 기준 30%+) |
| 전략 평균 `full_period_pnl < 0` | 8개 조합 평균이 음수 — 전략 자체가 불리 |

## 재실행 절차 (백테스트 수정 후)

```
1. Jesse 백테스트 재실행 (오류 수정 후)
   └─ v4_run_all.py 또는 개별 전략 재실행

2. Maker fee 재적용
   python3 scripts/apply_maker_fee.py

3. 데이터 무결성 검증
   python3 scripts/audit_v4_data.py
   → 모두 PASS 확인

4. 롤링 윈도우 테스트 재실행
   python3 scripts/rolling_window_analysis.py --skip-plots

5. Dashboard 재빌드 (선택)
   python3 scripts/build_v4_dashboard.py
```

## 관련 문서

- [backtest-skillset.md](backtest-skillset.md) — 백테스트 스크립트 전체 인덱스
- [jesse-engine.md](jesse-engine.md) — Jesse 엔진 설정 및 실행
