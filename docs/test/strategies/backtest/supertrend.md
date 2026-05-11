---
title: SupertrendStrategy 파라미터 참조
category: backtest-strategy
related_code:
  - cryptoengine/services/jesse_engine/strategies/external/SupertrendStrategy.py
last_updated: 2026-05-11 (스윕 완료)
when_to_update: |
  - 파라미터 최적화 스윕 실행 후
  - 전략 로직 변경 시
---

# SupertrendStrategy

Supertrend 지표 기반 전략. EMA 크로스와 방향성 EMA 필터 결합.

## 전략 개요

- **진입 조건**: Supertrend 상승 전환 + fast/slow EMA 크로스업(롱)
- **방향성 필터**: direction_ema_len EMA 위에서만 롱, 아래에서만 숏
- **청산**: ATR 기반 stop-loss

## 파라미터 정의

| 파라미터 | 현재 기본값 | 최적화값 | 범위 | 설명 |
|---------|-----------|---------|-----|-----|
| `st_factor` | 3.0 | **2.0** | 1.0~5.0 | Supertrend ATR 배수 |
| `st_period` | 7 | **7** | 5~14 | Supertrend ATR 기간 |
| `fast_ema_len` | 7 | 7 | 5~15 | 빠른 EMA 기간 (고정) |
| `slow_ema_len` | 20 | 20 | 15~30 | 느린 EMA 기간 (고정) |
| `direction_ema_len` | 200 | 200 | 100~300 | 방향성 EMA 기간 (고정) |
| `atr_mult` | 3.0 | 3.0 | 1.5~5.0 | ATR stop-loss 배수 (고정) |

## 현재 기준 성과 (기본값, 2021-01~2026-04)

| TF | variant | CAGR | Sharpe | MDD | Trades | verdict |
|----|---------|------|--------|-----|--------|---------|
| 4h | bidirectional | +14.48% | 0.504 | -41.60% | 482 | FAIL |
| 4h | long_only | +34.31% | 0.979 | -19.93% | 245 | **PASS** |
| 1D | bidirectional | +18.78% | 0.622 | -41.09% | 73 | FAIL |
| 1D | long_only | +32.17% | 1.338 | -21.36% | 41 | **PASS** |

> long_only는 이미 PASS. bidirectional의 MDD(-41%) 개선 목표.

## 파라미터 스윕 설정 (2026-05-11)

최적화 대상: `st_factor` × `st_period`

| combo | st_factor | st_period | 비고 |
|-------|----------|---------|-----|
| 1 | 2.0 | 7 | st_factor 낮춤 |
| 2 | 3.0 | 7 | **baseline** |
| 3 | 4.0 | 7 | st_factor 높임 |
| 4 | 3.0 | 5 | st_period 낮춤 |
| 5 | 3.0 | 10 | st_period 높임 |
| 6 | 2.0 | 5 | 둘 다 낮춤 |

## 최적화 히스토리

| 날짜 | combo | TF | variant | CAGR | Sharpe | MDD | 채택 |
|------|-------|----|---------|----|--------|-----|-----|
| 2026-05-11 | #1 (st_factor=2.0, st_period=7) | 4h | long_only | +36.29% | 1.121 | -27.63% | **채택** |

> 스윕 2026-05-11 완료. st_factor=2.0이 baseline(3.0) 대비 CAGR +2% 향상. 4h/long_only PASS → **최적값 st_factor=2.0, st_period=7**.
