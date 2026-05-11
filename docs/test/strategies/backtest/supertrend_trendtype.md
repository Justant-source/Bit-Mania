---
title: SupertrendTrendTypeStrategy 파라미터 참조
category: backtest-strategy
related_code:
  - cryptoengine/services/jesse_engine/strategies/external/SupertrendTrendTypeStrategy.py
last_updated: 2026-05-11 (스윕 완료)
when_to_update: |
  - 파라미터 최적화 스윕 실행 후
  - 전략 로직 변경 시
---

# SupertrendTrendTypeStrategy

Supertrend + TrendType 복합 전략. 두 지표의 동시 확인으로 신호 품질 향상.

## 전략 개요

- **진입 조건**: TrendType == +2.0 AND Supertrend 상승 AND EMA 크로스업(롱)
- **방향성 필터**: direction_ema_len EMA
- **청산**: ATR 기반 stop-loss

## 파라미터 정의

| 파라미터 | 현재 기본값 | 최적화값 | 범위 | 설명 |
|---------|-----------|---------|-----|-----|
| `st_factor` | 3.0 | **2.0** | 1.0~5.0 | Supertrend ATR 배수 |
| `atr_len` | 14 | **10** | 5~30 | TrendType ATR 기간 |
| `atr_ma_len` | 20 | 20 | 10~50 | ATR 이동평균 기간 (고정) |
| `di_len` | 14 | 14 | 5~30 | DI/ADX 기간 (고정) |
| `smooth` | 1 | 1 | 1~5 | 스무딩 (고정) |
| `st_period` | 7 | 7 | 3~20 | Supertrend ATR 기간 (고정) |
| `fast_ema_len` | 7 | 7 | 3~20 | 빠른 EMA (고정) |
| `slow_ema_len` | 20 | 20 | 10~50 | 느린 EMA (고정) |
| `direction_ema_len` | 200 | 200 | 100~300 | 방향성 EMA (고정) |
| `atr_mult` | 3.0 | 3.0 | 1.0~5.0 | ATR stop-loss 배수 (고정) |

## 현재 기준 성과 (기본값, 2021-01~2026-04)

| TF | variant | CAGR | Sharpe | MDD | Trades | verdict |
|----|---------|------|--------|-----|--------|---------|
| 4h | bidirectional | +12.98% | 0.495 | -45.44% | 376 | FAIL |
| 4h | long_only | +27.30% | 0.966 | -26.92% | 192 | **PASS** |
| 1D | bidirectional | +10.99% | 0.539 | -34.57% | 55 | FAIL |
| 1D | long_only | +24.55% | 1.522 | -25.76% | 33 | **PASS** |

> long_only PASS. bidirectional MDD(-45%) 및 Sharpe 개선 목표.

## 파라미터 스윕 설정 (2026-05-11)

최적화 대상: `st_factor` × `atr_len`

| combo | st_factor | atr_len | 비고 |
|-------|----------|---------|-----|
| 1 | 2.0 | 14 | st_factor 낮춤 |
| 2 | 3.0 | 14 | **baseline** |
| 3 | 4.0 | 14 | st_factor 높임 |
| 4 | 3.0 | 10 | atr_len 낮춤 |
| 5 | 3.0 | 18 | atr_len 높임 |
| 6 | 2.0 | 10 | 둘 다 낮춤 |

## 최적화 히스토리

| 날짜 | combo | TF | variant | CAGR | Sharpe | MDD | 채택 |
|------|-------|----|---------|----|--------|-----|-----|
| 2026-05-11 | #6 (st_factor=2.0, atr_len=10) | 4h | long_only | +33.44% | 1.121 | -29.05% | **채택** |

> 스윕 2026-05-11 완료. st_factor=2.0 + atr_len=10 조합이 baseline(3.0/14) 대비 CAGR +6%p 향상. **최적값 st_factor=2.0, atr_len=10**.
