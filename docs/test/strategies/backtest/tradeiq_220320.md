---
title: TradeIQ220320Strategy 파라미터 참조
category: backtest-strategy
related_code:
  - cryptoengine/services/jesse_engine/strategies/external/TradeIQ220320Strategy.py
last_updated: 2026-05-11 (스윕 완료)
when_to_update: |
  - 파라미터 최적화 스윕 실행 후
  - 전략 로직 변경 시
---

# TradeIQ220320Strategy

Parabolic SAR + RSI + Heikin Ashi 기반 전략 (TradeIQ 2022-03-20).

## 전략 개요

- **진입 조건**: PSAR 반전 + direction_ema_len 방향성 + RSI > 50(롱) / < 50(숏) + HA 캔들 확인
- **청산**: ATR 기반 stop-loss

## 파라미터 정의

| 파라미터 | 현재 기본값 | 최적화값 | 범위 | 설명 |
|---------|-----------|---------|-----|-----|
| `rsi_len` | 14 | **18** | 7~21 | RSI 기간 |
| `atr_mult` | 3.0 | **3.0** | 1.5~5.0 | ATR stop-loss 배수 |
| `psar_start` | 0.02 | 0.02 | 0.01~0.05 | PSAR 초기 가속 계수 (고정) |
| `psar_inc` | 0.02 | 0.02 | 0.01~0.05 | PSAR 가속 증가값 (고정) |
| `psar_max` | 0.2 | 0.2 | 0.1~0.5 | PSAR 최대 가속 계수 (고정) |
| `direction_ema_len` | 200 | 200 | 100~300 | 방향성 EMA 기간 (고정) |

## 현재 기준 성과 (기본값, 2021-01~2026-04)

| TF | variant | CAGR | Sharpe | MDD | Trades | verdict |
|----|---------|------|--------|-----|--------|---------|
| 4h | bidirectional | -3.25% | 0.060 | -44.78% | 807 | FAIL |
| 4h | long_only | +13.58% | 0.610 | -24.52% | 421 | **PASS** |
| 1D | bidirectional | +8.97% | 0.401 | -42.63% | 141 | FAIL |
| 1D | long_only | +24.46% | 0.894 | -21.61% | 87 | **PASS** |

> long_only PASS. bidirectional 음수 CAGR 및 MDD 개선 목표.

## 파라미터 스윕 설정 (2026-05-11)

최적화 대상: `rsi_len` × `atr_mult`

| combo | rsi_len | atr_mult | 비고 |
|-------|---------|---------|-----|
| 1 | 10 | 3.0 | rsi_len 낮춤 |
| 2 | 14 | 3.0 | **baseline** |
| 3 | 18 | 3.0 | rsi_len 높임 |
| 4 | 14 | 2.0 | atr_mult 낮춤 |
| 5 | 14 | 4.0 | atr_mult 높임 |
| 6 | 10 | 2.0 | 둘 다 낮춤 |

## 최적화 히스토리

| 날짜 | combo | TF | variant | CAGR | Sharpe | MDD | 채택 |
|------|-------|----|---------|----|--------|-----|-----|
| 2026-05-11 | #3 (rsi_len=18, atr_mult=3.0) | 1D | long_only | +29.94% | 1.080 | -27.34% | **채택** |

> 스윕 2026-05-11 완료. rsi_len=18이 baseline(14) 대비 CAGR +5.5%p 향상. **최적값 rsi_len=18, atr_mult=3.0**.
