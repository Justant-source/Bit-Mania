---
title: v8 Direction EMA Extension Sweep
sweep_id: v8
date: 2026-05-17
combos: 224
windows: 8
total_backtests: 1792
runtime_min: 55
---

# v8 — Direction EMA Extension Sweep

## 목적

v5_2 sweet-spot에서 상위 combo가 모두 `direction_ema_len=230~250` 경계에 몰림 (v6 long-trend 신호).  
코어 파라미터 고정, `direction_ema_len`만 250→400으로 확장 탐색.

## 그리드

| param | 값 |
|---|---|
| st_factor | 2.4, 2.5 (2) |
| st_period | 8, 9 (2) |
| fast_ema_len | 7, 8 (2) |
| slow_ema_len | 25, 27 (2) |
| direction_ema_len | **250, 270, 290, 310, 330, 360, 400 (7)** |
| atr_mult | 3.1, 3.2 (2) |

조합: 2×2×2×2×7×2 = **224 combos × 8 windows = 1,792 backtests**

## 결과 요약 (PostgreSQL `st_combos` WHERE `sweep_id='v8'`)

| 지표 | 값 |
|---|---|
| 완료 combos | 224 / 224 |
| 완료 window_results | 1,792 / 1,792 (0 failed) |
| Plateau quality | MIXED 224 / PLATEAU 0 / ISLAND 0 (DEFAULT_GRID 미포함 — 예상) |
| Top combo_pk | 2278 (sweet_spot_score=59.01) |
| Top mean_cagr | **431.3%** |
| v5_2 top mean_cagr 대비 | ~+115% (v5_2 top ≈200%) |

## 핵심 발견

**direction_ema가 길수록 성능 향상이 뚜렷함.**  
v5_2 range(230~270)에서 v8 range(250~400)로 확장하자 top mean_cagr이 2배 이상 상승.  
→ direction filter EMA 400 이상 추가 탐색 여지.

## 원본 결과 디렉토리

`backtest/results/combo_0_W1` ~ `combo_223_W8` (1,792개, root 소유)  
**2026-05-17 삭제됨 — PG가 SoT (st_window_results 1,792행 complete=True)**

## 관련 커밋

- `1ba0822` — v8 sweep 착수 (Phase B)
- `06949f2` — pg_worker race condition + pg_aggregate 구문 수정
- `06b2d59` — pg_aggregate Decimal 수정 + build_dashboard 템플릿 fallback
