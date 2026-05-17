---
title: v9 sweep — dir_ema 하단 확장 + slow_ema 확장
date: 2026-05-17
---

# v9 Sweep 결과 (v9a + v9b)

## 목적

v8에서 direction_ema_len 최적값이 250~290 구간에 몰린 신호를 확인하고,
v8 하단(240, 260)과 slow_ema_len(26, 28)을 추가 탐색. st_factor=2.4 고정.

## 그리드

### v9a (64 combos) — dir_ema 하단 확장
```json
{
  "st_factor": [2.4],
  "st_period": [8, 9],
  "fast_ema_len": [7, 8],
  "slow_ema_len": [25, 27],
  "direction_ema_len": [240, 260],
  "atr_mult": [3.1, 3.2]
}
```

### v9b (112 combos) — slow_ema 확장 (dir_ema 240~400 전구간)
```json
{
  "st_factor": [2.4],
  "st_period": [8, 9],
  "fast_ema_len": [7, 8],
  "slow_ema_len": [26, 28],
  "direction_ema_len": [240, 260, 270, 290, 310, 330, 360, 400],
  "atr_mult": [3.1, 3.2]
}
```

총: **176 combos × 8 windows = 1,408 backtests**

## 결과 요약

### v9a (64 combos)
- Tier pass: 0
- Plateau quality: MIXED=32, EDGE=32
- Top combo: pk=2352, score=56.89, mean_cagr=425.7%, plateau=EDGE (dir_ema=240~260)

### v9b (112 combos)
- Tier pass: 0
- Plateau quality: MIXED=32, EDGE=80
- Top combo: pk=2518, score=59.79, mean_cagr=442.2%, plateau=MIXED (slow_ema=26~28)

## 핵심 발견

- **slow_ema 26~28이 25, 27보다 소폭 우월** — v9b top score(59.79) > v9a top score(56.89)
- **MIXED plateau** (v9b top) vs **EDGE plateau** (v9a top): v9b가 neighborhood 연속성 확보
- dir_ema 240~260 추가는 v8(250~400) 대비 유의미한 개선 없음 — 하단 경계 확인
- Tier pass 0: worst_mdd 기준 3x에서 모든 combos가 full period -80% 초과 (recent W7+W8 기준 필터 필요)

## 판정

- 탐색 완료. sweet-spot 코어 확인 (st_factor=2.4, st_period=8~9, fast_ema=7~8, slow_ema=25~28, dir_ema=250~360, atr_mult=3.1~3.2)
- 실거래 = fa80 funding-arb 단독 결론 불변
