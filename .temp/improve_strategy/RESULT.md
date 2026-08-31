---
title: Supertrend v11 실행 결과 — #7908 유지
date: 2026-08-30
status: DONE (Stage A + hold-out). Stage B 스킵. 라이브 불변.
plan: .request/improve_strategy/2026-08-29_supertrend_v11_robust_optimum_plan.md
---

# 결론

**라이브는 combo `#7908` 유지.** Stage A 8윈도우에서 나온 채택 후보 5개는 정본 4h 리플레이 hold-out에서 전부 탈락. Stage B(교차 격자 ≤4,000)는 같은 8윈도우 선별이라 교체 근거가 되지 않음 → **진행하지 않음**.

라이브 yaml · Kill Switch · 레버리지 · `BYBIT_TESTNET=false`는 변경하지 않음.

# 한 줄 상태

| 단계 | 상태 |
|---|---|
| 환경 (S0·S1·S6 OHLCV) | 완료. Jesse 이미지 **재빌드 안 함** (2.1.2, 2026-05-18) |
| Stage A `v11a` | 완료. 2,493 combo × 8 = 19,944 jobs, fail 0. 2026-08-29 21:08 → 2026-08-30 14:30 KST |
| 8윈도우 §2 adopt | 후보 5개 (`#799 #895 #847 #871 #1034`) |
| hold-out S7 | 완료. 후보 5개 **전부 실패**. `#7908` 정본 게이트 PASS (198 / 219.06 / −66.70) |
| Stage B `v11b` | **스킵** (의미 없음 — 아래) |
| 라이브 파라미터 | **`#7908` 유지** (2.6 / 9 / 7 / 29 / 240 / atr 3.3) |

# 왜 8윈도우와 hold-out이 다른가

8윈도우는 Jesse 1m→4h, 윈도우별 $10k 독립, 산술평균 CAGR. hold-out은 라이브와 같은 Bybit-native 4h 종가 리플레이(수수료 0.055%/side, 95%×3x).

8윈도우에서 `#799`가 mean CAGR 301 vs `#7908` 281, worst MDD도 더 좋았지만, 정본 경로 전기간에서는 CAGR −29.7%p · MDD −10.3%p로 진다. 선별 지표가 라이브 경로로 이전되지 않음.

# Stage A 8윈도우 (Jesse, W1–W8)

`st_window_results` 집계. `v11a`는 `pg_aggregate` 미실행이라 `st_combos.mean_cagr` 컬럼은 NULL.

| combo | sf/sp/fe/se/de | mean CAGR% | worst MDD% | recent MDD% | n_pos | trades |
|---|---|---:|---:|---:|---:|---:|
| v10#7908 | 2.6/9/7/29/240 | 281.36 | −70.89 | −42.98 | 7 | 243 |
| v11a#799 | 2.4/9/7/29/210 | 301.05 | −68.96 | −40.20 | 7 | 255 |
| v11a#895 | 2.5/8/7/29/210 | 280.24 | −67.91 | −40.20 | 7 | 255 |
| v11a#847 | 2.5/6/7/29/210 | 279.95 | −68.15 | −40.20 | 7 | 253 |
| v11a#871 | 2.5/7/7/29/210 | 275.90 | −67.91 | −40.20 | 7 | 255 |
| v11a#1034 | 2.6/9/7/27/220 | 262.26 | −68.86 | −39.11 | 7 | 257 |

Stage B 축 win (pass ∧ R̃>#7908, 확장값): fast_ema 4–6, dir_ema 200–220, st_factor 2.45/2.55/2.65, st_period 5/11/12. slow_ema 31–32는 win 없음.

# Hold-out (라이브 4h 리플레이)

CSV: `cryptoengine/tests/fixtures/btc_4h_extended.csv` (19,777 bars, ~2026-08-28 20:00). 지표는 CSV 전체, 매매만 `[start,end)`.

`#7908` 정본 `--end 2026-05-01`: **198 / CAGR 219.06% / MDD −66.70% / Sharpe 1.667**.

## 전기간 (→ 2026-08-28)

| combo | trades | CAGR% | vs #7908 | MDD% | vs #7908 | Sharpe |
|---|---|---:|---:|---:|---:|---:|
| **v10#7908** | 207 | **212.10** | — | **−66.70** | — | 1.652 |
| v11a#1034 | 218 | 187.86 | −24.24 | −71.00 | −4.30 | 1.565 |
| v11a#799 | 216 | 182.39 | −29.71 | −76.97 | −10.27 | 1.537 |
| v11a#895 | 214 | 176.69 | −35.41 | −79.00 | −12.30 | 1.516 |
| v11a#847 | 213 | 173.51 | −38.59 | −78.82 | −12.12 | 1.504 |
| v11a#871 | 213 | 173.47 | −38.63 | −78.82 | −12.12 | 1.504 |

정본 구간(→2026-04-30)만 잘라도 동일 순위: `#7908` 219.06 / −66.70, 최선 후보 `#1034` 195.50 / −71.00.

## W9 (2026-05-01 → 2026-08-28, $10k 독립)

| rank | combo | trades | CAGR% | MDD% | final | W9 ok |
|---:|---|---:|---:|---:|---:|:---:|
| 1 | **v10#7908** | 9 | **75.35** | **−32.48** | $12,025 | Y |
| 2 | v11a#1034 | 9 | 45.05 | −36.55 | $11,299 | Y |
| 3 (동점) | #799 #847 #871 #895 | 10 | 22.84 | −39.92 | $10,699 | n |

W9 ok = lg가 하위 50% 컷오프(0.0675) **초과**. 동점 4개는 하위로 처리.

## 채택 게이트 (계획 §2.5 hold-out)

| combo | 전기간 CAGR≥ | 전기간 MDD≥ | W9 하위 50% 아님 | 통과 |
|---|:---:|:---:|:---:|:---:|
| v10#7908 | Y | Y | Y | (기준) |
| v11a#799 | n | n | n | n |
| v11a#895 | n | n | n | n |
| v11a#847 | n | n | n | n |
| v11a#871 | n | n | n | n |
| v11a#1034 | n | n | Y | n |

# Stage B를 안 하는 이유

계획 문장만 보면 축 win ≥1 이라 B로 가게 돼 있다. hold-out 이후에는 그 축(특히 dir_ema 200–220)이 정본 경로에서 `#7908`보다 나빴다. B는 같은 8윈도우 지표의 교차항을 하루 반 더 도는 일이고, 라이브 교체 근거가 되지 않는다.

의미 있는 후속은 Jesse 격자가 아니라 **선별 자체를 네이티브 4h 리플레이로 바꾸는 실험**뿐이며, 그건 계획의 Stage B가 아니다.

# 산출물 위치

| 파일 | 내용 |
|---|---|
| 이 파일 | 실행 결론 (에이전트 진입점) |
| `.temp/improve_strategy/v11_holdout.md` | hold-out 표 (복사) |
| `.temp/improve_strategy/v11_shortlist.json` | 6 combo × canonical/full/W9 수치 |
| `backtest/results/supertrend_x3_long_only/docs/sweeps/v11_holdout.md` | 동일, 스윕 docs 트리 |
| `backtest/scripts/analysis/holdout_report.py` | S7 러너 |
| `cryptoengine/tests/fixtures/_replay_supertrend.py` | `--csv/--params/--start/--end` (end 배타) |
| `backtest/scripts/analysis/v11a_vs_7908.py` | 8윈도우 §2 판정 (one-shot) |

재실행:

```bash
docker run --rm --cpuset-cpus 6,7 --network none \
  -v /home/justant/Data/Bit-Mania:/repo -w /repo \
  --entrypoint python cryptoengine-supertrend:latest \
  /repo/backtest/scripts/analysis/holdout_report.py
```

# 하지 말 것

- `docker compose build backtester` (Jesse latest 표류)
- `pg_worker.py` / `run_intrabar_backtest.py` 수정
- 라이브 yaml / Kill Switch / 3x 초과 / 테스트넷 전환
- `v11b` 격자 insert (요청 없는 한)
- 이 결과를 `docs/70-policy/strategy.md`에 넣기 전에 Doc-Sync 승인 필요 (커밋 시)

# 남은 선택 (사람)

1. 여기까지 닫기 — `#7908` 유지 (권장).
2. 별도 승인 후 `strategy.md` §9에 v10 완료 + v11 결론을 Doc-Sync.
3. 선별 지표를 4h 리플레이로 바꾸는 새 계획 — v11b가 아님.
