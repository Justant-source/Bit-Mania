#!/usr/bin/env python3
"""v12 G6-extension — funding-cost sensitivity re-check of the FROZEN v12 grid.

Does NOT modify replay_lib.py / run_grid.py / apply_plateau.py or backtest/results/v12/*
(all frozen per PREREGISTRATION.md). Re-scores the same 1,536-combo design-window grid
(bar[420]..2025-01-01) with Bybit funding cost applied (opt-in run_backtest(funding=...)
param, committed 2026-09-02) to check whether the already-reached NO_PLATEAU /
#7908-non-dominated conclusion still holds under this additional real-world cost.

This is a sensitivity check (same category as v12's G6 execution-assumption sensitivity),
not a new parameter search. No new candidate is selected or adopted here, regardless of
what the grid shows.

Usage: python backtest/scripts/analysis/v12/funding_sensitivity.py
"""
from __future__ import annotations

import csv
import importlib
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import replay_lib as L  # noqa: E402

RS = importlib.import_module("_replay_supertrend")

FUNDING_CSV = Path(__file__).resolve().parents[3] / "results" / "funding" / "bybit_btcusdt_funding_8h.csv"
OUT_DIR = Path(__file__).resolve().parents[3] / "results" / "2026-09-02"
OUT_PARQUET = OUT_DIR / "grid_scores_funding.parquet"
OUT_MD = OUT_DIR / "funding_grid_sensitivity.md"

C2_POS_BLOCK_RATIO = 0.64   # same as apply_plateau.py C2
C3_MIN_TRADES = 120         # same as apply_plateau.py C3
C4_NEIGHBOR_PASS_RATIO = 0.70

# Populated in main() before the ProcessPoolExecutor is created; inherited by fork()'d
# workers via copy-on-write (default multiprocessing start method on Linux) instead of
# being pickled per-call.
FUNDING: dict[int, float] = {}


def load_funding(path: Path) -> dict[int, float]:
    out = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            out[int(row["timestamp_ms"])] = float(row["funding_rate"])
    return out


def score_one(params: dict) -> dict:
    trades, eq = L.run(params, end_ms=L.design_end_ms(), funding=FUNDING)
    m = RS._metrics(trades, eq)
    lgs = L.block_lg(eq)
    s_raw = L.score_raw(lgs)
    row = dict(params)
    row.update(
        n_trades=len(trades), mdd=m["mdd"], cagr=m["cagr"], sharpe=m["sharpe"],
        s_raw=s_raw, n_pos_blocks=sum(1 for x in lgs if x > 0),
    )
    for i, lg in enumerate(lgs):
        row[f"lg{i}"] = lg
    return row


def main():
    global FUNDING
    FUNDING = load_funding(FUNDING_CSV)
    print(f"funding: {len(FUNDING)} settlements loaded from {FUNDING_CSV}")

    # #7908 baseline, design window (bar[420]..2025-01-01), funding-adjusted
    b = L.BASELINE
    trades_b, eq_b = L.run(b, end_ms=L.design_end_ms(), funding=FUNDING)
    m_b = RS._metrics(trades_b, eq_b)
    lgs_b = L.block_lg(eq_b)
    s_raw_b = L.score_raw(lgs_b)
    n_pos_b = sum(1 for x in lgs_b if x > 0)
    print(f"#7908 funding-adjusted design window: trades={len(trades_b)} mdd={m_b['mdd']:.3f} "
          f"cagr={m_b['cagr']:.3f} sharpe={m_b['sharpe']:.3f} s_raw={s_raw_b:.4f} "
          f"n_pos_blocks={n_pos_b}/{L.N_BLOCKS}")

    C1_MDD_MIN = m_b["mdd"]  # new funding-adjusted #7908 design-window MDD (was -66.697 pre-funding)

    combos = L.all_combos()
    print(f"scoring {len(combos)} combos with funding applied...")
    t0 = time.time()
    rows = []
    with ProcessPoolExecutor(max_workers=8) as ex:
        for i, row in enumerate(ex.map(score_one, combos, chunksize=8)):
            rows.append(row)
            if (i + 1) % 200 == 0:
                print(f"  {i+1}/{len(combos)}  {time.time()-t0:.1f}s")
    df = pd.DataFrame(rows)
    print(f"grid scored in {time.time()-t0:.1f}s")

    df["c1"] = df["mdd"] >= C1_MDD_MIN
    df["c2"] = df["n_pos_blocks"] >= np.ceil(C2_POS_BLOCK_RATIO * L.N_BLOCKS)
    df["c3"] = df["n_trades"] >= C3_MIN_TRADES
    df["hard_pass"] = df["c1"] & df["c2"] & df["c3"]

    # Direct dominance vs #7908's OWN funding-adjusted baseline (not the old pre-funding
    # -66.697/266.254 numbers) — MDD and CAGR both >=.
    df["dominates_7908"] = (df["mdd"] >= m_b["mdd"]) & (df["cagr"] >= m_b["cagr"])
    n_dominates = int(df["dominates_7908"].sum())

    key = {L.key5({k: row[k] for k in L.PARAMS5}): i for i, row in df.iterrows()}
    nb_pass_ratio = {}
    for i, row in df.iterrows():
        params = {k: row[k] for k in L.PARAMS5}
        nbs = L.neighbors(params)
        passes = [key[L.key5(nb)] for nb in nbs if L.key5(nb) in key]
        cnt = len(passes)
        pr = (df.loc[passes, "hard_pass"].sum() / cnt) if cnt else 0.0
        nb_pass_ratio[i] = pr
    df["nb_pass_ratio"] = df.index.map(nb_pass_ratio)
    df["c4"] = df["nb_pass_ratio"] >= C4_NEIGHBOR_PASS_RATIO
    df["plateau_pass"] = df["hard_pass"] & df["c4"]

    n_hard = int(df["hard_pass"].sum())
    n_plateau = int(df["plateau_pass"].sum())
    max_ratio = float(df.loc[df.hard_pass, "nb_pass_ratio"].max()) if n_hard else 0.0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PARQUET, index=False)

    unchanged = n_dominates == 0 and n_plateau == 0
    verdict_line = (
        "#7908은 펀딩비 반영 후에도 이 격자에서 비지배·plateau 없음 상태를 유지한다 — "
        "v12 결론(#7908 유지) 불변."
        if unchanged else
        "주의: 펀딩비 반영 후 결과가 원래 v12 결론과 달라졌다 — 사람 재검토 필요 "
        "(이 스크립트가 새 후보를 채택한 것은 아님, 사전등록 절차 없이 나온 관찰일 뿐)."
    )

    lines = []
    lines.append("# v12 격자 — 펀딩비 반영 민감도 재검 (G6 확장)\n")
    lines.append("date: 2026-09-02")
    lines.append("plan: backtest/results/2026-09-02/funding_cost_report.md 후속, "
                  "v12 프레임워크 위에서 실행")
    lines.append(
        "frozen v12 파이프라인(`replay_lib.py`/`run_grid.py`/`apply_plateau.py`, "
        "`backtest/results/v12/*`)은 전혀 수정하지 않았다. 같은 1,536-combo 설계구간 "
        "(bar[420]~2025-01-01) 격자를 펀딩비(Bybit 실측, 2020-03-25~ 커버리지, 그 이전은 "
        "0 가정)만 추가해 재채점했다. 새 파라미터 탐색이 아니라 v12 G6(실행가정 민감도)의 "
        "연장이며, 이 재검에서 나온 어떤 combo도 채택 후보로 취급하지 않는다.\n"
    )
    lines.append("## #7908 설계구간 기준선 비교\n")
    lines.append("| | 원 v12(펀딩無) | 펀딩반영 |")
    lines.append("|---|---|---|")
    lines.append(f"| trades | 164 | {len(trades_b)} |")
    lines.append(f"| MDD | -66.697% | {m_b['mdd']:.3f}% |")
    lines.append(f"| CAGR | 266.254% | {m_b['cagr']:.3f}% |")
    lines.append(f"| Sharpe | 1.778 | {m_b['sharpe']:.3f} |")
    lines.append(f"| S_raw | 0.3239 | {s_raw_b:.4f} |")
    lines.append(f"| 양수블록 | 11/15 | {n_pos_b}/15 |\n")
    lines.append("## 격자 재판정 비교\n")
    lines.append("| | 원 v12(펀딩無) | 펀딩반영 |")
    lines.append("|---|---|---|")
    lines.append(f"| hard_pass | 56/1536 | {n_hard}/1536 |")
    lines.append(f"| plateau_pass (C4) | 0 | {n_plateau} |")
    lines.append(f"| 최대 이웃통과비율(hard_pass 내) | 0.625 | {max_ratio:.3f} |")
    lines.append(f"| #7908(펀딩반영 자기 기준선)을 MDD·CAGR 동시 지배하는 combo | 0 | {n_dominates} |\n")
    lines.append(f"## 결론\n\n{verdict_line}\n")
    if n_dominates > 0:
        top = df[df.dominates_7908].sort_values("cagr", ascending=False).head(10)
        lines.append("지배 combo 상위 10개 (참고 관찰용 — 사전등록 없이 나온 결과라 채택 후보 아님):\n")
        cols = L.PARAMS5 + ["n_trades", "mdd", "cagr", "sharpe", "s_raw", "hard_pass", "nb_pass_ratio"]
        lines.append(top[cols].round(4).to_string(index=False))
    OUT_MD.write_text("\n".join(lines))
    print("\n".join(lines))
    print(f"\nwrote {OUT_MD}\nwrote {OUT_PARQUET}")


if __name__ == "__main__":
    main()
