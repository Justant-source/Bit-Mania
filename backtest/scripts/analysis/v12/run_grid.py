#!/usr/bin/env python3
"""v12 Phase 2 — score the full 1,536-combo grid on the design window (bar[420] .. 2025-01-01).

Never touches the holdout (>= 2025-01-01). Writes:
  backtest/results/v12/grid_scores.parquet   — one row per combo: params, trades, mdd, cagr,
                                                 sharpe, block_lg (15 floats), s_raw
Pure computation, no plateau/gate logic here (that's apply_plateau.py, run after this).
"""
from __future__ import annotations

import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import replay_lib as L  # noqa: E402
import importlib  # noqa: E402

RS = importlib.import_module("_replay_supertrend")

OUT = Path(__file__).resolve().parents[4] / "backtest" / "results" / "v12" / "grid_scores.parquet"


def score_one(params: dict) -> dict:
    trades, eq = L.run(params, end_ms=L.design_end_ms())
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
    combos = L.all_combos()
    print(f"scoring {len(combos)} combos, N_BLOCKS={L.N_BLOCKS} ...")
    t0 = time.time()
    rows = []
    with ProcessPoolExecutor(max_workers=8) as ex:
        for i, row in enumerate(ex.map(score_one, combos, chunksize=8)):
            rows.append(row)
            if (i + 1) % 200 == 0:
                print(f"  {i+1}/{len(combos)}  {time.time()-t0:.1f}s")
    df = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    print(f"done in {time.time()-t0:.1f}s -> {OUT}  ({len(df)} rows)")


if __name__ == "__main__":
    main()
