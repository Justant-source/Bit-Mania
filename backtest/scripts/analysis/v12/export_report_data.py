#!/usr/bin/env python3
"""Export a compact JSON payload for the v12 HTML comparison report.

Reads grid_scores_full.parquet + recomputes the #7908 baseline via replay_lib
(same code path as Phase 2) and writes v12/report_data.json.
"""
from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import replay_lib as L  # noqa: E402

RS = importlib.import_module("_replay_supertrend")
RESULTS = Path(__file__).resolve().parents[4] / "backtest" / "results" / "v12"
IN = RESULTS / "grid_scores_full.parquet"
OUT = RESULTS / "report_data.json"


def round_or_none(x, nd=4):
    return None if x is None or pd.isna(x) else round(float(x), nd)


def main():
    df = pd.read_parquet(IN)

    # block date labels
    labels = []
    for i in range(len(L.BLOCKS) - 1):
        d = L.BLOCKS[i]
        labels.append(f"{str(d.year)[2:]}-{d.month:02d}")

    # #7908 baseline, design window (same call as Phase 2 / PREREGISTRATION.md)
    trades, eq = L.run(L.BASELINE, end_ms=L.design_end_ms())
    m = RS._metrics(trades, eq)
    lgs = L.block_lg(eq)

    header = (
        ["id"] + L.PARAMS5 +
        ["n_trades", "mdd", "cagr", "sharpe", "s_raw", "s_smooth", "n_pos_blocks",
         "cmask", "nb_count", "nb_pass_ratio", "plateau"] +
        [f"lg{i}" for i in range(L.N_BLOCKS)]
    )

    rows = []
    baseline_row = (
        [-1] + [L.BASELINE[k] for k in L.PARAMS5] +
        [len(trades), round_or_none(m["mdd"]), round_or_none(m["cagr"]), round_or_none(m["sharpe"]),
         round_or_none(L.score_raw(lgs)), None, sum(1 for x in lgs if x > 0),
         None, None, None, None] +
        [round_or_none(x) for x in lgs]
    )
    rows.append(baseline_row)

    for i, r in df.iterrows():
        cmask = (1 if r.c1 else 0) | (2 if r.c2 else 0) | (4 if r.c3 else 0) | (8 if r.c4 else 0)
        row = (
            [int(i)] + [r[k] for k in L.PARAMS5] +
            [int(r.n_trades), round_or_none(r.mdd), round_or_none(r.cagr), round_or_none(r.sharpe),
             round_or_none(r.s_raw), round_or_none(r.s_smooth), int(r.n_pos_blocks),
             cmask, int(r.nb_count), round_or_none(r.nb_pass_ratio, 3), bool(r.plateau_pass)] +
            [round_or_none(r[f"lg{j}"]) for j in range(L.N_BLOCKS)]
        )
        rows.append(row)

    payload = {
        "meta": {
            "grid": L.GRID,
            "atr_fixed": L.ATR_FIXED,
            "n_blocks": L.N_BLOCKS,
            "block_labels": labels,
            "design_start": str(L.BLOCKS[0].date()),
            "design_end": L.DESIGN_END,
            "holdout_end": L.HOLDOUT_END,
            "n_grid": len(df),
            "n_hard_pass": int(df.hard_pass.sum()),
            "n_plateau": int(df.plateau_pass.sum()),
            "c1_threshold": -66.697,
            "c2_ratio": 0.64,
            "c3_min_trades": 120,
            "c4_ratio": 0.70,
        },
        "header": header,
        "rows": rows,
    }
    OUT.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"wrote {OUT} ({OUT.stat().st_size/1024:.1f} KB, {len(rows)} rows incl. baseline)")


if __name__ == "__main__":
    main()
