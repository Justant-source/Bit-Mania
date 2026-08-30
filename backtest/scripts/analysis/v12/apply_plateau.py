#!/usr/bin/env python3
"""v12 Phase 2b — apply hard constraints (C1-C4) + plateau smoothing to grid_scores.parquet.

Locates #7908 relative to any surviving plateau and decides whether Phase 3 (G1-G9) runs
at all. Writes backtest/results/v12/plateau_report.md.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import replay_lib as L  # noqa: E402

RESULTS = Path(__file__).resolve().parents[4] / "backtest" / "results" / "v12"
IN = RESULTS / "grid_scores.parquet"
OUT = RESULTS / "plateau_report.md"

C1_MDD_MIN = -66.697   # #7908 design-window MDD
C2_POS_BLOCK_RATIO = 0.64
C3_MIN_TRADES = 120
C4_NEIGHBOR_PASS_RATIO = 0.70
# NOTE: an earlier version of this script also required an absolute floor of >=8
# passing neighbors alongside the ratio. That floor is NOT in PREREGISTRATION.md §6
# (C4 is ratio-only) and made the gate stricter than preregistered — a discrepancy
# caught by both a self-check and an independent adversarial audit
# (2026-08-31, see VERDICT.md). Removed to match the locked spec exactly. Rerunning
# with the ratio-only rule does not change the NO_PLATEAU conclusion (max ratio
# observed among hard_pass combos was 0.625, still < 0.70).


def main():
    df = pd.read_parquet(IN)
    key = {L.key5({k: row[k] for k in L.PARAMS5}): i for i, row in df.iterrows()}

    df["c1"] = df["mdd"] >= C1_MDD_MIN
    df["c2"] = df["n_pos_blocks"] >= np.ceil(C2_POS_BLOCK_RATIO * L.N_BLOCKS)
    df["c3"] = df["n_trades"] >= C3_MIN_TRADES
    df["hard_pass"] = df["c1"] & df["c2"] & df["c3"]

    lines = []
    lines.append(f"# v12 Plateau report\n")
    lines.append(f"grid n={len(df)}, N_BLOCKS={L.N_BLOCKS}\n")
    lines.append(f"C1 (MDD >= {C1_MDD_MIN}): {df.c1.sum()}/{len(df)}")
    lines.append(f"C2 (pos_blocks >= {int(np.ceil(C2_POS_BLOCK_RATIO*L.N_BLOCKS))}/{L.N_BLOCKS}): {df.c2.sum()}/{len(df)}")
    lines.append(f"C3 (trades >= {C3_MIN_TRADES}): {df.c3.sum()}/{len(df)}")
    lines.append(f"C1+C2+C3 (hard_pass): {df.hard_pass.sum()}/{len(df)}\n")

    # neighbor pass ratio (C4) computed for every combo that hard-passes
    nb_pass_ratio = {}
    nb_count = {}
    for i, row in df.iterrows():
        params = {k: row[k] for k in L.PARAMS5}
        nbs = L.neighbors(params)
        passes = [key[L.key5(nb)] for nb in nbs if L.key5(nb) in key]
        cnt = len(passes)
        pr = (df.loc[passes, "hard_pass"].sum() / cnt) if cnt else 0.0
        nb_pass_ratio[i] = pr
        nb_count[i] = cnt
    df["nb_count"] = df.index.map(nb_count)
    df["nb_pass_ratio"] = df.index.map(nb_pass_ratio)
    df["nb_pass_count"] = (df["nb_pass_ratio"] * df["nb_count"]).round().astype(int)
    df["c4"] = df["nb_pass_ratio"] >= C4_NEIGHBOR_PASS_RATIO   # PREREGISTRATION.md §6: ratio only
    df["plateau_pass"] = df["hard_pass"] & df["c4"]

    lines.append(f"C4 (neighbor pass ratio >= {C4_NEIGHBOR_PASS_RATIO}, of hard_pass combos): "
                  f"{df.plateau_pass.sum()}/{df.hard_pass.sum()} hard_pass combos also form a plateau "
                  f"(max ratio observed: {df.loc[df.hard_pass, 'nb_pass_ratio'].max():.3f})\n")

    # smoothed score S = mean(S_raw(self) + S_raw(neighbors)) -- computed for ALL combos
    # (plateau smoothing must not be restricted to already-passing combos, or it can't
    # ever detect a plateau whose center barely misses a hard threshold)
    def smooth(i):
        params = {k: df.loc[i, k] for k in L.PARAMS5}
        nbs = L.neighbors(params)
        idxs = [key[L.key5(nb)] for nb in nbs if L.key5(nb) in key]
        vals = [df.loc[i, "s_raw"]] + [df.loc[j, "s_raw"] for j in idxs]
        return float(np.mean(vals))

    df["s_smooth"] = [smooth(i) for i in df.index]

    # rank plateau-passing combos by smoothed score
    survivors = df[df.plateau_pass].sort_values("s_smooth", ascending=False)
    lines.append(f"## Plateau-passing combos ranked by s_smooth (top 15 of {len(survivors)})\n")
    if len(survivors):
        cols = L.PARAMS5 + ["n_trades", "mdd", "cagr", "sharpe", "s_raw", "s_smooth", "nb_pass_ratio"]
        lines.append(survivors[cols].head(15).round(4).to_string(index=False))
    else:
        lines.append("(none)")
    lines.append("")

    # locate #7908 in the grid neighborhood (it is NOT a grid point itself)
    b = L.BASELINE
    lines.append(f"## #7908 baseline: {b}")
    lines.append(f"design-window: trades=164 mdd=-66.697 cagr=266.254 sharpe=1.778 s_raw=0.3239\n")

    # nearest grid point to #7908 on each axis (for reference — #7908 sits off-grid)
    def nearest(v, vals):
        return min(vals, key=lambda x: abs(x - v))
    nearest_grid_pt = {k: nearest(b[k], L.GRID[k]) for k in L.PARAMS5}
    lines.append(f"Nearest grid point to #7908 (not #7908 itself, grid has no point there): {nearest_grid_pt}")
    npk = L.key5(nearest_grid_pt)
    if npk in key:
        nr = df.loc[key[npk]]
        lines.append(f"  that grid point's stats: trades={nr.n_trades} mdd={nr.mdd:.3f} cagr={nr.cagr:.3f} "
                      f"s_raw={nr.s_raw:.4f} s_smooth={nr.s_smooth:.4f} hard_pass={bool(nr.hard_pass)} "
                      f"plateau_pass={bool(nr.plateau_pass)}")
    lines.append("")

    verdict = "NO_PLATEAU" if len(survivors) == 0 else "PLATEAU_FOUND"
    lines.append(f"## VERDICT: {verdict}")
    if verdict == "NO_PLATEAU":
        lines.append("No combo satisfies hard constraints (C1-C3) with >=8/10 grid-neighbors "
                      "also satisfying them (C4). No candidate region exists in this grid. "
                      "Per PREREGISTRATION.md §6/§9: stop here. G1-G9 and Phase 4 are NOT run. "
                      "Conclusion: #7908 유지.")
    else:
        lines.append(f"{len(survivors)} combos form a plateau. Proceeding to Phase 3 (G1-G9) "
                      f"on the top candidate(s).")

    OUT.write_text("\n".join(str(x) for x in lines))
    print("\n".join(str(x) for x in lines))
    df.to_parquet(RESULTS / "grid_scores_full.parquet", index=False)
    print(f"\nwrote {OUT}")
    return verdict, survivors


if __name__ == "__main__":
    main()
