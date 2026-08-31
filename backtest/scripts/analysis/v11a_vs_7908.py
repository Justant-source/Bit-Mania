#!/usr/bin/env python3
"""One-shot v11a vs #7908 verdict (plan §2). Read-only."""
from __future__ import annotations

import math
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "db"))
from _common import connect

RECENT = {"W7", "W8"}
AXIS = ("st_factor", "st_period", "fast_ema_len", "slow_ema_len", "direction_ema_len")


def lg(fb) -> float:
    return math.log(max(float(fb), 100.0) / 10000.0)


def key5(r):
    return (
        round(float(r["st_factor"]), 4),
        int(r["st_period"]),
        int(r["fast_ema_len"]),
        int(r["slow_ema_len"]),
        int(r["direction_ema_len"]),
    )


def main() -> int:
    conn = connect()
    combos = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pk, sweep_id, combo_id, st_factor, st_period, fast_ema_len,
                   slow_ema_len, direction_ema_len, atr_mult
            FROM st_combos
            WHERE (sweep_id='v10_notp' AND atr_mult=3.3) OR sweep_id='v11a'
            """
        )
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            r = dict(zip(cols, row))
            combos[r["pk"]] = r

        pks = list(combos)
        cur.execute(
            """
            SELECT combo_pk, "window", complete, cagr_adj, mdd_adj,
                   trades_count, finishing_balance
            FROM st_window_results
            WHERE combo_pk = ANY(%s) AND complete IS TRUE
            """,
            (pks,),
        )
        wins = defaultdict(list)
        for pk, w, complete, cagr, mdd, trades, fb in cur.fetchall():
            wins[pk].append(
                {
                    "w": w,
                    "cagr": float(cagr) if cagr is not None else None,
                    "mdd": float(mdd) if mdd is not None else None,
                    "trades": int(trades or 0),
                    "fb": float(fb) if fb is not None else 0.0,
                }
            )
    conn.close()

    stats = {}
    for pk, meta in combos.items():
        ws = wins.get(pk, [])
        if len(ws) < 8:
            continue
        cagrs = [x["cagr"] for x in ws]
        mdds = [x["mdd"] for x in ws]
        lgs = [lg(x["fb"]) for x in ws]
        recent = [x["mdd"] for x in ws if x["w"] in RECENT]
        n_pos = sum(1 for x in ws if x["cagr"] is not None and x["cagr"] > 0)
        lg_mean = mean(lgs)
        lg_std = stdev(lgs) if len(lgs) > 1 else 0.0
        stats[pk] = {
            **meta,
            "k5": key5(meta),
            "n_complete": len(ws),
            "n_positive": n_pos,
            "mean_cagr": mean(cagrs),
            "worst_mdd": min(mdds),
            "worst_mdd_recent": min(recent) if recent else None,
            "total_trades": sum(x["trades"] for x in ws),
            "lg_mean": lg_mean,
            "lg_std": lg_std,
            "R": lg_mean - lg_std,
            "lgs": lgs,
        }

    # baseline 7908
    base = None
    for s in stats.values():
        if s["sweep_id"] == "v10_notp" and int(s["combo_id"]) == 7908:
            base = s
            break
    if base is None:
        print("ERROR: #7908 not found")
        return 1

    # levels from actual union
    levels = {a: sorted({s[a] if a.startswith("st_f") or a == "st_factor" else int(s[a]) if a != "st_factor" else round(float(s[a]), 4) for s in stats.values()}) for a in AXIS}
    # fix st_factor
    levels["st_factor"] = sorted({round(float(s["st_factor"]), 4) for s in stats.values()})
    levels["st_period"] = sorted({int(s["st_period"]) for s in stats.values()})
    levels["fast_ema_len"] = sorted({int(s["fast_ema_len"]) for s in stats.values()})
    levels["slow_ema_len"] = sorted({int(s["slow_ema_len"]) for s in stats.values()})
    levels["direction_ema_len"] = sorted({int(s["direction_ema_len"]) for s in stats.values()})

    by_k5 = {s["k5"]: s for s in stats.values()}

    def neighbors(s):
        nbs = []
        k = list(s["k5"])
        for i, ax in enumerate(AXIS):
            lv = levels[ax]
            val = k[i]
            if val not in lv:
                continue
            idx = lv.index(val)
            for j in (idx - 1, idx + 1):
                if 0 <= j < len(lv):
                    kk = k[:]
                    kk[i] = lv[j]
                    o = by_k5.get(tuple(kk))
                    if o is not None:
                        nbs.append(o)
        return nbs

    def constraint(s):
        return (
            s["worst_mdd"] >= -71.0
            and s["worst_mdd_recent"] is not None
            and s["worst_mdd_recent"] >= -45.0
            and s["n_positive"] >= 7
            and s["total_trades"] >= 150
        )

    for s in stats.values():
        s["pass"] = constraint(s)

    for s in stats.values():
        nbs = neighbors(s)
        s["n_neighbors"] = len(nbs)
        pool = [s["R"]] + [n["R"] for n in nbs]
        s["Rt"] = mean(pool)
        feas = [s["pass"]] + [n["pass"] for n in nbs]
        s["nb_feas"] = sum(feas) / len(feas)
        s["plateau_ok"] = s["nb_feas"] >= 0.7 and s["n_neighbors"] >= 6

    # percentiles on lg per window index among union
    # pct: rank of lg_w within all combos for that window (lower better? plan: 0-100 lower=better)
    # We'll use rank of -lg so higher growth = lower percentile number
    by_win_lg = [[] for _ in range(8)]
    for s in stats.values():
        for i, v in enumerate(s["lgs"]):
            by_win_lg[i].append(v)
    for i in range(8):
        by_win_lg[i].sort(reverse=True)  # high lg first

    def pct_of(val, sorted_desc):
        # percentile 0 = best. rank / n * 100
        n = len(sorted_desc)
        # first index where val is >= (tied toward better)
        rank = 0
        for x in sorted_desc:
            if x > val + 1e-15:
                rank += 1
            else:
                break
        return 100.0 * rank / n

    for s in stats.values():
        pcts = [pct_of(s["lgs"][i], by_win_lg[i]) for i in range(8)]
        s["pct_mean"] = mean(pcts)
        s["pct_max"] = max(pcts)

    def adopt(s):
        return (
            s["pass"]
            and s["Rt"] > base["Rt"]
            and s["lg_mean"] > base["lg_mean"]
            and s["pct_max"] <= 76
            and s["plateau_ok"]
        )

    v11_pass = [s for s in stats.values() if s["sweep_id"] == "v11a" and s["pass"]]
    v10_pass = [s for s in stats.values() if s["sweep_id"] == "v10_notp" and s["pass"]]
    v11_adopt = [s for s in stats.values() if s["sweep_id"] == "v11a" and adopt(s)]
    v11_rt = [s for s in stats.values() if s["sweep_id"] == "v11a" and s["pass"] and s["Rt"] > base["Rt"]]

    print("=== baseline v10_notp #7908 ===")
    print(
        f"  sf={float(base['st_factor'])} sp={base['st_period']} fe={base['fast_ema_len']} "
        f"se={base['slow_ema_len']} de={base['direction_ema_len']} at={float(base['atr_mult'])}"
    )
    print(
        f"  mean_cagr={base['mean_cagr']:.2f} worst_mdd={base['worst_mdd']:.2f} "
        f"recent={base['worst_mdd_recent']:.2f} n_pos={base['n_positive']} trades={base['total_trades']}"
    )
    print(
        f"  lg_mean={base['lg_mean']:.4f} R={base['R']:.4f} Rtilde={base['Rt']:.4f} "
        f"pct_max={base['pct_max']:.1f} n_nb={base['n_neighbors']} nb_feas={base['nb_feas']:.2f} "
        f"plateau_ok={base['plateau_ok']} pass={base['pass']}"
    )

    print(f"\n=== universe ===")
    print(f"  combos with 8 windows: {len(stats)}  (v10@3.3 + v11a)")
    print(f"  constraint_pass v10@3.3={len(v10_pass)}  v11a={len(v11_pass)}")
    print(f"  v11a pass AND Rtilde>7908: {len(v11_rt)}")
    print(f"  v11a full §2 adopt (no hold-out): {len(v11_adopt)}")

    # axis wins: expansion values in v11a
    expansions = {
        "fast_ema_len": {4, 5, 6},
        "direction_ema_len": {200, 210, 220},
        "st_factor": {2.45, 2.55, 2.65},
        "slow_ema_len": {31, 32},
        "st_period": {5, 11, 12},
    }
    print("\n=== Stage B axis win (v11a pass AND Rtilde>7908 on expansion value) ===")
    for ax, vals in expansions.items():
        hits = [
            s
            for s in stats.values()
            if s["sweep_id"] == "v11a"
            and s["pass"]
            and s["Rt"] > base["Rt"]
            and round(float(s[ax]), 4) in {round(float(v), 4) for v in vals}
        ]
        print(f"  {ax} {sorted(vals)}: {len(hits)}  {'WIN' if hits else 'no'}")

    def show(title, rows, k=8):
        rows = sorted(rows, key=lambda x: (-x["Rt"], -x["lg_mean"]))[:k]
        print(f"\n=== {title} (top {len(rows)} by Rtilde) ===")
        print(
            f"{'sw':8} {'id':6} {'sf':5} {'sp':3} {'fe':3} {'se':3} {'de':4} "
            f"{'cagr':8} {'mdd':8} {'rec':8} {'R':7} {'Rt':7} {'pctM':6} {'nb':3} {'feas':5} {'ad':3}"
        )
        for s in rows:
            print(
                f"{s['sweep_id']:8} {int(s['combo_id']):6d} "
                f"{float(s['st_factor']):5.2f} {int(s['st_period']):3d} "
                f"{int(s['fast_ema_len']):3d} {int(s['slow_ema_len']):3d} {int(s['direction_ema_len']):4d} "
                f"{s['mean_cagr']:8.1f} {s['worst_mdd']:8.2f} {s['worst_mdd_recent']:8.2f} "
                f"{s['R']:7.3f} {s['Rt']:7.3f} {s['pct_max']:6.1f} {s['n_neighbors']:3d} "
                f"{s['nb_feas']:5.2f} {'Y' if adopt(s) else 'n'}"
            )

    show("v11a constraint_pass", v11_pass, 15)
    if v11_adopt:
        show("v11a FULL ADOPT", v11_adopt, 10)
    else:
        print("\n(no v11a combo meets full §2 adopt vs #7908 on 8-window metrics)")

    # closest near-misses: pass but fail Rt or lg_mean or pct or plateau
    almost = [s for s in v11_pass if not adopt(s)]
    print(f"\n=== v11a pass but not adopt: {len(almost)} ===")
    if almost:
        show("near-miss pass-only", almost, 10)
        reasons = defaultdict(int)
        for s in almost:
            if not (s["Rt"] > base["Rt"]):
                reasons["Rt<=7908"] += 1
            if not (s["lg_mean"] > base["lg_mean"]):
                reasons["lg_mean<=7908"] += 1
            if not (s["pct_max"] <= 76):
                reasons["pct_max>76"] += 1
            if not s["plateau_ok"]:
                reasons["plateau_fail"] += 1
        print("reason counts:", dict(reasons))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
