#!/usr/bin/env python3
"""S7 hold-out: replay shortlist + #7908 on canonical / full / W9 slices.

Plan §2.5: full-period CAGR and MDD must both be ≥ #7908 on the same path;
W9 must not sit in the bottom half of the candidate group.

Indicators: live Supertrend (Bybit-native 4h close fills). CSV default is
cryptoengine/tests/fixtures/btc_4h_extended.csv (through 2026-08-28 20:00).
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
REPLAY_PATH = REPO / "cryptoengine" / "tests" / "fixtures" / "_replay_supertrend.py"
DEFAULT_CSV = REPO / "cryptoengine" / "tests" / "fixtures" / "btc_4h_extended.csv"
OUT_DIR = REPO / "backtest" / "results" / "supertrend_x3_long_only" / "docs" / "sweeps"

CANONICAL = {"trades": 198, "cagr": 219.06, "mdd": -66.70, "sharpe": 1.667}
SLICES = (
    ("canonical", None, "2026-05-01"),       # through 2026-04-30 20:00
    ("full", None, "2026-08-29"),             # through 2026-08-28 20:00
    ("W9", "2026-05-01", "2026-08-29"),       # post-W8 hold-out, $10k fresh
)

# Stage A §2 adopt (8-window, no hold-out) + live baseline.
COMBOS = [
    {"label": "v10#7908", "sweep": "v10_notp", "combo_id": 7908,
     "st_factor": 2.6, "st_period": 9, "fast_ema": 7, "slow_ema": 29, "dir_ema": 240, "atr_mult": 3.3},
    {"label": "v11a#799", "sweep": "v11a", "combo_id": 799,
     "st_factor": 2.4, "st_period": 9, "fast_ema": 7, "slow_ema": 29, "dir_ema": 210, "atr_mult": 3.3},
    {"label": "v11a#895", "sweep": "v11a", "combo_id": 895,
     "st_factor": 2.5, "st_period": 8, "fast_ema": 7, "slow_ema": 29, "dir_ema": 210, "atr_mult": 3.3},
    {"label": "v11a#847", "sweep": "v11a", "combo_id": 847,
     "st_factor": 2.5, "st_period": 6, "fast_ema": 7, "slow_ema": 29, "dir_ema": 210, "atr_mult": 3.3},
    {"label": "v11a#871", "sweep": "v11a", "combo_id": 871,
     "st_factor": 2.5, "st_period": 7, "fast_ema": 7, "slow_ema": 29, "dir_ema": 210, "atr_mult": 3.3},
    {"label": "v11a#1034", "sweep": "v11a", "combo_id": 1034,
     "st_factor": 2.6, "st_period": 9, "fast_ema": 7, "slow_ema": 27, "dir_ema": 220, "atr_mult": 3.3},
]


def _load_replay():
    spec = importlib.util.spec_from_file_location("replay_st", REPLAY_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def lg(fb: float) -> float:
    return math.log(max(float(fb), 100.0) / 10000.0)


def _run(mod, csv_path: Path, combo: dict, start: str | None, end: str | None):
    trades, equity, df, ts = mod.run_backtest(
        csv_path=csv_path,
        st_factor=combo["st_factor"],
        st_period=combo["st_period"],
        fast=combo["fast_ema"],
        slow=combo["slow_ema"],
        dir_ema=combo["dir_ema"],
        atr_mult=combo["atr_mult"],
        start_ms=mod.date_ms(start) if start else None,
        end_ms=mod.date_ms(end) if end else None,
    )
    m = mod._metrics(trades, equity)
    eq_first = equity[0][0] if equity else None
    eq_last = equity[-1][0] if equity else None
    return {
        "trades": len(trades),
        "win_rate": m["win_rate"],
        "cagr": float(m["cagr"]),
        "mdd": float(m["mdd"]),
        "pf": float(m["pf"]),
        "sharpe": float(m["sharpe"]),
        "final": float(m["final"]),
        "lg": lg(m["final"]),
        "bars_csv": int(len(df)),
        "eq_bars": len(equity),
        "eq_first": datetime.fromtimestamp(eq_first / 1000, timezone.utc).strftime("%Y-%m-%d") if eq_first else None,
        "eq_last": datetime.fromtimestamp(eq_last / 1000, timezone.utc).strftime("%Y-%m-%d") if eq_last else None,
    }


def yn(v) -> str:
    return "Y" if bool(v) else "n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--out-dir", default=str(OUT_DIR))
    args = ap.parse_args()

    csv_path = Path(args.csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    mod = _load_replay()

    # Gate: #7908 on canonical fixture path (end exclusive 2026-05-01).
    print("=== canonical gate #7908 (end 2026-05-01 exclusive) ===", flush=True)
    gate = _run(mod, csv_path, COMBOS[0], None, "2026-05-01")
    print(
        f"  trades={gate['trades']} CAGR={gate['cagr']:.2f} MDD={gate['mdd']:.2f} "
        f"Sharpe={gate['sharpe']:.3f} {gate['eq_first']}~{gate['eq_last']}",
        flush=True,
    )
    ok_gate = (
        gate["trades"] == CANONICAL["trades"]
        and abs(gate["cagr"] - CANONICAL["cagr"]) < 0.02
        and abs(gate["mdd"] - CANONICAL["mdd"]) < 0.02
    )
    if not ok_gate:
        print("FAIL: canonical #7908 does not match 198 / 219.06 / -66.70")
        print(json.dumps(gate, indent=2))
        return 1
    print("  PASS vs 198 / 219.06 / -66.70", flush=True)

    rows = []
    for combo in COMBOS:
        rec = {**combo, "slices": {}}
        for name, start, end in SLICES:
            print(f"--- {combo['label']} {name} ---", flush=True)
            rec["slices"][name] = _run(mod, csv_path, combo, start, end)
            s = rec["slices"][name]
            print(
                f"  trades={s['trades']} CAGR={s['cagr']:.2f} MDD={s['mdd']:.2f} "
                f"final=${s['final']:.0f} {s['eq_first']}~{s['eq_last']}",
                flush=True,
            )
        rows.append(rec)

    base = next(r for r in rows if r["combo_id"] == 7908)
    bf, bw = base["slices"]["full"], base["slices"]["W9"]

    w9_sorted = sorted(rows, key=lambda r: (-r["slices"]["W9"]["lg"], r["combo_id"]))
    n = len(w9_sorted)
    w9_lgs_asc = sorted(r["slices"]["W9"]["lg"] for r in rows)
    # Even n: cutoff is the n/2-th from the bottom (0-index n/2-1). Ties at the
    # cutoff stay in the bottom half ("하위 50% 아님" = strictly above cutoff).
    w9_cut = w9_lgs_asc[n // 2 - 1] if n >= 2 else w9_lgs_asc[0]
    w9_rank = {}
    last_lg, last_rank = None, 0
    for i, r in enumerate(w9_sorted):
        lgv = r["slices"]["W9"]["lg"]
        if last_lg is None or abs(lgv - last_lg) > 1e-12:
            last_rank, last_lg = i + 1, lgv
        w9_rank[r["label"]] = last_rank

    for r in rows:
        f, w = r["slices"]["full"], r["slices"]["W9"]
        r["cagr_ge"] = bool(f["cagr"] + 1e-12 >= bf["cagr"])
        r["mdd_ge"] = bool(f["mdd"] + 1e-12 >= bf["mdd"])
        r["w9_rank"] = w9_rank[r["label"]]
        r["w9_ok"] = bool(w["lg"] > w9_cut + 1e-12)
        r["holdout_pass"] = r["cagr_ge"] and r["mdd_ge"] and r["w9_ok"]
        r["d_cagr"] = f["cagr"] - bf["cagr"]
        r["d_mdd"] = f["mdd"] - bf["mdd"]
        r["d_w9_cagr"] = w["cagr"] - bw["cagr"]
        r["d_w9_mdd"] = w["mdd"] - bw["mdd"]

    shortlist = {
        "csv": str(csv_path),
        "canonical_gate": gate,
        "combos": [
            {
                "label": r["label"],
                "sweep": r["sweep"],
                "combo_id": r["combo_id"],
                "params": {k: r[k] for k in ("st_factor", "st_period", "fast_ema", "slow_ema", "dir_ema", "atr_mult")},
                "slices": r["slices"],
                "cagr_ge": r["cagr_ge"],
                "mdd_ge": r["mdd_ge"],
                "w9_rank": r["w9_rank"],
                "w9_ok": r["w9_ok"],
                "holdout_pass": r["holdout_pass"],
            }
            for r in rows
        ],
    }
    (out_dir / "v11_shortlist.json").write_text(json.dumps(shortlist, indent=2, default=float) + "\n")

    lines = [
        "# v11 hold-out vs #7908",
        "",
        f"csv: `{csv_path}`",
        "path: live Supertrend replay (Bybit-native 4h close, fee 0.055%/side, 95%×3x).",
        "canonical = `--end 2026-05-01` exclusive. full = `--end 2026-08-29`. "
        "W9 = `--start 2026-05-01 --end 2026-08-29`, $10k independent.",
        "",
        f"canonical gate #7908: trades={gate['trades']} CAGR={gate['cagr']:.2f}% "
        f"MDD={gate['mdd']:.2f}% Sharpe={gate['sharpe']:.3f} — PASS vs 198 / 219.06 / −66.70.",
        "",
        "## Full period (2017-08 → 2026-08-28)",
        "",
        "| combo | sf/sp/fe/se/de | trades | CAGR% | vs #7908 | MDD% | vs #7908 | Sharpe | final | CAGR≥ | MDD≥ |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|:---:|:---:|",
    ]
    for r in rows:
        f = r["slices"]["full"]
        lines.append(
            f"| {r['label']} | {r['st_factor']}/{r['st_period']}/{r['fast_ema']}/{r['slow_ema']}/{r['dir_ema']} "
            f"| {f['trades']} | {f['cagr']:.2f} | {r['d_cagr']:+.2f} | {f['mdd']:.2f} | {r['d_mdd']:+.2f} "
            f"| {f['sharpe']:.3f} | {f['final']:.0f} | {yn(r['cagr_ge'])} | {yn(r['mdd_ge'])} |"
        )

    lines += [
        "",
        "## W9 (2026-05-01 → 2026-08-28)",
        "",
        f"rank by `lg = ln(max(fb,100)/10000)` (ties share rank). "
        f"W9 ok = lg > lower-half cutoff {w9_cut:.4f}.",
        "",
        "| rank | combo | trades | CAGR% | vs #7908 | MDD% | vs #7908 | final | lg | W9 ok |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|:---:|",
    ]
    for r in w9_sorted:
        w = r["slices"]["W9"]
        lines.append(
            f"| {r['w9_rank']} | {r['label']} | {w['trades']} | {w['cagr']:.2f} | {r['d_w9_cagr']:+.2f} "
            f"| {w['mdd']:.2f} | {r['d_w9_mdd']:+.2f} | {w['final']:.0f} | {w['lg']:.4f} | {yn(r['w9_ok'])} |"
        )

    lines += [
        "",
        "## Adopt gate (plan §2.5 hold-out only)",
        "",
        "| combo | full CAGR≥ | full MDD≥ | W9 not bottom 50% | hold-out pass |",
        "|---|:---:|:---:|:---:|:---:|",
    ]
    for r in rows:
        lines.append(
            f"| {r['label']} | {yn(r['cagr_ge'])} | {yn(r['mdd_ge'])} "
            f"| {yn(r['w9_ok'])} | {yn(r['holdout_pass'])} |"
        )

    passed = [r["label"] for r in rows if r["combo_id"] != 7908 and r["holdout_pass"]]
    lines += [
        "",
        f"v11a hold-out pass: {', '.join(passed) if passed else '(none)'}.",
        "Live yaml is unchanged. Stage B is still a separate decision.",
        "",
    ]
    md = "\n".join(lines) + "\n"
    (out_dir / "v11_holdout.md").write_text(md)
    print("\n" + md)
    print(f"wrote {out_dir / 'v11_holdout.md'} and {out_dir / 'v11_shortlist.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
