#!/usr/bin/env python3
"""Tripwire check — Part B of .temp/2026-08-31_slippage_tripwire_plan.md.

Two subcommands:
  extend-csv  — pull new 4h bars from the BACKTEST db (`jesse_db.ohlcv_4h`, Binance
                Spot) into cryptoengine/tests/fixtures/btc_4h_extended.csv, after
                validating that the CSV's existing tail matches it exactly. Aborts
                (no write) on any mismatch rather than silently splicing two
                inconsistent price series.

                Source is deliberately NOT the live `ohlcv_history` table: that one
                is Bybit USDT-perpetual, while this CSV — and therefore the tripwire's
                whole reference distribution — is Binance *spot*. Mixing them shifts
                closes by ~0.05% and flips marginal signals, which is exactly the
                drift this check first caught. To bring in fresh bars, run
                backtest/scripts/data/fetch_binance_vision_1m_to_pg.py first (same
                Binance Vision source), then this. See
                backtest/results/2026-08-31/csv_ohlcv_drift.md.
  check       — compute trailing-182-day log-growth (T1, rolling) and the latest
                *completed* mechanical 6-month block's log-growth (T2), against
                the frozen v12 design-window reference distribution, and append
                the result to backtest/results/tripwire/log.md.

Reference distribution is fixed at commit time — see plan §B.2 and
backtest/results/tripwire/PREREGISTRATION_TRIPWIRE.md (companion file; if it
does not exist yet, these three constants are still the frozen, plan-specified
values and must not be recomputed from live data):
  REF_MIN    = -0.578   (#7908 design-window worst single block, of 15)
  REF_P25    = -0.016   (#7908 design-window 25th percentile block)
  REF_MEDIAN =  0.324   (#7908 design-window median block, == S_raw)

Run (from repo root):
  python3 backtest/scripts/analysis/tripwire_check.py extend-csv
  python3 backtest/scripts/analysis/tripwire_check.py check [--no-log]
"""
from __future__ import annotations

import argparse
import csv as csvmod
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_V12 = _REPO / "backtest" / "scripts" / "analysis" / "v12"
if str(_V12) not in sys.path:
    sys.path.insert(0, str(_V12))
import replay_lib as L  # noqa: E402

RS = L.RS
CE_DIR = _REPO / "cryptoengine"
TRIPWIRE_DIR = _REPO / "backtest" / "results" / "tripwire"
PREREG_PATH = TRIPWIRE_DIR / "PREREGISTRATION_TRIPWIRE.md"
LOG_PATH = TRIPWIRE_DIR / "log.md"

REF_MIN = -0.578
REF_P25 = -0.016
REF_MEDIAN = 0.324

# PREREGISTRATION_TRIPWIRE.md §0.1/§0.2: every bar through 2026-08-31 was already
# observed when these thresholds were set, so blocks ending on or before this date are
# reference-only — they are computed and logged but never drive a T2 verdict. Judging
# them would be exactly the circularity (threshold picked after seeing the data) that
# the preregistration exists to prevent. First clean judgable block: [2026-10-01, 2027-04-01).
CLEAN_FROM = datetime(2026, 9, 1, tzinfo=timezone.utc)

# Backtest DB (Binance Spot candles) — NOT the live cryptoengine DB. See module docstring.
BACKTEST_PG_CONTAINER = "cryptoengine-backtest-postgres"
OHLCV_EXCHANGE = "Binance Spot"
OHLCV_SYMBOL = "BTCUSDT"

_PSQL_BASE = ["docker", "exec", BACKTEST_PG_CONTAINER,
              "psql", "-U", "jesse", "-d", "jesse_db"]


def _psql(query: str) -> str:
    """Read-only query against the backtest postgres. Never used for writes — every
    query here is a SELECT."""
    result = subprocess.run(
        _PSQL_BASE + ["-t", "-A", "-F", ",", "-c", query],
        cwd=str(CE_DIR), capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"psql query failed (rc={result.returncode}): {result.stderr.strip()}")
    return result.stdout


# ─────────────────────────── extend-csv ───────────────────────────

def cmd_extend_csv(args) -> int:
    csv_path = L.CSV
    rows = list(csvmod.DictReader(open(csv_path)))
    if not rows:
        raise RuntimeError(f"{csv_path} is empty")
    last_ts_ms = int(rows[-1]["timestamp"])
    last_ts_dt = datetime.fromtimestamp(last_ts_ms / 1000, timezone.utc)

    # 1) Overlap validation: the last `--tail` existing CSV rows must match
    #    jesse_db.ohlcv_4h (timestamp, close) exactly. A mismatch means the CSV and
    #    the table no longer share a source — abort loudly rather than splicing two
    #    inconsistent series (this is what caught the Bybit-vs-Binance mixup).
    n_tail = args.tail
    tail_rows = rows[-n_tail:]
    tail_start_dt = datetime.fromtimestamp(int(tail_rows[0]["timestamp"]) / 1000, timezone.utc)
    q_overlap = (
        "SELECT timestamp, close FROM ohlcv_4h "
        f"WHERE exchange='{OHLCV_EXCHANGE}' AND symbol='{OHLCV_SYMBOL}' "
        f"AND timestamp >= {int(tail_rows[0]['timestamp'])} AND timestamp <= {last_ts_ms} "
        "ORDER BY timestamp;"
    )
    db_tail: dict[int, float] = {}
    for line in _psql(q_overlap).strip().splitlines():
        if not line.strip():
            continue
        ts_s, close_s = line.split(",")
        db_tail[int(round(float(ts_s)))] = float(close_s)

    mismatches = []
    for r in tail_rows:
        ts = int(r["timestamp"])
        csv_close = float(r["close"])
        db_close = db_tail.get(ts)
        if db_close is None:
            mismatches.append((ts, csv_close, None, "missing in jesse_db.ohlcv_4h"))
        elif abs(db_close - csv_close) > 1e-6:
            mismatches.append((ts, csv_close, db_close, f"diff={db_close - csv_close:+.2f}"))

    if mismatches:
        print(f"ABORT: overlap validation failed — {len(mismatches)}/{len(tail_rows)} tail rows "
              f"mismatch between the CSV and jesse_db.ohlcv_4h ({OHLCV_EXCHANGE}):", file=sys.stderr)
        print(f"{'timestamp_ms':>14}  {'utc':<20}  {'csv_close':>11}  {'db_close':>11}  note", file=sys.stderr)
        for ts, c_csv, c_db, note in mismatches:
            dt = datetime.fromtimestamp(ts / 1000, timezone.utc)
            db_s = f"{c_db:.2f}" if c_db is not None else "N/A"
            print(f"{ts:>14}  {dt.isoformat():<20}  {c_csv:>11.2f}  {db_s:>11}  {note}", file=sys.stderr)
        print(
            "\nRefusing to extend: silently appending on top of a mismatched tail would "
            "splice two inconsistent price series into one CSV. Investigate the source of "
            "the discrepancy before proceeding — and note the source must stay Binance "
            "Spot (jesse_db), never the live Bybit-perp table.",
            file=sys.stderr,
        )
        return 1

    print(f"Overlap check OK: {len(tail_rows)} tail rows match exactly "
          f"({tail_start_dt.date()} .. {last_ts_dt.date()}).")

    # 2) Fetch bars strictly after the CSV's last timestamp.
    q_new = (
        "SELECT timestamp, open, high, low, close, volume FROM ohlcv_4h "
        f"WHERE exchange='{OHLCV_EXCHANGE}' AND symbol='{OHLCV_SYMBOL}' "
        f"AND timestamp > {last_ts_ms} ORDER BY timestamp;"
    )
    new_lines = [ln for ln in _psql(q_new).strip().splitlines() if ln.strip()]
    if not new_lines:
        print("No new bars — CSV already up to date. (idempotent no-op)")
        return 0

    new_rows = []
    for ln in new_lines:
        ts_s, o, h, lo, c, v = ln.split(",")
        new_rows.append([str(int(round(float(ts_s)))), o, h, lo, c, v])

    if not args.dry_run:
        with open(csv_path, "a", newline="") as f:
            w = csvmod.writer(f)
            w.writerows(new_rows)

    first_new = datetime.fromtimestamp(int(new_rows[0][0]) / 1000, timezone.utc)
    last_new = datetime.fromtimestamp(int(new_rows[-1][0]) / 1000, timezone.utc)
    verb = "would append" if args.dry_run else "appended"
    print(f"{verb.capitalize()} {len(new_rows)} new bar(s): {first_new.isoformat()} .. {last_new.isoformat()}")
    return 0


# ─────────────────────────── check ───────────────────────────

def _step6(dt: datetime) -> datetime:
    """Advance dt by exactly 6 calendar months — the identical stepping rule
    `replay_lib.block_boundaries()` uses internally, continued past its
    DESIGN_END cutoff (that function intentionally stops at DESIGN_END; this
    is new functionality, not a reimplementation of its frozen logic)."""
    y, m = dt.year, dt.month + 6
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return dt.replace(year=y, month=m)


def _extended_boundaries(latest_dt: datetime) -> list[datetime]:
    """Post-DESIGN_END block boundaries, continuing the exact same 6-month
    cadence anchored at the last *natural* (uncapped) design boundary —
    replay_lib.BLOCKS[-2], 2024-10-01 — not reset at DESIGN_END itself. This
    is what puts the in-progress block at [2026-04-01, 2026-10-01), matching
    plan §B.0. Returns [DESIGN_END, next_anchor, next_anchor+6mo, ...], with
    the final entry always past `latest_dt` (the open edge of the in-progress
    block)."""
    design_end_dt = datetime.strptime(L.DESIGN_END, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    cur = L.BLOCKS[-2]
    while cur <= design_end_dt:
        cur = _step6(cur)
    bounds = [design_end_dt, cur]
    while bounds[-1] <= latest_dt:
        bounds.append(_step6(bounds[-1]))
    return bounds


def cmd_check(args) -> int:
    ts, _o, _h, _lo, _c = RS._load_ohlcv(str(L.CSV))
    latest_dt = datetime.fromtimestamp(int(ts[-1]) / 1000, timezone.utc)

    # ── T1: trailing 182-day rolling log-growth, fresh $10k at the window start
    #    (a standalone rolling measure, distinct from the block series below —
    #    see plan §B.1/§B.3). ──
    trailing_start_dt = latest_dt - timedelta(days=182)
    trailing_start_ms = int(trailing_start_dt.timestamp() * 1000)
    _, eq_trail = L.run(L.BASELINE, start_ms=trailing_start_ms)
    trail_lg = L.block_lg(eq_trail, [trailing_start_dt, latest_dt + timedelta(seconds=1)])[0]
    t1_status = "WARNING" if trail_lg < REF_P25 else "clear"

    # ── T2: latest *completed* mechanical 6-month block, sliced from ONE
    #    continuous equity curve starting at DESIGN_END — matching how the
    #    design-window S_raw blocks were computed (block_lg docstring: one
    #    continuous curve, not independent $10k windows per block). ──
    bounds = _extended_boundaries(latest_dt)
    _, eq_holdout = L.run(L.BASELINE, start_ms=L.design_end_ms())
    lgs = L.block_lg(eq_holdout, bounds)

    completed = [(bounds[i], bounds[i + 1], lgs[i])
                 for i in range(len(lgs)) if bounds[i + 1] <= latest_dt]
    # split into contaminated (already-observed) and clean (judgable) blocks
    judgable = [b for b in completed if b[1] > CLEAN_FROM]
    inprogress = bounds[len(completed):len(completed) + 2] if len(completed) < len(lgs) else None

    if not completed:
        print("No completed holdout block yet — check is too early relative to DESIGN_END.",
              file=sys.stderr)
        return 1
    lo_b, hi_b, block_lg_val = (judgable[-1] if judgable else completed[-1])

    # T2 gate: 2 consecutive completed CLEAN blocks below REF_P25, OR any single clean
    # block below REF_MIN. Contaminated (pre-CLEAN_FROM) and in-progress blocks never
    # count toward the verdict.
    below_p25 = [lg < REF_P25 for _, _, lg in judgable]
    two_consecutive = any(below_p25[i] and below_p25[i + 1] for i in range(len(below_p25) - 1))
    any_below_min = any(lg < REF_MIN for _, _, lg in judgable)
    if not judgable:
        t2_status = "n/a (no clean block yet)"
    else:
        t2_status = "GATE" if (two_consecutive or any_below_min) else "clear"

    print(f"latest bar           : {latest_dt.isoformat()}")
    print(f"T1 trailing-182d lg   : {trail_lg:+.4f}  "
          f"(window {trailing_start_dt.date()} .. {latest_dt.date()})  "
          f"vs REF_P25={REF_P25:+.4f}  -> {t1_status}")
    t2_src = "clean" if judgable else "reference-only"
    print(f"T2 latest {t2_src:<14}: [{lo_b.date()} .. {hi_b.date()})  lg={block_lg_val:+.4f}  "
          f"vs REF_P25={REF_P25:+.4f} REF_MIN={REF_MIN:+.4f}  -> {t2_status}")
    print("completed holdout blocks so far:")
    for lo_, hi_, lg in completed:
        flag = "below MIN" if lg < REF_MIN else ("below P25" if lg < REF_P25 else "")
        tag = "judgable" if hi_ > CLEAN_FROM else "reference-only (already observed)"
        print(f"  [{lo_.date()} .. {hi_.date()})  lg={lg:+.4f}  {flag:<9}  [{tag}]")
    if not judgable:
        print(f"  -> no clean block completed yet; first judgable block ends 2027-04-01")
    if inprogress:
        print(f"in-progress block (excluded from judgment): "
              f"[{inprogress[0].date()} .. {inprogress[1].date()})")

    if not PREREG_PATH.exists():
        print(f"\nNOTE: {PREREG_PATH.relative_to(_REPO)} not found — REF_* constants are "
              "hardcoded from plan §B.2 pending that file's commit.", file=sys.stderr)

    if args.no_log:
        print("\n--no-log: skipping log.md append (dry run).")
        return 0

    TRIPWIRE_DIR.mkdir(parents=True, exist_ok=True)
    is_new = not LOG_PATH.exists()
    with open(LOG_PATH, "a") as f:
        if is_new:
            f.write("| run_date | trailing_182d_lg | T1 | latest_completed_block | block_lg | T2 |\n")
            f.write("|---|---|---|---|---|---|\n")
        run_date = datetime.now(timezone.utc).date().isoformat()
        f.write(f"| {run_date} | {trail_lg:+.4f} | {t1_status} | "
                f"[{lo_b.date()}, {hi_b.date()}) | {block_lg_val:+.4f} | {t2_status} |\n")
    print(f"\nLogged to {LOG_PATH.relative_to(_REPO)}")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("extend-csv", help="pull new bars from jesse_db.ohlcv_4h (Binance Spot) into the replay CSV")
    p1.add_argument("--tail", type=int, default=20,
                     help="number of existing tail rows to validate against the live table (default 20)")
    p1.add_argument("--dry-run", action="store_true", help="validate + report only, do not write the CSV")
    p1.set_defaults(func=cmd_extend_csv)

    p2 = sub.add_parser("check", help="compute T1/T2 tripwire status against the frozen reference distribution")
    p2.add_argument("--no-log", action="store_true", help="print results without appending to log.md")
    p2.set_defaults(func=cmd_check)

    args = ap.parse_args()
    sys.exit(args.func(args) or 0)


if __name__ == "__main__":
    main()
