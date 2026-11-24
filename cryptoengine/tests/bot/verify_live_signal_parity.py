#!/usr/bin/env python3
"""Compare live-recorded signals with what ``bot.strategy.evaluate`` computes on the same bars.

READ-ONLY: only SELECT statements are issued, and the session is opened with
``default_transaction_read_only=on`` so nothing can be written even by mistake.

    python3 tests/bot/verify_live_signal_parity.py postgresql://user:pass@host:5432/cryptoengine \
        [--limit N] [--min-bars 1000] [--tol 1e-6]

For every row of ``supertrend_signals`` the script takes the bybit BTCUSDT 4h bars of
``ohlcv_history`` ending at that row's ``bar_ts`` (the last ``CANDLE_LOOKBACK``=1000 bars,
exactly the window the live strategy used), runs ``evaluate`` and reports per-bar
mismatches in: st_dir, exit_signal, exit_reason, entry_ok and the indicator values
(st_line, fast_ema, slow_ema, dir_ema, price, atr_14; relative tolerance 1e-6).

History requirement: a bar can only be compared when at least ``--min-bars`` (default 1000)
bars precede-and-include it in ``ohlcv_history``; with fewer, the 240-EMA seed has not decayed
and live and replay legitimately differ. Such rows are SKIPPED and counted as ``skipped_short``
(never reported as mismatches). Lower ``--min-bars`` (minimum 260 = indicator warm-up) to
compare them anyway, knowing that dir_ema / entry_ok may differ.

Inputs that are not stored in ``supertrend_signals`` are reconstructed:
  * had_position       : taken from the row itself (position state before the decision);
  * entry price (ATR)  : the ``positions`` row open at the row's ``computed_at``; if none is
                         found, an ``atr_distance`` exit cannot be verified and is reported as
                         ``unverifiable`` rather than as a mismatch;
  * cooldowns          : derived from the previous bar's row (a previous exit signal is assumed
                         to have filled). If that exit was in fact rejected, entry_ok on the next
                         bar can differ; such cases are listed as mismatches to be eyeballed.
Exit code 0 when there are no mismatches, 1 otherwise, 2 on usage/connection errors.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import asyncpg  # noqa: E402

from bot.interfaces import BAR_MS  # noqa: E402
from bot.strategy import CANDLE_LOOKBACK, MIN_BARS, evaluate  # noqa: E402

INDICATORS = ("st_line", "fast_ema", "slow_ema", "dir_ema", "price", "atr_14")


@dataclass
class Report:
    compared: int = 0
    skipped_short: int = 0
    skipped_no_bars: int = 0
    unverifiable: int = 0
    mismatches: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.mismatches


def _close(a: float | None, b: float | None, tol: float) -> bool:
    if a is None or b is None:
        return a is b
    return math.isclose(float(a), float(b), rel_tol=tol, abs_tol=1e-9)


def compare_row(row: dict[str, Any], ours, tol: float, entry_known: bool) -> tuple[list[str], bool]:
    """(mismatch descriptions, unverifiable flag) for one stored row vs our evaluation."""
    diffs: list[str] = []
    unverifiable = False
    if int(row["st_dir"]) != ours.st_dir:
        diffs.append(f"st_dir db={row['st_dir']} ours={ours.st_dir}")
    for k in INDICATORS:
        if not _close(row[k], getattr(ours, k), tol):
            diffs.append(f"{k} db={row[k]!r} ours={getattr(ours, k)!r}")
    if bool(row["entry_ok"]) != ours.entry_ok:
        diffs.append(f"entry_ok db={row['entry_ok']} ours={ours.entry_ok}")
    if row["had_position"] and not entry_known and row["exit_reason"] == "atr_distance":
        unverifiable = True  # needs the entry price, which was not recoverable
    else:
        if bool(row["exit_signal"]) != ours.exit_signal:
            diffs.append(f"exit_signal db={row['exit_signal']} ours={ours.exit_signal}")
        if (row["exit_reason"] or None) != ours.exit_reason:
            diffs.append(f"exit_reason db={row['exit_reason']} ours={ours.exit_reason}")
    return diffs, unverifiable


async def verify(conn: asyncpg.Connection, *, min_bars: int = CANDLE_LOOKBACK, tol: float = 1e-6,
                 limit: int | None = None) -> Report:
    bars = await conn.fetch(
        "SELECT (EXTRACT(EPOCH FROM timestamp) * 1000)::BIGINT AS ts, open, high, low, close, volume "
        "FROM ohlcv_history WHERE exchange='bybit' AND symbol='BTCUSDT' AND timeframe='4h' ORDER BY timestamp")
    candles = [{"ts": int(r["ts"]), "open": float(r["open"]), "high": float(r["high"]),
                "low": float(r["low"]), "close": float(r["close"]), "volume": float(r["volume"])}
               for r in bars]
    index = {c["ts"]: i for i, c in enumerate(candles)}
    sigs = await conn.fetch(
        "SELECT *, (EXTRACT(EPOCH FROM bar_ts) * 1000)::BIGINT AS bar_ms FROM supertrend_signals ORDER BY bar_ts")
    positions = await conn.fetch(
        "SELECT entry_price, opened_at, closed_at FROM positions WHERE symbol LIKE 'BTC%' ORDER BY opened_at")
    rep = Report()
    if limit:
        sigs = sigs[-limit:]
    all_sigs = {int(s["bar_ms"]): s for s in await conn.fetch(
        "SELECT (EXTRACT(EPOCH FROM bar_ts) * 1000)::BIGINT AS bar_ms, had_position, exit_signal, exit_reason "
        "FROM supertrend_signals")}
    for s in sigs:
        ts = int(s["bar_ms"])
        i = index.get(ts)
        if i is None:
            rep.skipped_no_bars += 1
            continue
        window = candles[max(0, i + 1 - CANDLE_LOOKBACK): i + 1]
        if len(window) < max(min_bars, MIN_BARS):
            rep.skipped_short += 1
            continue
        entry_price, known = 0.0, False
        if s["had_position"]:
            for p in positions:
                if p["opened_at"] <= s["computed_at"] and (p["closed_at"] is None or p["closed_at"] >= s["computed_at"]):
                    entry_price, known = float(p["entry_price"]), True
        prev = all_sigs.get(ts - BAR_MS)
        last_liq, atr_cd = 0, 0
        if prev and prev["had_position"] and prev["exit_signal"]:
            last_liq = ts - BAR_MS
            if prev["exit_reason"] == "atr_distance":
                atr_cd = ts  # bar_ts + 4h of the exit bar
        ours = evaluate(window, has_position=bool(s["had_position"]), entry_price=entry_price,
                        last_liquidation_ts=last_liq, atr_cooldown_until=atr_cd)
        diffs, unverifiable = compare_row(dict(s), ours, tol, known)
        rep.compared += 1
        rep.unverifiable += int(unverifiable)
        if diffs:
            rep.mismatches.append(f"bar_ts={s['bar_ts'].isoformat()} (window={len(window)}): " + "; ".join(diffs))
    return rep


async def _amain(args: argparse.Namespace) -> int:
    try:
        conn = await asyncpg.connect(args.dsn, server_settings={"default_transaction_read_only": "on"})
    except Exception as exc:
        print(f"cannot connect: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    try:
        rep = await verify(conn, min_bars=args.min_bars, tol=args.tol, limit=args.limit)
    finally:
        await conn.close()
    for line in rep.mismatches:
        print("MISMATCH", line)
    print(f"compared={rep.compared} mismatched={len(rep.mismatches)} skipped_short_history={rep.skipped_short} "
          f"skipped_no_bars_in_ohlcv_history={rep.skipped_no_bars} unverifiable_atr_exit={rep.unverifiable}")
    return 0 if rep.ok else 1


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("dsn", help="postgresql://user:password@host:port/dbname")
    ap.add_argument("--limit", type=int, default=None, help="only the most recent N signal rows")
    ap.add_argument("--min-bars", type=int, default=CANDLE_LOOKBACK,
                    help="minimum bars of history before a row is compared (default 1000)")
    ap.add_argument("--tol", type=float, default=1e-6, help="relative tolerance for indicator values")
    sys.exit(asyncio.run(_amain(ap.parse_args())))


if __name__ == "__main__":
    main()
