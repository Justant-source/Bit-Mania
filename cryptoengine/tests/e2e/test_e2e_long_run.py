"""Scenario 15: 750 consecutive real 4h bars through the full app, invariants and replay parity."""

from __future__ import annotations

import csv
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from bot.interfaces import BAR_MS
from bot.report import pair_trades
from bot.risk import RiskConfig
from e2e_harness import E2EClock, Feed, E2ENotifier, build_stack, patch_kill_switch_clock

pytestmark = pytest.mark.asyncio

CSV = Path(__file__).resolve().parents[1] / "fixtures" / "btc_4h_extended.csv"
S, E = 18450, 19200        # live window: bars S .. E-1 (750 bars); replay is flat at S (asserted below)
START_WALLET = 10_000.0


def load_csv():
    rows = list(csv.DictReader(open(CSV)))
    return [{"ts": int(r["timestamp"]), "open": float(r["open"]), "high": float(r["high"]),
             "low": float(r["low"]), "close": float(r["close"]), "volume": float(r["volume"])}
            for r in rows]


def replay_trades(until_ts: int):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "fixtures"))
    from _replay_supertrend import run_backtest

    trades, _eq, _df, _ts = run_backtest(CSV, until=until_ts)
    return trades


@pytest.mark.slow
async def test_s15_long_run_750_real_bars(db, fx, monkeypatch):
    all_c = load_csv()
    assert len(all_c) > E
    window = all_c[S - 1000:E]                       # 1000 bars of history + the 750 live bars
    fx.set_balance(wallet=START_WALLET)
    clock = E2EClock(datetime.fromtimestamp((all_c[S - 1]["ts"] + BAR_MS) / 1000 + 3, tz=timezone.utc))
    patch_kill_switch_clock(monkeypatch, clock)
    feed = Feed(fx, window, spread=0.1)
    # kill switch / capital cap must not interfere: replay knows neither
    cfg = RiskConfig(daily_pct=1e6, weekly_pct=1e6, monthly_pct=1e6, daily_abs_usd=1e12,
                     weekly_abs_usd=1e12, monthly_abs_usd=1e12, max_capital_usd=1e12)
    st = await build_stack(db, fx, clock, notifier=E2ENotifier(), risk_cfg=cfg, max_capital=1e12,
                           feed=feed, housekeeping_s=4 * 3600)
    await st.app.start()

    n_bars = 0
    prev_open_ids = set()
    for k in range(S, E):
        idx_in_window = k - (S - 1000)
        await st.bar(idx_in_window)
        n_bars += 1
        # --- invariants after every bar
        pos = fx.position
        assert pos is None or pos.side == "long", f"short position at bar {k}"
        believed = st.trader.position_qty if st.trader.has_position else 0.0
        assert believed == pytest.approx(pos.size if pos else 0.0), f"belief != exchange at bar {k}"
        open_rows = await st.fetch("SELECT id FROM positions WHERE closed_at IS NULL")
        assert len(open_rows) == (1 if pos else 0), f"positions rows != exchange at bar {k}"
        assert all(o.status != "open" for o in fx.orders.values()), f"stray open order at bar {k}"
        if pos is not None:
            assert fx.position_stop_loss is not None

    # --- signal rows: exactly one per bar, strictly increasing, no gaps
    sigs = await st.signals()
    ts_list = [s["ts_ms"] for s in sigs]
    assert ts_list == [c["ts"] for c in all_c[S:E]]
    assert len(ts_list) == n_bars == len(set(ts_list))

    # --- orders: entries and exits alternate, every exit follows an entry, never short
    orders = await st.orders("status = 'filled'")
    seq = [("exit" if o["reduce_only"] else "entry") for o in orders]
    assert seq[0] == "entry"
    held = False
    for kind in seq:
        if kind == "entry":
            assert not held, "second entry while holding"
            held = True
        else:
            assert held, "exit without a preceding entry"
            held = False
    assert held == (fx.position is not None)
    assert all(o["side"] == "buy" for o in orders if not o["reduce_only"])
    assert all(o["side"] == "sell" for o in orders if o["reduce_only"])
    assert not [o for o in await st.orders() if o["status"] not in ("filled", "rejected", "cancelled")]

    # --- equity bookkeeping: realised PnL from the orders table == wallet change
    trades = pair_trades([dict(o) for o in orders])
    assert len(trades) >= 7
    assert sum(t.pnl for t in trades) == pytest.approx(fx.wallet - START_WALLET, rel=1e-9, abs=1e-6)
    snaps = await st.snapshots()
    assert float(snaps[-1]["total_equity"]) == pytest.approx(fx.wallet)
    prows = await st.positions()
    assert len(prows) == len(trades) and all(p["closed_at"] is not None for p in prows)
    assert len(await st.fetch("SELECT 1 FROM ohlcv_history")) >= n_bars
    assert st.notifier.anomalies() == [], st.notifier.anomalies()
    assert len(st.notifier.of("entry")) == len(trades) == len(st.notifier.of("exit"))
    exits = st.notifier.of("exit")
    assert {e["reason"] for e in exits} == {"ema_cross", "atr_distance"}

    # --- parity with the replay backtest (same bot.indicators; fills at close, replay flat at S)
    rep = replay_trades(all_c[E - 1]["ts"])
    ts_all = [c["ts"] for c in all_c]
    rep_in = [t for t in rep if t[0] >= all_c[S]["ts"]]
    assert not [t for t in rep if t[0] < all_c[S]["ts"] < t[2]], "replay holds a position at the window start"
    live = []
    entries = [o for o in orders if not o["reduce_only"]]
    exit_rows = [o for o in orders if o["reduce_only"]]
    for en, ex in zip(entries, exit_rows):
        e_ts = int(en["original_signal_ts"].timestamp() * 1000)
        x_ts = int(ex["original_signal_ts"].timestamp() * 1000)
        live.append((e_ts, x_ts))
    rep_pairs = [(t[0], t[2]) for t in rep_in]
    assert live == rep_pairs, (
        f"entry/exit bars differ from replay:\nlive   {live}\nreplay {rep_pairs}")
    live_reasons = [e["reason"] for e in exits]
    rep_reasons = [{"ema": "ema_cross", "atr": "atr_distance"}[t[7]] for t in rep_in]
    assert live_reasons == rep_reasons
