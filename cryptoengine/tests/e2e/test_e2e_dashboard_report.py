"""Scenario 16: dashboard (/api/summary) and daily report against the state a scenario produced."""

from __future__ import annotations

import aiohttp
import pytest

from bot.report import build_daily_report, pair_trades
from bot.web import start_web
from e2e_harness import DASH_TOKEN, atr_crash, cmd_update, entry_plan, extend, feed_for, free_port

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def plan():
    p = entry_plan()
    return p["bars"], p["idx"]


async def _scenario(make_stack, fx, clock, plan):
    """Enter at idx, exit by atr_distance on the next bar (virtual time stays within ~8 h of real
    time, which the dashboard's exit-reason matching needs: it compares bar times with DB NOW())."""
    bars, idx = plan
    full, exit_idx, _ = atr_crash(bars, bars[-1][3])
    feed = feed_for(fx, clock, full, idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    await st.bar(idx - 1)
    for j in range(idx, exit_idx + 1):
        await st.bar(j)
    return st, feed, idx, exit_idx


async def test_s16_dashboard_summary_matches_state(make_stack, fx, clock, plan):
    bars, idx = plan
    full, exit_idx, _ = atr_crash(bars, bars[-1][3])
    feed = feed_for(fx, clock, full, idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    port = free_port()
    web = await start_web(st.db, port=port, token=DASH_TOKEN)
    base = f"http://127.0.0.1:{port}"
    try:
        async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as http:
            # auth
            assert (await http.get(f"{base}/healthz")).status == 200
            assert (await http.get(f"{base}/api/summary")).status == 403
            assert (await http.get(f"{base}/api/summary?t=wrong")).status == 403
            assert (await http.get(f"{base}/")).status == 403

            await st.bar(idx - 1)
            await st.bar(idx)                                   # in position
            r = await http.get(f"{base}/api/summary?t={DASH_TOKEN}")
            assert r.status == 200
            s = await r.json()
            assert s["status"]["position"]["side"] == "long"
            assert s["status"]["position"]["size"] == pytest.approx(fx.position.size)
            assert s["status"]["position"]["entry_price"] == pytest.approx(fx.position.entry_price, abs=0.01)
            assert s["status"]["equity"] == pytest.approx(fx.wallet)
            assert s["status"]["trading"]["entries_blocked"] is False
            assert s["trades"] == [] and s["trade_totals"]["count"] == 0
            assert s["status"]["last_bar"]["signal"] == "진입"
            assert [x["bar_ts"] for x in s["signals"]][0].startswith("20")
            # cookie set after the first valid visit lets plain requests through
            assert (await http.get(f"{base}/api/summary")).status == 200

            for j in range(idx + 1, exit_idx + 1):
                await st.bar(j)                                 # ... until the ema_cross exit
            s = await (await http.get(f"{base}/api/summary?t={DASH_TOKEN}")).json()
            assert fx.position is None
            assert s["status"]["position"] is None
            assert s["status"]["equity"] == pytest.approx(fx.wallet)
            (t,) = s["trades"]
            assert s["trade_totals"]["count"] == 1
            (x,) = st.notifier.of("exit")
            # dashboard/report PnL = (exit_avg - entry_avg) * qty - entry fee - exit fee (orders table);
            # the exit notification's PnL only subtracts the exit fee (old order-manager rule)
            orders = await st.orders("status='filled'")
            (pt,) = pair_trades(orders)
            assert t["pnl"] == pytest.approx(pt.pnl)
            assert t["fees"] == pytest.approx(pt.fees)
            entry_fee = [o for o in orders if not o["reduce_only"]][0]["fee"]
            assert x["pnl"] - t["pnl"] == pytest.approx(entry_fee)
            assert t["exit_reason"] == "atr_distance"
            assert t["entry_price"] == pytest.approx(x["entry_price"])
            assert t["qty"] == pytest.approx(x["qty"])
            assert s["trade_totals"]["pnl"] == pytest.approx(pt.pnl)
            assert s["status"]["last_bar"]["signal"] == "청산"
            sig_rows = await st.signals()
            assert len(s["signals"]) == min(60, len(sig_rows))
            assert s["signals"][0]["exit_reason"] == "atr_distance"
            assert len(s["candles"]) >= 3 and s["candles"][-1]["c"] > 0
            curve = s["equity_curve"]
            assert curve and curve[-1]["v"] == pytest.approx(fx.wallet)
            assert s["status"]["heartbeat_at"] is not None

            # halting shows up
            await st.telegram_emergency_close()
            s = await (await http.get(f"{base}/api/summary?t={DASH_TOKEN}")).json()
            assert s["status"]["trading"]["entries_blocked"] is True
            assert "수동" in s["status"]["trading"]["reason"]
            assert s["status"]["kill_switch"]["level"] == 4
    finally:
        await web.stop()


async def test_s16_daily_report_numbers(make_stack, fx, clock, plan):
    st, feed, idx, exit_idx = await _scenario(make_stack, fx, clock, plan)
    orders = [o for o in await st.orders("status='filled'")]
    trades = pair_trades(orders)
    assert len(trades) == 1
    expected = trades[0].pnl
    text = await build_daily_report(st.db, st.app)
    win, loss = (1, 0) if expected > 0 else (0, 1)
    line24 = [l for l in text.splitlines() if "24h" in l and "건" in l]
    line30 = [l for l in text.splitlines() if "30d" in l and "건" in l]
    assert line24 and line30
    for line in (line24[0], line30[0]):
        assert f"1건 {win}승 {loss}패" in line
        assert f"PnL {expected:+,.2f}" in line
        assert f"수수료 {trades[0].fees:,.2f}" in line
    assert f"${fx.wallet:,.2f}" in text
    assert "포지션 없음 (FLAT)" in text
    assert "청산신호 atr_distance" in text
    assert "정상" in text
    # the exit notification quotes a different PnL (exit fee only): record the gap
    (x,) = st.notifier.of("exit")
    assert x["pnl"] != pytest.approx(expected)
    # scheduled report path: through BotApp.daily_report with the dashboard button
    st.notifier.calls.clear()
    await st.app.daily_report()
    (sent,) = st.notifier.of("send")
    assert sent["dedup_key"].startswith("daily_report:")
    assert sent["reply_markup"] is not None
    assert "1건" in sent["html"]
    # /report command
    upd = cmd_update()
    await st.commands.cmd_report(upd)
    t, kw = upd.effective_message.sent[-1]
    assert f"PnL {expected:+,.2f}" in t and kw["reply_markup"] is not None



async def test_s16_report_while_in_position(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, extend(bars, 0.01, 2), idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    text = await build_daily_report(st.db, st.app)
    assert "LONG" in text and f"{fx.position.size:.4f}" in text
    assert "0건 0승 0패" in text
    assert "진입 가능" in text or "마지막 신호" in text


async def test_s16_index_page_is_self_contained_and_token_protected(make_stack, fx, clock, plan):
    import re

    bars, idx = plan
    st = await make_stack(feed=feed_for(fx, clock, bars, idx - 1))
    await st.app.start()
    port = free_port()
    web = await start_web(st.db, port=port, token=DASH_TOKEN)
    try:
        async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as http:
            assert (await http.get(f"http://127.0.0.1:{port}/")).status == 403
            r = await http.get(f"http://127.0.0.1:{port}/?t={DASH_TOKEN}")
            assert r.status == 200 and "text/html" in r.headers["Content-Type"]
            html = await r.text()
            assert "api/summary" in html
            assert not re.search(r"(src|href)\s*=\s*[\"']https?://", html), "external resource in the page"
            assert (await http.get(f"http://127.0.0.1:{port}/")).status == 200        # cookie now set
    finally:
        await web.stop()
