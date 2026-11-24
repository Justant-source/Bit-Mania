"""Daily report (also /report). Built only from tables that are really written."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

from bot.notify import esc

log = structlog.get_logger("report")

KST = timezone(timedelta(hours=9))
# Smallest tradable lot is 0.001 BTC: anything below it left after an exit is dust.
DUST_QTY = 0.001 - 1e-9


# ── PnL pairing ─────────────────────────────────────────────────────────────


# close_reason values of `positions` rows closed without the bot's own exit order
# (`exit` rows are the bot's normal exit and keep pairing from `orders` only).
BOUNDARY_REASONS = ("exchange_side", "exchange_flat", "kill_switch", "emergency_close")
# A reduce-only sell written for a bot-side close lands just before closed_at.
_SELL_MATCH_SLACK = timedelta(seconds=60)

_UTC_MIN = datetime.min.replace(tzinfo=timezone.utc)


def _aware(v: Any) -> datetime | None:
    if not isinstance(v, datetime):
        return None
    return v if v.tzinfo else v.replace(tzinfo=timezone.utc)


def order_fill_time(r: dict[str, Any], key: str = "updated_at") -> datetime:
    """Time used to order an order row among others (``key`` falling back to the other column)."""
    other = "created_at" if key == "updated_at" else "updated_at"
    return _aware(r.get(key)) or _aware(r.get(other)) or _UTC_MIN


def close_boundaries(positions: list[dict[str, Any]] | None, orders: list[dict[str, Any]],
                     time_key: str = "updated_at") -> list[tuple[datetime, str]]:
    """``[(closed_at, close_reason)]`` for positions closed without a reduce-only sell of the bot.

    A positions row with ``closed_at`` set and ``close_reason`` in BOUNDARY_REASONS ends the open
    pairing at ``closed_at`` (exit price unknown) unless the ``orders`` rows hold a filled
    reduce-only sell between its ``opened_at`` and ``closed_at`` (+ a small slack), and before the
    next entry buy: then the normal pairing already closed that trade. Rows with other reasons
    (production history: ``exit``, duplicates) and open rows are ignored. Both the report and the
    dashboard use this one function so that they agree.
    """
    out: list[tuple[datetime, str]] = []
    if not positions:
        return out
    sells: list[datetime] = []
    buys: list[datetime] = []
    for r in orders:
        if float(r.get("filled_qty") or 0.0) <= 0:
            continue
        side = str(r.get("side") or "").lower()
        if side == "sell" and r.get("reduce_only"):
            sells.append(order_fill_time(r, time_key))
        elif side == "buy" and not r.get("reduce_only"):
            buys.append(order_fill_time(r, time_key))
    seen: set[tuple[datetime, str]] = set()
    for p in positions:
        closed = _aware(p.get("closed_at"))
        reason = p.get("close_reason")
        if closed is None or reason not in BOUNDARY_REASONS:
            continue
        opened = _aware(p.get("opened_at")) or _UTC_MIN
        upper = closed + _SELL_MATCH_SLACK
        later_buys = [b for b in buys if b > opened]
        if later_buys:
            upper = min(upper, min(later_buys))
        if any(opened <= s <= upper for s in sells):
            continue
        if (closed, reason) not in seen:  # historical duplicate rows
            seen.add((closed, reason))
            out.append((closed, reason))
    return sorted(out)


@dataclass
class ClosedTrade:
    opened_at: datetime
    closed_at: datetime
    qty: float
    entry_avg: float
    exit_avg: float | None  # None: closed on the exchange (stop/manual), exit price unknown
    fees: float             # entry (+ exit when known) fees
    pnl: float | None       # (exit_avg - entry_avg) * qty - fees; None when exit_avg is unknown
    exit_reason: str | None = None  # close_reason of the positions row for exchange-side closes


def pair_trades(rows: list[dict[str, Any]], positions: list[dict[str, Any]] | None = None) -> list[ClosedTrade]:
    """Pair filled entry buys with the reduce-only sells that follow (long only).

    Rows are filled/partially filled order rows. Fill time is ``updated_at``.
    A trade runs from the first entry buy while flat until the open quantity
    is gone (or only dust below the minimum lot remains). Several entry rows
    (limit partial fill + market fallback) and several exit rows are combined.
    Reduce-only sells while flat (orphans) are ignored.

    ``positions`` (rows of the `positions` table) adds trade boundaries: a position closed
    on the exchange without a sell row of ours (see ``close_boundaries``) closes the open trade
    at its ``closed_at`` with unknown exit price and PnL.
    """
    fills = []
    for r in rows:
        qty = float(r.get("filled_qty") or 0.0)
        px = r.get("filled_price") if r.get("filled_price") is not None else r.get("price")
        if qty <= 0 or px is None:
            continue
        fills.append((order_fill_time(r), 0, r.get("id") or 0, r, qty, float(px), float(r.get("fee") or 0.0)))
    # boundary events sort after the fills at the same instant
    for closed, reason in close_boundaries(positions, rows):
        fills.append((closed, 1, 0, {"boundary": reason}, 0.0, 0.0, 0.0))
    fills.sort(key=lambda x: (x[0], x[1], x[2]))

    trades: list[ClosedTrade] = []
    open_qty = 0.0
    e_qty = e_cost = e_fee = 0.0
    x_qty = x_val = x_fee = 0.0
    opened: datetime | None = None

    for t, _kind, _id, r, qty, px, fee in fills:
        if "boundary" in r:
            if open_qty >= DUST_QTY and opened is not None:
                trades.append(ClosedTrade(opened, t, e_qty, e_cost / e_qty, None, e_fee + x_fee, None, r["boundary"]))
                open_qty = 0.0
            continue
        side = str(r.get("side") or "").lower()
        reduce_only = bool(r.get("reduce_only"))
        if side == "buy" and not reduce_only:
            if open_qty < DUST_QTY:  # flat: start a new trade
                open_qty = e_qty = e_cost = e_fee = x_qty = x_val = x_fee = 0.0
                opened = t
            open_qty += qty
            e_qty += qty
            e_cost += qty * px
            e_fee += fee
        elif side == "sell" and reduce_only:
            if open_qty < DUST_QTY or opened is None:
                continue  # orphan exit
            q = min(qty, open_qty)
            open_qty -= q
            x_qty += q
            x_val += q * px
            x_fee += fee * (q / qty)
            if open_qty < DUST_QTY:
                entry_avg = e_cost / e_qty
                exit_avg = x_val / x_qty
                fees = e_fee + x_fee
                trades.append(ClosedTrade(opened, t, x_qty, entry_avg, exit_avg, fees,
                                          (exit_avg - entry_avg) * x_qty - fees))
                open_qty = 0.0
    return trades


def summarize(trades: list[ClosedTrade]) -> dict[str, float]:
    known = [t for t in trades if t.pnl is not None]
    return {
        "count": len(trades),
        "wins": sum(1 for t in known if t.pnl > 0),
        "losses": sum(1 for t in known if t.pnl <= 0),
        "unknown": len(trades) - len(known),
        "pnl": sum(t.pnl for t in known),
        "fees": sum(t.fees for t in trades),
    }


# ── formatting helpers ──────────────────────────────────────────────────────


def to_dt(v: Any) -> datetime | None:
    """datetime | epoch seconds/ms | ISO string -> aware datetime (or None)."""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, (int, float)):
        sec = v / 1000.0 if v > 1e11 else float(v)
        return datetime.fromtimestamp(sec, tz=timezone.utc)
    try:
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def fmt_kst(v: Any) -> str:
    d = to_dt(v)
    return d.astimezone(KST).strftime("%m-%d %H:%M KST") if d else "N/A"


def _f(v: Any) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def format_position(pos: dict[str, Any] | None) -> str:
    """Position lines for /status, /report and the emergency-close prompt."""
    if not pos:
        return "포지션 없음 (FLAT)"
    side = str(pos.get("side") or "long").upper()
    size = _f(pos.get("size")) or 0.0
    entry = _f(pos.get("entry_price"))
    upnl = _f(pos.get("unrealized_pnl"))
    liq = _f(pos.get("liquidation_price"))
    lines = [f"\U0001f7e2 <b>BTCUSDT {esc(side)}</b> <code>{size:.4f}</code>"]
    if entry is not None:
        lines.append(f"   진입가 <code>${entry:,.2f}</code>")
    if upnl is not None:
        lines.append(f"   미실현 <code>${upnl:+,.2f}</code>")
    if liq:
        lines.append(f"   청산가 <code>${liq:,.2f}</code>")
    return "\n".join(lines)


def _pct_change(now: float | None, then: float | None) -> str:
    if now is None or then is None or then <= 0:
        return "N/A"
    return f"{now - then:+,.2f} USD ({(now / then - 1) * 100:+.2f}%)"


def report_keyboard(dashboard_url: str):
    """Inline keyboard with the dashboard link; None when not configured."""
    if not dashboard_url:
        return None
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup([[InlineKeyboardButton("\U0001f4ca 대시보드 열기", url=dashboard_url)]])


# ── data access ─────────────────────────────────────────────────────────────


async def _snapshot_before(conn, at: datetime) -> float | None:
    row = await conn.fetchrow(
        "SELECT total_equity FROM portfolio_snapshots WHERE snapshot_at <= $1 "
        "ORDER BY snapshot_at DESC LIMIT 1", at)
    return _f(row["total_equity"]) if row else None


async def _load_report_data(db, now: datetime) -> dict[str, Any]:
    data: dict[str, Any] = {}
    async with db.pool.acquire() as conn:
        latest = await conn.fetchrow(
            "SELECT snapshot_at, total_equity, daily_drawdown, weekly_drawdown, monthly_drawdown "
            "FROM portfolio_snapshots ORDER BY snapshot_at DESC LIMIT 1")
        data["latest"] = dict(latest) if latest else None
        data["base"] = {
            "24h": await _snapshot_before(conn, now - timedelta(hours=24)),
            "7d": await _snapshot_before(conn, now - timedelta(days=7)),
            "30d": await _snapshot_before(conn, now - timedelta(days=30)),
        }
        rows = await conn.fetch(
            "SELECT id, side, order_type, quantity, price, status, filled_qty, filled_price, fee, "
            "reduce_only, created_at, updated_at, strategy_id FROM orders "
            "WHERE filled_qty > 0 ORDER BY updated_at, id")
        data["orders"] = [dict(r) for r in rows]
        try:
            prow = await conn.fetch(
                "SELECT opened_at, closed_at, close_reason FROM positions "
                "WHERE closed_at IS NOT NULL AND close_reason = ANY($1::text[]) ORDER BY closed_at",
                list(BOUNDARY_REASONS))
            data["closed_positions"] = [dict(r) for r in prow]
        except Exception as exc:  # pairing from orders alone is still right for bot-side exits
            log.warning("report_positions_query_failed", error=repr(exc))
            data["closed_positions"] = []
        sig = await conn.fetchrow(
            "SELECT bar_ts, st_dir, price, exit_signal, exit_reason, entry_ok, expected_action "
            "FROM supertrend_signals ORDER BY bar_ts DESC LIMIT 1")
        data["signal"] = dict(sig) if sig else None
        pos = await conn.fetchrow(
            "SELECT side, size, entry_price, opened_at FROM positions "
            "WHERE closed_at IS NULL ORDER BY opened_at DESC LIMIT 1")
        data["db_position"] = dict(pos) if pos else None
    return data


# ── report ──────────────────────────────────────────────────────────────────


async def build_daily_report(db, control) -> str:
    now = datetime.now(timezone.utc)
    status: dict[str, Any] = {}
    try:
        status = await control.status() or {}
    except Exception as exc:
        log.warning("report_status_failed", error=repr(exc))
    try:
        data = await _load_report_data(db, now)
    except Exception as exc:
        log.warning("report_db_failed", error=repr(exc))
        data = {"latest": None, "base": {}, "orders": [], "signal": None, "db_position": None}

    latest = data["latest"]
    equity = _f(status.get("equity"))
    if equity is None and latest:
        equity = _f(latest["total_equity"])

    lines = [f"\U0001f4c5 <b>일일 리포트 — {now.astimezone(KST).strftime('%Y-%m-%d %H:%M')} KST</b>", ""]

    # equity and changes
    lines.append(f"\U0001f4b0 잔고: <code>{'$%s' % format(equity, ',.2f') if equity is not None else 'N/A'}</code>")
    base = data["base"]
    grid = "\n".join(
        f"{label:<4} {_pct_change(equity, base.get(key))}"
        for label, key in (("24h", "24h"), ("7d", "7d"), ("30d", "30d"))
    )
    lines.append(f"<pre>{grid}</pre>")

    # position
    pos = status.get("position") if status else None
    if not status and data["db_position"]:
        p = data["db_position"]
        pos = {"side": p["side"], "size": p["size"], "entry_price": p["entry_price"]}
    lines.append("<b>포지션</b>")
    lines.append(format_position(pos))
    lines.append("")

    # closed trades
    trades = pair_trades(data["orders"], data.get("closed_positions"))
    lines.append("<b>청산 거래 (실현손익, 수수료 포함)</b>")
    rows = []
    for label, delta in (("24h", timedelta(hours=24)), ("30d", timedelta(days=30))):
        s = summarize([t for t in trades if t.closed_at >= now - delta])
        unk = f" (손익 불명 {int(s['unknown'])}건: 거래소 측 청산)" if s["unknown"] else ""
        rows.append(f"{label:<4} {int(s['count'])}건 {int(s['wins'])}승 {int(s['losses'])}패  "
                    f"PnL {s['pnl']:+,.2f}  수수료 {s['fees']:,.2f}{unk}")
    lines.append(f"<pre>{chr(10).join(rows)}</pre>")

    # drawdown
    def dd(key_status: str, key_snap: str) -> str:
        v = _f(status.get(key_status)) if status else None
        if v is None and latest:
            v = _f(latest.get(key_snap))
        return f"{v:.2f}%" if v is not None else "N/A"

    lines.append(
        f"⚠️ 낙폭: 일 <code>{dd('daily_dd_pct', 'daily_drawdown')}</code> | "
        f"주 <code>{dd('weekly_dd_pct', 'weekly_drawdown')}</code> | "
        f"월 <code>{dd('monthly_dd_pct', 'monthly_drawdown')}</code>")

    # kill/halt
    if status:
        if status.get("halted"):
            lines.append(f"\U0001f6d1 진입 차단: <code>{esc(status.get('halt_reason'))}</code> "
                         f"(Kill level {esc(status.get('kill_level'))})")
        else:
            lines.append(f"✅ 정상 (Kill level {esc(status.get('kill_level'))})")

    # last signal
    sig = data["signal"]
    if sig:
        direction = "상승" if (sig["st_dir"] or 0) > 0 else "하락"
        extra = ""
        if sig["exit_signal"]:
            extra = f" | 청산신호 {esc(sig['exit_reason'])}"
        elif sig["entry_ok"]:
            extra = " | 진입 가능"
        lines.append(
            f"\U0001f4e1 마지막 신호: {fmt_kst(sig['bar_ts'])} 봉, ST {direction}, "
            f"<code>${_f(sig['price']) or 0:,.2f}</code>, 동작 <code>{esc(sig['expected_action'])}</code>{extra}")
    else:
        lines.append("\U0001f4e1 마지막 신호: 없음")
    return "\n".join(lines)
