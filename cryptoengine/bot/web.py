"""Minimal read-only operator dashboard (aiohttp, Postgres only).

Routes: /healthz (open), / (page), /api/summary (JSON). Everything except
/healthz and the original dashboard (``bot.web_supertrend``: /supertrend, /css/, /js/,
/api/internal/supertrend/*, no token as before) needs the token (``?t=`` or the cookie set
by a valid ``?t=`` visit). An empty token means only /healthz and the original dashboard
are served.
"""

from __future__ import annotations

import hmac
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import structlog
from aiohttp import web

from bot.report import BOUNDARY_REASONS, close_boundaries, order_fill_time
from bot.web_supertrend import SupertrendProviders, is_open_path, register as register_supertrend

log = structlog.get_logger("web")

STATIC_DIR = Path(__file__).parent / "web_static"
COOKIE = "dash_t"
COOKIE_MAX_AGE = 30 * 24 * 3600
QTY_EPS = 1e-9
DUST_QTY = 0.0005  # below the 0.001 BTC step: treated as flat


# --------------------------------------------------------------------- utils

def _f(v: Any) -> float | None:
    if v is None:
        return None
    return float(v)


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).isoformat()
    return str(v)


def _parse_iso(v: Any) -> datetime | None:
    if not v:
        return None
    try:
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _json_default(o: Any) -> Any:
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        return _iso(o)
    raise TypeError(type(o))


async def _safe(coro, default):
    """Run a query; a failing section degrades to its default, never a 500."""
    try:
        return await coro
    except Exception as exc:  # noqa: BLE001
        log.warning("web_query_failed", error=str(exc))
        return default


# --------------------------------------------------------------- trade pairing

_BOUNDARY_LABEL = {
    "exchange_side": "거래소 측 청산(스탑/수동)",
    "exchange_flat": "거래소 측 청산(포지션 없음 확인)",
    "kill_switch": "킬스위치 청산",
    "emergency_close": "긴급 청산",
}


def pair_trades(rows: list[dict], exit_signals: list[dict] | None = None,
                positions: list[dict] | None = None) -> list[dict]:
    """Pair filled buys with the reduce-only sells that follow (long only).

    ``rows`` are order rows sorted by created_at with filled_qty > 0. A trade
    opens at the first non-reduce buy while flat, accumulates further buys, and
    closes once the sold quantity reaches the bought quantity (dust ignored).
    PnL = (exit_avg - entry_avg) * qty - fees (all fee rows of the trade).

    ``positions`` (rows of the `positions` table) adds boundaries through
    ``bot.report.close_boundaries`` (same rule as the daily report): a position closed on the
    exchange without a sell row of ours closes the open trade at its ``closed_at`` with
    exit_price / pnl / pnl_pct = None (unknown, never invented).
    """
    trades: list[dict] = []
    cur: dict | None = None

    def finish(t: dict) -> None:
        qty = t["buy_qty"] if t["sell_qty"] <= QTY_EPS else min(t["buy_qty"], t["sell_qty"])
        entry = t["buy_cost"] / t["buy_qty"]
        exit_ = t["sell_cost"] / t["sell_qty"]
        pnl = (exit_ - entry) * qty - t["fees"]
        notional = entry * qty
        trades.append({
            "opened_at": _iso(t["opened_at"]),
            "closed_at": _iso(t["closed_at"]),
            "entry_price": entry,
            "exit_price": exit_,
            "qty": qty,
            "fees": t["fees"],
            "pnl": pnl,
            "pnl_pct": (pnl / notional * 100.0) if notional else 0.0,
            "exit_reason": None,
            "_exit_start": t["exit_start"],
        })

    def finish_unknown(t: dict, at: datetime, reason: str) -> None:
        entry = t["buy_cost"] / t["buy_qty"]
        trades.append({
            "opened_at": _iso(t["opened_at"]),
            "closed_at": _iso(at),
            "entry_price": entry,
            "exit_price": None,
            "qty": t["buy_qty"],
            "fees": t["fees"],
            "pnl": None,
            "pnl_pct": None,
            "exit_reason": _BOUNDARY_LABEL.get(reason, reason),
        })

    # Rows are ordered by created_at here (the report orders by updated_at).
    events: list[tuple[datetime, int, Any]] = [
        (order_fill_time(r, "created_at"), 0, r) for r in rows]
    events += [(c, 1, reason) for c, reason in close_boundaries(positions, rows, "created_at")]
    events.sort(key=lambda e: (e[0], e[1]))  # stable: rows keep their input order

    for _t, kind, r in events:
        if kind == 1:
            if cur is not None and cur["buy_qty"] - cur["sell_qty"] > DUST_QTY:
                finish_unknown(cur, _t, r)
                cur = None
            continue
        qty = _f(r["filled_qty"]) or 0.0
        price = _f(r["filled_price"]) or _f(r.get("price")) or 0.0
        fee = _f(r.get("fee")) or 0.0
        if qty <= 0 or price <= 0:
            continue
        ts = r.get("updated_at") or r["created_at"]
        side = (r["side"] or "").lower()
        if side == "buy" and not r.get("reduce_only"):
            if cur is not None and cur["sell_qty"] > QTY_EPS:
                # previous trade was only partially exited and left; close it out
                finish(cur)
                cur = None
            if cur is None:
                cur = {"opened_at": r["created_at"], "buy_qty": 0.0, "buy_cost": 0.0,
                       "sell_qty": 0.0, "sell_cost": 0.0, "fees": 0.0,
                       "closed_at": None, "exit_start": None}
            cur["buy_qty"] += qty
            cur["buy_cost"] += qty * price
            cur["fees"] += fee
        elif side == "sell" and r.get("reduce_only") and cur is not None:
            cur["sell_qty"] += qty
            cur["sell_cost"] += qty * price
            cur["fees"] += fee
            cur["closed_at"] = ts
            if cur["exit_start"] is None:
                cur["exit_start"] = r["created_at"]
            if cur["buy_qty"] - cur["sell_qty"] <= DUST_QTY:
                finish(cur)
                cur = None

    # attach exit reason: latest exit signal on a bar that closed before the exit order
    sigs = sorted(exit_signals or [], key=lambda s: s["bar_ts"])
    for t in trades:
        if "_exit_start" not in t:  # boundary trade: reason already set
            continue
        start = t.pop("_exit_start")
        reason = None
        if start is not None:
            # exit signal on a bar that closed shortly (<= 8h, covers retries) before the exit order
            near = [s for s in sigs
                    if -timedelta(minutes=1) <= start - (s["bar_ts"] + timedelta(hours=4)) <= timedelta(hours=8)
                    and s.get("exit_reason")]
            reason = near[-1]["exit_reason"] if near else None
        t["exit_reason"] = reason or "기타(스탑/수동)"
    return trades


def trade_totals(trades: list[dict]) -> dict:
    known = [t for t in trades if t["pnl"] is not None]
    wins = sum(1 for t in known if t["pnl"] > 0)
    return {
        "count": len(trades),
        "wins": wins,
        "losses": len(known) - wins,
        "unknown": len(trades) - len(known),
        "pnl": sum(t["pnl"] for t in known),
        "fees": sum(t["fees"] for t in trades),
    }


# ---------------------------------------------------------------------- SQL

async def _bot_state(pool) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for r in await pool.fetch("SELECT key, value FROM bot_state"):
        v = r["value"]
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except ValueError:
                pass
        out[r["key"]] = v
    return out


async def _equity_curve(pool, days: int = 90) -> list[dict]:
    rows = await pool.fetch(
        """SELECT DISTINCT ON (date_trunc('hour', snapshot_at))
                  snapshot_at, total_equity
           FROM portfolio_snapshots
           WHERE snapshot_at >= now() - make_interval(days => $1)
           ORDER BY date_trunc('hour', snapshot_at), snapshot_at DESC""",
        days,
    )
    return [{"t": _iso(r["snapshot_at"]), "v": _f(r["total_equity"])} for r in rows]


async def _latest_equity(pool) -> tuple[float | None, str | None]:
    r = await pool.fetchrow(
        "SELECT total_equity, snapshot_at FROM portfolio_snapshots ORDER BY snapshot_at DESC LIMIT 1")
    return (_f(r["total_equity"]), _iso(r["snapshot_at"])) if r else (None, None)


async def _open_position(pool) -> dict | None:
    r = await pool.fetchrow(
        """SELECT side, size, entry_price, opened_at FROM positions
           WHERE closed_at IS NULL ORDER BY opened_at DESC LIMIT 1""")
    if not r:
        return None
    return {"side": r["side"], "size": _f(r["size"]), "entry_price": _f(r["entry_price"]),
            "opened_at": _iso(r["opened_at"])}


async def _trades(pool, limit: int = 30) -> list[dict]:
    rows = await pool.fetch(
        """SELECT side, order_type, status, quantity, price, filled_qty, filled_price,
                  fee, reduce_only, created_at, updated_at
           FROM orders
           WHERE COALESCE(filled_qty, 0) > 0 AND created_at >= now() - interval '800 days'
           ORDER BY created_at, id""")
    rows = [dict(r) for r in rows]
    if not rows:
        return []
    sigs = await _safe(pool.fetch(
        "SELECT bar_ts, exit_reason FROM supertrend_signals WHERE exit_signal AND bar_ts >= $1 ORDER BY bar_ts",
        rows[0]["created_at"] - timedelta(days=1)), [])
    prow = await _safe(pool.fetch(
        "SELECT opened_at, closed_at, close_reason FROM positions "
        "WHERE closed_at IS NOT NULL AND close_reason = ANY($1::text[]) ORDER BY closed_at",
        list(BOUNDARY_REASONS)), [])
    trades = pair_trades(rows, [dict(s) for s in sigs], [dict(p) for p in prow])
    return trades[-limit:][::-1]  # newest first


async def _signals(pool, limit: int = 60) -> list[dict]:
    rows = await pool.fetch(
        """SELECT bar_ts, st_dir, price, atr_14, entry_ok, exit_signal, exit_reason,
                  expected_action, had_position
           FROM supertrend_signals ORDER BY bar_ts DESC LIMIT $1""", limit)
    return [{
        "bar_ts": _iso(r["bar_ts"]), "st_dir": r["st_dir"], "price": _f(r["price"]),
        "atr": _f(r["atr_14"]), "entry_ok": r["entry_ok"], "exit_signal": r["exit_signal"],
        "exit_reason": r["exit_reason"], "action": r["expected_action"],
        "had_position": r["had_position"],
    } for r in rows]


async def _candles(pool, limit: int = 200) -> list[dict]:
    rows = await pool.fetch(
        """SELECT timestamp, open, high, low, close FROM ohlcv_history
           WHERE exchange='bybit' AND symbol='BTCUSDT' AND timeframe='4h'
           ORDER BY timestamp DESC LIMIT $1""", limit)
    return [{"t": _iso(r["timestamp"]), "o": _f(r["open"]), "h": _f(r["high"]),
             "l": _f(r["low"]), "c": _f(r["close"])} for r in reversed(rows)]


def trading_state(state: dict[str, Any], now: datetime) -> dict:
    reasons: list[str] = []
    halt = state.get("manual_halt") or {}
    if isinstance(halt, dict) and halt.get("active"):
        reasons.append("수동 중지: " + (halt.get("reason") or "사유 없음"))
    blk = state.get("portfolio_block_until") or {}
    until = _parse_iso(blk.get("until")) if isinstance(blk, dict) else None
    if until and until > now:
        reasons.append("포트폴리오 차단: " + (blk.get("reason") or "사유 없음"))
    return {
        "entries_blocked": bool(reasons),
        "reason": " / ".join(reasons) or None,
        "blocked_until": _iso(until) if until and until > now else None,
    }


async def build_summary(pool) -> dict:
    now = datetime.now(timezone.utc)
    state = await _safe(_bot_state(pool), {})
    hb = state.get("heartbeat") if isinstance(state.get("heartbeat"), dict) else {}
    hb_at = _parse_iso(hb.get("at"))
    equity, equity_at = await _safe(_latest_equity(pool), (None, None))
    signals = await _safe(_signals(pool), [])
    trades = await _safe(_trades(pool), [])
    kill = state.get("kill_switch") if isinstance(state.get("kill_switch"), dict) else None
    last_bar = None
    if signals:
        s = signals[0]
        sig = "청산" if s["exit_signal"] else ("진입" if s["entry_ok"] else "대기")
        last_bar = {**s, "signal": sig}
    return {
        "generated_at": _iso(now),
        "status": {
            "equity": equity,
            "equity_at": equity_at,
            "position": await _safe(_open_position(pool), None),
            "trading": trading_state(state, now),
            "kill_switch": kill,
            "heartbeat_at": _iso(hb_at),
            "heartbeat_age_sec": int((now - hb_at).total_seconds()) if hb_at else None,
            "last_bar": last_bar,
        },
        "equity_curve": await _safe(_equity_curve(pool), []),
        "candles": await _safe(_candles(pool), []),
        "trades": trades,
        "trade_totals": trade_totals(trades),
        "signals": signals,
    }


# ------------------------------------------------------------------- server

def _make_app(db, token: str, supertrend: SupertrendProviders | None = None) -> web.Application:
    pool_of = lambda: db.pool  # noqa: E731

    FORBIDDEN_TEXT = "접근 토큰이 필요합니다. Telegram에서 /dashboard 를 보내 받은 링크로 한 번 열면 이후에는 이 주소로 바로 열립니다."

    @web.middleware
    async def auth(request: web.Request, handler):
        if request.path == "/healthz" or is_open_path(request.path):
            return await handler(request)
        if not token:
            return web.Response(status=403, text=FORBIDDEN_TEXT)
        q = request.query.get("t", "")
        c = request.cookies.get(COOKIE, "")
        q_ok = bool(q) and hmac.compare_digest(q.encode(), token.encode())
        c_ok = bool(c) and hmac.compare_digest(c.encode(), token.encode())
        if not (q_ok or c_ok):
            return web.Response(status=403, text=FORBIDDEN_TEXT)
        resp = await handler(request)
        if q_ok and not c_ok:
            resp.set_cookie(COOKIE, token, max_age=COOKIE_MAX_AGE, httponly=True, samesite="Lax", path="/")
        return resp

    async def healthz(_r):
        return web.Response(text="ok")

    async def index(_r):
        html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        return web.Response(text=html, content_type="text/html", headers={"Cache-Control": "no-store"})

    async def summary(_r):
        data = await build_summary(pool_of())
        return web.Response(text=json.dumps(data, default=_json_default, ensure_ascii=False),
                            content_type="application/json", headers={"Cache-Control": "no-store"})

    app = web.Application(middlewares=[auth])
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/", index)
    app.router.add_get("/api/summary", summary)
    register_supertrend(app, pool_of, supertrend)  # the original /supertrend dashboard (no token)
    return app


class WebServer:
    def __init__(self, runner: web.AppRunner):
        self._runner = runner

    async def stop(self) -> None:
        await self._runner.cleanup()


async def start_web(db, *, port: int, token: str,
                    supertrend: SupertrendProviders | None = None) -> WebServer:
    if not token:
        log.warning("dashboard_token_empty_serving_only_healthz")
    runner = web.AppRunner(_make_app(db, token, supertrend), access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("web_started", port=port)
    return WebServer(runner)
