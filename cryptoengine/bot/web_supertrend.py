"""The original `/supertrend` dashboard: static page plus its read-only JSON API.

A port of the old Node/Express app (``dashboard/``, removed from the tree; see git history).
Page files live byte-for-byte in ``web_static/supertrend/`` and are served at the old URLs,
**without the token** (the old server had no auth either). ``/api/internal/supertrend/*``
reproduces the old SQL and the old JSON as node-postgres + ``res.json`` emitted it:

* NUMERIC / ``round(x::numeric, n)`` -> JSON string, double precision -> JSON number,
  timestamptz -> ``YYYY-MM-DDTHH:MM:SS.mmmZ`` (milliseconds, truncated);
* numbers are written the way ``JSON.stringify`` writes them (``5`` not ``5.0``, NaN -> null).

Three values used to come from Redis; they now come from optional provider callables
(``SupertrendProviders``) that ``bot.main`` builds from the bot's own objects.
Everything here is read-only.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable

import structlog
from aiohttp import web

log = structlog.get_logger("web_supertrend")

SUPERTREND_DIR = Path(__file__).parent / "web_static" / "supertrend"
STRATEGY_ID = "supertrend-01"
SYMBOL = "BTC/USDT:USDT"
BAR_MS = 4 * 60 * 60 * 1000
UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

API_PREFIX = "/api/internal/supertrend/"
OPEN_PREFIXES = (API_PREFIX, "/css/", "/js/")
OPEN_PATHS = ("/supertrend", "/supertrend/", "/supertrend.html")


def is_open_path(path: str) -> bool:
    """Paths served without the token (as the old dashboard did)."""
    return path in OPEN_PATHS or path.startswith(OPEN_PREFIXES)


# ---------------------------------------------------------------- JSON like Node

def _js_number(v: float) -> str:
    """Number::toString as JavaScript does it (JSON.stringify of a finite number)."""
    if v != v or v in (math.inf, -math.inf):
        return "null"
    if v == 0:
        return "0"
    if v == int(v) and abs(v) < 9007199254740992:
        return str(int(v))
    sign = "-" if v < 0 else ""
    d = Decimal(repr(abs(v)))
    _s, digit_t, exp = d.as_tuple()
    digits = "".join(map(str, digit_t)).rstrip("0") or "0"
    # value = 0.DIGITS * 10**n  with k = len(digits)
    k = len(digits)
    n = len(digit_t) + exp
    # (rstrip of trailing zeros does not change n)
    if k <= n <= 21:
        out = digits + "0" * (n - k)
    elif 0 < n <= 21:
        out = digits[:n] + "." + digits[n:]
    elif -6 < n <= 0:
        out = "0." + "0" * (-n) + digits
    else:
        e = n - 1
        es = ("+" if e >= 0 else "-") + str(abs(e))
        out = digits[0] + ("." + digits[1:] if k > 1 else "") + "e" + es
    return sign + out


def iso_ms(dt: datetime) -> str:
    """Date.prototype.toISOString (milliseconds, truncated)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt = dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _enc(v: Any) -> str:
    if v is None:
        return "null"
    if v is True:
        return "true"
    if v is False:
        return "false"
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return _js_number(v)
    if isinstance(v, Decimal):  # node-pg returns NUMERIC as its text
        return json.dumps(format(v, "f"), ensure_ascii=False)
    if isinstance(v, datetime):
        return '"' + iso_ms(v) + '"'
    if isinstance(v, dict):
        return "{" + ",".join(json.dumps(str(k), ensure_ascii=False) + ":" + _enc(x)
                              for k, x in v.items()) + "}"
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(_enc(x) for x in v) + "]"
    raise TypeError(type(v))


def _json(data: Any, status: int = 200) -> web.Response:
    return web.Response(text=_enc(data), status=status,
                        content_type="application/json", charset="utf-8",
                        headers={"Access-Control-Allow-Origin": "*"})


def _error(message: str) -> web.Response:
    return _json({"error": message}, 500)


# --------------------------------------------------------------- JS semantics

def _ms(dt: datetime) -> int:
    """Date.getTime() of a pg timestamptz: whole milliseconds since the epoch."""
    return (dt - _EPOCH) // timedelta(milliseconds=1)


def _trunc_ms(dt: datetime) -> datetime:
    """What node-pg hands back: a JS Date, i.e. milliseconds only."""
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)


def _pf(v: Any) -> float:
    """parseFloat(v) for a pg value (NUMERIC string, float, None -> NaN)."""
    if v is None:
        return math.nan
    if isinstance(v, (int, float, Decimal)):
        return float(v)
    m = re.match(r"\s*[+-]?(?:\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?)", str(v))
    return float(m.group(0)) if m else math.nan


def _truthy(v: Any) -> bool:
    return v is not None and v is not False and v != 0 and v == v and v != ""


def _div(a: float, b: float) -> float:
    """IEEE division like JS (x/0 -> Infinity/NaN, no exception)."""
    try:
        return a / b
    except ZeroDivisionError:
        if a != a or a == 0:
            return math.nan
        return math.inf if (a > 0) == (math.copysign(1.0, b) > 0) else -math.inf


def _to_fixed2(x: float) -> str:
    return str(Decimal(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


_ISO_RE = re.compile(
    r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?"
    r"(?:[Tt](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?(Z|z|[+-]\d{2}:?\d{2})?)?$")
# V8's legacy fallback (what `new Date("2026-11-01 12:30 GMT")` goes through)
_LEGACY_RE = re.compile(
    r"^(\d{4})-(\d{1,2})-(\d{1,2})"
    r"(?P<time>\s+(\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?(?:\s*([AaPp][Mm]))?)?"
    r"(?:\s*(Z|GMT|UTC|UT|[ECMP][SD]T)|(?(time)\s*|\s+)(?:GMT|UTC|UT)?([+-])(\d{2}):?(\d{2}))?\s*$", re.I)
_US_TZ = {"EST": -5, "EDT": -4, "CST": -6, "CDT": -5, "MST": -7, "MDT": -6, "PST": -8, "PDT": -7}


def _js_date(raw: str) -> datetime | None:
    """new Date(raw) for strings that start YYYY-MM-DD (the old code's guard).

    Strict ISO first (date-only = UTC; a date-time without offset = local time, UTC in the old
    container), then the common shapes of V8's legacy parser. Anything else is an Invalid Date
    (the old code then falls back to the leading-digits "days" rule).
    """
    try:
        return _js_date_iso(raw) or _js_date_legacy(raw)
    except (ValueError, OverflowError):
        return None


def _js_date_iso(raw: str) -> datetime | None:
    m = _ISO_RE.match(raw)
    if not m:
        return None
    y, mo, d, hh, mi, ss, frac, tz = m.groups()
    if not 1 <= int(mo or 1) <= 12 or not 1 <= int(d or 1) <= 31:
        return None
    base = datetime(int(y), int(mo or 1), 1, tzinfo=UTC) + timedelta(days=int(d or 1) - 1)  # V8 overflows 02-30
    if hh is not None:
        h = int(hh)
        if h == 24:
            if int(mi) or int(ss or 0) or int(frac or 0):
                return None
        elif h > 23 or int(mi) > 59 or int(ss or 0) > 59:
            return None
        base += timedelta(hours=h, minutes=int(mi), seconds=int(ss or 0),
                          milliseconds=int((frac or "0")[:3].ljust(3, "0")))
    if tz and tz not in ("Z", "z"):
        t = tz[1:].replace(":", "")
        if int(t[:2]) > 23 or int(t[2:]) > 59:
            return None
        base -= (1 if tz[0] == "+" else -1) * timedelta(hours=int(t[:2]), minutes=int(t[2:]))
    return base


def _js_date_legacy(raw: str) -> datetime | None:
    m = _LEGACY_RE.match(raw)
    if not m:
        return None
    y, mo, d, _t, hh, mi, ss, frac, ampm, name, sign, th, tm = m.groups()
    if not 1 <= int(mo) <= 12 or not 1 <= int(d) <= 31:
        return None
    base = datetime(int(y), int(mo), 1, tzinfo=UTC) + timedelta(days=int(d) - 1)
    if hh is not None:
        h = int(hh)
        if ampm:
            if not 1 <= h <= 12:
                return None
            h = h % 12 + (12 if ampm.lower() == "pm" else 0)
        if h > 24 or int(mi) > 59 or int(ss or 0) > 59 or (h == 24 and (int(mi) or int(ss or 0))):
            return None
        base += timedelta(hours=h, minutes=int(mi), seconds=int(ss or 0),
                          milliseconds=int((frac or "0")[:3].ljust(3, "0")))
    if name and name.upper() in _US_TZ:
        base -= timedelta(hours=_US_TZ[name.upper()])
    elif sign:
        if int(th) > 23 or int(tm) > 59:
            return None
        base -= (1 if sign == "+" else -1) * timedelta(hours=int(th), minutes=int(tm))
    return base


class _BadParam(Exception):
    """The old handler threw (TypeError) -> 500."""


def parse_time_param(raw: Any, default_days: int = 30) -> tuple[str, Any]:
    """('days', n) or ('ts', datetime). Mirrors parseTimeParam() of the old route."""
    if isinstance(raw, list):  # ?from=a&from=b -> JS array
        s = ",".join(raw)
        if re.match(r"^\d{4}-\d{2}-\d{2}", s):
            d = _js_date(s)
            if d is not None:
                return ("ts", d)
        raise _BadParam  # raw.match is not a function
    if not raw:
        return ("days", default_days)
    if re.match(r"^\d{4}-\d{2}-\d{2}", raw):
        d = _js_date(raw)
        if d is not None:
            return ("ts", d)
        if raw.startswith("0000") and _js_date("2000" + raw[4:]) is not None:
            raise _BadParam  # a valid JS Date in year 0: Postgres rejects the timestamp -> 500
    m = re.match(r"^(\d+)", raw)
    if m:
        return ("days", min(max(1, int(m.group(1))), 365))
    return ("days", default_days)


def _q(request: web.Request, name: str) -> Any:
    vals = request.query.getall(name, [])
    if not vals:
        return None
    return vals[0] if len(vals) == 1 else vals


def _first_truthy(*vals: Any) -> Any:
    for v in vals:
        if v:
            return v
    return vals[-1]


# ----------------------------------------------------------------- providers

@dataclass
class SupertrendProviders:
    """Replacements for the three Redis keys the old dashboard read (all optional)."""

    # strategy:status:supertrend-01 -> StrategyStatus-shaped dict or None
    strategy_status: Callable[[], Awaitable[dict | None]] | None = None
    # ce:positions:all -> list of Position.model_dump() dicts (open positions)
    positions: Callable[[], Awaitable[list[dict] | None]] | None = None
    # cache:ohlcv:bybit:BTCUSDT:4h -> {"ts","open","high","low","close","volume","confirmed"} or None
    in_progress_candle: Callable[[], Awaitable[dict | None]] | None = None


async def _call(fn, what: str):
    if fn is None:
        return None
    try:
        return await fn()
    except Exception as exc:  # noqa: BLE001 - old code: "Redis optional"
        log.warning("supertrend_provider_failed", what=what, error=str(exc)[:200])
        return None


class _Cached:
    """Single-flight TTL cache for an async producer (keeps REST calls to the exchange rare)."""

    def __init__(self, fn: Callable[[], Awaitable[Any]], ttl: float):
        self._fn, self._ttl = fn, ttl
        self._at = -1e18
        self._val: Any = None
        self._lock = asyncio.Lock()

    async def __call__(self) -> Any:
        async with self._lock:
            if time.monotonic() - self._at >= self._ttl:
                self._val = await self._fn()
                self._at = time.monotonic()
            return self._val


def make_providers(*, app: Any, executor: Any, exchange: Any,
                   status_ttl: float = 10.0, candle_ttl: float = 30.0) -> SupertrendProviders:
    """Providers from the live bot: BotControl.status(), the executor and the exchange."""

    async def _status() -> dict:
        return await app.status()

    status_c = _Cached(_status, status_ttl)

    async def _position() -> list[dict] | None:
        pos = await executor.get_position()
        return [pos.model_dump()] if pos is not None and pos.size > 0 else []

    pos_c = _Cached(_position, status_ttl)

    async def strategy_status() -> dict | None:
        st = await status_c()
        pos = st.get("position") or {}
        equity = st.get("equity") or 0.0
        hb = st.get("heartbeat_at")
        if isinstance(hb, datetime):
            last_tick = hb.astimezone(UTC).replace(tzinfo=None).isoformat()
        else:
            last_tick = datetime.now(UTC).replace(tzinfo=None).isoformat()
        return {
            "strategy_id": STRATEGY_ID,
            "is_running": not st.get("halted"),
            "allocated_capital": float(equity) * 0.95,
            "current_pnl": float(pos.get("unrealized_pnl") or 0.0),
            "position_count": 1 if pos else 0,
            "last_tick": last_tick,
        }

    async def in_progress() -> dict | None:
        bucket = int(time.time() * 1000) // BAR_MS * BAR_MS
        rows = await exchange.get_ohlcv(SYMBOL, "4h", limit=2)
        for r in reversed(rows or []):
            if int(r[0]) == bucket:
                return {"ts": bucket, "open": float(r[1]), "high": float(r[2]),
                        "low": float(r[3]), "close": float(r[4]), "volume": float(r[5] or 0),
                        "confirmed": False}
        return None

    return SupertrendProviders(strategy_status=strategy_status, positions=pos_c,
                               in_progress_candle=_Cached(in_progress, candle_ttl))


# ------------------------------------------------------------------- SQL / API

_SIGNAL_COLS = """SELECT bar_ts, computed_at, st_dir, round(st_line::numeric,2) AS st_line,
                 round(fast_ema::numeric,2) AS fast_ema, round(slow_ema::numeric,2) AS slow_ema,
                 round(dir_ema::numeric,2)  AS dir_ema,  round(price::numeric,2)    AS price,
                 round(atr_14::numeric,2)   AS atr_14,   allocated_capital,
                 had_position, entry_ok, exit_signal, exit_reason, expected_action,
                 round(expected_qty::numeric,6)       AS expected_qty,
                 round(expected_stop_loss::numeric,2) AS expected_stop_loss
               FROM supertrend_signals"""

_STATUS_COLS = """SELECT bar_ts, computed_at, st_dir,
          round(fast_ema::numeric,2) AS fast_ema, round(slow_ema::numeric,2) AS slow_ema,
          round(dir_ema::numeric,2)  AS dir_ema,  round(price::numeric,2)    AS price,
          round(atr_14::numeric,2)   AS atr_14,   allocated_capital,
          had_position, entry_ok, exit_signal, exit_reason, expected_action,
          round(expected_qty::numeric,6)       AS expected_qty,
          round(expected_stop_loss::numeric,2) AS expected_stop_loss
        FROM supertrend_signals
        ORDER BY bar_ts DESC
        LIMIT 1"""

_ORDER_COLS = """id, request_id, order_id, side, filled_qty, filled_price, fee,
                   reduce_only, status, created_at, updated_at"""

_EXP_COLS = """SELECT bar_ts, expected_action, price AS expected_price,
                     expected_qty, expected_stop_loss, exit_reason AS expected_exit_reason,
                     actual_exit_price, actual_exit_at, delay_note
              FROM supertrend_signals
              WHERE expected_action IN ('enter','exit')"""


def _rows(records) -> list[dict]:
    """Records -> dicts with timestamps cut to milliseconds (as node-pg Dates are)."""
    out = []
    for r in records:
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = _trunc_ms(v)
        out.append(d)
    return out


def register(app: web.Application, pool_of: Callable[[], Any],
             providers: SupertrendProviders | None = None) -> None:
    """Add the page, its assets and the JSON API to ``app``."""
    prov = providers or SupertrendProviders()

    # ---- static (same URLs as express.static + the explicit /supertrend route)

    async def page(_r: web.Request) -> web.StreamResponse:
        resp = web.FileResponse(SUPERTREND_DIR / "supertrend.html")
        resp.headers["Cache-Control"] = "public, max-age=0"
        resp.headers["Content-Type"] = "text/html; charset=UTF-8"
        return resp

    async def asset(request: web.Request) -> web.StreamResponse:
        folder = request.match_info["folder"]
        name = request.match_info["name"]
        path = (SUPERTREND_DIR / folder / name)
        if not re.fullmatch(r"[A-Za-z0-9._-]+", name) or name.startswith(".") or not path.is_file():
            raise web.HTTPNotFound()
        resp = web.FileResponse(path)
        resp.headers["Cache-Control"] = "public, max-age=0"
        if name.endswith(".js"):
            resp.headers["Content-Type"] = "application/javascript; charset=UTF-8"
        elif name.endswith(".css"):
            resp.headers["Content-Type"] = "text/css; charset=UTF-8"
        return resp

    app.router.add_get("/supertrend", page)
    app.router.add_get("/supertrend/", page)
    app.router.add_get("/supertrend.html", page)
    app.router.add_get("/{folder:css|js}/{name}", asset)

    # ---- API

    async def expected(request: web.Request) -> web.Response:
        try:
            pool = pool_of()
            fk, fv = parse_time_param(_q(request, "from"), 30)
            tk, tv = parse_time_param(_q(request, "to"), 0)
            if fk == "ts" and tk == "ts":
                rows = await pool.fetch(
                    f"{_SIGNAL_COLS} WHERE bar_ts BETWEEN $1::timestamptz AND $2::timestamptz "
                    "ORDER BY bar_ts ASC", fv, tv)
            elif fk == "ts":
                rows = await pool.fetch(
                    f"{_SIGNAL_COLS} WHERE bar_ts >= $1::timestamptz ORDER BY bar_ts ASC", fv)
            else:
                rows = await pool.fetch(
                    f"{_SIGNAL_COLS} WHERE bar_ts >= NOW() - $1::int * INTERVAL '1 day' "
                    "ORDER BY bar_ts ASC", fv)
            rows = _rows(rows)
            return _json({"signals": rows, "count": len(rows)})
        except Exception as exc:  # noqa: BLE001
            log.warning("supertrend_expected_error", error=str(exc)[:300])
            return _error("Failed to fetch expected signals")

    async def actual(request: web.Request) -> web.Response:
        try:
            pool = pool_of()
            fk, fv = parse_time_param(_q(request, "from"), 30)
            if fk == "ts":
                rows = await pool.fetch(f"""
                    SELECT {_ORDER_COLS}
                    FROM orders
                    WHERE strategy_id = $1
                      AND status IN ('filled', 'closed', 'partial')
                      AND created_at >= $2::timestamptz
                    ORDER BY created_at ASC""", STRATEGY_ID, fv)
            else:
                rows = await pool.fetch(f"""
                    SELECT {_ORDER_COLS}
                    FROM orders
                    WHERE strategy_id = $1
                      AND status IN ('filled', 'closed', 'partial')
                      AND created_at >= NOW() - $2::int * INTERVAL '1 day'
                    ORDER BY created_at ASC""", STRATEGY_ID, fv)
            orders = _rows(rows)
            entries = [o for o in orders if o["side"] == "buy" and not o["reduce_only"]]
            exits = [o for o in orders if o["side"] == "sell" and o["reduce_only"]]

            trips: list[dict] = []
            exit_idx = 0
            for entry in entries:
                while exit_idx < len(exits) and exits[exit_idx]["created_at"] <= entry["created_at"]:
                    exit_idx += 1
                ex = exits[exit_idx] if exit_idx < len(exits) else None
                if ex is not None:
                    reason = None
                    try:
                        lr = await pool.fetch("""
                            SELECT context->>'reason' AS reason
                            FROM service_logs
                            WHERE service = 'supertrend'
                              AND event = 'exit_order_submitted'
                              AND timestamp BETWEEN $1 - INTERVAL '5 minutes' AND $1 + INTERVAL '5 minutes'
                            ORDER BY timestamp DESC
                            LIMIT 1""", ex["updated_at"])
                        if lr:
                            reason = lr[0]["reason"]
                    except Exception:  # noqa: BLE001 - non-critical (table may not exist)
                        pass
                    entry_price = _pf(entry["filled_price"])
                    exit_price = _pf(ex["filled_price"])
                    qty = _pf(entry["filled_qty"])
                    fees = _pf(entry["fee"] or 0) + _pf(ex["fee"] or 0)
                    pnl = (exit_price - entry_price) * qty - fees
                    trips.append({
                        "entry_order_id": entry["order_id"], "exit_order_id": ex["order_id"],
                        "entry_time": entry["created_at"], "exit_time": ex["created_at"],
                        "entry_fill_time": entry["updated_at"], "exit_fill_time": ex["updated_at"],
                        "entry_price": entry_price, "exit_price": exit_price,
                        "qty": qty, "total_fees": fees, "realized_pnl": pnl, "exit_reason": reason,
                    })
                    exit_idx += 1
                else:
                    trips.append({
                        "entry_order_id": entry["order_id"], "exit_order_id": None,
                        "entry_time": entry["created_at"], "exit_time": None,
                        "entry_fill_time": entry["updated_at"], "exit_fill_time": None,
                        "entry_price": _pf(entry["filled_price"]), "exit_price": None,
                        "qty": _pf(entry["filled_qty"]),
                        "total_fees": _pf(entry["fee"] or 0),
                        "realized_pnl": None, "exit_reason": None, "is_open": True,
                    })
            return _json({"trades": trips, "count": len(trips)})
        except Exception as exc:  # noqa: BLE001
            log.warning("supertrend_actual_error", error=str(exc)[:300])
            return _error("Failed to fetch actual trades")

    async def compare(request: web.Request) -> web.Response:
        try:
            pool = pool_of()
            fk, fv = parse_time_param(_q(request, "from"), 30)
            if fk == "ts":
                exp_q = pool.fetch(f"{_EXP_COLS} AND bar_ts >= $1::timestamptz ORDER BY bar_ts ASC", fv)
            else:
                exp_q = pool.fetch(
                    f"{_EXP_COLS} AND bar_ts >= NOW() - $1::int * INTERVAL '1 day' ORDER BY bar_ts ASC", fv)
            act_q = pool.fetch("""
                SELECT id, request_id, side, filled_qty, filled_price, fee,
                       reduce_only, created_at, updated_at, original_signal_ts
                FROM orders
                WHERE strategy_id = $1
                  AND status IN ('filled', 'closed', 'partial', 'filled_delayed')
                ORDER BY created_at ASC""", STRATEGY_ID)
            exp_rows, act_rows = await asyncio.gather(exp_q, act_q)
            expected_rows = _rows(exp_rows)
            act_orders = _rows(act_rows)
            act_entries = [o for o in act_orders if o["side"] == "buy" and not o["reduce_only"]]
            act_exits = [o for o in act_orders if o["side"] == "sell" and o["reduce_only"]]

            compared: list[dict] = []
            entry_cur = 0
            exit_cur = 0
            for exp in expected_rows:
                exp_ts = _ms(exp["bar_ts"])
                bar_close = exp_ts + BAR_MS
                window_end = bar_close + BAR_MS
                exp_price = _pf(exp["expected_price"])
                if exp["expected_action"] == "enter":
                    while entry_cur < len(act_entries) and _ms(act_entries[entry_cur]["updated_at"]) < bar_close:
                        entry_cur += 1
                    fills = []
                    while entry_cur < len(act_entries):
                        act_ts = _ms(act_entries[entry_cur]["updated_at"])
                        if act_ts > window_end:
                            break
                        fills.append((act_entries[entry_cur], act_ts))
                        entry_cur += 1
                    exp_qty = _pf(exp["expected_qty"]) if _truthy(exp["expected_qty"]) else None
                    if not fills:
                        compared.append({
                            "bar_ts": exp["bar_ts"], "expected_action": "enter",
                            "expected_price": exp_price, "expected_qty": exp_qty,
                            "actual_price": None, "actual_qty": None, "actual_fill_time": None,
                            "timing_lag_ms": None, "slippage_pct": None, "qty_diff": None,
                            "status": "missed",
                        })
                    else:
                        for act, act_ts in fills:
                            ap = _pf(act["filled_price"])
                            compared.append({
                                "bar_ts": exp["bar_ts"], "expected_action": "enter",
                                "expected_price": exp_price, "expected_qty": exp_qty,
                                "actual_price": ap, "actual_qty": _pf(act["filled_qty"]),
                                "actual_fill_time": act["updated_at"],
                                "timing_lag_ms": act_ts - bar_close,
                                "slippage_pct": _div(ap - exp_price, exp_price) * 100,
                                "qty_diff": (_pf(act["filled_qty"]) - _pf(exp["expected_qty"])
                                             if _truthy(exp["expected_qty"]) else None),
                                "status": "matched",
                            })
                elif exp["expected_action"] == "exit":
                    if _truthy(exp["actual_exit_at"]) and _truthy(exp["actual_exit_price"]):
                        lag = _ms(exp["actual_exit_at"]) - bar_close
                        delayed = next((o for o in act_exits
                                        if o["original_signal_ts"] is not None
                                        and abs(_ms(o["original_signal_ts"]) - exp_ts) < 3_600_000), None)
                        aep = _pf(exp["actual_exit_price"])
                        compared.append({
                            "bar_ts": exp["bar_ts"], "expected_action": "exit",
                            "expected_exit_reason": exp["expected_exit_reason"],
                            "expected_price": exp_price, "expected_qty": None,
                            "actual_price": aep,
                            "actual_qty": _pf(delayed["filled_qty"]) if delayed else None,
                            "actual_fill_time": exp["actual_exit_at"],
                            "timing_lag_ms": lag,
                            "slippage_pct": _div(aep - exp_price, exp_price) * 100,
                            "delay_note": exp["delay_note"],
                            "status": "matched",
                        })
                    else:
                        while exit_cur < len(act_exits) and _ms(act_exits[exit_cur]["updated_at"]) < bar_close:
                            exit_cur += 1
                        fills = []
                        while exit_cur < len(act_exits):
                            act_ts = _ms(act_exits[exit_cur]["updated_at"])
                            if act_ts > window_end:
                                break
                            fills.append((act_exits[exit_cur], act_ts))
                            exit_cur += 1
                        if not fills:
                            compared.append({
                                "bar_ts": exp["bar_ts"], "expected_action": "exit",
                                "expected_exit_reason": exp["expected_exit_reason"],
                                "expected_price": exp_price, "expected_qty": None,
                                "actual_price": None, "actual_qty": None, "actual_fill_time": None,
                                "timing_lag_ms": None, "slippage_pct": None, "delay_note": None,
                                "status": "missed",
                            })
                        else:
                            for act, act_ts in fills:
                                ap = _pf(act["filled_price"])
                                compared.append({
                                    "bar_ts": exp["bar_ts"], "expected_action": "exit",
                                    "expected_exit_reason": exp["expected_exit_reason"],
                                    "expected_price": exp_price, "expected_qty": None,
                                    "actual_price": ap, "actual_qty": _pf(act["filled_qty"]),
                                    "actual_fill_time": act["updated_at"],
                                    "timing_lag_ms": act_ts - bar_close,
                                    "slippage_pct": _div(ap - exp_price, exp_price) * 100,
                                    "delay_note": None,
                                    "status": "matched",
                                })

            allr = sorted(compared, key=lambda r: _ms(r["bar_ts"]))
            slippages = [r["slippage_pct"] for r in allr if r["slippage_pct"] is not None]
            lags = [r["timing_lag_ms"] for r in allr if r["timing_lag_ms"] is not None]

            def mean(xs: list) -> float:
                if not xs:
                    return 0
                t = 0
                for x in xs:  # sequential, like reduce((a, b) => a + b)
                    t = t + x
                return _div(t, len(xs))

            return _json({
                "comparison": allr,
                "stats": {
                    "total": len(allr),
                    "matched": sum(1 for r in allr if r["status"] == "matched"),
                    "missed": sum(1 for r in allr if r["status"] == "missed"),
                    "avg_slippage_pct": mean(slippages),
                    "avg_lag_ms": mean(lags),
                },
            })
        except Exception as exc:  # noqa: BLE001
            log.warning("supertrend_compare_error", error=str(exc)[:300])
            return _error("Failed to compare signals")

    async def equity(request: web.Request) -> web.Response:
        try:
            pool = pool_of()
            fk, fv = parse_time_param(_first_truthy(_q(request, "from"), _q(request, "days")), 90)
            days = fv if fk == "days" else 90
            if fk == "ts":
                sig_q = pool.fetch("""
                    SELECT bar_ts, expected_action, expected_qty, price, allocated_capital
                    FROM supertrend_signals
                    WHERE bar_ts >= $1::timestamptz
                    ORDER BY bar_ts ASC""", fv)
                snap_q = pool.fetch("""
                    SELECT snapshot_at, total_equity
                    FROM portfolio_snapshots
                    WHERE snapshot_at >= $1::timestamptz
                    ORDER BY snapshot_at ASC""", fv)
            else:
                sig_q = pool.fetch("""
                    SELECT bar_ts, expected_action, expected_qty, price, allocated_capital
                    FROM supertrend_signals
                    WHERE bar_ts >= NOW() - $1::int * INTERVAL '1 day'
                    ORDER BY bar_ts ASC""", fv)
                snap_q = pool.fetch("""
                    SELECT snapshot_at, total_equity
                    FROM portfolio_snapshots
                    WHERE snapshot_at >= NOW() - $1::int * INTERVAL '1 day'
                    ORDER BY snapshot_at ASC""", fv)
            sig_rows, snap_rows = await asyncio.gather(sig_q, snap_q)
            signals = _rows(sig_rows)
            snaps = _rows(snap_rows)

            expected_equity = 200.0
            in_pos = False
            entry_price = 0.0
            pos_qty = 0.0
            curve: list[dict] = []
            for s in signals:
                if s["expected_action"] == "enter" and not in_pos:
                    in_pos = True
                    entry_price = _pf(s["price"])
                    pos_qty = _pf(s["expected_qty"]) if _truthy(s["expected_qty"]) else 0.0
                elif s["expected_action"] == "exit" and in_pos:
                    expected_equity += (_pf(s["price"]) - entry_price) * pos_qty
                    in_pos = False
                    pos_qty = 0.0
                curve.append({"ts": s["bar_ts"], "equity": _pf(_to_fixed2(expected_equity))})
            return _json({
                "expected": curve,
                "actual": [{"ts": r["snapshot_at"], "equity": _pf(r["total_equity"])} for r in snaps],
                "days": days,
            })
        except Exception as exc:  # noqa: BLE001
            log.warning("supertrend_equity_error", error=str(exc)[:300])
            return _error("Failed to fetch equity curves")

    async def status(_request: web.Request) -> web.Response:
        try:
            pool = pool_of()
            sig = _rows(await pool.fetch(_STATUS_COLS))
            strategy_status = await _call(prov.strategy_status, "strategy_status")
            live_position = None
            positions = await _call(prov.positions, "positions")
            if isinstance(positions, list):
                live_position = next((p for p in positions
                                      if p.get("symbol") in (SYMBOL, "BTCUSDT")), None)
            snap = await pool.fetch(
                "SELECT total_equity FROM portfolio_snapshots ORDER BY snapshot_at DESC LIMIT 1")
            total_equity = _pf(snap[0]["total_equity"]) if snap else None
            now = int(time.time() * 1000)
            next_bar = -(-now // BAR_MS) * BAR_MS
            return _json({
                "latest_signal": sig[0] if sig else None,
                "strategy_status": strategy_status,
                "live_position": live_position,
                "total_equity": total_equity,
                "next_bar_ts": iso_ms(datetime.fromtimestamp(next_bar / 1000, UTC)),
                "next_bar_eta_ms": next_bar - now,
            })
        except Exception as exc:  # noqa: BLE001
            log.warning("supertrend_status_error", error=str(exc)[:300])
            return _error("Failed to fetch status")

    async def candles(request: web.Request) -> web.Response:
        try:
            pool = pool_of()
            fk, fv = parse_time_param(_first_truthy(_q(request, "from"), _q(request, "days")), 90)
            cols = """SELECT timestamp AS ts,
              round(open::numeric,2)   AS open,  round(high::numeric,2)   AS high,
              round(low::numeric,2)    AS low,   round(close::numeric,2)  AS close,
              round(volume::numeric,4) AS volume
            FROM ohlcv_history
            WHERE exchange = 'bybit'
              AND symbol = 'BTCUSDT'
              AND timeframe = '4h'
              AND """
            if fk == "ts":
                rows = await pool.fetch(cols + "timestamp >= $1::timestamptz ORDER BY timestamp ASC", fv)
            else:
                rows = await pool.fetch(
                    cols + "timestamp >= NOW() - $1::int * INTERVAL '1 day' ORDER BY timestamp ASC", fv)
            rows = _rows(rows)
            return _json({"candles": rows, "count": len(rows)})
        except Exception as exc:  # noqa: BLE001
            log.warning("supertrend_candles_error", error=str(exc)[:300])
            return _error("Failed to fetch candles")

    async def in_progress(_request: web.Request) -> web.Response:
        try:
            bucket = int(time.time() * 1000) // BAR_MS * BAR_MS
            c = await _call(prov.in_progress_candle, "in_progress_candle")
            if not c or int(c.get("ts", 0)) != bucket:
                return _json({"candle": None, "source": "empty"})
            return _json({"candle": {
                "ts": bucket,
                "open": _pf(c.get("open")), "high": _pf(c.get("high")),
                "low": _pf(c.get("low")), "close": _pf(c.get("close")),
                "volume": _pf(c.get("volume") or 0),
                "confirmed": bool(c.get("confirmed")),
            }, "source": "exchange"})
        except Exception as exc:  # noqa: BLE001
            log.warning("supertrend_in_progress_error", error=str(exc)[:300])
            return _error("Failed to fetch in-progress candle")

    for path, handler in (("expected", expected), ("actual", actual), ("compare", compare),
                          ("equity", equity), ("status", status), ("candles", candles),
                          ("candles/in-progress", in_progress)):
        app.router.add_get(API_PREFIX + path, handler)
