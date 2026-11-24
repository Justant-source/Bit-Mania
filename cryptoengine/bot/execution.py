"""Order executor: post-only re-peg state machine, market fallback, orphan cleanup.

Ported from ``services/execution/order_manager.py`` (state machine, re-peg,
harvest, combined results, orphan restore), ``engine.py`` (safety gate before
execution, result persistence, realized-PnL helper, stop handling),
``position_tracker.py`` (the exchange position read only) and
``stoploss_manager.py`` (reduced to "no stale stop survives an exit").

Not ported: Redis pub/sub and caches, polling loops, the ``positions`` table
(another module owns it), trade-alert publishing, ``modify_order``, the
standalone StopLossManager (entries always carry the inline stop loss).

Stop loss on exit: entries send ``stopLoss`` inline (``params.stopLoss``), which
ccxt maps to the Bybit v5 ``order/create`` ``stopLoss`` field. That is a
*position-level* stop (tpslMode Full), not a separate order in the open-orders
list. Bybit's documented behaviour is that position-level TP/SL is cleared when
the position is fully closed; this could not be verified against the real
exchange here. As a belt-and-braces measure, after a reduce-only fill that
leaves the position flat the executor calls ``exchange.cancel_conditional_orders``
(when the exchange offers it) to cancel any lingering conditional order. The old
StopLossManager.cancel_stop_loss was a no-op in production for the same reason
(it only knew standalone stops, which were never placed).

Ambiguous placement failures: a ``create_order`` that timed out (or failed in an unknown way)
may still have reached the exchange. ``BybitConnector.last_error_ambiguous`` / ``is_ambiguous_error``
tell it from a definite exchange rejection. After an ambiguous failure the executor cancels all open
orders (``cancel_open_orders``), re-reads the position and compares it with the size recorded at the
start of ``execute()``: a fill nobody tracked is adopted (``untracked_fill_adopted``); if the position
cannot be read the order is abandoned with reason ``ambiguous_order_state`` (the Trader re-syncs from
the exchange after any non-filled outcome). Exchange truth, not API details, decides.

Result statuses: ``filled`` (a remainder below the minimum step is dropped and
still counts as filled), ``partial``, ``cancelled``, ``rejected``.
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import structlog

from bot.exchange import PostOnlyRejected, is_ambiguous_error
from bot.interfaces import EXCHANGE, STRATEGY_ID, SYMBOL, ExecutionResult
from shared.models.order import OrderRequest, OrderResult
from shared.models.position import Position

log = structlog.get_logger(__name__)

# Re-peg parameters (module level so tests can patch them).
REPEG_MAX_ATTEMPTS = 20
REPEG_INTERVAL_S = 10.0
REPEG_POLL_S = 0.5
REPEG_ERROR_SLEEP_S = 1.0

# Worst case 20 x 10s re-pegs + API latency + market fallback.
ORDER_TIMEOUT = 420.0

# Market order submission retry (only for exceptions raised by the connector).
MAX_RETRIES = 3
RETRY_BACKOFF_S = 0.5
_NON_RETRYABLE = ("AuthenticationError", "InsufficientFunds", "InvalidOrder", "BadSymbol", "PermissionDenied")

# Wait for a market order that did not come back terminal.
SETTLE_POLLS = 5
SETTLE_POLL_S = 0.5

# close_position
CLOSE_MAX_ORDERS = 3
CLOSE_RETRY_PAUSE_S = 1.0
CLOSE_READ_FAILURES = 3
ABORT_WAIT_S = 15.0

# Final order-row write: upsert retried PERSIST_TRIES times (sleeps PERSIST_BACKOFF_S, then twice that),
# ~3 s in total; a write that still fails is kept in memory and retried on the next executor activity.
PERSIST_TRIES = 3
PERSIST_BACKOFF_S = 1.0

FALLBACK_MIN_QTY = 0.001
FALLBACK_QTY_STEP = 0.001

_TERMINAL_DB = ("filled", "cancelled", "rejected", "expired")
_DB_STATUS = {"filled": "filled", "partial": "partially_filled", "cancelled": "cancelled", "rejected": "rejected"}
_FROM_DB = {"filled": "filled", "partially_filled": "partial", "cancelled": "cancelled",
            "expired": "cancelled", "rejected": "rejected"}

_RESULTS_MAX = 5_000


def floor_to_step(qty: float, step: float) -> float:
    """Floor ``qty`` to the exchange step (never negative)."""
    if qty <= 0:
        return 0.0
    if step <= 0:
        return qty
    return max(math.floor((qty + 1e-12) / step) * step, 0.0)


def compute_realized_pnl(position: Position, exit_price: float, qty: float, fee: float) -> tuple[float, float]:
    """``(realized_usd, realized_pct)``: gross PnL minus the exit fee only (entry fee not included)."""
    sign = 1.0 if position.side == "long" else -1.0
    gross = sign * (exit_price - position.entry_price) * qty
    pct = (gross / (position.entry_price * qty) * 100.0) if position.entry_price and qty else 0.0
    return gross - fee, pct


def format_hold_duration(opened_at: datetime | None, now: datetime) -> str | None:
    """``"3d 4h"`` / ``"2h 15m"`` / ``"45m"``; None when ``opened_at`` is unknown."""
    if opened_at is None:
        return None
    total_min = max(int((now - opened_at).total_seconds() // 60), 0)
    days, rem = divmod(total_min, 24 * 60)
    hours, minutes = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


@dataclass
class _Acc:
    """Running fill totals for one request (across re-peg attempts and the fallback)."""

    qty: float = 0.0
    notional: float = 0.0
    fee: float = 0.0
    order_ids: list[str] = field(default_factory=list)
    last_order_id: str = ""  # live (unharvested) limit order, "" when none
    last_price: float = 0.0
    target: float = 0.0  # quantity of the whole request
    baseline: float | None = None  # signed position size at the start of execute(); None = unknown
    abort: bool = False
    done: asyncio.Event = field(default_factory=asyncio.Event)

    def add(self, qty: float, price: float | None, fee: float) -> None:
        if qty <= 0:
            return
        self.qty += qty
        self.notional += qty * float(price or self.last_price or 0.0)
        self.fee += fee

    @property
    def avg_price(self) -> float | None:
        return self.notional / self.qty if self.qty > 0 else None


class OrderExecutor:
    """Implements ``bot.interfaces.Executor``."""

    def __init__(self, exchange: Any, db: Any, safety: Any, *, dry_run: bool = False) -> None:
        self.exchange = exchange
        self._db = db
        self._safety = safety
        self.dry_run = dry_run
        self._qty_limits: dict[str, tuple[float, float]] = {}
        self._results: dict[str, ExecutionResult] = {}
        self._pending: dict[str, asyncio.Future[ExecutionResult]] = {}
        self._inflight: dict[str, _Acc] = {}
        # Final order rows that could not be written (DB down): request_id -> upsert arguments.
        self._unwritten: dict[str, tuple[Any, ...]] = {}

    # ── lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Cancel orphaned open orders from a previous run.

        A restart kills any re-peg loop, so an order still resting on the
        exchange has no owner: if it filled later it would create a position
        outside the strategy's view. Rows left non-terminal in ``orders`` are
        resolved against the exchange (cancel if open and harvest what it
        filled; record the fill if it closed meanwhile; reject if it never
        reached the exchange).
        """
        try:
            async with self._db.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT request_id, order_id, status, symbol FROM orders
                    WHERE exchange = $1 AND status NOT IN ('filled','cancelled','rejected','expired')
                    ORDER BY created_at DESC LIMIT 1000
                    """,
                    EXCHANGE,
                )
        except Exception:
            log.exception("orphan_cleanup_query_failed")
            return
        log.info("inflight_orders_found", count=len(rows))
        for row in rows:
            rid, oid, symbol = row["request_id"], row["order_id"], row["symbol"]
            if not oid:
                # Never got an order id: it may still have reached the exchange and filled. We do
                # not invent fills; the operator must compare with the exchange position.
                await self._update_status(rid, "rejected")
                pos_size: float | None
                try:
                    pos = await self.exchange.get_position(SYMBOL)
                    pos_size = pos.size if pos is not None else 0.0
                except Exception:
                    pos_size = None
                log.error("orphan_pending_marked_rejected_check_exchange", request_id=rid,
                          status=row["status"], exchange_position_size=pos_size)
                continue
            try:
                status = await self.exchange.fetch_order_status(oid, symbol)
                if status == "open":
                    await self.exchange.cancel_order(oid, symbol)
                    res = await self.exchange.fetch_order_result(oid, symbol, rid)
                    final = "partially_filled" if res.filled_qty > 0 else "cancelled"
                    await self._update_from_values(rid, oid, final, res.filled_qty, res.filled_price, res.fee)
                    log.error("orphan_order_cancelled", request_id=rid, order_id=oid,
                              filled_qty=res.filled_qty)
                elif status == "closed":
                    res = await self.exchange.fetch_order_result(oid, symbol, rid)
                    await self._update_from_values(rid, oid, "filled", res.filled_qty, res.filled_price, res.fee)
                    log.warning("orphan_order_filled_during_downtime", request_id=rid, order_id=oid,
                                filled_qty=res.filled_qty)
                elif status in ("canceled", "expired", "rejected"):
                    res = await self.exchange.fetch_order_result(oid, symbol, rid)
                    final = "partially_filled" if res.filled_qty > 0 else (
                        "rejected" if status == "rejected" else "cancelled")
                    await self._update_from_values(rid, oid, final, res.filled_qty, res.filled_price, res.fee)
                else:
                    log.warning("orphan_order_status_unknown", request_id=rid, order_id=oid, status=status)
            except Exception:
                log.exception("orphan_cleanup_failed", request_id=rid, order_id=oid)

    # ── reads ────────────────────────────────────────────────────────────

    async def get_position(self) -> Position | None:
        await self._flush_unwritten()
        pos = await self.exchange.get_position(SYMBOL)
        if pos is None or pos.size <= 0:
            return None
        return pos

    async def get_balance(self) -> dict[str, float]:
        """``{"total", "free", "wallet", ...}``; ``wallet`` falls back to ``total``.

        Both are the Bybit wallet balance (unrealized PnL excluded), see
        ``BybitConnector.get_balance``.
        """
        await self._flush_unwritten()
        bal = dict(await self.exchange.get_balance())
        bal.setdefault("wallet", bal.get("total", 0.0))
        return bal

    # ── execute ──────────────────────────────────────────────────────────

    async def execute(self, order: OrderRequest, *, signal_ts: datetime | None = None) -> ExecutionResult:
        rid = order.request_id
        if self.dry_run:
            log.warning("dry_run_order_not_sent", request_id=rid, side=order.side, qty=order.quantity,
                        reduce_only=order.reduce_only)
            return ExecutionResult(request_id=rid, status="rejected", reason="dry_run")

        await self._flush_unwritten()
        # Idempotency: same request_id never sends a second order.
        if rid in self._results:
            return self._results[rid]
        pending = self._pending.get(rid)
        if pending is not None:
            return await asyncio.shield(pending)
        # Register before any await so a concurrent duplicate finds it.
        fut: asyncio.Future[ExecutionResult] = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        try:
            row = await self._fetch_row(rid)
            if row is not None:  # sent by an earlier run: report it, never resend
                result = self._result_from_row(row)
            else:
                result = await self._execute_new(order, signal_ts)
        except asyncio.CancelledError:
            fut.set_result(ExecutionResult(request_id=rid, status="cancelled", reason="caller_cancelled"))
            raise
        except Exception as exc:
            log.exception("execution_error", request_id=rid)
            result = ExecutionResult(request_id=rid, status="rejected", reason=f"execution_error: {exc}")
            await self._persist_result(order, result, signal_ts=signal_ts)
        finally:
            self._pending.pop(rid, None)
        self._remember(rid, result)
        if not fut.done():
            fut.set_result(result)
        return result

    async def _execute_new(self, order: OrderRequest, signal_ts: datetime | None) -> ExecutionResult:
        rid = order.request_id
        await self._persist_new(order, signal_ts)

        try:
            ok, reason = await self._safety.check_order(order)
        except Exception:
            log.exception("safety_check_error", request_id=rid)
            ok, reason = False, "safety_check_internal_error"
        if not ok:
            result = ExecutionResult(request_id=rid, status="rejected", reason=reason)
            await self._persist_result(order, result, signal_ts=signal_ts)
            return result
        self._safety.record_api_call()

        acc = _Acc(target=order.quantity)
        acc.baseline = await self._read_signed_position()
        if acc.baseline is None:
            log.warning("position_baseline_unknown", request_id=rid)
        if not order.reduce_only:
            self._inflight[rid] = acc
        try:
            try:
                leg = await asyncio.wait_for(self._work(order, acc), ORDER_TIMEOUT)
            except asyncio.TimeoutError:
                log.error("order_timeout", request_id=rid, timeout_s=ORDER_TIMEOUT, filled_qty=acc.qty)
                leg = ("cancelled", "order_timeout")
            result = await self._build_result(order, acc, *leg)
        finally:
            self._inflight.pop(rid, None)
            acc.done.set()
        await self._persist_result(order, result, acc, signal_ts=signal_ts)
        if order.reduce_only and result.any_fill:
            await self._after_exit()
        if result.status in ("rejected", "partial"):
            log.warning("order_not_filled", request_id=rid, status=result.status, reason=result.reason,
                        filled_qty=result.filled_qty, requested=order.quantity)
        return result

    # ── order work ───────────────────────────────────────────────────────

    async def _work(self, order: OrderRequest, acc: _Acc) -> tuple[str, str]:
        """Run the order; returns the last leg's ``(status, reason)``. Fills go to ``acc``."""
        if order.order_type == "market":
            return await self._market_leg(order, acc)
        return await self._repeg(order, acc)

    async def _repeg(self, order: OrderRequest, acc: _Acc) -> tuple[str, str]:
        """Limit order pegged to best bid/ask, re-pegged every REPEG_INTERVAL_S.

        Up to REPEG_MAX_ATTEMPTS attempts, each for the *remaining* quantity
        (never the full size again: re-sending the full size after a partial
        fill would over-fill). Then one market order for the remainder.
        Cancellation (timeout / shutdown) cancels the live order and records
        what it filled before re-raising.
        """
        rid = order.request_id
        min_qty, step = await self._get_qty_limits(order.symbol)
        leg: tuple[str, str] = ("cancelled", "repeg_exhausted")

        try:
            for attempt in range(1, REPEG_MAX_ATTEMPTS + 1):
                if acc.last_order_id:
                    await self._harvest_into(acc, order)
                if acc.abort:
                    return ("cancelled", "aborted")

                remaining = floor_to_step(order.quantity - acc.qty, step)
                if remaining < min_qty:
                    return ("filled", "")

                try:
                    book = await self.exchange.get_orderbook(order.symbol, limit=1)
                    levels = book.bids if order.side == "buy" else book.asks
                    peg = levels[0].price if levels else 0.0
                    peg = self.exchange.price_to_precision(order.symbol, peg)
                    if peg <= 0:
                        raise ValueError("empty order book")
                except Exception as exc:
                    log.warning("peg_price_failed", request_id=rid, attempt=attempt, error=str(exc)[:200])
                    await asyncio.sleep(REPEG_ERROR_SLEEP_S)
                    continue

                acc.last_price = peg
                attempt_order = order.model_copy(update={"price": peg, "quantity": remaining})
                log.info("limit_repeg_attempt", request_id=rid, attempt=attempt, price=peg,
                         quantity=remaining, cum_filled=acc.qty)

                try:
                    result: OrderResult = await self.exchange.place_order(attempt_order)
                except PostOnlyRejected:
                    log.info("postonly_rejected_repeg_now", request_id=rid, attempt=attempt, price=peg)
                    continue  # immediate retry, no wait
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if "InsufficientFunds" in type(exc).__name__:
                        log.error("repeg_insufficient_funds", request_id=rid, attempt=attempt)
                        leg = ("rejected", f"insufficient_funds: {str(exc)[:200]}")
                        break
                    log.warning("repeg_attempt_exception", request_id=rid, attempt=attempt, error=str(exc)[:200])
                    if is_ambiguous_error(exc):
                        if not await self._resolve_ambiguous(order, acc, f"{type(exc).__name__}: {exc}"):
                            return self._ambiguous_stop(acc)
                    await asyncio.sleep(REPEG_ERROR_SLEEP_S)
                    continue

                if result.status == "rejected" and not result.order_id:
                    err = getattr(self.exchange, "last_error", "") or "exchange_rejected"
                    log.warning("repeg_attempt_rejected", request_id=rid, attempt=attempt, error=err)
                    if "insufficient" in err.lower() or "110007" in err:
                        leg = ("rejected", f"insufficient_funds: {err[:200]}")
                        break
                    leg = ("rejected", err)
                    if getattr(self.exchange, "last_error_ambiguous", True):
                        if not await self._resolve_ambiguous(order, acc, err):
                            return self._ambiguous_stop(acc)
                    await asyncio.sleep(REPEG_ERROR_SLEEP_S)
                    continue

                if result.order_id:
                    acc.last_order_id = result.order_id
                    acc.order_ids.append(result.order_id)
                    await self._update_from_values(rid, result.order_id, "submitted", acc.qty,
                                                   acc.avg_price, acc.fee)

                if result.status == "filled":
                    acc.add(result.filled_qty or remaining, result.filled_price or peg, result.fee)
                    acc.last_order_id = ""
                    return ("filled", "")

                # Poll for a fill during REPEG_INTERVAL_S.
                poll_start = time.monotonic()
                while time.monotonic() - poll_start < REPEG_INTERVAL_S:
                    await asyncio.sleep(REPEG_POLL_S)
                    if acc.abort or not acc.last_order_id:
                        break
                    try:
                        polled = await self.exchange.fetch_order_result(acc.last_order_id, order.symbol, rid)
                    except Exception:
                        continue
                    if polled.status == "filled":
                        acc.add(polled.filled_qty or remaining, polled.filled_price or peg, polled.fee)
                        acc.last_order_id = ""
                        log.info("limit_order_filled_during_poll", request_id=rid, attempt=attempt)
                        return ("filled", "")
                    if polled.status in ("cancelled", "expired", "rejected"):
                        break  # dead order: harvest and re-peg now

            # Attempts exhausted (or aborted by an error): cancel remainder, harvest, market fallback.
            if acc.last_order_id:
                await self._harvest_into(acc, order)
            if acc.abort:
                return ("cancelled", "aborted")
            remaining = floor_to_step(order.quantity - acc.qty, step)
            if remaining < min_qty:
                return ("filled", "")
            log.warning("limit_repeg_exhausted_market_fallback", request_id=rid,
                        remaining_qty=remaining, cum_filled=acc.qty)
            fallback = order.model_copy(
                update={"order_type": "market", "price": None, "post_only": False, "quantity": remaining}
            )
            leg = await self._market_leg(fallback, acc)
            return leg
        except asyncio.CancelledError:
            if acc.last_order_id:
                try:
                    await asyncio.wait_for(asyncio.shield(self._harvest_into(acc, order)), timeout=10.0)
                    log.warning("repeg_cancelled_outstanding_order_cleaned", request_id=rid)
                except Exception:
                    log.error("repeg_cancel_cleanup_failed_check_exchange", request_id=rid,
                              order_id=acc.last_order_id)
            raise

    async def _market_leg(self, order: OrderRequest, acc: _Acc) -> tuple[str, str]:
        """Submit one market order (with retry on raised exceptions) and settle it."""
        rid = order.request_id
        if not acc.target:
            acc.target = order.quantity
        min_qty, step = await self._get_qty_limits(order.symbol)
        result: OrderResult | None = None
        last_error = ""
        for attempt in range(1, MAX_RETRIES + 1):
            ambiguous = False
            try:
                result = await self.exchange.place_order(order)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                result = None
                last_error = str(exc)
                exc_type = type(exc).__name__
                if any(k in exc_type for k in _NON_RETRYABLE):
                    log.error("order_non_retryable_error", request_id=rid, error_type=exc_type, error=last_error)
                    break
                log.warning("order_attempt_failed", request_id=rid, attempt=attempt, error=last_error)
                ambiguous = is_ambiguous_error(exc)
                if ambiguous:
                    last_error = f"{exc_type}: {last_error}"
            else:
                if result.status == "rejected" and not result.order_id:
                    if not getattr(self.exchange, "last_error_ambiguous", True):
                        return ("rejected", getattr(self.exchange, "last_error", "") or "exchange_rejected")
                    last_error = getattr(self.exchange, "last_error", "") or "ambiguous_rejection"
                    log.warning("order_attempt_failed", request_id=rid, attempt=attempt, error=last_error)
                    ambiguous = True
                    result = None
                else:
                    break
            if ambiguous:
                if not await self._resolve_ambiguous(order, acc, last_error):
                    return self._ambiguous_stop(acc)
                remaining = floor_to_step(acc.target - acc.qty, step)
                if remaining < min_qty:
                    return ("filled", "")
                if remaining < order.quantity - 1e-12:
                    order = order.model_copy(update={"quantity": remaining})
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_S * (2 ** (attempt - 1)))
        if result is None:
            return ("rejected", f"max_retries_exhausted: {last_error}")

        if result.order_id:
            acc.order_ids.append(result.order_id)
        if result.status == "rejected":
            return ("rejected", getattr(self.exchange, "last_error", "") or "exchange_rejected")

        result = await self._settle(result, order)
        if result.filled_qty > 0:
            acc.add(result.filled_qty, result.filled_price, result.fee)
        if result.status == "filled" and result.filled_qty > 0:
            return ("filled", "")
        if result.filled_qty > 0:
            return ("partial", f"market_order_{result.status}")
        if result.status == "rejected":
            return ("rejected", "market_order_rejected")
        return ("cancelled", f"market_order_{result.status}")

    # ── ambiguous placement failures ─────────────────────────────────────

    @staticmethod
    def _signed_size(pos: Position | None) -> float:
        if pos is None or pos.size <= 0:
            return 0.0
        return pos.size if pos.side == "long" else -pos.size

    async def _read_signed_position(self) -> float | None:
        """Signed exchange position size (long +, short -, flat 0); None when it cannot be read."""
        try:
            return self._signed_size(await self.exchange.get_position(SYMBOL))
        except Exception as exc:
            log.warning("position_read_failed", error=str(exc)[:200])
            return None

    @staticmethod
    def _ambiguous_stop(acc: _Acc) -> tuple[str, str]:
        """Leg result when the order state on the exchange could not be established."""
        return ("partial" if acc.qty > 0 else "rejected", "ambiguous_order_state")

    async def _resolve_ambiguous(self, order: OrderRequest, acc: _Acc, error: str) -> bool:
        """After an ambiguous placement failure: cancel what may rest, adopt what may have filled.

        Returns True when the order state is known again (``acc`` then matches the exchange
        position and the caller may continue), False when the caller must stop working the order.
        """
        rid = order.request_id
        log.error("ambiguous_order_failure", request_id=rid, error=error[:200], cum_filled=acc.qty)
        cancel = getattr(self.exchange, "cancel_open_orders", None)
        if cancel is None:
            log.error("ambiguous_cleanup_unavailable", request_id=rid)
            return False
        try:
            n = await cancel(SYMBOL)
        except Exception as exc:
            log.error("ambiguous_cancel_failed", request_id=rid, error=str(exc)[:200])
            return False
        if n:
            log.error("ambiguous_order_cancelled", request_id=rid, count=n)

        if acc.baseline is None:
            log.error("ambiguous_baseline_unknown", request_id=rid)
            return False
        try:
            pos = await self.exchange.get_position(SYMBOL)
        except Exception as exc:
            log.error("ambiguous_position_read_failed", request_id=rid, error=str(exc)[:200])
            return False
        now_size = self._signed_size(pos)
        delta = now_size - acc.baseline if order.side == "buy" else acc.baseline - now_size
        untracked = delta - acc.qty
        if untracked > 1e-9:
            price = acc.last_price or 0.0
            if price <= 0:
                try:
                    price = float((await self.exchange.get_ticker(order.symbol)).get("last") or 0.0)
                except Exception:
                    price = 0.0
            if price <= 0 and pos is not None:
                price = float(pos.entry_price or 0.0)
            acc.add(untracked, price, 0.0)
            log.error("untracked_fill_adopted", request_id=rid, qty=untracked, price=price,
                      cum_qty=acc.qty, baseline=acc.baseline, position_now=now_size)
        return True

    async def _settle(self, result: OrderResult, order: OrderRequest) -> OrderResult:
        """Market orders fill at once; if the first answer is not terminal, poll briefly."""
        if result.is_terminal or not result.order_id:
            return result
        for _ in range(SETTLE_POLLS):
            await asyncio.sleep(SETTLE_POLL_S)
            try:
                polled = await self.exchange.fetch_order_result(result.order_id, order.symbol, order.request_id)
            except Exception:
                continue
            if polled.is_terminal:
                return polled
            result = polled
        # Still not terminal: stop it from resting and read the final fill.
        log.error("market_order_unsettled", request_id=order.request_id, order_id=result.order_id)
        try:
            await self.exchange.cancel_order(result.order_id, order.symbol)
            return await self.exchange.fetch_order_result(result.order_id, order.symbol, order.request_id)
        except Exception:
            return result

    async def _harvest_into(self, acc: _Acc, order: OrderRequest) -> None:
        """Cancel the live limit order and add whatever it filled to ``acc``."""
        oid, acc.last_order_id = acc.last_order_id, ""
        result = await self._harvest_order(oid, order.symbol, order.request_id)
        if result is not None and result.filled_qty > 0:
            acc.add(result.filled_qty, result.filled_price, result.fee)
            log.info("repeg_partial_fill_harvested", request_id=order.request_id,
                     harvested_qty=result.filled_qty, cum_qty=acc.qty)

    async def _harvest_order(self, order_id: str, symbol: str, request_id: str) -> OrderResult | None:
        """Cancel an order and read its fill. The fill is read even when the cancel
        fails (already fully filled). The connector answers ``new`` with zero fill
        when a query fails, so that signature is re-queried once to avoid missing
        a partial fill (which would lead to over-filling)."""
        try:
            await self.exchange.cancel_order(order_id, symbol)
        except Exception as exc:
            log.warning("repeg_cancel_error_continuing", order_id=order_id, error=str(exc)[:200])

        result: OrderResult | None = None
        for fetch_attempt in (1, 2):
            try:
                result = await self.exchange.fetch_order_result(order_id, symbol, request_id)
            except Exception as exc:
                log.warning("harvest_fetch_error", order_id=order_id, fetch_attempt=fetch_attempt,
                            error=str(exc)[:200])
                result = None
            if result is not None and not (result.status == "new" and result.filled_qty == 0):
                return result
            await asyncio.sleep(0.3)
        if result is None:
            log.error("harvest_failed_partial_fill_may_be_missed", order_id=order_id, request_id=request_id)
        return result

    async def _get_qty_limits(self, symbol: str) -> tuple[float, float]:
        cached = self._qty_limits.get(symbol)
        if cached:
            return cached
        try:
            meta = (await self.exchange.get_min_order_sizes([symbol])).get(symbol, {})
            limits = (float(meta.get("min_qty") or FALLBACK_MIN_QTY),
                      float(meta.get("qty_step") or FALLBACK_QTY_STEP))
        except Exception as exc:
            log.warning("min_order_size_lookup_failed", symbol=symbol, error=str(exc)[:200])
            limits = (FALLBACK_MIN_QTY, FALLBACK_QTY_STEP)
        self._qty_limits[symbol] = limits
        return limits

    # ── result building ──────────────────────────────────────────────────

    async def _build_result(self, order: OrderRequest, acc: _Acc, leg_status: str, leg_reason: str) -> ExecutionResult:
        """Combine all legs. A remainder below the minimum step counts as filled."""
        min_qty, step = await self._get_qty_limits(order.symbol)
        rid = order.request_id
        if acc.qty > 0:
            remaining = floor_to_step(order.quantity - acc.qty, step)
            if remaining < min_qty:
                status, reason = "filled", ""
            else:
                status = "partial"
                reason = leg_reason or "remainder_unfilled"
        elif leg_status == "filled":
            # Remainder below min step and nothing filled: do not report a fill.
            status, reason = "rejected", "repeg_no_fill_below_min_qty"
        else:
            status, reason = leg_status, leg_reason
        return ExecutionResult(
            request_id=rid, status=status, filled_qty=round(acc.qty, 12), filled_price=acc.avg_price,
            fee=acc.fee, reason=reason, order_ids=list(acc.order_ids),
        )

    # ── close position ───────────────────────────────────────────────────

    async def close_position(self, reason: str) -> ExecutionResult | None:
        """Close the whole position with reduce-only market orders.

        First aborts any entry order still being worked (so it cannot fill after
        the close), then: read position, send a reduce-only market for its full
        size, re-read; at most CLOSE_MAX_ORDERS orders. Returns None when the
        exchange was already flat.
        """
        await self._abort_entries()
        sent = 0
        read_failures = 0
        results: list[ExecutionResult] = []
        flat = False
        while True:
            try:
                pos = await self.get_position()
                read_failures = 0
            except Exception as exc:
                read_failures += 1
                log.warning("close_position_read_failed", error=str(exc)[:200], n=read_failures)
                if read_failures >= CLOSE_READ_FAILURES:
                    return self._combine(results, flat=False, fallback_reason="position_read_failed")
                await asyncio.sleep(CLOSE_RETRY_PAUSE_S)
                continue
            if pos is None:
                flat = True
                break
            if sent >= CLOSE_MAX_ORDERS:
                break
            if sent > 0:
                await asyncio.sleep(CLOSE_RETRY_PAUSE_S)
            order = OrderRequest(
                strategy_id=STRATEGY_ID, exchange=EXCHANGE, symbol=SYMBOL,
                side="sell" if pos.side == "long" else "buy",
                order_type="market", quantity=pos.size, post_only=False, reduce_only=True,
            )
            log.warning("close_position_order", reason=reason, side=order.side, qty=order.quantity, n=sent + 1)
            results.append(await self.execute(order))
            sent += 1
            if self.dry_run:
                break

        if not results:
            return None if flat else self._combine(results, flat=False, fallback_reason="position_not_flat")
        return self._combine(results, flat=flat, fallback_reason="position_not_flat")

    @staticmethod
    def _combine(results: list[ExecutionResult], *, flat: bool, fallback_reason: str) -> ExecutionResult:
        rid = results[-1].request_id if results else "close"
        qty = sum(r.filled_qty for r in results)
        notional = sum(r.filled_qty * (r.filled_price or 0.0) for r in results)
        fee = sum(r.fee for r in results)
        ids = [i for r in results for i in r.order_ids]
        price = notional / qty if qty > 0 else None
        last_reason = results[-1].reason if results else ""
        if flat and qty > 0:
            status, reason = "filled", ""
        elif flat:
            status, reason = "cancelled", "flat_before_fill"
        elif qty > 0:
            status, reason = "partial", last_reason or fallback_reason
        else:
            status, reason = "rejected", last_reason or fallback_reason
        return ExecutionResult(request_id=rid, status=status, filled_qty=round(qty, 12),
                               filled_price=price, fee=fee, reason=reason, order_ids=ids)

    async def _abort_entries(self) -> None:
        """Stop re-pegging entry orders that are in flight and wait for their cleanup."""
        live = [a for a in self._inflight.values()]
        if not live:
            return
        log.warning("aborting_inflight_entries", count=len(live))
        for acc in live:
            acc.abort = True
        for acc in live:
            try:
                await asyncio.wait_for(acc.done.wait(), ABORT_WAIT_S)
            except asyncio.TimeoutError:
                log.error("abort_entry_wait_timeout")

    async def _after_exit(self) -> None:
        """After an exit: if flat, make sure no conditional (stop) order lingers."""
        try:
            if await self.get_position() is not None:
                return
            cancel = getattr(self.exchange, "cancel_conditional_orders", None)
            if cancel is not None:
                n = await cancel(SYMBOL)
                if n:
                    log.warning("stale_stop_orders_cancelled", count=n)
        except Exception:
            log.warning("stale_stop_cleanup_failed", exc_info=True)

    # ── persistence (never raises) ───────────────────────────────────────

    def _remember(self, rid: str, result: ExecutionResult) -> None:
        self._results[rid] = result
        if len(self._results) > _RESULTS_MAX:
            for k in list(self._results)[: _RESULTS_MAX // 2]:
                del self._results[k]

    async def _persist_new(self, order: OrderRequest, signal_ts: datetime | None) -> None:
        try:
            async with self._db.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO orders (request_id, exchange, symbol, side, order_type,
                                        quantity, price, status, strategy_id, post_only, reduce_only)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',$8,$9,$10)
                    ON CONFLICT (request_id) DO NOTHING
                    """,
                    order.request_id, order.exchange, order.symbol, order.side, order.order_type,
                    order.quantity, order.price, order.strategy_id, order.post_only, order.reduce_only,
                )
        except Exception:
            log.exception("persist_new_order_failed", request_id=order.request_id)
            return
        if signal_ts is not None:
            try:
                async with self._db.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE orders SET original_signal_ts = $2 WHERE request_id = $1",
                        order.request_id, signal_ts,
                    )
            except Exception:
                log.warning("persist_signal_ts_failed", request_id=order.request_id, exc_info=True)

    async def _persist_result(self, order: OrderRequest, result: ExecutionResult, acc: _Acc | None = None,
                              *, signal_ts: datetime | None = None) -> None:
        """Write the final result as an upsert on request_id (recreates the row when the INSERT was lost)."""
        oid = result.order_ids[-1] if result.order_ids else ""
        args = (order, signal_ts, oid, _DB_STATUS[result.status], result.filled_qty, result.filled_price, result.fee)
        for attempt in range(1, PERSIST_TRIES + 1):
            try:
                await self._upsert_result(*args)
                self._unwritten.pop(order.request_id, None)
                return
            except Exception as exc:
                log.warning("persist_result_failed", request_id=order.request_id, attempt=attempt,
                            error=repr(exc)[:200])
                if attempt < PERSIST_TRIES:
                    await asyncio.sleep(PERSIST_BACKOFF_S * attempt)
        self._unwritten[order.request_id] = args
        log.error("persist_result_gave_up_will_retry_later", request_id=order.request_id,
                  status=result.status, filled_qty=result.filled_qty)

    async def _flush_unwritten(self) -> None:
        """Retry final order rows that could not be written earlier (one attempt each, never raises)."""
        for rid, args in list(self._unwritten.items()):
            try:
                await self._upsert_result(*args)
            except Exception:
                return  # DB still down: try again at the next call
            self._unwritten.pop(rid, None)
            log.warning("persist_result_recovered", request_id=rid)

    async def _upsert_result(self, order: OrderRequest, signal_ts: datetime | None, order_id: str, status: str,
                             filled_qty: float, filled_price: float | None, fee: float) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO orders (request_id, order_id, exchange, symbol, side, order_type, quantity, price,
                                    status, filled_qty, filled_price, fee, strategy_id, post_only, reduce_only,
                                    original_signal_ts)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                ON CONFLICT (request_id) DO UPDATE SET
                    order_id = EXCLUDED.order_id, status = EXCLUDED.status,
                    filled_qty = EXCLUDED.filled_qty, filled_price = EXCLUDED.filled_price,
                    fee = EXCLUDED.fee, updated_at = NOW(),
                    original_signal_ts = COALESCE(orders.original_signal_ts, EXCLUDED.original_signal_ts)
                """,
                order.request_id, order_id, order.exchange, order.symbol, order.side, order.order_type,
                order.quantity, order.price, status, filled_qty, filled_price, fee, order.strategy_id,
                order.post_only, order.reduce_only, signal_ts,
            )

    async def _update_from_values(self, rid: str, order_id: str, status: str, filled_qty: float,
                                  filled_price: float | None, fee: float) -> None:
        try:
            async with self._db.pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE orders SET order_id = $2, status = $3, filled_qty = $4,
                                      filled_price = $5, fee = $6, updated_at = NOW()
                    WHERE request_id = $1
                    """,
                    rid, order_id, status, filled_qty, filled_price, fee,
                )
        except Exception:
            log.exception("update_order_failed", request_id=rid)

    async def _update_status(self, rid: str, status: str) -> None:
        try:
            async with self._db.pool.acquire() as conn:
                await conn.execute("UPDATE orders SET status = $2, updated_at = NOW() WHERE request_id = $1",
                                   rid, status)
        except Exception:
            log.exception("update_order_status_failed", request_id=rid)

    async def _fetch_row(self, rid: str) -> dict[str, Any] | None:
        try:
            async with self._db.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM orders WHERE request_id = $1", rid)
            return dict(row) if row else None
        except Exception:
            log.exception("fetch_order_row_failed", request_id=rid)
            return None

    @staticmethod
    def _result_from_row(row: dict[str, Any]) -> ExecutionResult:
        """Result of an earlier run of the same request_id (no new order is sent)."""
        status = _FROM_DB.get(row["status"])
        reason = ""
        if status is None:  # pending / submitted left by a crashed run
            status, reason = "rejected", "duplicate_request_id_unfinished"
        return ExecutionResult(
            request_id=row["request_id"], status=status, filled_qty=float(row.get("filled_qty") or 0.0),
            filled_price=row.get("filled_price"), fee=float(row.get("fee") or 0.0), reason=reason,
            order_ids=[row["order_id"]] if row.get("order_id") else [],
        )
