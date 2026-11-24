"""Risk: equity peaks, drawdowns, KillSwitch integration and halt logic.

Ported from services/orchestrator/core.py and portfolio_monitor.py. The shared
``KillSwitch`` is used unchanged; this module feeds it the same inputs the
orchestrator did and decides what its trigger means for the bot:

* PORTFOLIO while flat  -> alert only, entries stay allowed (2026-11-07).
* PORTFOLIO in position -> close it, block entries until the cooldown ends.
* Cooldown elapsed      -> rebase all three peaks to current equity so the same
  old loss cannot re-trigger, unblock entries (2026-11-07). The rebase time is
  persisted so a rebuild of the peaks after a restart honours it.
* MANUAL                -> BotControl.emergency_close; entries blocked until resume.

Everything that must survive a restart lives in ``bot_state``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import numpy as np
import structlog
import yaml

from bot.db import Database
from bot.interfaces import STRATEGY_ID, Executor, Notifier
from bot.state import MANUAL_HALT, PORTFOLIO_BLOCK_UNTIL, BotState, Clock, SystemClock, parse_ts
from shared.kill_switch import KillLevel, KillSwitch
from shared.models.position import PortfolioState, Position

log = structlog.get_logger("bot.risk")

KEY_PEAKS = "risk_peaks"
KEY_PEAK_REBASE_AT = "peak_rebase_at"
KEY_KILL_STATE = "kill_switch"  # web.py reads this key
SNAPSHOT_INTERVAL = timedelta(hours=1)
HISTORY_DAYS = 35


@dataclass
class RiskConfig:
    daily_pct: float = 5.0
    weekly_pct: float = 10.0
    monthly_pct: float = 15.0
    daily_abs_usd: float = 10.0
    weekly_abs_usd: float = 20.0
    monthly_abs_usd: float = 30.0
    cooldown_minutes: float = 60.0
    max_capital_usd: float = 1000.0

    @classmethod
    def from_dict(cls, cfg: dict[str, Any]) -> "RiskConfig":
        ks = cfg.get("kill_switch", {})
        d = cls()
        return cls(
            daily_pct=float(ks.get("max_daily_drawdown_pct", d.daily_pct)),
            weekly_pct=float(ks.get("max_weekly_drawdown_pct", d.weekly_pct)),
            monthly_pct=float(ks.get("max_monthly_drawdown_pct", d.monthly_pct)),
            daily_abs_usd=float(ks.get("max_daily_loss_abs_usd", d.daily_abs_usd)),
            weekly_abs_usd=float(ks.get("max_weekly_loss_abs_usd", d.weekly_abs_usd)),
            monthly_abs_usd=float(ks.get("max_monthly_loss_abs_usd", d.monthly_abs_usd)),
            cooldown_minutes=float(ks.get("cooldown_minutes", d.cooldown_minutes)),
            max_capital_usd=float(cfg.get("max_capital_usd", d.max_capital_usd)),
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "RiskConfig":
        with open(path) as f:
            return cls.from_dict(yaml.safe_load(f) or {})


@dataclass
class RiskStatus:
    equity: float
    daily_dd: float  # positive fractions, 0.05 = 5 % below the period peak
    weekly_dd: float
    monthly_dd: float
    level: KillLevel
    alert_only: bool
    newly_triggered: bool = False


def drawdown(current: float, peak: float) -> float:
    if peak <= 0:
        return 0.0
    return max(0.0, (peak - current) / peak)


def _period_keys(now: datetime) -> dict[str, str]:
    d = now.date()
    return {
        "daily_key": d.isoformat(),
        "weekly_key": (d - timedelta(days=d.weekday())).isoformat(),  # Monday UTC
        "monthly_key": d.replace(day=1).isoformat(),
    }


class RiskManager:
    def __init__(
        self,
        db: Database,
        state: BotState,
        executor: Executor,
        notifier: Notifier,
        *,
        config: RiskConfig,
        clock: Clock | None = None,
        on_position_closed: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._db = db
        self._state = state
        self._executor = executor
        self._notifier = notifier
        self._cfg = config
        self._clock: Clock = clock or SystemClock()
        self._on_position_closed = on_position_closed

        self.kill_switch = KillSwitch(
            daily_limit=-(config.daily_pct / 100.0),
            weekly_limit=-(config.weekly_pct / 100.0),
            monthly_limit=-(config.monthly_pct / 100.0),
            cooldown_hours=config.cooldown_minutes / 60.0,
            on_trigger=self._on_trigger,
            daily_loss_abs_usd=config.daily_abs_usd,
            weekly_loss_abs_usd=config.weekly_abs_usd,
            monthly_loss_abs_usd=config.monthly_abs_usd,
        )
        self.alert_only = False
        self._peaks: dict[str, Any] = {"daily": 0.0, "weekly": 0.0, "monthly": 0.0}
        self._keys: dict[str, str] = {}
        self._history: list[tuple[datetime, float]] = []
        self._last_dd = (0.0, 0.0, 0.0)
        self._position: Position | None = None
        self._position_known = False
        self._last_snapshot_at: datetime | None = None
        self._last_unrealized = 0.0

    # ── read-only views ─────────────────────────────────────────────────

    @property
    def level(self) -> KillLevel:
        return self.kill_switch.level

    @property
    def daily_dd(self) -> float:
        return self._last_dd[0]

    @property
    def weekly_dd(self) -> float:
        return self._last_dd[1]

    @property
    def monthly_dd(self) -> float:
        return self._last_dd[2]

    @property
    def peaks(self) -> dict[str, float]:
        return {k: float(self._peaks[k]) for k in ("daily", "weekly", "monthly")}

    # ── restore ─────────────────────────────────────────────────────────

    async def load(self) -> None:
        """Restore peaks, equity history and kill state after a (re)start."""
        now = self._clock.now()
        cutoff = now - timedelta(days=HISTORY_DAYS)
        try:
            async with self._db.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT snapshot_at, total_equity FROM portfolio_snapshots "
                    "WHERE snapshot_at >= $1 ORDER BY snapshot_at", cutoff,
                )
                last = await conn.fetchval("SELECT MAX(snapshot_at) FROM portfolio_snapshots")
            self._history = [(r["snapshot_at"], float(r["total_equity"])) for r in rows]
            self._last_snapshot_at = last
        except Exception:
            log.exception("risk_history_load_error")

        saved = self._state.get(KEY_PEAKS)
        if isinstance(saved, dict) and all(k in saved for k in ("daily", "weekly", "monthly")):
            self._peaks = {k: float(saved[k]) for k in ("daily", "weekly", "monthly")}
            self._keys = {k: str(saved.get(k, "")) for k in ("daily_key", "weekly_key", "monthly_key")}
        else:
            self._rebuild_peaks(now)

        ks = self._state.get(KEY_KILL_STATE)
        if isinstance(ks, dict) and int(ks.get("level", 0)) > 0:
            at = parse_ts(ks.get("triggered_at"))
            sw = self.kill_switch
            # shared/kill_switch.py has no restore API; set its state directly.
            sw._active_level = KillLevel(int(ks["level"]))
            sw._reason = str(ks.get("reason", ""))
            sw._triggered_at = at or now
            self.alert_only = bool(ks.get("alert_only", False))
            log.warning("kill_state_restored", level=sw._active_level.name, reason=sw._reason,
                        alert_only=self.alert_only)

    def _rebuild_peaks(self, now: datetime) -> None:
        """Period peaks from stored snapshots, ignoring anything before the last rebase."""
        floor = parse_ts(self._state.get(KEY_PEAK_REBASE_AT))
        since = [(t, e) for t, e in self._history if floor is None or t >= floor]
        keys = _period_keys(now)
        today = now.date()
        week_start = today - timedelta(days=today.weekday())
        month_start = today.replace(day=1)
        daily = [e for t, e in since if t.date() == today]
        weekly = [e for t, e in since if t.date() >= week_start]
        monthly = [e for t, e in since if t.date() >= month_start]
        self._peaks = {
            "daily": max(daily) if daily else 0.0,
            "weekly": max(weekly) if weekly else 0.0,
            "monthly": max(monthly) if monthly else 0.0,
        }
        self._keys = keys

    # ── evaluation ──────────────────────────────────────────────────────

    def _roll_periods(self, now: datetime, equity: float) -> None:
        keys = _period_keys(now)
        for period in ("daily", "weekly", "monthly"):
            if self._keys.get(f"{period}_key") != keys[f"{period}_key"]:
                self._peaks[period] = equity  # new period starts at current equity
        self._keys = keys
        for period in ("daily", "weekly", "monthly"):
            self._peaks[period] = max(float(self._peaks[period]), equity)

    def _cooldown_elapsed(self, now: datetime) -> bool:
        ks = self.kill_switch
        if not ks.is_triggered or ks.triggered_at is None or ks.level != KillLevel.PORTFOLIO:
            return False
        return now - ks.triggered_at >= ks.cooldown

    async def rebase_peaks(self, equity: float) -> None:
        now = self._clock.now()
        for period in ("daily", "weekly", "monthly"):
            self._peaks[period] = equity
        await self._state.set(KEY_PEAK_REBASE_AT, now.isoformat())
        await self._save_peaks()
        log.warning("drawdown_peaks_rebased", message="쿨다운 종료 — 낙폭 기준 고점 재설정", equity=equity)

    async def _save_peaks(self) -> None:
        await self._state.set(KEY_PEAKS, {**self.peaks, **self._keys})

    async def evaluate(
        self, equity: float, position: Position | None = None, *, position_known: bool = True,
    ) -> RiskStatus:
        """One risk cycle (housekeeping calls it every 15 min).

        ``equity`` is the wallet balance (unrealized PnL excluded, as today).
        """
        now = self._clock.now()
        equity = max(float(equity), 0.0)
        self._position, self._position_known = position, position_known
        self._last_unrealized = float(position.unrealized_pnl) if position else 0.0

        self._roll_periods(now, equity)
        self._history.append((now, equity))
        cutoff = now - timedelta(days=HISTORY_DAYS)
        self._history = [(t, e) for t, e in self._history if t >= cutoff]

        ks = self.kill_switch
        if self._cooldown_elapsed(now):
            await self.rebase_peaks(equity)
            self.alert_only = False
            await self._state.delete(PORTFOLIO_BLOCK_UNTIL)
            await self._state.delete(KEY_KILL_STATE)
            d = w = m = 0.0
        else:
            d = drawdown(equity, self._peaks["daily"])
            w = drawdown(equity, self._peaks["weekly"])
            m = drawdown(equity, self._peaks["monthly"])
        self._last_dd = (d, w, m)
        await self._save_peaks()

        # KillSwitch uses the negative convention; the portfolio fields below
        # are exactly what the orchestrator passed.
        ks_state = PortfolioState(
            total_equity=equity, unrealized_pnl=self._last_unrealized, realized_pnl_today=0.0,
            daily_drawdown=-d, weekly_drawdown=-w, monthly_drawdown=-m, strategies=[],
            kill_switch_triggered=ks.is_triggered,
        )
        prev_triggered_at = ks.triggered_at
        level = await ks.check(ks_state, monthly_drawdown=-m, system_healthy=True,
                               equity_at_open=equity)
        newly = level > KillLevel.NONE and ks.triggered_at != prev_triggered_at

        if level > KillLevel.NONE and not newly:
            log.warning("kill_switch_cooldown", message="kill switch still active (cooldown)",
                        level=level.name, reason=ks.reason)
        if level == KillLevel.PORTFOLIO and self.alert_only and not newly:
            # Alert-only while flat; if a position appeared (entries were allowed)
            # escalate to the normal halt path.
            if await self._position_present():
                log.critical("kill_switch_promoted",
                             message="kill switch 알림 전용 상태에서 포지션 감지 — 청산 및 진입 차단")
                self.alert_only = False
                await self._halt_and_close("promoted")
                await self._notifier.kill_switch(
                    level=level.name, reason=ks.reason,
                    action="알림 전용 상태에서 포지션 감지 — 청산 및 진입 차단",
                    daily_dd_pct=d * 100.0, weekly_dd_pct=w * 100.0,
                )
                await self._persist_kill_state()
        return RiskStatus(equity=equity, daily_dd=d, weekly_dd=w, monthly_dd=m,
                          level=ks.level, alert_only=self.alert_only, newly_triggered=newly)

    # ── trigger handling ────────────────────────────────────────────────

    async def _position_present(self) -> bool:
        """True unless the exchange is known to be flat (unknown counts as present)."""
        pos = self._position
        if not self._position_known:
            try:
                pos = await self._executor.get_position()
            except Exception:
                return True
        return pos is not None and pos.size > 0

    async def _halt_and_close(self, why: str) -> None:
        """Block entries until the cooldown ends, then close the position."""
        ks = self.kill_switch
        at = ks.triggered_at or self._clock.now()
        until = at + ks.cooldown
        try:  # the block applies in memory even if Postgres is down
            await self._state.set(PORTFOLIO_BLOCK_UNTIL, {
                "until": until.isoformat(), "reason": f"{ks.level.name}: {ks.reason}"[:200]})
        except Exception:
            log.exception("portfolio_block_persist_error")
        try:
            res = await self._executor.close_position("kill_switch")
            log.warning("kill_switch_close", why=why, status=getattr(res, "status", None))
        except Exception as exc:
            log.exception("kill_switch_close_error")
            await self._notifier.anomaly(
                kind="kill_switch_close_failed",
                message=f"킬스위치 청산 실패: {type(exc).__name__}: {str(exc)[:200]} — 수동 확인 필요",
            )
        if self._on_position_closed is not None:
            try:
                await self._on_position_closed("kill_switch")
            except Exception:
                log.exception("on_position_closed_error")

    async def _on_trigger(self, level: KillLevel, reason: str) -> None:
        """Callback fired by the shared KillSwitch on every trigger."""
        d, w, _ = self._last_dd
        if level == KillLevel.MANUAL:
            # emergency_close closes the position itself and sets the halt flag.
            action = "수동 긴급 청산"
        else:
            flat = level == KillLevel.PORTFOLIO and not await self._position_present()
            self.alert_only = flat
            if flat:
                action = "알림만 (보유 포지션 없음 — 진입 허용 유지)"
                log.warning("kill_switch_alert_only", reason=reason)
            else:
                action = {
                    KillLevel.STRATEGY: "전략 일시정지",
                    KillLevel.PORTFOLIO: f"포지션 청산 및 신규 진입 차단 (쿨다운 {self._cfg.cooldown_minutes:g}분)",
                    KillLevel.SYSTEM: "시스템 차단",
                }.get(level, level.name)
                await self._halt_and_close("trigger")
        await self._persist_kill_state()
        await self._notifier.kill_switch(level=level.name, reason=reason, action=action,
                                         daily_dd_pct=d * 100.0, weekly_dd_pct=w * 100.0)

    async def _persist_kill_state(self) -> None:
        ks = self.kill_switch
        try:
            if ks.is_triggered:
                at = ks.triggered_at
                await self._state.set(KEY_KILL_STATE, {
                    "level": int(ks.level), "level_name": ks.level.name, "reason": ks.reason,
                    "triggered_at": at.isoformat() if at else None,
                    "alert_only": self.alert_only,
                })
            else:
                await self._state.delete(KEY_KILL_STATE)
        except Exception:
            log.exception("kill_state_persist_error")

    # ── manual halt ─────────────────────────────────────────────────────

    async def manual_halt(self, requested_by: str, reason: str | None = None) -> None:
        """Block entries until resume and put the KillSwitch at MANUAL.
        Closing the position is the caller's job (it must work while blocked)."""
        reason = reason or f"Manual emergency by {requested_by}"
        try:  # the halt applies in memory even if Postgres is down
            await self._state.set(MANUAL_HALT, {
                "active": True, "by": requested_by, "at": self._clock.now().isoformat(),
                "reason": reason,
            })
        except Exception:
            log.exception("manual_halt_persist_error")
        await self.kill_switch.trigger_manual(reason)

    async def resume(self, requested_by: str) -> bool:
        """Clear the manual halt and reset the KillSwitch. True if anything was halted."""
        halt = self._state.get(MANUAL_HALT)
        was_halted = bool(isinstance(halt, dict) and halt.get("active")) or self.kill_switch.level == KillLevel.MANUAL
        if was_halted:
            await self._state.delete(MANUAL_HALT)
            await self.kill_switch.reset_manual()
            self.alert_only = False
            await self._state.delete(KEY_KILL_STATE)
            log.warning("manual_halt_cleared", by=requested_by)
        return was_halted

    # ── snapshots ───────────────────────────────────────────────────────

    def _sharpe_30d(self, now: datetime) -> float | None:
        recent = [e for t, e in self._history if t >= now - timedelta(days=30)]
        if len(recent) < 10:
            return None
        rets = [(recent[i] - recent[i - 1]) / recent[i - 1] for i in range(1, len(recent))
                if recent[i - 1] > 0]
        if len(rets) < 5:
            return None
        arr = np.array(rets)
        std = float(np.std(arr, ddof=1))
        if std == 0:
            return 0.0
        return round((float(np.mean(arr)) - 0.05 / 365) / std * float(np.sqrt(365)), 4)

    async def record_snapshot(
        self, equity: float, *, position_count: int = 0, unrealized: float | None = None,
        force: bool = False,
    ) -> bool:
        """Write a portfolio_snapshots row: at most hourly unless ``force`` (after fills)."""
        now = self._clock.now()
        if not force and self._last_snapshot_at is not None \
                and now - self._last_snapshot_at < SNAPSHOT_INTERVAL:
            return False
        d, w, m = self._last_dd
        upnl = self._last_unrealized if unrealized is None else unrealized
        strategies = [{"strategy_id": STRATEGY_ID, "is_running": True,
                       "allocated_capital": equity, "current_pnl": upnl,
                       "position_count": position_count}]
        try:
            async with self._db.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO portfolio_snapshots (
                        total_equity, unrealized_pnl, realized_pnl_today,
                        daily_drawdown, weekly_drawdown, monthly_drawdown,
                        sharpe_ratio_30d, strategies, snapshot_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
                    """,
                    equity, upnl, 0.0, d, w, m, self._sharpe_30d(now), json.dumps(strategies), now,
                )
        except Exception:
            log.exception("snapshot_error")
            return False
        self._last_snapshot_at = now
        return True
