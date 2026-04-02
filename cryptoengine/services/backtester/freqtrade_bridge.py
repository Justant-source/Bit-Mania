"""FreqtradeBridge — adaptor for running CryptoEngine strategies through
Freqtrade's backtesting infrastructure or as a standalone event-driven
backtest engine.

Provides:
  * ``configure_backtest`` — build a Freqtrade-compatible config dict.
  * ``run_backtest``       — execute the backtest (subprocess or in-process).
  * ``parse_results``      — normalise raw output into ``BacktestResult``.
"""

from __future__ import annotations

import json
import math
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TradeRecord:
    """Single trade emitted by the backtest engine."""

    open_ts: datetime
    close_ts: datetime
    symbol: str
    side: str
    quantity: float
    entry_price: float
    exit_price: float
    pnl: float
    fee: float
    duration_hours: float


@dataclass
class BacktestResult:
    """Normalised backtest output consumed by ``ReportGenerator``."""

    strategy: str
    start_date: str
    end_date: str
    initial_capital: float
    final_capital: float
    total_profit: float
    total_profit_pct: float
    max_drawdown: float
    max_drawdown_pct: float
    sharpe_ratio: float
    sortino_ratio: float
    win_rate: float
    total_trades: int
    avg_trade_duration_hours: float
    profit_factor: float
    trades: list[TradeRecord] = field(default_factory=list)
    equity_curve: list[float] = field(default_factory=list)
    drawdown_curve: list[float] = field(default_factory=list)
    daily_returns: list[float] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Bridge
# ---------------------------------------------------------------------------

class FreqtradeBridge:
    """Adapter that can run backtests either via the Freqtrade CLI or
    using an internal event-driven engine that consumes OHLCV DataFrames.
    """

    def __init__(
        self,
        freqtrade_path: str = "freqtrade",
        user_data_dir: str | None = None,
    ) -> None:
        self._ft_path = freqtrade_path
        self._user_data_dir = user_data_dir

    # ------------------------------------------------------------------
    # Configure
    # ------------------------------------------------------------------

    def configure_backtest(
        self,
        strategy: str,
        timerange: str,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a Freqtrade-compatible configuration dict.

        Parameters
        ----------
        strategy : strategy class name (e.g. ``"FundingArbStrategy"``).
        timerange : date range in Freqtrade format (``"20250101-20260101"``).
        config : base config to merge into; uses sensible defaults otherwise.

        Returns
        -------
        Complete Freqtrade config dict ready for ``run_backtest``.
        """
        base: dict[str, Any] = {
            "stake_currency": "USDT",
            "stake_amount": "unlimited",
            "dry_run": True,
            "trading_mode": "futures",
            "margin_mode": "isolated",
            "timeframe": "1h",
            "max_open_trades": 5,
            "exchange": {
                "name": "bybit",
                "key": "",
                "secret": "",
                "pair_whitelist": ["BTC/USDT:USDT", "ETH/USDT:USDT"],
            },
            "strategy": strategy,
            "timerange": timerange,
            "datadir": str(Path(self._user_data_dir or "/tmp") / "data"),
            "exportfilename": "",
        }

        if config:
            base = _deep_merge(base, config)

        log.info(
            "backtest_configured",
            strategy=strategy,
            timerange=timerange,
            pairs=base["exchange"]["pair_whitelist"],
        )
        return base

    # ------------------------------------------------------------------
    # Run  (in-process engine — no Freqtrade dependency required)
    # ------------------------------------------------------------------

    def run_backtest(
        self,
        *,
        strategy: str,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame | None = None,
        initial_capital: float = 10_000.0,
        fee_rate: float = 0.0006,
    ) -> BacktestResult:
        """Run an in-process event-driven backtest on OHLCV data.

        This is the primary mode for CryptoEngine strategies which do not
        need the full Freqtrade runtime.
        """
        log.info(
            "backtest_starting",
            strategy=strategy,
            bars=len(ohlcv),
            capital=initial_capital,
        )

        if ohlcv.empty:
            return self._empty_result(strategy, initial_capital)

        engine = _BacktestEngine(
            strategy=strategy,
            ohlcv=ohlcv,
            funding=funding,
            initial_capital=initial_capital,
            fee_rate=fee_rate,
        )
        result = engine.run()

        log.info(
            "backtest_complete",
            strategy=strategy,
            trades=result.total_trades,
            profit_pct=round(result.total_profit_pct, 4),
            sharpe=round(result.sharpe_ratio, 4),
            max_dd_pct=round(result.max_drawdown_pct, 4),
        )
        return result

    # ------------------------------------------------------------------
    # Run via Freqtrade subprocess (optional — requires freqtrade install)
    # ------------------------------------------------------------------

    def run_backtest_freqtrade(
        self, config: dict[str, Any], *, timeout: int = 600
    ) -> BacktestResult:
        """Shell out to ``freqtrade backtesting`` and parse the JSON results."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as cfg_file:
            json.dump(config, cfg_file)
            cfg_path = cfg_file.name

        export_file = tempfile.mktemp(suffix=".json")
        config["exportfilename"] = export_file

        cmd = [
            self._ft_path,
            "backtesting",
            "--config",
            cfg_path,
            "--strategy",
            config["strategy"],
            "--timerange",
            config.get("timerange", ""),
            "--export",
            "trades",
            "--export-filename",
            export_file,
        ]

        log.info("freqtrade_subprocess_start", cmd=" ".join(cmd))

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if proc.returncode != 0:
                log.error(
                    "freqtrade_backtest_failed",
                    returncode=proc.returncode,
                    stderr=proc.stderr[:2000],
                )
                raise RuntimeError(
                    f"freqtrade exited with code {proc.returncode}"
                )

            return self.parse_results(export_file, config["strategy"])

        except subprocess.TimeoutExpired:
            log.error("freqtrade_backtest_timeout", timeout=timeout)
            raise

    # ------------------------------------------------------------------
    # Parse Freqtrade JSON export
    # ------------------------------------------------------------------

    def parse_results(self, output_path: str, strategy: str) -> BacktestResult:
        """Parse a Freqtrade JSON export file into ``BacktestResult``."""
        path = Path(output_path)
        if not path.exists():
            raise FileNotFoundError(f"Backtest output not found: {path}")

        with path.open() as fh:
            raw = json.load(fh)

        strat_data = raw.get("strategy", {}).get(strategy, raw)
        trades_raw = strat_data.get("trades", [])

        trades: list[TradeRecord] = []
        for t in trades_raw:
            trades.append(
                TradeRecord(
                    open_ts=datetime.fromisoformat(t.get("open_date", "")),
                    close_ts=datetime.fromisoformat(t.get("close_date", "")),
                    symbol=t.get("pair", ""),
                    side=t.get("trade_direction", "long"),
                    quantity=float(t.get("amount", 0)),
                    entry_price=float(t.get("open_rate", 0)),
                    exit_price=float(t.get("close_rate", 0)),
                    pnl=float(t.get("profit_abs", 0)),
                    fee=float(t.get("fee_open", 0)) + float(t.get("fee_close", 0)),
                    duration_hours=float(t.get("trade_duration", 0)) / 3600.0,
                )
            )

        total_profit = sum(t.pnl for t in trades)
        initial_capital = float(strat_data.get("starting_balance", 10_000))
        winning = [t for t in trades if t.pnl > 0]
        losing = [t for t in trades if t.pnl <= 0]
        gross_profit = sum(t.pnl for t in winning)
        gross_loss = abs(sum(t.pnl for t in losing))

        return BacktestResult(
            strategy=strategy,
            start_date=strat_data.get("backtest_start", ""),
            end_date=strat_data.get("backtest_end", ""),
            initial_capital=initial_capital,
            final_capital=initial_capital + total_profit,
            total_profit=total_profit,
            total_profit_pct=(total_profit / initial_capital * 100)
            if initial_capital > 0
            else 0.0,
            max_drawdown=float(strat_data.get("max_drawdown_abs", 0)),
            max_drawdown_pct=float(strat_data.get("max_drawdown", 0)) * 100,
            sharpe_ratio=float(strat_data.get("sharpe", 0)),
            sortino_ratio=float(strat_data.get("sortino", 0)),
            win_rate=(len(winning) / len(trades) * 100) if trades else 0.0,
            total_trades=len(trades),
            avg_trade_duration_hours=(
                sum(t.duration_hours for t in trades) / len(trades)
                if trades
                else 0.0
            ),
            profit_factor=(gross_profit / gross_loss) if gross_loss > 0 else float("inf"),
            trades=trades,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _empty_result(strategy: str, capital: float) -> BacktestResult:
        return BacktestResult(
            strategy=strategy,
            start_date="",
            end_date="",
            initial_capital=capital,
            final_capital=capital,
            total_profit=0.0,
            total_profit_pct=0.0,
            max_drawdown=0.0,
            max_drawdown_pct=0.0,
            sharpe_ratio=0.0,
            sortino_ratio=0.0,
            win_rate=0.0,
            total_trades=0,
            avg_trade_duration_hours=0.0,
            profit_factor=0.0,
        )


# =========================================================================
# Internal event-driven backtest engine
# =========================================================================

class _BacktestEngine:
    """Minimal event-loop backtester that walks through OHLCV bars and
    applies strategy-specific signal logic.
    """

    def __init__(
        self,
        *,
        strategy: str,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame | None,
        initial_capital: float,
        fee_rate: float,
    ) -> None:
        self._strategy = strategy
        self._ohlcv = ohlcv
        self._funding = funding
        self._initial_capital = initial_capital
        self._fee_rate = fee_rate

        self._equity = initial_capital
        self._equity_curve: list[float] = [initial_capital]
        self._trades: list[TradeRecord] = []
        self._position: dict[str, Any] | None = None

    # ------------------------------------------------------------------

    def run(self) -> BacktestResult:
        """Walk bars and apply strategy signals."""
        handler = self._get_signal_handler()
        bars = self._ohlcv.reset_index()

        for idx in range(20, len(bars)):  # skip warm-up period
            bar = bars.iloc[idx]
            lookback = bars.iloc[max(0, idx - 200) : idx + 1]
            signal = handler(bar, lookback, idx)

            if signal == "buy" and self._position is None:
                self._open_position(bar, "buy")
            elif signal == "sell" and self._position is None:
                self._open_position(bar, "sell")
            elif signal == "close" and self._position is not None:
                self._close_position(bar)
            elif signal == "reverse" and self._position is not None:
                self._close_position(bar)
                new_side = "sell" if self._position is None else "buy"
                # position already closed, reverse
                self._open_position(bar, "buy" if bars.iloc[idx]["close"] > bars.iloc[idx]["open"] else "sell")

            self._equity_curve.append(self._equity + self._unrealized_pnl(bar))

        # Force close open position at end
        if self._position is not None:
            self._close_position(bars.iloc[-1])
            self._equity_curve[-1] = self._equity

        return self._build_result(bars)

    # ------------------------------------------------------------------
    # Signal handlers per strategy
    # ------------------------------------------------------------------

    def _get_signal_handler(self):
        handlers = {
            "funding_arb": self._signal_funding_arb,
            "grid_trading": self._signal_grid,
            "adaptive_dca": self._signal_dca,
            "combined": self._signal_combined,
        }
        return handlers.get(self._strategy, self._signal_funding_arb)

    def _signal_funding_arb(self, bar: Any, lookback: pd.DataFrame, idx: int) -> str | None:
        """Funding arb: enter when funding positive and spread exists."""
        close = float(bar["close"])
        sma20 = float(lookback["close"].tail(20).mean())
        sma50 = float(lookback["close"].tail(50).mean()) if len(lookback) >= 50 else sma20

        funding_rate = self._get_funding_rate(bar)

        if self._position is None:
            if funding_rate > 0.0001 and close < sma20:
                return "buy"
        else:
            held_bars = idx - self._position.get("entry_idx", idx)
            pnl_pct = self._unrealized_pnl(bar) / self._initial_capital
            if funding_rate < 0 or pnl_pct < -0.02 or pnl_pct > 0.03 or held_bars > 24 * 3:
                return "close"
        return None

    def _signal_grid(self, bar: Any, lookback: pd.DataFrame, idx: int) -> str | None:
        """Grid: buy on dips, sell on rallies around SMA."""
        close = float(bar["close"])
        sma = float(lookback["close"].tail(20).mean())
        dev = (close - sma) / sma if sma > 0 else 0

        if self._position is None:
            if dev < -0.005:
                return "buy"
        else:
            if dev > 0.005:
                return "close"
            pnl_pct = self._unrealized_pnl(bar) / self._initial_capital
            if pnl_pct < -0.01:
                return "close"
        return None

    def _signal_dca(self, bar: Any, lookback: pd.DataFrame, idx: int) -> str | None:
        """Adaptive DCA: buy regularly, more on dips."""
        if idx % 24 == 0:  # every ~24 bars (daily on 1h)
            if self._position is None:
                return "buy"
        if self._position is not None:
            pnl_pct = self._unrealized_pnl(bar) / self._initial_capital
            if pnl_pct > 0.05 or pnl_pct < -0.03:
                return "close"
        return None

    def _signal_combined(self, bar: Any, lookback: pd.DataFrame, idx: int) -> str | None:
        """Combined: simple momentum-based signals."""
        close = float(bar["close"])
        sma20 = float(lookback["close"].tail(20).mean())
        sma50 = float(lookback["close"].tail(50).mean()) if len(lookback) >= 50 else sma20

        if self._position is None:
            if close > sma20 > sma50:
                return "buy"
        else:
            pnl_pct = self._unrealized_pnl(bar) / self._initial_capital
            if close < sma20 or pnl_pct < -0.02 or pnl_pct > 0.04:
                return "close"
        return None

    # ------------------------------------------------------------------
    # Position management
    # ------------------------------------------------------------------

    def _open_position(self, bar: Any, side: str) -> None:
        entry = float(bar["close"])
        size = (self._equity * 0.95) / entry  # 95% capital utilisation
        fee = entry * size * self._fee_rate
        self._equity -= fee

        self._position = {
            "side": side,
            "entry_price": entry,
            "size": size,
            "entry_ts": bar.get("ts", bar.name) if hasattr(bar, "name") else None,
            "entry_idx": 0,
            "fee_paid": fee,
        }

    def _close_position(self, bar: Any) -> None:
        if self._position is None:
            return

        exit_price = float(bar["close"])
        size = self._position["size"]
        entry = self._position["entry_price"]
        side = self._position["side"]

        if side == "buy":
            pnl = (exit_price - entry) * size
        else:
            pnl = (entry - exit_price) * size

        fee = exit_price * size * self._fee_rate
        net_pnl = pnl - fee
        self._equity += net_pnl

        entry_ts = self._position.get("entry_ts")
        close_ts = bar.get("ts", bar.name) if hasattr(bar, "name") else None

        self._trades.append(
            TradeRecord(
                open_ts=pd.Timestamp(entry_ts) if entry_ts else datetime.min,
                close_ts=pd.Timestamp(close_ts) if close_ts else datetime.min,
                symbol="BTCUSDT",
                side=side,
                quantity=size,
                entry_price=entry,
                exit_price=exit_price,
                pnl=net_pnl,
                fee=fee + self._position.get("fee_paid", 0),
                duration_hours=0.0,
            )
        )
        self._position = None

    def _unrealized_pnl(self, bar: Any) -> float:
        if self._position is None:
            return 0.0
        price = float(bar["close"])
        entry = self._position["entry_price"]
        size = self._position["size"]
        if self._position["side"] == "buy":
            return (price - entry) * size
        return (entry - price) * size

    # ------------------------------------------------------------------
    # Funding rate lookup
    # ------------------------------------------------------------------

    def _get_funding_rate(self, bar: Any) -> float:
        if self._funding is None or self._funding.empty:
            return 0.0001  # default positive rate
        ts = bar.get("ts", bar.name) if hasattr(bar, "name") else None
        if ts is None:
            return 0.0001
        try:
            ts = pd.Timestamp(ts)
            mask = self._funding.index <= ts
            if mask.any():
                return float(self._funding.loc[mask, "rate"].iloc[-1])
        except Exception:
            pass
        return 0.0001

    # ------------------------------------------------------------------
    # Build result
    # ------------------------------------------------------------------

    def _build_result(self, bars: pd.DataFrame) -> BacktestResult:
        total_profit = self._equity - self._initial_capital
        winning = [t for t in self._trades if t.pnl > 0]
        losing = [t for t in self._trades if t.pnl <= 0]
        gross_profit = sum(t.pnl for t in winning)
        gross_loss = abs(sum(t.pnl for t in losing))

        # Drawdown from equity curve
        max_dd, max_dd_pct = _compute_drawdown(self._equity_curve)

        # Daily returns
        daily_returns = _compute_daily_returns(self._equity_curve)
        sharpe = _compute_sharpe(daily_returns)
        sortino = _compute_sortino(daily_returns)

        # Drawdown curve
        dd_curve = _drawdown_series(self._equity_curve)

        start_date = str(bars.iloc[0].get("ts", "")) if len(bars) > 0 else ""
        end_date = str(bars.iloc[-1].get("ts", "")) if len(bars) > 0 else ""

        return BacktestResult(
            strategy=self._strategy,
            start_date=start_date,
            end_date=end_date,
            initial_capital=self._initial_capital,
            final_capital=self._equity,
            total_profit=total_profit,
            total_profit_pct=(total_profit / self._initial_capital * 100)
            if self._initial_capital > 0
            else 0.0,
            max_drawdown=max_dd,
            max_drawdown_pct=max_dd_pct,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            win_rate=(len(winning) / len(self._trades) * 100)
            if self._trades
            else 0.0,
            total_trades=len(self._trades),
            avg_trade_duration_hours=(
                sum(t.duration_hours for t in self._trades) / len(self._trades)
                if self._trades
                else 0.0
            ),
            profit_factor=(gross_profit / gross_loss)
            if gross_loss > 0
            else float("inf"),
            trades=self._trades,
            equity_curve=self._equity_curve,
            drawdown_curve=dd_curve,
            daily_returns=daily_returns,
        )


# =========================================================================
# Utility functions
# =========================================================================

def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base*."""
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _compute_drawdown(equity_curve: list[float]) -> tuple[float, float]:
    """Return (max_drawdown_abs, max_drawdown_pct)."""
    if len(equity_curve) < 2:
        return 0.0, 0.0

    peak = equity_curve[0]
    max_dd = 0.0
    max_dd_pct = 0.0

    for val in equity_curve:
        if val > peak:
            peak = val
        dd = peak - val
        dd_pct = dd / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct

    return max_dd, max_dd_pct * 100


def _drawdown_series(equity_curve: list[float]) -> list[float]:
    """Return percentage drawdown at each point."""
    if not equity_curve:
        return []
    peak = equity_curve[0]
    dd = []
    for val in equity_curve:
        if val > peak:
            peak = val
        dd.append((val - peak) / peak * 100 if peak > 0 else 0.0)
    return dd


def _compute_daily_returns(equity_curve: list[float]) -> list[float]:
    """Simple period-over-period returns."""
    if len(equity_curve) < 2:
        return []
    return [
        (equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1]
        if equity_curve[i - 1] != 0
        else 0.0
        for i in range(1, len(equity_curve))
    ]


def _compute_sharpe(
    returns: list[float], risk_free: float = 0.0, periods: int = 365
) -> float:
    if len(returns) < 2:
        return 0.0
    mean_r = sum(returns) / len(returns)
    rf = risk_free / periods
    excess = mean_r - rf
    var = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
    std = math.sqrt(var) if var > 0 else 0.0
    if std == 0:
        return 0.0
    return (excess / std) * math.sqrt(periods)


def _compute_sortino(
    returns: list[float], risk_free: float = 0.0, periods: int = 365
) -> float:
    if len(returns) < 2:
        return 0.0
    mean_r = sum(returns) / len(returns)
    rf = risk_free / periods
    excess = mean_r - rf
    downside = [r ** 2 for r in returns if r < 0]
    if not downside:
        return float("inf") if excess > 0 else 0.0
    dd_dev = math.sqrt(sum(downside) / len(downside))
    if dd_dev == 0:
        return 0.0
    return (excess / dd_dev) * math.sqrt(periods)
