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
        fee_rate: float = 0.00055,
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
        self._next_size_ratio: float = 1.0  # combined_v2 포지션 크기 비율
        self._pending_combined_src: str | None = None  # combined_v2 시그널 출처

    # ------------------------------------------------------------------

    def run(self) -> BacktestResult:
        """Walk bars and apply strategy signals."""
        handler = self._get_signal_handler()
        bars = self._ohlcv.reset_index()

        for idx in range(20, len(bars)):  # skip warm-up period
            bar = bars.iloc[idx]
            lookback = bars.iloc[max(0, idx - 200) : idx + 1]
            self._next_size_ratio = 1.0  # reset per bar
            signal = handler(bar, lookback, idx)

            # Tuple signal: ("buy"|"sell", ratio) — allows size override
            if isinstance(signal, tuple) and len(signal) == 2 and signal[0] in ("buy", "sell"):
                side, ratio = signal
                if self._position is None:
                    self._next_size_ratio = float(ratio)
                    self._open_position(bar, side, idx)
            elif signal == "buy" and self._position is None:
                self._open_position(bar, "buy", idx)
            elif signal == "sell" and self._position is None:
                self._open_position(bar, "sell", idx)
            elif signal == "close" and self._position is not None:
                self._close_position(bar)
            elif signal == "reverse" and self._position is not None:
                self._close_position(bar)
                # position already closed, reverse
                self._open_position(bar, "buy" if bars.iloc[idx]["close"] > bars.iloc[idx]["open"] else "sell", idx)

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
            "adaptive_dca_graduated": self._signal_dca_graduated,
            "combined": self._signal_combined,
            "combined_v2": self._signal_combined_v2,
        }
        return handlers.get(self._strategy, self._signal_funding_arb)

    def _signal_funding_arb(self, bar: Any, lookback: pd.DataFrame, idx: int) -> str | None:
        """Funding arb: 완전한 델타 뉴트럴 2-레그 모델.

        실제 펀딩비 차익거래 구조:
          레그 1 — 숏 perp  (funding > 0 일 때) / 롱 perp  (funding < 0 일 때)
          레그 2 — 롱 spot  (funding > 0 일 때) / 숏 spot  (funding < 0 일 때)

        두 레그의 가격 손익은 정반대로 상쇄 → 순 가격 PnL = 0.
        오직 펀딩비 결제분만 수익/비용으로 기록된다.

        결제 주기: Bybit 8시간 (00:00 / 08:00 / 16:00 UTC)
        진입 조건: |funding_rate| >= 0.0001 (0.01%/8h ≈ 10.95% APY)
        청산 조건: 반대 방향 펀딩이 3회 연속 지속

        타임프레임 독립성:
          OHLCV 봉 크기는 진입/청산 실행 가격에만 영향.
          신호 판단은 항상 8h 정산 타임스탬프 기준.
        """
        ENTRY_THRESHOLD =  0.0001   # 0.01%/8h — 진입 최소 조건 (양/음 모두)
        REVERSE_EXIT    =  3        # 반대 방향 연속 N회 → 청산

        funding_rate = self._get_funding_rate(bar)

        # ── 8시간 정산 타이밍 감지 (00:00 / 08:00 / 16:00 UTC) ─────────
        ts = bar.get("ts", bar.name)
        try:
            ts_dt = pd.Timestamp(ts)
            if ts_dt.tzinfo is None:
                ts_dt = ts_dt.tz_localize("UTC")
            is_settlement = (ts_dt.hour % 8 == 0) and (ts_dt.minute == 0)
        except Exception:
            is_settlement = (idx % 8 == 0)

        # ── 펀딩비 정산: 수취(+) 또는 지불(-) 모두 equity에 즉시 반영 ──
        # direction = +1 → 숏 perp 보유 (funding>0 이면 수취, <0 이면 지불)
        # direction = -1 → 롱 perp 보유 (funding<0 이면 수취, >0 이면 지불)
        if self._position is not None and is_settlement:
            direction   = self._position.get("funding_direction", 1)
            pos_value   = self._position["size"] * self._position["entry_price"]
            # 실제 수령액 = pos_value × rate × direction
            # (+rate, direction=+1) → 수취  |  (-rate, direction=+1) → 지불
            net_funding = pos_value * funding_rate * direction
            self._equity += net_funding
            self._position["funding_accumulated"] = (
                self._position.get("funding_accumulated", 0.0) + net_funding
            )

        # ── 진입: 정산 시점 + 충분한 펀딩비가 있을 때 ─────────────────
        if self._position is None:
            if is_settlement:
                if funding_rate >= ENTRY_THRESHOLD:
                    return "sell"   # 숏 perp + 롱 spot  (funding > 0 → 숏이 수취)
                elif funding_rate <= -ENTRY_THRESHOLD:
                    return "buy"    # 롱 perp + 숏 spot  (funding < 0 → 롱이 수취)

        # ── 청산: 방향이 지속적으로 반전될 때 ──────────────────────────
        else:
            direction = self._position.get("funding_direction", 1)
            if is_settlement:
                funding_reversed = (direction > 0 and funding_rate < 0) or \
                                   (direction < 0 and funding_rate > 0)
                if funding_reversed:
                    self._position["reverse_count"] = (
                        self._position.get("reverse_count", 0) + 1
                    )
                else:
                    self._position["reverse_count"] = 0

                if self._position.get("reverse_count", 0) >= REVERSE_EXIT:
                    return "close"

        return None

    def _signal_grid(self, bar: Any, lookback: pd.DataFrame, idx: int) -> str | None:
        """Grid: enter only in ranging markets (low ADX + BB squeeze).

        Activation condition:
          * ADX(14) < 25  — no strong directional trend
          * BB width narrow (< 4% of mid-price) — market in squeeze

        Exit condition:
          * ADX >= 25 (trend breakout) or BB width widens (> 4%)
          * Stop-loss: PnL < -1% of initial capital
        """
        closes = lookback["close"].values.astype(float)
        highs = lookback["high"].values.astype(float) if "high" in lookback.columns else closes
        lows = lookback["low"].values.astype(float) if "low" in lookback.columns else closes

        # ── ADX(14) ────────────────────────────────────────────────────
        adx = 0.0
        period = 14
        if len(closes) >= period * 2:
            plus_dm_list: list[float] = []
            minus_dm_list: list[float] = []
            tr_list: list[float] = []
            for i in range(1, len(closes)):
                up = float(highs[i]) - float(highs[i - 1])
                down = float(lows[i - 1]) - float(lows[i])
                plus_dm_list.append(up if up > down and up > 0 else 0.0)
                minus_dm_list.append(down if down > up and down > 0 else 0.0)
                hl = float(highs[i]) - float(lows[i])
                hc = abs(float(highs[i]) - float(closes[i - 1]))
                lc = abs(float(lows[i]) - float(closes[i - 1]))
                tr_list.append(max(hl, hc, lc))

            if len(tr_list) >= period:
                import numpy as _np
                atr_val = _np.mean(tr_list[-period:])
                if atr_val > 0:
                    plus_di = 100 * _np.mean(plus_dm_list[-period:]) / atr_val
                    minus_di = 100 * _np.mean(minus_dm_list[-period:]) / atr_val
                    di_sum = plus_di + minus_di
                    if di_sum > 0:
                        adx = float(100 * abs(plus_di - minus_di) / di_sum)

        # ── Bollinger Band width ────────────────────────────────────────
        bb_narrow = False
        bb_window = closes[-20:] if len(closes) >= 20 else closes
        if len(bb_window) >= 5:
            import numpy as _np
            sma_bb = float(_np.mean(bb_window))
            std_bb = float(_np.std(bb_window))
            bb_width = (4 * std_bb / sma_bb) if sma_bb > 0 else 1.0  # (upper-lower)/mid
            bb_narrow = bb_width < 0.04  # squeeze threshold: 4%

        ranging = adx < 25 and bb_narrow

        if self._position is None:
            if ranging:
                return "buy"
        else:
            # Exit when trend emerges or BB widens out of squeeze
            if not ranging:
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

    def _signal_dca_graduated(self, bar: Any, lookback: pd.DataFrame, idx: int) -> str | tuple | None:
        """Graduated DCA: EMA50/EMA200 기반 포지션 크기 자동 조정.

        Test C 최우수 변형 (baseline 대비 +34.3%p 개선):
          price > EMA50  → 전체 크기 매수 (ratio=1.0)
          price > EMA200 → 절반 크기 매수 (ratio=0.5)
          price ≤ EMA200 → 진입 건너뜀 (강한 하락장 방어)

        청산: TP +5% 또는 SL -3%
        """
        closes = pd.Series(lookback["close"].values.astype(float))
        price = float(bar["close"])

        ema50 = float(closes.ewm(span=50, adjust=False).mean().iloc[-1])
        ema200 = float(closes.ewm(span=200, adjust=False).mean().iloc[-1])

        if idx % 24 == 0 and self._position is None:
            if price > ema50:
                return "buy"  # 전체 크기
            elif price > ema200:
                return ("buy", 0.5)  # 절반 크기
            # EMA200 이하: 건너뜀
            return None

        if self._position is not None:
            pnl_pct = self._unrealized_pnl(bar) / self._initial_capital
            if pnl_pct > 0.05 or pnl_pct < -0.03:
                return "close"
        return None

    def _signal_combined_v2(self, bar: Any, lookback: pd.DataFrame, idx: int) -> str | tuple | None:
        """Combined v2: FA 우선 + graduated DCA 보조 (그리드 완전 제거).

        포트폴리오 설계 (Test H 기준):
          FA     : 50% 자본 (횡보/고변동 레짐)
          DCA    : 10% 자본 (상승 추세)
          현금   : 나머지

        구현 방식 (단일 포지션 엔진):
          1. FA 진입 신호 → 50% 크기로 포지션 오픈
          2. FA 신호 없을 때 → graduated DCA를 10% 크기로 매수
          3. 청산: FA 역전 3회 또는 DCA TP/SL
        """
        # ── FA 시그널 우선 체크 ──────────────────────────────────────────
        fa_sig = self._signal_funding_arb(bar, lookback, idx)

        if self._position is not None:
            src = self._position.get("combined_v2_src", "fa")
            if fa_sig == "close" and src == "fa":
                return "close"
            if src == "dca":
                dca_sig = self._signal_dca_graduated(bar, lookback, idx)
                if dca_sig == "close":
                    return "close"
            return None

        # 포지션 없음
        if fa_sig in ("buy", "sell"):
            # FA 진입: 50% 자본
            self._next_size_ratio = 0.50
            # 포지션에 출처 표시 (이후 close 신호 라우팅용)
            self._pending_combined_src = "fa"
            return fa_sig

        # FA 없으면 graduated DCA (10% 자본)
        dca_sig = self._signal_dca_graduated(bar, lookback, idx)
        if dca_sig == "buy":
            self._next_size_ratio = 0.10
            self._pending_combined_src = "dca"
            return "buy"
        elif isinstance(dca_sig, tuple) and dca_sig[0] == "buy":
            self._next_size_ratio = 0.10 * dca_sig[1]
            self._pending_combined_src = "dca"
            return "buy"

        return None

    # ------------------------------------------------------------------
    # Position management
    # ------------------------------------------------------------------

    def _open_position(self, bar: Any, side: str, idx: int = 0) -> None:
        entry = float(bar["close"])
        size = (self._equity * 0.95 * self._next_size_ratio) / entry  # 95% * ratio
        fee = entry * size * self._fee_rate
        self._equity -= fee

        is_delta_neutral = self._strategy in ("funding_arb", "combined_v2") and \
                           getattr(self, "_pending_combined_src", None) != "dca"

        self._position = {
            "side": side,
            "entry_price": entry,
            "size": size,
            "entry_ts": bar.get("ts", bar.name) if hasattr(bar, "name") else None,
            "entry_idx": idx,
            "fee_paid": fee,
            # funding_arb: +1 = short perp (positive funding), -1 = long perp (negative funding)
            "funding_direction": 1 if side == "sell" else -1,
            # 델타 뉴트럴 여부: funding_arb / combined_v2 FA 레그
            "delta_neutral": is_delta_neutral,
            # 누적 펀딩 수취/지불액 추적 (trade PnL 보고용)
            "funding_accumulated": 0.0,
            # combined_v2: 시그널 출처 추적 (fa / dca)
            "combined_v2_src": getattr(self, "_pending_combined_src", None),
        }
        self._pending_combined_src = None  # reset

    def _close_position(self, bar: Any) -> None:
        if self._position is None:
            return

        size  = self._position["size"]
        entry = self._position["entry_price"]
        side  = self._position["side"]
        entry_ts  = self._position.get("entry_ts")
        close_ts  = bar.get("ts", bar.name) if hasattr(bar, "name") else None
        fee_entry = self._position.get("fee_paid", 0.0)

        if self._position.get("delta_neutral", False):
            # ── 델타 뉴트럴 (funding_arb) ────────────────────────────────
            # 레그1(perp) + 레그2(spot) 가격 PnL이 정확히 상쇄됨.
            # equity에는 이미 펀딩 수취/지불이 누적되어 있다.
            # 청산 시 exit fee만 차감하면 된다.
            exit_price = float(bar["close"])
            fee_exit   = exit_price * size * self._fee_rate
            self._equity -= fee_exit   # 입장 수수료는 _open_position에서 차감됨

            # trade PnL = 누적 펀딩 - 진입 수수료 - 청산 수수료
            net_pnl = self._position.get("funding_accumulated", 0.0) - fee_entry - fee_exit
        else:
            # ── 단일 레그 (grid, dca, combined) ─────────────────────────
            exit_price = float(bar["close"])
            if side == "buy":
                pnl = (exit_price - entry) * size
            else:
                pnl = (entry - exit_price) * size
            fee_exit = exit_price * size * self._fee_rate
            net_pnl  = pnl - fee_exit
            self._equity += net_pnl

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
                fee=fee_entry + fee_exit,
                duration_hours=0.0,
            )
        )
        self._position = None

    def _unrealized_pnl(self, bar: Any) -> float:
        if self._position is None:
            return 0.0
        # 델타 뉴트럴: 두 레그(perp + spot)의 가격 손익이 상쇄 → 0
        if self._position.get("delta_neutral", False):
            return 0.0
        price = float(bar["close"])
        entry = self._position["entry_price"]
        size  = self._position["size"]
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
