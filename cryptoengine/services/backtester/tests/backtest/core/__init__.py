"""core — 백테스트 공유 유틸리티 패키지.

빠른 임포트:
    from core.loader     import load_ohlcv, load_funding
    from core.metrics    import sharpe, mdd, cagr, safe_float, monthly_returns, profit_factor
    from core.db         import make_pool, save_result, DB_DSN, CREATE_VARIANT_TABLE
    from core.engine     import FAEngine, SimpleBacktester
    from core.constants  import TAKER_FEE, MAKER_FEE, SLIPPAGE_PCT, ...
    from core.exceptions import MissingDataError, DataQualityError, BacktestConfigError
    from core.data_loader import DataLoader
    from core.validation import validate_backtest_result, format_validation_report
"""
from .loader  import load_ohlcv, load_funding
from .metrics import sharpe, mdd, cagr, safe_float, monthly_returns, profit_factor
from .db      import make_pool, save_result, DB_DSN, CREATE_VARIANT_TABLE
from .engine  import FAEngine, SimpleBacktester
from .constants import (
    MAKER_FEE, TAKER_FEE, SLIPPAGE_PCT,
    ROUND_TRIP_FEE, ROUND_TRIP_FEE_FULL, ONE_WAY_FEE,
    FUNDING_INTERVAL_HOURS, FUNDING_INTERVALS_PER_DAY, FUNDING_INTERVALS_PER_YEAR,
    BYBIT_BTCUSDT_MIN_QTY, BYBIT_BTCUSDT_TICK_SIZE,
    MAX_LEVERAGE, DAILY_LOSS_KILL_SWITCH_PCT, MAX_DRAWDOWN_KILL_SWITCH_PCT,
    MIN_REGIME_CONFIDENCE, MIN_HOLD_BARS,
    FA_MIN_FUNDING_THRESHOLD, FA_MIN_CONSECUTIVE, FA_MAX_HOLD_BARS,
)
from .exceptions import MissingDataError, DataQualityError, BacktestConfigError
from .data_loader import DataLoader
from .validation import validate_backtest_result, format_validation_report

__all__ = [
    # loaders
    "load_ohlcv", "load_funding",
    # metrics
    "sharpe", "mdd", "cagr", "safe_float", "monthly_returns", "profit_factor",
    # database
    "make_pool", "save_result", "DB_DSN", "CREATE_VARIANT_TABLE",
    # engines
    "FAEngine", "SimpleBacktester",
    # constants
    "MAKER_FEE", "TAKER_FEE", "SLIPPAGE_PCT",
    "ROUND_TRIP_FEE", "ROUND_TRIP_FEE_FULL", "ONE_WAY_FEE",
    "FUNDING_INTERVAL_HOURS", "FUNDING_INTERVALS_PER_DAY", "FUNDING_INTERVALS_PER_YEAR",
    "BYBIT_BTCUSDT_MIN_QTY", "BYBIT_BTCUSDT_TICK_SIZE",
    "MAX_LEVERAGE", "DAILY_LOSS_KILL_SWITCH_PCT", "MAX_DRAWDOWN_KILL_SWITCH_PCT",
    "MIN_REGIME_CONFIDENCE", "MIN_HOLD_BARS",
    "FA_MIN_FUNDING_THRESHOLD", "FA_MIN_CONSECUTIVE", "FA_MAX_HOLD_BARS",
    # exceptions
    "MissingDataError", "DataQualityError", "BacktestConfigError",
    # data_loader
    "DataLoader",
    # validation
    "validate_backtest_result", "format_validation_report",
]
