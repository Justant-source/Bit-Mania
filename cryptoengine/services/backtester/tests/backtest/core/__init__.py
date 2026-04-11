"""core — 백테스트 공유 유틸리티 패키지.

빠른 임포트:
    from core.loader  import load_ohlcv, load_funding
    from core.metrics import sharpe, mdd, cagr, safe_float, monthly_returns, profit_factor
    from core.db      import make_pool, save_result, DB_DSN, CREATE_VARIANT_TABLE
    from core.engine  import FAEngine, SimpleBacktester
"""
from .loader  import load_ohlcv, load_funding
from .metrics import sharpe, mdd, cagr, safe_float, monthly_returns, profit_factor
from .db      import make_pool, save_result, DB_DSN, CREATE_VARIANT_TABLE
from .engine  import FAEngine, SimpleBacktester

__all__ = [
    "load_ohlcv", "load_funding",
    "sharpe", "mdd", "cagr", "safe_float", "monthly_returns", "profit_factor",
    "make_pool", "save_result", "DB_DSN", "CREATE_VARIANT_TABLE",
    "FAEngine", "SimpleBacktester",
]
