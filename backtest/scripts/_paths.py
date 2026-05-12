from pathlib import Path

BT_ROOT = Path(__file__).parent.parent        # /…/Bit-Mania/backtest/
SCRIPTS_ROOT = BT_ROOT / "scripts"
DATA_ROOT = BT_ROOT / "data"
RESULTS_ROOT = BT_ROOT / "results"
RUNTIME_ROOT = BT_ROOT / "runtime"
DASHBOARDS_ROOT = BT_ROOT / "dashboards"

OHLCV_ROOT = DATA_ROOT / "ohlcv"
FUNDING_ROOT = DATA_ROOT / "funding"
FEAR_GREED_ROOT = DATA_ROOT / "fear_greed"
MACRO_ROOT = DATA_ROOT / "macro_events"
