import os
from pathlib import Path

BT_ROOT = Path(__file__).parent.parent        # backtest/ (local) or /app (container)
SCRIPTS_ROOT = BT_ROOT / "scripts"
RUNTIME_ROOT = BT_ROOT / "runtime"

# ── Env-overridable paths (Docker container uses different mount points) ───────
# DATA_ROOT: local fallback checks backtest/data/, then ../data/ (project root)
_local_data = BT_ROOT / "data"
DATA_ROOT = Path(
    os.environ.get("BACKTEST_DATA_ROOT")
    or os.environ.get("DATA_DIR")
    or str(_local_data if _local_data.exists() else BT_ROOT.parent / "data")
)

RESULTS_ROOT = Path(os.environ.get("BACKTEST_RESULTS_ROOT", str(BT_ROOT / "results")))
DASHBOARDS_ROOT = Path(os.environ.get("BACKTEST_DASHBOARDS_ROOT", str(BT_ROOT / "dashboards")))

OHLCV_ROOT       = DATA_ROOT / "ohlcv"
FUNDING_ROOT     = DATA_ROOT / "funding"
FEAR_GREED_ROOT  = DATA_ROOT / "fear_greed"
MACRO_ROOT       = DATA_ROOT / "macro_events"
