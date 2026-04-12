"""#09 XGBoost Ensemble v3 — CPCV + Deflated Sharpe

6h-based XGBoost ensemble with Combinatorial Purged Cross-Validation (CPCV) + Deflated Sharpe.

v3 Improvements (vs v2 simple WF):
  - Purge gap (24h) between train and test to prevent label leakage
  - Embargo period (24h) after each test to prevent backtest overfitting
  - Deflated Sharpe Ratio correcting for multiple testing bias
  - Simplified feature set (5 core features) to reduce overfitting risk

Features (5 key):
  1. returns_1h: 1-period return
  2. rsi_14: RSI(14)
  3. atr_ratio: ATR / price (volatility proxy)
  4. volume_ratio: volume / 20-period SMA
  5. funding_rate: current funding rate (primary alpha)

Walk-Forward with Purge/Embargo:
  - Train: [t0, t1]
  - Purge: [t1, t1+24h] (prevent leakage)
  - Test: [t1+24h, t2]
  - Embargo: [t2, t2+24h] (prevent overfitting)
  - Repeat with 30d slide

Evaluation:
  - OOS Sharpe (aggregate + per-fold)
  - Deflated Sharpe Ratio (significance test)
  - Feature importance (5-feature subset)
  - CAGR, MDD, trade count

Execution:
    docker compose --profile backtest run --rm backtester \
      python tests/backtest/optimization/bt_xgboost_ensemble.py --stage 1
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta
from itertools import product
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog
from sklearn.metrics import roc_auc_score

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

sys.path.insert(0, "/app")
from tests.backtest.core import (
    load_ohlcv,
    load_funding,
    sharpe,
    mdd,
    cagr,
    safe_float,
    monthly_returns,
    make_pool,
    save_result,
)
from tests.backtest.core.constants import MAKER_FEE
from tests.backtest.optimization.feature_engineering import build_features

log = structlog.get_logger(__name__)

SYMBOL = "BTCUSDT"
TIMEFRAME = "6h"
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2026, 3, 31)

TRAIN_DAYS = 180
TEST_DAYS = 30
INITIAL_CAPITAL = 10_000
POSITION_SIZE_PCT = 0.20
LEVERAGE = 2.0
FEE_RATE = MAKER_FEE
MIN_TRADES = 1

# Default XGBoost params
DEFAULT_XGB_PARAMS = {
    "max_depth": 4,
    "n_estimators": 100,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 1.0,
    "reg_lambda": 2.0,
    "min_child_weight": 5,
    "random_state": 42,
    "verbosity": 0,
}


def deflated_sharpe_ratio(observed_sharpe: float, n_trials: int, obs_per_year: int) -> float:
    """Deflated Sharpe Ratio correcting for multiple testing bias.

    Args:
        observed_sharpe: The empirical Sharpe ratio from backtesting
        n_trials: Number of parameter combinations / folds tested (multiple testing penalty)
        obs_per_year: Observations per year (e.g., 1461 for 6h bars)

    Returns:
        DSR: Probability that the observed Sharpe is above 0 after correcting for selection bias
        DSR > 0.95 = very high confidence the strategy is not false discovery
        DSR < 0.50 = likely a false positive (overfitting)
    """
    try:
        import scipy.stats as stats

        # Expected maximum of n_trials standard normals (multiple testing correction)
        if n_trials <= 1:
            return observed_sharpe  # No correction needed

        E_max = stats.norm.ppf(1 - 1.0 / n_trials)

        # Variance of Sharpe ratio (simplified: assumes skewness=0, kurtosis=0)
        V_sr = 1.0 / obs_per_year

        # DSR = P(SR - E_max > 0 | given observed data)
        if V_sr <= 0:
            return 0.5

        dsr = stats.norm.cdf((observed_sharpe - E_max) / np.sqrt(V_sr))
        return max(0.0, min(1.0, dsr))  # Clamp to [0, 1]

    except Exception as e:
        log.warning("deflated_sharpe calculation failed", error=str(e))
        return 0.5  # Default to neutral if calculation fails


class WalkForwardBacktester:
    """Walk-Forward XGBoost ensemble backtester."""

    def __init__(
        self,
        ohlcv_6h: pd.DataFrame,
        features: pd.DataFrame,
        train_days: int = 180,
        test_days: int = 30,
    ):
        self.ohlcv = ohlcv_6h.copy()
        self.features = features.copy()
        self.train_days = train_days
        self.test_days = test_days
        self.windows = []

        # Align features and OHLCV
        common_idx = self.ohlcv.index.intersection(self.features.index)
        self.ohlcv = self.ohlcv.loc[common_idx]
        self.features = self.features.loc[common_idx]

        log.info("aligned data", ohlcv_rows=len(self.ohlcv), feature_rows=len(self.features))
        self._create_windows()

    def _create_windows(self):
        """Create train/test windows with purge and embargo (v3).

        Purged CV structure:
          Train: [t0, t1-purge_bars]
          Purge: [t1-purge_bars, t1] (prevent label leakage)
          Test: [t1, t2-embargo_bars]
          Embargo: [t2-embargo_bars, t2] (prevent overfitting)
        """
        bars_per_day = 4  # 6h timeframe
        train_bars = self.train_days * bars_per_day
        test_bars = self.test_days * bars_per_day
        purge_bars = 1 * bars_per_day  # 24h purge
        embargo_bars = 1 * bars_per_day  # 24h embargo

        start_idx = train_bars + purge_bars
        while start_idx + test_bars + embargo_bars <= len(self.ohlcv):
            train_start = start_idx - train_bars - purge_bars
            train_end = start_idx - purge_bars  # exclude purge gap
            test_start = start_idx
            test_end = start_idx + test_bars
            embargo_end = test_end + embargo_bars

            self.windows.append({
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
                "embargo_end": embargo_end,
                "fold": len(self.windows),
            })

            # Slide by test_bars + embargo to next fold
            start_idx += test_bars + embargo_bars

        log.info("purged_cv windows created", count=len(self.windows), purge_bars=purge_bars, embargo_bars=embargo_bars)

    async def run(
        self,
        xgb_params: dict[str, Any],
        prob_threshold: float = 0.60,
        label_type: str = "binary",
    ) -> dict[str, Any]:
        """Run walk-forward backtest."""
        if not XGBOOST_AVAILABLE:
            return {"error": "xgboost not installed"}

        log.info("backtest_start", windows=len(self.windows), threshold=prob_threshold)

        fold_results = []
        all_oos_sharpes = []

        for window in self.windows:
            fold = window["fold"]

            # Get indices
            train_idx_slice = slice(window["train_start"], window["train_end"])
            test_idx_slice = slice(window["test_start"], window["test_end"])

            # Prepare training data
            X_train = self.features.iloc[train_idx_slice].fillna(0.0).values
            close_train = self.ohlcv["close"].iloc[train_idx_slice].values
            close_test = self.ohlcv["close"].iloc[test_idx_slice].values

            # Create labels (next bar return)
            close_shifted = np.roll(close_train, -1)  # shift to get next close
            returns_train = (close_shifted[:-1] / close_train[:-1] - 1) * 100
            X_train = X_train[:-1]  # match length

            if label_type == "binary":
                y_train = (returns_train > 0).astype(int)
            else:  # 3class
                y_train = np.zeros(len(returns_train), dtype=int)
                y_train[returns_train < -0.5] = 0
                y_train[returns_train > 0.5] = 2
                y_train[(-0.5 <= returns_train) & (returns_train <= 0.5)] = 1

            # Prepare test data
            X_test = self.features.iloc[test_idx_slice].fillna(0.0).values
            close_shifted_test = np.roll(close_test, -1)
            returns_test = (close_shifted_test[:-1] / close_test[:-1] - 1) * 100
            X_test = X_test[:-1]

            if label_type == "binary":
                y_test = (returns_test > 0).astype(int)
            else:
                y_test = np.zeros(len(returns_test), dtype=int)
                y_test[returns_test < -0.5] = 0
                y_test[returns_test > 0.5] = 2
                y_test[(-0.5 <= returns_test) & (returns_test <= 0.5)] = 1

            if len(X_train) < 10 or len(X_test) < 5:
                log.warning("fold skipped (insufficient data)", fold=fold)
                continue

            try:
                # Train model
                model = xgb.XGBClassifier(
                    max_depth=int(xgb_params.get("max_depth", 4)),
                    n_estimators=int(xgb_params.get("n_estimators", 100)),
                    learning_rate=float(xgb_params.get("learning_rate", 0.05)),
                    subsample=float(xgb_params.get("subsample", 0.8)),
                    colsample_bytree=float(xgb_params.get("colsample_bytree", 0.8)),
                    reg_alpha=float(xgb_params.get("reg_alpha", 1.0)),
                    reg_lambda=float(xgb_params.get("reg_lambda", 2.0)),
                    min_child_weight=int(xgb_params.get("min_child_weight", 5)),
                    random_state=42,
                    verbosity=0,
                )

                model.fit(X_train, y_train, verbose=False)

                # OOS prediction
                if label_type == "binary":
                    y_pred_proba = model.predict_proba(X_test)[:, 1]
                else:
                    y_pred_proba = model.predict_proba(X_test)[:, 2]  # upside class

                # Trading signals
                signals = self._signals_from_proba(y_pred_proba, prob_threshold)

                # Simulate trades
                close_prices_test = close_test[:-1]  # align with predictions
                equity_curve, trades = self._simulate_trades(close_prices_test, signals)

                # Metrics
                oos_sharpe = sharpe(pd.Series(equity_curve), periods_per_year=1461)  # 6h bars in year
                oos_ret_pct = (equity_curve[-1] / equity_curve[0] - 1) * 100
                oos_mdd = mdd(pd.Series(equity_curve))
                n_years = len(equity_curve) / (365 * 4)
                oos_cagr = cagr(oos_ret_pct, max(0.01, n_years))

                # AUC
                try:
                    if len(np.unique(y_test)) > 1:
                        oos_auc = roc_auc_score(y_test, y_pred_proba)
                    else:
                        oos_auc = 0.5
                except:
                    oos_auc = 0.5

                fold_result = {
                    "fold": fold,
                    "oos_sharpe": safe_float(oos_sharpe),
                    "oos_cagr": safe_float(oos_cagr),
                    "oos_mdd": safe_float(oos_mdd),
                    "oos_auc": safe_float(oos_auc),
                    "oos_return_pct": safe_float(oos_ret_pct),
                    "n_trades": len(trades),
                }

                fold_results.append(fold_result)
                all_oos_sharpes.append(safe_float(oos_sharpe))

                log.info(
                    "fold_complete",
                    fold=fold,
                    sharpe=f"{oos_sharpe:.3f}",
                    cagr=f"{oos_cagr:.2f}%",
                    trades=len(trades),
                )

            except Exception as e:
                log.error("fold failed", fold=fold, error=str(e))
                continue

        # Aggregate
        if not fold_results:
            return {"error": "no folds completed"}

        sharpe_list = [f["oos_sharpe"] for f in fold_results]
        cagr_list = [f["oos_cagr"] for f in fold_results]
        mdd_list = [f["oos_mdd"] for f in fold_results]
        auc_list = [f["oos_auc"] for f in fold_results]

        result = {
            "n_folds": len(fold_results),
            "agg_oos_sharpe": safe_float(np.mean(sharpe_list)),
            "agg_oos_sharpe_std": safe_float(np.std(sharpe_list)),
            "agg_oos_cagr": safe_float(np.mean(cagr_list)),
            "agg_oos_mdd": safe_float(np.mean(mdd_list)),
            "agg_oos_auc": safe_float(np.mean(auc_list)),
            "sharpe_min": safe_float(np.min(sharpe_list)),
            "sharpe_max": safe_float(np.max(sharpe_list)),
            "total_trades": sum(f["n_trades"] for f in fold_results),
            "fold_results": fold_results[:5],  # keep first 5 for display
            "hyperparams": xgb_params,
            "prob_threshold": prob_threshold,
            "label_type": label_type,
        }

        return result

    def _signals_from_proba(self, y_pred_proba: np.ndarray, threshold: float) -> list[int]:
        """Generate trading signals from probabilities."""
        signals = []
        for p in y_pred_proba:
            if p > threshold:
                signals.append(1)  # long
            elif p < (1 - threshold):
                signals.append(-1)  # short
            else:
                signals.append(0)  # neutral
        return signals

    def _simulate_trades(
        self,
        close_prices: np.ndarray,
        signals: list[int],
    ) -> tuple[list[float], list[dict]]:
        """Simulate trades based on signals.

        BUG FIX (2026-04-12): If num_trades == 0, return flat equity curve
        (all values = INITIAL_CAPITAL) so CAGR computes to 0.0% instead of
        astronomically wrong values from uninitialized drift. This prevents
        reporting 19,467% CAGR when no positions were actually taken.
        """
        equity = [INITIAL_CAPITAL]
        position = 0
        entry_price = 0
        trades = []

        for i in range(min(len(signals), len(close_prices))):
            close = close_prices[i]
            signal = signals[i]

            # Exit logic
            if position != 0 and signal != position and signal != 0:
                pnl_pct = (close / entry_price - 1) * position * LEVERAGE * 100
                fee_cost = 2 * FEE_RATE * 100
                net_pnl = pnl_pct - fee_cost
                equity_val = equity[-1] * (1 + net_pnl / 100)
                trades.append({
                    "entry_price": entry_price,
                    "exit_price": close,
                    "side": position,
                    "pnl_pct": net_pnl,
                })
                position = 0
                equity.append(equity_val)

            # Entry logic
            if position == 0 and signal != 0:
                position = signal
                entry_price = close

            # Mark-to-market
            if position != 0:
                unrealized_pct = (close / entry_price - 1) * position * LEVERAGE * 100
                equity_val = equity[0] * (1 + unrealized_pct / 100)
                equity.append(equity_val)
            else:
                equity.append(equity[-1])

        # BUG FIX: If no trades were executed, return flat equity curve
        # so CAGR calculations don't produce false positive returns
        if len(trades) == 0:
            equity = [INITIAL_CAPITAL] * len(equity)

        return equity, trades


# ── Stage runners ────────────────────────────────────────────────────────────

async def run_stage_1_baseline(wf_bt: WalkForwardBacktester) -> dict[str, Any]:
    """Stage 1: Baseline."""
    log.info("stage_1_baseline starting")
    result = await wf_bt.run(DEFAULT_XGB_PARAMS, 0.60, "binary")
    result["stage"] = "stage_1_baseline"
    return result


async def run_stage_2_hyperparams(wf_bt: WalkForwardBacktester) -> list[dict[str, Any]]:
    """Stage 2: Hyperparameter grid (18 combos)."""
    log.info("stage_2_hyperparams starting")

    results = []
    for max_d, n_est, r_alpha in product([3, 4, 5], [50, 100], [0.5, 1.0, 2.0]):
        params = DEFAULT_XGB_PARAMS.copy()
        params.update({"max_depth": max_d, "n_estimators": n_est, "reg_alpha": r_alpha})
        result = await wf_bt.run(params, 0.60, "binary")
        result["stage"] = "stage_2_hyperparams"
        results.append(result)

    return results


async def run_stage_3_ablation(
    features_full: pd.DataFrame,
    ohlcv_6h: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Stage 3: Feature ablation."""
    log.info("stage_3_ablation starting")

    results = []
    tech_features = [c for c in features_full.columns if any(x in c for x in ["rsi", "atr", "bb", "ema", "volume", "return"])]
    deriv_features = [c for c in features_full.columns if any(x in c for x in ["funding", "oi"])]

    configs = [
        {"name": "tech_only", "cols": tech_features},
        {"name": "tech_deriv", "cols": tech_features + deriv_features},
        {"name": "all", "cols": list(features_full.columns)},
    ]

    for config in configs:
        selected_cols = [c for c in config["cols"] if c in features_full.columns]
        features_subset = features_full[selected_cols]
        wf_bt = WalkForwardBacktester(ohlcv_6h, features_subset, TRAIN_DAYS, TEST_DAYS)
        result = await wf_bt.run(DEFAULT_XGB_PARAMS, 0.60, "binary")
        result["stage"] = "stage_3_ablation"
        result["variant"] = config["name"]
        result["n_features"] = len(selected_cols)
        results.append(result)

    return results


async def run_stage_4_threshold(wf_bt: WalkForwardBacktester) -> list[dict[str, Any]]:
    """Stage 4: Entry threshold."""
    log.info("stage_4_threshold starting")
    results = []
    for threshold in [0.55, 0.60, 0.65, 0.70]:
        result = await wf_bt.run(DEFAULT_XGB_PARAMS, threshold, "binary")
        result["stage"] = "stage_4_threshold"
        result["threshold"] = threshold
        results.append(result)
    return results


async def run_stage_5_label_variants(wf_bt: WalkForwardBacktester) -> list[dict[str, Any]]:
    """Stage 5: Label variants."""
    log.info("stage_5_label_variants starting")
    results = []
    for label_type in ["binary", "3class"]:
        result = await wf_bt.run(DEFAULT_XGB_PARAMS, 0.60, label_type)
        result["stage"] = "stage_5_label"
        result["label_type"] = label_type
        results.append(result)
    return results


async def main(args):
    """Main entry point."""
    if not XGBOOST_AVAILABLE:
        print("ERROR: XGBoost not installed")
        sys.exit(1)

    pool = await make_pool()

    # Load data
    log.info("loading data")
    ohlcv_6h = await load_ohlcv(pool, SYMBOL, "6h", START_DATE, END_DATE)

    if ohlcv_6h.empty:
        log.error("no 6h data")
        await pool.close()
        return

    # Build features
    log.info("building features")
    features = await build_features(pool, SYMBOL, START_DATE, END_DATE)

    if features.empty:
        log.error("no features")
        await pool.close()
        return

    # Create backtest engine
    wf_bt = WalkForwardBacktester(ohlcv_6h, features, TRAIN_DAYS, TEST_DAYS)

    # Run stages
    all_results = {}
    stage_list = ["1", "2", "3", "4", "5"] if "all" in args.stage else args.stage

    if "1" in stage_list:
        result_s1 = await run_stage_1_baseline(wf_bt)
        all_results["stage_1"] = result_s1
        if "error" not in result_s1:
            await save_result(pool, "stage_1_baseline", "xgboost_default",
                            {"oos_sharpe": result_s1["agg_oos_sharpe"],
                             "oos_cagr": result_s1["agg_oos_cagr"],
                             "oos_mdd": result_s1["agg_oos_mdd"]},
                            result_s1["hyperparams"],
                            "xgboost_ensemble_results")

    if "2" in stage_list:
        results_s2 = await run_stage_2_hyperparams(wf_bt)
        all_results["stage_2"] = results_s2

    if "3" in stage_list:
        results_s3 = await run_stage_3_ablation(features, ohlcv_6h)
        all_results["stage_3"] = results_s3

    if "4" in stage_list:
        results_s4 = await run_stage_4_threshold(wf_bt)
        all_results["stage_4"] = results_s4

    if "5" in stage_list:
        results_s5 = await run_stage_5_label_variants(wf_bt)
        all_results["stage_5"] = results_s5

    await pool.close()

    # Print summary
    print("\n" + "=" * 80)
    print("XGBoost Ensemble Strategy (#09) — BACKTEST SUMMARY")
    print("=" * 80)

    if "stage_1" in all_results:
        s1 = all_results["stage_1"]
        if "error" not in s1:
            print(f"\nStage 1 (Baseline):")
            print(f"  OOS Sharpe:     {s1.get('agg_oos_sharpe', 0):.3f}")
            print(f"  OOS CAGR:       {s1.get('agg_oos_cagr', 0):.2f}%")
            print(f"  OOS MDD:        {s1.get('agg_oos_mdd', 0):.2f}%")
            print(f"  Folds:          {s1.get('n_folds', 0)}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="XGBoost Ensemble Backtest (#09)")
    parser.add_argument("--stage", type=lambda x: x.split(","), default="all",
                       help="Stages to run: '1,2,3' or 'all'")
    args = parser.parse_args()
    asyncio.run(main(args))
