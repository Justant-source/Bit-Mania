#!/usr/bin/env python3
"""
merge_funding_sources.py

Merge Binance gap-fill data (2019-09-08 ~ 2020-03-25) into BTCUSDT_8h.parquet.

The Bybit parquet has zero-filled rows for pre-Bybit months (2018-04 ~ 2020-02).
Binance BTCUSDT perpetual started 2019-09-08 and covers the gap until Bybit launched.

Merge logic:
  - For each Bybit row with funding_rate == 0 AND timestamp in Binance range:
      replace rate with the Binance rate at the nearest matching 8h timestamp
  - Bybit rows with rate != 0 are never overwritten
  - Binance timestamps are matched by finding the closest Bybit 8h slot

Result: BTCUSDT_8h.parquet gains ~6 months of real Binance data instead of zeros.
Original is backed up to BTCUSDT_8h.bybit_only.parquet.

Exit code: 0 on success, 1 on fatal error.
"""
import sys
from pathlib import Path

import pandas as pd
import numpy as np

BT_ROOT = Path(__file__).resolve().parent.parent.parent
BYBIT_PARQUET = BT_ROOT / "data" / "funding" / "BTCUSDT_8h.parquet"
BACKUP_PARQUET = BT_ROOT / "data" / "funding" / "BTCUSDT_8h.bybit_only.parquet"
BINANCE_PARQUET = BT_ROOT / "data" / "funding" / "binance_api" / "BTCUSDT_2019.parquet"

# Binance data covers 2019-09-08 → 2020-03-25; Bybit real data starts 2020-03-26
BINANCE_COVERAGE_END_MS = int(pd.Timestamp("2020-03-26", tz="UTC").timestamp() * 1000)


def main():
    if not BYBIT_PARQUET.exists():
        print(f"ERROR: {BYBIT_PARQUET} not found", file=sys.stderr)
        sys.exit(1)

    if not BINANCE_PARQUET.exists():
        print(f"ERROR: {BINANCE_PARQUET} not found — run fetch_binance_funding_api.py first", file=sys.stderr)
        sys.exit(1)

    # Load Bybit base
    bybit = pd.read_parquet(BYBIT_PARQUET).sort_values("timestamp").reset_index(drop=True)
    print(f"[bybit]   Loaded {len(bybit)} rows, range: "
          f"{pd.Timestamp(bybit['timestamp'].iloc[0], unit='ms', tz='UTC').date()} → "
          f"{pd.Timestamp(bybit['timestamp'].iloc[-1], unit='ms', tz='UTC').date()}")

    n_zero_before = int((bybit["funding_rate"] == 0).sum())
    print(f"[bybit]   Zero-rate rows: {n_zero_before}")

    # Load Binance gap fill
    binance = pd.read_parquet(BINANCE_PARQUET).sort_values("timestamp").reset_index(drop=True)
    print(f"[binance] Loaded {len(binance)} rows, range: "
          f"{pd.Timestamp(binance['timestamp'].iloc[0], unit='ms', tz='UTC').date()} → "
          f"{pd.Timestamp(binance['timestamp'].iloc[-1], unit='ms', tz='UTC').date()}")

    # Build Binance lookup: {timestamp_ms -> rate}
    # Binance events are also 8h-spaced; align to nearest Bybit slot (tolerance ±30 min)
    TOLERANCE_MS = 30 * 60 * 1000
    binance_ts = binance["timestamp"].to_numpy()
    binance_rates = binance["funding_rate"].to_numpy()

    # Only patch rows where: bybit rate == 0 AND timestamp < Bybit real start
    patch_mask = (bybit["funding_rate"] == 0) & (bybit["timestamp"] < BINANCE_COVERAGE_END_MS)
    patch_indices = bybit.index[patch_mask].to_numpy()
    print(f"[merge]   Rows eligible for patch: {len(patch_indices)}")

    n_patched = 0
    for idx in patch_indices:
        bybit_ts = int(bybit.at[idx, "timestamp"])
        # Find nearest Binance timestamp
        pos = np.searchsorted(binance_ts, bybit_ts)
        best_rate = None
        best_diff = TOLERANCE_MS + 1
        for p in (pos - 1, pos):
            if 0 <= p < len(binance_ts):
                diff = abs(int(binance_ts[p]) - bybit_ts)
                if diff <= TOLERANCE_MS and diff < best_diff:
                    best_diff = diff
                    best_rate = float(binance_rates[p])
        if best_rate is not None:
            bybit.at[idx, "funding_rate"] = best_rate
            n_patched += 1

    n_zero_after = int((bybit["funding_rate"] == 0).sum())
    print(f"[merge]   Patched: {n_patched} rows")
    print(f"[merge]   Zero-rate rows after merge: {n_zero_after} (was {n_zero_before})")

    # Backup original before overwriting
    if not BACKUP_PARQUET.exists():
        bybit_original = pd.read_parquet(BYBIT_PARQUET)
        bybit_original.to_parquet(BACKUP_PARQUET, engine="pyarrow", index=False)
        print(f"[backup]  Saved original → {BACKUP_PARQUET.name}")
    else:
        print(f"[backup]  Backup already exists → skipping overwrite of {BACKUP_PARQUET.name}")

    # Save merged
    bybit.to_parquet(BYBIT_PARQUET, engine="pyarrow", index=False)
    print(f"[done]    Wrote {len(bybit)} rows → {BYBIT_PARQUET}")
    print(f"          Real data coverage improved: {n_zero_before - n_zero_after} rows patched with Binance rates")


if __name__ == "__main__":
    main()
