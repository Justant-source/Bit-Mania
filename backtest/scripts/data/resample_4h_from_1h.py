"""resample_4h_from_1h.py — 1h 데이터에서 4h를 리샘플링.

Binance Vision은 spot 4h를 제공하지 않으므로
2017-08 ~ 2019-09-07 구간의 4h는 1h에서 리샘플.
결과는 backtest/data/ohlcv/BTCUSDT/4h/YYYY/MM.parquet 로 저장.
이미 존재하는 파일은 덮어쓰지 않음 (--force 옵션으로 강제 덮어쓰기 가능).
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

try:
    from _paths import OHLCV_ROOT
except ImportError:
    OHLCV_ROOT = Path(__file__).parent.parent.parent / "data" / "ohlcv"

SYMBOL = "BTCUSDT"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_1h(start: str, end: str) -> pd.DataFrame:
    """1h 파일들을 로드해서 합침. open_time/timestamp 컬럼 양쪽 처리."""
    src_dir = OHLCV_ROOT / SYMBOL / "1h"
    frames = []
    for f in sorted(src_dir.rglob("*.parquet")):
        df = pd.read_parquet(f)
        # 컬럼명 정규화: open_time → timestamp
        if "open_time" in df.columns and "timestamp" not in df.columns:
            df = df.rename(columns={"open_time": "timestamp"})
        df = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
        frames.append(df)
    if not frames:
        raise FileNotFoundError(f"No 1h parquet found in {src_dir}")
    df_all = pd.concat(frames, ignore_index=True)
    df_all["timestamp"] = pd.to_datetime(df_all["timestamp"], utc=True)
    df_all = df_all.sort_values("timestamp").drop_duplicates("timestamp")
    mask = (df_all["timestamp"] >= pd.Timestamp(start, tz="UTC")) & \
           (df_all["timestamp"] <= pd.Timestamp(end, tz="UTC"))
    return df_all[mask].reset_index(drop=True)


def resample_to_4h(df_1h: pd.DataFrame) -> pd.DataFrame:
    """1h DataFrame → 4h OHLCV."""
    df = df_1h.set_index("timestamp")
    df_4h = df.resample("4h", label="left", closed="left").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).dropna(subset=["open"]).reset_index()
    df_4h["source"] = "spot_resampled"
    return df_4h


def save_monthly(df_4h: pd.DataFrame, force: bool = False) -> int:
    """월별 Parquet 저장. 기존 파일이 있으면 스킵 (force=True 시 덮어씀)."""
    out_dir_base = OHLCV_ROOT / SYMBOL / "4h"
    saved = 0
    for (year, month), grp in df_4h.groupby(
        [df_4h["timestamp"].dt.year, df_4h["timestamp"].dt.month]
    ):
        out_dir = out_dir_base / str(year)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{month:02d}.parquet"
        if out_file.exists() and not force:
            logger.info(f"  SKIP (exists): {out_file.name}")
            continue
        grp = grp.sort_values("timestamp").reset_index(drop=True)
        grp.to_parquet(out_file, engine="pyarrow", compression="zstd", index=False)
        logger.info(f"  SAVED {out_file} ({len(grp)} rows)")
        saved += len(grp)
    return saved


def main() -> None:
    parser = argparse.ArgumentParser(description="Resample 1h → 4h for spot era")
    parser.add_argument("--start", default="2017-08-01")
    parser.add_argument("--end",   default="2019-09-07")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    logger.info(f"Loading 1h data {args.start} ~ {args.end}...")
    df_1h = load_1h(args.start, args.end + " 23:59:59")
    logger.info(f"Loaded {len(df_1h)} 1h rows")

    df_4h = resample_to_4h(df_1h)
    logger.info(f"Resampled to {len(df_4h)} 4h rows")

    total = save_monthly(df_4h, force=args.force)
    logger.info(f"[DONE] Saved {total} rows to {OHLCV_ROOT / SYMBOL / '4h'}/")


if __name__ == "__main__":
    main()
