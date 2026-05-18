#!/usr/bin/env python3
"""
p2_aggregate.py — SQLite → all_results.csv + top20.md

Usage (host or inside container):
    python3 p2_aggregate.py --db /result/p1_optimization/queue.sqlite3 \
                            --output-dir /result/p1_optimization
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

CHAMPION_HP = {
    'st_factor': 2.5,
    'st_period': 6,
    'fast_ema_len': 7,
    'slow_ema_len': 20,
    'direction_ema_len': 200,
    'atr_mult': 3.0,
    'sl_margin_pct': 0.0,
}

COLS = ['combo_id', 'st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
        'direction_ema_len', 'atr_mult', 'sl_margin_pct',
        'status', 'cagr', 'mdd', 'sharpe', 'trades', 'multiplier', 'win_rate']


def load_db(db_path: str) -> list[dict]:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"SELECT {', '.join(COLS)} FROM jobs ORDER BY combo_id"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def write_csv(rows: list[dict], output_dir: Path) -> Path:
    csv_path = output_dir / 'all_results.csv'
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=COLS)
        writer.writeheader()
        writer.writerows(rows)
    return csv_path


def is_champion(row: dict) -> bool:
    for k, v in CHAMPION_HP.items():
        if abs(row.get(k, -999) - v) > 0.001:
            return False
    return True


def write_top20(rows: list[dict], output_dir: Path) -> Path:
    done = [r for r in rows if r['status'] == 'done' and r['multiplier'] is not None]
    done.sort(key=lambda r: r['multiplier'] or 0, reverse=True)

    top20 = done[:20]

    # ensure champion is in list
    champ_row = next((r for r in done if is_champion(r)), None)
    champ_in_top20 = any(is_champion(r) for r in top20)

    n_done = len(done)
    n_error = sum(1 for r in rows if r['status'] == 'error')

    lines = [
        "# p1 최적화 — Top 20 (multiplier 정렬)",
        "",
        f"완료: {n_done} / {len(rows)} | 오류: {n_error}",
        "",
        "| 순위 | combo_id | st_f | st_p | f_ema | s_ema | d_ema | atr_m | sl% | "
        "mult | CAGR% | MDD% | Sharpe | trades | 비고 |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]

    for rank, r in enumerate(top20, 1):
        note = "**챔피언**" if is_champion(r) else ""
        lines.append(
            f"| {rank} | {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
            f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
            f"{r['atr_mult']} | {r['sl_margin_pct']:.0f} | "
            f"{r['multiplier']:.2f}x | {r['cagr']:.1f} | {r['mdd']:.1f} | "
            f"{r['sharpe']:.3f} | {r['trades']} | {note} |"
        )

    if not champ_in_top20 and champ_row:
        champ_rank = done.index(champ_row) + 1
        lines += [
            "",
            f"**챔피언 (rank #{champ_rank})**:",
            "",
            "| — | combo_id | st_f | st_p | f_ema | s_ema | d_ema | atr_m | sl% | "
            "mult | CAGR% | MDD% | Sharpe | trades | 비고 |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
            f"| #{champ_rank} | {champ_row['combo_id']} | {champ_row['st_factor']} | "
            f"{champ_row['st_period']} | {champ_row['fast_ema_len']} | "
            f"{champ_row['slow_ema_len']} | {champ_row['direction_ema_len']} | "
            f"{champ_row['atr_mult']} | {champ_row['sl_margin_pct']:.0f} | "
            f"{champ_row['multiplier']:.2f}x | {champ_row['cagr']:.1f} | "
            f"{champ_row['mdd']:.1f} | {champ_row['sharpe']:.3f} | "
            f"{champ_row['trades']} | **챔피언** |",
        ]

    md_path = output_dir / 'top20.md'
    md_path.write_text('\n'.join(lines) + '\n')
    return md_path


def print_summary(rows: list[dict]) -> None:
    done = [r for r in rows if r['status'] == 'done' and r['multiplier'] is not None]
    if not done:
        print("No done rows.")
        return
    done.sort(key=lambda r: r['multiplier'] or 0, reverse=True)
    best = done[0]
    champ = next((r for r in done if is_champion(r)), None)

    print(f"\n--- Summary ---")
    print(f"  Done: {len(done)} / {len(rows)}")
    print(f"  Best: combo_id={best['combo_id']} mult={best['multiplier']:.2f}x "
          f"cagr={best['cagr']:.1f}% mdd={best['mdd']:.1f}%")
    if champ:
        print(f"  Champion: mult={champ['multiplier']:.2f}x cagr={champ['cagr']:.1f}% "
              f"mdd={champ['mdd']:.1f}%")
        if best['multiplier'] and champ['multiplier']:
            improvement = (best['multiplier'] / champ['multiplier'] - 1) * 100
            print(f"  vs Champion: {improvement:+.1f}%")


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', type=str, default='/result/p1_optimization/queue.sqlite3')
    p.add_argument('--output-dir', type=str, default='/result/p1_optimization')
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = load_db(args.db)
    print(f"Loaded {len(rows)} rows from DB")

    csv_path = write_csv(rows, output_dir)
    print(f"CSV written: {csv_path}")

    md_path = write_top20(rows, output_dir)
    print(f"Top20 written: {md_path}")

    print_summary(rows)
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
