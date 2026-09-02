"""v13 사전 확률 추정용 — 펀딩 반영 홀드아웃 전이검정 + 7개 plateau combo 대조.
홀드아웃(2025-01~2026-08)은 이미 2026-08-31에 1,536개 전부 관측된 '소진된' 구간이다 — 여기서는
v13의 성공 확률을 추정하는 사전(prior) 근거로만 쓰고, v13 자체의 확인용 홀드아웃으로는 쓸 수 없다.
"""
import sys, time, csv
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import numpy as np, pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import replay_lib as L
import _replay_supertrend as RS

FUNDING = {}
with open(ROOT / "backtest/results/funding/bybit_btcusdt_funding_8h.csv") as f:
    for r in csv.DictReader(f):
        FUNDING[int(r["timestamp_ms"])] = float(r["funding_rate"])
HO = RS.date_ms("2025-01-01")

def ho_one(params):
    trades, eq = L.run(params, start_ms=HO, funding=FUNDING)
    m = RS._metrics(trades, eq)
    row = dict(params); row.update(h_net=m["net_pct"], h_mdd=m["mdd"], h_cagr=m["cagr"], h_sharpe=m["sharpe"], h_trades=len(trades))
    return row

def design_blocks(params):
    trades, eq = L.run(params, end_ms=L.design_end_ms(), funding=FUNDING)
    return L.block_lg(eq)

if __name__ == "__main__":
    design = pd.read_parquet(ROOT / "backtest/results/2026-09-02/grid_scores_funding.parquet")
    combos = L.all_combos()
    t0 = time.time()
    rows = []
    with ProcessPoolExecutor(max_workers=8) as ex:
        for i, row in enumerate(ex.map(ho_one, combos, chunksize=8)):
            rows.append(row)
    ho = pd.DataFrame(rows)
    print(f"holdout(funding) 1536 done {time.time()-t0:.0f}s")
    df = design.merge(ho, on=L.PARAMS5)
    df.to_parquet(ROOT / "backtest/results/2026-09-02/holdout_grid_funding.parquet", index=False)

    # #7908 baseline: design(funding) + holdout(funding)
    b = L.BASELINE
    b_ho = ho_one(b)
    b_blocks = design_blocks(b)
    print(f"\n#7908 holdout(funding): net={b_ho['h_net']:.2f}% cagr={b_ho['h_cagr']:.2f}% mdd={b_ho['h_mdd']:.2f}% sharpe={b_ho['h_sharpe']:.3f}")
    print(f"#7908 design blocks (funding): {np.round(b_blocks,3).tolist()}  s_raw={np.median(b_blocks):.4f}")

    # transfer test under funding
    rho, p = spearmanr(df.s_raw, df.h_net)
    print(f"\n전이검정(펀딩반영): design s_raw -> holdout net Spearman rho={rho:.3f} p={p:.2e}")
    rho2, p2 = spearmanr(df.cagr, df.h_net); print(f"  design cagr -> holdout net rho={rho2:.3f}")
    rho3, p3 = spearmanr(df.mdd, df.h_mdd); print(f"  design mdd -> holdout mdd rho={rho3:.3f}")

    # base rates on holdout
    beat_both = (df.h_cagr >= b_ho["h_cagr"]) & (df.h_mdd >= b_ho["h_mdd"])
    print(f"\n홀드아웃(펀딩)에서 #7908을 CAGR·MDD 동시에 이기는 combo: {beat_both.sum()}/1536 ({beat_both.mean()*100:.1f}%)")
    print(f"  CAGR만: {(df.h_cagr >= b_ho['h_cagr']).sum()}, MDD만: {(df.h_mdd >= b_ho['h_mdd']).sum()}")
    print(f"  #7908 홀드아웃 net 백분위: {(df.h_net < b_ho['h_net']).mean()*100:.1f}")
    print(f"  #7908 홀드아웃 MDD 백분위: {(df.h_mdd < b_ho['h_mdd']).mean()*100:.1f}")

    # design-side dominance vs holdout-side dominance overlap
    dom_design = (df.cagr >= 211.803) & (df.mdd >= -69.021)
    print(f"\n설계(펀딩) 동시지배 {dom_design.sum()} ∩ 홀드아웃 동시지배 {beat_both.sum()} = {(dom_design & beat_both).sum()}")
    hp = df.hard_pass
    print(f"설계 hard_pass {hp.sum()} 중 홀드아웃 동시지배: {(hp & beat_both).sum()}")
    pl = df.plateau_pass
    print(f"설계 plateau_pass {pl.sum()} 중 홀드아웃 동시지배: {(pl & beat_both).sum()}")

    # the 7 plateau combos: holdout + block head-to-head
    print("\n=== 7개 plateau combo ===")
    cols = L.PARAMS5 + ["mdd", "cagr", "s_raw", "h_net", "h_cagr", "h_mdd", "h_sharpe"]
    P = df[pl].sort_values("s_raw", ascending=False)
    print(P[cols].round(3).to_string(index=False))
    print("\n블록별 head-to-head (설계 15블록, 펀딩반영): combo가 #7908을 이긴 블록 수")
    for _, r in P.iterrows():
        blocks = [r[f"lg{i}"] for i in range(15)]
        wins = sum(1 for a, c in zip(blocks, b_blocks) if a > c)
        diff = np.array(blocks) - np.array(b_blocks)
        print(f"  {L.key5({k: r[k] for k in L.PARAMS5})}: {wins}/15 승, 평균차 {diff.mean():+.3f}, 중앙차 {np.median(diff):+.3f}, 최대손실블록 {min(blocks):+.3f} vs #7908 {min(b_blocks):+.3f}")

    # top-holdout combos: what are they, and where did they rank in design?
    print("\n=== 홀드아웃 net 상위 10 (펀딩반영) 과 그 설계구간 순위 ===")
    df["design_rank"] = df.s_raw.rank(ascending=False)
    top = df.sort_values("h_net", ascending=False).head(10)
    print(top[L.PARAMS5 + ["h_net", "h_mdd", "s_raw", "design_rank", "hard_pass", "plateau_pass"]].round(3).to_string(index=False))
