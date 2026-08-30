# v12 Plateau report

grid n=1536, N_BLOCKS=15

C1 (MDD >= -66.697): 80/1536
C2 (pos_blocks >= 10/15): 1120/1536
C3 (trades >= 120): 1536/1536
C1+C2+C3 (hard_pass): 56/1536

C4 (neighbor pass ratio >= 0.7, of hard_pass combos): 0/56 hard_pass combos also form a plateau (max ratio observed: 0.625)

## Plateau-passing combos ranked by s_smooth (top 15 of 0)

(none)

## #7908 baseline: {'st_factor': 2.6, 'st_period': 9, 'fast_ema': 7, 'slow_ema': 29, 'dir_ema': 240, 'atr_mult': 3.3}
design-window: trades=164 mdd=-66.697 cagr=266.254 sharpe=1.778 s_raw=0.3239

Nearest grid point to #7908 (not #7908 itself, grid has no point there): {'st_factor': 2.6, 'st_period': 8, 'fast_ema': 6, 'slow_ema': 30, 'dir_ema': 220}
  that grid point's stats: trades=171 mdd=-70.771 cagr=240.958 s_raw=0.3866 s_smooth=0.3943 hard_pass=False plateau_pass=False

## VERDICT: NO_PLATEAU
No combo satisfies hard constraints (C1-C3) with >=8/10 grid-neighbors also satisfying them (C4). No candidate region exists in this grid. Per PREREGISTRATION.md §6/§9: stop here. G1-G9 and Phase 4 are NOT run. Conclusion: #7908 유지.