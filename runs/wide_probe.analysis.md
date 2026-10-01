# W1 wide raw-input 5 s probe — P1 FAILS; a real but tiny delayed-direction signal that only the MLP finds

*2026-10-01. Notebook `runs/btc_wide_probe.ipynb` (executed on Colab T4, 10/10 cells, ~23 min training,
peak host RAM 8.2 / 13.6 GB). Artifacts `runs/wide_probe_results.json`, `runs/wide_probe_scores.npz`.
Pre-registration: `next_signal_ideas.md` Round 3, W1. Follow-up numbers below (AUC CIs vs chance,
per-week AUC, confidence deciles) are computed from the saved scores and the 5 s price series.*

**TLDR.**
- **P1 (economics, primary) FAILS clearly.** Best gross at a 5 s delay is +2.08 bp (`xgbw` top 2 %,
  n = 60, CI [−1.7, +4.3]); the MLP's tails are +0.15 / +0.48 bp with n = 230 / 340. The 9 bp bar is
  out of reach.
- **P2 and P3 pass formally but mean little economically.** The MLP beats xgboost on the same wide
  inputs (+0.027 AUC, CI [+0.010, +0.039]) and the 60-feature model (+0.043). But the controls sit at
  chance (`xgbw` 0.500) or below it (`xgb60` 0.484), and the MLP's own AUC is only **0.527**.
- **New finding:** under the delayed label the MLP's AUC is **0.527, CI [0.516, 0.544], above 0.5 in
  7/7 weeks**, and nearly as high as under the zero-delay label (0.534). The wide inputs carry a
  little direction information that lasts **past** the first 5 s, the first such signal in the project.
- **It is economically irrelevant.** On these triggers the 90 s move averages only **~10 bp**, so even
  a perfect sign predictor would make about the 9 bp fee. The MLP's accuracy runs from 0.50 to 0.544
  across confidence deciles (gross −0.1 → +0.9 bp). Required accuracy at 9 bp is ~0.95.
- **This closes BTC direction from this collector,** as pre-registered. Next: R4 or R5.

---

## 1. Run validity

| check | result |
|---|---|
| data | 68.8 d, 1,188,626 5 s slots; wide grid 99.8 % present, 100 % on needed rows; 664 inputs |
| labels | ±10 bp / 90 s touched on 10.76 % of bars (both arms); P(up \| touched) 0.521 |
| rows | 171,571 needed; flat wide input 171,571 × 1,992 |
| walk-forward | 7 weekly blocks, train 19k → 103k touched bars; 51,529 stage-1 trigger bars scored |
| early stopping | MLP best epoch mostly **0–2** (max 6), CNN 0–6, xgboost often 0–20 rounds: the same collapse as every earlier run |
| trigger-rate shift | the 08-17 week holds 24.6k triggers (48 % of the AUC sample), because the 7-day causal quantile met a volatility surge; per-week AUCs (§2) show the result is not carried by that week |

## 2. AUC on touched stage-1 triggers (n ≈ 28.5k, 42 days)

| model | label D (from t + 5 s) | 95 % CI | weeks > 0.5 | label Z (from t) | weeks > 0.5 |
|---|---|---|---|---|---|
| `xgb60` (60 engineered) | 0.484 | [0.461, 0.503] | 1/7 | 0.514 | 3/7 |
| `xgbw` (xgboost, wide) | 0.500 | [0.488, 0.523] | 3/7 | 0.514 | 6/7 |
| **`mlp` (wide)** | **0.527** | **[0.516, 0.544]** | **7/7** | 0.534 | 7/7 |
| `cnn` (wide, 12 bars) | 0.521 | [0.504, 0.543] | 6/7 | 0.528 | 7/7 |

MLP per week (D): 0.526 · 0.574 · 0.526 · 0.564 · 0.550 · 0.524 · 0.526.

Pre-registered paired tests (label D): **P2** `mlp` − `xgbw` = +0.0273, CI [+0.0100, +0.0388] → PASS;
`cnn` − `xgbw` = +0.0211, CI [−0.0018, +0.0410] → fail. **P3** `mlp` − `xgb60` = +0.0433,
CI [+0.0247, +0.0721] → PASS.

**How to read the passes.** They are relative to controls that learned nothing on 5 s data alone. The
60-feature xgboost reached 0.58–0.60 in R1 / 1d only when trained on the **331-day v1 year** at ±20 bp;
here it gets 68 days at ±10 bp and lands below chance. So "MLP beats trees" here means "the MLP finds a
weak signal where the trees find none", not that it is strong. Still, a network extracting direction
from wide inputs that the trees miss, stable across all seven weeks, has not happened before in this
project.

## 3. P1 — the pre-registered economics (label D, gross at a 5 s delay, one position)

| model | tail | n | gross | 95 % CI | |
|---|---|---|---|---|---|
| `xgbw` | top 1 % | 37 | +1.61 | [−4.18, +4.82] | fail |
| `xgbw` | top 2 % | 60 | +2.08 | [−1.69, +4.29] | fail |
| `mlp` | top 1 % | 230 | +0.15 | [−1.30, +0.89] | fail |
| `mlp` | top 2 % | 340 | +0.48 | [−0.83, +1.76] | fail |
| `cnn` | top 1 % | 141 | −1.15 | [−2.43, +0.16] | fail |
| `cnn` | top 2 % | 210 | +0.05 | [−1.09, +1.28] | fail |

Pass line: CI lower bound > 9.0 bp with n ≥ 100. Not close in any cell, and no cell at any delay or
tail in either label has a gross above +2.6 bp. The xgboost tails have n < 100 because its predictions
are compressed (p ∈ [0.40, 0.63]).

## 4. Why a real signal makes no money — the arithmetic

MLP (label D), all stage-1 triggers by confidence decile (overlapping, descriptive), 90 s hold from
t + 5 s:

| decile | 1 | 3 | 5 | 7 | 9 | **10** | all |
|---|---|---|---|---|---|---|---|
| accuracy | 0.500 | 0.520 | 0.516 | 0.525 | 0.531 | **0.544** | 0.517 |
| gross bp | −0.10 | +0.43 | +0.27 | +0.43 | +0.34 | **+0.92** | +0.21 |
| E\|move\| bp | 9.7 | 10.2 | 10.3 | 10.3 | 9.5 | 8.8 | 10.0 |

- Confidence does rank accuracy (0.50 → 0.544), so the signal is real. But the move it is predicting
  is **~10 bp**. At a 9 bp round trip, the required accuracy is `(1 + 9/10) / 2 ≈ 0.95`, and a
  perfect sign predictor would net about +1 bp. P1 was close to impossible on this trigger set and
  horizon whatever the model. That is the same arithmetic wall as `horizon_economics…` §6.
- The 1d / R1 tails reached +11–14 bp at zero delay only because they were the first-seconds move on
  ±20 bp bars. Removing that move (label D) leaves a fraction of a bp per trade.
- Label Z did not sharpen the fast signal either (MLP 0.534; tails ≈ 0 to +1.3 bp). On 68 days of 5 s
  data alone, nothing reaches the v1-trained 0.58–0.60.

## 5. What this settles

1. **P1 fails → the pre-registered kill applies.** Wide raw inputs at 5 s do not produce a tradeable
   signal after a realistic entry delay. With R1, the continuation test and the MLP arm, BTC direction
   from this collector is closed.
2. **The model-class question has a nuanced answer.** On the 60 engineered features the MLP equals
   xgboost (R1 C1). On the 664 wide inputs it finds a weak signal that xgboost does not (+0.027 AUC,
   7/7 weeks). So the wide inputs do contain a little extra information, and a network is the way to
   reach it. It is worth ~0.2–0.9 bp per trade.
3. **The binding constraint is magnitude, not prediction.** At 90 s the available move on trigger bars
   is ~10 bp, about the fee. Any future direction work needs a setting where the move is several
   times the fee, and that is not BTC at seconds-to-minutes on this data.

**Optional, low prior:** the delayed MLP signal could be retargeted at 15–60 min, where trigger moves
are 45–55 bp and required accuracy drops to ~0.58–0.60. The GBM probe found IC ≈ 0 at those horizons
with engineered features, and a 0.527-AUC signal decaying from 90 s is unlikely to reach 0.58. If
tried, pre-register a single horizon and use the same P1-style bar.

## Reproduce

```
cd runs/harness_wide
PYTHONPATH=<xgboost, torch, pyarrow, nbformat, nbclient, psutil> python test_notebook.py   # 36/36
python extract_w5_wide.py      # rebuilds data/w5_wide.parquet (~6 min)
```
AUC CIs, weekly AUCs and the decile table: load `wide_probe_scores.npz`, rank-AUC per day-bootstrap,
forward returns from `data/w5_60d.parquet` mid (90 s from t + 5 s).
