# Why the runs failed to bring profit — independent root-cause check

*Written 2026-09-22. Independent re-derivation of the economics from the saved
artifacts (`runs/run009f_scores.npz`), not a restatement of the existing
analyses. Companion to `run009f.analysis.md`, `economics_and_metrics.md`,
`ALL_RUNS_ANALYSIS.md` and `SESSION_NOTES_2026-09-21.md`.*

---

## Verdict

The runs do **not** fail from a bug, leakage, or a weak model. They fail because
**the heads predict *magnitude*, not *direction*** — and the predicted sign on the
triggered bars is worse than a coin flip. Realized gross edge ≈ 0 bp against a
10 bp taker round-trip floor, so no configuration nets positive.

The decisive decomposition: the triggers carry **13–17 bp** of move (enough to
clear fees *if the sign were known*); the model captures **−6 % of a perfect
direction predictor**. So the binding failure is **directional content**, not
magnitude and not sample size.

## What was verified (from data, not docs)

Method: load `runs/run009f_scores.npz`, take the top 0.1 % of each head's
probability **within each fold** (n = 666 per head), compute the raw forward
return in bp from `fwd_close` / `close`, and the first-touch labels from
`labels`. Reproduced the doc's base rates (lup_18 1.215 % vs implied 1.216 %) and
`|move| all @h18 = 4.03 bp` (doc: 4.0) before trusting the rest.

### 1. Heads select high-magnitude bars (the volatility artifact is real)

| | all bars | up18 triggers | dn18 triggers |
|---|---|---|---|
| mean \|move\| @h18 | 4.03 bp | **13.22 bp** | **17.27 bp** |
| P(either barrier touched first) | **2.30 %** | 47.9 % | 42.6 % |
| own-label lift | 1× | 18.0× | 13.7× |
| opposite-label lift | 1× | **23.9×** | **22.9×** |

Opposite-label lift ≥ own-label lift on nearly every head → the trigger says
"big move coming", not "up" or "down". This is the volatility artifact.

### 2. The direction is anti-predictive

| | gross bp @h18 | direction accuracy |
|---|---|---|
| up18 head traded long | −0.79 | **43.2 %** (coin = 50 %) |
| dn18 head traded short | −3.41 | **44.9 %** |

Daily t-stats of the gross edge: up18 +0.78, dn18 −1.06 over 19 days — not
significantly different from zero, and the wrong sign.

### 3. Market-neutral spread S ≤ 0 at every horizon

Re-derived S = `mean(ret | up-head triggers) − mean(ret | dn-head triggers)`:

| h | 6 | 9 | 12 | 15 | 18 | 24 |
|---|---|---|---|---|---|---|
| S (bp) | −1.5 | −3.4 | −3.4 | −3.8 | **−4.2** | −4.7 |

Doc value at h18 is −3.79 bp. Per-fold at h18: f0 −2.56 · **f1 −5.25 [−8.54, −0.63]** ·
f2 −8.26 · f3 −0.62. The sign is robustly ≤ 0 everywhere.

Caveat on significance: my union-of-days bootstrap CI at h18 is [−8.07, +0.47] —
it *just* includes 0, whereas the notebook's `boot_ci_spread` printed
[−7.22, −0.42]. The difference is the trigger definition (per-fold vs the
notebook's pooled-rate machinery) and the day grouping. The **point estimate ≤ 0
at every horizon** is the load-bearing result; "significantly negative" is
borderline in this re-derivation but fold 1 is independently negative.

### 4. The decisive decomposition — it is *not* "moves too small"

Economics on the up18 trigger set (n = 666):

| route | gross | net taker (−10 bp) | net maker (−4 bp) |
|---|---|---|---|
| **Oracle** (perfect sign) | 13.22 | **+3.22** | **+9.22** |
| Model (real up head) | −0.79 | −10.79 | −4.79 |

The triggers carry enough magnitude to clear the fee floor *if the sign were
known*. The model captures −6 % of perfect. The failure is directional.

## Why every configuration failed

1. **Signal is a ranking signal, not a direction signal.** IC +0.06–0.11 is real
   and significant (daily t +7.6) but transfers to the binary first-touch label
   as ~12–41× lift on *both* sides — symmetric → zero drift-cancelling edge.
2. **Fee floor.** Taker RT 10 bp, maker RT 4 bp. Breakeven hit ≈ 58.5 % taker /
   ≈ 49 % maker; achieved ≈ 7–15 %. Gross directional edge per trade ≤ ~+2 bp.
3. **Maker entry is structurally worse.** A resting limit fills exactly when the
   move already failed — adverse selection (`hit_f ≪ hit_m`). The 6 bp fee saving
   is cancelled by ~5 bp of selection cost. Verified across runs 008→011.
4. **Selection overfits the ~10-day val window.** 009e/009f found positive val
   spreads (+0.12–0.17 σ) that inverted in test (−2.9 to −5.9 bp). Fixing the
   estimator (54 → 2,300 triggers) did not help → never a sample-size problem.
5. **Regime instability.** Fold-level signs flip; fold 3 carries the only
   surviving (untradeable) effect.

Code confirms the arithmetic: gate A/E require *filled* net sim > 0 with CI > 0
(`btc_lstm.run.009f.ipynb` cell 17:78–133); fee constants `TAKER_FEE = 0.0005`,
`MAKER_FEE = 0.0002` (cell 4:46–47); sim pays −7 bp on every SL/time-stop
(cell 16:58–65).

## Ruled out (already tested, all dead)

Bigger models, exit rules (TP/SL), entry latency, label redefinition
(first-touch), EMA overlay, 5 s vs 15 s bars, maker/skip router (val found no
profitable τ → routed 0/2,091).

## What would actually change the verdict

1. **Out-of-window falsifier** — re-run the 009f evaluation on post-2026-09-20
   data. If S is still ≤ 0 the verdict closes for this data era. Cheap.
2. **`run.013` T3 — maker fill-timing** — the only untested input class; predict
   "will my limit fill AND reach TP before SL". Prerequisites: shift the bin-end
   features one bin (§6.4 causality) and the mid-relative re-index (§7.1
   stationarity), then port the 009f diagnostics so it is not read through the
   artifact-passing gate.
3. **Market-neutral label** — predict the up/down *spread* directly rather than
   which barrier is touched first, so the volatility artifact becomes
   structurally unrewardable instead of something selection must avoid.

## Bottom line

The model found volatility, not direction. Fees only matter because there is no
directional edge to pay them with.

## Reproducibility

Interpreter: `/home/nkout/projects/binance2/binance2/.venv/bin/python`
(numpy 2.3.2, pandas 3.0.3). Script pattern:

```python
import numpy as np
z = np.load("runs/run009f_scores.npz", allow_pickle=True)
cols = [str(c) for c in z["cls_cols"]]          # interleaved: lup_6, ldn_6, lup_9, ...
prob, lab, fold = z["prob"], z["labels"], z["fold_of"]
close, fc, dt = z["close"], z["fwd_close"], z["dt_s"]
def idx(n): return cols.index(n)
def ret_bp(h): return (fc[:, h-1] / close - 1.0) * 1e4
def per_fold_trig(sc, rate=0.001):
    t = np.zeros(len(sc), bool)
    for f in np.unique(fold):
        m = fold == f; k = max(1, int(round(rate * m.sum())))
        thr = np.partition(sc[m], -k)[-k]; ii = np.where(m)[0]
        t[ii[sc[m] >= thr]] = True
    return t
h = 18
tu = per_fold_trig(prob[:, idx('lup_18')])
td = per_fold_trig(prob[:, idx('ldn_18')])
r = ret_bp(h)
print("S =", r[tu].mean() - r[td].mean())       # ~ -4.2 bp
print("oracle net taker =", np.abs(r[tu]).mean() - 10)  # ~ +3.2 bp
```
