# GBM long-horizon probe — FALSIFIED: no long-horizon directional signal in the existing features, and the criterion-B "passes" were drift

*Analyzed 2026-09-22. Notebook: `runs/btc_lstm.probe.gbm.ipynb` (executed on Colab, 14/14 code cells clean, zero errors, device = cuda). Artifacts: `runs/gbm_probe_scores.npz`. Build notes: `gbm_probe.notes.md`. Motivation and the required-accuracy arithmetic: `horizon_economics_and_next_ideas.md` §7.*

**TLDR:** the pre-registered falsification branch fired. **Criterion A failed in all four
arms** — pooled IC at 15 min / 1 h is `+0.0050 / −0.0088 / +0.0014 / −0.0073`, daily-IC t
never reaches 3, and the direction heads early-stop at **0–14 boosting rounds** while the
magnitude head trains to 30–122. There is nothing to learn for direction in these features
at these horizons.

Criterion B printed **PASS in three of four arms**, and all three are artifacts. The probe
as built compared accuracy to the *fee-breakeven* number only. It had no blind benchmark —
a control `run009d.offline.md` §8 introduced for exactly this reason and which this probe
failed to inherit. With the control computed from the saved npz: **the trigger sets carry
extreme directional drift** (`P(up) = 0.347` at 15 min, **`0.873`** at 1 h), and
**always-long beats the model in 7 of 8 cells**, by up to 12.3 bp. Criterion B at 1 h
required 0.541; simply being long scored 0.873.

The magnitude head worked (2.12× / 2.14× |move| lift), re-confirming §7.2 — but it is
*worse* than the existing run.009f LSTM detector at 15 min (2.12× vs 3.59×), so stage 1
needs no retraining.

---

## 1. Execution validity

| check | result |
|---|---|
| cells executed | **14/14 clean, zero error outputs**, device = `cuda` |
| data | 412 w5 files, 1,186,560 rows → 1,161,525 after dropna, 68.8 d, schema v5 100 % |
| features | **76/76 kept** (`PRUNE_MODE='none'` — the 009d prune deliberately reverted) |
| purge | **720 bars (60 min)**, vs run.009f's `MAX_H` = 24 — the long-label leak is closed |
| folds | 4 × ~10 d test, unchanged from 009d/e/f; train 380 k → 821 k |
| long targets | h180 valid 1,110,092 (E\|move\| 11.48 bp) · h720 valid 983,881 (E\|move\| 22.57 bp) |
| gap assertion | passed — no long target survives a collector gap |

The run is sound. Everything below is about what it found, not whether it ran.

## 2. Criterion A — failed in all four arms

| horizon | model | pooled IC | daily-IC t | f0 | f1 | f2 | f3 | folds > 0 |
|---|---|---|---|---|---|---|---|---|
| 15 min | `dir_all` | **+0.0050** | +1.94 | +0.0062 | +0.0237 | +0.0013 | +0.0153 | 4/4 |
| 15 min | `dir_evt` | −0.0088 | **−2.44** | −0.0103 | +0.0199 | −0.0283 | −0.0156 | 1/4 |
| 1 h | `dir_all` | +0.0014 | +1.70 | +0.0090 | −0.0384 | +0.0124 | −0.0006 | 2/4 |
| 1 h | `dir_evt` | −0.0073 | +1.01 | +0.0527 | +0.0407 | −0.0493 | −0.0228 | 2/4 |

For scale, the run.009f LSTM's pooled IC at h6 is **+0.0602** — 12–40× larger than anything
here. The best arm (`h180 dir_all`, 4/4 folds positive, t = +1.94) is still short of the
`GATE_T = 3.0` threshold, and an IC of 0.0050 buys directional accuracy ≈ **0.502**
(`0.5 + ρ/π`) against a 0.582 requirement.

**Early stopping says the same thing independently.** Best iteration per fold:

| head | 15 min | 1 h |
|---|---|---|
| `mag` (stage 1) | 54 / 116 / 91 / 77 | 30 / 44 / 70 / 122 |
| `dir_all` | **14 / 1 / 2 / 5** | **4 / 0 / 0 / 7** |
| `dir_evt` | 0 / 28 / 15 / 33 | 2 / 5 / 11 / 0 |

The magnitude head finds structure and trains. The direction heads stop almost immediately —
`dir_all` at 1 h selects `best_iteration = 0` in two folds, i.e. an essentially constant
prediction. This is the same shape as the LSTM's epoch-1 collapse (`run009a.analysis.md`),
reached by a completely different model family.

## 3. The criterion-B passes are drift — the control the probe was missing

### 3.1 The defect

Criterion B as built asked only: *is accuracy ≥ the fee-breakeven accuracy?* It never asked
*is accuracy ≥ what you get for free?* `run009d.offline.md` §8 added the blind benchmark and
day-matched null precisely because that question matters, and the probe did not inherit them.
At 90 s the omission was harmless — the blind benchmark there is **+0.04 bp**. At 1 h on
high-volatility bars it is **+20.62 bp**, and the omission is fatal to the criterion.

### 3.2 The control, computed from `gbm_probe_scores.npz`

On the probe's **own** stage-1 trigger sets. `blind` = always-long on the same bars;
`excess` = model − blind; `perm pctile` = percentile of the model against 4,000 shuffles of
its own predicted signs across the same bars (holds the sign multiset, and therefore the
drift exposure, fixed).

| h | rate | model | n | acc | gross | **blind (long)** | **P(up)** | **excess** | perm pctile |
|---|---|---|---|---|---|---|---|---|---|
| 15 min | 0.1 % | `dir_all` | 645 | 0.348 | −7.39 | −10.10 | 0.347 | +2.70 | 30.6 % |
| 15 min | 0.1 % | `dir_evt` | 645 | 0.701 | +9.81 | −10.10 | 0.347 | **+19.90** | 100.0 % |
| 15 min | 1 % | `dir_all` | 6,456 | 0.553 | +2.78 | +3.48 | 0.542 | −0.70 | 97.7 % |
| 15 min | 1 % | `dir_evt` | 6,456 | 0.532 | +1.64 | +3.48 | 0.542 | −1.83 | 99.1 % |
| 1 h | 0.1 % | `dir_all` | 590 | 0.586 | +8.34 | **+20.62** | **0.873** | **−12.28** | 100.0 % |
| 1 h | 0.1 % | `dir_evt` | 590 | 0.642 | +13.72 | **+20.62** | **0.873** | **−6.90** | 100.0 % |
| 1 h | 1 % | `dir_all` | 5,875 | 0.571 | +6.86 | +18.89 | 0.661 | −12.02 | 100.0 % |
| 1 h | 1 % | `dir_evt` | 5,875 | 0.586 | +8.28 | +18.89 | 0.661 | −10.60 | 100.0 % |

**Seven of eight cells lose to always-long.** Both 1 h cells that "passed" criterion B give
up 6.9 and 12.3 bp versus doing nothing but holding a long on the same bars. Criterion B's
requirement at 1 h was 0.541; the trigger set's own `P(up)` is **0.873**.

### 3.3 The trigger sets are one regime, not a sample

| h | rate | n | distinct days | largest single day |
|---|---|---|---|---|
| 15 min | 0.1 % | 645 | 16 of 42 | 23.9 % |
| 1 h | 0.1 % | 590 | 12 of 42 | 25.3 % |

A 0.1 % trigger set lands on 12–16 days with a quarter of it inside one day. And the two
horizons' drifts **contradict each other** — `P(up) = 0.347` at 15 min versus `0.873` at 1 h,
from the same model family on overlapping windows. That is not a property of the market; it
is what a small, day-clustered sample looks like.

### 3.4 Why the sign-permutation null reads 100 % and still does not rescue anything

Six of eight cells sit at the 97.7–100th percentile of the sign-permutation null, which looks
like evidence of real sign assignment. It is not drift-free, and this is the methodological
point worth keeping:

> The permutation holds the **sign multiset** fixed but destroys the coupling between the
> model's sign and the *magnitude* it predicts. A model that goes long precisely on the
> biggest bars of an up-drifting window beats its own permutations without carrying any
> directional skill — it is size-weighted drift exposure.

The always-long benchmark has no such hole, and by it the model loses in 7 of 8 cells. Where
the two controls disagree, the blind benchmark is the one that corresponds to a trading
decision.

Note also a definitional caveat: the `acc` column is the **implied** accuracy
`(gross/E|move| + 1)/2`, i.e. magnitude-weighted. It maps directly to net bp, which is what
the economics need, but it is not the raw count of correct calls — the two differ whenever
sign and magnitude are coupled, which §3.4 shows they are.

### 3.5 The one cell that beats blind

`h180 dir_evt @ 0.1 %`: excess **+19.90 bp**, sign-perm 100th percentile. Reasons not to
believe it: n = 645 over 16 days; it is 1 of 8 cells with no multiple-testing correction; the
**same model at 1 % rate gives excess −1.83**; and its own pooled IC is **significantly
negative** (t = −2.44, 1/4 folds positive). A model that is anti-correlated with the target
overall cannot be 70 % accurate on a 645-bar subset of it except by luck. This is the same
shape as fold 3, the run.010a 0.610 router AUC, and the §6.6 deadband cell — all recorded and
all dead.

## 4. What did work

- **Stage 1 transfers, as §7.2 predicted.** The `mag` head gives 2.12× (15 min) and 2.14×
  (1 h) |move| lift, trained properly (30–122 rounds).
- **But it is worse than what already exists.** The run.009f LSTM classification heads give
  **3.59×** at 15 min on the same window (§7.2) versus the GBM's 2.12×, with E|move| on
  triggers of 44.5 bp versus 24.4 bp. **Do not retrain stage 1** — the detector in
  `run009f_scores.npz` is better and free.

## 5. The feature prune, re-judged

The 18 features dropped by run.009d carry **33.8 %** (15 min) and **26.7 %** (1 h) of total
gain, and 9/25 and 8/25 of the top 25. `DROP_HARMFUL` was derived from the h18 up-head **AP**
drop — a magnitude metric at a 90 s horizon — so on a long-horizon direction objective it is
measuring the wrong thing, as expected.

But the ranking is a **warning, not a vindication**. The top features at both horizons are
`funding_pressure`, `lsr_z`, `minute_cos/sin`, `vol_ratio_1h_24h`, `ma_gap_4h/24h`,
`hour_sin/cos`, `dow_cos`, `ret_norm_*` — almost entirely regime, time-of-day and trend
proxies. That is:

1. the feature class run.011 documented as carrying a 0.613 AUC that **died under
   walk-forward**; and
2. exactly the class that encodes a window's drift.

So the importance table is consistent with §3: the direction models found the drift. Putting
the pruned features back does not rescue direction — it reaches the drift faster.

## 6. Verdict

**Falsified, as pre-registered.** No usable long-horizon directional signal exists in the 76
bar-level features at 15 min or 1 h. Do not build the two-stage notebook on these inputs.

The §7 reasoning that motivated the probe was sound and is unaffected — the required accuracy
really does fall from 0.77 at 90 s to 0.59 at 1 h, and the volatility detector really does
transfer. The probe tested the remaining premise, that *some* directional signal exists at
those horizons to exploit the relaxed requirement, and that premise is false for this feature
set.

Per `horizon_economics_and_next_ideas.md` §6.7, two branches remain, and this closes one of
them. What is left:

1. **New input classes** — `run.013`'s price-level event panel, with its §6.4 bin-end
   causality fix and §7.1 mid-relative re-index. Untested, and now the only untested input.
2. **Close the direction line.** §6 already showed `c* = 0.214 bp` against a 2.0 bp maker
   fee; §7 showed the horizon escape needs a directional signal that §3 shows is not there.

## 7. Correction to make in the standing docs

`horizon_economics_and_next_ideas.md` §7.3 gives required accuracy as
`p = (1 + F/E|move|)/2`, implicitly benchmarked against a 0.50 coin. **On volatility-selected
bars at long horizons that benchmark is wrong** — the drift baseline on the probe's own
trigger sets was 0.347 and 0.873. Any future long-horizon criterion must read:

```
required accuracy = max( (1 + F/E|move|)/2 ,  P(up) on the same trigger set )
```

This is implemented in `btc_lstm.probe.gbm2.ipynb` cell G5b, along with the blind benchmark
and the sign-permutation null. The v1 notebook is kept unmodified as the executed record.

## 8. Reproducibility

Every number in §3 comes from `runs/gbm_probe_scores.npz` (35 MB, saved by the run) plus
`runs/run009f_scores.npz` for the §4 comparison. CPU only, seconds. Trigger sets are rebuilt
with the notebook's own `fold_top` on `p_{h}_mag`; `blind = mean(rawbp[trig])`;
`P(up) = mean(rawbp[trig] > 0)`; the null shuffles `sign(p_{h}_{key}[trig])` 4,000 times.
