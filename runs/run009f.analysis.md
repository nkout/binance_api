# run.009f analysis — the estimator was not the constraint: the test market-neutral spread is significantly NEGATIVE

*Analyzed 2026-09-21. Notebook: `runs/btc_lstm.run.009f.ipynb` (executed on Colab, 17/17 cells clean, no errors). Build notes: `run009f.notes.md`. Parents: `run.009e` (DE selection), `run.009d` (AP selection). Companion: `run009d.offline.md`, `run009e.analysis.md`, `SESSION_NOTES_2026-09-21.md`.*

**TLDR:** run.009f changed the model-selection estimator from a label-count directional edge (run.009e, ~55 val samples) to a **continuous market-neutral spread** on the vol-normalised target with ~1,300–2,300 samples per leg, and fixed the defective `S` confidence interval. The gate **FAILED on all six criteria, both sides**. The decisive result is criterion **E: `S = −3.79 bp, day-bootstrap CI [−7.22, −0.42]` — the test market-neutral spread is significantly _negative_**, not zero and not positive. Two of four folds are independently significantly negative (f0 −5.85 [−16.15, −0.40], f1 −2.90 [−7.12, −0.70]). This is a *stronger* result than a null: it rules out "the estimator was the constraint", which was the entire purpose of the run.

Across the three selection objectives the ordering is unambiguous and monotone — **AP > directional-edge > spread** — on signal quality, on directional content, and on economics. The AP-selected model (run.009d), whose selection metric this project criticised for rewarding the volatility artifact, had the **most** directional content of the three. The volatility-selection diagnosis is therefore confirmed for this feature set and horizon family, and there is no untested selection objective left on this ladder.

---

## Validity checks

- **Executed source is correct.** The first execution failed with `SyntaxError: 'break' outside loop` in cell 9 (a misplaced `best_state is None` fallback that closed the `for epoch` loop). Fixed, and the test's compile check switched from `ast.parse` (which does **not** validate `break`/`continue`/`return` placement) to `compile()`. See `run009f.notes.md` → "Bugs found and fixed during verification". This run's source is the corrected build.
- **One variable.** `EARLY_STOP` `'dir'` → `'spread'`, plus the CI instrumentation fix. Feature set (58), folds, model (230,790 params), hyperparameters, fee constants, trigger rules, `THETA_BY_H`, `TP_MULT`/`SL_MULT`, maker fill model and label construction are unchanged.
- **Same window and folds as 009d/009e** — 68.8 d, 42 test days, identical `n_train`/`n_val`/`n_test`, same base rates, same `F_DIM=58`. Comparison is clean.
- **The CI fix is live in both places.** The drift-control cell and the gate compute `S` independently and agree to the digit (`−3.79 [−7.22, −0.42]`), confirming `boot_ci_spread` is wired correctly and consistently.
- **The estimator is now well-powered.** Val legs are 1,085 / 1,428 / 1,876 / 2,297 triggers per fold (vs 54–115 in run.009e), and the per-epoch val-spread values are continuous (e.g. −0.3295, +0.1234, +0.3471 at fold 0) rather than quantised to multiples of 1/55.

## The result

### Gate — all six criteria fail, both sides

| criterion | up18 @0.1 % | dn18 @0.1 % |
|---|---|---|
| A maker sim > 0, CI > 0 | fail (−11.39 [−13.08, −9.26]) | fail (−12.81 [−14.43, −11.03]) |
| B (info) own-label lift ≥ 3× | pass (11.96×, 3/4) | pass (13.04×, 4/4) |
| **B directional content** | **fail (DE −3.83 pp, folds 1/4)** | **fail (DE −7.04 pp, folds 0/4)** |
| C fill ≥ 25 % + folds sim > 0 ≥ 3/4 | fail (78.4 %, 0/4) | fail (78.5 %, 0/4) |
| D neighbours sim > 0 | fail | fail |
| **E market-neutral S > 0, CI > 0** | **fail (S −3.79, CI [−7.22, −0.42])** | **fail (same)** |
| F beats null p97.5 in ≥ 3/4 folds | fail (1/4) | fail (0/4) |

Criterion B (info) still passes on both sides — the own-label lift of 12–13× is the **volatility artifact**, and it passes while the directional criterion fails. That is the third consecutive run in which the gate's own-label criterion is satisfied by the artifact.

### Market-neutral spread by horizon

| h | S (bp) | 95 % CI (spread) | |
|---|---|---|---|
| 6 | −0.96 | [−3.70, +1.32] | includes 0 |
| 9 | **−2.60** | [−5.36, −0.36] | **negative** |
| 12 | −2.87 | [−6.76, +0.28] | includes 0 |
| 15 | **−3.40** | [−7.10, −0.05] | **negative** |
| 18 | **−3.79** | [−7.22, −0.42] | **negative** |
| 24 | −3.74 | [−6.95, +0.91] | includes 0 |

Per fold at h18: **f0 −5.85 [−16.15, −0.40]** · **f1 −2.90 [−7.12, −0.70]** · f2 −3.67 [−8.55, +8.24] · f3 −1.59 [−7.40, +20.55] · **pooled −3.79 [−7.22, −0.42]**.

### Directional content, and the volatility mechanism

| head | own % | own lift | opp % | opp lift | DE (pp) | DC | \|fwd\| trig / all |
|---|---|---|---|---|---|---|---|
| up6 | 14.81 | 31.44 | 12.31 | 27.61 | +2.51 | 1.20 | 8.8 / 2.2 |
| up9 | 15.80 | 17.08 | 15.91 | 18.53 | −0.11 | 0.99 | 9.6 / 2.8 |
| up12 | 12.50 | 19.97 | 15.60 | 26.90 | −3.10 | 0.80 | 11.6 / 3.3 |
| up15 | 14.49 | 15.96 | 16.95 | 20.46 | −2.45 | 0.86 | 12.6 / 3.7 |
| **up18** | 14.54 | 11.96 | 18.36 | 16.87 | **−3.83** | **0.79** | 13.2 / 4.0 |
| up24 | 14.49 | 7.69 | 19.05 | 11.30 | −4.55 | 0.76 | 13.6 / 4.7 |
| dn6 | 10.93 | 24.52 | 16.22 | 34.42 | −5.29 | 0.67 | 9.9 / 2.2 |
| dn18 | 14.19 | 13.04 | 21.23 | 17.47 | −7.04 | 0.67 | 16.1 / 4.0 |
| dn24 | 16.56 | 9.82 | 25.44 | 13.49 | −8.87 | 0.65 | 16.9 / 4.7 |

Every horizon ≥ 9 has **negative** DE. The up heads' own/opposite lift ratio falls from 1.20 (h6) to 0.76 (h24); the dn heads sit at 0.62–0.71 throughout. Triggers still carry 4–5× the average |move|. The heads remain volatility detectors, and the directional tilt that AP selection had partially captured is now **inverted**.

### Economics (h18 @0.1 % ens)

| route | up18 | dn18 |
|---|---|---|
| taker sim | −11.73 [−13.20, −9.80] | −12.72 [−13.84, −11.42] |
| maker sim (primary config) | −11.39 [−13.08, −9.26] | −12.81 [−14.43, −11.03] |
| taker hit % / lift | 14.5 / 11.96 | 14.2 / 13.04 |
| maker fill % / hit_f % | 78.4 / 10.6 | 78.5 / 8.3 |

All worse than run.009d (−5.62 / −10.45 taker; −6.11 / −10.57 maker) and run.009e. `unan up18 @0.1 %` shows n = 11 with sim +6.95 — noise, not evidence; `unan @0.01 %` is n = 0 on both sides.

## The three selection objectives, ranked

| metric | 009d `'ap'` | 009e `'dir'` | **009f `'spread'`** |
|---|---|---|---|
| pooled IC h6 | +0.1093 | +0.0710 | **+0.0602** |
| pooled IC h18 / h24 | +0.0491 / +0.0394 | +0.0386 / +0.0347 | +0.0386 / +0.0344 |
| daily-IC t h6 / h18 | +13.53 / +8.55 | +9.92 / +6.65 | **+8.22 / +7.58** |
| up18 DE | +18.53 pp | +1.95 pp | **−3.83 pp** |
| up18 DC | 2.47 | 1.10 | **0.79** |
| dn18 DE | +0.71 pp | −5.76 pp | **−7.04 pp** |
| dn18 DC | 1.04 | 0.74 | **0.67** |
| test `S` @h18 | +5.60 | −2.43 | **−3.79 (sig.)** |
| taker up18 / dn18 | −5.62 / −10.45 | −10.19 / −12.33 | **−11.73 / −12.72** |
| maker up18 / dn18 | −6.11 / −10.57 | −10.36 / −12.41 | **−11.39 / −12.81** |
| pooled IC profile | decays with h | decays with h | **flat-to-inverted** |

**Monotone degradation across all three objectives.** AP dominates on every axis, including directional content. That is the most uncomfortable and most load-bearing result in this document: the metric that "rewards the artifact" was nonetheless the best of the three selectors available, because it is a stable whole-distribution statistic while both directional objectives are tail statistics that overfit the val window.

Note also that run.009f's fold-0 IC profile is **inverted** (+0.0277 at h6 rising to +0.0433 at h24) — unique among the runs, which all decay with horizon. Selecting on the h18 spread demonstrably reshaped the model's horizon profile.

## Why it happened — the val spread does not generalise

The selected-epoch val spreads were **positive** while the resulting test spread was **negative** — in every fold:

| fold | selected-epoch val spread, 3 seeds | mean | test `S` |
|---|---|---|---|
| 0 | +0.1299 · +0.0366 · +0.3471 | **+0.1712** | **−5.85** |
| 1 | −0.0144 · +0.2471 · +0.1373 | **+0.1233** | **−2.90** |
| 2 | +0.2211 · +0.0387 · +0.2135 | **+0.1578** | **−3.67** |
| 3 | +0.0823 · −0.0125 · −0.1039 | −0.0114 | −1.59 |

In folds 0–2 the selector found a mean val spread of **+0.12 to +0.17 σ** and the next ~10 days delivered **−2.9 to −5.9 bp**. Fold 3 is the only one where the val spread was ≈ 0, and it is the only fold whose test spread is not clearly negative.

So the objective is now well-powered (1,085–2,297 samples per leg, continuous values) and it **still** fails to carry across the ~10-day val→test boundary — in fact its sign inverts in the three folds where it was strongest. This is the same failure mode as run.009e — selection overfitting — but with a properly sized estimator rather than a noisy one. That is the informative part: **the problem was never the estimator's sample size.** Directional edge estimated on a 10-day window does not persist into the next 10 days in this feature set.

## What this settles

The pre-registered two-branch reading in `run009f.notes.md` was: *`S ≤ 0` with a CI bracketing it → the estimator was not the constraint; the volatility-selection diagnosis is final for this feature set and horizon family.*

That branch is hit, and harder than expected — the CI **excludes** zero on the negative side, and 2/4 folds do so independently. Therefore:

- **The estimator was not the constraint.** Three selection objectives, one diagnosis.
- **The economics lever remains closed.** Max fee-free gross edge stays ≈ +5 bp against a 10 bp taker floor; the maker route's ~5 bp selection cost still cancels its fee saving.
- **No untested selection objective remains on this ladder.** AP, directional edge, and continuous market-neutral spread have all been tried; AP is the best of them and it fails too.

## Caveats

1. **009f is not a clean test of "can a *good* model produce direction".** Its model is the weakest of the three (pooled IC h6 +0.0602 vs 009d's +0.1093); selecting on the val spread degraded the model. The certain reading is *"this estimator is worse than AP"*; the well-supported but not airtight reading is *"no selection objective on this ladder produces direction"*.
2. **The fold-3 up-head effect persists across all three runs at the 100.0th percentile of the day-matched null** (+10.88 → +10.40 → **+4.00**, decaying magnitude). It is a real property of that one 10.5-day window, it is **not tradeable** (fold 3 up18 gross +4.00 → net −6.00 with taker fees), and its own `S` is −1.59 (includes 0).
3. **Fold-level up-side patterns are not stable across models.** Fold 0's up head went from +4.04 at the 100.0th percentile (009d) to −1.21 at the 0.2nd (009f). Only fold 3's sign survives, which is one observation.
4. **Blind benchmarks are ≈0 in every fold** (+0.13 … −0.04), so none of this is drift; the day-matched null width is ±1.0–1.6 bp, and the model now sits at the 0.0–0.2nd percentile in folds 0–2 up — actively worse than random directional entry on the same days.

## Recommended next step — and why it is not another run on this ladder

Further work on this feature set × horizon family × fee structure will not produce a positive drift-cancelling spread. What remains, in order of value:

1. **Out-of-window falsifier.** Re-run 009f's evaluation on post-2026-09-20 data. If `S` is still ≤ 0, the verdict is closed for this data era. Needs a fresh tar; cheap.
2. **`run.013` — the price-level event panel.** The only genuinely untested input class (see below). Its T3 target (maker fill + favourable outcome) attacks the exact unknown that would change the verdict: whether fill-timing is predictable at all.
3. **A market-neutral *label*.** Predict the up/down *spread* rather than which barrier is touched first, so the volatility artifact becomes structurally unrewardable rather than something selection has to avoid.

---

## Appendix — reference tables (run.009f)

### Per-fold IC (seed-ensemble; ± = per-seed spread)

| Fold | n_test | h6 | h9 | h12 | h15 | h18 | h24 |
|---|---|---|---|---|---|---|---|
| 0 | 165,059 | +0.0277 ±0.0090 | +0.0303 ±0.0050 | +0.0333 ±0.0046 | +0.0361 ±0.0043 | +0.0386 ±0.0043 | +0.0433 ±0.0057 |
| 1 | 166,228 | +0.0962 ±0.0158 | +0.0718 ±0.0102 | +0.0625 ±0.0073 | +0.0554 ±0.0054 | +0.0499 ±0.0062 | +0.0404 ±0.0048 |
| 2 | 170,211 | +0.0857 ±0.0063 | +0.0657 ±0.0075 | +0.0546 ±0.0076 | +0.0463 ±0.0063 | +0.0379 ±0.0055 | +0.0276 ±0.0047 |
| 3 | 164,530 | +0.0473 ±0.0105 | +0.0400 ±0.0101 | +0.0371 ±0.0113 | +0.0350 ±0.0127 | +0.0333 ±0.0140 | +0.0296 ±0.0163 |
| **pooled** | 666,028 | **+0.0602** | +0.0490 | +0.0446 | +0.0415 | +0.0386 | +0.0344 |

### Daily-IC t-stats (42 days)

| h6 | h9 | h12 | h15 | h18 | h24 |
|---|---|---|---|---|---|
| +8.22 | +8.20 | +8.36 | +8.10 | +7.58 | +6.54 |

### Blind benchmark + day-matched null @h18 @0.1 % ens

| fold | side | n | model | blind | null p2.5 | null p97.5 | percentile |
|---|---|---|---|---|---|---|---|
| 0 | up | 150 | −1.21 | +0.13 | −0.83 | +1.10 | **0.2 %** |
| 0 | dn | 487 | −4.64 | −0.13 | −1.56 | +0.29 | 0.0 % |
| 1 | up | 204 | −1.53 | +0.06 | −1.26 | +1.31 | 0.9 % |
| 1 | dn | 602 | −1.37 | −0.06 | −1.33 | +0.56 | 2.1 % |
| 2 | up | 295 | −3.77 | +0.00 | −0.95 | +0.84 | 0.0 % |
| 2 | dn | 231 | +0.10 | −0.00 | −1.05 | +0.58 | 77.8 % |
| 3 | up | 266 | **+4.00** | +0.04 | −1.26 | +0.60 | **100.0 %** |
| 3 | dn | 484 | −5.59 | −0.04 | −0.94 | +0.48 | 0.0 % |

### Model-selection epochs

| run | fold 0 | fold 1 | fold 2 | fold 3 |
|---|---|---|---|---|
| 009d (`'ap'`) | 8 / 1 / 1 | 1 / 1 / 1 | 1 / 1 / 1 | 1 / 1 / 1 |
| 009e (`'dir'`) | 6 / 1 / 11 | 1 / 1 / 1 | 5 / 1 / 6 | 3 / 1 / 5 |
| **009f (`'spread'`)** | 5 / 10 / 8 | 1 / 2 / 1 | 2 / 2 / 2 | 5 / 7 / 5 |

`'ap'` collapses to epoch 1 almost everywhere; both directional objectives scatter. The spread objective escapes the epoch-1 collapse but selects models with **lower IC** — it trades calibration for a tail statistic, and loses.

### Artifacts

`/root/btc_lstm_run009f/run009f_scores.npz` (includes the artifact patch: `fwd_close`, `val_labels`, raw `pred`/`val_pred`, `theta_by_h`, `window_sec`), `lstm_run009f_h18.pt`, `run009f_selective.png` → Drive `btc_lstm_run009f/`. The npz makes the whole of this document reproducible offline without the tar.
