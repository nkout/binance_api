# run.009e analysis — the corrected objective made everything worse; the estimator was under-powered, not the hypothesis wrong

*Analyzed 2026-09-21. Notebook: `runs/btc_lstm.run.009e.ipynb` (executed on Colab, 17/17 cells clean, no errors, all three pre-run bug fixes present in the executed source). Build notes: `runs/run009e.notes.md`. Parent: `run.009d` (EARLY_STOP='ap'). Companion: `run009d.offline.md`.*

**TLDR:** run.009e changed exactly one scientific variable — the model-selection objective, from own-label average precision (`'ap'`) to **directional edge** `DE = own-label − opposite-label` on the top-0.1 % of val probs. The gate **FAILED on all six criteria, both sides**. More importantly, *every* metric regressed versus run.009d: pooled IC h6 +0.1093 → **+0.0710**, daily-IC t h6 +13.53 → +9.92, up18 test DE **+18.53 pp → +1.95 pp**, and the market-neutral spread `S` fell from +5.60 bp to **−2.43 bp** with negative point estimates at every horizon (h6–h24: −1.52 … −0.06); with the corrected interval only **h12** is significantly negative, plus **fold 0** (see the correction under "The market-neutral spread went negative"). Selecting on DE produced **less** test DE than selecting on AP.

The cause is a spec error, and it is diagnosable from the training log: `DIR_RATE = 0.001` on a val set of 54 k–115 k bars gives only **54–115 triggers**, and `DE` is a difference of **single-digit label counts** (observed val values quantise to multiples of 1/55 and 1/115; `DE_up = +0.0000` at two epochs where *zero* labels fired on the top-54 val triggers). The selector chased noise — selected epochs sat at val DE ≈ +0.10…+0.18 and produced test DE +0.0195, a 5–10× collapse. **So this experiment does not falsify "directional content is learnable"; it falsifies "top-0.1 % val DE is a usable selector."** The fix is an estimator swap plus a sample-size floor (run.009f).

Two things it *does* corroborate: the **dn heads carry no positive directional content** (DE −5.1 to −7.1 pp, DC 0.61–0.79 at every horizon, in both runs), and the **fold-3 up-side effect is a regime property of the data, not of the model** — it survives an entirely different model selection (+10.40 bp gross, 100.0th percentile of the day-matched null).

---

## Validity checks

- **Executed source is the fixed build.** The notebook in git contains all three pre-run fixes (`np.ndim(probs) == 2` guard in `directional_edge`, `pooled_triggers`, the `g_f = eu_f if side == 'up' else ed_f` null fix). Without them the notebook raises `IndexError` on the first training epoch. All 17 code cells executed, zero error outputs.
- **One variable.** `EARLY_STOP` `'ap'` → `'dir'`. Feature set (58), folds, model (230,790 params), hyperparameters, fee constants, trigger rules, `THETA_BY_H`, `TP_MULT`/`SL_MULT`, the maker fill model and the label construction are untouched. `TRAIN_ARM='baseline'`, `PRUNE_MODE='harmful+dead'`.
- **Same window and folds as run.009d** — 68.8 d, folds 10.1 / 10.1 / 10.1 / 10.5 d, 42 test days, identical `n_train`/`n_val`/`n_test` to the byte (381,515 / 529,418 / 673,263 / 822,421 etc.), same base rates, same `F_DIM=58`. The comparison is clean.
- **New gate.** A∧B∧C∧D∧E∧F on either side, versus A∧B∧C∧D for runs 009a–d. B was replaced (own-label lift → DE ≥ `DIR_MIN`), and E (market-neutral spread > 0 with CI excluding 0) and F (beats the day-matched null p97.5 in ≥3/4 folds) added.

## The result — every metric regressed

| metric | run.009d (`'ap'`) | **run.009e (`'dir'`)** |
|---|---|---|
| pooled IC h6 | +0.1093 | **+0.0710** |
| pooled IC h9 / h12 / h15 | +0.0822 / +0.0669 / +0.0574 | +0.0552 / +0.0469 / +0.0428 |
| pooled IC h18 / h24 | +0.0491 / +0.0394 | +0.0386 / +0.0347 |
| daily-IC t h6 | +13.53 | **+9.92** |
| daily-IC t h18 | +8.55 | +6.65 |
| pooled AP lup_18 / ldn_18 | 0.0963 / 0.1070 | 0.0795 / 0.0888 |
| up18 own % / opp % | 31.14 / 12.61 | 20.66 / 18.71 |
| **up18 DE** | **+18.53 pp** | **+1.95 pp** |
| up18 DC | 2.47 | 1.10 |
| dn18 own % / opp % | 18.55 / 17.84 | 16.31 / 22.07 |
| **dn18 DE** | +0.71 pp | **−5.76 pp** |
| dn18 DC | 1.04 | 0.74 |
| **market-neutral S (h18)** | **+5.60** [−1.43, +5.97] | **−2.43** [−4.95, +0.21] |
| taker up18 / dn18 sim @0.1 % | −5.62 / −10.45 | −10.19 / −12.33 |
| maker up18 / dn18 sim @0.1 % | −6.11 / −10.57 | −10.36 / −12.41 |
| gate | A/B/C/D = F/P/F/F | **A/B/C/D/E/F all fail** |

Per-fold IC also degraded and became noisier: h6 fold 0 +0.0542 ±0.0731 (was +0.0865), fold 2 +0.0537 ±0.0366 (was +0.1112), fold 3 +0.0864 ±0.0419 (was +0.1437).

**Fold 1 is bit-identical in both runs: `+0.1001 ±0.0017` at h6, and every other horizon matches too.** That is not a coincidence — in fold 1 both selectors chose the same epochs `[1, 1, 1]`, so the two runs trained the same three models and produced identical predictions. It is an internal control: the training loop, data path, folds and evaluation are otherwise unchanged, and *every* difference elsewhere in the run is attributable purely to a different epoch choice. It also shows how much of the 009d-vs-009e gap is selector-induced: the unchanged fold sits at the top of the IC range while the three folds where the selectors disagreed collapsed.

### The market-neutral spread went *negative*

| horizon | S (bp) | 95 % CI |
|---|---|---|
| h6 | −1.52 | [−2.90, **−0.23**] |
| h9 | −3.01 | [−4.10, **−0.36**] |
| h12 | −4.31 | [−5.15, **−1.11**] |
| h15 | −4.27 | [−5.69, **−1.16**] |
| h18 | −2.43 | [−4.95, +0.21] |
| h24 | −0.06 | [−3.91, +1.14] |

At every horizon the drift-cancelling long-up/short-dn portfolio has a negative point estimate, but with the corrected interval **only h12 is significant, and only marginally**; the rest include zero. Per fold at h18 only **fold 0** is significantly negative.

> **Correction to this section — now resolved with run.009e's own saved scores.** The `S CI` printed by run.009e is not an interval for `S`. It is `boot_ci_mean(concat(fw[up], −fw[dn]))`, the interval of the equal-per-trade combined mean, and it is both **centred on the wrong statistic** and **too narrow**. Recomputing from `run009e_scores.npz` with the union-of-days bootstrap:
>
> | h | n_up | n_dn | S (bp) | printed CI | **corrected CI** | verdict |
> |---|---|---|---|---|---|---|
> | 6 | 685 | 1,714 | −1.52 | [−2.90, −0.23] | **[−4.04, +0.79]** | includes 0 |
> | 9 | 625 | 1,905 | −3.01 | [−4.10, −0.36] | **[−6.33, +0.73]** | includes 0 |
> | 12 | 665 | 1,881 | −4.31 | [−5.15, −1.11] | **[−8.33, −0.04]** | negative (marginal) |
> | 15 | 653 | 1,892 | −4.27 | [−5.69, −1.16] | **[−8.43, +0.68]** | includes 0 |
> | 18 | 668 | 2,048 | −2.43 | [−4.95, +0.21] | **[−7.02, +3.74]** | includes 0 |
> | 24 | 693 | 2,154 | −0.06 | [−3.91, +1.14] | **[−5.39, +6.58]** | includes 0 |
>
> per fold at h18: f0 −9.94 **[−16.08, −3.41]** (negative) · f1 −2.84 [−7.71, +4.90] · f2 −6.69 [−16.93, +11.16] · f3 +3.48 [−7.85, +23.38].
>
> So the corrected reading is: **the spread is not positive anywhere, and it is significantly negative only at h12 and in fold 0.** The point estimates are unaffected. (The first run.009f build additionally passed a pre-negated dn leg to `boot_ci_spread`, making its interval `mean_up + mean_dn` — caught and fixed; see `run009f.notes.md`.)

## Why it happened — the estimator had ~5 samples' worth of information

`directional_edge` thresholds the top `DIR_RATE` of a head's val probs. At `DIR_RATE = 0.001`:

| fold | n_val | triggers (top 0.1 %) |
|---|---|---|
| 0 | 54,270 | 54 |
| 1 | 71,426 | 71 |
| 2 | 93,809 | 94 |
| 3 | 114,862 | 115 |

The triggered label rate is ~15–25 % (the volatility artifact lifts it well above the ~0.9 % base rate), so `DE = (own_hits − opp_hits)/n` is a difference of roughly **10 hits out of ~55**. One extra hit moves DE by ~0.018; the observed epoch-to-epoch swings of 0.05–0.20 are 3–11 hits. The training log shows the estimator degenerating outright:

- fold 0 seed 0: `DE_up = +0.0000` at epochs 1 **and** 2 — *zero* labels fired on the top-54 val triggers — then +0.1636, +0.1786, +0.1818
- fold 0 seed 1: `DE_up = +0.1818` at ep 1 → `+0.0000` at ep 2
- every reported val DE is an exact multiple of 1/55 or 1/115 (`+0.1818 = 10/55`, `+0.1026 = 12/117`)

The argmax then selected whichever epoch happened to spike, and — because `NaN`/`0.0` is treated as a real value — epochs with no signal at all were selectable. The val→test shrinkage is the signature: **val DE +0.10…+0.18 → test DE +0.0195**, a 5–10× collapse. That is selection on noise, and it is why the dn side came out *negative* on test: the val tail statistic is anti-correlated with test performance.

**Attribution.** `DIR_RATE = 0.001` was specified to mirror the gate's nominal trigger rate, without checking the number of val samples against the label base rate. The diagnosis in `run009d.offline.md` (the parent's edge is volatility selection, not direction) is unaffected by this — but run.009e is not a fair test of it.

## What this establishes, and what it does not

**Does not establish:** that directional content is unlearnable, or that the 009d result was an artifact of AP selection. The selector was too noisy for either reading.

**Does establish:**

1. **The dn heads have no positive directional content — and here actively inverted.** DE −5.76 pp at h18, and −5.11 to −7.12 pp across all six horizons; DC 0.61–0.79 at every horizon. run.009d had DC ≈ 1.04–1.16. Two independent model selections now agree that the down-side heads carry no direction out-of-sample. Combined with the dn heads' consistently negative gross EV in `run009d.offline.md` (positive only in the anti-correlated fold), the down-side is the clearest closed case in the project.
2. **The fold-3 up-side effect is a property of the data, not the model.** Fold 3 up18 still earns **+10.40 bp gross** and sits at the **100.0th percentile** of the day-matched random-entry null (blind +0.04, null p97.5 +1.01) — under a completely different model-selection objective that produced a much weaker model overall. That is now two independent runs. It remains one 10.5-day window and still one observation.
3. **`unan` unanimity collapsed.** up18 @0.1 % went from n=243 (run.009d) to **n=4**; @0.01 % to **n=0**. With seeds selecting wildly different epochs (fold 0 chose epochs 6 / 1 / 11), "all three seeds beyond their own rolling τ" almost never fires. The unanimity variant is not identifiable from this run.

## Known defect in the diagnostic (fixed in run.009f)

The printed `S CI` is `boot_ci_mean(concat(fw[up], −fw[dn]))` — the CI of the **equal-per-trade combined mean**, not of `S = mean(fw[up]) − mean(fw[dn])`. With n_up = 668 and n_dn = 2,048 the two differ (for h18 the concat mean is −2.48 vs S = −2.44). The point estimates of `S` are correct; the CIs are approximate. The same flaw is in `run009d.offline.md` §8 — see the correction note added there. run.009f replaces it with `boot_ci_spread`, which resamples the **union** of days so days shared by both legs stay paired.

**This is not a cosmetic difference.** Measured on the 009d-era data with the same point estimate (`S = +5.60`, n_up = 1,063, n_dn = 1,418): the old interval is `[−1.43, +5.97]` (width 7.40), the corrected union-of-days interval is **`[−1.80, +10.88]` (width 12.67)** — **1.71×** too narrow, and centred on the concat mean `+2.31` rather than on `S`. Every "CI excludes 0" claim in this document and in `run009d.offline.md` Part 2 was computed that way. The qualitative conclusions in `run009d.offline.md` rest on **point estimates** and survive (`S ≤ 0` in fold 3; `S` below the fee floor everywhere), but the significance statements had to be re-derived — the re-derivation for run.009e is in the table above and **cuts the negative claim from four horizons to one (h12) plus fold 0**. Two implementation bugs were found and fixed in the process: the interval was centred on the wrong statistic, and run.009f's first build passed a pre-negated dn leg to `boot_ci_spread` (producing `mean_up + mean_dn`). The test suite now asserts that the interval **brackets the point estimate** and that the dn leg is not pre-negated.

## Recommended next run (implemented as run.009f)

Keep the diagnosis, fix the estimator:

1. **`DIR_RATE` 0.001 → 0.02** (2 % of val = 1,085–2,297 triggers per leg).
2. **Select on the continuous val market-neutral spread**, not a binary-label tail difference: `mean(y_targets | up-head top-rate triggers) − mean(y_targets | dn-head top-rate triggers)`. It uses every triggered sample's target value, cancels drift by construction, and has no dependence on single-digit counts.
3. **Sample-size floor** (`MIN_TRIG_VAL = 500`): if either leg is short the metric is `NaN` and the epoch must be ineligible — `np.isfinite(cur) and cur > best` — with a `best_state is None` fallback so a never-selected model cannot crash the run. The current code silently treats `0.0000` as a real score, which is how degenerate epochs got selected.
4. **Fix the `S` CI** (`boot_ci_spread`).
5. Keep `'ap'` as the control: run.009d's numbers are already the same-window baseline.

## Cosmetic / carry-over issues

- The gate's failure guide (cell 17 footer) is still run.007-era prose and has no branch for "market-neutral spread significantly negative" — which is exactly this run's situation. It under-diagnoses.
- The `S CI` column label does not say which statistic it belongs to.

---

## Appendix — reference tables (run.009e)

### Setup

| Horizon | Wall-clock | θ | test base % (up/dn) |
|---|---|---|---|
| h6 | 30 s | 15 bp | 0.47 / 0.45 |
| h9 | 45 s | 15 bp | 0.93 / 0.86 |
| h12 | 60 s | 20 bp | 0.63 / 0.58 |
| h15 | 75 s | 20 bp | 0.91 / 0.83 |
| h18 | 90 s | 20 bp | 1.21 / 1.09 |
| h24 | 2 min | 20 bp | 1.89 / 1.69 |

Fees: taker RT 10 bp, taker-in/maker-out 7 bp, full-maker 4 bp. `SEQ_LEN` 192, `HIDDEN_DIM` 128, 3 seeds/fold, 4 expanding folds.

### Per-fold IC (seed-ensemble; ± = per-seed spread)

| Fold | n_test | h6 | h9 | h12 | h15 | h18 | h24 |
|---|---|---|---|---|---|---|---|
| 0 | 165,059 | +0.0542 ±0.0731 | +0.0516 ±0.0587 | +0.0504 ±0.0432 | +0.0525 ±0.0364 | +0.0520 ±0.0265 | +0.0540 ±0.0139 |
| 1 | 166,228 | +0.1001 ±0.0017 | +0.0726 ±0.0029 | +0.0594 ±0.0032 | +0.0502 ±0.0041 | +0.0438 ±0.0064 | +0.0353 ±0.0045 |
| 2 | 170,211 | +0.0537 ±0.0366 | +0.0407 ±0.0251 | +0.0337 ±0.0173 | +0.0296 ±0.0157 | +0.0255 ±0.0097 | +0.0222 ±0.0085 |
| 3 | 164,530 | +0.0864 ±0.0419 | +0.0625 ±0.0342 | +0.0483 ±0.0260 | +0.0416 ±0.0240 | +0.0352 ±0.0200 | +0.0282 ±0.0186 |
| **pooled** | 666,028 | **+0.0710** | +0.0552 | +0.0469 | +0.0428 | +0.0386 | +0.0347 |

### Drift control — own vs opposite lift, DE, DC, |fwd|, S @0.1 % ens

| head | own % | own lift | opp % | opp lift | DE (pp) | DC | \|fwd\| trig | \|fwd\| all | S (bp) |
|---|---|---|---|---|---|---|---|---|---|
| up6 / dn6 | 15.33 / 11.20 | 32.53 / 25.13 | 12.55 / 18.32 | 28.16 / 38.88 | +2.77 / −7.12 | 1.22 / 0.61 | 9.3 / 10.5 | 2.2 | −1.52 |
| up9 / dn9 | 17.44 / 14.28 | 18.85 / 16.62 | 17.60 / 21.36 | 20.49 / 23.09 | −0.16 / −7.09 | 0.99 / 0.67 | 10.5 / 12.2 | 2.8 | −3.01 |
| up12 / dn12 | 14.74 / 12.12 | 23.54 / 20.90 | 16.69 / 19.25 | 28.79 / 30.75 | −1.95 / −7.12 | 0.88 / 0.63 | 12.8 / 14.7 | 3.3 | −4.31 |
| up15 / dn15 | 18.07 / 15.06 | 19.90 / 18.19 | 19.30 / 22.15 | 23.30 / 24.39 | −1.23 / −7.08 | 0.94 / 0.68 | 14.2 / 16.5 | 3.7 | −4.27 |
| up18 / dn18 | 20.66 / 16.31 | 17.00 / 14.99 | 18.71 / 22.07 | 17.20 / 18.17 | **+1.95 / −5.76** | 1.10 / 0.74 | 15.2 / 16.4 | 4.0 | −2.43 |
| up24 / dn24 | 22.37 / 19.27 | 11.86 / 11.43 | 17.89 / 24.37 | 10.61 / 12.93 | +4.47 / −5.11 | 1.25 / 0.79 | 15.4 / 16.7 | 4.7 | −0.06 |

### Blind benchmark + day-matched null @h18 @0.1 % ens

| fold | side | n | model | blind | null mean | null p2.5 | null p97.5 | percentile |
|---|---|---|---|---|---|---|---|---|
| 0 | up | 184 | −0.01 | +0.13 | +0.19 | −0.77 | +1.35 | 36.4 % |
| 0 | dn | 344 | −9.93 | −0.13 | −0.44 | −1.50 | +0.42 | 0.0 % |
| 1 | up | 184 | −3.20 | +0.06 | +0.06 | −1.33 | +1.46 | 0.0 % |
| 1 | dn | 997 | +0.35 | −0.06 | +0.00 | −0.62 | +0.62 | 88.0 % |
| 2 | up | 136 | −2.00 | +0.00 | −0.16 | −1.63 | +1.06 | 0.5 % |
| 2 | dn | 173 | −4.70 | −0.00 | −0.53 | −1.65 | +0.55 | 0.0 % |
| 3 | up | 164 | **+10.40** | +0.04 | −0.23 | −1.48 | +1.01 | **100.0 %** |
| 3 | dn | 534 | −6.92 | −0.04 | −0.29 | −0.94 | +0.37 | 0.0 % |

### h18 selective table @0.1 % ens (taker)

| head | n | /day | hit % | base % | lift | sim bps [CI] | endpt bps |
|---|---|---|---|---|---|---|---|
| up18 | 668 | 15.90 | 20.7 | 1.21 | 17.00 | −10.19 [−13.02, −6.33] | −8.74 |
| dn18 | 2,048 | 48.76 | 16.3 | 1.09 | 14.99 | −12.33 [−15.16, −9.16] | −13.70 |

### Maker entry, primary config @0.1 % ens — h18

| head | n_trig | fill % | n_fill | hit_f % | lift_f | sim bps [CI] | EV/trig | missed | hit_m % |
|---|---|---|---|---|---|---|---|---|---|
| up18 | 668 | 78.6 | 525 | 14.5 | 11.91 | −10.36 [−13.00, −6.87] | −8.14 | +13.2 | 43.4 |
| dn18 | 2,048 | 79.0 | 1,617 | 10.6 | 9.77 | −12.41 [−15.27, −9.45] | −9.79 | +9.7 | 37.6 |

### Gate (run.009e)

| criterion | up18 @0.1 % | dn18 @0.1 % |
|---|---|---|
| A maker sim > 0, CI > 0 | fail (−10.36 [−13.00, −6.87]) | fail (−12.41 [−15.27, −9.45]) |
| B (info) own-label lift ≥3× | pass (17.00×, 4/4) | pass (14.99×, 4/4) |
| **B directional content** | **fail (DE +1.95 pp < 5.0, folds 2/4)** | **fail (DE −5.76 pp, folds 1/4)** |
| C fill ≥25 % + folds sim>0 ≥3/4 | fail (78.6 %, 0/4) | fail (79.0 %, 0/4) |
| D neighbours sim > 0 | fail (−10.71 @0.01 %, −9.94 @1 %) | fail (−13.95, −11.02) |
| **E market-neutral S > 0, CI > 0** | **fail (S −2.43)** | **fail (S −2.43)** |
| **F beats null p97.5 in ≥3/4** | **fail (1/4)** | **fail (0/4)** |

### Model-selection epochs (4 folds × 3 seeds)

| run | fold 0 | fold 1 | fold 2 | fold 3 |
|---|---|---|---|---|
| 009d (`'ap'`) | ep 8 / 1 / 1 | 1 / 1 / 1 | 1 / 1 / 1 | 1 / 1 / 1 |
| **009e (`'dir'`)** | ep 6 / 1 / 11 | 1 / 1 / 1 | 5 / 1 / 6 | 3 / 1 / 5 |

`'ap'` collapsed to epoch 1 almost everywhere; `'dir'` scattered — consistent with a noisy objective rather than a systematically later stopping point.

### Artifacts

`/root/btc_lstm_run009e/run009e_scores.npz`, `lstm_run009e_h18.pt`, `run009e_selective.png` → Drive `btc_lstm_run009e/`. The npz carries the 009e artifact patch (forward closes, raw preds, val labels), so all of the above is reproducible offline without the tar.
