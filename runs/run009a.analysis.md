# run.009a analysis — GATE FAILED on the longest window yet (68.8 d); the ranking signal is the strongest and most stable in the project, the economics are unmoved

*Analyzed 2026-09-20. Notebook: `runs/btc_lstm.run.009a.ipynb` (first-touch triple-barrier labels, 5-second `w5` bars, wall-clock constants ×3). Executed on Colab T4; all 15 cells ran clean, no errors.*

**TLDR:** run.009a is run.009's first-touch label experiment re-executed on the **68.8-day `w5` accumulate** (2026-07-13 → 09-20) — the long-window economic verdict the project had been waiting for since run.008, and by far the most statistically powerful run so far (1.10 M valid samples, 4 folds × ~10.1 test days, 42 test days). The gate **FAILED on both sides** — A (sim > 0) fail, C (per-fold sim > 0) **0/4 folds**, D fail; only B (ranking lift) passes. The regression signal, however, is the **strongest and most stable ever measured**: daily-IC t = **h6 +11.63 / h18 +9.18** over 42 days, positive IC in **4/4 folds at every horizon**, and the fold-3 calibration collapse that broke run.011 is **gone**. So the project's open question — *"does the edge survive 27+ test days / 4 week-long folds, or was it two lucky regimes?"* — is answered in the affirmative for the **ranking signal**, while the **economics remain exactly where runs 007/008/011 left them**: every horizon, every TP/SL variant, every fold, both sides, all negative at ≈ −10 bps. Adverse selection is confirmed at 3× the data (hit_f 12.1 % vs hit_m 39.0 % up; 12.5 % vs 46.1 % dn) with healthy fill rates (≈76–78 %). The decisive number: **dn18 fold 3 hit 38.1 % at lift 40× and still lost −11.06 bps** — the heads are ~20 hit-points short of the ≈58.5 % taker breakeven, and no amount of bar-level data closes that.

## Validity checks

- **Longest, cleanest window in the project:** 412 `w5` files, 1,186,560 rows loaded, schema **v5 100 %**, era start 2026-07-13 20:58:40 → data end 2026-09-20 15:50:45 = **68.8 days**. 1,161,525 rows after `dropna` (2.11 % dropped); **1,101,579 valid samples (94.8 %)**. 127 cadence breaks, largest gap 1.7 h — all absorbed by the contiguity/gap-validity masks.
- **76/76 features kept** (schema-3/4: 29/29) — no coverage-guard exclusions, no v1-compat degradation. The full feature set is finally under test on a window long enough to matter.
- **Folds have real power now:** test blocks of 10.1 / 10.1 / 10.1 / 10.5 days (vs run.010a's 4.7 d, run.011's 4.8 d). The notebook's own `<7 days → indicative only` warning fires on **none** of them. 42 test days ≥ the 27+ the cross-run doc asked for.
- **Training sizes:** 381 k → 822 k samples (expanding window), val 54 k → 115 k.
- **Execution integrity:** all 15 cells executed in order, zero error outputs, scores + model + plot persisted to Drive.

### Caveat that changes the reading — base rates are ~2× the sanity anchor

| label | run.011 (32.7 d) | **run.009a (68.8 d)** |
|---|---|---|
| lup_18 | 0.43 % | **0.93 %** (test pool 1.22 %) |
| ldn_18 | 0.36 % | **0.81 %** (test pool 1.09 %) |

The first-touch base rate roughly **doubled**. That is a regime/volatility change (the appended 08-15 → 09-20 stretch is materially more volatile — fold-2/3 train `lup_18` base rates are 0.9 % vs fold-0's 0.5 %), not a bug: the label, θ map, gap-validity rule and loader are byte-identical to run.009/010, and the ×3 wall-clock conversion is correct. But it means **this run is not number-comparable to run.008/010/011**, and the notebook's own read-guide item #1 ("Cell 10 must ≈ reproduce run.007: up6 −10.0 / dn6 −12.9") cannot hold — here up6 is −9.31 / dn6 −10.53. Treat run.009a as its own verdict on its own window, not a reproduction check.

## The signal — strongest and most stable yet

**Pooled seed-ensemble regression IC** (monotone decay, same shape as every prior run):

| h6 | h9 | h12 | h15 | h18 | h24 |
|---|---|---|---|---|---|
| +0.0886 | +0.0705 | +0.0615 | +0.0557 | +0.0494 | +0.0419 |

**Daily-IC t-stats, 42 days** (the metric the project trusts):

| horizon | run.011 (20 days) | **run.009a (42 days)** |
|---|---|---|
| h6 (30 s) | +7.75 | **+11.63** |
| h9 | +6.74 | +11.13 |
| h12 | +5.97 | +11.08 |
| h15 | +5.40 | +10.54 |
| h18 (90 s) | +4.66 | **+9.18** |
| h24 (2 min) | +4.01 | +6.91 |

**Per-fold IC — positive in 4/4 folds at every horizon:**

| Fold | n_test | h6 | h9 | h12 | h15 | h18 | h24 |
|---|---|---|---|---|---|---|---|
| 0 (08-10→08-20) | 164,984 | +0.0703 | +0.0603 | +0.0580 | +0.0578 | +0.0541 | +0.0523 |
| 1 (08-20→08-31) | 166,153 | +0.0612 | +0.0549 | +0.0542 | +0.0541 | +0.0528 | +0.0491 |
| 2 (08-31→09-10) | 170,136 | +0.1056 | +0.0774 | +0.0614 | +0.0486 | +0.0361 | +0.0222 |
| 3 (09-10→09-20) | 164,671 | +0.1332 | +0.1003 | +0.0800 | +0.0674 | +0.0584 | +0.0471 |
| **pooled** | 665,944 | **+0.0886** | +0.0705 | +0.0615 | +0.0557 | +0.0494 | +0.0419 |

**Opportunity-head test AP — no fold collapse:**

| head | fold 0 | fold 1 | fold 2 | fold 3 | pooled | base % |
|---|---|---|---|---|---|---|
| lup_18 | 0.0455 | 0.0914 | 0.0579 | 0.1761 | 0.0827 | 1.22 |
| ldn_18 | 0.0418 | 0.1071 | 0.0711 | 0.1792 | 0.0890 | 1.09 |

This is the important structural change vs run.011. There, fold 3 (newest) had the highest regression IC but a near-zero up-side AP (0.0039) — the "calibration-decay asymmetry" that fed the trailing-IC meta-filter idea. **Here fold 3 has the *highest* AP on both sides (0.176 / 0.179).** The asymmetry is gone: the BCE heads are now calibrated exactly where the regression signal is strongest. Combined with positive IC in every fold and reasonably balanced trigger shares (up18 by fold: 11 % / 38 % / 17 % / 33 %; dn18: 32 % / 36 % / 15 % / 17 % — max 38 %, vs run.009's 97 %-in-one-fold), the project's chronic regime-concentration concern (cross-run finding #6) is **substantially resolved for the ranking signal**.

## The gate — FAILED, both sides

Pre-registered: h18 heads (90 s), 0.1 % rolling-ensemble, maker entry (wait 6, δ = 0, delay 0), TP = SL = 1.0·θ.

| criterion | up18 | dn18 |
|---|---|---|
| **A** — maker sim > 0, day-boot CI > 0 | **fail** (−10.74, CI [−14.71, −7.57]) | **fail** (−10.99, CI [−13.04, −9.46]) |
| **B** — lift ≥ 3× pooled, ≥ 2× in ≥ 3/4 folds | pass (15.32×, 3/4) | pass (18.46×, 4/4) |
| **C** — fill ≥ 25 %, per-fold sim > 0 in ≥ 3/4 | **fail** (fill 75.8 % ok, **0/4** folds) | **fail** (fill 77.4 % ok, **0/4**) |
| **D** — neighbour rates sim > 0 | **fail** (−10.20 @0.01 %, −9.39 @1 %) | **fail** (−13.30 @0.01 %, −10.76 @1 %) |

Only B passes. **C is the damning one: `0/4` folds have a positive maker sim on either side** — this is not a pooled-mean artifact hiding a fold-2-style collapse; there is no good fold to hide in.

Per-fold maker sim at the gate:

| fold | up18 n / hit_f / sim | dn18 n / hit_f / sim |
|---|---|---|
| 0 | 150 / 0.0 % / −8.44 | 341 / 4.8 % / −9.67 |
| 1 | 502 / 7.4 % / −12.18 | 386 / 14.6 % / −10.16 |
| 2 | 224 / 13.4 % / −17.77 | 162 / 12.7 % / −12.49 |
| 3 | 440 / 21.5 % / −5.79 | 176 / 23.3 % / −14.10 |

Note **fold 0 up18: 150 triggers, 0.0 % filled hit rate** — the top-0.1 % up-head selection in that fold produced not a single path that reached +θ before −θ.

## The failure mode — adverse selection *and* the hit-rate wall, simultaneously

The notebook's cell-11 failure guide offers two branches; run.009a does not sit cleanly in either, and that is itself the finding.

| side @0.1 % | hit_f | hit_m | missed (gross bps) | fill % |
|---|---|---|---|---|
| up18 | **12.1 %** | **39.0 %** | +10.0 | 75.8 % |
| dn18 | **12.5 %** | **46.1 %** | +12.1 | 77.4 % |

- Branch 1 ("fill% < FILL_MIN, hit_m ≫ hit_f"): the *symptom* is present — hit_m is **3.2× / 3.7×** hit_f, the unfilled triggers carry +10–12 bps of gross move (the winners the limit never gets) — but fill% is a healthy 76–78 %, so it is not the fill-starvation branch.
- Branch 2 ("fill% fine, sim ≤ 0, hit_f ≈ cell-10 hit"): sim ≤ 0 and fill% fine, but hit_f (12.1 %) is **6.5 points below** the cell-10 taker hit (18.6 %) — maker fill mechanics cost real hit rate.

So the honest read is **both**: posting a limit converts an 18.6 % trigger hit into a 12.1 % filled hit (adverse selection, exactly run.008's mechanism, now measured on 3× the data), and the residual 12.1 % is still ~46 points below the ≈58.5 % taker / ≈49 % full-maker breakeven. Entry mechanics and prediction quality are both short of the mark.

## Economics — the wall is unchanged and now robust

Taker selective table (cell 10), h18 @0.1 % ens:

| head | n | /day | hit % | base % | lift | sim bps [95 % CI] | endpt bps |
|---|---|---|---|---|---|---|---|
| up18 | 1,316 | 31.3 | 18.6 | 1.22 | 15.32 | −10.39 [−14.91, −7.23] | −11.52 |
| dn18 | 1,065 | 25.4 | 20.1 | 1.09 | 18.46 | −10.40 [−12.15, −8.91] | −10.57 |

Maker (cell 10M) h18 @0.1 %:

| head | n_trig | fill % | n_fill | hit_f % | lift_f | sim bps [CI] | EV/trig | hit_m % |
|---|---|---|---|---|---|---|---|---|
| up18 | 1,316 | 75.8 | 998 | 12.1 | 9.98 | −10.74 [−14.71, −7.57] | −8.15 | 39.0 |
| dn18 | 1,065 | 77.4 | 824 | 12.5 | 11.48 | −10.99 [−13.04, −9.46] | −8.50 | 46.1 |

Everything else is negative too — there is no corner of this run that clears zero:

- **All six horizons** @0.1 % taker: up18 −9.31 (h6) … up24 −9.67; dn −10.22 … −11.22. Spread ≈2 bps, all < 0.
- **All five TP/SL variants:** best is (TP 1.0, SL 0.5) at up −9.75 / dn −9.64; worst (TP 1.0, no SL) −11.49 / −10.70. Exits add ~nothing (run.007's lesson, re-confirmed on 68 days).
- **Non-overlapping 1-BTC trades:** up18 −$13,338 (164 trades, mean −$81.3, win 31.7 %); dn18 −$13,316 (166, −$80.2, 31.3 %). Maker: −$11,469 / −$11,255.
- **Regression-threshold control** (`reg` variant, same rolling-τ): up18 @0.1 % −10.92, dn18 −9.34. Same **−9 to −11 bps** at hit ≈ base rate (lift 0.43× / 0.12×) — i.e. a purely un-selective threshold loses the same as the selective heads. The heads buy **lift** (15–18×) but **zero net**: the ~−10 bps is structural (fees + adverse-move distribution), not selection-bound.
- **The pre-registered rescue routes failed again:** 30 s-timing hybrid degenerates to **99.2 % / 88.2 % taker** (sim −10.36 / −10.54, i.e. plain taker); wait×δ grid all negative with deeper δ strictly worse; entry-delay stress −10.39 → −10.99 (up) / −10.40 → −10.65 (dn) ⇒ latency-robust, not a latency artifact.
- **Unanimity** is the only thing that looks less bad — up18 @0.1 % unan n=173, hit 28.9 %, sim −6.61 [−28.54, −2.57] — but the CI is enormous and the `+13.00` @0.01 % row is **7 trades** (n=7, 100 % hit): noise, not evidence.

**The decisive stat:** dn18 fold 3 — hit **38.1 %**, lift **40.3×**, sim **−11.06**, endpt −20.00. Even at ~38 % correct, the trade loses, because the losing paths are deep (dn stops gap through, so the average loser is far worse than −θ). Breakeven is ≈58.5 % taker. The heads would need to roughly **1.5×** their best fold-level hit rate; no bar-level lever in 12 runs has moved it by more than a couple of points.

## New observation — model selection picks epoch 1 in 10 of 12 seed-folds

This is not in any prior analysis and it matters.

Across the 4 folds × 3 seeds, the early-stop metric (mean h18 opportunity-head val AP) selects:

`ep1, ep1, ep8, ep7, ep1, ep1, ep1, ep1, ep1, ep1, ep1, ep1` → **10/12 select epoch 1**.

And the val IC tells the same story. Fold 0 seed 0: `IC_h18` = **+0.1124 (ep1)** → +0.0590 → +0.0285 → +0.0407 → +0.0286 → +0.0211 (ep6, early stop), while the training loss falls 1.33 → 0.39. The trunk learns the 90 s signal almost immediately and then **overfits it away** over 5–11 further epochs.

Consequences:
1. The reported ICs are effectively **one-epoch ICs**. The 12-epoch cosine schedule with patience 5 is mostly wasted compute, and the "best" model is the barely-trained one.
2. The model-selection objective and the value objective **diverge**: val AP peaks at ep 1, and further training lowers val IC — yet the regression *loss* keeps falling. The Huber+BCE objective is being optimised past the point of predictive value.
3. This reframes cross-run finding #1 ("capacity is not the constraint"): at these horizons the problem is not merely capacity — it is that **optimisation is unstable within one epoch** on this objective. A 240 k-parameter LSTM already overfits the 30–90 s signal in minutes of training.

This is a concrete, cheap lever (fewer epochs / lower LR / stronger dropout / an IC-based early-stop rather than AP) that no prior run has tested.

## Where this leaves the project

The cross-run doc gave the ≥45-day run two questions. run.009a answers the first and leaves the second open.

1. **Gate/regime — ANSWERED: the edge is not two lucky regimes.** 42 test days, 4 folds of ~10 days each, positive IC in 4/4 folds at every horizon, balanced trigger shares, no fold-3 AP collapse. The ranking signal **survives** and is stronger than at any shorter window. But the gate still fails on **economics**, in **every fold**, on both sides. ⇒ **"Collect more bar-level data" is now an exhausted lever.** The binding constraint is not statistical power.
2. **Fill-timing — STILL OPEN.** run.009a has no cell 10D (the pooled-OOF fill-split AUC) and no cell 10R (the router); it is run.009/010 lineage, not run.011's. So the 0.610→0.518 question from run.011 is untouched here. What run.009a *does* show is that the fill structure run.011 tried to exploit (hit_m ≫ hit_f) is **large and stable on 68 days** — so the question remains whether it is *causally predictable* from any captured feature. Bar-level features evidently are not (the heads that should capture it lose money).

Given that, the two remaining levers are the ones run.011/run.012 already named, and run.009a strengthens the case for both:

- **Event / sub-second order flow** (run.012's `ev_*` v5 features): the only remaining input class that could carry fill-timing information the 5 s bar averages away. run.009a removes the "maybe we just needed more bars" alternative explanation.
- **Execution mechanics** (queue position, maker-queue modelling): the ≈−10 bps is now demonstrably a property of *being in the trade*, not of *which* 0.1 % you pick.

## Cosmetic issues (do not affect results, do affect re-reads)

- The notebook's closing read-guide and several inline comments are **stale run.009-era text**: item #1 says cell 10 must reproduce run.007's `up6 −10.0 / dn6 −12.9` (impossible on a different window); item #4's gate description says "h6, 0.1 %" while the code gates **h18**; cell 13's header still calls h18 "the run.007 headline". On a 68.8-day window these are misleading — a fresh reader would reasonably conclude the run is broken because the "reproduction" fails. Worth cleaning when 009a-lineage code is next reused.
- The cell-11 failure guide has no branch for "adverse selection **and** hit-rate-bound **and** fill% healthy", which is exactly run.009a's situation. Its printed guidance therefore under-diagnoses the run.
- Cosmetic only: `H2_IDX = HORIZONS.index(6)` labels the 30 s head "h2" in comments/prints (it is `h6` at 5 s). Harmless but confusing.

---

# Appendix — reference tables

## Setup

| Horizon | Wall-clock | θ | Test base rate (up/dn) |
|---|---|---|---|
| h6 | 30 s | 15 bp | 0.47 % / 0.45 % |
| h9 | 45 s | 15 bp | 0.93 % / 0.86 % |
| h12 | 60 s | 20 bp | 0.63 % / 0.58 % |
| h15 | 75 s | 20 bp | 0.91 % / 0.83 % |
| h18 | 90 s | 20 bp | 1.22 % / 1.09 % |
| h24 | 2 min | 20 bp | 1.89 % / 1.69 % |

Fees: taker RT 10 bp, TP RT (taker-in/maker-out) 7 bp, full-maker RT 4 bp. 5 s bars, `SEQ_LEN` 192 (16 min), `HIDDEN_DIM` 128, 3 seeds per fold, 4 expanding folds × 10.1–10.5 d test. Model 240,042 params.

## Data / window

| item | value |
|---|---|
| files | 412 (`w5` only) |
| rows loaded | 1,186,560 |
| rows after dropna | 1,161,525 (2.11 % dropped) |
| valid samples | 1,101,579 (94.8 %) |
| window | 2026-07-14 20:58:30 → 2026-09-20 15:50:45 |
| schema-3+ era | 2026-07-13 20:58:40 (68.8 d), v5 100 % |
| features kept | 76/76 (schema-3/4 29/29) |
| cadence breaks | 127 (largest gap 1.7 h) |

## Frozen-val rule (comparison, not gated)

| head | rate | n | /day | hit % | lift | sim bps [CI] | endpt |
|---|---|---|---|---|---|---|---|
| up18 | 0.1 % | 1,683 | 40.1 | 16.3 | 13.45 | −10.43 [−14.01, −7.39] | −11.56 |
| dn18 | 0.1 % | 1,019 | 24.3 | 21.0 | 19.30 | −9.80 [−11.60, −8.57] | −9.99 |

## Seed-unanimity (realized rate < nominal by design)

| head | rate | n | hit % | lift | sim bps [CI] |
|---|---|---|---|---|---|
| up18 | 0.01 % | 7 | 100.0 | 82.30 | +13.00 [+13.00, +13.00] ← n=7, noise |
| up18 | 0.1 % | 173 | 28.9 | 23.79 | −6.61 [−28.54, −2.57] |
| up18 | 1 % | 1,940 | 25.7 | 21.17 | −8.56 [−14.27, −5.47] |
| dn18 | 0.1 % | 47 | 21.3 | 19.55 | −18.52 [−26.28, −10.25] |
| dn18 | 1 % | 1,768 | 24.8 | 22.81 | −10.15 [−12.09, −8.34] |

## Exploratory — all horizons @0.1 % ens (taker)

| head | n | hit % | lift | sim bps [CI] |
|---|---|---|---|---|
| up6 | 1,420 | 13.6 | 28.84 | −9.31 [−10.89, −8.49] |
| dn6 | 1,500 | 9.3 | 20.93 | −10.53 [−11.30, −9.44] |
| up9 | 1,346 | 19.8 | 21.36 | −9.45 [−11.51, −8.43] |
| dn9 | 1,250 | 13.0 | 15.18 | −10.80 [−11.85, −9.39] |
| up12 | 1,400 | 13.0 | 20.77 | −10.45 [−13.85, −8.69] |
| dn12 | 1,477 | 12.1 | 20.78 | −10.22 [−11.24, −8.51] |
| up15 | 1,402 | 16.0 | 17.67 | −10.44 [−13.98, −8.16] |
| dn15 | 1,358 | 15.2 | 18.41 | −10.28 [−11.62, −8.28] |
| up24 | 1,156 | 24.4 | 12.94 | −9.67 [−14.89, −5.23] |
| dn24 | 801 | 22.7 | 13.47 | −11.22 [−14.80, −8.96] |

## Exploratory — TP/SL grid, h18 @0.1 % ens

| head | TP·θ | SL·θ | sim bps [CI] |
|---|---|---|---|
| up18 | 1.0 | 1.0 | −10.39 [−14.91, −7.23] |
| up18 | 1.0 | 0.5 | −9.75 [−13.76, −7.06] |
| up18 | 1.0 | none | −11.49 [−18.07, −7.47] |
| up18 | 1.5 | 1.0 | −10.33 [−15.06, −6.92] |
| up18 | 1.5 | 0.5 | −9.68 [−13.75, −6.79] |
| dn18 | 1.0 | 1.0 | −10.40 [−12.15, −8.91] |
| dn18 | 1.0 | 0.5 | −9.64 [−11.36, −7.97] |
| dn18 | 1.0 | none | −10.70 [−13.80, −8.62] |
| dn18 | 1.5 | 1.0 | −10.56 [−12.57, −8.95] |
| dn18 | 1.5 | 0.5 | −9.74 [−11.56, −8.00] |

## Maker wait×δ grid, h18 @0.1 % ens (exploratory)

| head | wait | δ·θ | fill % | hit_f % | sim bps | EV/trig |
|---|---|---|---|---|---|---|
| up18 | 6 | 0.00 | 75.8 | 12.1 | −10.74 | −8.15 |
| up18 | 6 | 0.25 | 39.1 | 8.8 | −12.05 | −4.70 |
| up18 | 6 | 0.50 | 18.2 | 1.3 | −15.65 | −2.85 |
| up18 | 18 | 0.00 | 86.2 | 12.4 | −10.71 | −9.24 |
| up18 | 18 | 0.50 | 42.2 | 4.0 | −12.42 | −5.24 |
| dn18 | 6 | 0.00 | 77.4 | 12.5 | −10.99 | −8.50 |
| dn18 | 6 | 0.50 | 20.8 | 5.0 | −15.07 | −3.13 |
| dn18 | 18 | 0.00 | 86.6 | 14.0 | −10.96 | −9.49 |
| dn18 | 18 | 0.50 | 39.2 | 6.2 | −13.43 | −5.26 |

Deeper δ strictly lowers fill% without improving filled-hit — adverse selection worsens. Waiting longer (up to 18 bars = 90 s = the whole horizon) buys ~10 fill points and moves sim by <0.1 bp.

## Entry-delay stress @0.1 % ens

| head | variant | n | sim bps [CI] |
|---|---|---|---|
| up18 | taker+0 | 1,316 | −10.39 [−14.91, −7.23] |
| up18 | taker+3bar (15 s) | 1,316 | −10.99 [−15.02, −8.17] |
| up18 | maker delay=3 (15 s) | 873 | −14.15 [−17.80, −11.15] (fill 66.3 %) |
| dn18 | taker+0 | 1,065 | −10.40 [−12.15, −8.91] |
| dn18 | taker+3bar (15 s) | 1,065 | −10.65 [−12.47, −9.42] |
| dn18 | maker delay=3 (15 s) | 741 | −13.49 [−16.24, −11.53] (fill 69.6 %) |

## 30 s-timed hybrid @0.1 % ens

| head | n_trig | routed taker | fill % | hit_f % | sim bps [CI] | EV/trig |
|---|---|---|---|---|---|---|
| up18 | 1,316 | 1,306 (99.2 %) | 100.0 | 18.6 | −10.36 [−14.89, −7.21] | −10.36 |
| dn18 | 1,065 | 939 (88.2 %) | 97.0 | 19.0 | −10.54 [−12.34, −9.07] | −10.22 |

Degenerates to plain taker on the up side — the 30 s regression head says "leaving now" almost always (same result as run.008's h2 hybrid).

## Non-overlapping 1-BTC trades (tp = sl = 1.0·θ, time-stop h)

| config | trades | total $ | mean $ | win % |
|---|---|---|---|---|
| up18 @0.1 % ens (taker) | 164 | −13,338 | −81.3 | 31.7 |
| dn18 @0.1 % ens (taker) | 166 | −13,316 | −80.2 | 31.3 |
| up18 @0.1 % unan (taker) | 23 | −1,923 | −83.6 | 30.4 |
| dn18 @0.1 % unan (taker) | 15 | −1,375 | −91.7 | 40.0 |
| up18 @0.1 % ens (maker) | 144 filled (182 posted) | −11,469 | −79.6 | 27.1 |
| dn18 @0.1 % ens (maker) | 133 filled (170 posted) | −11,255 | −84.6 | 26.3 |
| up18 @0.1 % unan (maker) | 19 filled (26 posted) | −1,715 | −90.2 | 26.3 |
| dn18 @0.1 % unan (maker) | 11 filled (12 posted) | −1,069 | −97.2 | 27.3 |

## Model-selection epochs (4 folds × 3 seeds)

| fold | seed 0 | seed 1 | seed 2 |
|---|---|---|---|
| 0 | ep 1 | ep 1 | ep 8 |
| 1 | ep 7 | ep 1 | ep 1 |
| 2 | ep 1 | ep 1 | ep 1 |
| 3 | ep 1 | ep 1 | ep 1 |

10/12 select epoch 1. Fold-0 seed-0 val `IC_h18` by epoch: +0.1124, +0.0590, +0.0285, +0.0407, +0.0286, +0.0211 (early stop at ep 6) while train loss falls 1.33 → 0.39.

## How to read this run (notebook's own footer, with corrections)

1. ~~Cell 10 must ≈ reproduce run.007~~ — **void on this window.** The window changed (68.8 d vs run.007's ~170 d and run.011's 32.7 d); base rates are ~2× the anchor. Compare only within run.009a.
2. Cell 10M headline: read sim, fill% and EV/trig together. ✅ (sim > 0 with tiny fill is not tradeable; EV/trig is the honest per-signal number.)
3. hit_m vs hit_f is the adverse-selection verdict. ✅ confirmed: 39 %/46 % vs 12 % filled.
4. ~~Cell 11 GATE is h6~~ — the gate is **h18** (90 s). The 30 s-hybrid and wait×δ grid remain hypotheses, not results. ✅
5. If the gate passes: freqtrade cross-check before believing P&L. It failed — read the failure mode: here it is adverse selection **and** hit-rate-bound together.
6. ~~Schema-3/4 rerun needs ≥45 days of out3/out4 (~late Aug)~~ — **delivered**: 68.8 days of `w5`, 76/76 features. The remaining lever is event/sub-second order flow (run.012) or execution mechanics, not more bar data.

(End of file)
