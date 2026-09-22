# run.009d analysis — the feature prune moved the signal a little and the economics not at all; the real finding is that even at zero fees the edge is ≈+5 bp/trade against a 10 bp taker floor, and the only route cheap enough (full-maker, 4 bp) costs ~5 bp in adverse selection

*Analyzed 2026-09-21. Notebook: `runs/btc_lstm.run.009d.ipynb` (feature prune from the run.009b importance study, 76→58 features, on run.009a's 68.8-day `w5` window). Executed on Colab T4; all 16 cells ran clean, no errors, scores + model + plot persisted to Drive.*

> **UPDATE 2026-09-21 — superseded in part by `run009d.offline.md`.** The offline diagnostics (rebuilt from `data/60days_data.tar` + `data/run009d_scores.npz`, harness validated by exact reproduction of this run's published numbers) qualify two conclusions below:
>
> 1. **The economics conclusion is stronger than stated here.** This doc reads the pooled numbers as "a small real fee-free edge that fees erase". The pooled up-side gross edge is **entirely fold 3** (2026-09-10 → 09-20): up18 @0.1 % gross +6.23 bp pooled → **+0.03 bp with fold 3 removed**; up15 @0.01 % +8.48 [+1.40, +11.04] → **+1.31 [−4.94, +4.84]**. Per-fold gross signs are **+, −, −, +**, and the up/dn heads **mirror each other** in folds 2–3 (fold 3: up +10.88 / dn −13.49). No cell is net-positive with taker fees in any fold. The correct statement is *"outside fold 3 there is no measurable fee-free edge, and inside fold 3 the two arms are two sides of one directional bet"*, not *"a small edge that fees erase"*. **See `run009d.offline.md` Part 2**: an earlier draft of this note called fold 3 "trend exposure" — that is now **refuted** (fold 3's own net drift was +0.04 bp, and the market-neutral spread `S` is ≤ 0 in fold 3 and includes zero in all twelve pooled horizon×rate cells). The mechanism is **volatility selection with a weak directional tilt**: every head raises its *opposite* first-touch label by 6.8–39.8× base rate, trigger |move| is 3.3–5.3× the sample average, and the dn heads' own/opposite lift ratio is ≈ 1.0 (no direction at all).
> 2. **The val/test AP gap and the leakage audit are resolved.** Val base rates differ from test by up to **3.9×**, and the notebook prints **per-seed** val AP against an **ensemble** test AP — together this fully explains `0.073 vs 0.207` with no bug, no leak and no metric miscalibration. Leakage audit is clean (forward `.shift(-h)` only in targets/validity; all feature rollers backward; scaler fit on the train slice; evaluation runs on unsampled ends).
> 3. **Action-plan items 2–4 do not run offline from the npz alone** (the closing note at the foot of this doc is wrong — `close` is saved only at sample ends, with no forward path, no raw per-horizon `test_pred` and no val labels). The **tar plus npz** together reproduce everything, as `run009d.offline.md` demonstrates.
>
> Still valid here: the gate failure and its mechanism, the epoch-1 selection pathology (11/12 seed-folds), the "do not report +23 % at h6" argument, and the conclusion that the prune is not load-bearing.

**TLDR:** run.009d is the single-variable test of the prune hypothesis: drop the 13 "significantly harmful" and 5 constant-zero features that run.009b's permutation sweep flagged, everything else byte-identical to run.009a. The gate **FAILED on both sides** — A fail, C **0/4 folds**, D fail, only B (ranking lift) passes — for the fourth consecutive run. The pooled IC did rise at short horizons (h6 +0.0886 → +0.1093, +23 %), but that number is **not reportable**: the baseline is unpaired (no same-run `PRUNE_MODE='none'` control was executed), the effect is smaller than the dominant variance source (checkpoint selection — 11/12 seed-folds select **epoch 1**, and fold 0's per-seed IC spread is **±0.0808** vs ±0.0017–0.0045 elsewhere), and the per-fold sign of the change **flips** (fold 1 h24 −28 %, fold 2 h24 +42 %). The honest read is the notes' own fallback: the harmful features were not load-bearing. The economic verdict is unchanged and now precisely bounded: across all 12 horizon×side cells the **gross** (fee-free) edge per trigger caps at **+4.4 bp (up18) / +5.0 bp (up24)**, the down-side heads are **gross-negative at every horizon**, and the maker route's **4.4–6.5 bp selection cost cancels its 3–6 bp fee saving almost exactly** — which is why twelve runs of execution tweaks have moved nothing. The open lever is no longer features, data, or execution: it is **training/selection** (every number in this run comes from an effectively one-epoch model).

---

## Validity checks

- **Window / data:** 412 `w5` files, 1,186,560 rows, schema **v5 100 %**, era start 2026-07-13 20:58:40 → end 2026-09-20 15:50:45 = **68.8 days**. 1,162,022 rows after `dropna` (**2.07 %** dropped, vs run.009a's 2.11 %). 1,101,861 valid samples (94.8 %). 127 cadence breaks, largest gap 1.7 h — all absorbed by the contiguity/gap masks.
- **Same tar as run.009a** — the sanity anchor holds exactly (`lup_18` 0.93 % / `ldn_18` 0.81 % on valid samples), so the cross-run comparison is legitimate on data grounds.
- **Folds:** 10.1 / 10.1 / 10.1 / 10.5-day test blocks; 42 test days; the notebook's `<7 days → indicative only` warning fires on none of them.
- **Execution integrity:** all 16 code cells executed in order, zero error outputs; `run009d_scores.npz`, `lstm_run009d_h18.pt`, `run009d_selective.png` persisted.

### Caveat — the comparison is not exactly paired

Pruning changes the `dropna` subset, so the evaluation arrays are **not identical** to run.009a's: 1,162,022 rows (2.07 % dropped) vs 1,161,525 (2.11 %), valid samples 1,101,861 vs 1,101,579, fold-3 `n_test` 164,530 vs 164,671. Small (~0.1 %), but the fold boundaries shift by tens of bars and the specific test samples differ, so every cross-run delta carries an unpaired sample-composition term on top of the seed/execution term.

---

## The prune — head-to-head vs run.009a (same window)

| metric | run.009a | run.009d | Δ |
|---|---|---|---|
| pooled IC h6 | +0.0886 | +0.1093 | **+23 %** |
| h9 / h12 / h15 | +0.0705 / +0.0615 / +0.0557 | +0.0822 / +0.0669 / +0.0574 | +17 % / +9 % / +3 % |
| h18 / h24 | +0.0494 / +0.0419 | +0.0491 / +0.0394 | −0.6 % / **−6 %** |
| daily-IC t h6 / h18 | +11.63 / +9.18 | +13.53 / +8.55 | +1.90 / −0.63 |
| pooled AP up18 / dn18 | 0.0827 / 0.0890 | 0.0963 / 0.1070 | +0.014 / +0.018 |
| taker up18 / dn18 sim @0.1 % | −10.39 / −10.40 | −5.62 / −10.45 | +4.77 / −0.05 |
| maker up18 / dn18 sim @0.1 % | −10.74 / −10.99 | −6.11 / −10.57 | +4.63 / +0.42 |
| maker fill up / dn | 75.8 % / 77.4 % | 73.6 % / 78.7 % | −2.2 / +1.3 |
| maker hit_f up / dn | 12.1 % / 12.5 % | 22.5 % / 12.1 % | +10.4 / −0.4 |
| maker hit_m up / dn | 39.0 % / 46.1 % | 55.2 % / 42.4 % | +16.2 / −3.7 |
| **GATE A / B / C / D** | F / P / F / F | F / P / F / F | **unchanged** |

### Do not report "+23 % at h6" — three independent reasons it is unmeasurable

1. **Unpaired baseline.** The control the notebook itself prescribes — `PRUNE_MODE='none'`, "reproduces run.009a exactly; use it as the control" — was **never executed**. The baseline is run.009a's *published* numbers from a separate execution: different seeds, different data order, different `dropna` set.
2. **The effect is smaller than the dominant variance source.** Fold 0's per-seed IC_h6 spread is **±0.0808**, versus ±0.0017–0.0045 in folds 1–3. That is not init noise — it is **epoch-selection divergence**: in folds 1–3 all three seeds select epoch 1 and converge to near-identical IC, while fold 0 contains the one epoch-8 seed. A 3-seed mean in fold 0 therefore has SE ≈ 0.047, putting pooled h6 at roughly **0.109 ± 0.02**. The prune delta (+0.021) is **under one standard error of a single uncontrolled variance source**. Fold 0's epoch-8 seed proves checkpoint choice dominates the prune by an order of magnitude.
3. **The sign flips across folds.** Fold 1: h6 +64 % but h24 **−28 %**. Fold 2: h6 +5 % but h24 **+42 %**. Fold 3: all horizons +4–10 %. Fold 0: short up, long down. A genuine feature-set improvement has a horizon profile that holds sign across folds; this one inverts. Over 6 horizons × 2 heads × several metrics, "h6 up, h24 down" is also exactly what multiple-comparison noise produces.

### The method was the one the source study told them not to use

`run009b.importance.notes.md` states, in bold interpretation rules:

> *"the below-floor list is **descriptive only — do not prune from it**."*
> *"**To actually prune, use block ablation** (drop the block, retrain, compare) — not the per-feature permutation ranking."*

run.009d pruned exactly that list — 13 features at |t| > 2.37, measured on **fold 3 only**, 8 repeats, 20 k subsample — and left `DROP_BLOCKS=[]`. Two aggravating factors:

- The "harmful" list is a **negative** permutation drop, which the notes correctly call the stronger evidence class (a negative drop is not a redundancy artifact), but it is still fold-specific and unvalidated out-of-fold.
- Importance was measured on a checkpoint that is itself epoch-1-selected (run.009b loads the run.009a model). **Permutation importance of a barely-trained network need not survive training** — so the prune's rationale is weak independent of its measured effect.

**Verdict on the prune: no measured effect, methodologically disfavoured, cheap to settle with one paired run.** The honest conclusion is the notes' own fallback — the harmful features were not load-bearing.

---

## The signal — strong and stable, for this window

Pooled seed-ensemble regression IC (monotone decay, same shape as every prior run):

| h6 | h9 | h12 | h15 | h18 | h24 |
|---|---|---|---|---|---|
| +0.1093 | +0.0822 | +0.0669 | +0.0574 | +0.0491 | +0.0394 |

Daily-IC t-stats, 42 days: **h6 +13.53**, h9 +11.86, h12 +11.34, h15 +10.05, h18 +8.55, h24 +6.79. Positive IC in **4/4 folds at every horizon**. Fold 3 carries the strongest signal (AP 0.18–0.24, two to three times folds 0–2).

Ranking is not the problem, and has not been for four runs. Two caveats that bound the claim:

- **One window, one asset, one regime.** t = 13.5 means "the signal existed between 2026-07-13 and 2026-09-20," nothing more. All 42 test days sit inside a single 69-day period.
- **Fold-3 concentration is unexamined.** Per-fold IC rises monotonically fold 0 → fold 3 (0.0865 → 0.1437 at h6) and fold 3 has the least-negative maker sim (−3.73 up). The pooled edge may be substantially one high-volatility regime. Cheap to check by decomposing pooled lift per fold.

---

## The gate — FAILED, both sides

Pre-registered: h18 heads (90 s), 0.1 % rolling-ensemble, maker entry (wait 6, δ = 0, delay 0), TP = SL = 1.0·θ.

| criterion | up18 | dn18 |
|---|---|---|
| **A** — maker sim > 0, day-boot CI > 0 | **fail** (−6.11, CI [−9.59, −3.95]) | **fail** (−10.57, CI [−13.16, −7.19]) |
| **B** — lift ≥ 3× pooled, ≥ 2× in ≥ 3/4 folds | pass (25.63×, **4/4**) | pass (17.04×, **4/4**) |
| **C** — fill ≥ 25 % + per-fold sim > 0 in ≥ 3/4 | **fail** (fill 73.6 % ok, **0/4** folds) | **fail** (fill 78.7 % ok, **0/4**) |
| **D** — neighbour rates sim > 0 | **fail** | **fail** |

**C is the damning criterion: `0/4` folds have a positive maker sim on either side.** Not a pooled-mean artifact hiding a fold-2-style collapse — there is no good fold to hide in. Criterion B passing while every economic criterion fails also shows B is toothless: lift is a statistical-quality metric, not an economic one.

Per-fold maker sim at the gate:

| fold | up18 n / hit_f / sim | dn18 n / hit_f / sim |
|---|---|---|
| 0 | 193 / 8.7 % / −6.05 | 222 / 5.5 % / −13.82 |
| 1 | 184 / 3.1 % / −10.42 | 997 / 11.6 % / −9.36 |
| 2 | 79 / 22.2 % / −11.18 | 60 / 29.8 % / −6.82 |
| 3 | 607 / 34.5 % / −3.73 | 139 / 18.6 % / −15.17 |

---

## Economics — the wall, now bounded in gross terms

Fees: taker round trip **10 bp**, taker-in/maker-TP-out **7 bp**, full-maker **4 bp**. θ at h18 = 20 bp.

Gross (fee-free) edge per executed trade is the right lens, because it separates "signal too weak in bp" from "fees too high." All twelve horizon × side cells, **taker** route at 0.1 % ens (gross = `sim + 10`):

| head | sim bps | **gross bps** | head | sim bps | **gross bps** |
|---|---|---|---|---|---|
| up6 | −8.50 | **+1.50** | dn6 | −11.60 | **−1.60** |
| up9 | −7.50 | **+2.50** | dn9 | −11.15 | **−1.15** |
| up12 | −6.90 | **+3.10** | dn12 | −12.29 | **−2.29** |
| up15 | −5.99 | **+4.01** | dn15 | −11.66 | **−1.66** |
| up18 | −5.62 | **+4.38** | dn18 | −10.45 | **−0.45** |
| up24 | −5.04 | **+4.96** | dn24 | −10.90 | **−0.90** |

Two hard conclusions:

1. **The down-side heads have no gross edge at any horizon** (≈ 0 to −2 bp). This is not a fee problem; the prediction itself does not pay.
2. **The up-side gross edge caps at ≈ +5 bp** (up24 +4.96), rising monotonically with horizon, against a **10 bp taker floor**. Even fee-free, only the up side is positive, and only marginally.

### The adverse-selection cancellation — why execution tweaks do nothing

| route | gross per executed trade | fee floor | room |
|---|---|---|---|
| taker up18 @0.1 % | −5.62 + 10 = **+4.38 bp** | 10 bp | **−5.6 bp** |
| taker dn18 @0.1 % | −10.45 + 10 = **−0.45 bp** | 10 bp | negative gross |
| maker **filled** up18 | −6.11 + 4…7 = **−2.1…+0.9 bp** | 4 bp | **≈ 0, negative** |

The maker route is the only arithmetic path to viability (4 bp vs 10 bp). But it keeps only the 73.6 % of triggers that traded through the limit, and that subset's gross edge collapses from **+4.38 bp (all triggers)** to **≈ 0 (filled)**. So:

- **selection cost ≈ 4.4–6.5 bp**
- **fee saving ≈ 3–6 bp**

They cancel almost exactly. That is the whole story of runs 007 / 008 / 009a / 009d in one line — and it explains why twelve runs of entry-price, latency, exit-grid and router tweaks have moved nothing.

### Adverse selection is unambiguous and structural

| side @0.1 % | hit_f (filled) | hit_m (missed) | missed gross | fill % |
|---|---|---|---|---|
| up18 | **22.5 %** | **55.2 %** | +18.1 bp | 73.6 % |
| dn18 | **12.1 %** | **42.4 %** | +11.4 bp | 78.7 % |

Post-only at the touch fills exactly when early price action contradicts the signal; the true positives leave without you. This is not a fill-model artifact — any fill-on-trade-through model inherits it. For completeness on the fill model's bias direction: queue-position-0 on trade-through is **optimistic** on the worst fills (realistic queue removes them → favours the strategy), while close-only trade-through detection is **pessimistic** on touch-fills (adds less-adversely-selected fills → also favours the strategy). Net direction: mildly pessimistic on per-fill EV. P(a realistic queue-aware model flips maker h18 positive) ≈ **0.10–0.15** — it would need ~+4.5 bp of repair, more than plausible modelling error, and dn18 (−6.6 bp gross/fill) is further still.

### The pre-registered rescue routes failed again

- **Latency is not the cause:** taker+3 bar (15 s) → up18 −5.62 → −6.31, dn18 −10.45 → −10.52.
- **The 30 s-timing hybrid routed 100 % / 92.9 % taker** — it degenerated to plain taker and tested nothing. Third time this rescue has collapsed (run.008, run.009a, run.009d).
- **wait × δ grid all negative**; deeper δ strictly worse (up18: fill 73.6 % → 8.8 % as δ goes 0 → 0.5θ, hit_f 22.5 % → 12.8 %).
- **TP/SL grid all negative.** Best up18 variant is (1.5, 1.0) at −4.35 bp, still < 0.
- **Bracket costs ~1.9 bp vs hold-to-horizon:** up18 endpoint −3.77 (gross +6.23) vs sim −5.62 (gross +4.38). The symmetric TP = SL = 1·θ structure destroys value relative to simply holding to the horizon. Untested alternatives: wider SL, trailing, horizon exit.
- **Non-overlapping 1-BTC trades:** up18 −8,788 $ (152 trades, mean −57.8 $, win 32.2 %); dn18 −19,205 $ (215, −89.3 $, 29.8 %). Maker: −7,198 $ / −15,343 $.

### The one positive cell is noise

`unan up18 @0.01 %` maker: **+1.20 bp**, CI [+0.21, +16.00], **n_fill = 16**. On 42 days, sixteen trades with a 16 bp-wide CI is not evidence. In run.009a the analogous cell was n = 7. It is the best of the neighbouring rates chosen post hoc — do not chase it.

---

## The bigger problem — 11/12 seed-folds select epoch 1

| fold | seed 0 | seed 1 | seed 2 |
|---|---|---|---|
| 0 | **ep 8** | ep 1 | ep 1 |
| 1 | ep 1 | ep 1 | ep 1 |
| 2 | ep 1 | ep 1 | ep 1 |
| 3 | ep 1 | ep 1 | ep 1 |

**11 of 12** select epoch 1 (run.009a: 10/12 — slightly *worse*, not better). The selected model has seen ~40 days of training (≈ 700 k samples) once and nothing more. Train loss falls 1.42 → 0.53 while val AP decays from its epoch-1 peak (fold 3 seed 0: val AP_up18 = 0.0729 at ep 1, then 0.0563, 0.0612, 0.0636, 0.0657, 0.0645 → early stop). Fold 3 test AP_up18 at that checkpoint = **0.2071**.

- **Validity: not compromised.** Early stop reads val only; test is strictly after train+val; the protocol is identical across runs. Epoch-1 adds no inflation mechanism. Note the direction of the val ≪ test gap is *anti*-leakage: a leaking pipeline inflates the selection set, and here val is the *low* number. The most likely explanation (~0.70) is **prevalence**, not skill — AP is monotone in base rate at fixed ranking quality, and if the val base rate is ≈ 0.3–0.4 %, then val lift ≈ 18–24 × ≈ test lift 17 × → same model, different regime. Corroboration: realized trigger rate 25.3/day vs 17.3/day nominal (test is richer than the calibration period), fold-3 APs 2–3 × folds 0–2, per-fold IC rising monotonically. Checkable in one line: val base rate per fold per head. P(val slicing / label bug): ~0.15.
- **Interpretation: fully compromised.** You have never evaluated the architecture — a 230 k-parameter LSTM selected after one pass is operating as a shallow map of recent features. And test performance *beyond* the selected checkpoint is **unmeasured**: val's miscalibrated raw AP cannot see whether epochs 5–20 would improve test IC. P(test IC_h6 improves ≥ 20 % under fixed-epoch training + a prevalence-adjusted selection metric) ≈ **0.40**. Fold 0's epoch-8 seed proves checkpoint choice is a first-order variance source.
- **Prioritisation miss.** run.009a's own analysis named epoch-1 selection "a concrete, cheap lever … that no prior run has tested." run.009d ships `TRAIN_ARM='regularized'` and `EARLY_STOP='ic'` to test exactly that — and leaves **both OFF by default** to "stay single-variable." It re-tested the prune (the method the source study recommended against) and skipped the selection pathology (the method it recommended).
- **Also now closed:** run.012 (executed 2026-08-30) already tested the event-order-flow lever the cross-run doc named as the remaining input class — FAVOURABLE OOF AUC **0.492 / 0.510** → chance, gate FAILED. So "wait for event features" is not an open door either.

---

## What run.009d settles

- **Feature-prune lever: exhausted.** No measured effect; the flagged features were not load-bearing. Do not report the +23 % h6 figure.
- **Economics: unchanged and now precisely bounded.** Max gross edge ≈ +5 bp (up side, h24) against a 10 bp taker floor; down-side gross-negative at every horizon; maker's fee saving cancelled by selection cost. "Signal exists but fees kill it" is *too generous* — the accurate statement is: **the up-side gross edge sits below the cheapest achievable round-trip once selection is priced, and the down-side has no gross edge at all.**
- **Data / execution / input-class levers: closed.** More bars (68.8 d already), event order flow (0.49–0.51 AUC), latency (robust), entry-price arithmetic (selection cancels it), exit grid (all ≤ 0, bracket costs ~1.9 bp). None of these is the binding constraint.
- **Open lever: training and model selection.** Every number in this run comes from a one-epoch model chosen by a miscalibrated stop metric.

---

## Next experiment

**Highest value: a zero-fee gross-EV surface on the existing predictions** — {6 horizons} × {θ grid} × {trigger rates} × {exit: bracket / SL = 2θ / hold-to-horizon}, per fold, day-bootstrap CI. No retraining, hours of compute. It adjudicates the project's central question ("edge too small in bp" vs "fees/execution kill it") and exposes the **h6/h9 economics that have never been reported** — the strongest IC sits at h6/h9 while all economic analysis is run at h18.

Decision rule:

- No cell with gross ≥ 4 bp **and** per-fold ≥ 0 in ≥ 3/4 folds → the design space is dead at the current signal strength; the remaining lever is training, not execution.
- A cell with gross ≥ 8 bp and usable n → commission an L2-replay queue-aware fill study.

Falsifiers:

- **"Signal exists"** would be falsified by (a) an out-of-window inference test on post-2026-09-20 data, no retrain — IC halves or dies ⇒ window/regime artifact (P(survives at ≥ half IC) ≈ 0.6); (b) a leak audit finding anything.
- **"Fees kill it"** would be falsified by a gross-EV cell clearing the 4 bp maker floor with per-fold support, or by a queue-aware fill model flipping the filled-subset gross positive (P ≈ 0.10–0.15).

### Action plan (≈ 1–2 days)

1. **Val base rates per fold/head** — resolves the val/test AP gap (10 min).
2. **Metric-basis audit** — assert that val/test metrics *and* sims run on **unsampled** data (importance sampling on |y| must not leak into evaluation, or every AP/IC is inflated); audit feature-normalization stats (train-only?), the w5 window vs label boundary, and the trigger-threshold source (val, not test).
3. **Zero-fee gross-EV surface** (above), all horizons × θ × rate × exit, per fold.
4. **Out-of-window inference test**, post-2026-09-20, no retrain.
5. **One-fold retrain**: fixed 10–15 epochs, select on val **lift or AUC** (not raw AP); compare test IC against the epoch-1 checkpoint.
6. **Per-fold contribution decomposition** — is the edge one regime?
7. **Paired `PRUNE_MODE='none'` run** — only if the prune still matters after steps 3–6.

Steps 1–3 alone decide whether this project has a future or needs a pivot.

---

## Cosmetic issues (do not affect results, do affect re-reads)

- The closing read-guide compares against run.009a's **published** numbers as though the comparison were paired; it is cross-execution and unpaired.
- The failure-guide's branch list has no entry for "adverse selection **and** hit-rate-bound **and** healthy fill rate" — which is exactly this run's (and run.009a's) situation, so the printed guidance under-diagnoses it.
- `H2_IDX = HORIZONS.index(6)` labels the 30 s head "h2" in comments and prints (it is `h6` at 5 s). Harmless, confusing.
- Gate criterion B (lift) is a statistical-quality test that passes while every economic criterion fails; worth demoting to informational in the next gate revision.

---

## Appendix — reference tables

### Setup

| Horizon | Wall-clock | θ | Test base rate (up/dn) |
|---|---|---|---|
| h6 | 30 s | 15 bp | 0.47 % / 0.45 % |
| h9 | 45 s | 15 bp | 0.93 % / 0.86 % |
| h12 | 60 s | 20 bp | 0.63 % / 0.58 % |
| h15 | 75 s | 20 bp | 0.91 % / 0.83 % |
| h18 | 90 s | 20 bp | 1.21 % / 1.09 % |
| h24 | 2 min | 20 bp | 1.89 % / 1.69 % |

Fees: taker RT 10 bp, TP RT (taker-in/maker-out) 7 bp, full-maker RT 4 bp. `SEQ_LEN` 192 (16 min), `HIDDEN_DIM` 128, 3 seeds per fold, 4 expanding folds × 10.1–10.5 d test. Model 230,790 params (run.009a: 240,042 — the prune changed `F_DIM`).

### Data / window

| item | value |
|---|---|
| files | 412 (`w5` only) |
| rows loaded | 1,186,560 |
| rows after dropna | 1,162,022 (2.07 % dropped) |
| valid samples | 1,101,861 (94.8 %) |
| window | 2026-07-14 20:58:30 → 2026-09-20 15:50:45 |
| era start | 2026-07-13 20:58:40 (68.8 d), v5 100 % |
| features | 58/76 kept (v1_base 29, new_ctx 8, v3 21) |
| dropped | `minute_sin`, `minute_cos`, `vol_norm`, `largest_trade_rel`, `buy_accel`, `ma_gap_1h`, `ma_gap_4h`, `basis_z_4h`, `dow_sin`, `flow_net_widex_z`, `sell_tail_ratio`, `wall_imbal`, `wall_qty_norm`, `liq_flag`, `liq_cnt_log`, `liq_imbal`, `liq_notional_log`, `liq_notional_max_log` |
| cadence breaks | 127 (largest gap 1.7 h) |

### Per-fold IC (seed-ensemble; ± = per-seed spread)

| Fold | n_test | h6 | h9 | h12 | h15 | h18 | h24 |
|---|---|---|---|---|---|---|---|
| 0 | 165,059 | +0.0865 ±0.0808 | +0.0712 ±0.0667 | +0.0592 ±0.0542 | +0.0556 ±0.0448 | +0.0521 ±0.0380 | +0.0457 ±0.0279 |
| 1 | 166,228 | +0.1001 ±0.0017 | +0.0726 ±0.0029 | +0.0594 ±0.0032 | +0.0502 ±0.0041 | +0.0438 ±0.0064 | +0.0353 ±0.0045 |
| 2 | 170,211 | +0.1112 ±0.0024 | +0.0812 ±0.0016 | +0.0676 ±0.0064 | +0.0556 ±0.0030 | +0.0436 ±0.0061 | +0.0315 ±0.0046 |
| 3 | 164,530 | +0.1437 ±0.0045 | +0.1099 ±0.0036 | +0.0864 ±0.0058 | +0.0716 ±0.0038 | +0.0611 ±0.0051 | +0.0489 ±0.0069 |
| **pooled** | 665,988 | **+0.1093** | +0.0822 | +0.0669 | +0.0574 | +0.0491 | +0.0394 |

### Opportunity-head pooled AP vs base rate

| head | pooled AP | base % | head | pooled AP | base % |
|---|---|---|---|---|---|
| lup_6 | 0.0684 | 0.47 | ldn_6 | 0.0740 | 0.45 |
| lup_9 | 0.0889 | 0.93 | ldn_9 | 0.0952 | 0.86 |
| lup_12 | 0.0741 | 0.63 | ldn_12 | 0.0864 | 0.58 |
| lup_15 | 0.0854 | 0.91 | ldn_15 | 0.0973 | 0.83 |
| lup_18 | 0.0963 | 1.21 | ldn_18 | 0.1070 | 1.09 |
| lup_24 | 0.1082 | 1.89 | ldn_24 | 0.1253 | 1.69 |

### h18 selective table @ 0.1 % ens (taker, cell 10)

| head | n | /day | hit % | base % | lift | sim bps [95 % CI] | endpt bps |
|---|---|---|---|---|---|---|---|
| up18 | 1,063 | 25.31 | 31.1 | 1.21 | 25.63 | −5.62 [−10.25, −3.56] | −3.77 |
| dn18 | 1,418 | 33.76 | 18.5 | 1.09 | 17.04 | −10.45 [−13.01, −6.68] | −10.62 |

### Maker entry, primary config (wait 6, δ = 0, delay 0) — h18

| head | n_trig | fill % | n_fill | hit_f % | lift_f | sim bps [CI] | EV/trig | missed | hit_m % |
|---|---|---|---|---|---|---|---|---|---|
| up18 | 1,063 | 73.6 | 782 | 22.5 | 18.52 | −6.11 [−9.59, −3.95] | −4.50 | +18.1 | 55.2 |
| dn18 | 1,418 | 78.7 | 1,116 | 12.1 | 11.12 | −10.57 [−13.16, −7.19] | −8.32 | +11.4 | 42.4 |

### Frozen-val rule comparison (not gated)

| head | rate | n | /day | hit % | lift | sim bps [CI] | endpt |
|---|---|---|---|---|---|---|---|
| up18 | 0.1 % | 1,316 | 31.33 | 30.2 | 24.83 | −6.15 [−9.99, −4.38] | −4.29 |
| dn18 | 0.1 % | 3,863 | 91.98 | 15.4 | 14.11 | −10.73 [−12.75, −8.99] | −10.84 |

### Regression-threshold baseline (`reg` variant, same rolling-τ)

| head | rate | n | hit % | lift | sim bps |
|---|---|---|---|---|---|
| up18 | 0.1 % | 768 | 0.4 | 0.32 | −9.52 |
| dn18 | 0.1 % | 872 | 0.0 | 0.00 | −9.75 |

Thresholding the run.004-style regression score at the same rate loses ≈ the same −9 to −10 bp at hit ≈ base rate: the heads buy **lift** but **zero net**. The ~−10 bp is structural (fees + adverse-move distribution), not selection-bound.

### Exploratory — all horizons @ 0.1 % ens

| head | n | hit % | lift | sim bps [CI] | gross bps |
|---|---|---|---|---|---|
| up6 | 1,171 | 16.9 | 35.89 | −8.50 [−9.73, −7.81] | +1.50 |
| dn6 | 1,104 | 14.8 | 33.12 | −11.60 [−12.75, −10.40] | −1.60 |
| up9 | 1,164 | 26.6 | 28.79 | −7.50 [−9.71, −6.50] | +2.50 |
| dn9 | 1,238 | 17.6 | 20.50 | −11.15 [−12.12, −10.02] | −1.15 |
| up12 | 1,210 | 23.0 | 36.70 | −6.90 [−10.47, −5.37] | +3.10 |
| dn12 | 1,207 | 15.2 | 26.15 | −12.29 [−13.61, −10.79] | −2.29 |
| up15 | 1,099 | 27.8 | 30.66 | −5.99 [−9.80, −4.18] | +4.01 |
| dn15 | 1,228 | 18.2 | 21.93 | −11.66 [−13.07, −9.48] | −1.66 |
| up18 | 1,063 | 31.1 | 25.63 | −5.62 [−10.25, −3.56] | +4.38 |
| dn18 | 1,418 | 18.5 | 17.04 | −10.45 [−13.01, −6.68] | −0.45 |
| up24 | 945 | 34.9 | 18.52 | −5.04 [−10.43, −2.39] | +4.96 |
| dn24 | 1,748 | 19.6 | 11.64 | −10.90 [−13.65, −7.92] | −0.90 |

### Entry-delay stress @ 0.1 % ens

| head | variant | n | sim bps [CI] |
|---|---|---|---|
| up18 | taker+0 | 1,063 | −5.62 [−10.25, −3.56] |
| up18 | taker+3 bar (15 s) | 1,063 | −6.31 [−10.31, −4.49] |
| up18 | maker delay = 3 (15 s) | 666 | −9.55 [−11.72, −7.59] (fill 62.7 %) |
| dn18 | taker+0 | 1,418 | −10.45 [−13.01, −6.68] |
| dn18 | taker+3 bar (15 s) | 1,418 | −10.52 [−12.82, −7.23] |
| dn18 | maker delay = 3 (15 s) | 1,010 | −13.47 [−16.85, −10.35] (fill 71.2 %) |

### Non-overlapping 1-BTC trades (tp = sl = 1.0·θ, time-stop h)

| config | trades | total $ | mean $ | win % |
|---|---|---|---|---|
| up18 @0.1 % ens (taker) | 152 | −8,788 | −57.8 | 32.2 |
| dn18 @0.1 % ens (taker) | 215 | −19,205 | −89.3 | 29.8 |
| up18 @0.1 % unan (taker) | 37 | −476 | −12.9 | 54.1 |
| dn18 @0.1 % unan (taker) | 75 | −6,868 | −91.6 | 32.0 |
| up18 @0.1 % ens (maker) | 123 filled (164 posted) | −7,198 | −58.5 | 27.6 |
| dn18 @0.1 % ens (maker) | 181 filled (224 posted) | −15,343 | −84.8 | 26.0 |
| up18 @0.1 % unan (maker) | 32 filled (39 posted) | −470 | −14.7 | 53.1 |
| dn18 @0.1 % unan (maker) | 60 filled (78 posted) | −5,681 | −94.7 | 26.7 |

### Model-selection epochs (4 folds × 3 seeds)

| fold | seed 0 | seed 1 | seed 2 |
|---|---|---|---|
| 0 | ep 8 | ep 1 | ep 1 |
| 1 | ep 1 | ep 1 | ep 1 |
| 2 | ep 1 | ep 1 | ep 1 |
| 3 | ep 1 | ep 1 | ep 1 |

**11/12 select epoch 1.** Fold 3 seed 0 val `AP_up18` by epoch: 0.0729, 0.0563, 0.0612, 0.0636, 0.0657, 0.0645 (early stop at ep 6) while train loss falls 1.42 → 0.58; test AP_up18 at the epoch-1 checkpoint = 0.2071.

### Artifacts

| artifact | path |
|---|---|
| scores npz | `/root/btc_lstm_run009d/run009d_scores.npz` → Drive |
| model | `/root/btc_lstm_run009d/lstm_run009d_h18.pt` → Drive |
| plot | `/root/btc_lstm_run009d/run009d_selective.png` → Drive |

~~`run009d_scores.npz` carries per-fold val + test scores (`ends`, `score`, `prob`, `seed_prob`, `labels`, `y`, `horizons`, `cls_cols`, `dt_s`, `close`, `fold_of`, plus val equivalents), so all of the diagnostics above — and steps 1–4 of the action plan — run offline without retraining.~~

**Corrected:** the npz is **not sufficient on its own**. `close` is stored only at the sample ends (`close=close[ends_pool]`) — there is no forward price path, hence no offline TP/SL or net-bps; there are no raw per-horizon `test_pred` (only the per-fold standardised pooled `score`); and there are no val labels, hence no offline val base rates. Hit rates, lifts, AP/IC, rolling-τ triggers and per-fold decomposition are recoverable; **economics are not**. The **tar plus the npz** together reproduce everything — demonstrated in `run009d.offline.md`, whose harness reproduces this run's published h18 numbers and per-seed val APs exactly.
