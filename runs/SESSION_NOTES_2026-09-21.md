# Session notes — 2026-09-21

Standalone state doc for the `btc_lstm` run lineage. Companion detail lives in
`run009d.analysis.md`, `run009d.offline.md`, `run009e.analysis.md` and
`run009f.notes.md`; this file is the navigable summary someone can pick up cold.

---

## Where things stand

**`run.009f` has been executed and its gate FAILED on all six criteria, both sides.**
The pre-registered question is answered: the test market-neutral spread is
**significantly negative** (`S = −3.79 bp, CI [−7.22, −0.42]`), so the estimator was
**not** the binding constraint. Full write-up: `run009f.analysis.md`.
It is the fourth attempt to convert a real ranking signal into an economic edge,
and the first to fix the estimator rather than the feature set, the exits, or the
entry arithmetic.

The chain is now long enough to state the central finding plainly:

> **The heads locate high-magnitude bars, not direction.** Every head raises its
> *opposite* first-touch label by 6.8–39.8× base rate, trigger |move| is 3.3–5.3×
> the sample average, and `P(either barrier touched first)` goes from 2.30 % on
> all bars to 43.74 % on up18 triggers. The celebrated 25× "hit-rate lift" is
> mostly that. The drift-cancelling spread between the two heads is what matters,
> and it is not positive.

## Run lineage and verdicts

| run | change | pooled IC h6 | test `S` @h18 (point) | gate |
|---|---|---|---|---|
| 009 | triple-barrier first-touch labels | — | — | fail |
| 009a | w5 cadence, 68.8 d | +0.0886 | — | F/P/F/F |
| 009b | permutation-importance study (no training) | — | — | n/a |
| 009d | prune 76→58 features | +0.1093 | +5.60 | F/P/F/F |
| 009e | selection objective: AP → directional edge | **+0.0710** | **−2.43** | A–F all fail |
| **009f** | selection estimator: → continuous val spread | **+0.0602** | **−3.79** (sig.) | **A–F all fail** |

Every run fails on economics. The signal (IC, lift) has never been the problem;
the market-neutral spread and the fee floor are.

---

## The decisive offline findings (`run009d.offline.md`)

Rebuilt from `60days_data.tar` + `run009d_scores.npz`; harness validated by
**exact** reproduction of run.009d's published table and per-seed val APs.

- **Fold 3 carries everything.** up18 @0.1 % gross +6.23 bp pooled → **+0.03 bp
  with fold 3 removed**. up15 @0.01 % +8.48 → **+1.31 [−4.94, +4.84]**. Per-fold
  gross signs are **+, −, −, +**.
- **Fold 3 was not a trend.** Blind benchmark +0.04 bp; the day-matched random
  null puts the model at the **100.0th percentile** with null p97.5 ≈ +0.99. Fold
  3 is a property of the data: the same effect reappears under run.009e's
  completely different model selection.
- **The up/dn heads mirror each other** in folds 2–3 (fold 3: up +10.88 / dn
  −13.49). Both sets of triggered bars moved *up* — two directional bets on
  high-magnitude bars, one of which was wrong.
- **The dn heads carry no direction.** Own/opposite lift ratio pooled: 009d
  0.83–1.16 (≈1.0), 009e 0.61–0.79 (inverted). For dn6/dn9/dn12 the *opposite*
  label lift exceeds the own label lift.
- **Fee floors vs the ceiling.** Max fee-free gross edge across all 12
  horizon×side cells ≈ +5 bp against a 10 bp taker round trip; full-maker is
  4 bp but pays ~5 bp in adverse selection, which cancels the saving. No cell, in
  any fold, is net-positive with taker fees.
- **The S confidence intervals in this doc are approximate** — computed as the
  interval of the equal-per-trade concatenated mean, ~1.7× too narrow and centred
  off `S`. The **point estimates** are what the conclusions rest on. See the CI
  defect section below.

## run.009e — the corrected objective made things worse

`EARLY_STOP` went `'ap'` → `'dir'` (own-label − opposite-label, top-0.1 % of val).

| metric | 009d | 009e |
|---|---|---|
| pooled IC h6 | +0.1093 | **+0.0710** |
| daily-IC t h6 | +13.53 | +9.92 |
| up18 DE | +18.53 pp | **+1.95 pp** |
| dn18 DE | +0.71 pp | **−5.76 pp** |
| test `S` @h18 | +5.60 | **−2.43** |
| gate | F/P/F/F | **A–F all fail** |

Selecting on DE produced *less* test DE than selecting on AP.

**Cause: the estimator was under-powered, and this was a defect in the brief.**
`DIR_RATE = 0.001` on val sets of 54 k–115 k bars gives **54–115 triggers**, and
`DE` is a difference of single-digit label counts. Verified directly from the
saved `val_labels`: 336 triggers pooled at 0.1 % (~84/fold), own/opposite label
rates 12–25 % → ~10–20 hits per fold. Observed val DE values quantise to
multiples of 1/55, and two epochs scored exactly `+0.0000` because *zero* labels
fired on the top-54 val triggers. Selected-epoch val DE ≈ +0.10…+0.18 produced
test DE +0.0195 — a 5–10× collapse. Selection chased noise.

So run.009e does **not** falsify "directional content is learnable". It falsifies
"top-0.1 % val DE is a usable selector".

**Useful by-product:** fold 1 is **bit-identical** between 009d and 009e
(+0.1001 ±0.0017 at h6) because both selectors chose epochs `[1,1,1]` there. That
is an internal control: the training loop, data path and evaluation are
unchanged, and every other difference is attributable purely to epoch choice.

## The CI defect — and what it invalidated

`boot_ci_mean(concat(fw[up], −fw[dn]))` is the interval of the **equal-per-trade
combined mean**, not of `S = mean(fw[up]) − mean(fw[dn])`. It is centred on the
wrong statistic and ~1.7× too narrow. Measured on 009d-era data with the same
`S = +5.60`: printed `[−1.43, +5.97]` (width 7.40) vs correct `[−1.80, +10.88]`
(width 12.67). Runs 009a–e all printed the defective form.

Re-derived from run.009e's own `run009e_scores.npz` (possible because 009e added
the artifact patch), the corrected 009e intervals are:

| h | S | printed | corrected | verdict |
|---|---|---|---|---|
| 6 | −1.52 | [−2.90, −0.23] | [−4.04, +0.79] | includes 0 |
| 9 | −3.01 | [−4.10, −0.36] | [−6.33, +0.73] | includes 0 |
| 12 | −4.31 | [−5.15, −1.11] | **[−8.33, −0.04]** | negative (marginal) |
| 15 | −4.27 | [−5.69, −1.16] | [−8.43, +0.68] | includes 0 |
| 18 | −2.43 | [−4.95, +0.21] | [−7.02, +3.74] | includes 0 |
| 24 | −0.06 | [−3.91, +1.14] | [−5.39, +6.58] | includes 0 |

Per fold at h18: f0 −9.94 **[−16.08, −3.41]** · f1 −2.84 · f2 −6.69 · f3 +3.48.

**So the earlier "significantly negative at h6–h15" claim is retracted to h12
only, plus fold 0.** The point estimates — `S ≤ 0` everywhere in 009e — stand,
and that is the load-bearing result.

## run.009f — the build

**One scientific variable: the selection estimator.** `EARLY_STOP` goes
`'dir'` → `'spread'` — the **continuous val market-neutral spread**
`mean(y_targets | up-head top-rate triggers) − mean(y_targets | dn-head triggers)`,
which uses every triggered sample's target value rather than counting labels.
Config: `DIR_RATE = 0.02`, `MIN_TRIG_VAL = 500`, `np.isfinite(cur)` guard and a
`best_state is None` fallback so a never-selected model cannot crash the run.

Plus instrumentation: `boot_ci_spread` (union-of-days bootstrap of
`mean_up − mean_dn`, days shared by both legs stay paired) replaces the defective
interval everywhere; the artifact patch (forward closes, raw preds, val labels)
is inherited from 009e.

### Verification (all on the frozen revision)

| check | result |
|---|---|
| `test_run009f.py` | **2029/2029 PASS** |
| `exec_verify_009f.py` | **ALL EXECUTION CHECKS PASS** |
| independent harness `verify_009f_mine.py` | **ALL PASS** |
| outputs / exec_counts | **all cleared** |
| non-patched cells vs 009e | **source-identical, 0 differing** |
| val legs at `DIR_RATE=0.02` | **1,085 / 1,428 / 1,876 / 2,297** (was 54–115) |
| notebook's own `_spread_and_null_gate` | `S = +5.60`, CI **`[−1.80, +10.88]`** |

## Bugs found and fixed this session

| # | bug | symptom | fix |
|---|---|---|---|
| 1 | `directional_edge` given the 2-D `vp` but indexed as 1-D | `IndexError` on the **first training epoch** — the whole objective dead | `np.ndim(probs) == 2 → probs[:, j_own]` |
| 2 | `fold_triggers(r,j,rate,variant)` called with 3 args in 10 places | `TypeError: missing 'variant'` in both new cells | added `pooled_triggers` |
| 3 | day-matched null drawn on the up head's days when evaluating the dn head | silently wrong null percentiles | `g_f = eu_f if side == 'up' else ed_f` |
| 4 | 10 cells carried run.009d's executed outputs, incl. a `EARLY_STOP=ap` print | false evidence inside the new notebook | clear all outputs globally |
| 5 | cells 6 and 11 printed `run.009e` as the run identity in the 009f build | a 009f notebook announcing itself as 009e — and the test had been weakened with a blanket cell-11 skip, so it passed | patch labels to `run.009f`; narrow the skip to lineage-only; add explicit identity assertions |
| 6 | `boot_ci_spread` called with a pre-negated dn leg (`-fw[ed]`) | interval for `mean_up + mean_dn`, not `S` — it did not contain the point estimate | pass `fw[ed]`; assert the CI **brackets the estimate**; static check for a negated dn leg |
| 7 | the first draft of the CI correction in the docs was itself computed with bug 6 | published `[−0.77, +12.32]` / "1.77×" | corrected to `[−1.80, +10.88]` / **1.71×** |

Bugs 1–3 were found only by **executing the notebook's own functions** — the
first acceptance test re-implemented the metric and validated a copy, reporting
1995/1995 on a notebook that crashed on its first epoch. The suite now extracts
and executes the notebook's real code, plus a static call-arity check.

Bug 6 was a defect in the brief, not the implementation: the negation is correct
inside a concatenated mean and wrong inside a difference of means.

## run.009f — the estimator was not the constraint

The change worked mechanically: val legs went from 54–115 triggers to **1,085–2,297**, and
the val spread became a continuous, well-powered statistic. It still failed.

| h | S (bp) | 95 % CI (spread) | |
|---|---|---|---|
| 9 | −2.60 | [−5.36, −0.36] | negative |
| 15 | −3.40 | [−7.10, −0.05] | negative |
| **18** | **−3.79** | **[−7.22, −0.42]** | **negative** |
| 6 / 12 / 24 | −0.96 / −2.87 / −3.74 | all include 0 | — |

Per fold at h18: f0 **−5.85 [−16.15, −0.40]** · f1 **−2.90 [−7.12, −0.70]** · f2 −3.67 · f3 −1.59.
The drift cell and the gate compute `S` independently and agree to the digit — the CI fix
is wired correctly in both.

**The three selection objectives rank monotonically AP > DE > spread** on signal quality,
directional content and economics:

| | 009d `'ap'` | 009e `'dir'` | 009f `'spread'` |
|---|---|---|---|
| pooled IC h6 | +0.1093 | +0.0710 | +0.0602 |
| up18 DE | +18.53 pp | +1.95 pp | **−3.83 pp** |
| up18 DC | 2.47 | 1.10 | **0.79** |
| test `S` @h18 | +5.60 | −2.43 | **−3.79** |
| maker up18 | −6.11 | −10.36 | **−11.39** |

The AP-selected model — the one whose metric was criticised for rewarding the volatility
artifact — had the **most** directional content. AP is a stable whole-distribution
statistic; both directional objectives are tail statistics that overfit the val window.

**Why it failed, precisely:** in folds 0–2 the selector found a mean val spread of
**+0.12 to +0.17 σ**, and the next ~10 days delivered **−2.9 to −5.9 bp** — the sign inverts
in exactly the folds where the val signal was strongest. Same failure mode as 009e, but
with a properly sized estimator, which is the informative part: **the problem was never
sample size.**

**Cross-run stabilities:** the fold-3 up-head still beats the day-matched null at the
100.0th percentile (+10.88 → +10.40 → **+4.00**, decaying) — real, one 10.5-day window, not
tradeable (+4.00 gross − 10 bp taker = −6.00). Fold 0's up head, previously at the 100.0th
percentile (+4.04), is now at the **0.2nd** (−1.21): fold-level up patterns are **not**
stable across models. Only fold 3's sign survives.

## `run.013` — the one untested input class

`runs/btc_lstm.run.013.ipynb` is **unexecuted** (19 code cells, no outputs, config still
pointing at `run012`). It aggregates the raw event stream **by price value and through
time** — one row per `(time, side, price)` cell, 93 columns across all four streams —
rather than by 5 s bar. `features.explanation.run.013.md` documents it.

This is materially different from run.012's time-based `ev_*` block (fill-timing AUC
0.49–0.51), so that verdict does **not** cover it. Its most valuable target is **T3**:
post a maker limit at a level and predict **(a) whether it fills and (b) whether it reaches
TP before SL** — the exact unknown that decides whether the ~5 bp adverse-selection cost
can be avoided. That is the one result that would change the verdict.

**But it is not ready.** Its own doc flags two structural problems:

1. **Causality (§6.4).** `dist_to_top_bps`, `dist_to_mid_bps`, `level_rank` are computed at
   **bin end**; the doc says they must be shifted one bin for a live signal. Unshifted,
   T1/T3 results will be optimistic.
2. **Stationarity (§7.1, "the biggest structural fix").** The panel is keyed by **absolute
   price**, so "a model trained on absolute-price rows cannot generalise across time". The
   proposed fix is a fixed **depth grid around mid**. Until that is done the panel may not
   generalise by construction.

It also inherits run.012's evaluation machinery — the old gate (own-label lift, which the
volatility artifact passes) and the defective `S` CI. **Any 013 result must be ported to
the 009f diagnostics (DE/DC, `boot_ci_spread`, day-matched null) before it is
interpretable**, or it will "pass" on the artifact the way 009d–f's criterion B does.

## Open items

- **`run.009f` is unexecuted.** Epoch choices, DE, all economics unknown.
- **Criterion F** (`>= N_FOLDS-1` folds beating the day-matched null) is the
  strictest criterion given fold-level heterogeneity; expect it to be the hardest.
- **`unan` collapsed in 009e** (n = 4 at 0.1 %, n = 0 at 0.01 %) because seeds
  picked divergent epochs. Expect the same if `'spread'` selection is noisy.
- **Do not compare intervals across runs** — 009a–e printed the defective form.
- The gate's failure guide is still run.007-era prose and has no branch for
  "market-neutral spread ≤ 0", which is the situation in both 009d and 009e.

## Next action

**This ladder is closed.** Three selection objectives, one diagnosis; the test
drift-cancelling spread is ≤ 0 and at h18 significantly so. Further work on this feature
set × horizon family × fee structure will not produce a positive `S`.

What remains, in order of value:

1. **Out-of-window falsifier** (cheap, decisive for this era). Re-run the 009f evaluation on
   post-2026-09-20 data. If `S` is still ≤ 0 the verdict closes for this data era. Needs a
   fresh tar.
2. **`run.013`'s T3 target — fill timing.** The only untested input class and the only lever
   that could change the economics. Prerequisites before it can be believed: shift the
   bin-end features (§6.4), consider the mid-relative re-index (§7.1), and port the 009f
   diagnostics so the result is not read through the artifact-passing gate. Frame it as a
   *pipeline + fill-timing AUC* run, not another gate attempt.
3. **A market-neutral label.** Predict the up/down *spread* directly rather than which
   barrier is touched first, so the volatility artifact becomes structurally unrewardable
   instead of something selection must avoid.

## Artifact index

| file | what |
|---|---|
| `btc_lstm.run.009f.ipynb` | executed; gate failed all six criteria (keep for the record) |
| `SESSION_NOTES_2026-09-21.md` | this file |
| `run009f.notes.md` | 009f build notes: patches, config, gate, verification, bug table |
| `run009e.analysis.md` | 009e results, corrected-CI table, CI defect record |
| `run009f.analysis.md` | **009f results** — gate, S by horizon, DE/DC, the AP>DE>spread ranking, non-generalisation, run.013 assessment |
| `run009d.offline.md` | Part 2 §8–10: drift control, volatility-selection mechanism, CI correction |
| `run009d.analysis.md` | 009d results with the corrective UPDATE block |
| `run009e_scores.npz` | 009e predictions with the artifact patch — makes the above offline-analysable |

### Harness (`/tmp/opencode`, ephemeral per the run.012 convention)

`build_run009f.py` · `test_run009f.py` · `exec_verify_009f.py` ·
`verify_009f_mine.py` · `resolve_ci.py` · `diag_lib.py` · `close_map.npz` ·
`venv009/` (numpy 2.5.3, pandas 3.0.6).

`close_map.npz` (3.6 MB) is the reusable piece: an epoch→close map rebuilt from
the 3.9 GB tar, which makes **any** `run009*_scores.npz` fully analysable offline
without the tar. Rebuild with `build_close_map.py` (117 s).
