# run.009f — build notes (spread-based selection estimator)

Companion to `runs/btc_lstm.run.009f.ipynb`. Built + tested 2026-09-21.

## What it is

`run.009f` = **run.009e** (directional-edge selection) with the
**selection estimator changed** from label-count DE to continuous
market-neutral spread, and the **S confidence interval fixed**.

Lineage: `009` (label change) → `009a` (w5, 68.8 d) → `009d` (pruned) →
`009e` (corrected objective) → `009f` (spread estimator + CI fix).

## The change — one variable: the selection estimator

`run.009e` selected epochs on `directional_edge` (`DE`, own-label minus
opposite-label) computed on the **top 0.1 % of val probs**. Val sizes are
54,270 / 71,426 / 93,809 / 114,862, so that is **54–115 triggers**, and DE
is a difference of **single-digit label counts** (observed val values
quantise to multiples of 1/55 and 1/115: `+0.1818 = 10/55`, `+0.0000` at
two epochs where zero labels fired). Selecting on that chased noise:
selected-epoch val DE was ~+0.10…+0.18 and the resulting **test** DE was
+1.95 pp (up18) / −5.76 pp (dn18) — i.e. **worse than run.009d's AP-based
selection** (+18.53 / +0.71 pp). The estimator was under-powered.

`run.009f` changes `EARLY_STOP` from `'dir'` to `'spread'`. The new
estimator is the **val market-neutral spread** on the continuous
vol-normalised target — it uses every triggered sample's target value (not
label counts), cancels drift by construction (long up-head triggers, short
dn-head triggers), and at the new rate has >1000 samples per leg.

**Everything else stays identical.** Feature set, folds, model architecture,
hyperparameters, trigger rules, sims, fee constants, `THETA_BY_H`,
`TP_MULT`/`SL_MULT`, maker fill model, `PRUNE_MODE='harmful+dead'`,
`TRAIN_ARM='baseline'`.

## Config knobs (cell 4)

| knob | default | note |
|---|---|---|
| `EARLY_STOP` | `'spread'` | `'ap'` (009d) \| `'dir'` (009e) \| `'spread'` (009f) |
| `DIR_RATE` | `0.02` | selection rate (2 % of val = 1,085–2,297 triggers/leg) |
| `DIR_MIN` | `0.05` | gate B: pooled DE must reach this |
| `MIN_TRIG_VAL` | `500` | min triggers per leg for spread estimator |
| `PRUNE_MODE` | `'harmful+dead'` | unchanged from run.009d |
| `TRAIN_ARM` | `'baseline'` | unchanged from run.009d |

## How to read the result — the control

`EARLY_STOP='ap'` reproduces run.009d exactly. `EARLY_STOP='dir'`
reproduces run.009e exactly. Either is the control.

Compare against **run.009d** and **run.009e**. The pre-registered question is:

> "Does selecting on the continuous spread produce a better-selected
> checkpoint than the label-count DE that chased noise?"

## Cell patches

| cell | parent idx | patch |
|---|---|---|
| 0 header | 0 | run name → run.009f; describe the estimator change; cite the 009e under-powering |
| 4 config | 4 | dirs → run009f; `EARLY_STOP='spread'`; `DIR_RATE=0.02`; `MIN_TRIG_VAL=500` |
| 9 training | 9 | `val_mn_spread`; `v_spread`; `cur` 4-way branch; NaN guard; `best_state` fallback; hist + print |
| 13 walk-fwd | 13 | npz → `run009f_scores.npz` |
| 15 drift | 15 | `boot_ci_spread` helper; use it for every S CI; label column `S 95% CI (spread)` |
| 17 gate | 17 | use `boot_ci_spread` for criterion E; keep A/B/C/D/F logic; update header text |
| 18 plots | 18 | png → `run009f_selective.png` |
| 19 save | 19 | model → `lstm_run009f_h{H_SEL}.pt`; png name; read-guide updated |
| 20 notes | 20 | updated for run.009f |


## S confidence-interval fix

In cells 15 and 17 the printed `S CI` was `boot_ci_mean(concat(fw[up],
-fw[dn]))` — the CI of the **equal-per-trade combined mean**, NOT of
`S = mean(fw[up]) − mean(fw[dn])`. With n_up=668 vs n_dn=2,048 the two
differ. The new `boot_ci_spread` helper does a day-cluster bootstrap of S
directly, resampling the union of days so days shared by both legs stay
paired. **Point estimates of S are unchanged.**

## Gate (unchanged from run.009e)

The gate requires **A+B+C+D+E+F** on at least one side:

- **A** maker sim > 0, day-bootstrap CI > 0
- **B** directional content: pooled DE ≥ `DIR_MIN` AND per-fold DE > 0 in ≥ 3/4 folds
- **C** fill honesty: fill ≥ 25%, per-fold sim > 0 in ≥ 3/4 folds
- **D** neighbouring rates sim > 0
- **E** market-neutral spread S > 0 with day-bootstrap 95% CI excluding 0 —
  direct test of what is now selected for
- **F** model gross beats day-matched random-entry null 97.5th percentile in ≥ 3/4 folds

## Caveats

- **The estimator change is a hypothesis.** The continuous spread uses more
  samples and is drift-free by construction, but whether it produces a
  better-selected checkpoint on test data is an empirical question.
- **The CI fix changes printed CIs.** Point estimates of S are identical;
  only the confidence intervals change.
- **Retrains from scratch.** Same as run.009e — `F_DIM` is unchanged (58
  features), but the selection metric changes.
- **One window, one asset.** Same 68.8-day `60days_data.tar` as run.009d.

## Verification

`/tmp/opencode/build_run009f.py` (builder), `/tmp/opencode/test_run009f.py`
(test) and `/tmp/opencode/exec_verify_009f.py` (separate execution harness).
**2029/2029 PASS.** The notebook ships with **all outputs cleared**
(unexecuted — no Colab run has happened for run.009f). The test:

- compiles all 17 code cells with **`compile()`**, not `ast.parse` (IPython magics in cell 3 neutralised) — `ast.parse` does **not** validate
  `break`/`continue`/`return` placement, which is exactly how a structural
  SyntaxError shipped; cell 9 additionally has a dedicated regression check
  that its `break` has an enclosing loop and that the `best_state` fallback
  sits after the epoch loop;
- asserts all non-patched cells are **source** byte-identical to the parent;
- **asserts every code cell has `outputs == []` and `execution_count is None`**;
- checks patch size bounds;
- runs the static undefined-name scan (`symtable`) over all cells;
- **verifies the early-stop expression uses VAL data only** (`Y_all[val_ends]`,
  `vp` — no `test_`, `prob_pool`, `test_ends`);
- **static call-arity check** across all cells: 158 call sites against 42
  definitions;
- **asserts cells 6 and 11 identify as `run.009f`** and print no `run.009e`
  identity (lineage references to `run.009e` elsewhere are allowed, narrowly
  matched);
- **asserts `boot_ci_spread`'s CI brackets the point estimate `S`**, and that no
  `boot_ci_spread` call passes a pre-negated dn leg;
- **executes the notebook's own extracted functions** against real data:
  - `val_mn_spread` with a 2-D probs matrix runs and returns a float;
  - `val_mn_spread` returns `np.nan` for 1-D input (np.ndim != 2);
  - `val_mn_spread` returns `np.nan` when either leg is below `min_trig`;
  - `boot_ci_spread` runs and its 2.5/97.5 percentiles are finite and ordered;
  - `_spread_and_null_gate` returns S = **+5.60 bp** at h18 @0.1 %, CI
    **`[−1.80, +10.88]`**, and per-fold S `+5.70 / −2.84 / +2.27 / −2.61`,
    fold-3 S ≤ 0 — point estimates unchanged from run.009e, only the CI changed;
  - `_de_for_gate` pooled DE = **+18.53 pp** (unchanged);
  - `pooled_triggers` n=1,063 for up18 @0.1 % (unchanged);
  - `_fwd_all_drift` matches close-map forward return (max diff 0.0).

### Bugs found and fixed during verification

| bug | symptom | fix |
|---|---|---|
| cells 6 and 11 printed `run.009e` as the **run identity** (`run.009e: PRUNE_MODE=…`, `run.009e retrains from scratch`) | a 009f notebook announcing itself as 009e; the test had been weakened with a blanket skip of cell 11, so it passed anyway | patch both cells to `run.009f`; replace the blanket skip with narrow lineage-only allowances and add explicit identity assertions |
| `boot_ci_spread` was called with a **pre-negated dn leg** (`-fw[ed]`), carried over from the old `concat(fw[eu], −fw[ed])` form | the function already subtracts the two leg means, so the CI was for `mean_up + mean_dn`, not `S` — the interval did not even contain the point estimate | pass `fw[ed]`; add the bracket-the-estimate assertion plus a static check for a negated dn leg |
| the `best_state is None` fallback was inserted at **indent 4**, in front of `hist.append` — which closes the `for epoch` loop | the loop's `break` had **no enclosing loop**: `SyntaxError: 'break' outside loop`. run.009f **failed on Colab**, having executed cells 1–8 cleanly | move the fallback to **after** the loop (immediately before `model.load_state_dict`), in notebook and builder; switch the test's compile check from `ast.parse` to **`compile()`** and add the cell-9 regression check |

The interval defect was in the brief, not the implementation: the negation is
correct inside a concatenated mean and wrong inside a difference of means. It
also affected the first draft of the correction note in `run009d.offline.md`
and `run009e.analysis.md`, which have been re-derived from run.009e's own
`run009e_scores.npz` (the 009e npz artifact patch makes this possible without
the tar).

To re-run after editing the builder: `cd /tmp/opencode && ./venv009/bin/python
test_run009f.py && ./venv009/bin/python exec_verify_009f.py`.

---

## Status & next action — end of session 2026-09-21

**Ready to run, not yet executed.** `runs/btc_lstm.run.009f.ipynb` ships unexecuted
with all outputs cleared. Verified: `test_run009f.py` **2029/2029**,
`exec_verify_009f.py` **ALL EXECUTION CHECKS PASS**, and an independent harness
(`/tmp/opencode/verify_009f_mine.py`) **ALL PASS**. The notebook's own
`_spread_and_null_gate` executes on real data and returns `S = +5.60 bp`,
CI `[−1.80, +10.88]`, per-fold `+5.70 / −2.84 / +2.27 / −2.61`.

### The one number to watch

Test **market-neutral spread `S` at h18 @0.1 % ens** — gate criterion E — and its per-fold values.

- **`S > 0` with the CI excluding 0** → the estimator *was* the binding constraint.
  Escalate to an execution study (queue-aware fill model), because a positive
  drift-cancelling spread is the first thing in this project worth paying fees on.
- **`S ≤ 0` with a CI that brackets it** → the estimator was **not** the constraint.
  The volatility-selection diagnosis in `run009d.offline.md` Part 2 is then final
  for this feature set and horizon family, and the remaining levers are the ones
  already measured dead (more bar data, feature pruning, exit grids, latency,
  event order flow — run.012's fill-timing AUC was 0.49–0.51).

Also record: gate criteria A–F, per-horizon `DE`, the val→test spread shrinkage
(selection quality), and which epochs each seed selects.

### Baselines — same window, same folds

| run | selection | pooled IC h6 | test `S` h18 (point) | gate |
|---|---|---|---|---|
| 009d | `'ap'` | +0.1093 | +5.60 | A/B/C/D = F/P/F/F |
| 009e | `'dir'` | +0.0710 | −2.43 | A–F all fail |
| **009f** | `'spread'` | ? | ? | ? |

**Do not compare intervals across runs.** 009a–e printed the defective
`concat`-mean interval; only 009f's is a confidence interval for `S`. Compare
**point estimates** only.

### Open items / known issues

- The `S CI` in runs 009a–e is the wrong statistic (concat-mean, ~1.7× too
  narrow, centred off `S`). Only 009f's interval is correct.
- 009e's significance claims were re-derived from `run009e_scores.npz`: the
  spread is significantly negative **only at h12** (marginal, upper bound −0.04)
  and in **fold 0** (−9.94 [−16.08, −3.41]). The earlier "h6–h15" claim is retracted.
- Gate criterion F (`>= N_FOLDS-1` folds beating the day-matched null) is strict
  given fold-level heterogeneity; expect it to be the hardest criterion.
- `unan` collapsed in 009e (n = 4 at 0.1 %) because seeds picked divergent
  epochs; expect the same if `'spread'` selection is noisy.
- 009f is unexecuted — epoch choices, DE, and all economics are unknown.
- **Harness lives in `/tmp/opencode`** (ephemeral, per the run.012 convention):
  `build_run009f.py`, `test_run009f.py`, `exec_verify_009f.py`,
  `verify_009f_mine.py`, `resolve_ci.py`, `diag_lib.py`, `close_map.npz`.
  `close_map.npz` (3.6 MB) is the reusable piece — it makes any
  `run009*_scores.npz` fully analysable offline without the 3.9 GB tar.

### Artifacts written this session

| file | what |
|---|---|
| `btc_lstm.run.009f.ipynb` | new notebook, unexecuted |
| `run009f.notes.md` | this file |
| `run009e.analysis.md` | run.009e results, corrected-CI table, bug record |
| `run009d.offline.md` | Part 2 §8–10 (drift control, volatility-selection mechanism) + CI correction |
| `run009d.analysis.md` | run.009d results (corrected UPDATE block) |

### Correction log (this session)

1. `directional_edge` called with the 2-D `vp` matrix but indexed as 1-D →
   `IndexError` on the first training epoch (run.009e build; fixed pre-run).
2. `fold_triggers` called with 3 args in 10 places → `TypeError`
   (run.009e build; fixed pre-run via `pooled_triggers`).
3. Day-matched null drawn on the up head's days while evaluating the dn head
   (run.009e build; fixed).
4. run.009e carried run.009d's executed outputs and a stale `EARLY_STOP=ap` print
   (fixed; all outputs cleared).
5. Cells 6 and 11 printed `run.009e` as the run identity in the 009f build, and
   the test had been weakened with a blanket cell-11 skip (fixed; narrow
   lineage allowances + explicit identity assertions added).
6. `boot_ci_spread` called with a pre-negated dn leg (`-fw[ed]`), so the interval
   was for `mean_up + mean_dn`, not `S` — it did not contain the point estimate.
   **This was a defect in the brief, not the implementation** (the negation is
   correct inside a concatenated mean, wrong inside a difference of means).
   Fixed at 3 call sites + builder; the suite now asserts the CI brackets the
   estimate and that the dn leg is not negated.
7. Doc-level: the first draft of the CI correction in `run009d.offline.md` and
   `run009e.analysis.md` was itself computed with bug 6 (`[−0.77, +12.32]`,
   "1.77×"). Correct values: `[−1.80, +10.88]`, **1.71×**, and the 009e
   significance retraction above.
