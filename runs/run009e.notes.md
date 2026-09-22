# run.009e — build notes (corrected model-selection objective)

Companion to `runs/btc_lstm.run.009e.ipynb`. Built + tested 2026-09-21.

## What it is

`run.009e` = **run.009d** (feature-pruned first-touch labels) with the
**model-selection objective corrected** from own-label AP to directional edge.

Lineage: `009` (label change) → `009a` (w5, 68.8 d) → `009d` (pruned) →
`009e` (corrected objective).

## The change — one variable: the selection objective

The offline diagnostics in `run009d.offline.md` proved that the model's
celebrated 25× "hit-rate lift" is mostly a **volatility artifact**: every head
raises its OPPOSITE first-touch label by 6.8–39.8× base rate, because its
triggers are high-|move| bars (3.3–5.3× the sample average). The model is
selected on own-label AP, which **rewards** this artifact.

`run.009e` changes `EARLY_STOP` from `'ap'` to `'dir'`. The new objective is
the **directional edge** `DE = own_label − opposite_label` on top-rate
triggers. `DE` is drift-free by construction: pure volatility selection raises
own and opp equally, so `DE → 0`.

**Everything else stays identical.** Feature set, folds, model architecture,
hyperparameters, trigger rules, sims, fee constants, `THETA_BY_H`,
`TP_MULT`/`SL_MULT`, maker fill model, `PRUNE_MODE='harmful+dead'`,
`TRAIN_ARM='baseline'`.

## Config knobs (cell 4)

| knob | default | note |
|---|---|---|
| `EARLY_STOP` | `'dir'` | `'ap'` (run.009a/d) \| `'ic'` \| `'dir'` (run.009e) |
| `DIR_RATE` | `0.001` | nominal rate for the val directional-edge computation |
| `DIR_MIN` | `0.05` | gate B: pooled DE must reach this |
| `PRUNE_MODE` | `'harmful+dead'` | unchanged from run.009d |
| `TRAIN_ARM` | `'baseline'` | unchanged from run.009d |

## How to read the result — the control

`EARLY_STOP='ap'` reproduces run.009d exactly. That is the control.

Compare against **run.009d**. The pre-registered question is:

> "Does selecting on directional edge produce DE > 0 and S > 0 on test, or
> does the parent's lift survive only as the volatility artifact?"

If the parent's edge was the volatility artifact, the corrected objective
should produce `DE ~ 0` and `S ~ 0` on test — which is the expected outcome.

## Cell patches

| cell | parent idx | patch |
|---|---|---|
| 0 header | 0 | run name → run.009e, body describes objective change |
| 4 config | 4 | output dirs → run009e, `EARLY_STOP='dir'`, `DIR_RATE`, `DIR_MIN` |
| 6 load | 6 | run-identity label → `run.009e prune (from run.009d)` |
| 9 training | 9 | `directional_edge()` helper, `cur` expression extended, hist/print extended |
| 10 prune | 10 | header → run.009e |
| 11 prune summary | 11 | run-identity prints → `run.009e` |
| 12 section | 12 | header → run.009e |
| 13 walk-fwd | 13 | npz → run009e_scores.npz, `val_pred_pool`, artifact patch (`fwd_close`, `val_labels`, `pred`, `val_pred`, `theta_by_h`, `window_sec`) |
| 15 NEW | — | drift-control diagnostics (own/opp lift, DE/DC, spread S, blind benchmark, null) |
| 16 maker sim | 15 | unchanged (source identical to parent cell 15) |
| 17 GATE | 16 | criteria B (directional content), E (spread S), F (null benchmark) added |
| 18 plots | 17 | filename → run009e_selective.png |
| 19 save | 18 | filenames → run009e, read-guide updated |
| 20 notes | 19 | updated for run.009e |

## Gate (deliberately stricter)

The gate now requires **A+B+C+D+E+F** on at least one side:

- **A** maker sim > 0, day-bootstrap CI > 0 (unchanged)
- **B** directional content: pooled DE ≥ `DIR_MIN` AND per-fold DE > 0 in ≥ 3/4 folds
- **C** fill honesty: fill ≥ 25%, per-fold sim > 0 in ≥ 3/4 folds (unchanged)
- **D** neighbouring rates sim > 0 (unchanged)
- **E** market-neutral spread S > 0 with day-bootstrap 95% CI excluding 0
- **F** model gross beats day-matched random-entry null 97.5th percentile in ≥ 3/4 folds

The old B (own-label lift ≥ 3×) is printed as **B (info)** — informational
only, since it passes on the volatility artifact.

## Caveats

- **The objective change is a hypothesis.** `DE = own − opp` is drift-free by
  construction, but whether it produces a better-selected checkpoint on test
  data is an empirical question.
- **Retrains from scratch.** Same as run.009d — `F_DIM` is unchanged (58
  features), but the selection metric changes.
- **Vol-normalised labels are out of scope.** The label construction is
  unchanged from run.009d. A separate future run should normalise labels by
  realised volatility.
- **One window, one asset.** Same 68.8-day `60days_data.tar` as run.009d.

## Verification

`/tmp/opencode/build_run009e.py` (builder), `/tmp/opencode/test_run009e.py`
(test) and `/tmp/opencode/exec_verify_009e.py` (separate execution harness).
**2015/2015 PASS.** The notebook ships with **all outputs cleared** (unexecuted —
no Colab run has happened for run.009e). The test:

- compiles all 17 code cells (IPython magics in cell 3 neutralised);
- asserts all non-patched cells are **source** byte-identical to the parent
  (outputs/exec_count are not compared — they are cleared globally);
- **asserts every code cell has `outputs == []` and `execution_count is None`**;
- **asserts no source line prints `run.009d:` (with colon)** and that cells 6
  and 11 identify as `run.009e` in their print lines;
- checks patch size bounds;
- runs the static undefined-name scan (`symtable`) over all cells;
- **verifies the early-stop expression uses VAL data only** (no `test_`,
  `prob_pool`, or test ends in the DE block);
- **static call-arity check** across all cells: 154 call sites against 39
  definitions (bare-name calls only; skips `*args` / `**kwargs` and attribute
  calls such as `plt.show()`);
- **executes the notebook's own extracted functions** — not re-implementations —
  against real data from `data/run009d_scores.npz` + `/tmp/opencode/close_map.npz`:
  - `directional_edge` with the exact 2-D call shape cell 9 uses;
  - `directional_edge` equals an independent 1-D reference to `1e-12`;
  - `pooled_triggers` length is aligned with `ends_pool`;
  - `_fwd_all_drift` equals the close-map forward return (max diff `0.0`);
  - `_spread_and_null_gate` returns S = **+5.60 bp, CI [−1.43, +5.97]**,
    per-fold S `+5.70 / −2.84 / +2.27 / −2.61`, fold-3 S ≤ 0;
  - `_de_for_gate` pooled DE = **+18.53 pp**;
  - up18 own = 31.14 %, opposite = 12.61 %; dn18 18.55 % / 17.84 %; up24 34.88 % / 11.52 %;
  - blind benchmark fold 3 up18: +0.04 bp; day-matched null: model +10.88,
    null p97.5 ≈ +0.99, model percentile 100.0;
  - tolerances: ±0.02 for percentages, ±0.05 bp for bps.

### Runtime bugs found and fixed during verification

The first build passed its own suite but **could not run**. Two crashes, both in
the new code, found only by executing the notebook's own functions:

| bug | symptom | fix |
|---|---|---|
| `directional_edge` called with the 2-D `vp` matrix but indexed as a 1-D per-head vector | `IndexError: too many indices for array` on the **first training epoch** (`EARLY_STOP='dir'` is the default) | added an `np.ndim(probs) == 2 → probs = probs[:, j_own]` guard |
| `fold_triggers` (signature `(r, j, rate, variant)`) called with 3 args in 10 places in cells 15/17 | `TypeError: missing 1 required positional argument: 'variant'` | added `pooled_triggers(j, rate, variant)` and replaced all 10 call sites |
| `_spread_and_null_gate` drew its day-matched null on the **up** head's trigger days even when evaluating the **dn** head | silently wrong null percentiles | `g_f = eu_f if side == 'up' else ed_f` for both the model gross and the null |

The root cause of the missed regression was that the acceptance test
**re-implemented** the metric instead of executing the notebook's code — it
validated a copy. It now extracts and executes the notebook's own functions, and
the static arity check generalises the fix so an argument-count mismatch cannot
ship again.

To re-run after editing the builder: `cd /tmp/opencode && ./venv009/bin/python
test_run009e.py && ./venv009/bin/python exec_verify_009e.py`.
