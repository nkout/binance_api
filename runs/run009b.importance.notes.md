# run.009b feature importance + block controls — build notes

Companion to `runs/btc_lstm.run.009b.importance.ipynb`. Built + tested 2026-09-20.

## What it is

A Colab notebook that loads the **trained run.009a model from Drive** and answers
"which of the 76 features does the model actually use?" — with a **control** so
the block-level answers are interpretable.

- Cells 1–9 are **byte-identical** to `btc_lstm.run.009a.ipynb` (imports, Drive
  mount + tar extract, config, feature pipeline, loader/arrays, folds, model,
  training helpers). Only `OUTPUT_DIR` / `DRIVE_SAVE_DIR` are repointed
  (`.../btc_lstm_run009b_importance`). Verified by the test.
- Cells 10–34 are new.

## Data / model paths

| what | path |
|---|---|
| bars tar | `/content/drive/MyDrive/60days_data.tar` → extracts to `/root/btc_data` |
| model | `/content/drive/MyDrive/btc_lstm_run009a/lstm_run009a_h18.pt` |
| outputs | `/content/drive/MyDrive/btc_lstm_run009b_importance/` |

If the checkpoint is absent, `TRAIN_IF_MISSING=True` retrains fold 3 with
run.009a's own `train_fold`. The checkpoint holds only the **last** fold
(run.009a cell 15 saves `results[-1]`), so importance is measured on fold 3.

## Method

On the fold-3 test set, scaled with the checkpoint's own scaler:

1. **Per-feature permutation importance** — a feature's whole 192-step trajectory
   is shuffled *across samples*. The drop in `ic_h6`, `ic_h18`, `ap_up18`,
   `ap_dn18` is its importance. Ranked against a **pooled** noise floor
   (`POOLED_FLOOR_MULT` × median across-repeat sd).
2. **Grouped permutation importance** — same per block (`v1_base`, `new_ctx`,
   `v3`), with across-repeat **sd** reported.
3. **Random matched-size block control** — the null for (2). For each block,
   `N_RANDOM_CONTROLS` random feature sets of the *same size* are drawn from the
   other features and permuted identically. A block is only special if its drop
   sits outside that random range.
4. **Univariate read** (model-free) — Spearman IC and direction-agnostic AUC of
   each feature's last-timestep value.

Inputs are permuted **in place and restored**; the test asserts bitwise restore.
Group permutation uses a slice copy (`orig[perm]`) rather than copying the whole
`(n, T, F)` tensor, which keeps the control pass affordable.

## Config knobs

| knob | default | note |
|---|---|---|
| `EVAL_FOLD` | `N_FOLDS-1` (3) | must match the saved model; other folds need retraining |
| `N_EVAL` | 20000 | test windows held in RAM (~1.2 GB). Full fold-3 test ≈ 165k ≈ 9.6 GB — do not set 0 |
| `N_REPEATS` | **8** | was 2; 2 gives a 1-dof sd and an unreliable floor |
| `N_RANDOM_CONTROLS` | **5** | random matched-size blocks per group |
| `POOLED_FLOOR_MULT` | **2.0** | pooled floor = MULT × median repeat sd |
| `TRAIN_IF_MISSING` | True | retrain fold 3 if the .pt is absent |
| `IMP_SEED` | 0 | deterministic permutations |

Runtime at defaults: ~15 min per-feature sweep + ~10 min grouped/control on a T4.

## Interpretation rules (why the controls exist)

- **Per-feature permutation importance is correlation-blind.** A feature whose
  information is duplicated by a correlated feature scores low even when it
  matters. Observed in the first 009a run: `vol_norm` is the strongest univariate
  feature (AUC 0.91 on *both* up and dn labels) yet fell below the permutation
  floor, because the same volatility information is available from
  `mid_rv_norm`, `depl_total_norm`, `vol_ratio_1h_24h` and others. The
  below-floor list is **descriptive only — do not prune from it.**
- **Block drops need the size control.** Permuting k features removes k inputs'
  worth of information regardless of which k they are, so a large block can drop
  a lot just by being big.
- **Univariate AUC mostly measures volatility.** First-touch labels require a big
  move in *either* direction, so the top univariate features score high on both
  `lup_18` and `ldn_18`. That is a property of the label, not directional skill.
- **To actually prune, use block ablation** (drop the block, retrain, compare) —
  not the per-feature permutation ranking.

## Gotchas encoded

- `torch.load(..., weights_only=False)` — the checkpoint carries a pickled sklearn
  `StandardScaler`; torch ≥ 2.6 defaults to `weights_only=True` and would refuse it.
- Feature list in the checkpoint is asserted equal to the rebuild; a mismatched
  tar aborts rather than scoring the wrong columns.
- `EVAL_FOLD` is asserted to be the last fold, matching what run.009a saved.
- The control's draw is guarded when a block exceeds half the feature count.

## Verification

`/tmp/opencode/build_run009a_importance.py` (builder) and
`/tmp/opencode/test_run009a_importance.py` (test). **58/58 PASS.** The test:

- compiles all 21 code cells (IPython magics in cell 3 neutralised; proven
  inherited from run.009a, not introduced);
- asserts cells 1–9 byte-identical to run.009a (cell 4 modulo the dir patch);
- extracts the notebook's **actual** `permuted_importance` /
  `permuted_importance_group` source from the built `.ipynb`, exec's it, and runs
  it against a synthetic model that provably reads only one feature:
  signal feature AP drop 0.9074 / IC drop 0.9695 vs noise feature 0.0000 / 0.0000;
  signal ranked #1; input tensor restored bitwise;
- checks degenerate all-zero labels do not crash `metrics_from`;
- runs a **static undefined-name scan** (`symtable`) over all cells in execution
  order, so a name used before it is bound in any earlier cell fails the build;
- **executes every consumer cell** (rank/prune, grouped, control, univariate,
  plot, save) against the real producer output, so a producer/consumer column
  mismatch fails;
- asserts the control cell's signal-bearing block beats its random null
  (0.9168 vs 0.0000) and that a size-matched random block stays near zero;
- asserts the save cell imports `shutil` itself rather than receiving it;
- asserts all five output artifacts are written and copied.

Negative tests confirm the checks bite: reverting the `_sd` column fix
reproduces `KeyError: 'ap_up18_drop_sd'`; deleting `import shutil` reproduces
`NameError: name 'shutil' is not defined`.

To re-run after editing the builder: `cd /tmp/opencode && ./impvenv/bin/python
test_run009a_importance.py`.

## Failure history

**Build 1** was executed on Colab and crashed at the rank/prune cell after the
pipeline, checkpoint load, baseline and the full permutation sweep had succeeded.
Three defects, all in the new cells: a producer/consumer column mismatch
(`ap_up18_sd` vs `ap_up18_drop_sd`, in the rank cell and the plot's `xerr=`), and
`shutil` used but never imported (run.009a imported it inside its own save cell,
which is not part of cells 1–9). The original test passed because it validated the
producer's columns and the notebook's wiring by substring but never executed a
consumer cell — that gap is now closed by the static scan and the
consumer-execution pass.

**Build 2 (`009b`)** executed cleanly end-to-end. Baseline on fold-3 test (20k
subsample): `ic_h6 +0.1258`, `ic_h18 +0.0457`, `ap_up18 +0.1601`,
`ap_dn18 +0.1913` — consistent with run.009a's fold 3. Its results are validated
in the session record; the executed notebook is preserved in git at commit
`2b9022d`.

**Build 3 (this one)** adds the random matched-size block control, `N_REPEATS` 8,
the pooled noise floor, group sd reporting, and 009b output naming.

## Validation of build 3

See `run009b.importance.validation.md`. Outcome: `v3` confirmed special (0/5
random blocks; +2.7σ; 97% of h6 IC); `v1_base` redundant (−9.9σ below its null);
`new_ctx` not significant (t = −1.78); **13/76 features significantly harmful**
(incl. `vol_norm`, the strongest univariate feature); **5/76 constant zero** in
fold 3. Next step is a block-ablation run using the harmful list.
