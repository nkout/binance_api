# run.009d — build notes (feature prune from the run.009b importance study)

Companion to `runs/btc_lstm.run.009d.ipynb`. Built + tested 2026-09-20.

## What it is

`run.009d` = **run.009** (triple-barrier first-touch labels) on the **promoted w5
cadence**, with the **run.009b feature-importance findings** applied.

### Why run.009a's cells and not run.009's

`run.009` was executed on a **w15** tar (`20day_btc_data.tar.gz`, 125 files,
20.8 d, 2026-07-13 → 08-02). The current `60days_data.tar` contains **only
`out5.w5.*` files**, so a `WINDOW_SEC=15` notebook loads **zero files** on it —
it cannot run. The w5 realization of the same experiment is `run.009a`, and all
the feature-importance evidence is from that w5 model. So `run.009d` uses
run.009a's cells (identical wall-clock horizons, folds, model, training loop,
calibration, maker fill model and gate).

Lineage: `009` (label change) → `009a` (w5, 68.8 d) → `009d` (pruned).

## The change — one variable: the feature set

`run.009b` permuted each of the 76 features against the fold-3 test set of the
run.009a checkpoint (8 repeats, 20k windows, pooled noise floor) and measured the
drop in h18 up-head AP.

| group | n dropped | features |
|---|---|---|
| significantly **harmful** (negative drop, \|t\| > 2.37) | 13 | `minute_sin`, `vol_norm`, `ma_gap_4h`, `dow_sin`, `basis_z_4h`, `ma_gap_1h`, `sell_tail_ratio`, `wall_imbal`, `largest_trade_rel`, `wall_qty_norm`, `flow_net_widex_z`, `buy_accel`, `minute_cos` |
| **constant zero** in the eval window | 5 | `liq_flag`, `liq_cnt_log`, `liq_imbal`, `liq_notional_log`, `liq_notional_max_log` |

**76 → 58 features** by default. Kept composition: `v1_base` 29, `new_ctx` 8,
`v3` 21.

`vol_norm` is the headline drop: the **strongest univariate feature in the set**
(AUC 0.9108 up / 0.9142 dn) and significantly harmful on permutation (t = −4.2).

## Config knobs (cell 4)

| knob | default | note |
|---|---|---|
| `PRUNE_MODE` | `'harmful+dead'` | `'none'` \| `'harmful'` \| `'harmful+dead'` |
| `DROP_BLOCKS` | `[]` | block ablation: `'new_ctx'`, `'v3'`, `'v1_base'` |
| `DROP_HARMFUL` | 13 names | from run.009b |
| `DROP_DEAD` | 5 names | from run.009b |
| `TRAIN_ARM` | `'baseline'` | `'regularized'` sets LR 2e-4, dropout 0.4 |
| `EARLY_STOP` | `'ap'` | `'ic'` selects the epoch on validation IC instead |

Feature counts per mode (verified by the test): `none` 76 · `harmful` 63 ·
`harmful+dead` 58 · `+new_ctx` 64 · `+v3` 47 · `+v1_base` 41.

`TRAIN_ARM` and `EARLY_STOP` are **exploratory and OFF by default** so the primary
run stays single-variable. Rationale: run.009a's validation IC peaked at epoch 1
in 10/12 seed-folds and decayed with training, so the model gets roughly one epoch
of useful learning at LR 1e-3; `'regularized'` slows that down, `'ic'` tests
whether the AP-based selector was picking the wrong epoch.

## How to read the result — the control

`PRUNE_MODE='none'` reproduces run.009a (apart from output paths). That is the
control for any claim this run makes.

Compare against **run.009a fold 3** (same 68.8-day window, 42 test days):

| metric | run.009a |
|---|---|
| taker h18 @0.1% | up18 −10.39 / dn18 −10.40 bps, lift 15.32 / 18.46 |
| maker h18 @0.1% | up18 −10.74 / dn18 −10.99 bps, fill 75.8% / 77.4% |
| pooled IC | h6 +0.0886 → h24 +0.0419 |
| daily-IC t | h6 +11.63 / h18 +9.18 |

The prune is worth keeping only if pooled IC and/or the gate improve on the same
window. **A flat result is a real result** — it means the harmful features were
not load-bearing.

## Caveats

- **The prune is a hypothesis, not a proven fix.** Per-feature permutation
  importance is correlation-blind: it under-rates a feature whose information is
  duplicated elsewhere. The 13 "harmful" features are the *stronger* evidence
  class (a negative drop is not a redundancy artifact), but dropping them is still
  a test.
- **Retrains from scratch.** `F_DIM` changes with the feature set (76 → 58), so
  the run.009a checkpoint cannot be loaded.
- **Fold 3 only for the evidence.** The importance study used one fold; per-fold
  stability of the harmful list is untested.
- **The economics are untouched.** run.009a showed the gate fails because the
  trigger hit rate (~12–19%) sits far below the ~58% taker breakeven, and even 38%
  hit at 40× lift lost money. A better feature set raises the signal; it does not
  by itself clear the fee wall.
- **Block ablation is the more robust prune method** — `DROP_BLOCKS` exists
  because it does not depend on the correlation-blind per-feature ranking.

## Provenance / patches

Cells 1–9 and 10–15 are run.009a's, byte-identical except:

| cell | patch |
|---|---|
| 4 config | output dirs → `run009d`; run.009d prune + training config appended |
| 6 load/arrays | prune block inserted before the coverage-guard print |
| 9 train helpers | early-stop metric made selectable (`EARLY_STOP`) |
| 10 walk-forward | `run009a_scores.npz` → `run009d_scores.npz` |
| 14 plots | `run009a_selective.png` → `run009d_selective.png` |
| 15 save | model filename → `run009d`; stale run.009-era read-guide replaced |

New cells: header (0), prune summary + sanity anchor (10–11), section markdown
(12), README (19).

## Verification

`/tmp/opencode/build_run009d.py` (builder) and `/tmp/opencode/test_run009d.py`
(test). **58/58 PASS.** The test:

- compiles all 16 code cells (IPython magics in cell 3 neutralised);
- asserts cells 1–9 and 10–15 are byte-identical to run.009a except the six
  declared patches, and that each patch is bounded in size;
- runs the static undefined-name scan (`symtable`) over all cells in execution
  order;
- **executes the notebook's actual prune block** against the real 76-feature list
  and asserts `harmful+dead` → 58 with composition 29/8/21 and `ALL_FEATURES`
  order preserved, plus every other mode (`none` 76, `harmful` 63, `new_ctx` 64,
  `v3` 47, `v1_base` 41);
- asserts every name in the drop lists is a real feature;
- checks the artifact renames and that the stale run.007 reproduction guide is
  gone.

Negative tests confirm the checks bite: a typo in `DROP_HARMFUL` fails the
feature-name and 76→58 checks; removing the prune insertion fails the cell-6 and
prune-block checks.

To re-run after editing the builder: `cd /tmp/opencode && ./impvenv/bin/python
test_run009d.py`.

## Suggested sequence

1. Run `PRUNE_MODE='none'` first if you want a same-VM baseline, then
   `'harmful+dead'` — or just run the default and compare to run.009a's published
   numbers above.
2. If the prune helps, the next clean question is `DROP_BLOCKS=['new_ctx']` (the
   block run.009b found indistinguishable from its random null) and, separately,
   `TRAIN_ARM='regularized'`.
3. If the prune is flat, the importance route is exhausted and the remaining
   lever is a new input class — the v5 event stream (run.012), not more bar-level
   features.
