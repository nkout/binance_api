# GBM long-horizon probe — build notes

Companion to `runs/btc_lstm.probe.gbm.ipynb` (v1, **executed** — kept as the record) and
`runs/btc_lstm.probe.gbm2.ipynb` (v2, adds the drift control). Built + tested 2026-09-22.
**Results: `gbm_probe.analysis.md` — FALSIFIED.**
Rationale and the measurements that motivate it: `horizon_economics_and_next_ideas.md` §7.

## What it is

A **probe, not a gated run.** One question, answered as cheaply as possible before any
two-stage notebook gets built:

> The 90 s programme needs 0.77 directional accuracy and has ~0.52. At 15 min – 1 h the
> requirement falls to 0.61 / 0.59 (taker) because `E|move|` on the stage-1 trigger set
> grows 18.6 → 44.5 → 55.8 bp while the fee stays fixed. **Is there any directional signal
> at those horizons in the existing 76 features?**

It implements both of the proposals it was built for: **configurable horizon** (`LONG_H`)
and a **two-stage split** (`mag` = big-move detector, `dir_*` = direction), including the
"train direction only on big moves" arm.

## Lineage

Built from `btc_lstm.run.009f.ipynb` by `runs/harness_gbm_probe/build_gbm_probe.py`.

| cell | source | change |
|---|---|---|
| 0 | new | markdown header: motivation, measured context, pre-registered read-out |
| 1 | 009f cell 1 | **only** `'xgboost'` added to the pip list |
| 2, 3 | 009f cells 2, 3 | **byte-identical** (transfer helpers, Drive mount + tar) |
| 4 | 009f cell 4 | output dirs repointed · `PRUNE_MODE='none'` · probe config appended |
| 5 | 009f cell 5 | **byte-identical** — the whole feature pipeline, 76 features |
| 6 | 009f cell 6 | **byte-identical** — load, era slice, coverage guard, arrays |
| 7 | 009f cell 7 | purge widened `MAX_H` → `max(MAX_H, max(LONG_H))` |
| 8 | new | markdown divider |
| 9–15 | new | cells G1–G7 (the probe) |

Everything from 009f cell 8 onward (the LSTM model, training loop, sims, gate, plots) is
**dropped**. The probe does not simulate exits and does not reuse the old gate — that gate
passes on the volatility artifact (criterion B) and carried the defective `S` CI.

## The three changes that matter

1. **`PRUNE_MODE='none'` (76 features, not 58).** `DROP_HARMFUL` was derived from the h18
   up-head **AP** drop — a *magnitude* metric at a *90 s* horizon — and it dropped
   `basis_z_4h`, `ma_gap_1h`, `ma_gap_4h`, `minute_sin/cos`, `dow_sin`, `vol_norm`, … i.e.
   the slow directional/carry block. Wrong objective and wrong horizon for this question.
   Cell G6 re-judges the prune on a direction objective and prints how much gain the 18
   dropped features actually carry.

2. **Purge widened to `max(LONG_H)`.** A 1 h label started just before a fold boundary
   resolves 720 bars later — inside the next split. With 009f's `MAX_H`(24) purge that is
   direct label leakage into test. The test asserts `val_end + PURGE <= test_start` and
   `train_hi + PURGE <= val_start` on every fold.

3. **Long targets built outside `prepare()`.** Cell G1 computes them from `close` directly
   so cells 1–5 stay byte-identical and `MAX_H` / `sample_valid` / `contig` keep exactly
   their 009f meaning. `vol_h` mirrors `prepare()`'s own formula (rolling std of the h-bar
   price change) with a horizon-scaled window — a 720-bar diff inside a 720-bar window
   would be a single overlapping observation.

## Config

| knob | default | note |
|---|---|---|
| `LONG_H` | `[180, 720]` | 15 min and 1 h at 5 s bars |
| `LAGS` | `[12, 60, 180]` | feature deltas (1/5/15 min); asserted `<= SEQ_LEN-1` |
| `EVENT_RATE` | `0.05` | 'eventful' share for the `dir_evt` training subset |
| `TRIG_RATES` | `[0.001, 0.01]` | stage-1 trigger rates evaluated |
| `TRAIN_CAP` | `400_000` | rows per fold; lower it to trade accuracy for speed |
| `GATE_T` | `3.0` | criterion A daily-IC t threshold |
| `GBM_PARAMS` | depth 5, lr 0.05, `min_child_weight=200` | shallow + heavily smoothed on purpose |

Design matrix: `76 × (1 + 3 lags) = 304` columns. `contig[e]` already guarantees 191
contiguous bars behind `e`, so every lag is a real bar rather than one across a gap.

## The three heads

| head | target | trained on | answers |
|---|---|---|---|
| `mag` | `\|y\|` | all bars | stage 1 — the big-move detector |
| `dir_all` | signed `y` | all bars | stage 2, baseline |
| `dir_evt` | signed `y` | **eventful bars only** | the "train on big moves only" hypothesis |

`dir_evt`'s eventful threshold is taken from the **training** split's own `\|y\|` quantile
and its early stopping uses the **val** split's — no test information anywhere.

## Pre-registered read-out

- **A (signal)** — daily-IC t ≥ `GATE_T` and IC > 0 in ≥ 3/4 folds, at 15 min or 1 h.
- **B (economics)** — accuracy on the stage-1 top-0.1 % ≥ the **maker** requirement
  (0.545 @ 15 min, 0.536 @ 1 h), recomputed from this run's own `E|move|`.
- **C (net)** — net at 7 bp > 0 with a day-clustered CI excluding 0.
- **Falsification** — A fails at both horizons → the existing bar features carry no usable
  long-horizon direction. Do **not** build the two-stage notebook; move to new inputs
  (run.013 price panel) or close the direction line.

**A-pass / B-fail is the run.009d–f pattern** (signal real, too weak to trade) and is not a
green light. Cell G7 prints this distinction rather than a single PASS/FAIL.

## Runtime

GPU (T4) ~10–20 min, CPU ~40–80 min; `device` is auto-detected via `torch.cuda.is_available()`
and falls back to CPU. Needs `60days_data.tar` on Drive, same as run.009f.

## Verification

`runs/harness_gbm_probe/test_gbm_probe.py` — **98/98 PASS**, run twice: once with a stubbed xgboost and
once against **real xgboost 3.4.1** (`REAL_XGB=1`) to check API compatibility. The test:

- `compile()`s every code cell — *not* `ast.parse`, which does not validate `break`
  placement and is how run.009f shipped a `SyntaxError` past a green suite;
- asserts cells 2, 3, 5, 6 are **byte-identical** to run.009f and that cells 1, 4, 7 differ
  only in the ways listed above;
- asserts outputs and execution counts are cleared everywhere (run.009f's defect #4 was
  10 cells carrying a parent run's executed outputs — the first build of this notebook had
  the same bug and the test caught it);
- static undefined-name scan in execution order via `symtable` (this is what caught the
  missing `import shutil` in run.009b build 1);
- **executes cells G1–G7** against a synthetic 40 k-bar world with a real collector gap,
  then asserts: the design matrix blocks equal the intended lag deltas; no long target
  survives a gap; targets are clipped to `TARGET_CLIP`; **no prediction exists outside a
  test block** (leak check); the bootstrap CI brackets its point estimate; required
  accuracy is monotone in fee; and the identity the economics rests on,
  `gross == (2·acc − 1)·E|move|`;
- negative control: the `max(LAGS) <= SEQ_LEN-1` assertion is confirmed to fire.

Two real defects were found and fixed this way: inherited executed outputs, and a
`symtable` predicate bug in the scan itself (`is_assigned()` is False for imports — the
correct predicate is `is_local()`).

## Known limitations

- **Per-fold trigger thresholds**, not the rolling-causal `τ` the 009f gate uses. Slightly
  optimistic; acceptable for a probe, must be replaced before any economic claim.
- **No exit simulation.** Net bp is hold-to-horizon minus a flat fee; no TP/SL, no maker
  fill model, no adverse-selection cost. A B/C pass here is an upper bound, not a P&L.
- **Single seed per head.** 009f used 3-seed ensembles; seed variance is unmeasured here.
- **`EMOVE_TRIG_REF` is reference-only** — the notebook recomputes `E|move|` from its own
  data and uses that for the requirement.

---

## v1 -> v2: the missing control (added 2026-09-22, after the v1 run)

**Defect in v1.** Criterion B compared directional accuracy to the *fee-breakeven* number
only, and never to a zero-skill baseline. `run009d.offline.md` §8 had already introduced the
blind benchmark and day-matched null for exactly this reason; this probe did not inherit
them. The omission is harmless at 90 s (blind benchmark +0.04 bp) and fatal at 1 h, where the
trigger set's own `P(up)` is **0.873** and always-long earns **+20.62 bp** — so criterion B,
which required 0.541, is passed by doing nothing. v1 printed three PASSes on that basis; all
three are drift, and 7 of 8 cells actually lose to always-long. See `gbm_probe.analysis.md` §3.

**Fix in v2** — new cell **G5b**, on the model's own trigger sets:

| reference | what it controls for |
|---|---|
| `blind` | always-long on the same bars — the drift the selection is exposed to |
| `signperm` | the model's own predicted signs **shuffled** across the same bars (4,000 draws): isolates sign *assignment*, holding the sign multiset fixed |
| `volmatch` | always-long on bars picked by **causal trailing realised vol** at the same rate — a model-free version of the same selection |

and a **revised criterion B**: `acc >= max(fee requirement, P(up) on the trigger set)`, plus
an explicit `excess over blind` column in bp.

**Known limit of `signperm`, documented so it is not over-read:** permutation holds the sign
multiset fixed but destroys the coupling between the model's sign and its predicted
*magnitude*. A model that goes long on the biggest bars of an up-drifting window beats its own
permutations with no directional skill. Six of eight v1 cells sit at the 97.7–100th percentile
of this null while losing to always-long. **Where the two controls disagree, `blind` is the
one that corresponds to a trading decision.**

`btc_lstm.probe.gbm.ipynb` (v1) is left **unmodified with its outputs** as the executed
record. v2 is unexecuted. The control can also be computed offline from
`gbm_probe_scores.npz` without re-running anything — that is how §3 of the analysis was
produced.

**Verification after the patch:** `132 PASS, 0 FAIL`, again run twice (stubbed and real
xgboost 3.4.1). The new assertions check that `excess == model − blind`, that the drift cell's
gross agrees with the economics cell to 1e-6, and that `P(up)` and the percentile are in
range. The `compile()` check earned its keep a second time here — the first build of G5b had
an unterminated string literal from a nested-quote escape, which `ast.parse` would also have
caught but which no amount of reading did.
