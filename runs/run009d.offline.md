# run.009d offline diagnostics — the fee-free edge is one 10-day regime and, mechanistically, volatility selection not direction; every up-side cell collapses to ≈0 without fold 3

*Run 2026-09-21, after run009d.analysis.md. Harness: `/tmp/opencode/{diag_lib.py,run_diag2.py,run_diag3.py,run_diag4.py}`, venv `/tmp/opencode/venv009`. Inputs: `data/60days_data.tar` (412 `out5.w5.*`, 1,186,560 rows) + `data/run009d_scores.npz`. No retraining, no GPU.*

**TLDR:** the saved `npz` alone cannot answer the economic questions (`close` is stored only at sample ends, there are no raw per-horizon regression predictions and no val labels). Rebuilding the epoch→close series from the tar **exactly** (max|diff| = 0.0 against `npz['close']`; identical row count, era start, cadence breaks) makes all of it reproducible offline, and the harness reproduces the notebook's published numbers to the digit as a second check. Three results change the picture:

1. **The val/test AP gap is fully explained — there is no bug.** Val base rates differ from test by up to **3.9×** (fold 0 up18: 0.25 % val vs 0.85 % test), and the notebook prints **per-seed** val AP while test AP is the **3-seed ensemble**. On a like-for-like ensemble basis and normalised by base rate, val and test agree — val is *better* in 5 of 8 fold-sides. No leakage, no miscalibration.
2. **Nothing clears the fee floor.** The fee-free gross edge per trigger never reaches the 10 bp taker round trip; the cells whose day-bootstrap CI excludes zero are +2.6 to +8.5 bp (up9/up12/up15 @0.01 %). No fold, no cell, no horizon is **net-positive** with taker fees. The single positive net number in the whole surface is up24 @0.01 % hold-to-horizon at **+0.18 bp** (n = 163) — indistinguishable from zero.
3. **The pooled edge is one 10-day window.** Per-fold gross EV @0.1 % **flips sign** (up18: +4.04, −3.20, −2.25, **+10.88** bp) with the sign consistent *across all six horizons within each fold* — a regime signature, not noise. **Remove fold 3 and every up-side cell collapses to ≈0** (up18 @0.1 %: +6.23 → **+0.03 bp**; up15 @0.01 %: +8.48 → **+1.31 bp, CI [−4.94, +4.84]**). And within folds 2 and 3 the **up and dn heads mirror each other** (fold 3: up18 +10.88 / dn18 −13.49; fold 2: up18 −2.25 / dn18 +4.52). ~~the tell for directional trend exposure~~ — **Part 2 §8–10 shows this is not trend exposure** (fold 3's net drift was +0.04 bp) but **volatility selection with a weak directional tilt**, and the drift-cancelling spread is ≤ 0 in fold 3.

Against the pre-registered decision rule in `run009d.analysis.md` (*no cell with gross ≥ 4 bp and per-fold ≥ 0 in ≥ 3/4 folds → design space dead at current signal strength*): the best case is **2/4 folds ≥ 0**. **The design space is dead at the current signal strength, and the only lever that remains untested is training / model selection.**

> **Part 2 (below) revises the mechanism but strengthens the verdict.** Fold 3 was not a trend (its net drift was +0.04 bp). The real mechanism is **volatility selection with a weak directional tilt**: every head raises its *opposite* first-touch label by 6.8–39.8× base rate, trigger |move| is 3.3–5.3× the sample average, the dn heads' own/opposite lift ratio is ≈ 1.0, and the **market-neutral spread is ≤ 0 in fold 3 and includes zero in all twelve pooled horizon × rate cells.** New recommended objective: the own/opposite lift ratio or the market-neutral spread — *not* own-label AP, which rewards the artifact.

---

## Method and validation

The notebook indexes forward paths **positionally** (`close[e + k]`) on the post-dropna array, and its own validity rule asserts row `e+h` is exactly `h·WINDOW_SEC` ahead. So an epoch→close lookup at `t + k·5 s` is equivalent, and requires replicating *one* column instead of 58 features.

| check | result |
|---|---|
| rows / cadence breaks / era start / end | 1,186,560 · 127 breaks (largest 1.7 h) · 2026-07-13 20:58:40 · 2026-09-20 15:50:45 — **identical to the notebook's printed output** |
| `close_at(npz['dt_s'])` vs `npz['close']` | n = 666,028, **max\|diff\| = 0.000000e+00**, 0 mismatches |
| reproduce published h18 @0.1 % ens taker | up18 n=1,063 hit 31.14 % lift 25.63 **sim −5.62 CI [−10.25, −3.56]**; dn18 n=1,418 hit 18.55 % lift 17.04 **sim −10.45 CI [−13.01, −6.68]** — **exact** |
| reproduce per-seed val AP_up18, fold 3 | rebuilt 0.0729 / 0.0791 / 0.0673 vs notebook-printed 0.0729 / 0.0791 / 0.0673 — **exact** |

Two independent exact reproductions (test sim + val AP) plus a bit-exact close series. Everything below is computed the same way.

---

## 1. The val/test AP gap is explained — not a bug

| fold | side | val base % | val AP (ens) | val lift | test base % | test AP (ens) | test lift |
|---|---|---|---|---|---|---|---|
| 0 | up | 0.25 | 0.0589 | 23.85 | 0.85 | 0.0760 | 8.98 |
| 0 | dn | 0.13 | 0.0395 | 29.39 | 0.53 | 0.0651 | 12.27 |
| 1 | up | 1.89 | 0.0930 | 4.93 | 2.08 | 0.0653 | 3.13 |
| 1 | dn | 0.99 | 0.1277 | 12.90 | 2.16 | 0.1087 | 5.04 |
| 2 | up | 0.87 | 0.0617 | 7.13 | 0.85 | 0.0619 | 7.30 |
| 2 | dn | 1.15 | 0.0961 | 8.39 | 0.73 | 0.0952 | 13.11 |
| 3 | up | 0.68 | 0.0864 | 12.64 | 1.09 | 0.2207 | 20.32 |
| 3 | dn | 0.66 | 0.1333 | 20.25 | 0.94 | 0.1853 | 19.61 |

Two mechanisms, both benign:

- **Prevalence.** Fold 0's test base rate is **3.4× / 3.9×** its val base rate. AP is monotone in base rate at fixed ranking quality, so val AP must be mechanically lower there. Fold 1 dn is 2.18×; fold 2 dn is *inverted* (val 1.15 % > test 0.73 %).
- **Per-seed vs ensemble.** The notebook's training loop prints per-seed val AP (`ap(...)` on one seed's `vp`); the cell-13 test table computes `ap(...)` on the 3-seed mean. Ensembling lifts AP. The `0.073 → 0.207` scare compares a single model to an ensemble.

Normalised by base rate, val and test agree, and val is *stronger* in 5 of 8 fold-sides (23.85 vs 8.98 lift in fold 0 up). The one large residual is fold 3 up (12.64 val vs 20.32 test) — the test window was simply easier for the up-head, which is exactly the fold-3 luck the economics independently expose in §3–4.

**Consequence for run009d.analysis.md:** the epoch-1 selection is *still* an interpretation problem (the model is undertrained and the stop metric cannot see past the selected checkpoint), but it is **not** evidence of a bug, a leak, or a mis-calibrated val metric. Severity of that finding drops from "compromises interpretation" to "optimisation is wasteful and the ceiling is untested."

## 2. Fee-free gross surface — nothing clears the fee floor

`gross_hold` = mean signed forward return over triggers, **no fees**, hold to horizon. This is the ceiling a perfect execution layer could capture. Floors: taker RT 10 bp, taker-in/maker-out 7 bp, full-maker 4 bp.

| head | rate | n | /day | hit % | lift | **GROSS bp** | 95 % CI | net taker |
|---|---|---|---|---|---|---|---|---|
| up6 | 0.01 % | 242 | 5.8 | 16.94 | 35.96 | +0.14 | [−5.74, +3.36] | −9.02 |
| up6 | 0.10 % | 1,171 | 27.9 | 16.91 | 35.89 | +1.83 | [+0.01, +2.87] | −8.50 |
| up9 | 0.01 % | 294 | 7.0 | 22.79 | 24.63 | **+2.60** | [+1.08, +3.60] | −7.07 |
| up9 | 0.10 % | 1,164 | 27.7 | 26.63 | 28.79 | **+3.23** | [+0.07, +4.60] | −7.50 |
| up12 | 0.01 % | 292 | 7.0 | 22.26 | 35.56 | **+4.59** | [+1.47, +7.82] | −5.20 |
| up12 | 0.10 % | 1,210 | 28.8 | 22.98 | 36.70 | +3.80 | [−0.44, +5.71] | −6.90 |
| up15 | 0.01 % | 313 | 7.5 | 32.59 | 35.89 | **+8.48** | [+1.40, +11.04] | −2.58 |
| up15 | 0.10 % | 1,099 | 26.2 | 27.84 | 30.66 | **+5.29** | [+0.04, +7.83] | −5.99 |
| up18 | 0.01 % | 350 | 8.3 | 38.57 | 31.75 | +9.60 | [−0.70, +12.79] | −3.01 |
| up18 | 0.10 % | 1,063 | 25.3 | 31.14 | 25.63 | +6.23 | [−0.38, +9.22] | −5.62 |
| up24 | 0.01 % | 163 | 3.9 | 39.88 | 21.15 | +10.18 | [−2.14, +15.87] | −2.21 |
| up24 | 0.10 % | 946 | 22.5 | 34.88 | 18.50 | +7.63 | [−0.72, +11.55] | −5.04 |
| dn6 | 0.10 % | 1,104 | 26.3 | 14.76 | 33.12 | −2.12 | [−4.57, +0.14] | −11.60 |
| dn9 | 0.10 % | 1,238 | 29.5 | 17.61 | 20.50 | −0.87 | [−2.35, +1.13] | −11.15 |
| dn12 | 0.10 % | 1,207 | 28.7 | 15.16 | 26.15 | **−3.46** | [−5.72, −0.94] | −12.29 |
| dn15 | 0.10 % | 1,228 | 29.2 | 18.16 | 21.93 | −1.92 | [−4.20, +1.57] | −11.66 |
| dn18 | 0.10 % | 1,418 | 33.8 | 18.55 | 17.04 | −0.62 | [−4.49, +3.91] | −10.45 |
| dn24 | 0.10 % | 1,748 | 41.6 | 19.62 | 11.64 | −1.92 | [−7.04, +2.51] | −10.90 |

- **Not one cell reaches +10 bp.** The best point estimates (+9.60 up18, +10.18 up24, both @0.01 %) have CIs spanning zero on n = 350 / 163.
- The statistically robust cells are all up-side and all small: **+2.60 / +3.23 / +4.59 / +5.29 / +8.48 bp**.
- **The dn heads are gross-negative at every horizon and every rate.** dn12 @0.1 % is *significantly* negative (−3.46, CI excludes 0): those heads destroy value before fees.
- At the 1 % rate everything collapses to ≈0: the triggers are then near the base rate, and the gross edge vanishes — the signal lives only in the extreme tail.
- **Net taker is negative in all 36 cells.** The only net-positive number anywhere is up24 @0.01 % hold-to-horizon at **+0.18 bp** — noise.

### Exit structure (h18 @0.1 %, net bps, real fees)

| variant | up18 gross | up18 net | dn18 gross | dn18 net |
|---|---|---|---|---|
| bracket 0.5·θ | +0.89 | −7.68 | −1.26 | −10.14 |
| bracket 1.0·θ (gated) | +3.45 | −5.62 | −1.00 | −10.45 |
| bracket 1.5·θ | +4.97 | **−4.43** | −1.09 | −10.82 |
| tp 1.0 / sl 2.0 | +3.37 | −5.69 | −1.49 | −10.93 |
| tp 1.5 / sl 0.5 | +4.72 | −4.70 | −0.49 | −10.24 |
| tp 1.0 / no SL | +3.35 | −5.71 | −1.43 | −10.86 |

Confirms the earlier read: the symmetric bracket costs the up side **~1.8 bp vs hold-to-horizon** (gross +3.45 vs +6.23 from §2 — the difference being bracket exits at ±θ rather than at the horizon), the widest bracket is least bad, and **every dn variant has negative gross** — no exit structure rescues a head with no gross edge.

## 3. Per-fold structure — sign flips, and up/dn mirror each other

Gross bp by fold, @0.1 % ens:

| head | fold 0 (08-10→08-20) | fold 1 (08-20→08-31) | fold 2 (08-31→09-10) | fold 3 (09-10→09-20) |
|---|---|---|---|---|
| up6 | +2.36 | +1.27 | −2.37 | +2.55 |
| up9 | +4.06 | −3.16 | −0.87 | +5.19 |
| up12 | +3.18 | −3.43 | −2.57 | +6.83 |
| up15 | +3.99 | −2.41 | −1.69 | +9.07 |
| up18 | +4.04 | −3.20 | −2.25 | **+10.88** |
| up24 | +4.94 | −3.27 | −2.30 | **+13.86** |
| dn6 | −4.02 | −1.92 | +3.34 | −1.60 |
| dn9 | −0.60 | −0.70 | +6.79 | −4.45 |
| dn12 | −6.58 | −1.80 | +5.18 | −7.21 |
| dn15 | −0.72 | −1.77 | +11.97 | −8.29 |
| dn18 | +1.66 | +0.35 | +4.52 | **−13.49** |
| dn24 | −2.65 | −0.93 | +3.37 | **−13.56** |

Two things are unmistakable:

1. **The sign is consistent across all six horizons within each fold** (up: +,+ ,−,−,−,− ,+,+,+,+,+,+ per fold 0,1,2,3). That is a **regime** signature — a per-fold market state — not per-cell noise.
2. **The up and dn heads mirror each other in folds 2 and 3.** Fold 2: up negative, dn strongly positive. Fold 3: up strongly positive, dn strongly negative. If the heads carried idiosyncratic microstructure alpha (which barrier is touched first, independent of drift) there is no reason for the two sides to invert together.

~~**Fold 3 was a 10.5-day up-trend.** The up-heads' apparent edge there is directional exposure to that trend — the first-touch label converts a trending regime into a high hit rate (up18 fold 3: hit 45.47 % at 41.86× lift). Fold 2 is the same mechanism in reverse.~~ **RETRACTED — see Part 2 §8.** Fold 3's net drift was **+0.04 bp** (blind benchmark); it was not a trend. The mirroring is real but the mechanism is **volatility selection**, not trend exposure: in fold 3 the up-triggered bars moved +10.88 bp and the dn-triggered bars moved **+13.49 bp** — *both* up. The two arms are two directional bets on high-magnitude bars, and the market-neutral spread between them is ≤ 0 (§8). The correct observation from the mirror is only that **the run's selectivity was never separated from direction**, which Part 2 then did.

## 4. Leave-one-fold-out — fold 3 carries everything

`gross bp (n)` with each fold removed, @0.1 % and @0.01 %:

| head | rate | pooled | wo fold 0 | wo fold 1 | wo fold 2 | **wo fold 3** | folds ≥ 0 |
|---|---|---|---|---|---|---|---|
| up6 | 0.10 % | +1.83 | +1.70 | +1.95 | +2.26 | +0.98 | 3/4 |
| up9 | 0.10 % | +3.23 | +3.06 | +4.31 | +3.65 | +0.42 | 2/4 |
| up12 | 0.10 % | +3.80 | +3.94 | +5.00 | +4.48 | −0.36 | 2/4 |
| up15 | 0.10 % | +5.29 | +5.58 | +6.83 | +5.96 | +0.41 | 2/4 |
| up18 | 0.10 % | +6.23 | +6.71 | +8.20 | +6.91 | **+0.03** | 2/4 |
| up24 | 0.10 % | +7.63 | +8.27 | +10.25 | +8.44 | **+0.30** | 2/4 |
| up15 | 0.01 % | +8.48 | +10.28 | +8.98 | +8.48 | **+1.31** | 2/4 |
| up18 | 0.01 % | +9.60 | +11.29 | +10.54 | +9.60 | **+0.38** | 2/4 |
| up24 | 0.01 % | +10.18 | +13.65 | +12.23 | +10.71 | **+0.88** | 2/4 |

**Every up-side cell collapses to ≈0 (or negative) when fold 3 is dropped**, while dropping any other fold leaves the pooled number essentially intact. Removing fold 3 costs only ~half the triggers (up18 @0.1 %: 57.1 % of its triggers are in fold 3) but 100 % of the edge. Note also that fold 0 alone is positive — so this is not "one fold, one direction": it is **+4.04 / −3.20 / −2.25 / +10.88**, i.e. two positive folds and two negative folds, with fold 3 dominating the magnitude.

**The best pooled cell does not survive either.** up18 @0.01 % per fold, net taker: fold 0 n = 70 **−6.91 bp**; fold 1 n = 15 **−19.67 bp**; fold 2 n = 0 (no triggers); fold 3 n = 265 **−1.04 bp** (gross +8.96). **No fold is net-positive.** The single positive net number in the entire surface remains up24 @0.01 % hold-to-horizon **+0.18 bp** on n = 163.

---

## 5. Verdict against the pre-registered decision rule

`run009d.analysis.md` set: *no cell with gross ≥ 4 bp **and** per-fold ≥ 0 in ≥ 3/4 folds → the design space is dead at the current signal strength; the remaining lever is training.*

- Best per-fold-positive count on any cell: **2/4** (and only because fold 0 is mildly positive).
- Highest gross with a CI excluding zero: **+8.48 bp** (up15 @0.01 %) — below the 10 bp taker floor, and **+1.31 bp [−4.94, +4.84]** without fold 3.
- No cell, in any fold, is net-positive with taker fees. The maker route's ~5 bp selection cost cancels its ~5 bp fee saving.

**⇒ Design space dead at the current signal strength. The lever is training and model selection, not features, data or execution.**

This supersedes the framing in `run009d.analysis.md`, which read the pooled numbers as "a small real edge that fees erase". The pooled numbers are **one regime**. ~~The correct statement: outside fold 3 there is no measurable fee-free edge, and inside fold 3 the up/dn mirror shows the edge is trend exposure, not alpha.~~ **Corrected in Part 2 §8–10:** fold 3 was *not* a trend (its own net drift was +0.04 bp) and the market-neutral spread is ≤ 0 there. The mechanism is **volatility selection with a weak directional tilt** — see Part 2.

## 6. Caveats

- **Fold 3 is one observation.** It is also the newest, largest-train fold and the highest-volatility period. A persistent "high volatility = up-heads work" regime is *possible* and would be a real, tradeable, conditional edge. One 10.5-day window cannot distinguish that from a single lucky trend. This is falsifiable only with new data (the out-of-window test).
- **Up/dn mirroring is inferred from four folds**, two of which give a clean mirror. Four points is thin for a regime claim; the mechanism is nonetheless unambiguous in direction.
- **~~No drift control was run.~~** **Done — Part 2 §8.** The market-neutral variant was run: all twelve pooled spreads include zero, fold 3's is ≤ 0, and the day-matched random-entry null puts the model at the 100th/0th percentile of a ±1.5 bp distribution. The remaining gap is that this control is offline only — it is not in the notebook, so future runs will repeat the omission unless it is added to the gate.
- Fold 0's high per-seed IC spread (±0.0808) and fold 1's val/test base-rate mismatch remain; neither changes the economic verdict.
- `theta` was held at the notebook's per-horizon map for the label, and varied only for the exit bracket. Label-θ sensitivity was not swept (the gross-hold column is θ-independent, so the ceiling is unaffected).

## 7. What this means for the next run

1. **Add the drift/market-neutral control to the notebook gate** (Part 2 §8–9 gives the three tests: market-neutral spread `S`, day-matched random-entry null, blind benchmark). The current heads' P&L is not separated from direction, and criterion B (own-label lift ≥ 3×) is **passed by the volatility artifact** — both barriers are raised ~15–25× on the selected bars. Offline it is already done; it must live in the notebook.
2. **Change the model-selection objective** away from own-label AP toward the **own/opposite lift ratio** or the market-neutral spread. The current objective explicitly rewards the artifact.
3. **Normalise the labels by realised volatility** before constructing the first-touch barriers (vol-scaled θ, or residualise the target on contemporaneous range) so "large move" stops being the dominant signal.
4. **Then** the training experiment — the only untested lever: fixed 10–15 epochs, select on the corrected objective, plus a `TRAIN_ARM='regularized'` arm. The model is selected at epoch 1 in 11/12 seed-folds, so the signal ceiling is unmeasured. **Note:** this is now lower priority than 1–3, because until the objective is corrected the retrain will optimise the artifact.
5. **Do not run** more feature prunes, exit-grid sweeps, latency stresses or event-feature blocks. All measured dead (this run + run.012's 0.49–0.51 fill-timing AUC).
6. **Out-of-window test** on post-2026-09-20 data is the highest-value *falsifier*: it would say whether the fold-3 regime recurs. Needs a fresh tar.

### Harness (reusable)

| file | purpose |
|---|---|
| `/tmp/opencode/build_close_map.py` | tar → epoch→close map (117 s; `/tmp/opencode/close_map.npz`, 3.6 MB) |
| `/tmp/opencode/diag_lib.py` | verbatim ports of `sim_exit` / `rolling_triggers` / `boot_ci_mean` / `first_touch`, plus the alignment + reproduction checks |
| `/tmp/opencode/run_diag2.py` | val base rates, gross-EV surface, exit variants, per-fold decomposition |
| `/tmp/opencode/run_diag3.py` | val-AP per-seed-vs-ensemble, leave-one-fold-out, trigger fold composition |
| `/tmp/opencode/run_diag4.py` | full leave-one-fold-out table, per-fold gross by head, per-fold net |
| `/tmp/opencode/run_diag5.py` | Part 2 §8 — spread decomposition `S`/`D`, blind benchmark, day-matched random-entry null, per-day drift regression |
| `/tmp/opencode/run_diag6.py` | Part 2 §9 — own vs opposite label lift, \|fwd\| ratios, trigger overlap (Jaccard), P(either barrier) |
| `/tmp/opencode/run_diag7.py` | Part 2 §9A/9B — per-fold spread `S`, directional-content metric (own lift ÷ opposite lift) |
| `/tmp/opencode/sklearn_lite.py` | AP matching `sklearn.average_precision_score` (notebook's `ap()`) |

**Notebook patch to make this permanent** (currently the npz is insufficient): save forward closes `close[e+1..e+h]`, raw per-horizon `test_pred`, `val_labels`, `val_score`. Then every diagnostic above runs from the npz alone with no tar.

---

# Part 2 (same day) — drift control and the direction-vs-volatility test

*Added after Stage 7–9. `run_diag5.py`, `run_diag6.py`, `run_diag7.py`. This part **refutes** the "fold 3 was a trend" mechanism hypothesised in Part 1 §3 → the real mechanism is volatility selection with a weak directional tilt, and it is stronger evidence than the trend story.*

## 8. Drift control: fold 3 was **not** a trend

The Part-1 hypothesis was that fold 3's up-side edge is short-horizon trend exposure. Two tests kill it outright.

**Blind benchmark — the fold's own unconditional mean move:**

| horizon | fold 3 blind long | fold 3 blind short |
|---|---|---|
| h15 @0.1 % | +0.03 bp | −0.03 |
| h18 @0.1 % | +0.04 | −0.04 |
| h24 @0.1 % | +0.06 | −0.06 |

**Fold 3 had essentially zero net drift.** The up-heads earned +10.88 bp (h18) in a fold whose average bar moved +0.04 bp. This is not trend-following.

**Day-matched random-entry null** (same trigger count, same day composition, uniformly random bars, 2 000 draws):

| fold | side | n | model | null mean | null p2.5 | null p97.5 | model percentile |
|---|---|---|---|---|---|---|---|
| 3 | up | 607 | **+10.88** | +0.34 | −0.32 | +0.99 | **100.0 %** |
| 3 | dn | 139 | **−13.49** | +0.14 | −1.17 | +1.44 | **0.0 %** |
| 2 | up | 79 | −2.25 | +0.29 | −1.17 | +1.82 | 0.2 % |
| 2 | dn | 60 | +4.52 | +0.08 | −1.60 | +1.88 | 99.9 % |
| 0 | up | 193 | +4.04 | +0.26 | −0.79 | +1.71 | 100.0 % |
| 1 | up | 184 | −3.20 | +0.09 | −1.33 | +1.46 | 0.0 % |

The model sits at the **100th / 0th percentile** of a drift-matched null in every fold — its triggers are genuinely extreme bars, not the fold's average. But the null also shows the bar is ~±1.5 bp wide, and the *sign* flips fold to fold.

**Market-neutral spread** `S = mean(+fwd | up-trigger) − mean(+fwd | dn-trigger)` — long the up-head's triggers, short the dn-head's. This cancels any common drift; genuine directional skill gives `S > 0`, direction-agnostic volatility selection gives `S ≈ 0`:

| head | fold 0 | fold 1 | fold 2 | **fold 3** | pooled | pooled 95 % CI |
|---|---|---|---|---|---|---|
| h15 @0.1 % | +3.27 | −4.18 | +10.27 | **+0.78** | +3.38 | [−1.92, +4.90] |
| h18 @0.1 % | +5.70 | −2.84 | +2.27 | **−2.61** | +5.60 | [−1.43, +5.97] |
| h24 @0.1 % | +2.29 | −4.20 | +1.07 | **+0.30** | +5.71 | [−2.42, +5.35] |
| h18 @0.01 % | −3.77 | −13.40 | — | **−17.76** | +6.76 | [−3.14, +10.11] |
| h24 @0.01 % | −1.12 | −4.44 | −14.45 | **−20.34** | +9.82 | [−3.74, +10.73] |

> **CORRECTION (2026-09-21, after run.009e; re-derived from `run009e_scores.npz`).** The `pooled 95 % CI` column in the table above is **not** a confidence interval for `S`. It is `boot_ci_mean(concat(+fwd|up, −fwd|dn))` — the day-bootstrap interval of the **equal-per-trade combined mean**, whereas `S = mean(+fwd|up) − mean(+fwd|dn)` weights the two legs **equally as means**. The two coincide only when the legs are the same size. At h18 @0.1 %: n_up = 1,063, n_dn = 1,418, `S = +5.61` but the combined mean is **+2.31** — the interval is centred on the latter, not on `S`. Measured against the corrected union-of-days bootstrap on the same data: printed `[−1.43, +5.97]` (width 7.40) vs corrected **`[−1.80, +10.88]` (width 12.67)** — the printed interval is **1.71×** too narrow. The **point estimates of `S` are correct**; the intervals are not, and they over- or under-state `|S|` depending on the relative leg sizes.
>
> The qualitative conclusions do not change, because they rest on point estimates: `S` is ≤ 0 in fold 3, and `S` is below the fee floor everywhere. The **significance** statements do change. Re-deriving run.009e's per-horizon intervals (table below) cuts its "significantly negative" claim from four horizons to **one** (h12) plus **fold 0**. A correct interval resamples the **union** of days so days shared by both legs stay paired and subtracts the two leg means — implemented as `boot_ci_spread` in run.009f. Two bugs were found in the process: the interval was centred on the wrong statistic, and run.009f's first build passed a pre-negated dn leg (yielding `mean_up + mean_dn`); both are fixed, and the suite now asserts the interval **brackets the point estimate** and that the dn leg is not negated. Details in `runs/run009e.analysis.md`.

**Two decisive facts:**

1. **All twelve pooled `S` values have day-bootstrap CIs that include zero** (see also the full horizon×rate grid in Part 1 §7A addition below — none excludes 0). There is **no statistically supported market-neutral edge at any horizon or trigger rate.**
2. **Fold 3's `S` is ≈0 or negative** (+0.78, −2.61, +0.30, −17.76, −20.34). The fold that produces the entire headline gross edge contributes **nothing** to the drift-cancelling spread. Its +10.88 / −13.49 pair is the two arms of a directional bet, and the neutral combination of that pair is ≤ 0.

The positive pooled `S` values (+5.6 to +9.8 bp at long horizons) come from **folds 0 and 2**, not fold 3 — and carry CIs spanning zero.

## 9. The mechanism: volatility selection, not direction

**The decisive test.** If a head predicts *direction*, its triggers must raise its **own** first-touch label and leave the **opposite** label near base rate. If it merely locates *volatility*, **both** labels are elevated on its triggers.

| head @0.1 % ens | own % | own lift | **opposite %** | **opposite lift** | \|fwd\| trig | \|fwd\| all |
|---|---|---|---|---|---|---|---|
| up6 | 16.91 | 35.89 | 10.93 | **24.52** | 9.5 | 2.2 |
| dn6 | 14.76 | 33.12 | 18.75 | **39.80** | 11.8 | 2.2 |
| up9 | 26.63 | 28.79 | 14.69 | 17.10 | 12.0 | 2.8 |
| dn9 | 17.61 | 20.50 | 20.11 | **21.74** | 11.9 | 2.8 |
| up12 | 22.98 | 36.70 | 11.65 | 20.10 | 13.9 | 3.3 |
| dn12 | 15.16 | 26.15 | 19.14 | **30.57** | 15.2 | 3.3 |
| up15 | 27.84 | 30.66 | 12.47 | 15.05 | 15.6 | 3.7 |
| dn15 | 18.16 | 21.93 | 19.79 | 21.79 | 15.4 | 3.7 |
| **up18** | **31.14** | **25.63** | **12.61** | **11.58** | 16.6 | 4.0 |
| **dn18** | 18.55 | 17.04 | 17.84 | **14.69** | 14.7 | 4.0 |
| up24 | 34.88 | 18.50 | 11.52 | 6.83 | 17.1 | 4.7 |
| dn24 | 19.62 | 11.64 | 20.65 | 10.95 | 15.3 | 4.7 |

- **Every head raises the opposite label by 6.8–39.8× base.** For the dn heads at h6 / h9 / h12 the opposite lift is *higher* than the own lift (39.80 vs 33.12, 21.74 vs 20.50, 30.57 vs 26.15).
- **Triggers are extreme-magnitude bars:** mean \|forward move\| is **3.3–5.3× the sample average**.
- **The celebrated "hit-rate lift" is largely a volatility artifact.** P(*either* first-touch label fires) goes from **2.30 % on all bars to 43.74 % on up18 triggers** (h18) and 0.92 % → 27.84 % (h6). On a high-volatility bar one barrier *must* be touched first, so both conditional label rates rise together — and the own-label lift of 25× is measured against a 1.21 % base rate that the volatility alone explains most of.

**Directional-content metric — own-label lift ÷ opposite-label lift** (drift-free, 1.0 = no direction, only volatility):

| head | fold 0 | fold 1 | fold 2 | fold 3 | pooled |
|---|---|---|---|---|---|
| up6 | — | 1.36 | 1.02 | 1.38 | **1.46** |
| up12 | 7.45 | 0.40 | 1.16 | 2.02 | **1.83** |
| up15 | 2.70 | 0.61 | 1.10 | 2.28 | **2.04** |
| up18 | 3.01 | 0.71 | 0.95 | 2.53 | **2.21** |
| up24 | — | 0.33 | 1.57 | 2.93 | **2.71** |
| dn6 | 0.77 | 0.72 | 0.96 | 1.11 | **0.83** |
| dn12 | 0.61 | 0.92 | 1.44 | 0.95 | **0.86** |
| dn18 | 0.83 | 1.28 | 1.71 | 0.82 | **1.16** |
| dn24 | 1.47 | 0.95 | 1.84 | 0.88 | **1.06** |

This metric tracks the fold-level P&L sign closely — up18: fold 0 ratio 3.01 → gross +4.04; fold 1 ratio 0.71 → −3.20; fold 2 ratio 0.95 → −2.25; fold 3 ratio 2.53 → +10.88. **Recommended as a standing diagnostic: it is label-based, drift-free, and needs no P&L simulation.**

Reading it:

- **The dn heads carry no directional content at any horizon** (pooled 0.83–1.16, mean ≈ 1.0). That fully explains why dn gross EV is negative at every horizon and why dn12 is *significantly* negative.
- **The up heads carry a weak but real directional tilt** (pooled 1.46 → 2.71, rising with horizon), present in folds 0 and 3, absent or inverted in folds 1–2.
- On up18's triggers the tilt is quantifiable: against a direction-neutral bar with the same 43.74 % either-touch rate, the own:opposite split should be ≈ 22.8 % : 20.9 %; observed **31.14 % : 12.61 %** — a genuine **≈ ±8 pp** tilt toward the own barrier. It is simply too small to survive a heavy-tailed, near-symmetric return distribution, and it does not survive drift cancellation (§8).

**The heads are not the same detector.** Up/dn trigger overlap (Jaccard) is only 0.003–0.053 (h18: 8 shared bars out of ~2 400). They locate different volatile bars with opposite tilts — consistent with the mirror P&L.

## 10. Revised verdict

Part 1's conclusion stands and is now mechanistically grounded:

- **No market-neutral edge exists with statistical support.** All twelve pooled spreads include zero; fold 3's is ≤ 0.
- **The headline "25× hit-rate lift" is mostly volatility detection.** Both barriers are raised on the selected bars; the own-labelled lift is inflated by the same mechanism that raises the opposite label.
- **The dn heads have no directional content at all** (own/opp ≈ 1.0) — their negative economics is not an execution problem, it is a no-signal problem.
- **The up heads have a real but weak directional tilt** worth ~+6 bp gross at h18, concentrated in fold 3, absent in folds 1–2, and **worth ≈ 0 once drift is cancelled.** It does not clear any fee floor.
- ⇒ **The design space is dead at the current signal strength** (best per-fold-positive count 2/4). The only untested lever remains **training and model selection** — and the target to optimise is now explicit: the **market-neutral spread `S`** and the **own/opposite lift ratio**, not the own-label AP the model is currently selected on.

**Recommended notebook changes this implies:**

1. Select the model on **own/opposite lift ratio** or on the market-neutral spread, not on own-label AP — the current objective rewards volatility detection, which is exactly the artifact.
2. **Normalise the labels by realised volatility** before building the first-touch barriers (a vol-scaled θ, or residualise the target on contemporaneous mid-price range) so that "big move" stops being the dominant signal.
3. Report `S`, the own/opposite ratio and the blind benchmark in the gate, alongside the existing criteria — criterion B (own-label lift ≥ 3×) is currently passed *by the artifact*.
