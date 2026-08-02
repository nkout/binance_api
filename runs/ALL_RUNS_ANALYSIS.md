# BTC Orderbook LSTM — Cross-Run Analysis (run.001 → run.010)

*Synthesized 2026-08-02 from project memory. Numbers are as reported by each
run's executed notebook; where a result was later shown non-robust (e.g. the
EMA cross), that is called out inline.*

## Goal & setup

Predict short-horizon BTC direction from Binance USDT-M futures orderbook
microstructure (15s bars). Experiments live in `runs/btc_lstm.run.NNN.ipynb`,
executed on Google Colab T4 (12 GB RAM / 12 GB GPU). Data arrives as
`btc_data.tar.xz` via Google Drive. The user's stated goal is **selective**:
"happy if most is 'no idea' and some good guesses so I find trade
opportunities" — i.e. abstaining prediction that concentrates on rare, tradable
moves, not blanket accuracy.

## The economics floor (why most targets are untradeable)

- Maker fee 0.02%/side → round trip ~$30–40 at BTC $75–110k.
- Taker round trip ≈ 10 bps; full-maker RT ≈ 4 bps; maker-in/taker-out ≈ 7 bps.
- A 20 bp take-profit pays θ − 7 = **+13 bp** on a hit; the average non-hit
  costs ≈ **−18 bp** (SL −30, time-stop −10 + drift) → **breakeven hit ≈ 58.5%**
  at taker, ≈ **49%** at full-maker. Every run is ultimately measured against
  this arithmetic.
- 2-min/$50 targets are below the fee floor by construction — hence the move to
  a selective, tail-focused objective and 30–90s "opportunity" labels.

## Data eras (the single most important confound)

| Era | Collector | Prefix | Adds | Status in tar |
|-----|-----------|--------|------|---------------|
| v1 | `binance_live_orderbook.py` (~1 yr) | `out.` | book only, no sub-bar trades | present |
| v2 | `..._v2.py` (~15 d) | `out2` | 10 sub-bar trade features | **missing from tar in runs 003–008** |
| v3 | `..._v3.py` (2026-07-07) | `out3` | Cont OFI, add/cancel flow, microprice dev, within-bar vol/RV/flips, trade-size tails, liq counts, `schema_version` | accumulating |
| schema-4 | same collector | `out4` | wide flow band, book walls, trade bursts, ETH lead-lag | accumulating |
| schema-5 | v4/v4.1 collector | `out5` | REST-polled mark/funding/spread (markPrice ws is dead) | **first real run 2026-07-19** |

**Runs 003–008 all executed in "v1-compat mode"** — the schema-3/4 columns were
absent from the tar (only 1–2 of 29 v3 features survived), so six consecutive
runs tested the *v1 feature set* under evolving objectives, NOT the microstructure
features they were designed for. This is the dominant caveat across the middle of
the project. Schema-5 data (out5) first flowed end-to-end on 2026-07-19.

## Run-by-run

| Run | Design in one line | Gate / verdict |
|-----|--------------------|----------------|
| 001 | Classification LSTM, $50 fixed threshold, h=8 | No signal (test 63% vs 74% dummy, WR 50.1%); backtest flawed |
| 002 | 7.4M-param LSTM(512×3)+attention, 3-class $50/2-min | **Worse than dummy** (lift −6.9%); 35× params did worse ⇒ capacity is not the constraint |
| 003 | Regression + directional-weighted training, XGB baseline | No edge (IC≈0 both model families) ⇒ **features, not capacity** |
| 004 | +12 offline features (regime/seasonality), h240 | h240 rejected; but new features added real short-h signal (IC_h8 +0.0168, daily t=+4.8) — kept as base layer |
| 005 | EMA-cross overlay + h8 LSTM force-exit (user's idea) | **Gate FAILED** — h8 score has no tail concentration; overlay churns fees |
| 006 | Selective/opportunity BCE heads, P(MFE≥θ), h∈{8,40,240} | **Gate FAILED** — "lift good, net ≤ 0": heads DO concentrate hits but the endpoint move reverts by bar 40; exit rule untested |
| 007 | Short h=2..6 (30–90s), TP/SL-at-θ exit | **Gate FAILED** (A,C fail; B passes) — strongest ranking yet (h6 lift 9.4×, IC_h2 t=+12.9) but sim net ≤ 0; TP/SL exit adds ~nothing at short h; remaining lever = **entry price** |
| 008 (v1) | Maker-entry sim + entry-delay stress | **Gate FAILED** — maker entry WORSE than taker via **adverse selection** (hit_f 13% vs hit_m 48%); latency not the issue. All v1 execution levers now dead |
| 008.v4data | First real schema-5 run (7 d, 29/29 v3 feats) | **Gate FAILED** — no boost, but can't measure one (7 d ≪ 45 d, low-vol week, no ablation). "Wait for volume," not "v3 doesn't help" |
| 008.v4data_try_02 | Same, 20.8 d / 13 test days (3× data) | **Gate FAILED** — but regression IC now +ve & daily-significant to h8 (t +2.1..+3.9): first real schema-5 signal. Killed by adverse selection + **2-of-4-fold regime concentration** |
| 009 | Triple-barrier **first-touch** labels | **NULL** — gate byte-identical to try_02; labels flip <1% (only at h8). Adverse selection is fill-mechanics, not a label artifact |
| 010 | **5-second bars** (run.009 at 5s, wall-clock identical) | **Design PASS / gate FAIL** — 30s daily-IC t +4.07>3.87, 90s +3.35>2.10, fold-concentration far better (42%/68% vs 97%) ⇒ **promote w5**; gate still fails via same adverse selection, economics unmoved by finer bars |

## Cross-cutting findings

1. **Capacity is not the constraint** (cleared twice). run.002's 7.4M-param
   attention model did *worse* than an always-flat dummy; run.003's tiny LSTM and
   an XGBoost baseline both landed at IC≈0 on the same data. Compute headroom
   should go to multi-seed ensembles and iteration speed, not bigger models.

2. **Features are the constraint.** The only things that ever moved the needle
   were *new inputs*: run.004's regime/seasonality features (IC_h8 daily t=+4.8),
   and — finally — schema-5 volume in try_02 (regression IC significant to h8).

3. **The ranking signal is real; the economics are the problem.** From run.006 on,
   the opportunity heads reliably concentrate hits (lift up to 9–17×) and beat the
   regression-threshold control. But *net after fees is ≤ 0 everywhere* because the
   hit rate (~13–27% filled) sits below the ~49–58% breakeven.

4. **All three execution levers are tested and dead:**
   - *Exits* (run.007): TP/SL-at-θ adds ~nothing at 30–90s — touches survive to
     the endpoint, and SL locks in −30 bp on adverse paths.
   - *Entry price* (run.008): maker entry is **worse** than taker via adverse
     selection — a resting limit fills exactly when the move fails (hit_m ≫ hit_f).
   - *Latency* (run.008): taker+1bar ≈ taker+0 — the edge is latency-robust, just
     below fees.

5. **Adverse selection is fill-mechanics, not a labeling artifact (run.009).**
   First-touch relabeling was hypothesized to fix it and did nothing (labels
   flipped <1%, only at h8; gate byte-identical). A maker limit fills only when
   price returns to it = when the move is failing — independent of the training
   label. Label-side levers (first-touch, and by extension cost-weighted BCE)
   cannot touch this; it's an execution-signal problem.

6. **Regime instability is chronic and now the headline risk.** Folds routinely
   disagree in sign; try_02's "significant" pooled IC is carried by 2 of 4 folds,
   and triggers cluster almost entirely in single folds (148/152 in one). Pooled
   significance ≠ a stable edge.

7. **The EMA-cross baseline is a period/data-pipeline artifact, not an edge**
   (freqtrade cross-check, 2026-07-08). run.005's idealized sim showed EMA 20/50
   L/S +$9.8k maker; freqtrade on independent exchange candles showed −$53.4k
   (trade count 950≈970 and B&H −31% both matched, so the setups aligned). Every
   mechanical cause (fees, funding, lookahead, fill timing, sizing, price proxy)
   was ruled out by measurement — the real reason is that the EMA edge is
   **period-unstable** (monthly gross flips sign), so total P&L is determined by
   which bars are in the test window. A real edge reproduces on independent data;
   this one doesn't.

8. **Dataset is verified correct vs Binance** (2026-07-08). Aggregating collector
   15s mid bars to 5-min OHLCV matches Binance futures candles to ~+0.01 bps.
   *Timezone gotcha:* `future_timestamp`/`spot_timestamp` are UTC epoch **seconds**;
   the `*_datetime` **string** columns are LOCAL (UTC+3, Greece). Always align on
   the epoch treated as UTC; never parse the datetime string.

## Current state & the one remaining lever

- Signal quality has genuinely improved on schema-5 data (try_02: directional IC
  significant out to the trading horizon for the first time) — best evidence yet
  that volume + v3 features help.
- But **no run has produced a tradable gate pass.** The binding constraint is the
  trigger-time hit rate itself, and the two things that could move it are:
  1. **The ≥45-day schema-3/4 (out5) dataset** — mid/late Aug 2026. Gives the
     gate its designed statistical power *and* the regime coverage the 2/4-fold
     concentration demands. **This is the single highest-value next action.**
     Rerun run.008/try_02 unchanged; ideally add a v3-features-off ablation cell
     on the same window to isolate the feature contribution. **Per run.010,
     run it on 5-second bars (w5), not 15s** — 5s sharpened the 30s/90s daily-IC
     past run.009 and cut trigger fold-concentration, at no measured cost.
     The 45-day run now carries **two** questions, not one:
     - **(gate/regime)** does the edge survive across 27+ test days / 4
       week-long folds, or was it two lucky regimes?
     - **(fill-timing)** re-run **Cell 10D** on the 45-day set — its ~0.50 AUC
       on run.010 hit the same 13-day power wall as the gate, so the 45-day
       result is the clean tiebreaker: still ~0.50 → collect **sub-second
       order-flow** (the only justified reason to go finer than 5s); lifts to
       ≥~0.58 → the fill-timing signal was buried in noise, build a **maker/skip
       router** on the current features, no new data needed.
  2. A genuinely different **execution signal** for maker entry (one that predicts
     a pause-before-continuation so the limit fills ahead of the move) — but the
     h2-reg-timing hybrid already degenerated to ~98% taker, so this likely needs
     the microstructure features, not cleverer plumbing.
- Ruled out as levers: bigger models, exit rules, entry latency, label
  redefinition (first-touch), and the EMA overlay.

## run.010 — executed 2026-08-02 (5-second bars: resolution experiment)

Notebook `runs/btc_lstm.run.010.ipynb` = run.009 with **one variable changed**:
bar width 15s→5s (the v4.1 collector's `out5.w5.*` stream, accumulating since
2026-07-13, never trained on). Every time constant is converted to keep
wall-clock meaning identical: HORIZONS [2,3,4,5,6,8]→[6,9,12,15,18,24]
(30s–2min), SEQ_LEN 64→192 (16min), VOL_WINDOW 240→720 (1h), H_SEL 6→18
(90s, θ=20bp), ENTRY_WAIT 2→6 (30s), small bar-count rollers ×3, θ per
wall-clock horizon unchanged. Rationale: the signal lives at 30–90s (IC halves
every ~30s) — at 15s the whole phenomenon is 2–6 bars wide; 5s triples
resolution exactly there. "New input" = the only lever class that has ever
moved this project (finding #2).

**Design experiment, not an economic gate** (~13 test days, same window as
run.009 → clean comparison, same power caveat). Pre-registered read-out vs
run.009's identical-window numbers:
- **Promote w5** (45-day run switches to 5s) iff daily-IC t at the 30s head
  (h6) AND 90s head (h18) beat run.009's +3.87/+2.10, AND primary-cell
  lift/hit don't degrade, AND trigger fold-share is not worse (run.009:
  148/152 in one fold; informational fold-share print added under the gate).
- **Kill w5** iff 30s/90s daily-IC doesn't materially improve → 45-day run
  stays on w15, zero further cost.
- Sanity anchor: 90s/20bp first-touch base rates must ≈ run.009's
  (lup 0.45% / ldn 0.43%); far off ⇒ tar or cadence handling broken.
- The printed gate is wall-clock-identical to run.009's; a PASS at ~13 test
  days is a design signal, NOT trading evidence. Economic verdict stays
  reserved for the ≥45-day run (~Aug 28).

New loader safeguard now in the notebook: files are filtered on `.w5.` in the
filename — a mixed w15+w5 tar can no longer silently interleave cadences.
Build the upload tar from the w5 files only
(`tar czf 20day_btc_data_w5.tar.gz out5.w5.*.csv*`). Smoke-tested 2026-08-02
on local w5 files against a mixed w5+w15 dir: filter loads w5 only, 99%+ of
bar gaps == 5s, schema v5 100%, 29/29 v3 features, labels/targets build.

### Results (executed on Colab, analyzed 2026-08-02)

**Data / cadence sanity — clean.** 119 w5-only files, 342,720 rows → 302,152
valid samples, 19.9 days (2026-07-14 → 08-02), schema-5 100%, cadence clean
(37 tiny breaks, largest 0.1h). Sanity anchor **holds**: first-touch base
rates lup_18 0.48% / ldn_18 0.45% ≈ run.009's 0.45% / 0.43% → cadence handling
correct, numbers directly comparable.

**Design hypothesis — PASSED cleanly** (the point of the run). Every
pre-registered promote condition is met:

| metric | run.009 (15s) | run.010 (5s) | verdict |
|--------|---------------|--------------|---------|
| daily-IC t, 30s head (h6) | +3.87 | **+4.07** | beats ✅ |
| daily-IC t, 90s head (h18) | +2.10 | **+3.35** | beats ✅ |
| all-horizon daily-IC | — | t = +3.21 … +4.07, **all 12 days, all 6 horizons** | uniformly significant |
| pooled IC (h6 / h18) | lower | +0.0746 / +0.0383 | ~doubled |
| trigger fold-concentration | up6 = **97%** in one fold | up18 max **42%**, dn18 **68%** | far better (both <97%, up18 <60% ideal) |

⇒ **Promote w5: the 45-day run should switch to 5-second bars.** Residual
caveat — the IC is still somewhat regime-carried (folds 1 & 3 hold it,
IC_h6 ≈ +0.157 each; folds 0 & 2 weak, +0.010 / +0.049), but the trigger
spread is dramatically healthier than run.009's single-fold clustering.

**GATE — FAILED as expected** (pre-declared design run, ~13 test days). Same
adverse-selection wall as run.008/009, unmoved by finer resolution:
- up18 maker sim **−8.38 bps** CI [−9.39, −7.84]; dn18 **−7.66 bps**
  CI [−8.72, −4.65] — CIs entirely below 0.
- Fill fine (78% / 76%) but **hit_f ≪ hit_m** (up 3.9% filled vs 16.1% missed;
  dn 6.2% vs 30.5%) — the limit fills only when the move fails.
- Per-fold sim 0/4 positive both sides; the 30s-timed hybrid is still negative
  (up −9.51 / dn −8.00), so timing does not rescue the fills. All taker sims
  −8 … −10 bps too.

**Verdict:** the resolution hypothesis is **confirmed** — 5s sharpens the
signal exactly where it lives, past run.009 on both target heads with far less
fold-concentration. But the economics remain gated by fill mechanics, which
finer bars do not touch (consistent with run.009's finding that adverse
selection is execution, not signal/label). The tradable verdict stays reserved
for the ≥45-day run (~Aug 28) — which should now run on **w5**.

### Fill-split diagnostic (Cell 10D, added + run 2026-08-02)

Direct attack on the adverse-selection mechanism: split the primary-config
triggers into filled/unfilled and favourable/adverse, then ask whether any
observable feature at the trigger bar predicts the split **out-of-fold**
(day-grouped CV logistic on all features). Read on run.010's own data:

| side | triggers | fill% | favourable(all) | FILLED OOF AUC | FAVOURABLE OOF AUC |
|------|----------|-------|-----------------|----------------|--------------------|
| up18 | 1,434 | 78.3% | 15.6% | 0.495 | (n/a — see below) |
| dn18 | 1,213 | 75.7% | 17.2% | 0.489 | 0.518 |

**~0.50 everywhere = chance.** Neither "will it fill" nor "will the fill be
profitable" is predictable from the current features (incl. all v3
microstructure). The tempting single-feature AUCs (`book_imbal_deep` 0.615,
`cancel_imbal_near` 0.638, `vol_ratio_1h_24h` 0.634) are **in-sample** and do
not survive out-of-fold — overfit, not signal. Two caveats: (1) the FAVOURABLE
target hit its own 13-day power wall — day-folds with zero rare positives made
a fold-averaged AUC undefined (up18 returned `nan`); the cell was patched to
**pooled out-of-fold predictions** (`cross_val_predict` → one AUC) so it
resolves on re-run. (2) At ~12 test days this diagnostic is under-powered by the
same logic as the gate. Per the pre-registered read-out, ~0.50 points to **data
resolution (sub-second order-flow)** as the remaining lever, not cleverer use of
the current features — but hold that trigger for the 45-day re-run (below).

## Operational notes

- Build the tar with `tar cJf btc_data.tar.xz out*.csv.gz`; verify schema-3/4
  presence with `tar tf … | grep -c '^out[34]'` (the old `out.*` glob missed
  out2/3/4 — the recurring "v1-compat mode" cause).
- Project `.venv` is broken; use the `binance2` venv
  (`/home/nkout/projects/binance2/binance2/.venv`) for local numpy/pandas.
  Pandas 3.0 there defaults datetime64 to µs — cast via `astype('datetime64[s]')`,
  not `astype('int64')//10**9`.
- Collector v4.1 dual-run (15s+5s) validated 2026-07-15 (≤0.045% bar loss, gaps
  only at real network outages). markPrice websocket is dead → v4 REST-polls
  premiumIndex; `opt_est_funding_rate_sample` actually holds estimatedSettlePrice
  (intentional, unused by features).
