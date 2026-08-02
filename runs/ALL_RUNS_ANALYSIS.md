# BTC Orderbook LSTM — Cross-Run Analysis (run.001 → run.009)

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
     on the same window to isolate the feature contribution.
  2. A genuinely different **execution signal** for maker entry (one that predicts
     a pause-before-continuation so the limit fills ahead of the move) — but the
     h2-reg-timing hybrid already degenerated to ~98% taker, so this likely needs
     the microstructure features, not cleverer plumbing.
- Ruled out as levers: bigger models, exit rules, entry latency, label
  redefinition (first-touch), and the EMA overlay.

## run.010 — pre-registered 2026-08-02 (5-second bars: resolution experiment)

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
