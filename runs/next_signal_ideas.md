# Next signal ideas — after the horizon programme closed

*Written 2026-09-22 after reading every analysis doc in `runs/`. Each idea is built around what
those docs settled, not around re-opening a closed branch. Each has a pre-registered first test
and kill criterion, in the project's own style.*

## What is settled (do not re-litigate)

| branch | result | source |
|---|---|---|
| 5 s – 90 s direction | `c* = 0.214 bp` vs 2.0 bp maker; perfect next-bar oracle only 1.15 bp | `horizon_economics_and_next_ideas.md` §6 |
| 15 min – 1 h direction | no signal in the 76 bar features (IC ≈ 0, heads stop at 0–14 rounds) | `gbm_probe.analysis.md` |
| 4 h mean reversion | IC −0.34 stationary, yet **−1.39 bp gross / 7 yr**; left-skewed, dies in drawdowns | `v1_4h_feasibility.analysis.md` §9 |
| volatility detector | **real and robust**: eventful AUC 0.879, \|move\| lift 4.6× @90 s → 2.3× @1 h, no retraining | `horizon_economics…` §7.2, `run014.plan.md` §2 |

What every run so far shares: **one asset (BTC), a directional bet, and Binance VIP0 fees taken as
fixed.** Each idea below breaks at least one of those three.

Controls every idea must pass (the ones that actually caught errors, `horizon_economics…` §8.5):
one-position-at-a-time P&L, blind / drift benchmark, sign-permutation or day-matched null, and
enough sample length. Plus the economic pre-check `required acc = max((1+F/E|move|)/2, P(up))`.

---

## 1. Monetise the volatility detector without direction

The project owns the hard part (a magnitude forecast) and spent 14 runs trying to bolt direction
onto it.

- **1a — breakout / continuation on detector triggers.** When the detector fires, rest OCO stop
  orders at ±k bp; ride whichever side triggers. **Honest caveat:** under a martingale a stop-entry
  has zero expected value (optional stopping), so this works *only if* moves continue after the
  detector fires. That is one testable claim. Offline from `run009f_scores.npz`, no new data.
  - *Kill:* primary cell net at 10 bp taker not > 0 with day-bootstrap CI > 0, or does not beat
    the day-matched random-entry null p97.5, or continuation `P(same side) < 0.55`.
  - **Built and run 2026-09-22 — FALSIFIED** (`breakout_probe.analysis.md`): continuation 0.516,
    net −6.30 bp [−26.7, +15.0], 72.8th pct of the day-matched null, 0 of 55 cells with n ≥ 100
    net-positive. Two by-products: a trailing realised-vol score matches the detector's 1 h lift
    (so vol-conditioned ideas can run on the 7-yr klines), and breakouts after RV *spikes* tend to
    *reverse* (n ≈ 30, hypothesis only) → follow-up **1c: fade RV-spike breakouts on 7-yr klines**.
- **1b — sell volatility, detector as veto.** Short-dated BTC option selling earns the documented
  variance risk premium; its failure mode is the left-tail spike — exactly what killed the 4 h
  strategy. Sell only when the detector says *quiet*. Needs Deribit/Binance options IV history
  (Deribit DVOL is free). *Kill:* detector-vetoed short-straddle P&L not better than un-vetoed in
  tail loss (worst-5 % days) **and** mean.

## 1d. v1-year two-stage probe (user proposal) — executed, FAILED

Stage 1 model-free (trailing 5-min realised |r|), stage 2 xgboost trained only on big-move bars,
331 days at 15 s, monthly walk-forward. Notebook `runs/btc_v1_stage2_probe.ipynb`, data
`data/v1_year/v1_15s.parquet` (231 MB, built by `harness_v1_stage2/extract_v1_15s.py`), tests
`harness_v1_stage2/test_notebook.py` (all pass, incl. a future-scramble leak test and a full smoke
execution). Kill: pooled + ≥ 75 % of months AUC ≥ 0.65, beats label-permutation null, beats best
constant side and fee-required accuracy, net @ 7 bp CI > 0. **Executed 2026-09-22 — FAILED** (`v1_stage2_probe.analysis.md`): AUC 0.584
  (real vs null 0.512, 8/9 months > 0.55) but trades at 50.6 % acc / +0.64 bp. The confident top 1 %
  hits 71.8 % / +11.1 bp causally, yet decays to +3.4 bp with a 15 s entry delay and misses taker fees
  even at zero delay. Only open thread: measure the 0–15 s decay on the 5 s / event data.

## 2. Cross-sectional multi-asset (rank coins, not time BTC)

A market-neutral long/short across 30–50 perps fixes three measured failures at once: the **drift
confound** (cancelled by construction), **IC ≠ P&L** (cross-sectional rank *is* the trade), and
**power** (~N× the independent observations; the 4 h test needed 8× more data). Factors: 1 d – 1 w
momentum/reversal, funding, OI change, volume shock. Free 5-min klines + funding for every perp.
- *First test:* port `harness_v1_feas/validate_5y.py` to top-40 perps, untuned, walk-forward,
  c\* by holding period.
- *Kill:* no factor with c\* > 4 bp and ≥ 5/7 years positive after a multiple-testing correction.

## 3. Funding carry (delta-neutral)

Long spot / short perp, collect funding; days–weeks holds amortise the 4 fee legs. The prediction
is funding *persistence*, conditioned on OI / L-S (v1 year) — not price direction.
- *First test:* REST funding history + klines, net of both legs and rebalancing.
- *Kill:* annualised net < risk-free, or worst month < −5 %.

## 4. Daily time-series momentum, vol-targeted

Complement of the failed 4 h MR: MR lost in 2021-10→22-06 and 2023-01→08, which are trend-following
regimes; TSMOM is positively skewed. Daily needs ~0.54 accuracy; 7-yr klines are on disk.
Caveat: the EMA cross was period-unstable (`ALL_RUNS_ANALYSIS.md` #7) — test untuned over all 7
years. Stretch: 50/50 blend with the 4 h MR rule (may be negatively correlated).
- *Kill:* blind-benchmark excess ≤ 0, or sign-perm < 97.5, or < 5/7 years positive.

## 5. Event studies on the unrecoverable data

216 days of liquidations, 331 days of 15 s OI and L/S — nobody else has these. Measure 1–24 h
forward returns after a **pre-registered ≤ 5 events** (liquidation clusters, OI spikes, funding
extremes, L/S extremes) rather than an always-on model. Small n → sign-perm + blind benchmark
mandatory.

## 6. Change the fee, not the model

Every result assumed VIP0 Binance. The 5 s oracle clears maker at a 30 s hold; a zero/negative
maker venue changes the arithmetic that closed the short horizons, and the volatility detector is
the natural quote-withdrawal filter for passive quoting.
- *First step:* desk-check current fee schedules; re-run §6 c\* at c = 0 / 0.5 / 1 bp (minutes).

## Process: a standard screening harness

Package the controls into one function every candidate runs through before any notebook is built:
c\* vs holding period · model/oracle c\* ratio · blind benchmark · one-position P&L · per-trade
skew · per-year table · sign-perm percentile · multiple-testing correction (the project already
has several best-of-72 cells that looked real).

## Order (superseded — see Round 2)

~~1a → 1c → 6 → 2 → 4 → 3 → 5 → 1b~~

---

## Round 2 — after 1a and 1d (2026-09-22)

What the two new runs added:
- **1a breakout probe:** magnitude alone does not become P&L through linear entries (continuation 0.516).
- **1d v1-year two-stage:** direction on big moves is real (AUC 0.584, 8/9 months) but trades at
  50.6 % / +0.64 bp. The confident top 1 % hits 71.8 % / +11.1 bp at zero delay, +3.4 bp at a 15 s
  delay, +0.1 bp at 30 s, and misses 10 bp taker fees even at zero delay (net +1.14, CI [−1.09, +3.79]).
- Pattern after ~15 BTC direction tests: **the signal is real, and always too small or too fast to pay fees.**

### Ranked by value per effort

**Cheap and decisive (about an hour each)**

| # | idea | first test | kill |
|---|---|---|---|
| R1 | **Latency decay of the confident tail** — the only live thread from 1d | re-score the 1d stage-2 design on the 60-day 5 s set (+ v5 event stream for sub-second); gross at 0 / 1 / 2 / 5 / 10 / 15 s entry delay, causal threshold, one position at a time | gross at a realistic 1–2 s delay < ~10 bp. If it survives but still misses fees, keep it only as an entry/exit timing or quote-pull overlay |
| R2 | **Fade RV-spike breakouts (1c)** — breakouts after realised-vol spikes reversed in 1a (n ≈ 30) | 7-yr 5-min klines, OHLC only, untuned, one position, leg split, blind benchmark, per-year table | net @ 7 bp CI ≤ 0, or < 5/7 years positive, or one leg carries it |
| R3 | **Re-price the fees (idea 6)** | recompute c\* (`horizon_economics…` §6) and the 1d confident-tail net at the fee tiers actually available (VIP / BNB discount / other venues) | no reachable round trip makes any measured cell net-positive → fees are not the lever |

**Structural changes (a day each)**

| # | idea | why it is different | kill |
|---|---|---|---|
| R4 | **Cross-sectional multi-asset (idea 2)** — long strongest / short weakest across 30–50 perps on momentum, reversal, funding, OI change | cancels drift, rank skill *is* the trade, ~N× the sample — the three things that failed on BTC alone | no factor with c\* > 4 bp and ≥ 5/7 years positive after multiple-testing correction |
| R5 | **Funding carry (idea 3)** — long spot / short perp, days–weeks | predicts funding persistence, not price | annualised net < risk-free, or worst month < −5 % |
| R6 | **Daily TSMOM, vol-targeted (idea 4)** | wins in the 2021-22 / 2023 regimes that sank 4 h mean reversion; right-skewed | blind excess ≤ 0, sign-perm < 97.5, or < 5/7 years positive |

**Needs new data or a venue**

| # | idea | need |
|---|---|---|
| R7 | **Option selling with the vol detector as veto (1b)** — uses the one robust asset to avoid the tail | implied-vol history (Deribit DVOL) |
| R8 | **Event studies (idea 5)** on liquidations / OI / L-S, ≤ 5 pre-registered events | small n → sign-perm + blind benchmark mandatory |

### Status (2026-09-22)

- **R3 done — fees are not the lever** (`fee_reprice.analysis.md`). Reachable tier = VIP0 + BNB
  (9.0 bp taker RT). Nothing measured is significantly net-positive there; closest is the 1d tail at
  zero delay, +2.14 bp net with CI lower bound 0.1 bp short.
- **R1 built, ready for Colab GPU** — `runs/btc_latency_decay_probe.ipynb`; needs
  `data/w5_60d.parquet` (132 MB) on Drive next to `v1_15s.parquet`. Tests
  `harness_latency/test_notebook.py` all pass (exact equivalence with the 1d features at 15 s,
  leak test at 5 s, full smoke run). Pass line: gross at 5 s delay, CI lower bound > 9.0 bp.

### Decision point

If R1 and R3 both fail, **stop BTC direction work entirely** and move the effort to R4 or R5 —
different return sources, not another angle on the same one.

### Order

R1 + R3 (together ~1 h) → R4 → R2 → R5 → R6 → R7 → R8.

---

## Round 3 — big-move direction (2026-09-23)

Brainstorm restricted to big-move slots: (1) OI / liquidation quadrant of the first leg,
(2) moves go against the crowded side (funding, L/S), (3) break vs reject at price levels on 7-yr
klines, (4) estimated liquidation-cluster magnets, (5) spot-led vs perp-led first leg, (6) maker fade
ladder on detector slots, (7) scheduled slots (macro, funding times, expiry), (8) Coinbase premium,
(9) options skew.

- **Done — lower trigger + OI / funding / L-S conditioning: NULL** (`continuation_conditioned.analysis.md`).
  After ±X bp (5–30), reaching 2X before the anchor is martingale; gross 0 ± 1 bp on 68 d (5 s) and
  330 d (15 s); 3/90 conditioned cells CI > 0 (chance). Closes ideas 1 and 2 in their cheap form.
- **Next cheap test:** idea 3 (price levels, 7-yr klines). Then R4 / R5 per the decision point.

### MLP arm in R1 — pre-registration (2026-09-23, before the code)

Question (user): does a flat deep net (MLP) on the same inputs find direction the tree model missed?
Prior: low — xgboost already combines features non-linearly, and the ceilings (oracle, martingale
continuation, 15 s decay) are about information, not model class. Added to
`btc_latency_decay_probe.ipynb` so it costs one Colab run.

- **Model:** PyTorch MLP, 60 features + missing-value flags, train-set clip at p0.5/p99.5 → z-score →
  NaN = 0; 3 hidden layers 256-128-64, GELU, dropout 0.2, AdamW, early stop on val AUC; 3 seeds averaged.
  Same `evt` training rows (all big-move bars), same label (90 s / ±20 bp first touch), same val window.
- **C1, model-class test (primary for this arm):** v1 holdout on 15 s bars — train to 2026-05-14,
  val 10 days, test 2026-05-25 → 08-15 (~80 days), identical splits for xgboost and MLP. AUC on
  touched stage-1 triggers. **MLP better** iff AUC_mlp ≥ 0.62 **and** AUC_mlp − AUC_xgb ≥ 0.02 with
  day-bootstrap 95 % CI lower bound > 0. Otherwise: model class is not the bottleneck; close
  "deep net on these features".
- **C2, economics:** arm `v1x_mlp` (whole v1 year → 5 s OOS) through the same R1 decay table and the
  same pass line (gross at 5 s delay, CI lo > 9.0 bp, n ≥ 100). Information only unless C1 passes.
- Also reported (information): holdout top-1 % / top-2 % tail gross at 0 and 15 s delay, both models;
  AUC of the 50/50 average of both.

### Status (2026-09-23) — R1 + MLP executed: both FAIL → decision point reached

`latency_decay_probe.analysis.md`. R1: `v1x` top 1 % +5.3 bp @ 0 s → −0.4 @ 5 s (top 2 % +3.4 → −0.6);
linear 1 s estimate +4.2 < 9, so no event-stream check; accrual +4.9 bp already in the first 5 s.
C1: xgb 0.5956 vs MLP 0.5958 AUC (Δ +0.0001, CI [−0.014, +0.018]); tails identical. The 1d tail
replicates on the May–Aug holdout (+13.8 bp @ 0 s → +4.8 @ 15 s). **BTC direction work stops; next
R4 or R5.** Loose end: `v1x_mlp` 5 s tail (n ≈ 28, 10 days) — rerun only on post-09-20 5 s data.

### W1 — wide raw-input 5 s DNN, two label arms — pre-registration (2026-09-23, before the code)

Question (user): no deep net has ever seen the **full 825-column** 5 s collector output (full spot +
futures ladders, OFI, add/cancel flow, early/late trade counts, walls, bursts, ETH mid). Runs 010 and
009d–f used 76 engineered features; 1d / R1 / MLP used 60. Can a wide-input network find direction
that survives entry latency? Prior: low (every signal so far is gone within ~15 s).

- **Data:** `60days_data.tar` (412 files, one schema, 68.7 d). Mechanical, name-based transforms only
  (no hand-crafted features): prices → bp vs futures mid, $ spreads / std → bp, ETH mid → 5 s return,
  non-negative quantities → log1p, signed → asinh; drop timestamps, constants, duplicate `_sum`/`_count`
  helpers, the time-to-funding column (time proxy). Spec saved as `harness_wide/wide_spec.json`.
- **Label arms.** **D (primary): first touch of ±10 bp within 90 s measured from the bar *after* the
  signal (t + 5 s)**, so the model cannot earn the first-seconds move that R1 showed is uncatchable.
  **Z (secondary):** the same label from t, to see whether wide inputs sharpen the fast signal.
  θ = 10 bp (not 20) for sample size: 128k training bars vs 21k.
- **Models, per label:** `mlp` wide MLP on [bar, 1-min mean, 5-min mean] (~3 × 620 inputs);
  `cnn` 1-D CNN over the last 12 bars (1 min) × all channels; controls `xgbw` xgboost on the same
  flat wide inputs, `xgb60` xgboost on the 60 engineered features. Weekly walk-forward from day 21,
  7-day val, purge 4 × 90 s, 2 seeds each.
- **Trades:** as R1: stage-1 triggers (trailing 5-min RV, 7-day causal 5 %), tail on |p − 0.5| with an
  expanding causal threshold, one position, hold 90 s from entry, delay 0 / 5 / 10 / 15 / 30 s.
- **P1 (economics, primary):** any wide model (`mlp`, `cnn`, `xgbw`) under label D has top 1 % or
  top 2 % gross at a **5 s** delay with day-bootstrap CI lower bound **> 9.0 bp** and n ≥ 100.
- **P2 (DNN vs trees):** `mlp` or `cnn` AUC − `xgbw` AUC ≥ 0.02 with CI lower bound > 0 (label D,
  touched stage-1 triggers, pooled OOS).
- **P3 (wide vs engineered):** best wide AUC − `xgb60` AUC ≥ 0.02 with CI lower bound > 0 (label D).
- Label Z is information only. **Kill:** P1 fails → wide-input 5 s direction closed, and with it BTC
  direction from this collector.
- Power: ~47 out-of-sample days in 7 weekly folds.

### Status (2026-10-01) — W1 executed: P1 FAIL (P2 / P3 pass, economically irrelevant)

`wide_probe.analysis.md`. No wide model clears 9 bp at a 5 s delay (best +2.08 bp, n = 60). The MLP
finds a real delayed signal the trees miss (label D AUC 0.527, CI [0.516, 0.544], 7/7 weeks; +0.027
over `xgbw`), but on these triggers the 90 s move is ~10 bp ≈ the fee, so the top-confidence decile
earns +0.9 bp. **BTC direction from this collector is closed. Next: R4 (cross-sectional) or R5
(funding carry).**

---

## R4 — cross-sectional perp factors — pre-registration (2026-10-01, before the code)

Rank coins against each other instead of timing BTC: long the strongest / short the weakest on a
factor, dollar-neutral. Cancels the drift confound by construction, the rank *is* the trade, and N
coins per day multiply the sample. Daily frequency, local CPU.

**Data (survivorship-free).** Every USDT-M perpetual ever listed, from the Binance public archive
(`data.binance.vision`, which keeps delisted contracts: LUNA, FTT, SRM, …): 1 h klines (OHLC, quote
volume, taker-buy volume) and funding-rate history, 2020-01 → 2026-08. Symbols must match
`^[A-Z0-9]+USDT$` (no delivery contracts, no `…SETTLED` relists). Excluded bases: stablecoins / fiat /
indices (USDC, BUSD, TUSD, FDUSD, USDP, DAI, EUR, BTCDOM, DEFI, and any symbol whose 30-day realised
volatility is < 1 % annualised, i.e. pegged).

**Universe, point in time, each day at 00:00 UTC:** listed ≥ 60 days, complete 1 h data for the
previous 30 days, **top 40 by trailing 30-day quote volume**. The study starts on the first day with
≥ 40 eligible contracts.

**Factors (fixed definitions, no tuning):**
| id | definition at day t (data up to 00:00) |
|---|---|
| MOM28 | log return t−28 d → t−1 d (skips the last day) |
| MOM7 | log return t−7 d → t−1 d |
| REV1 | log return t−1 d → t |
| FUND7 | mean funding rate per 8 h over the last 7 days |
| VSHOCK | log(quote volume last 24 h / mean daily quote volume of the prior 30 d) |
| RVOL30 | std of 1 h log returns over the last 30 days |

**Sign** of each factor is fixed on the **in-sample period = first 2 years of the study** by the sign of
its mean daily rank IC, then never changed. **Out-of-sample = everything after** (~4 years). All
verdicts are on OOS only.

**Portfolio.** Daily rebalance at 00:00 UTC; long the top quintile (8 coins), short the bottom
quintile (8), equal weight, dollar-neutral (1 long + 1 short gross). Holding H ∈ {1, 3, 7} days via H
staggered sub-books (each 1/H of capital), so the daily P&L is non-overlapping. Returns are
close-to-close from 1 h closes; **funding is real P&L** (longs pay, shorts receive every settlement
while held); **cost 4.5 bp per side** × turnover (VIP0 + BNB taker). A delisted coin is closed at its
last available price.

**Metrics per factor × H (18 cells).** OOS mean daily net return, annualised Sharpe, Newey–West t
(10 lags), c\* = gross mean / mean daily one-way turnover (break-even cost per side, bp), per-year
table, max drawdown, BTC beta, long / short leg split, and a **1 h entry-lag** row (enter at 01:00
instead of 00:00: the latency control).

**Null.** Cross-sectional permutation: shuffle factor values across coins within each day (1,000×) →
percentile of the OOS gross mean.

**PASS for a cell (all required):** OOS net mean > 0 with **Holm-corrected p < 0.05 across the 18
cells**; c\* ≥ 6.75 bp (1.5 × the 4.5 bp cost); positive net in ≥ 3 of the 4 OOS years; permutation
percentile ≥ 97.5; net > 0 with the 1 h entry lag. **Kill:** no cell passes → R4 closed in this form
(price / volume / funding factors, top-40, daily) → R5. Open interest is not included (archive history
only from late 2021); if a cell passes, an OI factor becomes a separate pre-registered test.

### Status (2026-10-01) — R4 executed: FAIL

`r4_xsec.analysis.md`. No cell passes Holm. The rank signals are real out of sample (low-vol IC
−0.099, t −14.7; 1–28 d reversal t −3 to −4), but equal-weight shorts of volatile alts lose on the
mean (median +22.6 bp/day vs mean −7.8 for RVOL30) to +150–500 % pump days. Only FUND7 (long
negative-funding coins) is positive: +17.2 bp/day net, 3/4 years, perm 98.7, but NW t 1.32 and the
gross is entirely funding collected. **Next: R5 (delta-neutral funding carry)**; optional R4b
(inverse-vol, capped weights) only as a new pre-registration tested on post-2026-08 data.

---

## R5 — delta-neutral funding carry — pre-registration (2026-10-01, before the code)

R4 found funding to be the only positive return source (FUND7 +33 bp/day of funding collected) and the
right-tail price risk to be what sinks unhedged books. R5 collects funding with the price risk hedged:
**long spot + short perp** on the same coin. The prediction is funding *persistence*, not direction.

**Data.** Perp 1 h klines + funding from `data/xsec/` (R4). Spot 1 h klines for every coin that was ever
in the R4 top-40, plus BTC / ETH, from the archive (`data/spot/monthly/klines`, which keeps delisted
pairs; perp-only coins have no spot and are not hedgeable). 2020-10 → 2026-08.

**Universe, daily at 00:00 UTC:** the R4 point-in-time top-40 perps (same definition) **∩ coins with a
spot USDT pair** that has complete 1 h data for the prior 30 days and ≥ 2 M USD mean daily spot quote volume.

**Primary strategy `CARRY+` (all parameters fixed now, no tuning):**
- Signal: FUND7 = mean funding per 8 h over the last 7 days (R4 definition).
- **Enter** when FUND7 ≥ 0.03 % (3× the 0.01 % default rate); **exit** when FUND7 < 0.01 %, when the coin
  leaves the eligible set, or on delisting (closed at the last available prices).
- At most 10 open positions; new entries ranked by FUND7. Each position is 1/10 of capital: spot
  notional n plus perp margin n / 3 (3× perp leverage), so n = capital / 10 / (4/3).
- Daily P&L of a position: spot return − perp return (the basis change) + funding received by the short
  perp at every settlement while open. Costs per side: **spot 7.5 bp, perp 4.5 bp** (VIP0 + BNB, taker),
  so a round trip costs 24 bp of notional.
- Idle capital earns 0. Results are reported as return on total capital.
- Margin stress: count days when the perp's high since entry rises more than 25 % above the entry price
  (a 3× short needs a top-up from the spot side); report them. P&L is unaffected if topped up.

**Benchmark `BTCETH`:** the same hedge on BTC and ETH, held permanently, 50/50, no signal (the classic
"cash-and-carry"). **Information arm `CARRY−`:** the mirror (long perp + short spot when FUND7 ≤ −0.03 %),
**gross of spot borrow cost** (not in the archive); report the break-even borrow rate.
**Latency control:** everything again with execution at 01:00.

**PASS (`CARRY+`, all required):** annualised net return on capital ≥ 4.5 % (≈ USD risk-free); net
positive in **every calendar year** 2021–2025 plus 2026 YTD; **worst month > −5 %**; Newey–West t of
daily net ≥ 2; positive with the 1 h execution lag. Reported but not required: excess over `BTCETH`
(does the selection add anything?). **Kill:** fail → R5 closed in this form; carry is not a strategy
here at VIP0 fees.

### Status (2026-10-01) — R5 executed: FAIL

`r5_carry.analysis.md`. `CARRY+` +3.65 %/yr (< 4.5 % risk-free; 2025 −2.65 %, 2026 −0.14 %), low risk
(worst month −1.4 %) but only 1.05 positions / 17 % of days invested. The BTC/ETH cash-and-carry
benchmark made +8.77 %/yr but has decayed (2025 3.75 %, 2026 ≈ 1.5 % annualised). `CARRY−` +37 %/yr is
gross of spot borrow (break-even ~168 %/yr per position), so it is not evidence of an edge.
Options: carry overlay (forward test), a live borrow-rate check for `CARRY−`, or a programme write-up.

---

## Round 4 — after the programme summary (2026-10-05)

Context: `PROGRAMME_SUMMARY.md`. Every tested edge was too small for the 9 bp fee, killed by a left-skewed
payoff, or a premium that has been competed away. Round 4 targets the second and third failures with
data already on disk. Run order **R6 → R9 → R4b**.

**Not pursued, and why.** Finer bars (1–2 s): R1 already answered this. The linear 1 s estimate was
+4.2 bp < 9 (`latency_decay_probe.analysis.md`), the 5 s oracle makes only 1.15 bp, and the binding
constraint is execution latency, not bar size. More BTC direction work: closed four ways. R2 (fade RV
spikes): prior near zero after the martingale continuation result. R8 (event studies): n too small.
Deferred: R7 (options with detector veto, needs Deribit data) and market making on mid-cap alts
(spreads 5–20 bp > fee; needs the event-stream collector pointed at alts).

**Fresh data.** The historical samples are spent on the hypotheses tested so far. Restart the
collector (or a paper-trading logger) for forward tests regardless of which idea is chosen.

### R6 — daily trend-following + volatility targeting — pre-registration (before the code)

Why: trend-following is the one structure with a right-skewed payoff, the opposite of what sank the
90 s book, 4 h mean reversion and the R4 short legs. Daily turnover makes 4.5 bp per side nearly
irrelevant.

**Data:** `data/btcusdt_5m_klines.pkl` (BTC perp 5 min, 2019-09 → 2026-09) resampled to daily at
00:00 UTC; `data/xsec/` (860 perps, 1 h klines + funding, incl. delisted).

| arm | rule (all parameters fixed) |
|---|---|
| **1A BTC trend (primary)** | signal = mean of sign(trailing return) over 20 / 60 / 120 d ∈ [−1, +1]; position = signal × 40 %/yr ÷ trailing 30-day realised vol, capped at 2× leverage; rebalance daily at 00:00 UTC, only when the target position changes by > 10 % |
| 1B vol-targeted BTC hold | same sizing, signal ≡ +1 (sizing tool, not alpha) |
| 1C per-coin trend | R4 point-in-time top-40; each coin the 1A rule, **long / flat** only, inverse-vol weights |
| info | 1A long / short and 1C long / short |

**Accounting:** perp funding as real P&L; cost 4.5 bp per side × turnover (VIP0 + BNB taker); 1-day
execution lag row. **Benchmarks:** buy-and-hold BTC, equal-weight top-40 long, cash at 4.5 %/yr.
**Metrics:** annualised net, Sharpe, max DD, skew, per-year table, turnover, long / short split.

**PASS (1A, all required):**
- net Sharpe ≥ buy-and-hold Sharpe + 0.3
- max DD ≤ ½ of buy-and-hold's
- beats cash in ≥ 5 of 7 years
- block bootstrap (random signals with the same turnover) percentile ≥ 97.5
- positive with the 1-day lag

**Kill:** fail → the trend premium is not present here or is competed away. 1B and 1C are reported,
and a 1C pass is a hypothesis for a forward test, not a result. Caveat: 7 years hold only a few
independent trend episodes, so power is low. Local CPU, about half a day.

### R9 — `CARRY−` borrow-rate check

Question: R5's `CARRY−` (long perp / short spot on negative funding) made +37 %/yr **gross of spot
borrow**, with a per-position break-even borrow rate of ~168 %/yr. Does the borrow cost absorb it, or
is the coin not borrowable at all?

**Data (needs a Binance API key with read-only permissions, exported as an environment variable;
never written to the repo):** `GET /sapi/v1/margin/interestRateHistory` (history per asset; depth to be
verified), `GET /sapi/v1/margin/crossMarginData` (current rates and limits),
`GET /sapi/v1/margin/maxBorrowable` (current availability).

**Steps:**
1. **Snapshot:** every coin with FUND7 ≤ −0.03 % per 8 h today: annualised funding received vs the
   current borrow rate, and whether it is borrowable (and how much).
2. **History:** pull the rate history as far back as the API allows; recompute `CARRY−` net of borrow
   over that window, using the R5 code and costs.

**Kill:** net of borrow < risk-free over the available window, **or** most entry-day coins are not
borrowable. Prior: dead (negative funding is the price of a scarce short). 30 min – 2 h.

### R4b — low-volatility factor with a bounded tail — pre-registration (before the code)

Why: the strongest OOS signal in the programme is low vol beating high vol (RVOL30 IC −0.099,
t −14.7). R4 lost money only because the short leg held volatile small coins that pumped +150–500 %.
Keep the signal; remove the short tail.

**Primary construction (one, no grid):**
- daily rebalance at 00:00 UTC; long the bottom RVOL30 quintile, inverse-vol weighted, ≤ 20 % per coin
- **hedge with a short BTC perp** sized to the long book's trailing 60-day beta (the only short is BTC)
- H ∈ {1, 7} days with staggered sub-books (as R4); funding as P&L; 4.5 bp per side; 1 h entry-lag row

**Key control:** the identical beta-hedged construction on an **equal-weight** long of the same
universe. Without it the arm could just be "alts vs BTC". The factor return is primary minus control.
**Info arm:** long low vol / short high vol, inverse-vol weights, short side ≤ 2.5 % per coin.

**Samples (R4's OOS period has been seen):**
1. **Primary: disjoint universe, volume ranks 41–100**, point in time, 2020-10 → 2026-08. A
   cross-sectional replication; not independent in time, since it is the same market.
2. Top-40 re-run: information only.
3. Forward from 2026-09: accumulates.

**PASS (primary, Holm across its cells, all required):**
- net > 0 with Newey–West t ≥ 2
- beats the beta-hedged equal-weight control
- positive in ≥ 3 of 4 years
- permutation percentile ≥ 97.5
- positive with the 1 h lag
- median and mean daily return have the same sign (the R4 lesson)

**Kill:** fail → low vol is real but not harvestable without shorting lottery coins. Reuses
`harness_xsec`; check archive coverage for ranks 41–100 first. Local CPU, about a day.


### Status (2026-10-05) — R6 executed: FAIL (narrowly)

`r6_trend.analysis.md`. 1A (BTC trend, long / short): Sharpe 0.92 vs buy-and-hold 0.69 (needs +0.30), max DD −33 % vs
−79 %, shift-null 97.9th pct, lag-1d +21 %/yr, but beats cash in 4 of 7 years (needs 5) and nets less than
buy-and-hold (+31.8 vs +39.6 %/yr); Sharpe-difference CI [−0.67, +1.22]. All gain is the long side (short leg +0.4 %/yr).
1C (per-coin, long / flat) fails 4 of 5 (shift-null 76th pct). Information arm `A_long` (Sharpe 1.21, 5 / 7 years) was
not pre-registered as primary: a forward-test hypothesis only. Next: R9, then R4b.

### R4b — implementation details fixed before the code (2026-10-05)

The R4b pre-registration above left these open; they are fixed here, before any R4b number has been computed.

- **Universe:** the R4 point-in-time eligibility (listed ≥ 60 d, complete 30-day 1 h data, not stable / index), ranked by
  trailing 30-day quote volume; **primary = ranks 41–100** (60 coins, quintile 12); BTC is excluded from every universe
  (it is the hedge). Study starts on the first day with ≥ 100 eligible coins.
- **Factor:** RVOL30 as in R4 (std of 1 h log returns over 30 d). Long = the lowest-RVOL quintile (12 coins), ranked only
  when ≥ 90 % of the universe has a value.
- **Weights:** ∝ 1 / RVOL30 inside the quintile, normalised to 1, capped at 0.20 per coin with the excess redistributed.
- **Beta hedge:** short BTC perp, notional = β on day d, where β is the OLS slope of the alt book's daily total return
  (price + funding, net of any short leg) on BTC's daily total return over days d − 60 … d − 1; needs 60 days of book
  history, otherwise no position. Staggered H = 1 and 7: the book is the mean of the last H days' target weights.
- **Cost:** 4.5 bp per side on the turnover of the whole book including the BTC hedge. **1 h lag row:** same weights,
  returns measured from 01:00. Return is on the long notional (long book = 1; hedge is extra gross).
- **Control:** equal-weight long of the *whole* ranks 41–100 universe with the identical hedge. Factor return = primary − control.
- **Info arm:** long low-vol as primary, short the highest-RVOL quintile inverse-vol weighted with total gross 0.30 and ≤ 0.025
  per coin, BTC hedge on the net book. Top-40 (ranks 1–40) re-run of the primary construction, information only.
- **Pass criteria, per cell (H ∈ {1, 7}), all required:** net mean > 0 with Newey–West t ≥ 2 **and** Holm-adjusted one-sided
  p < 0.05 across the two cells; primary − control daily difference has NW t ≥ 2; positive in ≥ 75 % of the consecutive
  365-day blocks; random-quintile permutation percentile (same construction, 300 draws) of the **gross** mean ≥ 97.5 *(corrected during testing, before any real-data run: a quintile redrawn at random every day pays far more turnover than a persistent rank, so a net-mean null is biased against the null; net > 0 is tested separately)*; net mean
  > 0 with the 1 h lag; median and mean daily return of the same sign. The study passes if any cell passes.
- **Forward test:** the archive ends 2026-08-30, so a post-2026-08 test is not possible yet; it accumulates from the collector.

### Status (2026-10-05) — R4b executed: FAIL

`r4b_lowvol.analysis.md`. Ranks 41–100, 1,864 days from 2021-07: primary (long low-RVOL30 quintile, inverse-vol, BTC short
sized to 60-day beta) nets −6.0 bp/day (NW t −1.18) at H = 1, −5.7 at H = 7; 0–1 of 5 blocks positive; negative with the lag.
The beta-hedged equal-weight control loses −9.4 bp/day (alts bleed vs BTC); primary − control +3.4 bp/day (t 1.09), the right
sign but not significant. Top-40 re-run (info) +4.1 bp/day, t 0.81. Round 4 remaining: R9 (borrow-rate check, needs a
read-only API key).

---

## Round 5 — V1: is realised volatility mispriced against DVOL? — pre-registration (2026-10-05, before the code)

Origin: `vol_monetisation_probe.plan.md` (spec for monetising the volatility detector with options). Before any detector work,
test the premise with data on disk: **is 30-day forward realised volatility predictably different from the implied volatility
the market quotes (Deribit DVOL), given a standard realised-vol forecast?** If a HAR-RV forecast carries no information that
DVOL lacks, a detector whose marginal contribution is small cannot rescue the trade. This is R7 (options with a volatility
veto) in its cheapest form, with **no detector** and **no options chain**: the P&L is a frictionless proxy for a delta-hedged
30-day volatility position (short vol earns `IV − RV` in annualised vol points).

**Data.** Deribit DVOL daily OHLC (public API, 2021-03-24 →), 30-day constant-maturity BTC implied volatility, annualised %.
BTC 5-minute klines (`data/btcusdt_5m_klines.pkl`, 2019-09 → 2026-09, complete) for realised volatility.

**Definitions (day t = 00:00 UTC).**
- `IV_t` = close of the DVOL daily bar of day t − 1 (the value at 00:00 of t).
- Daily realised variance of day d = sum of squared 5-minute log returns of the UTC day (needs all 288 bars).
- `RV_t` (forward) = 100 · sqrt(365 · mean daily variance over days t … t + 29): the volatility realised over the 30 days IV prices.
- **HAR features at t (all known at t):** log of the annualised vol of day t − 1, of the mean variance over t − 5 … t − 1, and over
  t − 22 … t − 1. **Target** log RV_t. Expanding-window OLS from 2020-01-01, refit on the first day of each month using only
  samples whose 30-day window ended before the refit day (purge 30 d). Forecast `F_t = exp(pred + ½ s²)`, s² the training residual
  variance. Every DVOL day is out of sample for HAR.

**Tests (one horizon, no grid).**
- **P0 (information):** mean `IV − RV` (the variance risk premium) over all days, Newey–West (30 lags) t, and on one entry per
  30 days.
- **P1 (skill beyond the market, primary):** regress `log(RV_t / IV_t)` on `log(F_t / IV_t)` (intercept + slope), Newey–West 30 lags.
  Efficient pricing means slope 0; a forecast the market lacks means slope > 0. **P1 passes iff slope > 0 with NW t ≥ 2.**
  *(Changed during test design, before any real-data number: the vol-point version `RV − IV` on `F − IV` returns slope 1 under a
  constant proportional premium `IV = c·E[RV]`, which is a level premium, not information. The log-ratio version returns 0.)*
- **P2 (tradeable, gated on nothing, reported either way):** daily entry of a 30-day position with P&L `x_t = pos_t · (IV_t − RV_t)
  − f·|pos_t|`, `f` = 2.0 vol points per entry (friction assumption, **unverified**; 0 / 1 / 3 also reported). Arms:
  **veto-short** (`pos = −1`, i.e. short vol, only when `F_t < IV_t`, else flat), **long-timed** (long vol when `F_t > IV_t`),
  **always-short** (benchmark). **P2 passes iff veto-short has mean x > 0 at f = 2 with a moving-block-bootstrap (block 30,
  2,000 draws) 95 % CI lower bound > 0 and is positive in ≥ 75 % of calendar years of the sample.**
- Controls: moving-block bootstrap (block 30) because entries overlap; entry-lag row (use DVOL of day t, i.e. one day later);
  a naive forecast arm (trailing 30-day RV instead of HAR); median vs mean; worst 1 % of entries; per-year table.

**Kill / decision.**
- **P1 fails** → the market's IV already contains what HAR knows: close the option branch; do not run the detector probe.
- **P1 passes, P2 fails** → mispricing exists but not at the assumed friction; the detector probe is optional and only worth
  running if its projected gain exceeds the shortfall.
- **Both pass** → the premise is live; the detector probe in the spec becomes worth running, and so does a real option-cost study.
- Caveat fixed now: ~65 independent 30-day periods; DVOL is a 30-day index (horizon-matched by construction); the sample is
  one regime-rich market; P&L is a frictionless variance proxy, not an executed option book.

### Status (2026-10-05) — V1 executed: P1 and P2 pass, narrowly; evidence carried by 2021–22

`vrp_dvol.analysis.md`. Sample 2021-03 → 2026-08 (1,978 entry days, ~66 independent months). VRP +5.2 vol points (NW t 3.25). P1: HAR slope
+0.41 (t 3.18; naive −0.02) → PASS. P2: veto-short +3.04 vol pts per entry day at a 2-point friction, CI [+1.46, +4.78], 5 / 6 years positive
(needs 5) → PASS by 0.1–0.4 point margins. Diagnostics: slope +0.77 (t 3.06) in 2021–22 vs +0.19 (t 1.43) in 2023–26; veto-short +8.1 → +0.6 per
entry day; veto minus always-short −0.15 (CI [−2.1, +2.0]); tail cut (worst 1 % −17 vs −60) but not the May-2021 event. Decision: premise
not dead, but weak and decaying; forward-score the rule first, then a real option-cost study; the detector probe last.

### V1 forward scoring — pre-registration (2026-10-05, before the code)

The V1 verdict rests on a sample that is now spent (and mostly 2021–22). Forward test of the **frozen** rule: the exact `harness_vrp/vrp.py` pipeline
(HAR-RV on log RV, expanding OLS refit monthly with a 30-day purge; `IV_t` = DVOL close of day t − 1; position short volatility iff `F_t < IV_t`,
else flat; 2 vol points friction per active entry). **Forward window = entry days after 2026-08-23** (the last in-sample entry day).
- **Inputs refreshed 2026-10-05:** BTC 5-minute klines to 2026-10-05 (`runs/harness_v1_feas/fetch_klines.py`), DVOL to 2026-10-05.
- **Regression check:** re-running the pipeline on entry days ≤ 2026-08-23 must reproduce the V1 numbers exactly (P1 slope, veto-short mean).
- **Ledger:** `harness_vrp/forward_ledger.csv`, one row per forward entry day: IV, F, naive, position, status (complete = the 30-day window has
  finished; open = realised-so-far shown), RV, P&L at f = 2 for veto-short and always-short. Rows are committed before their windows complete so
  the signal is on record before the outcome.
- **No verdict until ≥ 180 completed forward entry days** (about six independent months, early 2027). Then **PASS iff** veto-short mean at f = 2 is
  > 0 with a moving-block-bootstrap (block 30) 95 % CI lower bound > 0 **and** the forward P1 slope (HAR) is > 0. Anything earlier is reported as
  descriptive and n is stated. Rescoring is a rerun of `forward_score.py`; no parameter may change.

---

## R9 — `CARRY−` borrow-rate check — implementation details fixed before the code (2026-10-05)

R9 was registered in Round 4. Facts found while preparing it: no API key is configured in this environment, but Binance publishes per-asset **VIP0 cross-margin
daily interest rates and borrow limits** on an undocumented public web endpoint (`https://www.binance.com/bapi/margin/v1/public/margin/vip/spec/list-all`, no auth).
That gives a **current snapshot only**; the signed history endpoint (`/sapi/v1/margin/interestRateHistory`) still needs a key. So R9 is run as a snapshot plus a
re-pricing of the R5 `CARRY−` sample at **today's** rates, which is a proxy for the past, stated as such.

- **S1, today's candidates.** Every USDT perp whose FUND7 (sum of funding over the last 7 days ÷ 21, the R4 / R5 definition) ≤ −0.03 % per 8 h, from the public futures
  endpoints (`premiumIndex` to pre-filter on `lastFundingRate` ≤ −0.01 %, then `fundingRate` history). For each: funding collected by a long perp
  (annualised = −FUND7 × 3 × 365), whether the base asset is on the cross-margin list, its borrow rate (daily × 365, simple), borrow limit in USD (coin limit × mark price),
  and net = funding − borrow − amortised costs (24 bp round trip, R5 costs, over the R5 mean `CARRY−` hold of 15 d = 5.84 %/yr). Base asset = symbol minus `USDT`,
  with the `1000` / `10000` / `1000000` / `1M` multiplier prefixes stripped.
  **S1 ALIVE iff ≥ 3 candidates are borrowable with net ≥ 4.5 % (risk-free) and a borrow limit ≥ 50,000 USD; otherwise DEAD for today.**
- **S2, R5 `CARRY−` re-priced.** Re-run `CARRY−` exactly as R5 (same eligibility, signal, 10 positions, 3× perp, costs) but (a) only coins **currently** on the cross-margin
  list are eligible (a coin that is not borrowable today is treated as not tradable), and (b) each open position pays the coin's current VIP0 borrow rate on its spot-short
  notional (0.075 of capital per position). Report net %/yr, per-year, mean positions, coin coverage (share of R5 episode-days on coins not listed today), and the
  **uniform multiple of today's rates at which net equals the 4.5 % risk-free rate**. **S2 ALIVE iff net ≥ 4.5 % and positive in every calendar year 2021–2025.**
- **Kill / decision.** Both DEAD → `CARRY−` is closed (the +37 %/yr gross was the price of a scarce short). Either ALIVE → note it as a lead only: rates today are not
  rates then, so the next step would be the signed history with a read-only key.
- Caveats fixed now: the endpoint is undocumented and may change; VIP0 cross-margin rates are for the account tier a small trader has; isolated-margin-only assets are not
  listed; the proxy ignores that borrow rates spike exactly when funding is most negative.

### Status (2026-10-06) — R9 executed (snapshot + re-pricing): DEAD by the letter; prior falsified

`r9_borrow.analysis.md`. S1: 11 candidates (FUND7 ≤ −0.03 %), 9 borrowable, funding 41–1,434 %/yr vs borrow 8–87 %/yr, but every VIP0 borrow limit $1.5k–5.6k → 0 qualify (need ≥ 3
with ≥ $50k) → DEAD. S2: R5 `CARRY−` on currently-borrowable coins at today's rates +18.9 %/yr (gross of borrow on those coins +26.7, R5 original +37.3), Sharpe 3.8, max DD −1.5 %,
but 2024 −0.51 % → DEAD on "every year 2021–2025 positive". Break-even at 2.83× today's rates: borrow does **not** absorb the gross, contrary to the prior. Capacity-bound (small account) and
rests on today's rates; the signed history (read-only key) is still open. `r9_borrow_snapshot.json` starts a borrow-rate history.

### R9 follow-up — daily borrow snapshots (built 2026-10-06; cron schedule removed by the user the same day: the machine is not always on)

`harness_xsec/snapshot_borrow.py` (tests `test_snapshot.py` 9/9, network mocked) stores one `data/borrow_history/borrow_YYYY-MM-DD.json.gz` per UTC day: VIP0 borrow rate and limit for every
cross-margin asset, mark price and last funding rate for every USDT perp, and the S1 candidates with FUND7. Run it by hand (or from an always-on host) whenever possible: the first success of each UTC day is kept, later runs that day skip without
fetching, failures exit 1 and write nothing; log `data/borrow_history/snapshot.log`. Gaps are tolerable for the purpose but shrink the sample. About 15 KB a day. `data/` is not in git. Use: after ~90 days, re-price
`CARRY−` on the live candidates with the rates actually paid, and forward-score it; until then the R9 verdict stands as registered.

---

## Round 6 — after the programme close: three new return sources (pre-registered 2026-10-08, before any code)

Context: `PROGRAMME_SUMMARY.md`. BTC direction is closed at every horizon. The rank signals were real but the short legs sank on pumps.
The only positive sources were collected premia (carry, VRP), and they were capped by capacity (R5: ~1 hedgeable position; R9: borrow
limits $1.5–5.6k per coin). Round 6 tests three ideas that avoid all four failure modes. Order: **N1 → N4** (same harness, data on disk,
minutes each) **→ N2** (needs a new fetcher). Deferred: N3 basis-spike convergence, N5 listing / unlock drift, N6 BTC → alt lead-lag at
1 min, N7 post-pump decay.

**Facts fixed before writing this (no returns looked at):**
- **Eligible coins per day** (R4 eligibility: ≥ 60 days listed, complete 30-day 1 h data, not stable / index).
  - Thresholds: ≥ 100 from 2021-05-22 · ≥ 150 from 2023-04-05 · ≥ 200 from 2023-11-26 · ≥ 250 from 2024-04-07.
  - Yearly median: 109 (2021) · 136 · 174 · 256 · 400 · 540 (2026).
- **Funding interval mix** (share of settlements at 8 h / 4 h / 1 h).
  - 8 h: ≈ 100 % in 2020–22 · 91 % in 2023 · 48 % in 2024 · 20 % in 2025 · 17 % in 2026.
  - 4 h: 9 % in 2023 · 52 % in 2024 · 73 % in 2025 · 80 % in 2026.
  - 1 h: 7 % in 2025, 3 % in 2026.
  - Anything defined "at 00/08/16 UTC" therefore misses most settlements after 2023, so N4 is defined per settlement event.
- **Already seen, so not evidence for N1** (R4b, `r4b_output.txt`).
  - Ranks 41–100: the beta-hedged equal-weight alt basket lost −9.4 bp/day (NW t −1.62, sd 2.48 %/day, 1 of 5 blocks positive).
  - Ranks 1–40: the same control was **+0.0 bp/day**.
- **Archive end.** The perp archive ends 2026-08-30. Before any N1 / N4 run, the archive is extended with `fetch_archive.py` to the
  latest complete month. Days after 2026-08-30 have been seen by no study, so they are reported as a separate **forward block**
  (descriptive; the verdict uses the registered window).

**Controls common to N1, N4 and N2** (the programme's lessons, §6 of the summary):
- Tests written and passing before the real run: look-ahead / leak test, exact P&L identity, a planted-effect world that passes, and
  ≥ 10 null worlds that pass ≤ 1 time.
- 4.5 bp per side taker cost on Binance, funding as real P&L, and a 1 h execution-lag row.
- Median vs mean, trimmed mean (1 % each tail), worst day / event and worst 1 %, per-year table, NW t.
- Holm correction across each idea's registered cells.
- Parameters are fixed here. Nothing is tuned. Information arms are not promoted after the fact.

### N1 — long BTC / short a diversified mid-cap alt basket, beta-neutral

**Hypothesis.** After removing BTC beta, mid-cap perps lose to BTC: token emissions and unlocks keep diluting them, and long-biased
retail crowds them. The idea came from R4b's control. That control ran on ranks 41–100, so those ranks are information only. The test
uses coins R4b never held.

**Construction (one primary cell).**
- **Universe.** Point-in-time **volume ranks 101–200**, R4 eligibility, BTC excluded. The study starts on the first day with ≥ 200
  eligible coins (2023-11-26), plus a 60-day beta warm-up, and runs to 2026-08-30 (≈ 950 days).
- **Short book.** Weights ∝ 1 / RVOL30 over the 100 coins, normalised to a total of 1, **capped at 2 % per coin** with the excess
  redistributed (the `lowvol.inv_vol_weights` routine). A coin is skipped if RVOL30 is missing. The book is formed only when ≥ 90 coins
  have a value.
- **Hedge.** Long BTC perp at notional β_d, where β_d is the OLS slope of the short book's daily total return on BTC's over
  d − 60 … d − 1 (the `lowvol.hedged_book` routine with the long leg empty).
- **Holding.** H = 7 staggered cohorts (one cell; H = 1 as information).
- **Accounting.** Funding is P&L on every leg (shorts usually receive it), 4.5 bp per side on all turnover including the hedge, and a
  1 h lag row. Returns are on the short notional (= 1).

**Information arms (reported, not part of the verdict).**
- Equal-weight short of the same universe.
- Ranks 41–100, the reverse of the R4b control: a replication of what has already been seen.
- Ranks 1–40.
- P&L decomposition: alt price, alt funding, BTC price, BTC funding, costs.
- Largest single-coin daily loss contribution, and the 10 worst days with the coin responsible.

**Power, stated before the run.** At R4b's effect size (9.4 bp/day) and dispersion (2.48 %/day), 950 days give an expected NW t of
≈ 1.2. A historical t ≥ 2 needs ≈ 16 bp/day. **The history can kill this idea but is unlikely to confirm it**, so the verdict has three
outcomes.

**Verdict (H = 7 cell).**

**PASS** requires all of the following:
- net mean > 0 with NW t ≥ 2;
- median and mean both > 0;
- trimmed mean > 0;
- ≥ 4 of the 5 consecutive 182-day blocks positive;
- 1 h lag net > 0;
- worst day > −20 % of the short notional.

**ALIVE → forward test** if PASS fails only on the t-stat, with 1 ≤ t < 2. Then the frozen rule is scored forward. The verdict comes at
≥ 365 forward days with NW t ≥ 2 on forward days alone. That needs daily data after the archive end: monthly archive extensions are
enough, so no always-on collector is required.

**DEAD** otherwise. Then the R4b bleed was specific to ranks 41–100 / 2021–26 and does not generalise.

### N4 — funding-settlement effect

**Hypothesis.** Ahead of an extreme settlement the paying side reduces its position, pushing price toward the receiver; after the
settlement the pressure lifts and price partly reverts. The timing is fixed by the exchange clock, so latency does not matter.

**Events.**
- Every funding settlement s (any interval) of a coin in the point-in-time **top 100 by volume**, R4 eligibility, from 2021-05-22 (≥ 100
  eligible) to 2026-08-30.
- **Signal:** the **previous** settlement's rate r_prev, which is known before s. This is the causal choice. The settled rate r_s is
  finalised only at s and is used as information only.
- **Extreme:** |r_prev| ≥ 0.10 % per settlement (≥ 10 bp paid by one side, the same order as the round-trip fee). The sign convention is
  q = −sign(r_prev), i.e. the receiving side's direction.

**Trades** (1 h klines; entry and exit at hourly closes; one position per coin per event).
- **PRE:** enter q at the close of the bar ending at s − 1 h, exit at the close at s. The exit is treated as before the settlement, so
  **no funding is credited**.
- **POST:** enter −q (expected reversion) at the close at s, exit at s + 1 h. This trade does not hold through any settlement.
- Gross = signed price return in bp. Net = gross − 9.0 bp (VIP0 + BNB taker round trip).

**Controls.**
- **Placebo:** the same coin, with the same q, over the bar 2 h earlier (PRE) or 2 h later (POST). That bar is not adjacent to a
  settlement in either the 4 h or the 8 h regime. Report event minus placebo.
- **Blind / drift:** the unsigned mean over the same bars.
- **Splits:** r_prev > 0 vs < 0; 8 h vs 4 h vs 1 h interval; per year.
- **Inference:** a bootstrap clustered by **settlement timestamp**, because events in the same hour are correlated.
- **Lag row:** entry one hour later is the POST trade itself; for PRE the lag row is the placebo.
- **Information:** the same cells signed by the settled r_s.

**PASS (per arm; Holm across PRE and POST).** Net mean > 0 with clustered-bootstrap 95 % CI lower bound > 0. Event minus placebo > 0
with CI lower bound > 0. Positive in ≥ 4 of 6 calendar years with events. Both funding-sign legs ≥ 0. n ≥ 300.

**Kill.** Both arms fail → no tradeable settlement effect at 1 h resolution. **Caveat:** a 1 h bar dilutes an effect concentrated in
the final minutes. A minute-scale follow-up is a new pre-registration, and it lands back in latency territory (R1), so the prior for it
is low.

### N2 — cross-venue funding spread, perp against perp (Binance vs Bybit)

**Hypothesis.** The same coin's perp pays different funding on different venues, and the spread persists for days. Long the low-funding
venue and short the high-funding one: the price risk cancels, and the trade collects the spread. This removes both binding constraints
on carry: perp-only coins need no spot leg (R5), and there is no borrow limit (R9).

**Data.**
- **Binance:** the archive (on disk).
- **Bybit:** the public v5 API (`/v5/market/funding/history`, `/v5/market/kline` at 1 h, `/v5/market/instruments-info`), via a new
  resumable fetcher `harness_xsec/fetch_bybit.py` with tests.
- **Window:** from the first day ≥ 20 symbols are on both venues, to the archive end.
- **Survivorship caveat** (fixed now): the Bybit API lists only current instruments, so coins delisted from Bybit are missing. The
  missing coin count is reported. Delisting is closed at the last common price.

**Universe, daily 00:00 UTC.** The Binance point-in-time top 100 by volume ∩ coins with a Bybit USDT linear perp that has complete 1 h
klines for the prior 30 days and ≥ 2 M USD mean daily Bybit volume.

**Strategy `XFUND` (all parameters fixed).**
- **Signal:** Δ7 = 7-day mean funding per 8 h on Bybit − on Binance. Each venue's settlements are summed over the window and normalised
  to 8 h, so the 4 h and 1 h intervals are handled.
- **Entry / exit:** enter when |Δ7| ≥ 0.03 % per 8 h; exit when |Δ7| < 0.01 %, or the coin leaves the set, or it delists. At most 10
  positions, ranked by |Δ7|.
- **Legs:** short the perp on the venue with higher funding, long on the other.
- **Sizing:** each position uses 1/10 of capital as margin, 3× per leg, so each leg's notional is n = 0.15 × capital.
- **Daily P&L:** n × (return of the long-venue perp − return of the short-venue perp + funding received − funding paid).
- **Costs per side:** Binance 4.5 bp, Bybit 5.5 bp (VIP0 taker), so 20 bp per round trip of notional.
- **Margin stress:** count days when either leg is > 25 % against its entry.
- **Lag row:** everything again at 01:00.
- **Benchmark:** the 4.5 %/yr risk-free rate. Idle capital earns 0.

**PASS (all required):**
- annualised net on capital ≥ 4.5 %;
- net positive in every calendar year of the window (partial years included if ≥ 6 months);
- worst month > −5 %;
- NW t of daily net ≥ 2;
- 1 h lag net > 0;
- mean open positions ≥ 2. This is the capacity criterion that `CARRY+` failed.

**Reported:**
- the share of P&L from funding vs cross-venue price basis;
- the median spread hold;
- the 1 % worst days;
- the thinner venue's median daily volume per held coin, as a capacity measure.

**Kill.** Fail → cross-venue funding dispersion between two retail venues is too small or too short-lived to pay four taker legs.
Bybit is chosen as the first venue because both venues are USDT-margined at VIP0. OKX and Hyperliquid (USDC, hourly funding) would be a
new registration.

### N2 implementation details (fixed 2026-10-12, before the run; the pre-registration is silent on these)

- **Survivorship is better than the caveat assumed.**
  - `instruments-info?status=Closed` lists delisted Bybit perps (308 of 1,106 USDT perps).
  - Their funding and 1 h klines stay queryable with a window that ends before the delisting, so they are included.
  - The remaining gap is symbol renames. Binance → Bybit mapping: exact name, else a unique match after stripping 10^k multipliers (1000SHIB ↔ SHIB1000).
    Of 600 Binance perps ever in the top 100, 574 map. The 26 unmapped are reported.
- **Sign flip.** A position also exits when sign(Δ7) changes: the "higher-funding venue" it was built on no longer holds. The same day it may re-enter in the
  new direction, charged as an exit plus an entry.
- **Set conditions.** The set also requires at least one settlement on each venue in the prior 7 days, so a missing funding history never reads as a 0 rate.
- **Lag row.** The same positions (decided at 00:00), with both venues' returns and funding measured from 01:00.
- **P&L basis.** Daily P&L is on constant leg notional, as in R5, with no rebalancing cost.
- **Margin stress.** Counted on hourly closes: the long leg below entry × 0.75 or the short leg above entry × 1.25, at any hour of the holding.
- **Years criterion.** Applies to calendar years with ≥ 182 days in the window.

### Status (2026-10-12)

- Archive extended to 2026-09-30 (`fetch_archive.py --extend 2026-09`; 895 symbols).
- **N4 run: POST PASS, PRE FAIL** (`n4_settlement.analysis.md`).
  - POST: +25.4 bp net per event, CI [18.4, 32.4]; event − placebo +33.4; n 20,847.
  - Forward month September 2026: +6.8 bp, CI [−15.9, +30.5], uninformative.
- **N4-X FAIL → N4 CLOSED** (`n4_settlement.analysis.md` §5).
  - About 26 of the hour's 43 bp move happen in the first minute after the settlement.
  - Entry at s + 2 min nets +0.8 bp at 19 bp all-in, CI [−18.7, +20.7], median −8.1.
  - N4-F is not started.
- **N1 run: DEAD** (`n1_altshort.analysis.md`).
  - +8.1 bp/day net but NW t 0.91, and only 3 of 5 blocks positive (+17.9, +26.5, +18.1, −5.3, −17.5).
  - Same shape in every band (ranks 1–40, 41–100, equal-weight): a 2024 → mid-2025 BTC-dominance phase that reversed, not a mid-cap premium.
  - Forward September 2026: −15.6 %.
- **N2 run: FAIL** (`n2_xfund.analysis.md`).
  - +10.35 %/yr on capital, NW t 2.02, mean 4.3 positions (capacity passes).
  - 2023 −0.98 % and 2026 −1.11 %; worst month −11.5 % (2026-07).
  - Funding +20.4 %/yr, eaten by cross-venue basis −5.1 and costs −4.9. 2025 is +37.8 %, and two coins make half the P&L.
  - The delisting rule ("last common price") was implemented after the first run exposed flat zero-volume Binance bars after delistings. Both runs FAIL.
- **Round 6 complete: N4 closed (execution), N1 DEAD, N2 FAIL.**

### N4 follow-up — forward ledger and minute-scale execution check (pre-registered 2026-10-12, before any data after 2026-09-30)

The rule is frozen exactly as registered for POST. Events: |r_prev| ≥ 0.10 %, top 100, R4 eligibility, membership from the day starting ≤ s − 3h. Trade −q from
s to s + 1h, 9 bp round trip. Nothing is re-tuned. The 2 h hold and the |r| ≥ 0.5 % bucket seen in the diagnostics are not used.

**N4-F forward ledger.**
- **Window:** settlements from 2026-10-01 on, scored after each monthly `fetch_archive.py --extend`. September 2026 has already been seen, so it is reported
  separately and is not part of the forward verdict.
- **First checkpoint:** ≥ 4 forward months and ≥ 5,000 events.
  - CONFIRMED if net mean > 0 with clustered-bootstrap 95 % CI lower bound > 0, and median > 0.
  - FAILED if net mean ≤ 0.
  - Otherwise continue.
- **Final:** at 12 forward months, the same rule. CI lower bound > 0 → CONFIRMED, else FAILED.
- Reported every time: event − placebo, the per-sign legs, worst event, worst 1 %.

**N4-X execution check (1-minute data).**
- **Sample:** 2,000 registered events drawn at random (seed 0) from 2025-01-01 → 2026-08-30, using Binance daily 1-minute futures klines for those coin-days.
- **Executable entry:** the open of the minute starting at s + 2 min, so the order is placed after the settlement print is visible. Exit at the close at s + 1h.
- **Cost:** 9 bp fee + a fixed 10 bp round trip allowance for spread and slippage, 19 bp in total.
- **PASS:** net mean > 0 with clustered CI lower bound > 0, and median > 0.
- **Reported:** the mean cumulative signed path minute by minute (0–120 min), and the same with entry at s + 1, 5 and 15 min (information).
- **FAIL** → the effect is not capturable by a retail taker. N4 is closed regardless of N4-F.

## Round 7 — N5: drift after a new token's Binance perp listing (pre-registered 2026-10-12, before any code)

Context: Round 6 is complete with nothing tradeable (`PROGRAMME_SUMMARY.md` §2.6). The programme's edges failed because they were too fast to trade (R1, N4), too
small in capacity (`CARRY+`, R9), or unstable across years (N1, N2, R5, V1). N5 is chosen to avoid all three:
- daily horizon;
- a flow of events that grows over time;
- a cause that is structural, not a regime: new tokens list with a small float, and supply then unlocks into them.

### Facts fixed before writing (no prices or funding looked at)

**Listings.** The first 1 h bar of each Binance USDT-M perp in the archive.
- 11 symbols already existed in January 2020; 2 bases were re-listed under a new name or multiplier.
- `exchangeInfo` dates match the first bar to within hours.

**Excluded from the crypto universe:**
- 211 `TradFi` perps (equities and commodities, 2026; `underlyingSubType` = TradFi);
- 1 index perp.

All 2026 listings are still in `exchangeInfo`, so the tag is complete. Delisted symbols are all pre-TradFi crypto.

**"New token".** No Binance spot pair, or a spot pair whose first monthly archive file is at most 3 months before the perp's listing month.
- Source: the archive directory listing; 471 of 892 symbols have spot.
- Seasoned coins (spot > 3 months earlier, median 21 months) are excluded. They form an information arm.

**Liquidity.** Binance perp quote volume in the 24 h before entry ≥ $5 M.

**Event counts:**

| year | crypto perp listings | new tokens | new, ≥ $5 M | seasoned |
|---|---|---|---|---|
| 2020 | 69 | 37 | 29 | 32 |
| 2021 | 58 | 25 | 23 | 33 |
| 2022 | 26 | 15 | 15 | 11 |
| 2023 | 98 | 42 | 42 | 56 |
| 2024 | 131 | 96 | 96 | 35 |
| 2025 | 238 | 225 | 224 | 13 |
| 2026 | 47 | 47 | 46 | 0 |

- **472 events** have entry + 28 d ≤ 2026-08-31.
- Only 1 event fits the forward block (exit by 2026-09-30) for H = 28; a handful fit for H = 14.
- 3 events stop trading within 28 days of entry.

**Caveat (fixed now).** A token's history on other venues is not visible. A "new" Binance perp can be a coin that traded elsewhere for years, which mostly
affects 2020–22.

### Hypothesis

New tokens list with a small circulating float and a large locked supply. Airdrop recipients, early investors and market makers sell into the listing demand
over the following weeks. So a new perp underperforms BTC over its first weeks by more than a seasoned alt does on the same dates.

### Construction (one primary cell)

**Entry.**
- At the first 00:00 UTC that is ≥ 48 h after the listing L. This skips the listing frenzy, and the entry time is known in advance.
- Liquidity filter: ≥ $5 M perp quote volume in the 24 h before entry.

**Trade.**
- Short the new perp and long the BTC perp at equal notional (β = 1, fixed: a new coin has no history to estimate beta from).
- **H = 14 days primary.** H = 7 and H = 28 are information.
- Exit at 00:00 of entry + H.

**Delisting / stop.** The archive prints flat zero-volume bars after a delisting. The event is closed at the coin's last traded price (`xfund.death_hour`), and the BTC
leg at the same hour.

**Event P&L (buy-and-hold over the hold), per unit notional:**
- Short leg: −(P_exit / P_entry − 1) + funding received by a short.
- Long BTC leg: (BTC_exit / BTC_entry − 1) − funding paid by a long.
- Costs: 4.5 bp × 4 sides = **18 bp**.

**Portfolio view (for the capital criteria).**
- Each event uses 2 % of capital notional on each leg.
- At most 25 events open at once; later events are skipped and the count is reported.
- Daily marking on constant notional (as R5 / N2). Idle capital earns 0.

### Controls

- **Seasoned control (the N1 lesson).** For each event over the same dates: short an equal-weight basket of Binance top-100 perps aged ≥ 365 days (ex-BTC), long
  BTC at β = 1, same costs. Event − control isolates the listing effect from an alts-vs-BTC regime.
- **Blind:** unhedged short of the new coin. **Lag row:** entry at 01:00.
- **Statistics:** median vs mean, trimmed mean (1 % each tail), worst event, worst 1 % of events.
- **Inference:** bootstrap clustered by **entry ISO week**, because events in the same week share market moves.
- **Tables:** per year; per entry-day volume tercile; by token type (`underlyingSubType`, information).
- **Tests before the real run:**
  - leak test (nothing after the entry moves a decision);
  - P&L identity by hand;
  - delisting closure;
  - a planted world that passes;
  - ≥ 10 null worlds that pass ≤ 1 time;
  - a world with an alts-vs-BTC drift but no listing effect, which must fail the control criterion.
- **Information arms (never promoted):** H = 7 and H = 28; seasoned listings; liquidity floors $2 M and $10 M; entry at L + 24 h.

### Power, stated before the run

- If the 14-day hedged event return has sd ≈ 35 %, then n ≈ 470 gives SE ≈ 1.6 %, and ≈ 2 % with clustering.
- The CI clears 0 only for a mean of ≳ 4 % per event.
- The portfolio (≈ 100 events a year × 2 %) earns ≈ 2 × the mean per event per year, so 4 % per event ≈ 8 %/yr.
- 2024–25 hold 320 of the 472 events, so the per-year criterion is the binding one for older years.

### PASS (all required, H = 14 cell)

1. Event net mean > 0, with clustered-bootstrap 95 % CI lower bound > 0.
2. Event median > 0 and trimmed mean > 0.
3. Event − seasoned control > 0, with CI lower bound > 0.
4. Event net mean positive in every calendar year with ≥ 20 events except at most one (years: 2020, 2021, 2023, 2024, 2025, 2026; 2022 has 15).
5. 1 h lag row net mean > 0.
6. Portfolio:
   - annualised net ≥ 4.5 % on capital;
   - NW t ≥ 2;
   - worst month > −5 %;
   - worst day > −5 %.
7. n ≥ 300.

**Kill.** If any criterion fails, N5 is dead. The drift after a new token's perp listing, if it exists, is not a stable, harvestable premium after funding and
squeezes. Nothing (threshold, H, entry delay, filters) is re-tuned. A variant would be a new pre-registration scored only on data after 2026-09-30.

### Status (2026-10-12)

Pre-registered only. Next: `harness_xsec/listing.py` + `test_listing.py` (synthetic worlds), then `run_n5.py`. Metadata inputs to save with the harness:
the spot first-month map and the `exchangeInfo` snapshot (TradFi / index tags).

### N5 implementation details (fixed 2026-10-14, before the run; the pre-registration is silent on these)

- **Event set.** Reproduces the registered counts exactly in every year except one 2025 listing: **BOBUSDT**. The registration dropped it as a "re-listed base"
  (its base matches the earlier `1000000BOBUSDT`). The rule is kept as registered: a symbol whose base without a 1000 / 1M prefix was listed earlier is `relisted`
  and excluded. **Data finding:** the two prices differ by far more than a multiplier (0.0215 vs 0.0266 × 10⁻⁶), so BOBUSDT is a different token. The report gives
  the primary mean with it put back. The other base collision, `CATUSDT`, is a TradFi equity and was excluded anyway. "Existed in January 2020" = first bar
  before 2020-02-01 (11 symbols).
- **In sample / forward.** In sample = exit (entry + H) ≤ 2026-08-31 00:00 for each H. Forward = exits after that, up to the last archive bar (2026-10-01 00:00).
  With H = 28 the in-sample count is 472 by construction.
- **Funding in the event P&L.** A buy-and-hold leg pays or receives each settlement in (entry, end] on its value at the settlement, `r_s × P(s) / P(entry)`.
  A settlement exactly at entry is not paid; one exactly at exit is. The portfolio view marks on constant notional, so there funding is unweighted (as in R5 / N2).
- **Death.** A leg ends at the coin's last traded bar close (`xfund.death_hour`: ≥ 24 trailing zero-volume bars), or after its last bar if the file stops early.
- **Control.** At the entry day, take the point-in-time top 100 by 30-day quote volume among R4-eligible symbols. Drop BTC, TradFi and index perps, and keep coins
  first listed ≥ 365 days earlier. Each coin is a buy-and-hold short with its own death closure and value-weighted funding. The event's long BTC leg and 18 bp are
  added. The control is defined only when ≥ 10 coins qualify. Events without one (early 2020: no coin is yet 365 days old) are left out of criterion 3 and counted.
- **Lag row.** Selection is unchanged. Entry and exit both move by + 1 h, and the event set is the same.
- **Blind short** costs 9 bp (two sides).
- **Statistics.**
  - Bootstrap: 10,000 draws, seed 0, percentile CI, ISO week of the entry (`isocalendar`).
  - Trimmed mean drops ⌊1 % × n⌋ events from each tail.
  - Worst 1 % = mean of the worst max(1, ⌊1 % × n⌋) events.
  - Per-year criterion: by entry year.
- **Portfolio.**
  - Window: from the first in-sample entry day to 2026-08-30. Mean daily net × 365 = annualised; NW t uses 10 lags (`xsec.nw_t`).
  - The open cap is checked on the entry day against events still open that day.
  - Entry cost is booked on the first held day and exit cost on the last.
- **Volume terciles** are cut on the in-sample primary events' 24 h volume.
- **Token type** is the first non-"Crypto" `underlyingSubType` tag, or "delisted" when the symbol is no longer in `exchangeInfo`.

### Status (2026-10-14) — N5 executed: DEAD (2 of 10 criteria)

`n5_listing.analysis.md`; harness `harness_xsec/listing.py`, `run_n5.py`, `test_listing.py` (27/27).
- **Result.** 473 events. Event net mean **−3.77 %** per 14 days (CI [−21.9, +7.7]), but **median +14.6 %**, 71 % win, trimmed +5.9 %.
- **Why it dies.** 15 coins more than doubled in the hold, and COAIUSDT (+3,653 %) alone costs 7.4 % of the mean. 2025 is −13.3 %. Portfolio +2.0 %/yr, NW t 0.38,
  worst month −18.9 %.
- **Post-hoc (information only).** A +50 % buy-stop, triggered intrahour with a gap fill, gives +4.3 % per event (CI [+0.5, +8.0]). But it is only +1.9 % over the seasoned
  control (CI [−2.0, +5.8]), so it is mostly alts-vs-BTC exposure.
- **Not re-registered.** A stopped variant would be scored only on listings after 2026-09-30, and would need about 12–18 months of forward data.

---

## Round 8 — M1: mid-cap alt market-making feasibility from free L1 quotes (pre-registered 2026-10-12, before any code)

Context: BTC direction is closed at every horizon (`horizon_economics_and_next_ideas.md` §8): the 5 s–90 s signal is 9.4× too weak for the maker
fee, and even a perfect next-bar oracle reaches only 1.15 bp. The programme's measured binding constraints are the fee/move ratio, a signal
half-life of seconds, and a 50–150 ms execution latency from Greece. Rounds 6–7 failed as too fast to trade (R1, N4), too small in capacity
(`CARRY+`, R9), or unstable across years (N1, N2, N5-pending). M1 breaks a different assumption: instead of *predicting direction* it
*supplies liquidity* on instruments where the quoted spread is large relative to the fee. The programme's own close flagged it: "market making on
mid-cap alts (spreads 5–20 bp > fee; needs the event-stream collector pointed at alts)" (`next_signal_ideas.md`, Round 4, "Not pursued"). The best
asset the programme produced — the volatility detector (§4.7) — is used here as a *toxicity / quote-pull filter*, its documented "exact inverse" use.

**Facts fixed before writing (schemas and counts only; no spread, mark-out or P&L looked at).**
- Free historical L1 quotes exist only at
  `data.binance.vision/data/futures/um/daily/bookTicker/<SYM>/<SYM>-bookTicker-<YYYY-MM-DD>.zip`, cols
  `update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,transaction_time,event_time`.
  **Coverage 2023-05-16 → 2024-03-30, then discontinued** (verified against the S3 listing, 2026-10-12). 14–54 MB/symbol-day, ~1.0 M rows/day; a
  daily file may cover < 24 h (ARBUSDT 2023-05-16 spanned ~10 h 29 m–20 h 24 m UTC), so the coverage fraction is recorded per symbol-day.
- Signed trades: `.../aggTrades/<SYM>/<SYM>-aggTrades-<YYYY-MM-DD>.zip`, cols
  `aggTradeId,price,qty,firstTradeId,lastTradeId,timestamp,isBuyerMaker`; full history from 2019-12-31. Aggressor sign `d = +1` iff `isBuyerMaker=false`.
- `bookDepth` is **not** an L2 book (cols `timestamp,percentage,depth,notional`; ±% aggregate bands at ~30 s) → not used.
- Fee anchors: Binance VIP0 maker 2.0 bp / taker 5.0 bp per side; Liquidity-Provider rebate −0.1 … −0.5 bp. `COST_BP = 4.5` (`xsec.py`) is the taker anchor.

**Hypothesis.** On mid-cap perps the quoted spread is wide (5–20 bp) and the flow is less informed than on BTC, so a passive quoter's half-spread
capture exceeds adverse selection plus fee — a maker edge BTC (spread ≈ 1 bp against a 2 bp maker fee) cannot have. If true, the identical
measurement on BTC and the majors must be ≤ 0. That is the control.

**Universe (point in time, each sampled day at 00:00 UTC).** Binance USDT-M perps with R4 eligibility (≥ 60 days listed, complete prior-30-day 1 h
data, not stable / index), ranked by trailing 30-day quote volume. **Primary band = volume ranks 41–150** (mid-cap). **Comparator = BTCUSDT + the
top-10 by volume** (majors). A coin with no `bookTicker` file on a sampled day is skipped and counted.

**Sample (fixed).** Four calendar days — **2023-07-19, 2023-10-18, 2024-01-17, 2024-03-13** (Wednesdays inside the `bookTicker` window) — × every
eligible symbol in the band on that day, plus the comparator set. Every band coin is present on all four days it is eligible.

**Metrics (per symbol-day, then pooled equal-weight across symbols; all in bp).**
1. Relative quoted spread `qs = (a − b)/mid · 1e4`; median, mean, deciles.
2. Huang–Stoll decomposition on signed trades: effective half-spread `e = d·(p − m)`, realized `r(τ) = d·(p − m_τ)`, impact `= e − r`; the identity
   `e = r + impact` is checked exactly. τ ∈ {1, 5, 30} s.
3. Maker entry mark-out (Gatto decomposition): for a fill on the passive side at the touch, `s = −d`, `capture = s·(m0 − f)/m0`,
   `net(τ) = s·(m_τ − f)/m0`, and `net_after_fee(τ) = net(τ) − (fee_maker − rebate)`. Report τ ∈ {1, 5, 30, 60} s.
4. OFI (Cont–Kukanov–Stoikov, L1): `e_n` from `(P^B, q^B, P^A, q^A)`; regress the 1 s mid change on OFI; report R².

**Controls.** Blind / drift benchmark (unsigned mean move on the same bars); day-clustered bootstrap (resample whole sampled days) for every CI;
BTC/majors as the comparator; the identity and leak tests below.

**PASS (primary cell, mid-cap band, pooled equal-weight; all required).**
- `net_after_fee(τ = 5 s)` mean > 0 with a day-clustered bootstrap 95 % CI lower bound > 0;
- median relative quoted spread > 2 × maker fee (i.e. > 4 bp);
- `net_after_fee(5 s)` on the band > the same on the BTC/majors comparator;
- same sign on median and mean, and positive on ≥ 3 of the 4 sampled days.

**Kill.** Fail → mid-cap alt market-making is not viable at the reachable retail maker fee tier on free L1 data; the deferred idea is closed in this
form. A live-collected forward test (post-2024-03) is a new registration.

**Limitations (fixed now).** L1 only: no queue position, so fill probability is modelled as "a trade at the touch fills the resting touch quote"
(a back-of-queue bound). The window is 2023-05 → 2024-03 only. `bookTicker` coverage may be partial-day. Survivorship is handled by the
point-in-time R4 eligibility. Book-update vs trade causality is ambiguous from L1; the reference mid is read strictly before the fill timestamp.

**Tests before the real run.** Formula units on synthetic quotes/trades; the Huang–Stoll identity `e = r + impact` exactly; the mark-out identity
`capture + adverse = net`; OFI on a synthetic book; a leak test (nothing after t moves a decision at ≤ t); a planted maker-edge world that passes;
≥ 10 null worlds (iid mid) that pass ≤ 1 time; end-to-end on a synthetic market.

### Status (2026-10-14) — M1 executed: FAIL (4 of 5 criteria)

`m1_mmalt.analysis.md`; harness `harness_mm_alt/` (tests 0 failures). 484 symbol-days: 440 band, 44 BTC / majors.
- **Spread.** The band's median quoted spread is **2.2 bp** (the premise assumed 5–20 bp).
- **Mark-out.** A passive touch fill captures +1.91 bp and loses −4.31 bp to the 5 s mid move: **−2.40 bp before fees**, −4.30 bp after (CI [−4.91, −3.72]).
- **Breadth.** 0 of 440 symbol-days and 0 of 4 days are positive after fees, and every spread tercile is negative before fees. The band is worse than the majors (−3.75 bp).
- **Implication.** Break-even needs a ~2.4 bp-per-side maker rebate. Closed in this form; no fee tier rescues naive quoting.
- **Mid-run loader change.** The loader was made memory-lean after two OOM kills of the session. Its output is identical: 2 old rows re-derived on all 35 fields.

---

## Round 9 — R9-H: `CARRY−` on historical borrow rates; Q1: squeeze risk from positioning (pre-registered 2026-10-17, before any code)

Context: `new_features.md` (2026-10-17) lists new Binance data and proposes three more BTC-direction tests: §6 positioning at 4 h, §7 a trend gate on the 4 h rule, §8 a
model-class check.
- **§7 not registered.** Its read-only diagnostic (§7.2) was run first: a scratch script, not committed, on the 1,625 trades of `v1_4h_feasibility.analysis.md` §9,
  which it reproduces exactly.
  - Counter-trend trades do carry the losses: with-trend +3.21 bp gross (n 880, CI [−5.2, +11.6]), counter-trend −6.82 (n 745).
  - But the gated half misses the 4 bp maker fee and four of the five proposed §7.4 criteria, in sample: trend drift alone is +1.48, worst 1 % −407 vs −444, positive
    in 4 of 6 years.
- **§8.1 not registered on its own.** At 4 h the GBM stopped at iteration 0 and the linear signed-z sum beat it; at 90 s the MLP matched xgboost. Its success branch
  (+0.02 IC) would leave pooled IC near 0.06, below the ≈ 0.085 the fee needs.
- **§0 applied.** The liquidation / mark-price WebSocket URLs now use the routed `/market/ws/` path. Checked live on 2026-10-17: the legacy path delivers nothing; the
  routed path delivers `markPrice` and 54 of the 56 `forceOrder` events that `!forceOrder@arr` showed on the same 40 perps in 3 minutes.

Round 9 aims at the two places where the programme found money:
- `CARRY−`, the only positive net at retail size, blocked by unknown historical borrow rates (R9);
- the right tail that sank the single-coin short books (R4, N5).

### R9-H — `CARRY−` on the borrow rates actually charged

**Facts fixed before writing.**
- Endpoint `GET /sapi/v1/margin/interestRateHistory`: `USER_DATA`, HMAC-SHA256 signed, weight 1. Parameters `asset`, `vipLevel=0`, `startTime`, `endTime` (≤ 30 days
  apart). How far back it goes is not known; finding that is part of the run.
- **Needs a read-only API key** (reading only; trading, margin and withdrawals disabled), passed as the `BINANCE_KEY` / `BINANCE_SECRET` environment variables and never
  written to the repo, a log or an output file. The responses are market rates, not account data; they are cached raw, with the fetch time, under `data/borrow_api/`
  (not in git).
- R9 S2 (today's rates, today's list): +18.88 %/yr, 2024 −0.51 %, break-even at 2.83× today's rates. 193 coins appear in R5 `CARRY−` episodes; 43 are not on today's
  list (14.7 % of position-days).
- One public snapshot exists, `data/borrow_history/borrow_2026-10-05.json.gz`; it is the cross-check below.

**Question.** Does `CARRY−` earn more than the risk-free rate when it pays the borrow rate charged at the time, on coins that could be borrowed at the time?

**Construction (R5 unchanged except for borrow).**
- **Assets fetched:** the base asset of every coin that meets the `CARRY−` entry condition (FUND7 ≤ −0.03 % per 8 h inside the R5 universe) on at least one day
  2020-10-14 → 2026-08-30. The list comes from the R5 code before any request. Base-asset mapping as R9 (strip `1000` / `10000` / `1000000` / `1M`).
- **Requests:** contiguous 30-day windows per asset from 2020-10-01 to 2026-09-30, at most 5 requests a second.
- **Daily rate r[d, j]:** the mean `dailyInterestRate` of the records stamped in UTC day d. If day d has no record, use the last record of the previous 3 days;
  otherwise there is no rate.
- **Point-in-time borrowability:** B[d, j] = r[d, j] exists. This replaces R9's "on today's list", which is a survivor list.
- **Book:** R5 `CARRY−` (signal, entry and exit, 10 slots, 3× perp, 24 bp round trip, funding as P&L) on eligibility ∧ B. Each open position pays r[d, j] on its
  spot-short notional (`carry.N_SLOT` = 0.075 of capital) for every day it is open. A position whose coin stops being borrowable closes that day (a recall).
- **Window:** from the first UTC day on which BTC, ETH and BNB all have a rate (the endpoint's depth) to 2026-08-30. Earlier days are not used.

**Rows (same window).**
- (a) primary;
- (b) historical rates on today's list, which separates the rate effect from the list effect;
- (c) R9 S2 recomputed on the window;
- (d) R5 gross;
- (e) primary with execution at 01:00 (the R5 latency control).

**Reported.**
- Net %/yr on capital, per calendar year, NW t (10 lags), worst month, max drawdown, mean positions.
- Borrow cost %/yr, the mean paid rate on held positions, and the uniform multiple of the historical rates at which net = 4.5 %.
- Share of R5 position-days not borrowable at the time; share of the top three coins.
- Paid rate on held days with FUND7 ≤ −0.10 % vs other held days (the concern that rates spike in squeezes).

**Tests before the real run (network mocked).**
- Signing against Binance's published example: query `symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559`
  with the documented secret gives `c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71` (checked 2026-10-17).
- The windows cover every day exactly once.
- The day-rate rule: mean within the day, 3-day carry, otherwise none.
- Equivalence: with r = today's constant rates and B = today's list, the code reproduces R9 S2 to the basis point.
- Leak test: scrambling rates after day T leaves everything at ≤ T unchanged.
- Exact P&L identity.

**Checks on the real data before scoring (stop and report if either fails).**
- Every record has `vipLevel` 0.
- For assets in both, the history's 2026-10-05 rate is within 10 % (relative) of that day's public snapshot for ≥ 90 % of assets.

**Decision (tiers fixed now).**
- **PASS (all required):**
  - net ≥ 4.5 %/yr;
  - positive in every full calendar year of the window;
  - worst month > −5 %;
  - NW t ≥ 2;
  - row (e) positive;
  - break-even multiple ≥ 1.5.

  Next step: a new registration for a forward ledger of live candidates plus a small live pilot sized inside the VIP0 borrow limits.
- **ALIVE-forward:** every PASS criterion except "every year", with exactly one negative year that is > −2 %. Next step: a forward paper ledger only, no money. This
  tier exists because 2024 was −0.51 % at today's rates (R9 S2). It is written down now so that it cannot be added after the numbers.
- **DEAD:** anything else. `CARRY−` is closed.
- **UNDERPOWERED:** fewer than 3 full calendar years of history. Descriptive only.

**Not tested here.**
- Capacity: the history has no limits. Today's VIP0 limits ($1.5k–5.6k per coin) cap the deployable capital at roughly $10–25k.
- Whether borrow was actually available when a rate was published (pool exhausted, recalls).
- Margin operations: collateral split between the margin and futures wallets, top-ups when a coin pumps.
- Account eligibility: before any pilot, the user confirms in the Binance UI that Cross Margin borrowing is available on the account.

**Prior.** Positive on the rate question, since R9 breaks even at 2.8× today's rates. The risks are the every-year bar and rate spikes during squeezes.

#### R9-H implementation details (fixed 2026-10-17, after format checks on the real data, before any P&L)

**Format facts found with the key.** These came from record timestamps and value counts only; no P&L was computed.
- **Key:** `apiRestrictions` shows reading only. Spot / margin trading, futures, withdrawals and transfers are all off; there is no IP restriction.
- **Depth:** the history reaches 2020-09-30 for BTC.
- **Cadence:** one record a day at 00:00 UTC through 2023-02. From **2023-03-01** there is also a record at every intraday rate change (BTC 48–101 distinct values a month, TRB 474). No truncation was seen, with up to 550 records per 30-day request.
- **Not-listed windows:** HTTP 400 `-11027 asset … is not supported` is returned **per window**. It comes back for any window before the asset joined cross margin
  (1INCH, ACH, AGLD in 2020-10; 0G, ACE, AEVO also in 2023-06), and for every window of an asset never on margin (CARV, BWET). Such a window has no records,
  so the asset is not borrowable in it. (First found as a bug: the fetcher stopped an asset's fetch at its first `-11027`; corrected and re-fetched before any
  P&L.)
- **Removed assets keep a frozen rate.** An asset taken off cross margin keeps a daily record whose value no longer changes:
  - SRM, BTS, ANC: one value a month through 2026-09;
  - WAVES: frozen from 2025;
  - FTT: frozen 2023–24, live again 2025.

  So the registered B ("a rate exists") would also count delisted assets as borrowable.
- **Calibration** on the 379 assets of the 2026-10-05 cross-margin list that have a history (last 30 days):
  - 6 (1.6 %) kept one value for all 30 days: AERO, ESP, MARSCOIN, RE, WBETH, 牛来, which are pegged or newly listed.
  - WAVES, SRM, BTS and ANC kept one value; FTT, TRB and MANA changed 193–474 times.
  - A rule based on intraday activity is worse: 15 % of listed assets went more than 7 days without an intraday change.

**Amended B (primary).** B[d, j] = a rate exists on day d (the registered rule) **and** the asset is not frozen.
- Frozen applies from 2023-03-31, 30 days into the intraday regime: every record in [d − 30 d, d) has the same value.
- Before 2023-03-31 a freeze cannot be seen, because rates then changed rarely even for BTC. An asset taken off margin while its spot pair kept trading therefore counts as
  borrowable in that period; row (b) bounds this.
- The registered rule is reported as **row (f), "rate exists"**. The verdict uses the amended primary, and the report states whether rows (b) and (f) land in the same
  tier.

**Other details.**
- **Windows:** [2020-10-01 + 30k days, + 30 d − 1 ms], 74 windows to 2026-09-30, plus one window 2026-10-01 → 2026-10-15 used only for the snapshot cross-check.
- **Errors:** a `-11027` reply is cached for its window as a window without records, and the fetch continues; an asset with every window `-11027` is never
  borrowable. 418 / 429 / 5xx replies and network errors are retried with backoff. At most 5 requests a second across threads.
- **Day rate and timing:** the day rate uses records stamped in [d, d + 1 d) UTC, and a position held over (d, d + 1] pays r[d]. Borrowability at d uses day d's
  records; the daily 00:00 record is known at the decision time.
- **Window end:** 2026-08-30, as registered. The archive now runs to 2026-09-30; the later days are not used.
- **Row (c):** uses the saved R9 snapshot (`harness_xsec/r9_borrow_snapshot.json`, 2026-10-06), so that it reproduces R9 S2. Row (b) uses the same list.
- **Assets:** the base assets of coins with elig ∧ FUND7 ≤ −0.03 % on at least one day of the window, computed from the R5 panels.

### Q1 — squeeze risk from positioning (`metrics` archive), aimed at the short-book tail

**Facts fixed before writing (listings and schemas only; no prices, returns or pumps looked at).**
- `data.binance.vision/data/futures/um/daily/metrics/<SYM>/<SYM>-metrics-<YYYY-MM-DD>.zip`. Daily files only (there is no monthly dataset).
  - Columns `create_time, symbol, sum_open_interest, sum_open_interest_value, count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio,
    count_long_short_ratio, sum_taker_long_short_vol_ratio`.
  - 5-minute rows; every column filled on the five days sampled (2020-09 → 2026-10).
- **Coverage.**
  - 1,038 symbols. BTCUSDT from 2020-09-01; ETH, SOL and DOGE from 2021-12-01; no missing days in those four listings.
  - Delisted contracts are kept: SRM, LUNA, FTT, WAVES, MATIC, FTM, ANC, BTS, RNDR, OCEAN, AGIX, EOS and MKR were checked.
  - 375 of the 376 symbols ever in the R4 top-40 have a directory. LENDUSDT, delisted in 2020, has none.
- **Quirks.**
  - The 2020 files hold duplicate rows (576 a day).
  - Recent files are not sorted by `create_time`.
  - Files continue for months after a delisting.
  - One stray key exists (`MATICUSDT-metrics-1993.zip`).

  Rule: keys must match `-metrics-YYYY-MM-DD.zip`; rows are de-duplicated on `create_time` and sorted.
- **Column meanings (checked against REST below):**
  - `count_long_short_ratio` = global account long/short ratio;
  - `sum_toptrader_long_short_ratio` = top-trader position ratio;
  - `count_toptrader_long_short_ratio` = top-trader account ratio;
  - `sum_taker_long_short_vol_ratio` = taker buy/sell volume ratio;
  - `sum_open_interest` = open interest in coin units.

**Hypothesis.**
- The single-coin short books lost their median gains to a few pumps:
  - R4 `RVOL30` H 1: median +22.6 bp/day, mean −7.8;
  - N5: median +14.6 % per event, mean −3.8 %, with COAI +3,653 %.

  N1's losses were market-wide alt rallies instead; Q1 does not address those.
- A squeeze needs fuel: leveraged positions that must be closed. So a coin whose open interest has grown, and whose accounts lean short relative to its own history,
  is more likely to pump in the next week than a coin with the same volatility and funding.

**Score (signs fixed by the mechanism; no fitting, no tuning).** At day d 00:00 UTC, from the last row with `create_time` ≤ d − 10 min:
- C1 = log(OI[d] / OI[d − 7 d]), where OI = `sum_open_interest`. It is in coin units, so price moves do not drive it.
- C2 = −z30(`count_long_short_ratio`): the coin's value minus the mean of its previous 30 daily values, divided by their sd. It needs ≥ 20 of the 30 values.
- **S** = mean of the cross-sectional percentile ranks of C1 and C2 within the day's universe (coins with both defined).
- Information only:
  - each component alone;
  - the top-trader minus global divergence, z30(`sum_toptrader_long_short_ratio`) − z30(`count_long_short_ratio`);
  - z30 of the 24 h mean taker ratio.

**Stage A — does S predict pumps beyond volatility and funding? (primary)**
- **Grid.** Mondays at 00:00 UTC, **2022-01-03 → 2026-08-24 (243 weeks)**, giving non-overlapping 7-day windows per coin. Forward block: Mondays from 2026-08-31 whose
  window is inside the archive, reported separately.
- **Universe.** The point-in-time top 100 by 30-day quote volume among R4-eligible perps, minus BTC and the TradFi / index perps (N5 tags). A coin-week without S is
  dropped and counted.
- **Label.** Pump = the highest 1 h high in the next 168 hours is ≥ 1.5 × the close at entry. Information labels: close-to-close 7-day return ≥ +50 %; thresholds of
  +30 % and +100 %.
- **Strata.** Each Monday, RVOL30 quintiles × FUND7 terciles within the universe (R4 definitions), giving 15 cells. In each cell, flag the round(n/3) coins with the
  highest S.
- **Statistic.** **Lift** = pumps flagged / pumps expected to be flagged. Each pump adds round(n/3)/n of its cell to the expectation. Lift 1 means no information
  beyond volatility and funding.
- **Inference.**
  - Bootstrap over weeks: 10,000 draws, seed 0.
  - Permutation null: shuffle S within each (week, cell), 2,000 draws.
- **Reported.** Lift by year; unstratified AUC of S, RVOL30, −FUND7 and each component; counts of pumps and coin-weeks.

**Stage A PASS (all required).**
1. Lift ≥ 1.20, with the bootstrap 95 % CI lower bound > 1.0.
2. Permutation percentile ≥ 97.5.
3. Lift > 1 in ≥ 4 of the 5 calendar years 2022–2026.

**Stage B — does the filter fix a registered short book? (run only if stage A passes)**
- **Book.** R4 `RVOL30`, H = 7, exactly as registered:
  - top-40 universe;
  - long the 8 lowest-RVOL30 coins, short the 8 highest, equal weight;
  - 7 staggered sub-books;
  - 4.5 bp per side, funding as P&L;
  - delisted coins closed at their last price.

  R4 OOS: net −7.8 bp/day. The horizon matches the 7-day label.
- **Filter.** Each day, flag the coins in the top third of S within the top-40 universe. The short leg becomes the 8 highest-RVOL30 *unflagged* coins; the long leg is
  unchanged. A coin with no S is not flagged.
- **Window.** 2022-01-01 → 2026-08-30; forward days reported separately.
- **Null.** Each day, flag the same number of coins at random and apply the same replacement rule (500 draws, seed 0). Information arm: flag by most-negative FUND7
  instead of S.
- **Stage B PASS (all required).**
  1. Paired improvement (filtered − unfiltered daily net) with NW t ≥ 2, and above the 97.5th percentile of the random-flag null.
  2. Filtered book net > 0 with NW t ≥ 2.
  3. The filtered book's worst 1 % of days is less severe than the unfiltered book's.
  4. Filtered net positive in ≥ 3 of the 4 years 2022–2025.
  5. Filtered net > 0 with the 1 h lag.

**Tests before the real run.**
- The zip reader handles the quirks (duplicates, unsorted rows, stray keys).
- Known-at rule: no row after d − 10 min moves anything at d.
- Units of z30 and the ranks.
- Leak test: scrambling metrics and prices after d leaves S and the flags at ≤ d unchanged.
- Null worlds (≥ 10) give lift ≈ 1 and pass at most once.
- A planted world where pumps follow high C1 and low global L/S, at fixed volatility and funding, passes stage A.
- A world where pumps follow volatility only fails stage A (lift ≈ 1).
- Stage B equivalence: with no coin flagged, the book reproduces R4 `RVOL30` H 7 exactly.

**Check on the real data before scoring.** Compare 30 days of archive rows with the REST endpoints (`globalLongShortAccountRatio`, `topLongShortPositionRatio`,
`topLongShortAccountRatio`, `takerlongshortRatio`, `openInterestHist`, period 5m) for BTCUSDT and two alts. If `count_long_short_ratio` does not match the global
account ratio, or `sum_open_interest` does not match open interest, stop and re-register C1 / C2 before computing any score.

**Power, stated before the run.**
- **Stage A.** About 100 coins × 243 weeks ≈ 24,000 coin-weeks. The pump base rate has not been looked at; at 1–3 % it gives roughly 240–730 pumps. With 400 pumps the
  SE of the flagged share is about √(⅓ · ⅔ / 400) ≈ 2.4 points, more with clustering. So a lift of 1.20 (40 % vs 33 %) is at the edge of detection, and 1.35 is clear.
- **Stage B.** If the paired daily difference has sd ≈ 1.5 %, about 1,700 days give SE ≈ 4 bp/day before the NW correction. The filtered book's own daily sd is about
  5–6 %, so "net > 0 with NW t ≥ 2" needs roughly +25 bp/day. That is the binding criterion, and its prior is low.

**Verdicts.**
- **Stage A fails → Q1 DEAD.** Positioning does not predict weekly pumps beyond volatility and funding.
- **Stage A passes, stage B fails → risk information only.** S can size or skip the shorts of a future book; it is not a strategy.
- **Both pass → a forward paper ledger** of the filtered book (a separate registration); no money before it scores. Stage B's in-sample days were already seen
  through R4, so stage B alone is not a trading result.

**Prior.** Stage A moderate: crowding before squeezes is widely described, but funding already carries much of it. Stage B low.

### Status (2026-10-17)

- Pre-registered only. The §0 WebSocket fix is applied and was checked live (above).
- **R9-H** waits for the user's read-only key.
- **Q1** next: a `metrics` fetcher with the REST cross-check, `harness_xsec/squeeze.py` + `test_squeeze.py` (synthetic worlds), then `run_q1.py`.
- Q1's daily files are about 1–2 × 10⁵ requests (every coin-day in the top-100 from 2021-12, plus the 30-day lookbacks). Fetch them resumably, with a few threads.
