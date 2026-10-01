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
