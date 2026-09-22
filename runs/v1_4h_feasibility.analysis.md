# 4-hour feasibility check on the v1 year — a real, stationary mean-reversion signal that still misses the fee floor by 3–5×

*2026-09-22. Data: `data/v1_year/v1_5min.pkl` (92,892 five-minute bars, 334.8 days, 96.3 % coverage — see `v1_year_data_audit.md`). Harness: `runs/harness_v1_feas/{build_v1_5min.py,feas_4h.py}`. CPU only, ~3 min end to end.*

**TLDR:** the 4 h horizon has, for the first time in this project, a **large, stationary,
out-of-sample directional signal** — `range_pos_24h` carries a daily-IC t of **−15.4 in
train and −17.9 in test** with mean IC ≈ **−0.34** in both halves, and 24 of 29 features
keep their IC sign across the split. It is short-horizon **mean reversion**: long when
price sits low in its 24 h range and has recently fallen.

It is still not tradeable. **Per tradeable entry it earns +1.5 to +2.7 bp against a 7 bp
cost floor.** The much larger per-signal figures (+13.8 bp at the 5 % tail) are inflated
roughly **9×** by 48-fold window overlap and do not survive a one-position-at-a-time
simulation.

---

## 1. The signal is real and stationary

Univariate daily-IC t, computed separately on the train region (2025-09-16 → 2026-01-21)
and the test region (2026-01-21 → 2026-08-14):

| feature | t (train) | t (test) | IC train | IC test | sign holds |
|---|---|---|---|---|---|
| `range_pos_24h` | −15.42 | **−17.86** | −0.337 | **−0.344** | yes |
| `ret_norm_24h` | −13.03 | **−19.11** | −0.343 | **−0.367** | yes |
| `ret_norm_4h` | −7.83 | −13.94 | −0.168 | −0.244 | yes |
| `lsr_z` | +6.13 | +9.15 | +0.184 | +0.204 | yes |
| `ret_norm_1h` | −5.87 | −10.95 | −0.089 | −0.141 | yes |
| `basis_bp` | +2.87 | +5.53 | +0.079 | +0.118 | yes |

**24/29 features keep their IC sign out of sample**, and 11/29 clear |t| ≥ 3. For scale,
the best 90 s signal in the project is IC +0.06. This is 5× larger and it does not decay
across the split.

The top three are all price-shape features (range position, 24 h and 4 h momentum), and
all negative — i.e. **mean reversion**, not momentum. The slow features that motivated
fetching the year (`lsr_z`, `basis`, `oi_chg`, `liq_imb`) are significant but secondary.

## 2. Why the GBM found nothing, and what did

The MSE-objective GBM early-stopped at `best_iteration = 0` in **7 of 8 folds** across both
arms and produced a *negative* pooled OOS IC. That is not evidence of no signal — it is the
wrong objective: the relationship is strong in **rank** and noise-dominated in **magnitude**,
so val-RMSE never improves. A sign-classifier did little better (OOS IC +0.0002).

A trivial model works far better: z-score the top-k train features over a rolling 7 days,
sign each by its **train-region** IC, sum. Out of sample:

| k | OOS pooled IC | daily-IC t | gross (per signal) |
|---|---|---|---|
| 1 | +0.0212 | +15.79 | −0.29 bp |
| 3 | +0.0406 | **+19.87** | +0.04 bp |
| 8 | +0.0412 | +18.77 | +0.89 bp |

Note the gap already visible here: **daily-IC t of +20 and gross of ≈ 0 bp.** Within-day
rank skill does not equal P&L.

## 3. The tail looked tradeable — and was an overlap artifact

Per-signal economics of the k = 3 combo, out of sample, `E|move| = 64.7 bp`:

| selection | n | acc | gross/signal | net @7 bp | 95 % CI |
|---|---|---|---|---|---|
| top/bot 50 % | 52,377 | 0.497 | −0.45 | −7.45 | [−5.22, +3.93] |
| top/bot 25 % | 26,190 | 0.516 | +2.00 | −5.00 | [−4.88, +8.25] |
| top/bot 10 % | 10,476 | 0.555 | +6.88 | −0.12 | [−0.43, +14.59] |
| **top/bot 5 %** | 5,238 | 0.608 | **+13.78** | **+6.78** | **[+4.89, +23.44]** |
| **top/bot 1 %** | 1,048 | 0.709 | **+29.04** | **+22.04** | **[+11.90, +50.22]** |

Monotone in selectivity, CIs excluding zero, spread over 160 days with a 3.6 % max-day
share, **positive in all 8 test months**, and at the **100th percentile** of a
sign-permutation null (model +13.78 vs null p97.5 +2.57). Every control this project uses
passes.

**And it is still wrong.** A 4 h horizon on 5-min bars means each move is counted up to
**48 times**, once per bar in the episode. You cannot hold 48 overlapping positions; you
enter once. Simulating one position at a time — enter on signal if flat, hold 4 h, exit:

| selection | trades | win % | **gross/trade** | net @4 bp | net @7 bp | net @10 bp |
|---|---|---|---|---|---|---|
| top/bot 10 % | 489 | 53.4 % | **+2.73** | −1.27 | −4.27 | −7.27 |
| top/bot 5 % | 296 | 55.4 % | **+1.46** | −2.54 | −5.54 | −8.54 |
| top/bot 1 % | 85 | 56.5 % | +16.58 | +12.58 | +9.58 | +6.58 |

**The 5 % cell collapses from +13.78 to +1.46 — a 9.4× overstatement.** The mechanism: the
signal keeps firing as price extends further, so late-episode signals have the best forward
returns, and entering at the *first* signal gets the episode average instead.

## 4. The one surviving cell is drift, not skill

`top/bot 1 %` clears fees (+9.58 net @7 bp) on 85 trades. The leg split disqualifies it:

| selection | long leg | short leg | both > 0? |
|---|---|---|---|
| top/bot 10 % | +6.31 | +7.45 | yes — balanced |
| top/bot 5 % | +18.45 | +9.11 | yes — balanced |
| **top/bot 1 %** | **+55.22** | **+2.86** | technically, but ~all of it is the long leg |

At the 1 % tail `P(up) = 0.617` and always-long earns **+26.18 bp** versus the strategy's
+29.04 — excess of only **+2.86**. That cell is long-the-dip in a rising window, i.e. beta.
The 5–10 % cells are genuine two-sided mean reversion, and they are the ones that fail.

## 5. Verdict

**Criterion A: passes** — a stationary, out-of-sample directional signal exists at 4 h,
which is the first time in this project. **Criteria B and C: fail.** Per tradeable entry
the robust cells earn **+1.5 to +2.7 bp** against **7 bp** (mixed) or **4 bp** (full-maker,
which is reachable at a 4 h hold where adverse selection is ~8 % of the move rather than
27 %). The shortfall is **3–5×**.

That is much closer than anything before — `c* = 0.214 bp` vs 2.0 bp was 9.4× at 5 s, and
required accuracy at 90 s was above 1.0 — but it is still a real gap, not a rounding error.

## 6. Method note worth carrying forward

**Per-signal averaging over overlapping windows is not a P&L.** At a 4 h horizon on 5-min
bars the inflation is ~9×. Every economic claim must come from a one-position-at-a-time
simulation. This project's 90 s gate tables are less exposed (0.1 % triggers overlap
rarely) but the principle now has a measured magnitude, and it defeated *every* other
control — day-clustered CIs, sign-permutation null, month-by-month stability, blind
benchmark. None of them detect overlap inflation; only the trade sim does.

## 7. What would close a 3–5× gap

Untested, in rough order of promise:

1. **Better entry within the episode.** Entering at the first signal is the worst case.
   A causal rule that waits for the score to stop extending (a "reversal confirmed" trigger)
   could capture more of the +13.78 the late-episode signals see.
2. **Exit on signal decay rather than a fixed 4 h hold.** The 4 h horizon was chosen for the
   fee arithmetic, not because the reversion completes there.
3. **Full-maker execution.** At a 4 h hold a patient limit is realistic, taking the floor to
   4 bp; the 10 % cell then needs only +1.3 bp more.
4. **Position sizing by signal strength** rather than a binary ±1, which the per-signal IC
   suggests is where the information is.

None of these is guaranteed, and the honest prior after 14 runs is that a 3–5× gap does not
close. But unlike every previous branch, the signal here is real, large and stationary, and
the remaining problem is execution rather than prediction.

---

# §8 — Implementation of the §7 ideas (2026-09-22)

*Harness: `runs/harness_v1_feas/strat_4h.py`. Event-driven, one position at a time, real
path-dependent exits, three-way time split (signs from SIGN, every rule/threshold chosen on
VAL, TEST evaluated once), then a 4-fold walk-forward with per-fold re-selection.*

**Result: the tuning ideas do not replicate. The untuned rule is the better object, and it
lands at +10.17 bp gross / +6.17 net at maker fees — with a CI that includes zero.**

## 8.1 Single frozen split — the `confirm` entry looked like it worked

Entry rules implemented: `first` (baseline), `confirm` (enter only once |score| stops
extending), `wait_k` (k consecutive bars above threshold), `tick` (price already turning).
Exits: fixed hold, `decay` (|score| falls below a fraction of threshold), `flip`, TP/SL
brackets. Sizing: flat vs ∝|score|. 224 configs, selected on VAL, one TEST evaluation.

| variant | n | gross | net maker | net mixed | 95 % CI (gross) |
|---|---|---|---|---|---|
| baseline (first entry, 4 h hold) | 134 | +5.74 | +1.74 | −1.26 | [−4.85, +16.59] |
| frozen best-on-VAL (`confirm`, sized) | 103 | **+12.08** | +8.08 | +5.08 | [+0.01, +26.46] |

A 2.1× improvement, exactly as §7.1 predicted. But the leg split was **long +16.96 vs short
+2.00**, only 3/5 months were positive, and the VAL estimate (+24.3 net) shrank to +5.1 on
TEST — the signature of picking the argmax of 224 noisy estimates.

## 8.2 Walk-forward kills it

Four disjoint test blocks, signs **and** config re-chosen from each fold's own past:

| fold | test period | selected config | VAL net | TEST gross | baseline gross |
|---|---|---|---|---|---|
| 0 | 01-09 → 03-05 | `wait_k`/time/96/q0.90/sz | +31.2 | **−12.77** | −2.58 |
| 1 | 03-05 → 05-02 | `first`/time/48/q0.95/sz | +1.3 | +23.91 | +23.91 |
| 2 | 05-02 → 06-23 | `first`/time/96/q0.95 | +19.4 | **−4.28** | +13.32 |
| 3 | 06-23 → 08-15 | `first`/time/96/q0.95/sz | +22.3 | +3.06 | +5.13 |

VAL and TEST are uncorrelated (+31→−12.8, +1.3→+23.9, +19.4→−4.3, +22.3→+3.1), and the
selected config **underperforms the untuned baseline in 3 of 4 folds**.

| pooled | gross |
|---|---|
| per-fold selected config | **+2.91 bp** |
| untuned baseline | **+10.17 bp** |

**Tuning costs 7 bp per trade.** The §7 ideas are not wrong in principle — `confirm` really
did help on one split — but they cannot be selected reliably from this much data, and the
selection process destroys more than the rules add.

## 8.3 The honest object: the untuned walk-forward rule

Threshold = q95 of |score| on the preceding window; top-3 feature signs from the preceding
region; enter on first crossing; hold 4 h; one position at a time. **No tuning at all.**

| | |
|---|---|
| trades | 202 over 126 days, win 55.0 % |
| gross | **+10.17 bp**, median +9.72, 95 % CI **[−1.86, +22.61]** |
| leg split | long n=112 **+12.30** · short n=90 **+7.51** — both positive, balanced |
| per fold | −2.6 · +23.9 · +13.3 · +5.1 (3/4 positive) |
| by month | −14, +7, +39, +14, +10, +16, +6, −4 (**6/8 positive**) |
| sign-permutation null | model at the **90.3rd** percentile (needs > 97.5) |
| net @ maker 4 bp | **+6.17**, CI [−5.86, +18.61] → **fail** |
| net @ mixed 7 bp | +3.17, CI [−8.86, +15.61] → fail |

This passes every *structural* check that killed the earlier candidates — walk-forward with
no lookahead, one-position accounting (no overlap inflation), a **balanced two-sided leg
split** (so it is not drift), and stability across folds and months. It fails only on
**significance**: 202 trades against ~45 bp of per-trade noise cannot resolve a 10 bp edge.

Closing that needs roughly **4× the trades** — the CI half-width scales as 1/√n, so ~800
trades ≈ 3–4 years at this rate.

## 8.4 The finding that matters most

**The winning signal does not use any of the data that was expensive to obtain.** The three
features carrying it are `range_pos_24h`, `ret_norm_24h`, `ret_norm_4h` — position in the
24 h range, and 24 h / 4 h normalised returns. All three are computable from **plain OHLC
klines**. The open interest, long/short ratio, book ladder and funding — the unobtainable
part of the year — are statistically significant but secondary, and none reaches the top 3.

So the power problem has a free solution: **Binance serves 5-min klines back to 2019/2020.**
The same strategy can be evaluated on 5+ years — ~1,000+ trades, CI half-width ~±5 bp —
today, with no collector and no proprietary data.

## 8.5 Next step

Re-run §8.3 unchanged on **5 years of public 5-min klines**. It is the same code path: the
feature builder needs only `high`, `low`, `close`.

- If the edge holds near +10 bp gross over ~1,000 trades, the CI excludes zero and this is
  a real, tradeable, maker-fee-viable mean-reversion strategy — the first in the project.
- If it decays toward zero, the year-long result was a favourable window and the direction
  line closes for good.

Either way it is decisive, free, and answerable in an afternoon. **Do not tune it** (§8.2);
run the untuned rule.

---

# §9 — 7-year validation on public klines: FALSIFIED (2026-09-22)

*Data: `data/btcusdt_5m_klines.pkl` — BTCUSDT USDT-M perp, 5-min, **740,451 bars,
2019-09-08 → 2026-09-22, 2,571 days, 100.00 % coverage** (zero missing bars). Harness:
`runs/harness_v1_feas/{fetch_klines.py,validate_5y.py}`. The §8.3 rule, unchanged and
untuned, 8 walk-forward folds.*

**The one-year result does not replicate. Over 7 years the edge is `−1.39 bp` gross.**

## 9.1 Result

| fold | test period | n | gross |
|---|---|---|---|
| 0 | 2021-10 → 2022-06 | 237 | **−10.32** |
| 1 | 2022-06 → 2023-01 | 195 | +8.88 |
| 2 | 2023-01 → 2023-08 | 211 | **−12.55** |
| 3 | 2023-08 → 2024-04 | 206 | **−10.52** |
| 4 | 2024-04 → 2024-11 | 195 | +6.70 |
| 5 | 2024-11 → 2025-06 | 197 | +5.97 |
| 6 | 2025-06 → 2026-02 | 203 | +1.83 |
| 7 | 2026-02 → 2026-09 | 181 | +2.33 |

| pooled | |
|---|---|
| trades | **1,625** over 1,048 days (8× the v1 sample) |
| gross | **−1.39 bp**, 95 % CI [−7.72, +4.89] |
| win rate | 54.9 % |
| **median** | **+11.47 bp** |
| leg split | long +1.79 · **short −4.80** |
| by year | 2021 −31.1 · 2022 +4.4 · 2023 −10.8 · 2024 +4.1 · 2025 +1.6 · 2026 +1.5 |
| sign-permutation null | model at the **33.2nd** percentile (needs > 97.5) |
| net @ maker 4 bp | −5.39, CI [−11.72, +0.89] → **fail** |

Against the decision rule fixed before the run: CI lower bound > 4 bp — **no** (−7.72);
both legs positive — **no** (short −4.80); years positive ≥ 5/7 — **no** (4/6);
sign-perm > 97.5 — **no** (33.2). All four fail.

The top-3 features were `range_pos_24h`, `ret_norm_24h`, `ret_norm_4h` in **every one of
the 8 folds**, so this is the same signal the v1 year found, evaluated properly.

## 9.2 Why it looked good on one year — and the mechanism

**Win rate 54.9 %. Median trade +11.47 bp. Mean trade −1.39 bp.**

The strategy wins more often than not and still loses money. The return distribution is
heavily left-skewed: many modest winners, a thin tail of large losers that eats the mean.
That is buying dips in a downtrend — mean reversion gets run over precisely when the move
is not a dip.

The fold pattern is the same story: the two worst blocks are **2021-10 → 2022-06 (−10.32)**
and **2023-01 → 2023-08 (−12.55)**, i.e. the drawdown regimes. The v1 year
(2025-09 → 2026-08) happened to contain no such episode, which is exactly why it showed
+10.17 with 6/8 months positive.

This is the **same payoff asymmetry `economics_and_metrics.md` §7.5 identified at 90 s**
("win capped, loss uncapped — that is why the loss is a spread while the win is a
ceiling"), reappearing at a 4-hour horizon with a different signal. It was not a
90-second phenomenon; it is a property of the trade structure.

## 9.3 What this settles

- **The 4 h mean-reversion line is closed.** A real, stationary rank signal (IC −0.34,
  daily-t −18, stable out of sample) converts to a **negative** mean P&L over 7 years.
  IC ≠ economics, now demonstrated at a third horizon.
- **The v1 year's +10.17 bp was a favourable-window artifact.** 202 trades could not
  distinguish it from zero, and 1,625 trades say it is zero or below. The CI that
  "included zero" was doing its job.
- **Section 8.4's inference was right and its conclusion was wrong in the best way.** The
  signal really was computable from free klines — which is what made the falsification
  cost an afternoon instead of a year of collection.

## 9.4 Method note

Every structural control this project developed passed on the one-year sample: walk-forward,
one-position accounting, balanced leg split, 6/8 months positive, day-clustered CI. Only
**sample length** separated the artifact from the truth. The sign-permutation percentile was
the one statistic that stayed honest throughout — 90.3 on the year (already below its 97.5
bar) and 33.2 over 7 years.

**Ranked by what actually caught errors in this project:** (1) out-of-sample sample length,
(2) non-overlapping trade accounting (§8.3, caught a 9.4× inflation), (3) the blind /
drift benchmark (`gbm_probe.analysis.md` §3), (4) the sign-permutation null. Day-clustered
CIs and month-by-month stability caught nothing that the others missed.
