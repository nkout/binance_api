# Binance research programme — summary (2025-08 → 2026-10)

*Written 2026-10-01 as the programme's closing document; updated 2026-10-05 with Round 4 (§2.4: daily trend-following R6 and the
hedged low-volatility factor R4b, both FAIL) and 2026-10-06 with two post-close probes (§2.5: the DVOL variance-premium check V1 and the
borrow-rate check R9). `programme_report.html` is the 2026-10-01 version and includes none of these. Standalone: start here. Every number below
comes from a write-up in `runs/`, listed in §8, and every result there is reproducible from a harness
or an executed notebook in this repository.*

## The verdict in one paragraph

Over roughly 14 months the programme built a data collector, a year of unique order-book and
positioning data, and ~30 pre-registered experiments across four return sources: **BTC direction**
(5 s to 4 h, LSTM / GBM / MLP / CNN, engineered and raw inputs), **cross-sectional perp factors**
(860 contracts, survivorship-free) **funding carry** (delta-neutral) and, in a last round, **daily trend-following** and a **beta-hedged low-volatility factor**. The finding is consistent:
**the signals are real and the edges are not tradeable at a retail fee tier.** Every direction signal
was either smaller than the 9 bp round trip or gone within seconds. The cross-sectional rank effects
were strong out of sample but lost money to the right tail of small-coin pumps. The one source that
paid, funding carry, is a premium you collect rather than predict, and on the hedgeable majors it has
compressed to roughly the risk-free rate. The last round confirmed the pattern: a BTC trend rule cut drawdown by more than half but
earned less than holding BTC and missed its pre-registered margin, and a BTC-hedged low-volatility book lost to the alt-vs-BTC bleed.
Two post-close probes left small residuals rather than strategies: a volatility-premium rule that passed its gates narrowly on a 2021–22
sample (and is flat since), and a negative-funding carry that survives today's borrow rates but is capped by borrow limits at a few thousand
dollars per coin. **No tested strategy beat cash after costs at a scale and with evidence that held up outside the early sample.**

---

## 1. What was built

| asset | what | where |
|---|---|---|
| collectors v1–v5 | Binance spot + USDT-M futures order book (20-level ladders), trades, OI, long/short ratio, funding, liquidations, ETH mid; 15 s (v1) and 5 s (v5) bars, raw event stream (v5) | `binance_live_orderbook*.py` |
| v1 year | 331 days at 15 s, 784 columns; OI, L/S and the full ladder for the whole year, funding + liquidations for 216 days. **Not re-downloadable** (the API keeps ~30 days of OI / L/S) | `data/v1_year/` |
| v5 60-day set | 68.8 days at 5 s, 825 columns | `data/60days_data.tar`, `w5_60d.parquet`, `w5_wide.parquet` |
| BTC klines | 7 years of 5-min perp klines (740k bars) | `data/btcusdt_5m_klines.pkl` |
| perp archive | every USDT-M perp ever listed (860, incl. delisted), 1 h klines + funding, 2020-01 → 2026-08; spot 1 h klines for 282 of them | `data/xsec/` |
| harnesses | tested, reproducible test code for each later study (equivalence, leak, planted-signal and null-world tests) | `runs/harness_*` |

The collector data were verified against Binance's own candles to ~0.01 bp (`ALL_RUNS_ANALYSIS.md`).

## 2. The branches and what closed each

### 2.1 BTC direction (runs 001–014, probes 1a / 1d / R1 / MLP / W1 / continuation)

| horizon | best signal found | what it takes | result |
|---|---|---|---|
| **5–90 s** (runs 001–011, 13 LSTM runs) | IC +0.06 … +0.11, daily t up to +12.9; real and stable | break-even cost `c* ≥ 2 bp` (maker) | **c\* = 0.21 bp**. Even a *perfect* next-bar oracle makes 1.15 bp, below the maker fee (`horizon_economics…` §6) |
| **90 s, big-move bars** (1d, v1 year) | AUC 0.584 (null 0.512), 8/9 months; top 1 % confidence +11 bp at 0 s delay | ≥ 9 bp at a realistic entry delay | **+3.4 bp at 15 s, +0.1 bp at 30 s.** Replicated on new months (+13.8 → +4.8 bp); on 5 s bars +5.3 → **−0.4 bp at 5 s** (R1). The move is over within ~5 s |
| **5 s, every raw column** (W1) | 664 inputs; MLP finds a delayed signal the trees miss (AUC 0.527, 7/7 weeks) | ≥ 9 bp after a 5 s delay | **≤ +2 bp.** On these triggers the 90 s move is only ~10 bp, so even a perfect sign predictor nets ~1 bp |
| **after a 5–30 bp move** (continuation) | — | P(continue) well above the martingale baseline | **Martingale:** gross 0 ± 1 bp on 68 d (5 s) and 330 d (15 s); OI / funding / L-S conditioning adds nothing (3/90 cells, chance) |
| **15 min – 1 h** (GBM probe) | volatility detector transfers (2.3–3.6× \|move\| lift) | any directional signal | **IC ≈ 0**; heads stop at 0–14 rounds; apparent passes were drift (P(up) 0.873 on the trigger set) |
| **4 h** (v1 feasibility + 7-year validation) | `range_pos_24h` IC −0.34, stationary, daily t −18 | mean P&L > costs | **−1.39 bp gross over 1,625 trades / 7 years**, sign-perm percentile 33: many small wins, a few large losses |

Model class was tested directly and is not the limit: a 7.4 M-parameter attention LSTM did worse than
a dummy (run 002); on the same 60 features MLP = xgboost (AUC 0.5958 vs 0.5956, R1 C1).
Execution levers were exhausted early: TP/SL exits add nothing, maker entry is *worse* than taker
(adverse selection: a resting limit fills when the move fails), latency changes little at 15 s bars.

### 2.2 Cross-sectional perp factors (R4)

Point-in-time top-40 perps, six factors × three holding periods, signs fixed on two in-sample years,
**1,417 out-of-sample days**, Holm correction across 18 cells.

- **The rank effects are real:** low realised volatility beats high (IC −0.099, t −14.7 OOS); recent
  winners underperform at 1–28 days (t −3 to −4). Every in-sample sign held out of sample.
- **They lose money anyway:** equal-weight shorts of volatile alts win on the median day and lose on
  the mean to +150 % … +500 % pump days (RVOL30 book: median +22.6 bp/day, mean −7.8). Dollar-neutral
  books swing ~5 % a day.
- **The only positive cell was funding:** FUND7 (long negative-funding coins) +17.2 bp/day net, but
  t 1.32 and a −97 % drawdown; its gross is entirely funding collected.

### 2.3 Funding carry (R5)

- **Selective carry** (long spot / short perp when 7-day funding ≥ 0.03 % per 8 h): **+3.65 %/yr**,
  below the 4.5 % risk-free rate, negative in 2025 and 2026. It is capacity-bound: 1.05 positions on
  average, invested 17 % of days, because most funding-rich coins are perp-only.
- **Permanent BTC + ETH cash-and-carry:** +8.77 %/yr over 2020-10 → 2026-08, but **25.5 % in 2021,
  then 1.8, 6.0, 9.4, 3.8 %, and ~1.5 % annualised in 2026**. Competed away to about the risk-free rate.
- **Negative-funding mirror** (+37 %/yr) is **gross of spot borrow**. R9 (2026-10-06, §2.5) re-priced it at today's borrow rates: +18.9 %/yr on the coins borrowable today,
  capacity-bound, history of borrow rates still unknown (§5).

### 2.4 Round 4: daily trend-following (R6) and hedged low volatility (R4b)

Both target the failures above (left-skewed payoffs, the short-leg pump tail) with data already on disk; both pre-registered with
fixed parameters, tested on synthetic worlds first, and failed.

- **R6, trend-following + volatility targeting** (`r6_trend.analysis.md`). BTC, 20 / 60 / 120-day sign ensemble, 40 % vol target,
  2020-05 … 2026-08 window of 2,313 days. **Net Sharpe 0.92 vs buy-and-hold 0.69** (needs +0.30; got +0.23), max drawdown −33 % vs
  −79 %, beats cash in 4 of 7 years (needs 5), shift-null 97.9th percentile, +21 %/yr with a one-day lag, but **nets less than
  holding BTC** (+31.8 vs +39.6 %/yr). The Sharpe-difference CI is [−0.67, +1.22]; the short leg earned +0.4 %/yr, so all the gain is
  being flat through 2022. A per-coin long / flat version (top-40) fails four of five criteria. The long-only variant (Sharpe 1.21)
  was an information arm and is a forward-test hypothesis only.
- **R4b, low-volatility long, BTC-beta hedged** (`r4b_lowvol.analysis.md`). Volume ranks 41–100 (a universe R4 never used),
  1,864 days from 2021-07. Long the lowest-RVOL30 quintile, inverse-vol weights, short BTC sized to the trailing 60-day beta:
  **−6.0 bp/day (−22 %/yr), NW t −1.18**; 0–1 of 5 yearly blocks positive; negative with the 1 h lag. The hedged equal-weight control
  loses −9.4 bp/day (alts bleed against BTC), so the low-vol selection adds **+3.4 bp/day (t 1.1)**: right sign, not significant.
  The top-40 re-run (information) is +4.1 bp/day, t 0.8.
- **Reading.** Trend-following here is a risk-control result, in line with the volatility detector and vol targeting being the
  robust assets of the programme. The low-volatility effect is real as a rank signal and not harvestable as a hedged long-only tilt.

### 2.5 Post-close probes (2026-10-06): the DVOL variance premium (V1) and the borrow-rate check (R9)

Both were cheap tests of premises the closing write-up had left open; both pre-registered before the code.

- **V1, is 30-day realised BTC volatility predictably different from Deribit DVOL?** (`vrp_dvol.analysis.md`; spec `vol_monetisation_probe.plan.md`, which proposed
  monetising the volatility detector with options). 1,978 entry days 2021-03 → 2026-08, HAR-RV forecast refit monthly, ~66 independent months. **Variance premium +5.2 vol
  points (NW t 3.25).** P1: the HAR forecast carries information the market lacks (slope +0.41, t 3.18; the naive trailing-RV forecast −0.02). P2: short volatility only
  when the forecast is below implied earns +3.04 vol points per entry day at an assumed 2-point friction (CI +1.46 … +4.78), 5 of 6 calendar years positive (needs 5).
  **Both pre-registered gates pass, by minimum margins, and the evidence is old:** slope +0.77 (t 3.06) in 2021–22 vs +0.19 (t 1.43) in 2023–26; the rule earns +8.1 per
  entry day in 2021–22 and +0.6 since; it does not beat always-short in mean (−0.15, CI −2.1 … +2.0); the proxy omits delta-hedge cost and the convex tail (worst variance-payoff
  entry −128 vega points). Same decay shape as funding carry. A forward test of the frozen rule is running as a ledger (`harness_vrp/forward_score.py`, `forward_ledger.csv`;
  verdict only at ≥ 180 completed forward entry days, about early 2027). First run: 43 forward days, 13 complete, the rule flat on all (HAR forecast 44 > implied 36.5).
- **R9, does spot borrow absorb the +37 %/yr `CARRY−`?** (`r9_borrow.analysis.md`; no API key, so today's public VIP0 borrow rates, current only). **The prior was wrong:** on coins
  borrowable today, `CARRY−` nets +18.9 %/yr at today's rates (R5 gross 37.3, borrowable-only 26.7), Sharpe 3.8, max drawdown −1.5 %, break-even at 2.8× today's rates. By the registered
  bars it is still DEAD: every VIP0 borrow limit is $1.5k–5.6k per coin (needed ≥ 3 coins at ≥ $50k) and 2024 is −0.51 % (needed every year 2021–2025 positive). It is a small-account,
  capacity-bound carry resting on today's rates, which probably flatter the history (borrow demand spikes in squeezes; delisted coins excluded). A snapshot script
  (`harness_xsec/snapshot_borrow.py`) builds a rate history when run (manually; the cron schedule was removed because the machine is not always on).

## 3. Why: the arithmetic that kept recurring

1. **Fees vs move size.** The reachable tier at this volume is VIP0 + BNB: **9.0 bp taker round trip**,
   6.3 bp taker / maker, 3.6 bp maker / maker. Higher tiers need tens of millions of USD a month
   (`fee_reprice.analysis.md`). Required accuracy is `(1 + F / E|move|) / 2`. At 90 s on trigger bars
   (E|move| ≈ 10–13 bp) that is 0.85–0.95; achieved accuracy was 0.52–0.55.
2. **Signal half-life vs latency.** The strongest direction information decays in seconds (IC halves
   every ~30 s). By the time an order can be placed, most of the predicted move has happened.
3. **Horizon trade-off.** Longer horizons make the move large enough to pay fees, but the information
   is gone: IC ≈ 0 at 15 min – 1 h; the 4 h signal is real but its payoff is left-skewed.
4. **Payoff shape.** The same "win small, lose big" asymmetry appeared three times independently: the
   90 s TP/SL book, the 4 h mean-reversion rule, and the cross-sectional short legs.
5. **Premia vs predictions.** The only consistent positive (carry) is a premium for bearing risk or
   supplying a scarce trade, and it shrinks as capital arrives.

## 4. What survives — real, documented, not tradeable here

| finding | strength | doc |
|---|---|---|
| short-horizon microstructure ranking signal | IC +0.06–0.11, daily t up to +12.9, 4/4 folds | `run011.analysis.md` |
| volatility detector | eventful AUC 0.879; \|move\| lift 4.6× @90 s → 2.3× @1 h without retraining | `horizon_economics…` §7.2 |
| big-move direction tail | +11–14 bp at zero delay, 7/7 months, replicated | `v1_stage2_probe…`, `latency_decay_probe…` |
| wide-input delayed signal (MLP) | AUC 0.527, CI [0.516, 0.544], 7/7 weeks; trees cannot find it | `wide_probe.analysis.md` |
| 4 h range position | IC −0.34, stationary over 7 years | `v1_4h_feasibility…` |
| cross-sectional low-vol and reversal | IC −0.099 (t −14.7) and −0.02 (t −3 to −4) OOS | `r4_xsec.analysis.md` |
| funding carry | positive in every year on BTC/ETH; compressed to ~risk-free | `r5_carry.analysis.md` |
| BTC trend + vol targeting | drawdown −33 % vs −79 % at Sharpe 0.92 vs 0.69; a risk reducer, not a return source | `r6_trend.analysis.md` |
| low-vol selection among alts | +3.4 bp/day over a hedged alt basket, t 1.1 (insignificant) | `r4b_lowvol.analysis.md` |
| BTC variance risk premium | implied above realised by 5.2 vol points (t 3.25); HAR beats the market's price in 2021–22, not clearly since | `vrp_dvol.analysis.md` |
| negative-funding carry net of today's borrow | +18.9 %/yr on borrowable coins, break-even 2.8× rates; capped by $1.5k–5.6k borrow limits | `r9_borrow.analysis.md` |

These are the right inputs for **sizing, risk control or quote management** if a base strategy with
its own edge ever exists. The volatility detector is the most robust asset the programme produced.

## 5. What would change the verdict

Each of these changes the economics rather than the model:
- **A much cheaper fee tier or venue.** A maker rebate or near-zero maker fee reopens the 30–90 s
  ranking signal (oracle clears maker at a 30 s hold), but only with a solution to adverse selection.
  The untested `run.013` price-level panel (maker fill timing) is the one experiment aimed at that.
- **Co-located, sub-second execution.** The big-move tail is worth +11–14 bp at zero delay. A venue
  and infrastructure that act within ~100 ms could capture part of it. That is a different business.
- **Cheap spot borrow** on negative-funding coins. **R9 was run 2026-10-06** (`r9_borrow.analysis.md`, today's public VIP0 borrow rates; history needs a key): borrow does **not**
  absorb the gross. `CARRY−` on currently-borrowable coins at today's rates nets +18.9 %/yr (break-even at 2.8× today's rates), but it is capacity-bound (VIP0 borrow limits $1.5k–5.6k per coin)
  and failed the registered bars on capacity and one year (2024 −0.51 %).
- **Other instruments.** Options: V1 tested only the premise (DVOL vs realised, frictionless proxy) and found a premium that has faded. A real option book (spreads, delta-hedge cost, the
  convex tail) and the detector probe in `vol_monetisation_probe.plan.md` are not done; the suggested order is the forward ledger first, a real option-cost study second, the detector last.
- **Forward tests.** The historical samples are spent. The long-only trend variant (R6 `A_long`) and the low-vol tilt (R4b) are
  hypotheses to score on data after 2026-08, which needs the collector (or a paper-trading logger) running again. The V1 rule has a forward ledger already
  (`harness_vrp/`), and a borrow-rate history can accrue from `snapshot_borrow.py` runs.

## 6. Method lessons (what actually caught errors)

Ranked by how often they changed a conclusion (1–7 and 12; 8–11 were added after the later rounds):
1. **Sample length.** The 4 h strategy passed every structural control on one year and was an
   artifact over seven. Short samples produced several "significant" cells that did not replicate.
2. **One-position, non-overlapping P&L.** Caught a 9.4× inflation that no other control saw.
3. **Blind / drift benchmark.** Caught three false passes where P(up) on the trigger set was 0.87.
4. **Entry-delay rows.** The only control that separated the real +11 bp tail from a tradeable one.
5. **Sign-permutation / random-portfolio null, and Holm across cells.**
6. **Pre-registration and an economic pre-check** (`required accuracy = max((1 + F/E|move|)/2, P(up))`).
   Applied early, it would have saved most of runs 007–011.
7. **Tests that execute the artifact.** Three defect classes shipped past green suites that tested
   proxies; later harnesses use equivalence tests, leak tests (scramble the future), planted-signal
   worlds and null worlds. Two verdict-cell bugs were caught only by re-deriving results from saved data.
8. **A control that nets out the dominant term.** R4b's beta-hedged equal-weight control (−9.4 bp/day) showed the book's loss was the
   alt-vs-BTC bleed, and that the factor's real contribution was +3.4 bp/day. Without it a hedged long-only result is uninterpretable.
9. **Check the null before the real run.** A permutation null that redraws a random quintile daily pays far more turnover than a
   persistent rank, which biased a net-mean null; planted-signal and null worlds exposed it on synthetic data, and the fix was
   recorded before any real-data number existed. A plain-pandas re-derivation of R6 matched the harness exactly.
10. **Stratify a narrow pass by period.** V1 passed both gates, and only the split by period (slope t 3.06 in 2021–22, 1.43 afterwards; +8.1 → +0.6 per entry day) showed that the
    evidence was old. A pass at the minimum margin is a prompt for that check, not a result.
11. **Written priors can be wrong, and a bar set in advance can decide the verdict.** R9's stated expectation (borrow absorbs the gross) was falsified, while the registered capacity bar ($50k)
    and the every-year bar still returned DEAD. Both facts are reported; the bars were not changed after the numbers.
12. **Rank IC ≠ P&L.** Strong IC with negative P&L appeared at 90 s (volatility artifact) and in the
   cross-section (skew). Always report median vs mean and the worst 1 % of days.

## 7. Reproducing

- Local interpreter: `/home/nkout/projects/binance2/binance2/.venv/bin/python`; extra packages
  (pyarrow, xgboost, torch, nbformat, nbclient, ipykernel, psutil) go on `PYTHONPATH`.
- GPU studies are Colab notebooks built by `runs/harness_*/build_notebook.py` (builders refuse to
  overwrite executed notebooks); each has a `test_notebook.py` with a SMOKE execution.
- Cross-sectional, carry, trend and low-vol studies run locally in about a minute: `runs/harness_xsec/run_r4.py`,
  `run_r5.py`, `run_r6.py`, `run_r4b.py` (data: `fetch_archive.py`, ~45 min, resumable); each has a `test_*.py`
  (xsec 20, carry 15, trend 29, lowvol 24 checks). R9: `r9_borrow.py` (+ `test_r9.py`, 13) and `snapshot_borrow.py` (+ `test_snapshot.py`, 9).
- V1: `runs/harness_vrp/` (`fetch_dvol.py`, `run_vrp.py`, `diag_vrp.py`, `forward_score.py`, `test_vrp.py` 26 checks). Refresh the forward ledger with
  `runs/harness_v1_feas/fetch_klines.py`, `fetch_dvol.py`, then `forward_score.py`.
- Large data stays out of git (`.gitignore`: `data/`, `*.npz`, `*.pkl`, `*.tar`).

## 8. Document index

| area | documents |
|---|---|
| LSTM run lineage (001–011) | `ALL_RUNS_ANALYSIS.md`, `run007…run011.analysis.md`, `run009*.md`, `SESSION_NOTES_2026-09-21.md` |
| root cause and economics | `no_profit_root_cause.md`, `economics_and_metrics.md`, `horizon_economics_and_next_ideas.md` |
| longer horizons | `gbm_probe.analysis.md`, `v1_year_data_audit.md`, `v1_4h_feasibility.analysis.md` |
| big-move direction | `breakout_probe.analysis.md`, `v1_stage2_probe.analysis.md`, `continuation_conditioned.analysis.md`, `latency_decay_probe.analysis.md`, `wide_probe.analysis.md` |
| fees | `fee_reprice.analysis.md` |
| cross-section and carry | `r4_xsec.analysis.md`, `r5_carry.analysis.md`, `r9_borrow.analysis.md` |
| trend and hedged low volatility (Round 4) | `r6_trend.analysis.md`, `r4b_lowvol.analysis.md` |
| volatility premium and borrow (post-close) | `vrp_dvol.analysis.md`, `vol_monetisation_probe.plan.md` (spec, not implemented), `harness_vrp/forward_ledger.csv`, `r9_borrow.analysis.md` |
| ideas, pre-registrations, status | `next_signal_ideas.md` (Rounds 1–5, R1–R6, R4b, R9, V1, W1) |
| untested designs | `run014.plan.md` (marked superseded 2026-10-05), `features.explanation.run.013.md`, `btc_lstm.run.012.md` |
