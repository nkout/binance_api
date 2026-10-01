# Binance research programme — summary (2025-08 → 2026-10)

*Written 2026-10-01 as the programme's closing document. Standalone: start here. Every number below
comes from a write-up in `runs/`, listed in §8, and every result there is reproducible from a harness
or an executed notebook in this repository.*

## The verdict in one paragraph

Over roughly 14 months the programme built a data collector, a year of unique order-book and
positioning data, and ~30 pre-registered experiments across three return sources: **BTC direction**
(5 s to 4 h, LSTM / GBM / MLP / CNN, engineered and raw inputs), **cross-sectional perp factors**
(860 contracts, survivorship-free) and **funding carry** (delta-neutral). The finding is consistent:
**the signals are real and the edges are not tradeable at a retail fee tier.** Every direction signal
was either smaller than the 9 bp round trip or gone within seconds. The cross-sectional rank effects
were strong out of sample but lost money to the right tail of small-coin pumps. The one source that
paid, funding carry, is a premium you collect rather than predict, and on the hedgeable majors it has
compressed to roughly the risk-free rate. **No tested strategy beat cash after costs.**

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

## 2. The three branches and what closed each

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
- **Negative-funding mirror** (+37 %/yr) is **gross of spot borrow**. Negative funding is largely the
  price of a scarce short, so the borrow cost is expected to absorb it; it could not be tested.

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

These are the right inputs for **sizing, risk control or quote management** if a base strategy with
its own edge ever exists. The volatility detector is the most robust asset the programme produced.

## 5. What would change the verdict

Each of these changes the economics rather than the model:
- **A much cheaper fee tier or venue.** A maker rebate or near-zero maker fee reopens the 30–90 s
  ranking signal (oracle clears maker at a 30 s hold), but only with a solution to adverse selection.
  The untested `run.013` price-level panel (maker fill timing) is the one experiment aimed at that.
- **Co-located, sub-second execution.** The big-move tail is worth +11–14 bp at zero delay. A venue
  and infrastructure that act within ~100 ms could capture part of it. That is a different business.
- **Cheap spot borrow** on negative-funding coins (a live borrow-rate check is the cheap first step).
- **Other instruments.** Options (selling volatility with the detector as a veto, idea 1b) were never
  tested: no implied-volatility history was collected.

## 6. Method lessons (what actually caught errors)

Ranked by how often they changed a conclusion:
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
8. **Rank IC ≠ P&L.** Strong IC with negative P&L appeared at 90 s (volatility artifact) and in the
   cross-section (skew). Always report median vs mean and the worst 1 % of days.

## 7. Reproducing

- Local interpreter: `/home/nkout/projects/binance2/binance2/.venv/bin/python`; extra packages
  (pyarrow, xgboost, torch, nbformat, nbclient, ipykernel, psutil) go on `PYTHONPATH`.
- GPU studies are Colab notebooks built by `runs/harness_*/build_notebook.py` (builders refuse to
  overwrite executed notebooks); each has a `test_notebook.py` with a SMOKE execution.
- Cross-sectional and carry studies run locally in about a minute: `runs/harness_xsec/run_r4.py`,
  `run_r5.py` (data: `fetch_archive.py`, ~45 min, resumable).
- Large data stays out of git (`.gitignore`: `data/`, `*.npz`, `*.pkl`, `*.tar`).

## 8. Document index

| area | documents |
|---|---|
| LSTM run lineage (001–011) | `ALL_RUNS_ANALYSIS.md`, `run007…run011.analysis.md`, `run009*.md`, `SESSION_NOTES_2026-09-21.md` |
| root cause and economics | `no_profit_root_cause.md`, `economics_and_metrics.md`, `horizon_economics_and_next_ideas.md` |
| longer horizons | `gbm_probe.analysis.md`, `v1_year_data_audit.md`, `v1_4h_feasibility.analysis.md` |
| big-move direction | `breakout_probe.analysis.md`, `v1_stage2_probe.analysis.md`, `continuation_conditioned.analysis.md`, `latency_decay_probe.analysis.md`, `wide_probe.analysis.md` |
| fees | `fee_reprice.analysis.md` |
| cross-section and carry | `r4_xsec.analysis.md`, `r5_carry.analysis.md` |
| ideas, pre-registrations, status | `next_signal_ideas.md` (Rounds 1–3, R1–R5, W1) |
| untested designs | `run014.plan.md`, `features.explanation.run.013.md`, `btc_lstm.run.012.md` |
