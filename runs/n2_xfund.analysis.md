# N2 XFUND cross-venue funding spread (Binance vs Bybit) — FAIL: +10.4 %/yr, NW t 2.02 and capacity passes, but 2023 and 2026 are negative and the worst month is −11.5 %

*2026-10-12. Harness `runs/harness_xsec/`:*
- *code: `fetch_bybit.py`, `xfund.py`, `run_n2.py`;*
- *tests: `test_xfund.py`, 24/24 PASS (fetcher with a fake API, simulation rules, P&L identity, look-ahead, planted / null worlds, delisting rule);*
- *results: `n2_results.json`, `n2_output.txt`; first run before the delisting fix: `n2_output_first_run.txt`, `n2_results_first_run.json`;*
- *pre-registration: `next_signal_ideas.md`, Round 6 / N2, plus "N2 implementation details", fixed before the run;*
- *data: `data/bybit/`, public v5 API, 574 perps including delisted ones, about 20 min to download. The run takes about 70 s on CPU.*

**TLDR.**
- **FAIL by the registered letter.**
  - Annualised net **+10.35 %** on capital (needs ≥ 4.5 %) ✓.
  - NW t **2.02** ✓.
  - 1 h lag +9.8 %/yr ✓.
  - Mean open positions **4.3** ✓. That is the capacity bar `CARRY+` failed, and it now passes.
  - Calendar years: 2023 **−0.98 %** and 2026 **−1.11 %** ✗.
  - Worst month: **−11.5 %** (July 2026) ✗.
- **The funding spread is real and large:** +20.4 %/yr gross from funding alone.
- **What takes most of it:**
  - Cross-venue price basis: −5.1 %/yr.
  - Taker costs: −4.9 %/yr.
- **Wild days.** The two venues' prices can come apart during squeezes and delistings. One HUSDT day was −16 %, another +24 %. In June 2026 HUSDT traded at 2× on Bybit vs Binance.
- **The result is concentrated in time and in a few coins.**
  - 2025 alone is +37.8 %. The other full years are +0.4, −1.0, +8.5 and −1.1.
  - Two coins (AIA +13.9 %, H +10.4 %) make about half of the total P&L.
- **Forward month (September 2026, unseen):** −2.4 %.

## 1. Data and coverage

- **Bybit.**
  - 1,106 USDT perps, 308 of them closed.
  - Delisted perps keep their funding and 1 h klines when the query window ends before the delisting, so the survivorship caveat in the pre-registration largely does not apply.
  - 600 Binance perps were ever in the top 100. 574 map to Bybit (5 only after stripping a multiplier, such as 1000SHIB ↔ SHIB1000). The 26 unmapped are in the output.
- **Window:** 2021-10-01 (first day with ≥ 20 coins in the set) → 2026-08-30, 1,795 days.
- **Coverage of Binance top-100 coin-days:** a Bybit perp exists on 98.8 %, and 88.7 % pass every set filter. Mean set size 85.6.
- **Delisting finding.** The Binance archive keeps printing **flat, zero-volume 1 h bars for months after a contract is delisted**. For example, HIFI last traded on
  2025-10-03 but has bars to 2026-09.
  - The first run held the frozen Binance leg while the Bybit leg kept moving, which is a naked position.
  - The registered rule is "delisting is closed at the last common price". It is now implemented as follows: from the first hour either venue is dead (file ended,
    or ≥ 24 h of only zero-volume bars to the end), both legs are closed. 133 coins are affected.
  - **I found this by inspecting the worst and best days after the first run.** Both runs are reported:

    | run | annualised net | NW t | 2023 | 2026 | worst month | verdict |
    |---|---|---|---|---|---|---|
    | first run | +13.0 % | 2.16 | −0.98 % | −10.3 % | −10.2 % | **FAIL** |
    | delisting rule | +10.35 % | 2.02 | −0.98 % | −1.11 % | −11.5 % | **FAIL** |

- **Effect on the earlier single-venue studies (R4, R4b, R5, N1, N4).**
  - A frozen price earns 0, the same as a position closed at the last price, so these flat bars create no false P&L there.
  - They can, however, keep a dead coin in a universe for up to about 30 days, until its 30-day volume and volatility decay.
  - That dilutes a book with something like cash. It does not bias the sign of a result, but it was not checked number by number.

## 2. Registered result (with the delisting rule)

| metric | value |
|---|---|
| annualised net on capital | **+10.35 %** (gross +15.3; funding +20.4, basis −5.1, cost −4.9) |
| NW t / Sharpe / max drawdown | 2.02 / 0.68 / −27.7 pp |
| 1 h lag net | +9.83 %/yr |
| daily net mean / median / worst / worst 1 % mean / best | +2.8 bp / 0.0 / −16.2 % / −3.0 % / +23.6 % |
| mean open positions / days with ≥ 1 / days at the cap of 10 | 4.26 / 87 % / 19 % |
| episodes / median hold / trades per year | 811 / 7 days / 328 |
| capacity: thinner venue's 30-day mean daily volume per held coin-day | median $23 M (10 % quantile $5.8 M) |
| margin stress: episodes with a leg > 25 % against entry | 371 of 811 |

The margin-stress count is high because each leg alone moves more than 25 % on these coins, even while the pair stays hedged. A real account needs margin on both venues to
survive that without a forced close, which this backtest does not model.

**By year (% of capital):**

| year | funding | basis | cost | net |
|---|---|---|---|---|
| 2021 (Oct–Dec, partial) | +7.3 | −0.1 | −0.8 | +6.4 |
| 2022 | +11.6 | −8.2 | −3.0 | **+0.4** |
| 2023 | +5.8 | −4.3 | −2.6 | **−1.0** |
| 2024 | +11.1 | +0.4 | −3.1 | +8.5 |
| 2025 | +42.3 | +2.4 | −6.9 | **+37.8** |
| 2026 (to Aug) | +22.1 | −15.3 | −7.8 | **−1.1** |

**Worst months:**

| month | net | funding | basis |
|---|---|---|---|
| 2026-07 | −11.5 % | −6.1 | −4.1 |
| 2022-11 (FTX) | −3.8 % | | |
| 2026-02 | −3.4 % | | |
| 2026-03 | −3.1 % | | |
| 2023-09 | −2.3 % | | |

**Criteria:**
- [x] ≥ 4.5 %/yr
- [ ] every year with ≥ 6 months positive (2023, 2026)
- [ ] worst month > −5 %
- [x] NW t ≥ 2
- [x] lag > 0
- [x] mean positions ≥ 2

**→ FAIL.**

## 3. Reading

- **Funding dispersion between the venues is real and wide enough to trade.**
  - |Δ7| is ≥ 0.03 % per 8 h on 4.2 % of set coin-days.
  - Mean positions are 2.0–9.0 per year.
  - The thinner venue still trades a median $23 M a day per held coin.
  - Capacity, the constraint that killed `CARRY+` and R9, is not the problem here.
- **The problem is what comes with a wide spread.** It appears on squeezed, distressed or delisting coins. There the venues' prices also come apart (HUSDT 2×), funding
  can swing within a week (July 2026: −6 % from funding, as the 7-day signal lagged a reversal), and one leg can stop trading.
  - Basis cost −5 %/yr overall, −15 % in 2026.
  - Two coins make about half the P&L.
  - 2025 makes most of the total.
- **Same shape as the earlier carry results.** It is a premium for holding the tail of exchange-specific dislocations. It looks good on average and is unstable by year.

## 4. Verdict

**N2: FAIL** under the registered rules, in both the first run and the corrected one. No forward test.

Not promoted (seen after the fact):
- excluding coins with extreme funding, or the H / AIA-type distressed names;
- a faster exit on a funding reversal;
- adding the 2021 partial year.

Any of these would be a new pre-registration on data after 2026-09-30.
