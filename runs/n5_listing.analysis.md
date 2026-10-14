# N5 drift after a new token's Binance perp listing — DEAD: the typical event wins (median +14.6 % in 14 days, 71 % of events) and the mean loses to a handful of pumps

*2026-10-14. Harness `runs/harness_xsec/` (`listing.py`, `run_n5.py`, `test_listing.py` 27/27 PASS; post-hoc `n5_diagnostics.py`). Results
`harness_xsec/n5_results.json`, `n5_output.txt`, `n5_diagnostics.txt`. Pre-registration: `next_signal_ideas.md`, Round 7 / N5 (committed 5533770 before any code) and
"N5 implementation details" (written 2026-10-14 before the run). Metadata: `harness_xsec/n5_meta/` (829f8ab). Local CPU, ~50 s.*

**TLDR.**
- **DEAD by the registered rule: 2 of 10 criteria pass** (median and trimmed mean > 0; n ≥ 300).
  - 473 events, 2020-07 → 2026-08.
  - **Event net mean −3.77 %** per 14-day event, clustered CI [−21.9, +7.7]. Event − seasoned control −7.0 %. 1 h lag −4.4 %.
  - Portfolio: +2.0 %/yr, NW t 0.38, worst month −18.9 %, worst day −12.7 %.
- **The drift exists in the typical event.**
  - **Median +14.6 %**, 71 % of events win, 1 % trimmed mean +5.9 %. The median is positive in every entry year (+8.2 … +17.9 %). The median of event − control is +12.7 %.
  - Without the 5 worst events (COAI, H, AIA, AVNT, IP) the mean is **+7.1 %**.
- **The right tail kills it — the same failure as R4's short legs.** 15 of 473 coins more than doubled within 14 days, and 3 rose more than 300 %.
  - COAIUSDT (2025-09-28) rose **+3,653 %** on about $1.1 B a day of volume: one event at −3,499 %.
  - 2025 holds 221 events and the September 2025 pump cluster: event mean −13.3 % that year.
- **Post-hoc (information only, cannot promote): a stop-loss keeps a positive mean, but not a listing-specific one.**
  - Rule: buy-stop at +50 % on the coin, triggered on the 1 h high, filled at the worse of the stop and the bar open, plus 0.5 % slippage.
  - Result: **+4.3 % per event, CI [+0.5, +8.0]**, 6 of 7 years positive.
  - Net of the registered seasoned control it is **+1.9 %, CI [−2.0, +5.8]**. About half the stopped return is the 2024–25 alts-vs-BTC bleed that N1 showed is a regime.
  - A stopped version is a forward-test hypothesis, nothing more.

## 1. Run validity

| check | result |
|---|---|
| tests | 27/27, listed below |
| event set | reproduces the registered table in every year; the one difference (BOBUSDT, dropped by the registration as a "re-listed base") is kept out as registered. It is in fact a different token from `1000000BOBUSDT` (prices 0.0215 vs 0.0266 × 10⁻⁶). Putting it back: mean −3.75 % |
| control coverage | 443 of 473 events; 30 early-2020 events have no coin aged ≥ 365 days in the top 100 |
| data sanity | the worst events are genuine, liquid pumps: COAI 0.18 → 7.9 USDT over 14 days on ~$1.1 B/day, AIA 0.42 → 2.57 on ~$450 M/day, H on ~$710 M/day; not bad prints |
| delistings | 1 event closed at a delisting (last traded price, BTC leg closed at the same instant) |

The 27 tests:
- classification (pre-2020, TradFi, index, new, seasoned, re-listed);
- entry instant and ISO-week clustering;
- a hand-computed event (price step, value-weighted funding with the entry settlement excluded and the exit settlement included, BTC leg, 18 bp);
- delisting closure, both via zero-volume flat bars printed at a wrong price and via a file that stops;
- look-ahead: prices and volume after entry leave selection unchanged; prices after exit leave P&L unchanged;
- portfolio constant-notional marks match a hand sum, and the open cap skips a later event;
- the bootstrap resamples weeks;
- verdict logic;
- synthetic worlds on the on-disk format:
  - a planted new-token drift passes all 10 criteria (event +9.8 %, CI [7.3, 12.3]);
  - 10 null worlds pass 0 times;
  - an alts-vs-BTC drift with no listing effect gives event +7.8 % but **fails the control criterion** (event − control −0.6 %, CI [−2.6, +1.3]);
  - end-to-end report.

## 2. Registered cell (new tokens, entry ≥ L + 48 h, ≥ $5 M, short coin / long BTC, H = 14 days, 18 bp)

| | n | mean | 95 % CI (week-clustered) | median | trimmed 1 % | win | worst | worst 1 % |
|---|---|---|---|---|---|---|---|---|
| **event net** | 473 | **−3.77 %** | [−21.86, +7.70] | **+14.64** | +5.92 | 71 % | −3,499 % | −1,205 % |
| event − seasoned control | 443 | −7.02 % | [−26.42, +5.12] | +12.72 | +3.32 | 68 % | −3,522 % | −1,214 % |
| control alone (short seasoned top-100 / long BTC) | 443 | +1.73 % | | +1.88 | +1.93 | 59 % | −54.7 % | −43.9 % |
| blind short (unhedged) | 473 | −4.47 % | | +14.55 | +5.21 | 67 % | | |
| 1 h lag row | 473 | −4.42 % | | | | | | |

Decomposition of the mean (%):
- coin price: short leg loses 3.02 (the pumps);
- coin funding: shorts *paid* 1.35 (crowded shorts on new listings pay longs);
- BTC price: +1.13;
- BTC funding: +0.34;
- cost: −0.18.

**Per entry year** (net mean / median / event − control):

| year | n | mean | median | − control |
|---|---|---|---|---|
| 2020 | 29 | +20.2 | +10.1 | n/a |
| 2021 | 23 | −0.9 | +8.6 | +0.3 |
| 2022 | 15 | +4.4 | +14.2 | +7.7 |
| 2023 | 42 | −0.9 | +8.2 | −0.4 |
| 2024 | 96 | +4.4 | +15.7 | +5.5 |
| 2025 | 221 | **−13.3** | +17.9 | −17.5 |
| 2026 | 47 | +3.1 | +11.4 | +2.9 |

Criterion 4 fails: 2021, 2023 and 2025 are negative, and at most one is allowed.

**Portfolio** (2 % of capital per leg per event, ≤ 25 open, marked daily on constant notional, 2020-07-03 → 2026-08-30):
- **+2.02 %/yr, NW t 0.38**, Sharpe 0.16;
- worst month −18.9 %, worst day −12.7 %, max drawdown −43.9 %;
- mean 2.9 events open, at most 19, none skipped.

Daily constant-notional marking buys back the short as a coin rises, which is why the portfolio loses far less than the buy-and-hold event mean. It is still not
significant.

**Criteria:**

| # | criterion | result |
|---|---|---|
| 1 | event mean > 0, CI lower > 0 | FAIL (−3.77 %) |
| 2 | median and trimmed mean > 0 | **PASS** |
| 3 | event − control > 0, CI lower > 0 | FAIL (−7.02 %) |
| 4 | positive in every qualifying year except at most one | FAIL (3 negative) |
| 5 | 1 h lag > 0 | FAIL |
| 6a–d | portfolio ≥ 4.5 %/yr, NW t ≥ 2, worst month and worst day > −5 % | FAIL on all four |
| 7 | n ≥ 300 | **PASS** |

**Forward block** (exits after 2026-08-31, unseen): 2 events, +3.8 % and +20.4 %. Descriptive only.

## 3. Information arms (not part of the verdict)

| arm | n | mean | median | trimmed | − control |
|---|---|---|---|---|---|
| H = 7 | 473 | +1.0 % [−3.7, +5.2] | +9.0 | +3.0 | −0.5 % |
| H = 28 | 472 | −24.5 % [−93.8, +11.1] | +20.3 | +8.1 | −30.9 % |
| seasoned listings | 165 | −2.0 % | +9.9 | +1.4 | +1.4 % |
| liquidity ≥ $2 M | 483 | −4.0 % | +14.3 | +5.5 | −7.0 % |
| liquidity ≥ $10 M | 459 | −4.0 % | +14.9 | +6.0 | −6.9 % |
| entry ≥ L + 24 h | 479 | −2.5 % | +13.9 | +6.8 | −6.0 % |

The median grows with the hold (+9 → +15 → +20 %) and so does the tail (worst event −508 → −3,499 → −14,646 %).

Every liquidity floor and entry delay gives the same picture. The *most* liquid tercile is the worst in the mean (−14.8 %) and the best in the median (+19.8 %): the
big pumps are high-volume coins.

By token type, listings tagged "Alpha" in `exchangeInfo` hold the worst tails (mean −49.6 %, n 77, including COAI). DeFi (n 104) and AI (n 60) are positive in
the mean. This is not significant, and it is post-hoc.

## 4. Post-hoc: does bounding the loss keep the drift? (`n5_diagnostics.py`; information only)

| stop on the coin | fill model | mean | 95 % CI | median | − control | stops fired |
|---|---|---|---|---|---|---|
| none (registered) | — | −3.77 % | [−21.9, +7.7] | +14.6 | −7.0 % | — |
| +25 % | 1 h close | +2.95 % | [+0.0, +5.8] | +7.8 | | 185 |
| +50 % | 1 h close | +4.09 % | [+0.2, +8.0] | +13.6 | | 94 |
| +100 % | 1 h close | +3.36 % | [−1.6, +8.1] | +14.3 | | 43 |
| +25 % | **1 h high, max(stop, open) + 0.5 %** | +2.33 % | [−0.4, +5.1] | −0.4 | +0.5 % [−2.5, +3.5] | 219 |
| **+50 %** | **1 h high, max(stop, open) + 0.5 %** | **+4.29 %** | **[+0.5, +8.0]** | +13.1 | **+1.9 % [−2.0, +5.8]** | 111 |
| +100 % | 1 h high, max(stop, open) + 0.5 % | +4.14 % | [−0.5, +8.7] | +14.3 | +1.4 % [−3.2, +6.1] | 47 |

The control column subtracts the registered (unstopped) seasoned control. Seasoned alts rarely move 50 % in 14 days, so a stop would change it little.

Reading:
- A stop turns the mean positive at every level, and the +50 % stop's CI clears zero even with intrahour triggering and gap fills.
- The listing-specific part (− control) is +0.5 … +1.9 % and never significant. The stopped book is mostly short-alts-vs-BTC exposure, which paid in
  2024–25 and reversed after (N1).
- Slippage on a buy-stop during a squeeze can exceed 0.5 %. The 1 h high also misses sub-hour spikes that reverse.
- The stop level was chosen after seeing the data. Three levels are shown so the conclusion does not rest on one.

## 5. What this adds to the programme

1. **Third independent appearance of the short-tail failure** (R4 short legs, N1's worst days, N5): in crypto, a short of a small, new or volatile coin wins in
   the median and loses in the mean. A median or IC result does not show whether a short is harvestable. Check the mean, the worst 1 % and a bounded-loss
   version.
2. **New listings do drift down against BTC in the typical case** (median +14.6 %, all years, all liquidity floors). That supports the structural story: small
   float, unlocks, airdrop selling. But it is not separable from the alts-vs-BTC regime once the tail is cut.
3. **Funding works against the trade** (−1.35 % per event): shorts on new listings are crowded and pay.
4. **If revisited:** a new pre-registration scored only on listings after 2026-09-30, with these elements fixed in advance:
   - the bounded-loss construction (+50 % stop, intrahour trigger, gap fill);
   - the seasoned control, as the criterion that matters;
   - about 100 events a year at the 2025 pace, so a verdict needs about 12–18 months of forward data.
