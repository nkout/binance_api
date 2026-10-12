# N4 funding-settlement event study — POST passed on 1 h bars, then FAILED the 1-minute execution check (N4-X): the move is in the first minute. N4 is CLOSED

*2026-10-12. Harness `runs/harness_xsec/` (`settle.py`, `run_n4.py`, `test_settle.py` 20/20 PASS; xsec, lowvol, carry tests unchanged). Results
`harness_xsec/n4_results.json`, `n4_output.txt`; post-hoc diagnostics `n4_diagnostics.py` → `n4_diagnostics.txt`. Pre-registration: `next_signal_ideas.md`,
Round 6 / N4 (committed 304d068 before any code). Archive extended to 2026-09-30 first (`fetch_archive.py --extend 2026-09`). Local CPU, ~45 s per run.*

**Update 2026-10-12, N4-X (§5): FAIL → N4 closed.**
- Re-priced on 1-minute data for 2,000 sampled 2025–26 events, about 26 bp of the hour's 43 bp move happens **in the first minute after the settlement**.
- Entering at s + 2 min leaves +19.8 bp gross. Net of 19 bp all-in that is **+0.8 bp**, CI [−18.7, +20.7], median −8.1.
- The 1 h result was real price movement, but a retail taker cannot capture it. It is a latency race, the R1 territory.

**TLDR (1 h study, before N4-X).**
- **POST passes every registered criterion.** The trade: right after an extreme settlement (|previous rate| ≥ 0.10 %), take the paying side for one hour, i.e.
  short a coin whose funding is strongly negative and long one whose funding is strongly positive.
  - **+25.4 bp net per event**, 95 % CI [18.4, 32.4], clustered by settlement time. n = 20,847 across 9,694 settlement timestamps.
  - Event minus placebo: +33.4 bp [23.7, 43.6]. Holm p = 0.001.
  - Median +19.3 bp, trimmed mean +23.3 bp.
  - Both funding signs positive. Positive in 4 of 6 years.
- **PRE fails.** −7.9 bp net; the event is no better than the placebo.
- **Post-hoc checks support it being real price movement, not a measurement artifact.** None of them can change the verdict.
  - Entering at the first trade after the settlement gives the same result: the price jump across the settlement instant is 0.4 bp.
  - The effect decays over two hours.
  - It rises steadily with |rate|.
  - It is spread over 465 coins.
- **What would make it real money is still unknown.**
  - The cost model has the fee only, with no spread or slippage.
  - The tails are short squeezes: worst event −130 %, mean of the worst 1 % −19 %.
  - The effect only appears from 2023, and 2025–26 hold 73 % of the events.
  - The one forward month (September 2026) is uninformative: +6.8 bp, CI [−15.9, +30.5].

## 1. Registered result (settlements 2021-05-22 → 2026-08-30, top-100 universe, |r_prev| ≥ 0.10 %, 9 bp round trip)

| arm | net mean | 95 % CI | event − placebo | 95 % CI | median | trimmed | worst | worst 1 % mean | years + | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| PRE (q, s − 1h → s) | −7.9 | [−14.1, −1.4] | −7.4 | [−17.9, 2.7] | −9.0 | −7.8 | −8,138 | −2,056 | 4/6 | **FAIL** |
| POST (−q, s → s + 1h) | **+25.4** | **[18.4, 32.4]** | **+33.4** | **[23.7, 43.6]** | +19.3 | +23.3 | −12,955 | −1,931 | 4/6 | **PASS** |

All figures are bp per event on the traded notional. 20,848 events met the definition; 1 was dropped for a missing bar. Funding calc_time is never more than 1 s from the hour.

**POST by year (net):**
- 2021: −1.8 (n 1,108)
- 2022: −6.4 (n 1,641)
- 2023: +27.1 (n 1,519)
- 2024: +32.8 (n 1,323)
- 2025: +24.9 (n 5,855)
- 2026: +33.3 (n 9,401)

The event rate went from ~4–5 per day to 16 per day in 2025 and 39 per day in 2026. It rose together with the 4 h / 1 h funding intervals and with the
count of small listings.

**POST by sign:**
- Rate < 0 (91 % of events; trade short after the settlement): +23.1 net.
- Rate > 0 (trade long): +49.9 net.

**POST by interval:**

| interval | net | event − placebo |
|---|---|---|
| 8 h | +8.3 | +20.7 |
| 4 h | +41.5 | +62.4 |
| 1 h | +17.2 | +8.3 |

On 1 h coins the placebo bar is itself next to a settlement, so the placebo is not clean there.

**Blind (unsigned) bars:** PRE bar −1.4 bp, POST bar −24.1 bp. Because 91 % of events are negative-rate, the unsigned post-settlement hour falls.

**Information arm, signed by the settled rate r_s (not tradeable):** POST +25.3 net, the same as the registered arm. The signal is about the regime, not the last print.

## 2. Post-hoc diagnostics (not registered; descriptive)

| POST variant (gross bp; net = gross − 9) | mean | 95 % CI | median |
|---|---|---|---|
| registered: C(s) → C(s + 1h) | 34.4 | [27.4, 41.4] | 28.3 |
| entry at OPEN of the bar starting at s (first trade after the settlement) | 34.0 | [26.9, 41.0] | 27.5 |
| the jump C(s) → OPEN(s) | 0.4 | [0.3, 0.5] | 0.0 |
| delayed 1 h: s + 1h → s + 2h | 11.3 | [5.0, 17.2] | 9.3 |
| delayed 2 h: s + 2h → s + 3h | 1.0 | [−6.4, 8.0] | 5.0 |
| hold 2 h: s → s + 2h | 45.5 | [36.4, 54.0] | 36.2 |
| hold 4 h: s → s + 4h | 44.7 | [27.7, 59.6] | 45.0 |

- **Not bid-ask bounce or a last-trade artifact.** The effect starts after the settlement, not at the settlement print.
- **The pressure lasts about 2 hours, then it is gone.** One reading: funding collectors leave after the settlement; shorts who closed to avoid the payment come back.
  This is a story, not a test.
- **Dose-response.** Gross POST by |r_prev|:
  - 0.1–0.2 %: 21.7 bp (n 11,399)
  - 0.2–0.5 %: 21.6 bp
  - 0.5–1 %: 81.8 bp
  - ≥ 1 %: 156.2 bp (n 1,061)

  Placebo rises too at the top end (19, 63), so part of the extreme-rate gain is a general drift of those coins.
- **Breadth.** 465 coins; 62 % have positive total net. The top 5 coins hold 21 % of the total and the top 20 hold 52 %. Excluding the top 10 coins: +19.2 bp net.
  Per-coin mean net: average 24.3, median 14.3.
- **Liquidity in the trade hour.** Quote volume in the POST hour: 10 % / 50 % / 90 % quantiles $0.64 M / $5.5 M / $48 M. Events per settlement timestamp: mean 2.2,
  max 80.

## 3. What is still open (why this is not yet a strategy)

1. **Spread and slippage are not in the 9 bp.** These are often small, squeezed contracts at a busy moment. The 1 h data cannot measure the spread. Needed: entry at
   s + 1 min … s + 5 min from 1-minute klines, and spread from the bookTicker / aggTrades archive for a sample of events.
2. **Tail risk drives sizing.** POST is mostly short into strongly negative funding, i.e. into crowded shorts. One event lost 130 % of notional in an hour, and
   1 % of events average −19 %.
   - Per-event notional has to be small relative to capital.
   - Since about 39 events per day can open, the book needs a concurrent-exposure cap.
   - A daily-P&L view with a sizing rule has not been computed yet.
3. **1 h-interval coins: the exit falls on the next settlement.** On these coins the exit instant is itself a settlement. A short still open at that instant pays
   funding that can reach the rate cap. Execution must exit before it. The registered accounting assumes it does.
4. **The effect is recent.** 2021–22 were slightly negative, and every positive year is 2023+, when the event count and the 4 h regime grew. A regime change
   (exchange rules, funding caps, more arbitrageurs) could close it.
5. **Forward month (2026-08-31 → 2026-09-30, unseen by any study):**
   - POST: +6.8 bp net, CI [−15.9, +30.5], 917 events / 511 clusters; median +19.4.
   - PRE: −16.3.

   One month is too little to confirm or refute.

## 4. Verdict and next step

**N4: PASS (POST arm) by the registered letter. N4 PRE: FAIL.**

The pre-registration says nothing about promotion after a PASS. Following the programme rules (V1 precedent), the frozen POST rule now goes to:
- **(a) a forward ledger**, scored on monthly archive extensions;
- **(b) a minute-scale execution check** that adds spread and slippage.

Both get pre-registered in `next_signal_ideas.md` before any data after 2026-09-30 is fetched. No parameter (threshold, holding time, universe) changes.
The 2-hour hold and the |r| ≥ 0.5 % bucket look better, but they were found after the fact, so they are information only.

## 5. N4-X — minute-scale execution check: FAIL (2026-10-12)

*`n4x.py`, `run_n4x.py`, `test_n4x.py` 9/9 PASS. Results `n4x_results.json`, `n4x_output.txt`, `n4x_sample.csv`. 1-minute data `data/xsec/min1/` (1,482 coin-days,
none missing). The minute prices match the 1 h archive exactly at s and s + 1h for all 2,000 events.*

**Sample.**
- 2,000 events drawn at random (seed 0) from the 15,256 registered events 2025-01-01 → 2026-08-30.
- 241 coins, 1,717 settlement times.
- The sample's 1 h POST gross is +42.8 bp, so the sample is representative or slightly favourable.

| registered: entry O(s + 2 min), exit P(s + 1h), 19 bp all-in | value |
|---|---|
| net mean | **+0.78 bp**, 95 % CI [−18.7, +20.7] |
| median / trimmed | −8.1 / −5.0 |
| worst / worst-1 % mean | −4,557 / −2,028 |
| criteria | mean > 0 ✓ · CI lower bound > 0 ✗ · median > 0 ✗ → **FAIL** |

**Signed path from P(s), paying direction (mean, bp; information):**

| minute | 1 | 2 | 5 | 15 | 30 | 60 | 90 | 120 |
|---|---|---|---|---|---|---|---|---|
| mean | 25.8 | 23.5 | 27.4 | 33.1 | 39.6 | 42.8 | 53.0 | 51.9 |
| median | 16.1 | 16.9 | 21.4 | 23.7 | 24.6 | 29.2 | 35.8 | 43.2 |

- **About 60 % of the hour's mean move, and more than half of the median move, happens within one minute of the settlement.** The 1 h "entry at the first trade after s"
  diagnostic (§2) could not see this: the bar's open is the first trade after s, and it sits before the jump. The jump happens inside the first minute.
- **Gross by entry minute:**

  | entry | gross mean | CI |
  |---|---|---|
  | s + 1 min | 17.4 | — |
  | s + 2 min | 19.8 | [0.3, 39.7] |
  | s + 5 min | 15.3 | — |
  | s + 15 min | 10.4 | — |

  Fee-only (9 bp) net at s + 2 min is +10.8, CI [−8.7, +30.7], median +1.9. Even with zero spread it is not significant.
- **The slow part afterwards is real but small.** It adds about 20 bp gross over the hour from minute 2, with a wide CI. It continues to about 90 minutes, but at 19 bp
  all-in it is about zero.
- **By sign:**
  - Rate < 0 shorts (94 %): −5.7 net.
  - Rate > 0 longs: +97.6, but n = 125 and the CI includes 0.
- **By year:** 2025 −11.9, 2026 +8.2. Neither is significant.

**Verdict.** **N4-X FAIL.** By the registered rule (FAIL → N4 closed regardless of N4-F), **N4 is closed.** The forward ledger N4-F is not started.

The settlement effect is real, but it is priced within about a minute, by participants faster than a retail taker. A sub-minute version would be a latency strategy.
The programme already showed that is out of reach (R1). It is not re-registered.

**Not promoted (seen after the fact):**
- the rate > 0 long leg;
- a longer hold from s + 2 min;
- the |r| ≥ 0.5 % bucket.
