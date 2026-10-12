# N1 long BTC / short a mid-cap alt basket, beta-neutral — DEAD: NW t 0.91, the gain sits in 2024 → mid-2025 and reverses after, in every volume band

*2026-10-12. Harness `runs/harness_xsec/` (`altshort.py`, `run_n1.py`, `test_n1.py` 25/25 PASS; reuses `lowvol.universe_band / inv_vol_weights / hedged_book`).
Results `harness_xsec/n1_results.json`, `n1_output.txt`. Pre-registration: `next_signal_ideas.md`, Round 6 / N1 (committed 304d068 before any code). Archive extended
to 2026-09-30. Local CPU, ~35 s.*

**TLDR.**
- **DEAD by the registered rule.**
  - +8.1 bp/day net (+29 %/yr on the short notional), but **NW t 0.91**. ALIVE needed ≥ 1.0 and PASS ≥ 2.
  - Only **3 of 5** 182-day blocks positive.
  - Median, trimmed mean, lag and worst-day criteria all pass.
- **The gain is a regime, not a premium.** Blocks run +17.9, +26.5, +18.1, then −5.3, −17.5 bp/day: shorting alts against BTC paid from 2024-01 to 2025-07, then lost.
  Every band shows the same shape: ranks 41–100, ranks 1–40 and an equal-weight book. So it is not something specific to mid-caps.
- **Risk is far larger than the mean.**
  - Daily sd 2.66 % (≈ 51 %/yr), Sharpe 0.58.
  - Max drawdown −69 pp (sum of daily returns).
  - The 2 % cap works: the worst single-coin loss is −2.5 % of notional (HIFI +440 % in a day). The worst *book* days (−8 % to −10 %) come from broad alt
    rallies, not from one coin.
- **Forward month (2026-08-31 → 09-29, unseen):** −52 bp/day, **−15.6 %** in 30 days, consistent with the last two blocks.

## 1. Registered cell (ranks 101–200 ex-BTC, inverse-RVOL30 capped 2 %, H = 7, 2024-01-27 → 2026-08-30, 947 days)

The band first exists on 2023-11-28, and the 60-day beta warm-up follows. Every day in the window is live, and the book is formed on all of them.

| metric | value |
|---|---|
| net / gross / cost | **+8.07** / +8.41 / 0.34 bp/day |
| NW t | **0.91** |
| median / trimmed (1 %) | +8.40 / +6.46 |
| annualised / Sharpe | +29.4 % / 0.58 |
| daily sd / worst day / worst 1 % mean | 2.66 % / −9.62 % / −8.79 % |
| max drawdown | −68.6 pp |
| beta of net to BTC | +0.02 (the hedge works; mean BTC long 1.31) |
| 1 h lag net | +7.73 |
| 182-day blocks (bp/day) | +17.9, +26.5, +18.1, **−5.3, −17.5** → 3 / 5 |

**Criteria:**
- [ ] NW t ≥ 2 (0.91)
- [x] median and mean > 0
- [x] trimmed > 0
- [ ] ≥ 4 / 5 blocks
- [x] lag > 0
- [x] worst day > −20 %

PASS fails on two criteria, so ALIVE (t-stat only, with 1 ≤ t < 2) cannot apply. **→ DEAD.**

**P&L decomposition (bp/day):**

| alt price | alt funding | BTC price | BTC funding | cost |
|---|---|---|---|---|
| +3.78 | −0.44 | +7.34 | −2.27 | −0.34 |

- The alt-short leg's price gain is under half the total. Most of it is the 1.3× BTC hedge leg, through a beta that is unstable from day to day.
- Shorts paid funding on net (−0.44) rather than collecting it.

## 2. Information arms (same window; not part of the verdict)

| arm | net bp/day | NW t | median | blocks (bp/day) |
|---|---|---|---|---|
| H = 1, same construction | +8.2 | 0.92 | +4.2 | 18.5, 27.6, 18.8, −4.4, −18.2 |
| equal-weight short, ranks 101–200 | +7.0 | 0.77 | +2.8 | 17.6, 28.8, 17.5, −8.8, −21.5 |
| ranks 41–100, inv-vol cap 3.3 % | +11.7 | 1.37 | +9.7 | 27.0, 24.6, 22.9, −11.1, −5.3 |
| ranks 1–40, inv-vol cap 5 % | +9.0 | 1.32 | +10.6 | 25.8, 17.8, 12.1, −3.1, −6.9 |

The info arms use a cap of 2 / band size (twice equal weight, as the primary's 2 % on 100 names) so that the book can be fully invested. All four have the same
sign pattern, and the 101–200 band is the weakest, not the strongest.

- **The prior is falsified.** The pre-registration expected emission / unlock dilution to hit mid-caps hardest. The data show an alts-vs-BTC phase that hit every size
  band alike (2024 → mid-2025) and then reversed.
- **R4b's ranks 41–100 bleed**, seen in the 2021–26 control, is re-seen here on 2024–26 at +11.7 bp/day for the short. It is not independent evidence, and it
  carries the same reversal.

## 3. Worst days

- **Largest single-coin daily loss:** HIFI on 2025-09-12, −2.46 % of notional, on a +440 % coin day. The cap kept a 5× pump to a 2.5 % hit.
- **The 10 worst book days** are −7.7 % to −9.6 %. In each, the largest single-coin loss is under 0.8 %: these are broad alt rallies (2024-11, 2024-12, 2025-02,
  2025-11), sometimes with the BTC leg losing too (2025-11-04: BTC leg −11.1 %).
- **Diversification removed the single-pump tail that killed R4.** It cannot remove the market-wide "alt season" factor, which is the trade itself.

## 4. Verdict

**N1: DEAD.** No forward test.

Following the pre-registration's DEAD reading: the alt-vs-BTC underperformance seen in R4b's control is not a stable premium of mid-caps. It is a phase of BTC dominance in
2024 → mid-2025, and it reversed after, including in the unseen September 2026. Holding it is a directional macro bet on BTC dominance with about 50 %/yr volatility.
That is the kind of exposure the programme has avoided since the BTC-direction close.
