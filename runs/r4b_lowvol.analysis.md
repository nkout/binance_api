# R4b low-volatility factor, beta-hedged — FAIL; the hedge removes BTC but not the alt bleed, and low-vol selection adds only an insignificant +3 bp/day

*2026-10-05. Harness `runs/harness_xsec/` (`lowvol.py`, `run_r4b.py`, `test_lowvol.py` 24/24 PASS; R4 20/20, R5 15/15, R6 29/29
still pass). Results `harness_xsec/r4b_results.json`, `harness_xsec/r4b_output.txt`. Data: the R4 perp archive (`data/xsec/`).
Pre-registration and the implementation details fixed before the code: `next_signal_ideas.md`, R4b. Local CPU, ~80 s.*

**TLDR.**
- **FAIL on every cell and nearly every criterion.** On the disjoint universe (volume ranks 41–100, 2021-07 → 2026-08,
  1,864 days) the primary book (long the lowest-RVOL30 quintile, inverse-vol weights, short BTC sized to the 60-day beta)
  nets **−6.0 bp/day (−21.8 %/yr), NW t −1.18** at H = 1 and −5.7 bp/day at H = 7. Positive in 0 of 5 and 1 of 5 365-day
  blocks, negative with the 1 h lag.
- **The factor itself is not rejected, but it is small.** Primary minus the beta-hedged equal-weight control is
  **+3.4 bp/day (NW t 1.09)** at H = 1 and +3.7 (t 1.29) at H = 7: right sign, about +12 %/yr, not significant.
- **Why the book loses:** the beta hedge neutralises BTC but the alts then bleed against it. The equal-weight control
  alone loses −9.4 bp/day. The low-vol long leg earned +3.9 bp/day in price (−1.2 funding) while the BTC short cost
  −9.9 bp/day in price (+1.9 funding) because BTC rose 8.3 bp/day on average over this window.
- **Top-40 (info, ranks 1–40 ex BTC):** primary +4.1 bp/day (NW t 0.81), beats control by +4.1 (t 0.95); fails too.
- **Verdict:** per the pre-registered kill, low volatility is real but not harvestable in this form: it needs shorts
  of lottery coins that this construction does not carry, and long-only low-vol alts hedged with BTC only earns the
  alts-vs-BTC spread plus a small selection effect.

## 1. Run validity

| check | result |
|---|---|
| tests | 24/24: rank bands (disjoint, ordered, equal to `xsec.universe`), inverse-vol weights and caps (incl. redistribution and the short arm 12 × 0.025 = 0.30), staggered mean, hedge beta (unhedged 1.51 → hedged 0.00 on a planted world), look-ahead (changing vols after d and returns from d on leaves positions at ≤ d unchanged), exact P&L and cost identity incl. the hedge leg, planted low-vol premium world passes every criterion, 10 null worlds pass 0 times, an alts-vs-BTC drift without a vol premium is *not* credited to the factor, end-to-end on the disk format |
| universe | point-in-time ranks 41–100 by trailing 30-day volume, BTC excluded; ≥ 100 eligible coins from 2021-05-24; study 2021-07-24 (+60-day beta warm-up) → 2026-08-30 |
| flat days | 125 of 1,864 (6.7 %): 63 days in 2022 with fewer than 100 eligible coins, plus the 60-day beta rebuild after the gap. They count as zero return in every statistic. Live-day mean −6.4 bp/day, t −1.18 |
| median | the table's median **+0.0** is an artifact of those 124 exact-zero days sitting at the middle of the distribution; the median on live days is **−2.9 bp/day** |
| correction made during testing | the permutation null is on the **gross** mean (a random quintile redrawn every day pays several times the turnover of a persistent rank, which biased the net-mean null; found on synthetic worlds, before any real-data run; recorded in `next_signal_ideas.md`) |

## 2. Primary universe, ranks 41–100 (bp per day, net of 4.5 bp per side)

| H | arm | net | ann | Sharpe | NW t | median | std | skew | worst day | max DD | β to BTC | blocks + | 1 h lag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | **primary** | **−6.0** | −21.8 % | −0.55 | −1.18 | +0.0* | 2.06 % | +0.24 | −13.7 % | −159 pp | −0.01 | 0 / 5 | −6.0 |
| 1 | control (EW, hedged) | −9.4 | −34.4 % | −0.73 | −1.62 | +0.0* | 2.48 % | −0.23 | −13.8 % | −275 pp | 0.00 | 1 / 5 | −9.0 |
| 1 | info: long low-vol / short high-vol | −3.0 | −10.9 % | −0.32 | −0.66 | +0.0* | 1.78 % | +0.69 | −10.1 % | −97 pp | −0.02 | 1 / 5 | −3.2 |
| 7 | **primary** | **−5.7** | −20.8 % | −0.52 | −1.09 | +0.0* | 2.11 % | +0.11 | −13.7 % | −161 pp | −0.03 | 1 / 5 | −5.4 |
| 7 | control | −9.4 | −34.4 % | −0.72 | −1.62 | +0.0* | 2.49 % | −0.33 | −14.1 % | −269 pp | −0.02 | 1 / 5 | −9.1 |
| 7 | info long / short | −2.6 | −9.5 % | −0.29 | −0.59 | +0.0* | 1.74 % | +0.58 | −9.5 % | −94 pp | −0.02 | 1 / 5 | −2.4 |

\*median artifact, see §1. Costs are small (0.3–0.9 bp/day); the book is not cost-limited.

Primary H = 1 by 365-day block (bp/day): −6.8, −8.4, −7.9, −6.7, −0.4. By calendar year (net %): 2021 −16.6, 2022 −17.6,
2023 −17.0, 2024 −26.5, 2025 −50.2, 2026 (8 mo) +5.7.

Pre-registered criteria (primary):

| criterion | H = 1 | H = 7 |
|---|---|---|
| net > 0, NW t ≥ 2, Holm p < 0.05 | ✗ (−6.0, t −1.18, p 1.0) | ✗ (−5.7, t −1.09, p 1.0) |
| primary − control, NW t ≥ 2 | ✗ (+3.4, t 1.09) | ✗ (+3.7, t 1.29) |
| ≥ 75 % of 365-day blocks positive | ✗ (0 / 5) | ✗ (1 / 5) |
| permutation percentile of gross ≥ 97.5 | ✗ (91.0) | ✓ (100.0, but the null mean is itself negative) |
| positive with 1 h lag | ✗ (−6.0) | ✗ (−5.4) |
| median and mean same sign, mean > 0 | ✗ | ✗ |

The H = 7 permutation pass says only that this quintile beats *random* quintiles of the same alts, which lose even more.

## 3. Reading it

1. **P&L decomposition (H = 1, live and flat days).** Long alt leg: price **+3.9**, funding −1.2 bp/day. BTC hedge leg (mean
   notional 0.94): price **−9.9**, funding +1.9. Costs −0.6. Total −6.0. BTC averaged +8.3 bp/day over the window
   (+17.8 %/yr compounded); the hedge notional was also higher when BTC went on to rise (covariance term +2.1 bp/day,
   not look-ahead: β uses days up to d − 1), which made the hedge more expensive.
2. **The alts-vs-BTC bleed is the dominant term.** After removing β, mid-cap perps lost to BTC every 365-day block.
   The control, a plain beta-hedged basket of the whole universe, loses −9.4 bp/day. This is what the pre-registered
   control was for: without it, a positive result here would have been the bleed run in reverse.
3. **The low-vol selection effect is positive but weak:** +3.4 bp/day (≈ +12 %/yr) over the control with NW t 1.1–1.3. In R4
   the same factor had out-of-sample IC −0.099 (t −14.7) on the top-40; the rank effect is real, but harvested as a long-only
   tilt it is a small fraction of the cross-sectional dispersion of alt returns.
4. **Shorting the high-vol quintile helps the book but is capped at 2.5 % per coin.** The info arm improves the primary by
   +3 bp/day (−3.0 vs −6.0) with a lower beta need and positive skew, yet is still negative. The gross short leg is only 0.30 of
   the long notional, so this is not the R4 short leg that sank on pumps.
5. **Top-40 re-run (information only):** primary +4.1 bp/day, 3 of 5 blocks positive, NW t 0.81; the first block is +29.6 bp/day
   (2020-12 → 2021-12, the alt rally) and the other four are −10 … +5. Not a result.
6. **The post-2026-08 forward test cannot be run yet** (archive ends 2026-08-30).

## 4. What this settles

- **R4b is closed in its pre-registered form.** Low-volatility alts, inverse-vol weighted and hedged with a BTC short, do not
  earn a positive return on a universe the earlier study did not use; the control shows why (alts lose to BTC after β).
- Combined with R4 (rank signals strong, shorts sink on pumps) the lesson is the same as the programme's: the rank effect
  exists, and the way to monetise it in this market (a long-only tilt, or a short leg of lottery coins) is not
  available at a retail fee tier with these instruments.
- Nothing new survives for trading. The one thing left standing from this round is the descriptive result: low-vol selection
  beats a random alt basket in sign and is insignificant in size.

## Reproduce

```
cd runs/harness_xsec
PY=/home/nkout/projects/binance2/binance2/.venv/bin/python   # + pyarrow on PYTHONPATH
$PY test_lowvol.py     # 24/24
$PY run_r4b.py         # ~80 s -> r4b_results.json, r4b_output.txt
```
