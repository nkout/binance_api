# R4 cross-sectional perp factors — FAIL; rank signals are strong, but right-tail pumps eat the shorts, and funding carry is the only thing that earns

*2026-10-01. Harness `runs/harness_xsec/` (`fetch_archive.py`, `xsec.py`, `run_r4.py`, `test_xsec.py`
20/20 PASS). Results `harness_xsec/r4_results.json`, `harness_xsec/r4_output.txt`. Data: every USDT-M
perp ever listed from the Binance public archive, 860 symbols incl. delisted (688 MB in `data/xsec/`,
not in git). Pre-registration: `next_signal_ideas.md`, R4. Local CPU, ~1 min.*

**TLDR.**
- **FAIL as pre-registered:** no factor × holding cell passes. Every cell has Holm-adjusted p = 1.0.
- **The rank signals are real and strong.** Out of sample, low realised volatility beats high
  (`RVOL30` IC −0.099, t −14.7), and recent winners underperform at 1–28 days (`MOM28` / `MOM7` /
  `REV1` IC −0.02, t −3.2 to −3.9).
- **But rank ≠ P&L.** Equal-weight quintile shorts of volatile alts lose on the *mean* while winning
  on the *median* (`RVOL30` short leg: median +16.8 bp/day, mean −12.9). A few +150 % to +500 % pump
  days a year (MYX, ALPACA, SIREN, …) wipe out the edge. Daily volatility of a dollar-neutral 8/8 book
  is ~5 %.
- **The one positive cell is funding carry:** `FUND7` (long the most negative-funding coins, short the
  most positive) nets +17.2 bp/day, positive in 3 of 4 OOS years, permutation 98.7th percentile, with
  the 1 h lag also positive. It fails only on significance (NW t 1.32, Sharpe 0.67, −97 % max
  drawdown). Its gross is **all funding** (+33 bp/day collected, −16 bp/day of price drift).
- **Next:** R5 (delta-neutral funding carry). R4 says funding is the return source in this market,
  and the hedge removes exactly the tail risk that sank this book.

---

## 1. Run validity

| check | result |
|---|---|
| symbols | 860 archived USDT perps → 857 after excluding stable / index bases; **376 were in the top-40 at some point** (survivorship-free: delisted LUNA, FTT, SRM, … included) |
| study | 2020-10-14 (first day with ≥ 40 eligible) → 2026-08-30; full universe on 2,148 / 2,148 days |
| in-sample / OOS | IS to 2022-10-14 (730 d, signs fixed here); **OOS 1,417 days** (2022-10 → 2026-08) |
| null | 1,000 random 8 / 8 books from the same daily universe (equivalent to permuting factor values) |
| tests | look-ahead (scrambling every hour ≥ 00:00 of d leaves all factors at ≤ d unchanged), funding sign, delisting, listing age, staggering, planted-reversal world passes, random-walk world passes nothing |
| data sanity | universe daily returns: p0.1 −37 %, p99.9 +54 %, 136 / 85,920 coin-days with \|TR\| > 50 %. The largest are real events (ALPACA +505 % on 2025-04-30 ahead of its delisting, DOGE +390 % 2021-01-28, MYX +301 % 2025-09-08), not bad ticks |

## 2. Signs and rank IC (daily Spearman vs next-day return)

| factor | sign (IS) | IC IS (t) | IC OOS (t) |
|---|---|---|---|
| MOM28 | −1 | −0.035 (−3.2) | **−0.024 (−3.9)** |
| MOM7 | −1 | −0.022 (−2.5) | −0.020 (−3.2) |
| REV1 | −1 | −0.025 (−2.9) | −0.019 (−3.2) |
| FUND7 | −1 | −0.031 (−4.0) | −0.004 (−0.7) |
| VSHOCK | −1 | −0.035 (−4.2) | −0.010 (−1.6) |
| **RVOL30** | −1 | **−0.108 (−11.7)** | **−0.099 (−14.7)** |

Every in-sample sign held out of sample, and four of six stayed significant: short-horizon reversal at
every lookback, and a large low-volatility effect.

## 3. Pre-registered cells (OOS, bp per day; cost 4.5 bp per side)

| factor | H | gross | net | net, 1 h lag | funding | c\* | Sharpe | NW t | years (4 × 365 d) | perm pct | max DD |
|---|---|---|---|---|---|---|---|---|---|---|---|
| MOM28 | 1 | −22.5 | −25.4 | −26.6 | −16.0 | — | −0.94 | −1.94 | −26 −26 −34 −14 | 0.5 | −404 % |
| MOM7 | 7 | −11.5 | −13.5 | −14.9 | −6.1 | — | −0.72 | −1.46 | −1 +1 −26 −30 | 0.0 | −248 % |
| REV1 | 7 | −3.6 | −5.6 | −5.4 | −1.0 | — | −0.60 | −1.22 | 0 −3 −6 −14 | 14.3 | −108 % |
| **FUND7** | **1** | **+19.7** | **+17.2** | **+15.7** | **+33.0** | **34.8** | **0.67** | **1.32** | **+24 −13 +17 +43** | **98.7** | **−97 %** |
| FUND7 | 3 | +15.5 | +13.7 | +13.5 | +28.7 | 39.4 | 0.58 | 1.12 | +21 −9 +16 +29 | 100.0 | −98 % |
| FUND7 | 7 | +9.7 | +8.4 | +8.5 | +23.6 | 34.6 | 0.42 | 0.78 | +13 −3 +14 +10 | 99.8 | −103 % |
| VSHOCK | 1 | −11.2 | −17.4 | −18.9 | −11.5 | — | −0.71 | −1.35 | −12 −4 −31 −24 | 10.9 | −380 % |
| RVOL30 | 1 | −6.9 | −7.8 | −10.7 | −22.1 | — | −0.27 | −0.55 | −20 −5 +9 −18 | 24.0 | −275 % |

(All 18 cells in `harness_xsec/r4_output.txt`. Max DD is the drop in the cumulative *sum* of daily
returns, in percentage points. c\* is only meaningful when gross is positive.)

**FUND7 H1 against the criteria:** net > 0 ✓, c\* 34.8 ≥ 6.75 ✓, 3/4 years ✓, permutation 98.7 ✓,
1 h lag positive ✓, **Holm p < 0.05 ✗** (one-sided p 0.09 before correction, 1.0 after). It fails on
noise, not on sign.

## 4. Why strong rank signals lose money — the right tail

| book (sign −1, H 1, OOS) | mean | **median** | daily std | skew | mean without the 1 % best + worst days |
|---|---|---|---|---|---|
| RVOL30 (long low-vol, short high-vol) | −7.8 | **+22.6** | 5.6 % | −3.0 | **+7.7** |
| MOM28 (long losers, short winners) | −25.4 | −14.4 | 5.2 % | −0.4 | −29.4 |
| FUND7 (long negative funding, short positive) | +17.2 | +2.9 | 4.9 % | +1.9 | +11.5 |

- **RVOL30** wins on the typical day (median +22.6 bp) and in the rank IC, but its short leg holds the
  most volatile alts, which occasionally go +150 % to +500 % in a day (worst book day −74 %). The mean
  goes negative. Trim the 1 % tails and it is +7.7 bp/day. The low-volatility effect is real; an
  equal-weight short of lottery coins is the wrong way to harvest it.
- **MOM28**'s losses are not tail-driven (trimmed −29 bp). Shorting 4-week winners in crypto
  loses steadily, even though the rank IC says winners underperform on the median coin.
- **FUND7** earns from the long leg: the bottom-quintile coins pay longs **0.127 % per 8 h**
  (~0.38 %/day). Those coins are heavily shorted, drift down in price (long-leg median −14.8 bp/day),
  and are occasionally squeezed upward, which is where the positive skew comes from.

## 5. What this settles

1. **R4 is closed in its pre-registered form.** Daily price / volume / funding factors on the top-40,
   equal-weight quintiles: no cell survives Holm across 18 tests.
2. **The rank effects are real, robust and in-sample-predicted:** reversal at 1–28 days and the
   low-volatility anomaly. This is the first set of out-of-sample-stable signals in the project. The
   obstacle is the payoff shape of shorting small alts, the same "win small, lose big" asymmetry that
   killed the 4 h BTC strategy (`v1_4h_feasibility.analysis.md` §9), not the signal.
3. **Funding is the only return source that showed up positive,** and it is collected, not predicted.
   R5 takes that directly and hedges the price risk.

## 6. Next steps

- **R5, delta-neutral funding carry (recommended).** Short perp / long spot on persistently
  positive-funding coins: no price exposure, so the pump tail that dominates §4 does not apply. The
  bottom-quintile side (long perp on negative funding) needs a spot short (margin borrow), which is
  harder. Pre-register both legs separately; the spot archive (`data/spot/monthly/klines`) gives the hedge.
- **Optional R4b, risk-controlled construction, as a new pre-registration.** Inverse-volatility
  weights with a per-coin cap, on the RVOL30 and reversal factors. This is motivated by §4, so it must
  be treated as a new hypothesis: pre-register one construction, no grid, Holm across its cells.
  Because the OOS period has now been seen, the honest test is on data after 2026-08.

## Reproduce

```
cd runs/harness_xsec
PY=/home/nkout/projects/binance2/binance2/.venv/bin/python   # + pyarrow on PYTHONPATH
$PY fetch_archive.py --workers 32   # ~30 min, resumable -> data/xsec/
$PY test_xsec.py                    # 20/20
$PY run_r4.py                       # ~1 min -> r4_results.json
```
