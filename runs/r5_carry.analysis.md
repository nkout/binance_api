# R5 delta-neutral funding carry — FAIL; plain BTC/ETH carry beat the selection but has decayed below risk-free, and the negative-funding "profit" is the borrow cost

*2026-10-01. Harness `runs/harness_xsec/` (`carry.py`, `run_r5.py`, `test_carry.py` 15/15, R4 tests still
20/20 after the `synth.py` refactor). Results `harness_xsec/r5_results.json`, `harness_xsec/r5_output.txt`.
Data: R4 perp archive + spot 1 h klines for the 376 coins ever in the top-40 (282 have a spot pair; the rest
are perp-only and cannot be hedged). Pre-registration: `next_signal_ideas.md`, R5. Local CPU, ~1 min.*

**TLDR.**
- **`CARRY+` FAILS.** Long spot / short perp when 7-day funding ≥ 0.03 % per 8 h: **+3.65 %/yr** on
  capital, below the 4.5 % risk-free rate, and negative in 2025 (−2.65 %) and 2026 YTD. It is low-risk
  (worst month −1.4 %, max drawdown −2.9 %, NW t 3.15) but rarely invested: on average **1.05 positions,
  invested on 17 % of days**. When it does trade it earns well (+51 %/yr per position). There just
  aren't enough hedgeable coins with high positive funding.
- **The selection loses to doing nothing clever.** The benchmark, a permanent BTC + ETH cash-and-carry,
  made **+8.77 %/yr** (Sharpe 9.5, worst month −0.44 %, positive every year), beating `CARRY+` by
  5.1 %/yr (t −5.2).
- **But the benchmark has decayed:** 25.5 % in 2021, then 1.8 % (2022), 6.0 %, 9.4 %, 3.8 % (2025),
  and **~1.5 % annualised in 2026 YTD**. It beat risk-free in only 3 of 5 full years and is well below
  it now. Funding carry on majors has been competed away.
- **`CARRY−` (+37 %/yr gross) is not money on the table.** It is long perp / short spot on
  negative-funding coins, and it is **gross of spot borrow**. Negative funding is what the market pays
  because the spot short is scarce or expensive; the per-position break-even borrow rate is
  ~168 %/yr. Without borrow-rate history this cannot be tested, and the arbitrage logic says the
  borrow cost absorbs most of it.

---

## 1. Run validity

| check | result |
|---|---|
| period | 2020-10-14 → 2026-08-30, 2,147 days (same start as R4) |
| universe | R4 top-40 ∩ spot pair with complete 30-day 1 h data and ≥ 2 M USD/day spot volume: **31.2 of 40 eligible per day** |
| tests | hysteresis, 10-position cap, look-ahead (positions at ≤ d unchanged when later inputs change), exact P&L identity (n · (Rs − Rp + F)), mirror sign, spot eligibility + volume floor, ms/µs archive timestamps, end-to-end synthetic (constant 0.1 % funding coin earns exactly n · 0.3 %/day, basis 0, one round trip) |
| costs | spot 7.5 + perp 4.5 bp per side → 24 bp per round trip of notional; 3× perp leverage → notional 0.075 of capital per slot |

## 2. Results (annualised, % of capital)

| | `CARRY+` (primary) | `BTCETH` (benchmark) | `CARRY−` (info, **gross of borrow**) |
|---|---|---|---|
| net | **+3.65** | +8.77 | +37.33 |
| = funding + basis − costs | 4.18 − 0.13 − 0.39 | 8.84 + ≈0 − ≈0 | 30.30 + 8.37 − 1.34 |
| Sharpe / NW t | 2.61 / 3.15 | 9.50 / 8.51 | 3.35 / 5.37 |
| worst month / max DD | −1.37 % / −2.87 % | −0.44 % / −0.61 % | −0.07 % / −0.72 % |
| 1 h execution lag | +3.62 | — | +36.03 |
| mean positions / days invested | 1.05 / 17 % | 2 / 100 % | 3.07 / 81 % |
| episodes, mean hold, coins | 129, 17.5 d, 86 | — | 439, 15.0 d, 193 |
| per position (per unit notional) | +51 %/yr | — | +168 %/yr |
| margin stress (perp +25 % above a short entry) | **54 of 129 episodes** | — | — |

Calendar years (net %):

| | 2020 (Q4) | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 (8 mo) |
|---|---|---|---|---|---|---|---|
| `CARRY+` | +2.12 | +11.97 | 0.00 | +1.44 | +8.76 | **−2.65** | **−0.14** |
| `BTCETH` | +4.20 | +25.46 | +1.80 | +5.95 | +9.38 | +3.75 | +1.03 |
| `CARRY−` | +1.87 | +5.73 | +36.26 | +33.64 | +3.57 | +74.84 | +63.65 |

**Pre-registered criteria for `CARRY+`:** annualised ≥ 4.5 % ✗ (3.65) · every year > 0 ✗ (2025, 2026) ·
worst month > −5 % ✓ · NW t ≥ 2 ✓ · 1 h lag > 0 ✓ → **FAIL.**

## 3. Reading it

1. **The binding constraint for `CARRY+` is capacity, not edge.** Positive-funding opportunities above
   0.03 % per 8 h on liquid, spot-listed coins are rare (one position on average, 17 % of days). Most of
   the funding-rich coins are perp-only memes that cannot be hedged. Idle capital earns nothing in this
   accounting, which is why the always-invested benchmark wins.
2. **Margin stress is frequent.** 54 of 129 episodes saw the perp rise > 25 % above the short's entry.
   High funding goes with pumps. P&L is hedged, but a 3× short needs collateral moved from the spot side
   in time, an operational risk the backtest does not charge for.
3. **Cash-and-carry on BTC/ETH is real but compressed.** The full-period +8.8 % is mostly 2021's
   +25 %. Since 2022 it has averaged ~4.6 %/yr, roughly risk-free with exchange and operational risk
   added, and 2026 runs at ~1.5 % annualised. It is not a strategy that beats cash today.
4. **`CARRY−` is a price, not an anomaly.** Its returns (biggest names BNB, WAVES, BCH, ENA, TRUMP)
   concentrate in coins and periods where spot borrowing is constrained (launchpools, squeezes,
   delisting fights). The pre-registration made it information-only for exactly this reason.

## 4. What this settles, and the programme

- **R5 is closed in its pre-registered form.** Selective positive-funding carry does not beat
  risk-free at VIP0 fees after 2024, and the passive BTC/ETH version has decayed to around or below it.
- **Across R4 and R5,** funding is the only return source that ever showed up positive, and it is a
  *collected* premium (carry), not a predicted move. Its size has shrunk as the trade got crowded. What
  remains is either capacity-limited (alt carry), compressed (BTC/ETH carry), or a payment for a scarce
  short (negative funding).
- **Options if work continues** (each a new pre-registration, forward-tested on post-2026-08 data
  because this sample is now spent):
  1. **Carry overlay:** BTC/ETH carry as the base with alt `CARRY+` positions using the idle capital.
     Fixes the utilisation problem, but the base return is near risk-free now.
  2. **`CARRY−` with real borrow rates:** Binance margin interest history needs an API key. A live
     snapshot of current borrow rates and availability for the coins `CARRY−` would have held would
     show whether any of the +168 %/yr survives. Cheap, and decisive in one direction.
  3. **Stop and write up the programme.** The honest summary is that at retail fee tiers, none of the
     tested sources (BTC direction at 5 s – 4 h, cross-sectional factors, funding carry) produced an
     edge above risk-free after costs.

## Reproduce

```
cd runs/harness_xsec
PY=/home/nkout/projects/binance2/binance2/.venv/bin/python   # + pyarrow on PYTHONPATH
$PY fetch_archive.py --spot --symbols-file ever_in_universe.json --workers 32   # spot klines, ~15 min
$PY test_carry.py && $PY test_xsec.py                                         # 15/15, 20/20
$PY run_r5.py                                                                  # ~1 min -> r5_results.json
```
