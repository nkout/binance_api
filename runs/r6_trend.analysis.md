# R6 daily trend-following + volatility targeting — FAIL (narrowly): lower drawdown and a better Sharpe than buy-and-hold, but not by the pre-registered margin, and the gain is drawdown avoidance, not return

*2026-10-05. Harness `runs/harness_xsec/` (`trend.py`, `run_r6.py`, `test_trend.py` 29/29 PASS; R4 20/20 and R5 15/15
still pass). Results `harness_xsec/r6_results.json`, `harness_xsec/r6_output.txt`. Data: the R4 perp archive
(`data/xsec/`, BTCUSDT 1 h klines + funding, 857 perps for the per-coin arm). Pre-registration:
`next_signal_ideas.md`, Round 4, R6. Local CPU, ~1 min.*

**TLDR.**
- **1A FAILS two of five criteria.** Net Sharpe 0.92 vs buy-and-hold 0.69 (needs ≥ +0.30; got +0.23), and it beats
  cash in 4 of 7 calendar years (needs 5). It passes max drawdown (−33 % vs −79 %), the shift-null (97.9th
  percentile) and the 1-day lag (+21 %/yr).
- **It does not make more money than holding BTC.** Net +31.8 %/yr vs +39.6 %/yr for buy-and-hold. What it buys is
  risk: volatility 35 % vs 57 %, drawdown −33 % vs −79 %, skew +1.03 vs +0.14. The paired-bootstrap CI of the
  Sharpe difference is **[−0.67, +1.22]**, so "better than buy-and-hold" is not established.
- **All of the gain is the long side.** Long leg +33.2 %/yr, short leg +0.4 %/yr. The rule helps by being flat
  through 2022 (−4 % vs BTC −66 %), not by profiting from downtrends.
- **1C (per-coin trend, long / flat) FAILS four of five criteria** (Sharpe 0.28 vs inverse-vol control 0.05, but
  shift-null percentile 76.2, drawdown −51 % vs −90 %, beats cash 3 of 6). Not distinguishable from the control.
- **Verdict:** the pre-registered kill applies: the trend premium is not established here beyond a risk
  reduction. See §4 for the caution on the long-only variant.

## 1. Run validity

| check | result |
|---|---|
| tests | 29/29: signal values, vol targeting realises ~40 %, cap, band, look-ahead (scrambling later prices leaves everything at ≤ d unchanged), exact P&L and cost identity, XS weights, shift null, planted-trend world passes (Sharpe 1.33 vs −0.61, null 100th pct), 20 random-walk worlds → 0 pass, end-to-end on the on-disk format |
| independent re-derivation | arm A and buy-and-hold recomputed from the raw klines and funding with plain pandas (no harness code): Sharpe 0.92 / 0.692, +31.83 / +39.62 %/yr, per-year returns identical |
| window | BTC 2020-05-01 → 2026-08-30, 2,313 days (120-day lookback + 30-day vol warm-up from the 2020-01 archive start); per-coin 2021-02-11 → 2026-08-30, 2,027 days (first full top-40 day 2020-10-14 + 120 d) |
| costs | 4.5 bp per side on every position change, funding as real P&L, idle capital earns 0 |

## 2. BTC arms (net, % per year, on capital)

| arm | net | Sharpe | vol | max DD | skew | worst day | exposure | beats cash | 1-day lag |
|---|---|---|---|---|---|---|---|---|---|
| **A** trend, long / short | +31.8 | **0.92** | 34.6 % | −33.3 % | +1.03 | −9.7 % | 0.57 | 4 / 7 | +21.4 |
| A_long (info) | +32.3 | 1.21 | 26.6 % | −22.8 % | +1.85 | −9.7 % | 0.34 | 5 / 7 | +26.4 |
| B vol-targeted hold | +32.0 | 0.71 | 45.1 % | −65.8 % | +0.19 | −14.2 % | 0.85 | 4 / 7 | +32.7 |
| BH buy-and-hold | +39.6 | 0.69 | 57.3 % | −78.9 % | +0.14 | −15.4 % | 1.00 | 4 / 7 | +39.3 |

Calendar years (net %): 

| | 2020 (8 mo) | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 (8 mo) |
|---|---|---|---|---|---|---|---|
| A | +128.3 | +4.3 | −4.0 | +52.5 | +34.7 | −6.0 | +17.0 |
| BH | +199.7 | +17.7 | −65.7 | +136.5 | +96.3 | −11.0 | −12.9 |

Pre-registered criteria for 1A: Sharpe ≥ BH + 0.3 ✗ (0.92 vs 0.69 + 0.3) · max DD ≤ ½ of BH ✓ (−33.3 vs −39.5) ·
beats cash ≥ 5 of 7 years ✗ (4; 2021, 2022, 2025 below 4.5 %) · shift-null ≥ 97.5 ✓ (97.9) · lag-1d positive ✓ → **FAIL.**

## 3. Reading it

1. **Risk reduction, not alpha.** A earns less than buy-and-hold in every year except 2022, 2025 and 2026 and
   makes up the difference by avoiding the 2022 drawdown. B (the same sizing with no signal) already cuts the
   2022 loss only from −66 % to −55 %, so the sign signal is what keeps A out of the 2022 decline. The short
   leg contributes +0.4 %/yr: shorting the downtrends earned nothing.
2. **Two of the seven years are partial** (2020 from May, 2026 to August) and 2020 contributes +128 %. Without it A's
   record is +4, −4, +53, +35, −6, +17: positive but modest, and below cash in 2021, 2022, 2025.
3. **The Sharpe edge is within noise.** Paired stationary bootstrap of the Sharpe difference vs buy-and-hold:
   95 % CI [−0.67, +1.22]. Seven years hold only a handful of independent trend episodes.
4. **Timing does carry information:** the shift-null puts A at the 97.9th percentile of the same positions
   shifted against returns (median null Sharpe −0.02). That is weak evidence that the signal times BTC, but it is
   a statement about timing, and the economic bar (beat buy-and-hold by 0.3 Sharpe, beat cash in 5 of 7 years) was
   not met.
5. **Per-coin trend (1C) adds nothing over its control.** 1C Sharpe 0.28 vs inverse-vol always-long 0.05
   (CI of the difference [−0.36, +0.81]); shift-null 76th percentile. It lowers drawdown (−51 % vs −90 %) but
   only 29 % of capital is invested on average, and the skew is −1.88 (left tail from altcoin crashes while long).

## 4. Caution on the long-only variant

`A_long` (signal clipped at 0) has Sharpe 1.21, −22.8 % drawdown and beats cash in 5 of 7 years, which would
pass the Sharpe and cash criteria. It was an **information arm in the pre-registration, not the primary**, and it
was looked at after A failed. Promoting it now would be choosing the best of several variants after the fact.
It is a hypothesis for a forward test (pre-register it and score it on data after 2026-08), nothing more.

## 5. Clarifications and deviations from the pre-registration

- **Information arms.** The pre-registration said "1A long / short and 1C long / short" as information arms, but
  the primary 1A signal in [−1, +1] is already long / short. Implemented as: primary 1A = long / short; info
  `A_long` = long-only; primary 1C = long / flat; info `1C_ls` = signed.
- **Window.** The archive's funding starts 2020-01, so the 120-day lookback puts the first live day at
  2020-05-01 (6.3 years). "Years" means calendar years in the window: 7 for BTC (2020 and 2026 partial), 6 for 1C;
  the cash criterion is scaled to ⌈5/7 × years⌉ (5 of 7, 5 of 6).
- **Rebalance band.** "Moves by more than 10 %" implemented as an absolute 0.10 of capital.
- **Null.** Circular shift of the position path against returns (2,000 shifts, ≥ 90 days; 500 for 1C) instead of
  "random signals with the same turnover". It keeps exposure, turnover and position autocorrelation exactly.
- Position weights drift with price between rebalances is not modelled (positions held at fixed notional fractions).

## 6. What this settles

- **R6 is closed in its pre-registered form.** A diversified 20 / 60 / 120-day trend rule on BTC cut drawdown by
  more than half and raised Sharpe by 0.23, but did not earn more than buy-and-hold and did not clear the
  pre-registered margin. A per-coin version does no better than its control.
- **It is a sizing / risk result, in line with the programme's finding** that the robust assets are risk
  controls (the volatility detector, vol targeting), not return sources.

## Reproduce

```
cd runs/harness_xsec
PY=/home/nkout/projects/binance2/binance2/.venv/bin/python   # + pyarrow on PYTHONPATH
$PY test_trend.py          # 29/29
$PY run_r6.py              # ~1 min -> r6_results.json, r6_output.txt
```
