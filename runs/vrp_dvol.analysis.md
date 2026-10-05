# V1 DVOL vs forward realised volatility — pre-registered gates PASS, narrowly; the evidence is carried by 2021–2022 and the veto does not beat always-short

*2026-10-05. Harness `runs/harness_vrp/` (`fetch_dvol.py`, `vrp.py`, `run_vrp.py`, `diag_vrp.py`, `test_vrp.py` 26/26 PASS). Results
`harness_vrp/vrp_results.json`, `vrp_output.txt`, `vrp_diag_output.txt`. Data: Deribit DVOL daily (public API, 2021-03-24 → 2026-10-05,
2,022 days, no gaps) in `data/dvol_daily.parquet` (not in git); BTC 5-minute klines to 2026-09-22. Pre-registration: `next_signal_ideas.md`,
Round 5, V1. Local CPU, seconds. Origin: `vol_monetisation_probe.plan.md`.*

**TLDR.**
- **Both pre-registered gates pass.** P1: the HAR-RV forecast carries information the DVOL market price lacks (slope of
  `log(RV/IV)` on `log(F/IV)` = **+0.41, NW t 3.18**; the naive trailing-RV forecast has slope −0.02). P2: short volatility only when
  the forecast is below IV earns **+3.04 vol points per entry day at a 2-point friction, CI [+1.46, +4.78]**, positive in 5 of 6 calendar years.
- **There is a variance risk premium:** mean IV 60.2 vs mean RV 55.0, **+5.2 vol points (NW t 3.25)**, IV above RV on 70 % of days.
- **But the pass is narrow and old.** P2's year test is met by 0.1–0.4 vol points (2023 +0.4, 2026 +0.1; 2025 −0.3 is the negative year).
  P1's slope is **+0.77 (t 3.06) in 2021–22 and +0.19 (t 1.43, not significant) in 2023–26.** The veto-short book earns +8.1 per entry day
  in 2021–22 and +0.6 in 2023–26. The premium shows the same decay as funding carry.
- **The veto does not beat always-short:** veto minus always-short at f = 2 is **−0.15 (CI [−2.09, +1.97])**. What it does is cut the typical
  tail (worst 1 % of entries −17 vs −60 vol points), not the worst event (May 2021, RV 158 vs IV 76, −84 either way).
- **Read it as:** the premise is not dead, but it is weaker than the gates suggest. It does not justify building the detector probe or an
  options account yet.

## 1. Run validity

| check | result |
|---|---|
| tests | 26/26: RV from known-sigma 5-minute GBM, missing bars → NaN, alignment (IV_t = DVOL close of day t − 1; RV_t = days t … t + 29; naive = days t − 30 … t − 1), leak tests (scrambling prices after day T leaves IV, HAR inputs, naive and the HAR forecast at ≤ T unchanged; later targets differ), HAR recovers a planted persistent vol, NW / block-bootstrap checks, an efficient-IV world (a pure level premium) fails P1 (0 of 10 pass), a noisy-IV world passes P1 and P2, exact P&L identity |
| sample | 1,978 entry days, 2021-03-25 → 2026-08-23 (the last 29 days lack a forward window); ~66 independent 30-day periods |
| HAR | expanding OLS on log RV, refit monthly from 2020-09, 30-day purge; every DVOL day is out of sample |
| two defects caught by the tests before the run | the vol-point P1 regression returns slope 1 under a constant proportional premium (replaced by the log-ratio form in the pre-registration, before any real-data number); the P&L sign convention was inverted for short vol (fixed; the worlds test caught it) |

## 2. Pre-registered results

**P0 premium.** Mean IV 60.2, mean RV 55.0, VRP +5.19 vol points, NW t 3.25; median +7.51; non-overlapping (n = 66) +6.91, t 3.18; worst 1 %
−58.3. IV exceeded RV on 70 % of days.

**P1 skill beyond the market.**

| forecast | slope | NW t | corr | sd of log(F/IV) |
|---|---|---|---|---|
| HAR-RV | **+0.412** | **3.18** | +0.23 | 0.14 |
| naive trailing 30-day RV | −0.023 | −0.29 | −0.02 | 0.18 |

Moving-block bootstrap of (x, y) pairs: slope 95 % CI [+0.18, +0.68].

**P2 arms** (vol points per entry day, flat days count 0; short vol earns IV − RV):

| arm | f = 0 | f = 2 | active | per active entry (f = 2) mean / median | worst 1 % / worst (f = 2) | CI (f = 2) | 1-day-later entry |
|---|---|---|---|---|---|---|---|
| **veto-short** (F < IV) | +3.62 | **+3.04** | 29 % | +10.47 / +11.51 | −17.3 / −83.8 | [+1.46, +4.78] | +2.94 |
| long-timed (F > IV) | −1.57 | −2.99 | 71 % | −4.21 / −7.60 | −30.9 / −48.1 | [−5.03, −0.98] | −3.08 |
| always-short | +5.19 | +3.19 | 100 % | +3.19 / +5.51 | −60.3 / −84.1 | [+0.09, +6.20] | +3.18 |
| veto-short, naive forecast | +3.90 | +2.47 | 72 % | +3.44 / +5.55 | −34.1 / −79.0 | [−0.09, +4.97] | +2.46 |

By calendar year at f = 2: veto-short **+8.4, +7.8, +0.4, +1.9, −0.3, +0.1** (2021 … 2026 YTD); always-short +6.1, +8.6, +2.2, +3.3, +0.2, −2.7;
long-timed +1.3, −2.9, −5.4, −4.0, −4.3, −1.1. Veto-short minus always-short at f = 2: −0.15, CI [−2.09, +1.97].

| criterion | result |
|---|---|
| P1: slope > 0, NW t ≥ 2 | ✓ (+0.412, 3.18) |
| P2: veto-short mean > 0 at f = 2, block-bootstrap CI lower bound > 0, ≥ 75 % of years positive | ✓ (+3.04, CI lower +1.46, 5 / 6 years, needs 5) |

By the pre-registered decision table, **both pass**.

## 3. Robustness diagnostics (run after the verdict; descriptive, they do not change it)

| check | result |
|---|---|
| P1 slope by period | 2021–22 **+0.77 (t 3.06)**; 2023–26 **+0.19 (t 1.43)**; 2024–26 +0.21 (t 1.37) |
| drop the worst window (May 2021, RV/IV 2.09) | slope +0.45, t 3.52: not carried by one episode |
| HAR forecast bias | mean log(RV/F) is −0.06 in 2021 and −0.16 … −0.26 afterwards: the forecast runs 15–30 % above realised vol, so F < IV on only 14 % of days from 2023 (60 % in 2021–22) |
| veto-short by period | 2021–22: gross +9.27, active 60 %, break-even friction 15 vol points per active entry; 2023–26: gross +0.87, active 14 %, net at f = 2 **+0.59**, break-even 6.3 |
| convex (variance-swap) payoff `(IV² − RV²) / (2 IV)` instead of vol points | veto-short +2.22 at f = 2, worst 1 % −19.5, **worst −127.8**; always-short +1.03, worst 1 % −79.0; 2025 −0.3, 2026 +0.0 |

## 4. Reading it

1. **A premium exists, but it was earned mainly in 2021–22.** From 2023 the veto book is essentially flat, and always-short earns +0.7, +2.1, −0.9, −4.3
   (2023 … 2026, variance-payoff version, f = 2). This is the same decay shape as funding carry in R5.
2. **The forecast contains real information about relative mispricing,** and the naive forecast does not. That is the part of the
   pre-registered premise that survives. But the gain is strong in 2021–22 and not significant afterwards (t 1.43), and the monthly
   independent sample is only ~66, so "the market is persistently mispriced" is not established.
3. **The veto is a risk filter more than a return source.** It leaves the mean unchanged versus always-short and cuts the 1 % tail from −60
   to −17 vol points, by being out of the market 71 % of the time (86 % since 2023). It does not avoid the single worst event: the forecast
   (F 72 vs IV 76) said "quiet" on 2021-05-02 and RV came in at 158.
4. **The proxy flatters the trade.** DVOL is an index, not a position. The P&L is a frictionless vol-point (and variance-payoff) proxy for a
   delta-hedged option book. The friction of 2 vol points per entry is an unverified assumption and does not include delta-hedging cost
   (perp taker fees on every hedge rebalance), margin, or the convexity loss when RV spikes: the worst variance-payoff outcome is −128 vega points.
5. **The forecast bias matters for the rule, not for P1.** The log-ratio regression is invariant to a constant bias in F; the rule `F < IV`
   is not, which is why it rarely trades after 2022. A recalibrated threshold would be a new, tuned hypothesis.

## 5. What this settles and what it does not

- **The pre-registered premise holds on its letter:** HAR carries information beyond DVOL (P1) and a veto-short proxy clears a 2-vol-point
  friction (P2). By the pre-registered decision this makes the detector probe in `vol_monetisation_probe.plan.md` and a real
  option-cost study "worth running".
- **It does not make the trade attractive.** The evidence is carried by 2021–22; 2023–26 are ≤ +0.6 vol points per entry day in the
  best arm; the veto adds nothing in mean; and the proxy omits the costs that matter. The honest summary is that **a volatility premium
  exists and is shrinking**, the same as carry.
- **What the detector would have to do.** Its job would be to add information to HAR in 2023–26, where HAR's own edge is not significant.
  The detector scores cover only 40–331 days (a window dominated by the same recent regime), so even a positive result would have
  very low power: ~35 independent months at best.
- **Next steps if pursued, cheapest first:** (i) forward-score this exact rule on the weeks since 2026-08-23 as DVOL accrues (zero cost);
  (ii) price one real option book: Deribit expired-instrument trades give actual spreads and fills; (iii) only then the detector probe.
  I would not start (iii) first.

## Reproduce

```
cd runs/harness_vrp
PY=/home/nkout/projects/binance2/binance2/.venv/bin/python   # + pyarrow on PYTHONPATH
$PY fetch_dvol.py       # public Deribit API -> data/dvol_daily.parquet
$PY test_vrp.py         # 26/26
$PY run_vrp.py          # -> vrp_results.json, vrp_output.txt
$PY diag_vrp.py         # robustness diagnostics -> vrp_diag_output.txt
```
