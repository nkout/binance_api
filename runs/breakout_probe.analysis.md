# Breakout probe (idea 1a) — FALSIFIED: moves do not continue after the volatility detector fires

*2026-09-22. Harness: `runs/harness_breakout/breakout_probe.py` (+ `test_breakout_probe.py`, 8/8
PASS), results `runs/harness_breakout/breakout_probe_results.json`. Input: `runs/run009f_scores.npz`
only — CPU, 3 s. Motivation: `next_signal_ideas.md` §1a.*

**TLDR:** the detector-triggered breakout fails all four pre-registered criteria. Continuation is
**0.516** (coin), net at taker is **−6.30 bp [−26.7, +15.0]**, the model sits at the **72.8th**
percentile of a day-matched random-entry null, and the only positive leg is the long side
(+27.6 vs short −16.0) — i.e. the 1 h up-drift on high-vol bars that `gbm_probe.analysis.md` §3
already documented, not continuation. Across 55 exploratory cells with n ≥ 100, **none is
net-positive at taker**, and none of the 48 positive-skew trailing-stop cells grosses more than the
10 bp fee. Magnitude alone does not become P&L through linear instruments on this data.

---

## 1. What was tested

When `P(lup_18) + P(ldn_18)` crosses a **causal** threshold (quantile of the same score on the fold's
own validation window, which immediately precedes the test window), rest OCO stop orders at
±k bp. The first 5 s close through a level is a taker entry at that close (gap slippage included);
exit after H bars (taker) or by a k-bp trailing stop; no cross within W bars → cancel at no cost.
One position at a time; a trigger is used only if the whole window stays inside one contiguous
segment of one fold.

Why this is the right question: under a martingale a stop entry has zero expected value, so any
gross here *is* conditional continuation. The synthetic test confirms the simulator has no built-in
bias — random walk gross **−0.49 ± 0.47 bp** (hold) and **−0.004 ± 0.08** (trail); an AR(1)
momentum world gives **+1.06**.

**Pre-registered primary:** rate 0.1 %, k = 10 bp, W = 15 min, H = 60 min, hold, 10 bp RT.

## 2. Sanity

| score @ 0.1 % | realised trigger rate | E\|move\| 1 h on triggers | all bars | lift |
|---|---|---|---|---|
| detector (`det18`) | 0.268 % | 50.4 bp | 24.6 bp | **2.05×** |
| model-free trailing 5-min realised \|r\| (`rv5m`) | 0.399 % | 57.4 bp | 24.6 bp | **2.33×** |

The detector's 1 h lift matches `horizon_economics…` §7.2 (2.27× with the per-fold threshold).
The realised rate is 2.7× nominal because the test windows were more volatile than their val
windows (the same prevalence shift as `run009d.offline.md` §1).

**Side finding:** at the 1 h horizon a one-line trailing realised-vol score gives *at least* the
detector's magnitude lift (rates differ, so this is not an equal-rate comparison). For anything
at ≥ 1 h the LSTM detector is not needed, which means volatility-conditioned ideas can be tested
on the **7-year klines** instead of 42 days.

## 3. Primary cell — FAIL on all four criteria

| | value | criterion | |
|---|---|---|---|
| orders placed / filled | 32 / 31 over 19 days | — | |
| gross / median | +3.70 / +0.77 bp | — | |
| net @ 10 bp | **−6.30**, CI [−26.74, +14.97] | P1 net > 0, CI lo > 0 | **FAIL** |
| day-matched null | mean −1.77, p97.5 +15.27, model **72.8th** pct | P2 > p97.5 | **FAIL** |
| continuation P(side·ret > 0) | **0.516** | P3 ≥ 0.55 | **FAIL** |
| long / short leg | **+27.62** (n 14) / **−16.00** (n 17) | P4 both > 0, ≥ 3/4 folds | **FAIL** |
| per fold | +4.8 · −2.3 · −38.7 · **+52.7** | | 2/4 |

**The primary is underpowered** (n = 31; the CI is ±20 bp): one-position-at-a-time with a 75-min
lock-up and clustered triggers leaves few independent trades in 42 days. It cannot *prove* zero.
But it shows none of the signatures a real effect would: continuation is a coin flip, it does not
beat random entry on the same days, and its only positive component is the long leg on up-drifting
days, carried by fold 3 — the same pattern as every earlier false positive in this repo.

## 4. Exploratory grid (96 cells) — what the larger samples say

Grid: {det18, rv5m} × rate {0.1 %, 1 %} × k {5, 10, 20} × W {5, 15 min} × H {15, 60 min} × {hold, trail}.

- **55 cells with n ≥ 100: 0 net-positive at taker.** Best: det18 1 % k5 W15m H60m hold, gross
  +6.66, net −3.34 [−16.9, +11.9]. Best upper CI bound anywhere is +11.9 bp.
- **Trailing stop (the positive-skew design), 48 cells:** skew +2.7 … +7.7 as intended, win rate
  0.35–0.51, but **gross never exceeds the 10 bp fee** (max +9.2, n = 76). The trend-follower shape
  is there; the drift to pay for it is not.
- **Long leg ≫ short leg in almost every hold cell** — drift, not continuation. (On
  volatility-selected bars in this window `P(up)` at 1 h was 0.873 — `gbm_probe.analysis.md` §3.)
- **Best-of-96 cell** (det18 0.1 % k5 W15m H60m hold): net +3.65, CI [−14.8, +24.0], n = 33, legs
  +35.5 / −4.6. Maximum of 96 noisy cells with no multiple-testing correction; recorded so it is
  not rediscovered as a result.
- **Model-free `rv5m` at 0.1 %, hold: strongly negative** (gross −20 to −32 bp, continuation
  0.35–0.46, n = 23–34). After a realised-vol *spike*, a breakout tends to **reverse**. Tiny n and
  inverted from what was tested, so it is a hypothesis, not a finding — but see §5.

## 5. What this settles, and what it opens

- **Closed:** turning the volatility forecast into P&L via linear breakout entries, on this data.
  The detector finds big-move bars; after the first ±k bp of the move, the rest is a coin flip.
  This is consistent with the project's other results: the forecaster carries magnitude, never sign.
- **Still open for the volatility asset:** convex payoffs (option selling with the detector as a
  veto, idea 1b) and quote withdrawal (idea 6). Neither needs direction, and neither is touched by
  this result.
- **New, cheap, well-powered follow-up:** *fade* breakouts after realised-vol spikes. Needs only
  OHLC, so it runs on the **7-year 5-min klines** (`data/btcusdt_5m_klines.pkl`) with thousands of
  events instead of 30. Pre-register it untuned, with the leg split, blind benchmark and one-position
  accounting — the 4 h mean-reversion result (`v1_4h_feasibility.analysis.md` §9) is the warning
  that reversion signals can be real and still lose to the left tail.

## Reproduce

```
cd runs/harness_breakout
/home/nkout/projects/binance2/binance2/.venv/bin/python test_breakout_probe.py   # 8/8 PASS
/home/nkout/projects/binance2/binance2/.venv/bin/python breakout_probe.py        # ~3 s
```
