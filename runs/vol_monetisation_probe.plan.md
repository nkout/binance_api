# Vol-detector monetisation — probe spec (NOT implemented)

*Authored 2026-10-05. Status: **spec for review — no code written.** Companion to
`horizon_economics_and_next_ideas.md` §4.7, `next_signal_ideas.md` (R7), `PROGRAMME_SUMMARY.md` §5.*

---

## 1. Why this probe exists

The programme closed with one durable, out-of-sample asset: the **volatility detector**
(eventful AUC 0.879; `P(either barrier touched)` 2.30 % → 43.7 %; |move| lift 4.63× @90 s →
2.27× @1 h with no retraining). Every *directional* expression of it is dead. The user's chosen
goal is to **monetise the detector itself**.

The only instrument whose payoff is directly convex in |move| is an **option**. So the candidate
monetisation is a volatility trade: the detector forecasts realized vol (RV), the options market
prices implied vol (IV), and the edge is `RV_forecast vs IV`. This is `next_signal_ideas.md` R7
("sell volatility, detector as veto"), sharpened.

**The spec exists to decide, cheaply and before any options infrastructure, whether that edge is
real.**

## 2. The unknown this probe resolves

> Does the detector carry information about future realized vol **beyond a naive baseline**, at a
> horizon where **liquid BTC options exist** — and is forward RV conditional on the detector
> **mispriced against IV**?

This single unknown decides between the two surviving monetisation shapes:

| outcome | meaning | next step |
|---|---|---|
| detector predicts RV at **daily+** horizons | there is a horizon-match with liquid options | Option 1 (trade the vol premium) |
| detector's skill is **intraday only** | no liquid instrument at that horizon | Option 2 (sell the fast signal) or hedge-timing overlay |

**Prior (honest).** The measured decay (0.97× by 6 h) suggests the detector's skill is
intraday-only, which points to Option 2. This probe is designed to falsify that prior if it is
wrong — not to confirm it.

## 3. Method

### 3.1 Detector score (no retraining — reuse saved scores)

| artifact | what it gives | role |
|---|---|---|
| `runs/v1_stage2_scores.npz` | `p_evt_6`, `p_evt_20` (+ `p_trig_*`, `touched_*`, `ret_*`) over **1,358,390 bars ≈ 331 d** | full-year detector (primary) |
| `runs/run009f_scores.npz` | `prob` (lup/ldn @ 6 horizons), 666,028 bars, 5 s, 40.8 d | high-resolution detector (cross-check) |
| `runs/gbm_probe_scores.npz` | `p_180_mag`, `p_720_mag` (15 min / 1 h magnitude heads) + `rawbp_180/720`, `close`, `dt_s` | long-horizon magnitude model + realized-move ground truth |

`event_score_t = p_evt_20` (v1) and `= mean_h(P(lup_h) + P(ldn_h))` (009f). **Verify horizon units
(bar count → seconds) inside the harness before use** — the field names are not self-documenting.

### 3.2 Forward realized vol

From `close` + `dt_s` (009f, 5 s) and `ts` (v1_stage2), compute forward RV over a horizon grid:

```
RV_h(t) = sqrt( Σ_{i=t+1}^{t+h} r_i² )      r_i = log(close_i / close_{i-1})
h ∈ {1 h, 6 h, 1 d, 3 d, 7 d}
```

Gaps handled with the §6-appendix contiguous-segment rule (`dt` step == window, same segment) —
forced flat across collector gaps; never book RV across a gap.

### 3.3 The decisive test — does the detector aggregate to a daily+ RV forecast?

The detector's native horizon is 90 s–1 h; options trade in days–weeks. The one way Option 1 works
is if a **daily aggregate** of the detector predicts **next-day / next-week RV**. So the primary
test aggregates the detector per UTC day (three aggregates: daily **max**, daily **mean**, daily
**trigger count**) and asks whether it adds out-of-sample predictive power over a naive baseline.

**Baseline:** HAR-RV (daily, weekly, monthly trailing RV) — the standard RV benchmark.
**Metric:** out-of-sample QLIKE and OOS R², walk-forward, day-grouped.
**Model:** OLS/log-linear HAR vs HAR + detector-aggregate, expanding window, no tuning.

### 3.4 Conditional VRP (only at the horizon where §3.3 finds skill)

At the best horizon `h*`:
1. Bucket days by detector level (deciles), compute forward RV per bucket.
2. Compare to the IV at a matching maturity — **DVOL** (Deribit 30-day IV, free, historical from
   2021-03) for the 1-week/1-month end.
3. Report the conditional VRP = (forward RV − IV) by detector bucket, day-clustered CI.

**IV-data constraint (the #1 risk).** DVOL is a free 30-day constant-maturity index and covers the
1-week/1-month case. **Per-expiry historical IV for short-dated options is not freely available**
from Deribit (chains are live-only); it would require reconstruction from expired-instrument trade
history or a paid vendor. The probe therefore tests `h* ∈ {1 d, 7 d, 30 d}` against DVOL first;
if `h*` is intraday, the VRP test is **not run** — that outcome already selects Option 2.

## 4. Pre-registration (write before running)

- **P1 (skill, primary):** a daily-aggregated detector improves next-1-day RV forecast OOS QLIKE
  over HAR by a **material** margin (threshold set at spec-review; suggest ≥ 2 % QLIKE) with a
  day-grouped 95 % CI lower bound > 0. Repeat for 7 d.
- **P2 (mispricing, conditional on P1):** detector-conditioned forward RV differs from DVOL-implied
  RV in the *predicted direction* — detector-high → RV > IV, detector-quiet → RV < IV — with a
  day-clustered CI excluding zero.
- **Kill (both directions decisive):**
  - **P1 fails** (detector adds nothing beyond HAR at 1 d/7 d) → the detector is intraday-only →
    **Option 1 is closed**; the probe reports the intraday skill and hands off to Option 2.
  - **P1 passes but P2 fails** (RV forecast beats HAR but the market's IV already reflects it) →
    no mispricing → Option 1 closed; hand off to Option 2.
  - **P2 passes** → Option 1 is live; write the trade design (short-vol-with-veto vs
    long-vol-timed), including options fees + delta-hedge cost.
- **Controls (mandatory, from `horizon_economics…` §8.5):** day-grouped CV; out-of-sample only;
  a naive trailing-RV baseline; sign/permutation or day-matched null on the VRP spread; report
  median vs mean.

## 5. Data needed (new, all free)

| item | source | status |
|---|---|---|
| Deribit DVOL hourly/daily OHLC | Deribit public API (`get_volatility_index_data`), no auth | to fetch |
| (conditional) short-dated per-expiry IV | Deribit expired-instrument trades, or vendor | **may be unavailable — flagged** |

Nothing else: detector scores, realized vol inputs and klines are on disk (`data/`).

## 6. Effort & deliverables

- **Effort:** ~1–2 days, CPU only, no GPU, no retraining.
- **Deliverables:**
  - `runs/harness_volmon/` — `fetch_dvol.py`, `run_volmon.py`, `test_volmon.py`
    (equivalence + leak test: scramble the future, assert RV/IV alignment is causal).
  - `runs/vol_monetisation_probe.analysis.md` + `runs/vol_monetisation_results.json`.
  - A one-paragraph decision: Option 1 (with trade-design handoff) or Option 2.

## 7. Risks

1. **Horizon mismatch** (detector minutes, options days) — this is the hypothesis, not a bug. The
   probe measures it; a null result is the expected, useful outcome.
2. **IV-data unavailability** for short-dated per-expiry — handled by restricting the VRP test to
   the DVOL-covered horizons (§3.4).
3. **Detector score provenance** — `p_evt_*` from the 1d stage-2 run is a weaker detector (AUC
   0.584) than 009f's 0.879; use 009f where the windows overlap as a cross-check, and state which
   detector each result uses.
4. **DVOL is 30-day** — a mismatch to a 1-day RV is a real limitation; report it explicitly rather
   than silently comparing.

## 8. What this does NOT do

- No retraining, no new collector, no options account, no trading.
- Does not reopen any directional branch.
- Does not test the options-fee / delta-hedge economics — that is the *next* spec, gated on P2.
