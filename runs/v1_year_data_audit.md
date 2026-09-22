# v1-era year of data — download and audit

*2026-09-22. Twelve monthly tars fetched from the collection server to
`data/v1_year/` (5.1 GB, 12/12 complete, 0 failures). Source collector:
`binance_live_orderbook.py` (v1, `window_sec = 15`, 784 columns). Audit harness:
`/tmp/.../audit_v1.py` — filename-based coverage plus per-month column sampling.*

**TLDR:** the year is real and near-continuous — **331 unique calendar days** out of a
336-day span, at 15 s, 24 h/day. The slow-signal columns this was fetched for are alive
for most of it, with one hard boundary: **funding rate and liquidations die on
2026-04-24** and never return. Open interest, long/short ratio and the full depth ladder
survive **all 331 days**. Funding is recoverable from Binance REST; liquidations are not.

This is the first dataset in the project with enough span to test a 4 h horizon, which
is the only horizon where the fee requirement (0.54 maker) and achievable accuracy
overlap — see `horizon_economics_and_next_ideas.md` §7.

---

## 1. Coverage

| month | files | days | expected | files/day | collector instances | gaps |
|---|---|---|---|---|---|---|
| 2509 | 98 | 17 | 30 | 5.8 | odtgk, wiccu | collection starts 09-14 |
| 2510 | 184 | **31** | 31 | 5.9 | odtgk | — |
| 2511 | 168 | 29 | 30 | 5.8 | odtgk → pjhsr | 11-27→11-29 |
| 2512 | 185 | **31** | 31 | 6.0 | pjhsr | — |
| 2601 | 170 | 30 | 31 | 5.7 | pjhsr → mrfni | 01-28→01-30 |
| 2602 | 165 | **28** | 28 | 5.9 | mrfni | — |
| 2603 | 150 | 28 | 31 | 5.4 | mrfni → nsgpe → zyzjh | 03-07→03-10, 03-15→03-17 |
| 2604 | 178 | **30** | 30 | 5.9 | nsgpe | — |
| 2605 | 184 | **31** | 31 | 5.9 | nsgpe | — |
| 2606 | 179 | **30** | 30 | 6.0 | mcxci, nsgpe, rzeac | — |
| 2607 | 182 | **31** | 31 | 5.9 | rzeac | — |
| 2608 | 82 | 15 | 31 | 5.5 | rzeac | ends 08-15 |

**1,925 files · 331 unique days · 2025-09-14 → 2026-08-15.** Each file is 4 h of 15 s
bars (960 rows, 784 columns).

Two things this rules out:

- **No dedupe needed.** The six collector prefixes are *sequential restarts*, not
  concurrent writers — files/day stays at 5.4–6.0 rather than doubling.
- **No sampling gaps.** 6 files/day × 4 h = 24 h/day. This is continuous data, not a
  subset.

## 2. Column availability — one hard boundary

| column | status | usable window |
|---|---|---|
| `opt_open_interest_sample` | **alive throughout** | all 331 days |
| `opt_long_short_ratio_sample` | **alive throughout** | all 331 days |
| depth ladder (20 levels × bid/ask × spot & futures) | alive throughout | all 331 days |
| trade qty / vwap / samples, spread, bid/ask OHLC | alive throughout | all 331 days |
| `opt_funding_rate_sample` | **dies 2026-04-24** | 2025-09-14 → 2026-04-23 |
| `opt_mark_price_sample`, `opt_index_price_sample`, `opt_spread_sample` | die 2026-04-24 | same |
| `opt_long/short_force_exit_qty_sum` (liquidations) | die 2026-04-24 | same |
| `opt_est_funding_rate_sample` | alive, but **misnamed** — holds `estimatedSettlePrice`, a price not a rate (see `economics_and_metrics.md` §5) | — |

**The boundary is sharp.** File-level scan of the transition:

| day | files | funding populated | liquidations non-zero |
|---|---|---|---|
| 2026-03-01 … 04-22 | 6/day | 100 % | 100 % |
| **2026-04-23** | 6 | 83 % | 83 % |
| **2026-04-24** onward | 6/day | **0 %** | **0 %** |

Cause is almost certainly the documented Binance change behind
`ALL_RUNS_ANALYSIS.md`'s note *"markPrice websocket is dead → v4 REST-polls
premiumIndex"*. The v4/v5 collectors got the REST fallback; **v1 never did**, so from
2026-04-24 it wrote the `-1` sentinel forever. This also explains why the v5-era 60-day
dataset has all five `liq_*` features in `DROP_DEAD` as constant zero.

*(The earlier `runs/out.nsgpe.*.csv.gz` samples that showed everything as `-1` are from
May/June 2026 — after the boundary. They were not representative of the year.)*

## 3. What this gives you

| feature set | window | days | 4 h observations |
|---|---|---|---|
| **Everything** incl. funding + liquidations | 2025-09-14 → 2026-04-23 | **216** | ~1,300 |
| **OI + L/S + book ladder + volumes** | full year | **331** | ~1,990 |
| Full year **with funding back-filled from REST** | full year | **331** | ~1,990 |

**Recoverable:** funding rate (`/fapi/v1/fundingRate`), mark and index price
(`markPriceKlines` / `premiumIndexKlines`) are all free from Binance REST with years of
history, and join on timestamp. Doing that restores everything except one thing.

**Permanently lost:** **liquidations** after 2026-04-23. Binance does not serve
liquidation history, so the 216-day window is the only place this project will ever have
it. If liquidation flow turns out to matter, that window is the whole sample.

**Unobtainable elsewhere, for the full 331 days:** open interest and long/short ratio at
15 s (the API caps both at ~30 days of history), and the full depth ladder. These are the
differentiated part of the asset — nobody can re-download them.

## 4. Statistical power by horizon

Using `E|move|` from `horizon_economics_and_next_ideas.md` §1 (√t-scaled from the
measured 4.03 bp @ 90 s) and 331 days of non-overlapping windows:

| horizon | E\|move\| | req. accuracy (taker / maker) | obs | detectability of the maker requirement |
|---|---|---|---|---|
| 1 h | 25.5 bp | 0.696 / 0.578 | 7,944 | easy to detect — but 0.58 is a high bar |
| **4 h** | **51 bp** | **0.598 / 0.539** | **1,986** | **0.539 sits at 3.6σ — workable** |
| 1 day | 125 bp | 0.540 / 0.516 | 331 | 0.516 at 0.6σ — **underpowered** |

Restricted to the 216-day full-feature window, the 4 h cell drops to ~1,300 observations
and 2.9σ — still usable, and the fallback if liquidations prove essential.

**4 h remains the only horizon where the requirement and the power both work.**

## 5. Overlap with the v5 era

The v1 year runs to 2026-08-15; the v5 60-day set starts 2026-07-13. That is **~33 days
of overlap** where both the v1 book ladder + OI + L/S *and* the full v5 76-feature set
exist for the same bars. Useful for checking that a v1-feature model and a v5-feature
model agree on the same window before trusting the longer v1 history.

## 6. Recommended next step

A **feasibility check, not a full run** (per §4.1's lesson — measure before building):

1. Aggregate the v1 15 s bars to **5 min** (a year → ~95 k rows, trivially in memory; a
   4 h horizon loses nothing by coarsening).
2. Build ~10–15 slow features: OI level/Δ/z, L/S ratio and Δ, basis (futures − spot) and
   z, depth-ladder shape (near/far imbalance, slope, total depth), realised-vol regime,
   volume imbalance. Join funding from REST.
3. Walk-forward, purged folds, **purge ≥ 4 h** — this is exactly the leak the probe's
   cell-7 patch closed.
4. Measure the **daily-IC t-stat** of each feature and of a small GBM against the 4 h
   vol-normalised forward return.
5. Gate with the controls this session established: accuracy ≥
   `max(fee requirement, P(up) on the same trigger set)`, blind benchmark, day-clustered
   CI. **The drift confound is larger at 4 h than at 1 h** — `gbm_probe.analysis.md` §3.

If the daily-IC t-stat is below ~3 on 331 days, the slow-horizon line is closed too and
the answer is `horizon_economics_and_next_ideas.md` §8.

## 7. Reproducing the audit

```
python audit_v1.py        # pass 1: filename coverage; pass 2: column population
```

Pass 1 reads no file contents (filenames carry instance, sequence and date), so it is
instant. Pass 2 samples 3 files per month. The file-level boundary scan in §2 reads only
4 columns per file via `usecols`, so a 3-month scan takes ~1 min on a laptop.
