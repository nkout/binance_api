# New features: what else Binance publishes and how to collect it

*Written 2026-10-17. Companion to `runs/PROGRAMME_SUMMARY.md` and `runs/no_profit_root_cause.md`. Nothing here has been tested. Each item is a candidate input, not a signal. Items marked **Verify** were taken from Binance docs or third-party tooling and have not yet been checked against a real download.*

## Where the programme stands

**Outcome: no profitable strategy so far.** Over about 30 pre-registered tests (`runs/PROGRAMME_SUMMARY.md`), no strategy beat cash after costs at a usable scale, with evidence that held up outside the early sample.

**Scope.** This holds for this setup: retail latency (Greece, \~50–150 ms to Binance), the reachable VIP0 + BNB fee tier (9 bp taker round trip) and public data. It does not show that these markets cannot be traded by anyone. The edges found are real, but they belong to faster or better-capitalised participants, or they are premia that have been competed down:

- **Microstructure direction:** priced within seconds.
- **Big-move tail:** +11–14 bp at zero delay, ≈ 0 after a 5 s delay.
- **Cross-sectional and short books:** win in the median, lose in the mean to pumps.
- **Carry and variance premium:** compressed toward the risk-free rate.

**Still open: residuals, not strategies.**

| item | status | doc |
| --- | --- | --- |
| Negative-funding carry (CARRY−) | ≈ +18.9 %/yr at today's VIP0 borrow rates on borrowable coins. Borrow limits cap it at $1.5k–5.6k per coin, and 2024 was −0.51 %. | `r9_borrow.analysis.md` |
| Volatility-premium rule (V1) | Forward ledger flat so far: as of 2026-10-14, 52 forward days, 22 complete, rule inactive on all. A verdict needs ≥ 180 completed forward days, around early 2027. | `vrp_dvol.analysis.md`, `harness_vrp/forward_ledger.csv` |
| BTC trend-following (R6) | A risk reducer: max drawdown −33 % vs −79 %, Sharpe 0.92 vs 0.69, but +31.8 %/yr vs +39.6 %/yr for holding BTC. | `r6_trend.analysis.md` |

**This document's proposals (§6–§8)** are the remaining untested ideas, and the realistic prior on each is low. The cheapest ways to close questions are the model-class check (§8.1) and the trend-gate diagnostic (§7.2). Both use only data already on disk.

## Why these sources, and at which horizon

More order-book detail is not the gap. W1 already fed every raw column (664 inputs) to xgboost and an MLP and found AUC 0.527, worth at most +2 bp after a 5 s delay. Features can only raise accuracy, and even a perfect sign predictor earns at most `E|move| − fee`, which is ≈ 1 bp at 5 s and ≈ 0–3 bp on the 90 s triggers.

The required accuracy is `(1 + F / E|move|) / 2`. At F = 9 bp:

| horizon | E\|move\| (BTC, approx.) | required accuracy |
| --- | --- | --- |
| 90 s (trigger bars) | 10–13 bp | 0.85–0.95 |
| 4 h | 55–60 bp (recent 40-day window) · 90–100 bp (long-run vol) | ≈ 0.58 · ≈ 0.55 |
| 1 d | ≈ 140 bp (recent, √-scaled) · 200+ bp (long-run vol) | ≈ 0.53 · ≈ 0.52 |

The 4 h and 1 d requirements depend on the volatility regime. The recent figures come from `horizon_economics_and_next_ideas.md` §7.2, where 3 h and 6 h moves were 54.9 and 61.3 bp at a detector lift of about 1 (close to unconditional). The recent 1 d figure scales the 4 h move by √6. The long-run figures assume typical multi-year BTC volatility. Recompute E|move| on each test window before applying the pre-check.

So the sources below are meant as inputs for **4 h to daily** directional work, and for regime and risk control. Orderbook data had IC ≈ 0 already at 15 min to 1 h, so the useful new information has to come from positioning, term structure and options.

**Rule of thumb:** prefer sources with downloadable history, because they can be tested now. Sample length was the method lesson that changed the most conclusions (`PROGRAMME_SUMMARY.md` §6.1). Forward-only sources need months of collection before a first test.

---

## 0. Fix first: the liquidation stream has been dead since 2026-04-23

*Status 2026-10-17: applied to the three lines below and checked live. The legacy `/ws/` path delivered nothing. The routed path delivered `markPrice` every 3 s (its default rate) and 54 of the 56 `forceOrder` events that `!forceOrder@arr` showed on the same 40 perps in 3 minutes. The pre-registrations that follow this review are Round 9 in `next_signal_ideas.md`. The readers now treat the liquidation columns as missing (NaN) from 2026-04-23 17:03:30 UTC (`liq_cutoff.py`).*

*Update 2026-10-20: `binance_live_orderbook_v5.py` is back to its original code (dead route), as are v2–v4, and the fix lives in the new `binance_live_orderbook_v6.py`. v6 reads `forceOrder` together with a `markPrice@1s` heartbeat on one routed `/market/stream` connection. It writes one trailing column, `opt_force_order_live`: 1 when the feed was verified live for the whole bar. Readers take v6 liquidation values only where that column is 1, so no restart date is stored. In a live test, a copy pointed at the legacy path wrote 0 in every bar.*

Binance split the USDⓈ-M futures WebSocket into routed base URLs, and the legacy URLs were retired on 2026-04-23:

- `/public` serves `@depth` and `@bookTicker`.
- `/market` serves `@aggTrade`, `@markPrice`, `@kline_*`, tickers and `@forceOrder`.
- `/private` serves user data.

Connections without a routed path now receive only `/public` streams. That is why `@depth` (and `@trade`, per the v1 audit) kept working while `@markPrice` and `@forceOrder` went silent. This is the same change behind the "markPrice websocket is dead" note in `ALL_RUNS_ANALYSIS.md`, and the reason the v5 60-day set has all `liq_*` columns constant zero.

| file | line | current | change to |
| --- | --- | --- | --- |
| `binance_live_orderbook_v5.py` | 544 | `wss://fstream.binance.com/ws/{sym}@forceOrder` | `wss://fstream.binance.com/market/ws/{sym}@forceOrder` |
| `binance_live_orderbook.py` (v1) | 292 | same pattern | same fix |
| `binance_live_orderbook.py` (v1) | 318 | `wss://fstream.binance.com/ws/{sym}@markPrice` | `wss://fstream.binance.com/market/ws/{sym}@markPrice` |

Notes:

- **Fix this first.** Liquidations cannot be backfilled. The `liquidationSnapshot` archive on data.binance.vision is no longer published, so your own collector is the only source.
- **The stream undercounts.** It pushes only the *largest* liquidation per symbol per 1000 ms. Use it as an event flag or a count of seconds with liquidations, not as liquidated volume.
- **Cross-market pressure.** `!forceOrder@arr` on the same `/market` path streams all symbols over one connection, which gives a market-wide liquidation count.
- **Verify** after the fix: watch for non-empty `forceOrder` messages during the next volatile hour.

---

## 1. Downloadable history (testable now)

All of these live on the free static archive `https://data.binance.vision`. The repo already has working helpers: `runs/harness_xsec/fetch_archive.py` (`get`, `list_prefix`, `read_zip_csv`) and `runs/harness_mm_alt/fetch_mm.py` (`object_url`).

Path patterns:

```
daily:    data/futures/{um|cm}/daily/{dataset}/{SYM}/{SYM}-{dataset}-{YYYY-MM-DD}.zip
monthly:  data/futures/{um|cm}/monthly/{dataset}/{SYM}/{interval}/{SYM}-{interval}-{YYYY-MM}.zip   (kline-type datasets)
options:  data/option/daily/{dataset}/{SYM}/...
```

Each zip has a `.CHECKSUM` sibling (SHA-256). Use `list_prefix()` to find what exists rather than guessing dates.

Minimal standalone reader:

```python
import io, zipfile, urllib.request, pandas as pd

def day_file(market, dataset, sym, day):            # market: "um" | "cm"
    url = (f"https://data.binance.vision/data/futures/{market}/daily/"
           f"{dataset}/{sym}/{sym}-{dataset}-{day}.zip")
    with urllib.request.urlopen(url, timeout=60) as r:
        z = zipfile.ZipFile(io.BytesIO(r.read()))
    return pd.read_csv(z.open(z.namelist()[0]))

m = day_file("um", "metrics", "BTCUSDT", "2026-10-01")
print(m.shape, m.columns.tolist(), m["create_time"].head(3).tolist())
```

### 1.1 `metrics`: open interest and positioning (top priority)

- **Dataset:** `futures/um/daily/metrics/{SYM}` (also exists for `cm`).
- **Columns:** `create_time, symbol, sum_open_interest, sum_open_interest_value, count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio, count_long_short_ratio, sum_taker_long_short_vol_ratio`.
- **Why it matters:** `v1_year_data_audit.md` says OI and L/S history are "not re-downloadable" because the REST API keeps about 30 days. This archive likely corrects that, for every perp and across years, which turns OI and positioning from a forward-only input into a testable one.
- **Verify:**
  1. Time resolution (expected 5 min).
  2. First available date for BTCUSDT and for a mid-cap.
  3. Gaps, using a coverage audit like `v1_year_data_audit.md`.
  4. The column-to-REST mapping, by comparing the last 30 days against the endpoints in §2.1. The likely mapping is: `sum_toptrader_*` ↔ `topLongShortPositionRatio`, `count_toptrader_*` ↔ `topLongShortAccountRatio`, `count_long_short_ratio` ↔ `globalLongShortAccountRatio`, and `sum_taker_*` ↔ `takerlongshortRatio`.
- **Feature ideas (4 h to daily):**
  - ΔOI over 4 h / 24 h as a z-score.
  - The price-change × OI-change quadrant (new longs, short covering, new shorts, long liquidation).
  - Top-trader vs global L/S divergence.
  - Cross-sectional ranks of these across the 860-perp universe in `data/xsec/`.

### 1.2 Premium-index, mark-price and index-price klines

- **Datasets:** `premiumIndexKlines`, `markPriceKlines`, `indexPriceKlines` for `um` and `cm`, monthly and daily, with intervals down to 1 m.
- **REST equivalents (recent data and live):** `GET /fapi/v1/premiumIndexKlines`, `/fapi/v1/markPriceKlines`, `/fapi/v1/indexPriceKlines`, paged by `startTime`.
- **Why it matters:** these give the path of funding pressure *inside* each funding window. Settled funding only shows the end point, and the v4/v5 REST polling of `premiumIndex` has no history before the collector started.
- **Feature ideas:**
  - Premium drift over the last N hours before settlement.
  - Premium vs settled rate.
  - Perp-vs-index basis z-score at 1 h.
  - `indexPriceKlines` also provides the spot reference `S` for §1.3.

### 1.3 Quarterly futures: calendar basis and term structure

- **Archive:** monthly klines for delivery contracts, named `BTCUSDT_YYMMDD` (`um`) and `BTCUSD_YYMMDD` (`cm`). List them with `list_prefix("data/futures/um/monthly/klines/")` and keep names containing `_`.
- **REST:**
  - `GET /fapi/v1/continuousKlines` with `pair`, `contractType` = `CURRENT_QUARTER` | `NEXT_QUARTER` | `PERPETUAL`, and `interval` gives a continuous series.
  - `GET /futures/data/basis` with `pair`, `contractType`, `period` from 5m to 1d, and `limit` ≤ 500. It serves **the last 30 days only**, so use it for live data and the archive for history.
- **Features:**
  - Annualized basis: `(F / S − 1) × 365 / days_to_expiry`.
  - Front-to-next slope.
  - Quarterly basis minus annualized perp funding, a carry dislocation.
- **Roll rule:** switch contracts a fixed number of days before expiry, and never stitch prices across a roll.

### 1.4 COIN-M (inverse) perps

- **Archive:** `data/futures/cm/...` for `BTCUSD_PERP`: klines, `metrics`, premium-index klines and funding. **Verify** the funding path under `cm`.
- **REST:** base `https://dapi.binance.com`: `/dapi/v1/klines`, `/dapi/v1/fundingRate`, and `/futures/data/openInterestHist` with `pair` + `contractType`.
- **Features:** USDT-M minus COIN-M funding spread, and COIN-M share of total BTC OI. COIN-M is BTC-collateralized and has a different trader base, so the two books can disagree.
- **Caution:** the Binance derivatives change log mentions COIN-M being integrated into the USDⓈ-M architecture. Check the current endpoint paths before building.

### 1.5 `bookDepth` (lower priority)

- **Dataset:** `futures/{um|cm}/daily/bookDepth`, with columns `timestamp, percentage, depth, notional`. These are cumulative depth and notional in ± % bands around mid. **Verify** the resolution.
- **Why it matters:** it is the only way to backfill book *shape* over years. Use it only at 4 h and longer; at seconds-to-minutes it is the input class the programme has already exhausted.

### 1.6 Options: `BVOLIndex`

- **Dataset:** `data/option/daily/BVOLIndex/{SYM}`, with columns `calc_time, symbol, base_asset, quote_asset, index_value`. List the prefix to get symbol names.
- **What it is:** Binance's own volatility index, mainly a cross-check on the Deribit DVOL used in V1.
- **What's gone:** the option `EOHSummary` archive stopped on 2023-10-23, so there is no history of the Binance option surface. See §2.2.

---

## 2. Live-only (collect forward)

### 2.1 Top-trader and global ratios via REST

- **Endpoints** (`https://fapi.binance.com`): `/futures/data/topLongShortPositionRatio`, `/futures/data/topLongShortAccountRatio`, `/futures/data/globalLongShortAccountRatio`, `/futures/data/openInterestHist`, `/futures/data/takerlongshortRatio`.
- **Parameters:** `symbol`, `period` (5m … 1d), `limit` ≤ 500, `startTime`, `endTime`. Only the **latest 30 days** are served.
- **Current state:** the collector polls only `globalLongShortAccountRatio` with `limit=1`.
- **When this is needed:** only if the `metrics` archive (§1.1) turns out to be coarse, delayed or gappy. Otherwise use it for the cross-check and for live values.
- **Collection:** a daily backfill job is simpler and more robust than polling every 5 min. Any gap longer than 30 days is permanent.

```python
import time, requests

def backfill(path, symbol, period="5m", days=30, step_ms=300_000):
    end = int(time.time() * 1000)
    t, rows = end - days * 86_400_000, []
    while t < end:
        t2 = min(t + 500 * step_ms, end)
        r = requests.get(f"https://fapi.binance.com/futures/data/{path}",
                         params=dict(symbol=symbol, period=period, limit=500,
                                     startTime=t, endTime=t2), timeout=30)
        r.raise_for_status()
        rows += r.json()
        t = t2 + 1
        time.sleep(0.5)
    return rows   # write as JSON lines with the fetch time; dedupe on timestamp
```

### 2.2 Binance Options surface

- **REST** (base `https://eapi.binance.com`):
  - `/eapi/v1/exchangeInfo` lists live symbols and expiries.
  - `/eapi/v1/mark` returns, per option: `markPrice, bidIV, askIV, markIV, delta, gamma, vega, theta`. Weight 5. Omit `symbol` to get all options.
  - `/eapi/v1/openInterest` takes `underlyingAsset=BTC` and `expiration=YYMMDD`.
  - `/eapi/v1/index` and `/eapi/v1/ticker`.
- **WebSocket:** `wss://nbstream.binance.com/eoptions/ws/<stream>`, for example `BTC@openInterest@<YYMMDD>` (60 s updates).
- **Cadence:** a snapshot of `/eapi/v1/mark` every 1–5 min, and OI per expiry every 5 min.
- **Features:**
  - ATM IV per expiry (term structure).
  - 25-delta risk reversal (skew).
  - Put/call OI ratio.
  - OI concentration by strike.
  - IV minus the HAR forecast from `harness_vrp/`.
- **Cautions:** Binance options are thinner than Deribit, so compare the two before trusting skew. There is no history (§1.6), so the first test is months away.

### 2.3 Margin borrow-rate history (needs an API key)

- **Endpoint:** `GET https://api.binance.com/sapi/v1/margin/interestRateHistory`. It is `USER_DATA`, so the request must be signed.
- **Parameters:** `asset`, `vipLevel=0`, `startTime`, `endTime` (at most one month apart; the default is the last 7 days), `timestamp`, `signature`. Weight 1.
- **Key:** create a read-only API key with trading and withdrawals disabled. Keep it in environment variables or a gitignored `.env`, and never commit it.
- **Backfill:** page backwards one month at a time until the response is empty. That also answers how far back the history goes.
- **Why it matters:** this is the missing input in R9 (`r9_borrow.analysis.md` §5): CARRY− net of *historical* borrow rather than today's rates.

```python
import hmac, hashlib, os, time, urllib.parse, requests

KEY, SECRET = os.environ["BINANCE_KEY"], os.environ["BINANCE_SECRET"]

def signed_get(path, **params):
    params["timestamp"] = int(time.time() * 1000)
    q = urllib.parse.urlencode(params)
    sig = hmac.new(SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()
    r = requests.get(f"https://api.binance.com{path}?{q}&signature={sig}",
                     headers={"X-MBX-APIKEY": KEY}, timeout=30)
    r.raise_for_status()
    return r.json()

# one month of BTC borrow rates at VIP0
# signed_get("/sapi/v1/margin/interestRateHistory", asset="BTC", vipLevel=0,
#            startTime=t0, endTime=t0 + 30 * 86_400_000)
```

- **Also:** schedule the existing public-rate `harness_xsec/snapshot_borrow.py` on the collection server, which is always on, instead of the laptop.

---

## 3. Storage and point-in-time rules

1. **Raw first.** Keep original zips and raw JSON lines with a local fetch timestamp. Derive features in separate read-only scripts.
2. **Know what each timestamp means.** For every dataset, establish whether `create_time` / `open_time` marks the start or the end of the period. Use kline `close_time`. Treat a `metrics` row as known only at `create_time` plus one period. A wrong convention here is a look-ahead leak.
3. **Cross-check.** Compare archive against REST over the overlapping 30 days before trusting any archive column.
4. **Audit coverage per dataset**, in the format of `v1_year_data_audit.md`: days present, gaps, dead columns.
5. **Delisted symbols:** the archive keeps printing flat zero-volume bars after a delisting (Round 6 data finding). Apply the same handling here.

## 4. How to test (same protocol as Rounds 4–8)

- Pre-register in `runs/next_signal_ideas.md` before writing code.
- Run the economic pre-check `required accuracy = max((1 + F / E|move|) / 2, P(up))` with F = 9 bp.
- Use horizons from 4 h to daily, with one position at a time and non-overlapping P&L.
- Report mean, median and the worst 1 %, with a bounded-loss variant for any short book.
- Include a drift / blind benchmark, a sign-permutation or random-portfolio null, and Holm correction across cells.
- Split every pass by period. A narrow pass is a prompt for that check, not a result.
- **Prior: small effects.** These series are widely watched and sold by data vendors. OI, funding and L/S added nothing to short-horizon continuation (3 of 90 cells, chance level).

## 5. Priority order

0. **Fix the `@forceOrder` URL (§0) now.** This is a bug fix, not a research step. It takes minutes, and every day without it loses liquidation data that cannot be backfilled.
1. **Model-class check (§8.1).** It uses only data already on disk, and its result decides whether every later test uses linear or flexible models, so it goes first.
2. **Audit the `metrics` archive (§1.1)** for resolution, start date and gaps, plus the REST cross-check. Then run the 4 h test proposed in §6, with the model class chosen in step 1.
3. **Trend gate on the 4 h rule (§7).** It needs no new data (the 7-year klines are on disk), so it can run in parallel with step 2.
4. **Download premium-index klines and quarterly klines (§1.2, §1.3).** That gives years of funding pressure and calendar basis.
5. **Start the forward collectors:** the top-trader backfill job (if needed), options snapshots, and the borrow-history pull. Start these early: they need months before a first test.
6. **Then COIN-M, `bookDepth` and `BVOLIndex`.**
7. **Cross-sectional DNN (§8.2)**, only if step 1 shows flexible models beat linear.

## 6. Proposal: `metrics` columns in the 4 h harness

*Proposal only. Pre-register the final version in `runs/next_signal_ideas.md` before any code.*

### 6.1 What is already known

The v1 year already carried global L/S, spot–perp basis, OI change and liquidation imbalance, and `v1_4h_feasibility.analysis.md` tested them at 4 h:

- **Out-of-sample IC:** `lsr_z` +0.20 and `basis_bp` +0.12. `oi_chg` and `liq_imb` were significant but secondary.
- **Combined with the price-shape features** (k = 3 signed-z combo): daily-IC t +20, but a pooled out-of-sample IC of only +0.04 and gross ≈ 0 bp per signal.
- **Direction accuracy:** 0.497 on the top/bottom 50 % and 0.516 on the top/bottom 25 %. With E|move| = 64.7 bp, the requirement at 9 bp is about 0.57.
- **Why the daily IC misled:** it ranks heavily overlapping 4 h windows within each day, which measures intraday timing, not whether the next 4 h goes up or down. Judge on pooled IC over non-overlapping trades.

### 6.2 Target

Under a normal approximation, accuracy on the top 10 % of |score| is

```
acc ≈ Φ( ρ · 2.06 / √(1 − ρ²) )      2.06 = E[|Z| given |Z| > 1.645]
```

where ρ is the pooled IC on non-overlapping trades.

| pooled IC ρ | implied top-10 % accuracy |
| --- | --- |
| 0.04 (existing combo) | ≈ 0.53 |
| 0.085 | ≈ 0.57 |
| 0.10 | ≈ 0.58 |

**The new features must roughly double pooled IC out of sample**, from 0.04 to about 0.09–0.10. Accuracy is necessary but not sufficient: the 4 h mean-reversion rule also lost to a few large losses, so the payoff shape is tested separately (§6.4).

### 6.3 Design

- **Data:** the `metrics` archive for BTCUSDT over its full history (after the §1.1 audit), joined point-in-time to 4 h bars resampled from the 7-year klines (`data/btcusdt_5m_klines.pkl`). Each `metrics` row counts as known at `create_time` plus one period.
- **Arms:**
  - *Baseline:* the k = 3 price-shape combo from the 4 h study.
  - *Baseline + metrics:* adds a fixed list of `metrics` features: ΔOI over 4 h and 24 h (z-score), the price-change × OI-change quadrant, top-trader vs global L/S divergence, and the taker volume ratio (z-score).
  - The increment between the two arms is the result, not the absolute level.
- **Model:** the same signed-z sum as the 4 h study, with each sign fixed on the train region. The MSE GBM early-stopped at iteration 0 there, so it is not the right tool.
- **Split:** train on the first half of the available history and test on the rest. Report the test half by calendar year.
- **Trades:** one position at a time, non-overlapping 4 h holds, entering on the top 10 % of |score| with thresholds fixed on train. 9 bp round trip.

### 6.4 Metrics and controls

- **Primary:** pooled IC on non-overlapping trades, and accuracy on the top 10 %.
- **Economics:** mean net P&L at 9 bp, median, worst 1 %, and a bounded-loss (stop) variant.
- **Drift benchmark:** `required = max((1 + F / E|move|) / 2, P(up) on the same trigger set)`.
- **Nulls:** sign-permutation null, plus a day-block bootstrap for every CI.

### 6.5 Pass criteria (proposed)

All of the following, out of sample:

1. Pooled IC of baseline + metrics ≥ 0.09, and at least 0.03 above the baseline arm, with the bootstrap CI of the difference excluding 0.
2. Top-10 % accuracy ≥ the drift-adjusted requirement.
3. Mean net P&L > 0 with the CI lower bound > 0, positive in a majority of calendar years, and no single year contributing more than half of the total.
4. The worst 1 % and the bounded-loss variant reported alongside, whatever the verdict.

### 6.6 Power

There are about 6 non-overlapping 4 h bars a day, so the top 10 % gives roughly 220 trades a year. The 95 % CI on accuracy is about ±3.8 points at 3 years (n ≈ 660) and ±3.0 points at 5 years (n ≈ 1,100). If the `metrics` archive starts late, the test may be underpowered to separate 0.53 from 0.57. Check the start date before committing.

### 6.7 Expectation

Small. The same feature family has already failed to move accuracy at 4 h, and these series are widely watched. A FAIL closes positioning-at-4 h for BTC. A PASS goes to a forward test, not to trading.

## 7. Proposal: slow trend gate on the 4 h mean-reversion rule

*Proposal only. Pre-register before code. Needs no new data: the 7-year BTC klines are already on disk.*

### 7.1 Why not moving averages as a feature

- **Not new information.** The distance from price to a moving average is a weighted sum of past returns. `ret_norm_1h/4h/24h` and `range_pos_24h` already encode it.
- **Already tested at three horizons:**
  - 90 s: the EMA-crossover overlay (run 005) failed because it churned fees.
  - 4 h: the price-shape features were the strongest, but with negative IC (mean reversion, not trend).
  - Daily: the 20/60/120-day trend rule (R6) cut drawdown from −79 % to −33 %, but netted +31.8 %/yr against +39.6 %/yr for holding BTC.

### 7.2 The failure it targets

The 7-year validation of the untuned 4 h rule (`v1_4h_feasibility.analysis.md` §9): 1,625 trades, win rate 54.9 %, median +11.47 bp, **mean −1.39 bp**. The long leg made +1.79 bp and the short leg −4.80 bp. The three worst walk-forward blocks were 2021-10 → 2022-06 (−10.32) and 2023-01 → 2023-08 (−12.55), both drawdowns, and 2023-08 → 2024-04 (−10.52), a strong rally.

**Hypothesis:** the losses concentrate in trades against the slow trend, meaning buying dips in a downtrend and selling rallies in an uptrend. A gate that only allows with-trend reversion trades should remove the left tail.

**Diagnostic first (read-only, minutes).** Split the existing 1,625 trades by the trend sign at entry and report mean, median and worst 1 % for with-trend vs counter-trend trades. If the counter-trend trades do not carry the losses, stop here.

### 7.3 Design

- **Base rule, unchanged:** the untuned walk-forward 4 h rule from §9. Threshold = q95 of |score| on the preceding window, top-3 feature signs from the preceding region, entry on first crossing, 4 h hold, one position at a time.
- **Gate, unchanged from R6:** the trend signal in `harness_xsec/trend.py`, the mean of `sign(P[d] / P[d − L] − 1)` over L = 20, 60, 120 days, from the previous 00:00 UTC daily close. Take long signals only when the signal is > 0 and short signals only when it is \< 0. **Do not tune the lookbacks.** In the 4 h study, tuning cost 7 bp per trade.
- **Arms:**
  - **A, ungated:** must reproduce −1.39 bp.
  - **B, gated:** the test arm.
  - **C, trend drift:** the mean 4 h return in the trend direction over all bars in the same trend state. This is what the gate alone earns without any reversion timing.
- **Fees:** maker 4 bp round trip (realistic at a 4 h hold) and taker 9 bp.

### 7.4 Pass criteria (proposed)

All of the following:

1. **Gated mean gross > +4 bp**, with the day-clustered CI lower bound > 0.
2. **Beats trend drift:** B exceeds C by at least 4 bp per trade, so reversion timing adds something beyond simply following the trend.
3. **Both legs positive**, and positive in at least 5 of the calendar years.
4. **Left tail improves:** the worst 1 % of B is smaller in magnitude than A's.
5. **Sign-permutation null:** above the 97.5th percentile.

### 7.5 Power and caveats

- **Low power.** The ungated rule's 95 % CI was \[−7.72, +4.89\] at n = 1,625. If the gate keeps about half the trades, the CI half-width grows to roughly ±9 bp, so a true +4 bp edge would usually not reach significance.
- **Few episodes.** The sample holds only a handful of trend episodes, and the gate's effect will be decided by them.
- **Not out of sample.** These years were already used to diagnose the failure, so any pass is partly in-sample by construction. A PASS goes to a forward test on data after 2026-09, not to trading.

### 7.6 Expectation

Moderate to low. The gate targets the mechanism the repo identified. But R6 found that most of the trend signal's value came from being flat in 2022, and shorting downtrends earned only +0.4 %/yr, so the gated short leg may not improve.

## 8. Proposal: can a flexible model combine features into direction?

*Proposal only. Pre-register before code. This is step 1 of the priority order: it needs only data already on disk, and its answer decides whether §6, §7 and later tests use linear or flexible models.*

### 8.1 Model-class check: linear vs GBM vs small MLP at 4 h

**Question.** Direction information can sit in interactions between features even when no single feature shows it. Do flexible models find any at 4 h, at the sample sizes available?

**What is already known:**

- **Trees at 4 h:** xgboost captures interactions by construction. The MSE GBM stopped at iteration 0 in 7 of 8 folds, a sign-classifier GBM reached an out-of-sample IC of +0.0002, and the linear signed-z sum did better (`v1_4h_feasibility.analysis.md` §2).
- **MLP vs xgboost on the same 60 features (90 s):** AUC 0.5958 vs 0.5956 (R1 C1).
- **W1 (5 s, 664 raw inputs):** MLP AUC 0.527 where xgboost got 0.500. This is the one case where a net found something the trees missed, and it was worth at most +2 bp.
- **Run 002:** a 7.4M-parameter attention LSTM did worse than a dummy.
- **Gap:** no MLP has been tested at 4 h.

**Data arms:**

- **(a) v1 year:** the 29 features of the 4 h study, including `lsr_z`, `basis_bp`, `oi_chg` and `liq_imb`. About 1,990 non-overlapping 4 h samples.
- **(b) 7-year klines:** price-shape features only (`ret_norm_1h/4h/24h`, `range_pos_24h` and the other price-derived columns in `harness_v1_feas/`). About 15,000 non-overlapping samples. This is the informative arm.
- **(c) Later:** arm (b) plus the `metrics` features, once §1.1 is audited.

**Models** (same features and folds for all):

- **Linear:** the signed-z sum from the 4 h study, and L2-regularised logistic regression.
- **GBM:** depth 2, log-loss objective (not MSE), small learning rate, early stopping on a time-ordered validation slice.
- **MLP:** 1–2 hidden layers of 16–32 units, dropout and weight decay, early stopping, averaged over 5 seeds.

**Label and folds.** The label is the sign of the vol-normalised 4 h forward return. Use walk-forward folds: the 4-fold split of the 4 h study for arm (a), and yearly expanding windows for arm (b). Leave a purge gap of at least 28 h between train and test, because features look back 24 h and the label looks forward 4 h.

**Controls, run before the real data:**

- **Planted-interaction world.** Keep the real feature matrix and replace the target with `sign(x₁·x₂ + noise)`, where x₁ and x₂ have zero marginal IC, at a strength that gives a pooled IC of about 0.10. GBM and MLP must recover a pooled IC of at least 0.07 while linear stays near 0. If they cannot, a null on real data is uninformative at this sample size.
- **Null world.** Shuffle the target in day blocks. All models must sit at IC ≈ 0.

**Metrics.** Pooled IC on non-overlapping trades (primary), top-10 % accuracy, log-loss, and the paired difference flexible − linear with a day-block bootstrap CI.

**Decision:**

- **If GBM or MLP beats linear** by at least +0.02 pooled IC in arm (b), with the CI excluding 0: use the flexible model in §6 and §7, and consider §8.2.
- **Otherwise:** linear models only from here on, and drop plans for a single-asset BTC DNN.

**Power.** The IC standard error is about 1/√n, which gives a 95 % CI of about ±0.044 in arm (a) and ±0.016 in arm (b). The paired difference is tighter than either, but arm (a) can only detect large gaps.

**Expectation.** On small, noisy tabular samples, flexible models rarely beat linear. The likely outcome is "linear only", which is still useful because it closes the question cheaply.

### 8.2 Where a DNN does make sense: the cross-section

Run this only if §8.1 shows flexible models help.

- **Why the cross-section.** Where neural nets have improved return prediction in the literature (Gu, Kelly and Xiu, 2020), the gain came from breadth: thousands of assets over decades. A single asset at 4 h gives about 2,200 samples a year, while the perp cross-section adds hundreds of coins per day.
- **Data.** Point-in-time top-100 perps from `data/xsec/` (the survivorship-free R4 universe), daily bars, with price-shape, funding and (after §1.1) `metrics` features per coin.
- **Target.** Each coin's next-day (or 3-day) return rank relative to the others. This is market-neutral, which removes the drift problem.
- **Models.** A shared MLP across coins vs a linear rank model built on R4's known effects (low volatility, short-term reversal) as the baseline.
- **Book and evaluation.** Dollar-neutral, weights capped at 2 % per coin (as in N1), 9 bp round trip. Report mean, median, worst 1 % of days, and results by year. R4's rank effects were real and still lost money to small-coin pumps, so IC alone is not a result.
- **Expectation.** Low to moderate. Breadth fixes the sample-size problem, not the pump tail.

## Sources

- WebSocket base-URL split: [https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice)
- Liquidation stream: [https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams)
- Basis endpoint: [https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Basis](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Basis)
- Options mark price: [https://developers.binance.com/docs/derivatives/option/market-data/Option-Mark-Price](https://developers.binance.com/docs/derivatives/option/market-data/Option-Mark-Price)
- Options open interest: [https://developers.binance.com/docs/derivatives/option/market-data/Open-Interest](https://developers.binance.com/docs/derivatives/option/market-data/Open-Interest)
- Margin interest-rate history: [https://developers.binance.com/docs/margin_trading/borrow-and-repay/Query-Margin-Interest-Rate-History](https://developers.binance.com/docs/margin_trading/borrow-and-repay/Query-Margin-Interest-Rate-History)
- Archive overview and download scripts: [https://github.com/binance/binance-public-data](https://github.com/binance/binance-public-data)
- Neural nets in return prediction: Gu, Kelly and Xiu (2020), "Empirical Asset Pricing via Machine Learning", *Review of Financial Studies*.