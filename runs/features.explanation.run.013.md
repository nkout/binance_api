# Features explanation — run.013

This document explains (1) the meaning of every value in the raw event data
and (2) the mathematical definition of the price-level features produced by
`load_price_level_features()` in `runs/btc_lstm.run.013.ipynb`.

The collector is `binance_live_orderbook_v5.py`. It writes verbatim Binance
websocket messages to `events.<tag>.w<win>.<YYMMDD_HH>.jsonl.gz`, one file per
stream. The notebook then aggregates those raw messages **by price value and
through time**.

---

## 1. Raw data — meaning of each value

Each JSONL line is one raw websocket message wrapped by the collector:

```json
{"s":"fd","r":1787184000.085840,"d":{...raw message verbatim...}}
{"s":"fd","r":1787184000.000000,"k":"snapshot","d":{...REST depth snapshot...}}
```

### Top-level fields

| key | meaning |
|-----|---------|
| `s` | **stream tag** — which feed the message came from |
| `r` | **receive time**, epoch seconds (with fraction). This is the canonical clock: the collector's local time when the message arrived. Used for bar bucketing and all timing. |
| `k` | **`"snapshot"`** marks a book-resync baseline (a full REST depth snapshot). Absent on normal stream messages. |
| `d` | the raw Binance payload, verbatim |

### Possible values of `s` — all four streams are aggregated

| value | meaning |
|-------|---------|
| `fd` | **future depth** — BTCUSDT (futures) orderbook depth diff |
| `ft` | **future trade** — BTCUSDT (futures) trade/aggTrade |
| `sd` | **spot depth** — spot orderbook depth diff |
| `st` | **spot trade** — spot trade/aggTrade |

The stream mapping is defined in the collector as
`EVENT_TAGS = {"spot": ("sd", "st"), "future": ("fd", "ft")}`.

**Every stream is aggregated separately and never mixed.** Each depth stream
(`fd`, `sd`) builds its own orderbook and its own depth features (prefixed
`fd_` and `sd_`); each trade stream (`ft`, `st`) produces its own trade
features (prefixed `ft_` and `st_`).

### Depth message (`s` = `fd` or `sd`), `d.e` = `"depthUpdate"`

| key | meaning |
|-----|---------|
| `d.e` | event type — `"depthUpdate"` (partial orderbook delta) |
| `d.E` | **event time**, epoch milliseconds (when the exchange emitted it) |
| `d.T` | **transaction time**, epoch milliseconds |
| `d.s` | symbol, e.g. `"BTCUSDT"` |
| `d.ps` | pair, e.g. `"BTCUSDT"` |
| `d.U` | **first update id** in this batch (inclusive) |
| `d.u` | **last update id** in this batch (inclusive) |
| `d.pu` | **previous update id** (`pu = U − 1` in futures); used for reconnect continuity checks |
| `d.b` | **bids**: array of `[price, qty]` deltas |
| `d.a` | **asks**: array of `[price, qty]` deltas |
| `d.st` | trade-stream-stopped flag (1 = trades not streaming) |

`b`/`a` contain **only the price levels that changed** (a delta, not the full
book):

- `["1000.00","36.660"]` → the level at price 1000.00 is now at **quantity 36.660** (added or resized).
- `["66537.50","0.000"]` → **quantity 0** = that level was **removed** from the book.

### Trade message (`s` = `ft` or `st`), `d.e` = `"trade"`

| key | meaning |
|-----|---------|
| `d.e` | `"trade"` |
| `d.E` | event time (ms) |
| `d.T` | transaction time (ms) |
| `d.t` | **trade id** (monotonic; used for dedupe) |
| `d.p` | **price** of the trade |
| `d.q` | **quantity** of the trade |
| `d.X` | trade status: `"NA"` = **not applied** (placeholder trade, see below) |
| `d.b` / `d.a` | buyer / seller order ids |
| `d.m` | **is-buyer-maker** flag. `true` → the buyer was the maker, so the **taker was the seller** (aggressive sell); `false` → the buyer was the taker (aggressive buy). |

**Placeholder trades:** Binance sometimes emits trades with `X: "NA"`,
`p: "0"`, `q: "0"` (book-empty / undetermined fills). These carry no
information and are **skipped** in the aggregation (`q ≤ 0` is dropped), so
they never create feature rows.

---

## 2. Aggregation setup

`load_price_level_features(events_dir, window_sec, time_step=None, roll_window=12)`

- **Time bucketing**: `time_step` seconds (default = `window_sec` = 5s).
  A message with receive time `r` falls into the bin

  $$b = \left\lfloor \frac{r}{\mathrm{time\_step}} \right\rfloor \cdot \mathrm{time\_step}$$

- **Price grouping**: prices are grouped on the **0.1 tick**:

  $$\mathrm{tick} = \mathrm{round}(p \times 10),\qquad \mathrm{price} = \mathrm{tick}/10$$

- **Two series per price**: every distinct price produces two independent
  series — one for the `bid` side and one for the `ask` side.

- **Four streams, never mixed**: each of `fd`, `sd` builds its own resting
  book (stateful across bins) and its own depth features; each of `ft`, `st`
  builds its own trade features. The output row for a `(time, side, price)`
  cell carries the features of every stream that had activity there, under the
  prefixes below.

- **Book reconstruction**: the running resting quantity at every `(side, tick)`
  is carried across bins (stateful), so `qty_open` is the carry-forward value
  even when the price was untouched in the previous bin. `fd` and `sd` books
  are tracked independently.

- **Output**: a sparse panel — one row per `(time, side, price)` cell with
  activity — but each price's rows form a full timeseries.

---

## 3. Notation

The feature formulas below are written once per group; **every group is
replicated per stream** with the column prefix:

| prefix | stream | applies to |
|--------|--------|------------|
| `fd_` | future depth | base, A, B, D, and the depth E features |
| `sd_` | spot depth | base, A, B, D, and the depth E features |
| `ft_` | future trades | trade base, C, and `ft_qty_traded_roll` |
| `st_` | spot trades | trade base, C, and `st_qty_traded_roll` |

For example, `fd_qty_flow` and `sd_qty_flow` are the future-depth and spot-depth
gross churn at the same cell; `ft_qty_sold` and `st_qty_sold` are the
aggressively-sold volume from future and spot trades.

For a given cell (bin `b`, side `s`, price `p`) and a given **depth stream**,
let the depth updates at this price within the bin occur at receive times
`r_1 < r_2 < … < r_K` with **absolute** quantities `q_1, q_2, …, q_K`. Let `q_0`
be the resting quantity carried in from the previous bin (the "open").

Define the per-update change

$$\delta_k = q_k - q_{k-1} \qquad (k = 1,\dots,K)$$

For a given **trade stream**, let there be `N` trades at this price within the
bin, each with quantity `q_i`, maker flag `m_i`, and receive time `r_i`
(`i = 1,\dots,N`).

---

## 4. Feature definitions

### Base features (resting-book volume) — depth streams (`fd_*`, `sd_*`)

| feature | formula |
|---------|---------|
| `qty_open` | $$q_0$$ |
| `qty_close` | $$q_K$$ |
| `qty_net_change` | $$q_K - q_0$$ |
| `qty_added` | $$\sum_k \max(\delta_k,\, 0)$$ |
| `qty_removed` | $$\sum_k \max(-\delta_k,\, 0)$$ |
| `qty_flow` | $$\sum_k |\delta_k| = \mathrm{qty\_added} + \mathrm{qty\_removed}$$ |
| `n_updates` | $$K$$ |
| `n_pulls` | $$\sum_k \mathbf{1}[q_k = 0]$$ (times the level was emptied) |

`qty_added`/`qty_removed` sum the **positive/negative** increments separately;
`qty_flow` is the gross churn (total absolute volume moved at this price).

### Group A — liquidity dynamics (depth streams `fd_*`, `sd_*`)

| feature | formula / meaning |
|---------|-------------------|
| `n_adds` | $$\sum_k \mathbf{1}[q_{k-1}=0 \;\wedge\; q_k>0]$$ — level created |
| `n_resizes` | $$\sum_k \mathbf{1}[q_{k-1}>0 \;\wedge\; q_k>0]$$ — level resized (changed but not emptied) |
| `qty_min` | $$\min(q_0, q_1, \dots, q_K)$$ |
| `qty_max` | $$\max(q_0, q_1, \dots, q_K)$$ |
| `qty_range` | $$\mathrm{qty\_max} - \mathrm{qty\_min}$$ |
| `mean_delta` | $$\frac{1}{K}\sum_k \delta_k = \frac{q_K - q_0}{K}$$ (average update size) |
| `std_delta` | $$\sqrt{\frac{1}{K}\sum_k \delta_k^2 - \mathrm{mean\_delta}^2}$$ (population std of update sizes) |
| `pull_to_add_ratio` | $$\dfrac{\mathrm{qty\_removed}}{\mathrm{qty\_added}}$$ if `qty_added > 0`, else NaN (drain vs replenish) |
| `turnover` | $$\dfrac{\mathrm{qty\_flow}}{\mathrm{qty\_open}}$$ if `qty_open > 0`, else NaN (churn relative to resting size) |

`n_adds + n_resizes + n_pulls = n_updates` (each update is exactly one of:
add, resize, or pull/remove).

### Group B — timing / presence (depth streams `fd_*`, `sd_*`)

Let `E_k` be the event time (ms) of update `k`, and define the inter-update gap

$$g_k = (r_k - r_{k-1}) \times 1000 \quad(\text{ms}),\qquad k=2,\dots,K$$

| feature | formula / meaning |
|---------|-------------------|
| `t_first_off` | $$(r_1 - b)\times 1000$$ — ms offset of the first update in the bin |
| `t_last_off` | $$(r_K - b)\times 1000$$ — ms offset of the last update in the bin |
| `mean_gap_ms` | $$\frac{1}{K-1}\sum_{k=2}^{K} g_k$$ (NaN if K<2) |
| `min_gap_ms` | $$\min_k g_k$$ |
| `max_gap_ms` | $$\max_k g_k$$ |
| `latency_ms` | $$\frac{1}{K}\sum_k (r_k \times 1000 - E_k)$$ — mean feed delay (receive − event time) |

### Trade features (base) — trade streams (`ft_*`, `st_*`)

| feature | formula / meaning |
|---------|-------------------|
| `n_trades` | $$N$$ |
| `qty_traded` | $$\sum_i q_i$$ |
| `n_buyer_maker` | $$\sum_i \mathbf{1}[m_i]$$ — buyer is maker (taker **sold**) |
| `qty_buyer_maker` | $$\sum_i q_i\,\mathbf{1}[m_i]$$ |
| `n_buyer_taker` | $$\sum_i \mathbf{1}[\neg m_i]$$ — buyer is taker (taker **bought**) |
| `qty_buyer_taker` | $$\sum_i q_i\,\mathbf{1}[\neg m_i]$$ |
| `qty_sold` | `qty_buyer_maker` — BTC aggressively **sold** at this price |
| `qty_bought` | `qty_buyer_taker` — BTC aggressively **bought** at this price |

A trade is assigned to the **ask** side when `m = false` (taker lifted the ask)
and to the **bid** side when `m = true` (taker hit the bid).

### Group C — trade dynamics (trade streams `ft_*`, `st_*`)

| feature | formula / meaning |
|---------|-------------------|
| `qty_max_trade` | $$\max_i q_i$$ |
| `median_trade_qty` | median of `{q_i}` |
| `p90_trade_qty` | 90th percentile of `{q_i}` |
| `taker_buy_share` | $$\dfrac{\mathrm{n\_buyer\_taker}}{N}$$ (NaN if N=0) |
| `time_imb_ms` | qty-weighted sell-minus-buy timing (see below) |
| `first_trade_off` | $$(\min_i r_i - b)\times 1000$$ |
| `last_trade_off` | $$(\max_i r_i - b)\times 1000$$ |

`time_imb_ms` uses the within-bin offset `off_i = (r_i - b)×1000`:

$$
\text{sell\_avg} = \frac{\sum_{i:\,m_i} q_i\,\mathrm{off}_i}{\sum_{i:\,m_i} q_i},
\qquad
\text{buy\_avg} = \frac{\sum_{i:\,\neg m_i} q_i\,\mathrm{off}_i}{\sum_{i:\,\neg m_i} q_i}
$$

$$
\mathrm{time\_imb\_ms} = \mathrm{sell\_avg} - \mathrm{buy\_avg}
$$

Positive means sell flow arrived **later** in the bin than buy flow (when both
sides are present; single-sided bins use `sell_avg` or `-buy_avg`).

### Group D — book position (depth streams `fd_*`, `sd_*`; needs full-book reconstruction)

Each depth stream reconstructs its own book. At bin end, define the top of that
stream's book over all resting levels (qty > 0):

$$
\text{best\_bid\_p} = \max\{p' : \text{bid qty at } p' > 0\},
\qquad
\text{best\_ask\_p} = \min\{p' : \text{ask qty at } p' > 0\}
$$

$$
\mathrm{mid} = \frac{\mathrm{best\_bid\_p} + \mathrm{best\_ask\_p}}{2}
$$

| feature | formula / meaning |
|---------|-------------------|
| `dist_to_top_bps` | bid: $$\dfrac{\mathrm{best\_bid\_p} - p}{\mathrm{best\_bid\_p}}\times 10^4$$; ask: $$\dfrac{p - \mathrm{best\_ask\_p}}{\mathrm{best\_ask\_p}}\times 10^4$$ — how deep the level sits below/above the top of its own side, in basis points (0 = at the top) |
| `dist_to_mid_bps` | $$\dfrac{p - \mathrm{mid}}{\mathrm{mid}}\times 10^4$$ — signed distance to mid (bid negative, ask positive) |
| `level_rank` | ordinal rank of `qty_close` among all resting levels (qty > 0) on the **same side** of the stream's book at bin end, sorted by qty descending: $$\mathrm{rank} = \#\{q' > \mathrm{qty\_close}\} + 1$$ (1 = largest, i.e. "the wall"); NaN if `qty_close ≤ 0` |
| `side_flip` | 1 if the same price also appeared on the **opposite** side within the same bin (in that stream's book), else 0 |

### Group E — per-price rolling (post-processed over each stream's observed bins)

For a fixed `(side, price)` and stream, sort its bins by `t`: `t_1 < t_2 < …`.
Denote the value of a column `X` at the `i`-th observed bin by `X_i`. The
rolling window is `roll_window` (default 12 bins ≈ 1 min at 5s), with
`min_periods = 1`, and `ε = 1e-9`.

Depth rolling features (computed per depth stream): `fd_qty_flow_z`,
`sd_qty_flow_z`, `fd_qty_close_mom`, `sd_qty_close_mom`.

Trade rolling features (computed per trade stream): `ft_qty_traded_roll`,
`st_qty_traded_roll`.

| feature | formula / meaning |
|---------|-------------------|
| `qty_close_mom` | $$X_i - X_{i-1}$$ for `X = qty_close` — change in resting quantity vs the previous bin |
| `qty_flow_z` | $$\dfrac{\mathrm{qty\_flow}_i - \mu_W}{\sigma_W + \varepsilon}$$ where $$\mu_W = \frac{1}{W'}\sum_{j=\max(1,i-W+1)}^{i}\mathrm{qty\_flow}_j$$ and $$\sigma_W$$ is the matching rolling std — a rolling z-score of flow activity |
| `qty_traded_roll` | $$\sum_{j=\max(1,i-W+1)}^{i}\mathrm{qty\_traded}_j$$ — rolling sum of traded quantity |

Rolling windows run over the price's **observed** bin sequence (the panel is
sparse), so "last `roll_window` bins" means the last `roll_window` bins in which
that price had activity.

---

## 5. Quick reference — feature column list (93 columns)

```
t, side, price,
# fd — future depth (base + A + B + D + E)
fd_qty_open, fd_qty_close, fd_qty_net_change, fd_qty_added,
fd_qty_removed, fd_qty_flow, fd_n_updates, fd_n_pulls,
fd_n_adds, fd_n_resizes, fd_qty_min, fd_qty_max, fd_qty_range,
fd_mean_delta, fd_std_delta, fd_pull_to_add_ratio, fd_turnover,
fd_t_first_off, fd_t_last_off, fd_mean_gap_ms, fd_min_gap_ms,
fd_max_gap_ms, fd_latency_ms,
fd_dist_to_top_bps, fd_dist_to_mid_bps, fd_level_rank, fd_side_flip,
fd_qty_flow_z, fd_qty_close_mom,
# sd — spot depth (same 29 features)
sd_qty_open, ..., sd_side_flip, sd_qty_flow_z, sd_qty_close_mom,
# ft — future trades (trade base + C + roll)
ft_n_trades, ft_qty_traded, ft_n_buyer_maker, ft_qty_buyer_maker,
ft_n_buyer_taker, ft_qty_buyer_taker, ft_qty_sold, ft_qty_bought,
ft_qty_max_trade, ft_median_trade_qty, ft_p90_trade_qty,
ft_taker_buy_share, ft_time_imb_ms, ft_first_trade_off, ft_last_trade_off,
ft_qty_traded_roll,
# st — spot trades (same 16 features)
st_n_trades, ..., st_qty_traded_roll
```

Full ordered list (as produced by the function):

```
t, side, price,
fd_qty_open, fd_qty_close, fd_qty_net_change, fd_qty_added, fd_qty_removed,
fd_qty_flow, fd_n_updates, fd_n_pulls, fd_n_adds, fd_n_resizes, fd_qty_min,
fd_qty_max, fd_qty_range, fd_mean_delta, fd_std_delta, fd_pull_to_add_ratio,
fd_turnover, fd_t_first_off, fd_t_last_off, fd_mean_gap_ms, fd_min_gap_ms,
fd_max_gap_ms, fd_latency_ms, fd_dist_to_top_bps, fd_dist_to_mid_bps,
fd_level_rank, fd_side_flip,
sd_qty_open, sd_qty_close, sd_qty_net_change, sd_qty_added, sd_qty_removed,
sd_qty_flow, sd_n_updates, sd_n_pulls, sd_n_adds, sd_n_resizes, sd_qty_min,
sd_qty_max, sd_qty_range, sd_mean_delta, sd_std_delta, sd_pull_to_add_ratio,
sd_turnover, sd_t_first_off, sd_t_last_off, sd_mean_gap_ms, sd_min_gap_ms,
sd_max_gap_ms, sd_latency_ms, sd_dist_to_top_bps, sd_dist_to_mid_bps,
sd_level_rank, sd_side_flip,
ft_n_trades, ft_qty_traded, ft_n_buyer_maker, ft_qty_buyer_maker,
ft_n_buyer_taker, ft_qty_buyer_taker, ft_qty_sold, ft_qty_bought,
ft_qty_max_trade, ft_median_trade_qty, ft_p90_trade_qty, ft_taker_buy_share,
ft_time_imb_ms, ft_first_trade_off, ft_last_trade_off,
st_n_trades, st_qty_traded, st_n_buyer_maker, st_qty_buyer_maker,
st_n_buyer_taker, st_qty_buyer_taker, st_qty_sold, st_qty_bought,
st_qty_max_trade, st_median_trade_qty, st_p90_trade_qty, st_taker_buy_share,
st_time_imb_ms, st_first_trade_off, st_last_trade_off,
fd_qty_flow_z, sd_qty_flow_z, fd_qty_close_mom, sd_qty_close_mom,
ft_qty_traded_roll, st_qty_traded_roll
```