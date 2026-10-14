"""M1 mid-cap alt market-making feasibility — pure metric library + loaders (pre-registered in
next_signal_ideas.md, Round 8 — M1). Local CPU, deterministic; the only network-touching pieces are
the archive loaders, and they are never called by the metric functions.

Pipeline: free daily L1 bookTicker + signed aggTrades -> quoted-spread / Huang-Stoll / maker mark-out /
OFI per symbol-day -> equal-weight pool across symbol-days -> pre-registered PASS criteria.

CONVENTIONS
  day d             the UTC calendar day a symbol-day file belongs to (YYYY-MM-DD).
  strictly prior    every reference mid / touch used for a fill is read from a book update with
                    transaction_time < trade timestamp (_asof_index: the pandas.merge_asof direction="backward",
                    allow_exact_matches=False rule, by binary search). This is the anti-look-ahead rule; the reference mid
                    is read strictly before the fill timestamp (see the M1 limitations).
  forward mid       the mid at t + tau is the last book mid with transaction_time <= t + tau
                    (allow_exact_matches=True). It is in the future by construction, so only the
                    quantities that legitimately use it (r, adverse, net) may touch it.
  d (aggressor)     +1 for a buyer-initiated trade (isBuyerMaker == False), -1 for a seller-initiated.
  s (passive)       -d: the sign of the passive / maker side of that trade.
  e, r, impact      Huang-Stoll, all in bp normalised by the entry mid m: e = d(p-m)/m*1e4 is the
                    effective half-spread paid by the aggressor (earned by the maker),
                    r = d(p-m_tau)/m*1e4 is the maker's realised half-spread after tau,
                    impact = e - r = d(m_tau-m)/m*1e4 is the adverse selection / price impact.
                    Identity (checked exactly): e == r + impact.
  maker mark-out    Gatto entry decomposition, in bp normalised by m0 (mid strictly before the fill):
                    capture = s(m0-f)/m0*1e4, adverse(tau) = s(m_tau-m0)/m0*1e4,
                    net(tau) = capture + adverse = s(m_tau-f)/m0*1e4.
                    Identity (checked exactly): capture + adverse == net.
                    net_after_fee(tau) = net(tau) - (MAKER_BP - REBATE_BP)   [one passive side].
  OFI               Cont-Kukanov-Stoikov L1 order-flow imbalance per book update; ofi_predictive
                    resamples mid + OFI onto a regular step_s grid and OLS-regresses dmid_bp on OFI.
  coverage          fraction of the UTC day spanned by transaction_time of the bookTicker file
                    (a daily file may cover < 24 h; the M1 pre-registration records it per symbol-day).
  row               one derived symbol-day dict, written by analyse_day / fetch_mm, consumed by pool:
                    {symbol, day, cohort ("band"|"comparator"), n_bt, n_tr, coverage,
                     qs_median_bp, qs_mean_bp, qs_d10_bp, qs_d90_bp,
                     hs: {e_bp, r_bp, impact_bp, identity},      # r_bp / impact_bp keyed by str(tau)
                     mk: {capture_bp, adverse_bp, net_bp, net_after_fee_bp, identity},
                     ofi: {r2, beta, n}}
                    tau-keyed dicts use str(tau) keys so rows survive a JSON round-trip.
  pool              equal-weight across symbol-days (mean of the per-symbol-day values); CIs come from
                    day_clustered_bootstrap, which resamples whole UTC days.

Universe (select_universe): point-in-time R4 eligibility + trailing-30-day quote-volume ranks.
  Primary path  : pandas.read_parquet on data/xsec/klines/*.parquet (needs pyarrow or fastparquet).
  Fallback path : this environment has NO parquet engine, so when read_parquet raises we fall back to
                  a documented volume PROXY: rank data/xsec/symbols.json["kept"] (USDT perps minus the
                  stable / index bases) by the number of data/xsec/min1/<SYM>/ day files — an activity
                  proxy that is deterministic, survivorship-safe and needs no parquet. If min1 is
                  absent, the proxy degrades to the symbols.json["kept"] ordering.
  Injection     : pass universe={"band": [...], "comparator": [...]} (or set SYNTHETIC_UNIVERSE) to
                  supply a synthetic universe for tests; the real data layout is then never touched.

Outputs are JSON / .npz / .csv(.gz) only — never parquet (pyarrow is not installed).
"""
import io, json, os, zipfile
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.path.join(HERE, "..", "..", "data", "xsec")

# ------------------------------------------------------------------ constants (M1 pre-registration)
COST_BP = 4.5          # taker anchor (xsec.COST_BP)
MAKER_BP = 2.0         # Binance VIP0 maker, per side
TAKER_BP = 5.0         # Binance VIP0 taker, per side
REBATE_BP = 0.1        # liquidity-provider rebate (conservative end of -0.1 .. -0.5)
BAND = (41, 150)       # primary universe: trailing-30-day quote-volume ranks 41..150 (mid-caps)
COMPARATOR_N = 10      # BTCUSDT + top-10 by volume (majors)
TAUS = (1, 5, 30, 60)  # mark-out horizons, seconds; Huang-Stoll uses the same set
HS_TAUS = (1, 5, 30)   # Huang-Stoll horizons per the pre-registration
SAMPLE_DAYS = ["2023-07-19", "2023-10-18", "2024-01-17", "2024-03-13"]
EXCLUDE_BASES = {"USDC", "BUSD", "TUSD", "FDUSD", "USDP", "DAI", "EUR", "BTCDOM", "DEFI", "USDE", "PYUSD", "AEUR"}
DAY_MS = 86_400_000

BOOK_COLS = ["update_id", "best_bid_price", "best_bid_qty", "best_ask_price", "best_ask_qty",
             "transaction_time", "event_time"]
TRADE_COLS = ["aggTradeId", "price", "qty", "firstTradeId", "lastTradeId", "timestamp", "isBuyerMaker"]

# injectable synthetic universe for tests (see CONVENTIONS); None = use the real data layout
SYNTHETIC_UNIVERSE = None


# ------------------------------------------------------------------ loading
def _blob(path_or_bytes):
    if isinstance(path_or_bytes, (bytes, bytearray, memoryview)):
        return bytes(path_or_bytes)
    with open(path_or_bytes, "rb") as fh:
        return fh.read()


def read_zip_csv(blob, names):
    """First CSV member of a zip blob, parsed straight from the decompression stream (no full-text copy: a BTC bookTicker
    day is ~10 M rows); older archive files have no header row."""
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        name = z.namelist()[0]
        with z.open(name) as fh:
            first = fh.readline().decode()
        if not first.strip():
            raise ValueError("empty csv member in zip")
        header = 0 if not first[:1].isdigit() else None
        with z.open(name) as fh:
            df = pd.read_csv(fh, header=header)
    if header is None:
        df.columns = names[: df.shape[1]]
    return df


def read_bookticker_zip(path_or_bytes) -> pd.DataFrame:
    """L1 quote updates -> mid, spread, rel_spread_bp. transaction_time / event_time are epoch ms.

    Columns are converted in place and the frame is re-sorted only when it is not already in (transaction_time, update_id)
    order, so a ~20 M-row BTC day is held once, not three times.
    """
    df = read_zip_csv(_blob(path_or_bytes), BOOK_COLS)[BOOK_COLS]
    for c in BOOK_COLS:
        if not pd.api.types.is_numeric_dtype(df[c]):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if df.isna().to_numpy().any():
        df = df.dropna()
    for c in ("update_id", "transaction_time", "event_time"):
        if df[c].dtype != np.int64:
            df[c] = df[c].astype("int64")
    for c in ("best_bid_price", "best_bid_qty", "best_ask_price", "best_ask_qty"):
        if df[c].dtype != np.float64:
            df[c] = df[c].astype("float64")
    tt, uid = df["transaction_time"].to_numpy(), df["update_id"].to_numpy()
    if len(tt) > 1 and not ((tt[1:] > tt[:-1]) | ((tt[1:] == tt[:-1]) & (uid[1:] >= uid[:-1]))).all():
        df = df.sort_values(["transaction_time", "update_id"], kind="stable")
    out = df.reset_index(drop=True)
    del df
    mid = (out["best_bid_price"] + out["best_ask_price"]) / 2.0
    out["mid"] = mid
    out["spread"] = out["best_ask_price"] - out["best_bid_price"]
    out["rel_spread_bp"] = np.where(mid > 0, out["spread"] / mid * 1e4, np.nan)
    return out


_TRADE_ALIASES = {
    "agg_trade_id": "aggTradeId", "quantity": "qty", "first_trade_id": "firstTradeId",
    "last_trade_id": "lastTradeId", "transact_time": "timestamp", "is_buyer_maker": "isBuyerMaker",
}


def read_aggtrades_zip(path_or_bytes) -> pd.DataFrame:
    """Signed trades -> aggressor sign d and t_s (epoch s). d = +1 iff isBuyerMaker == False.

    Archive files exist in two layouts: an older header-less form (camelCase columns assigned
    from TRADE_COLS) and the current header form with snake_case columns and 'true'/'false'
    flags. Both are normalised to the camelCase names here.
    """
    df = read_zip_csv(_blob(path_or_bytes), TRADE_COLS)
    df = df.rename(columns={k: v for k, v in _TRADE_ALIASES.items() if k in df.columns})
    bm = df["isBuyerMaker"]
    if str(bm.dtype) == "bool":
        bm = bm.astype(int)
    elif bm.dtype == object:
        bm = bm.astype(str).str.strip().str.lower().map({"true": 1, "false": 0})
    df = df.assign(isBuyerMaker=pd.to_numeric(bm, errors="coerce"))
    out = pd.DataFrame({
        "aggTradeId": pd.to_numeric(df["aggTradeId"], errors="coerce").astype("int64"),
        "price": pd.to_numeric(df["price"], errors="coerce").astype("float64"),
        "qty": pd.to_numeric(df["qty"], errors="coerce").astype("float64"),
        "firstTradeId": pd.to_numeric(df["firstTradeId"], errors="coerce").astype("int64"),
        "lastTradeId": pd.to_numeric(df["lastTradeId"], errors="coerce").astype("int64"),
        "timestamp": pd.to_numeric(df["timestamp"], errors="coerce").astype("int64"),
        "isBuyerMaker": pd.to_numeric(df["isBuyerMaker"], errors="coerce").astype("int64"),
    })
    out = out.dropna().sort_values(["timestamp", "aggTradeId"], kind="stable").reset_index(drop=True)
    out["d"] = np.where(out["isBuyerMaker"] == 0, 1.0, -1.0)   # isBuyerMaker False -> buyer aggressor
    out["t_s"] = out["timestamp"] // 1000
    return out


# ------------------------------------------------------------------ 1 quoted spread
def quoted_spread_stats(bt) -> dict:
    """Median / mean / deciles of the relative quoted spread, in bp."""
    x = np.asarray(bt["rel_spread_bp"], float)
    x = x[np.isfinite(x) & (x > 0)]
    if x.size == 0:
        return dict(n=0, median_bp=np.nan, mean_bp=np.nan, d10_bp=np.nan, d90_bp=np.nan)
    return dict(n=int(x.size), median_bp=float(np.median(x)), mean_bp=float(x.mean()),
                d10_bp=float(np.percentile(x, 10)), d90_bp=float(np.percentile(x, 90)))


# ------------------------------------------------------------------ 2 Huang-Stoll
def _asof_index(bt, ts_ms, strict):
    """Row of the last book update with transaction_time < ts (strict) or <= ts, per timestamp; -1 when none.

    Same rule as pandas.merge_asof(direction="backward") on the time-sorted book (the last of several updates sharing a
    millisecond wins), but by binary search on the time column: no key / reference DataFrame copies of a ~10 M-row book.
    """
    tt = bt["transaction_time"].to_numpy(np.int64)
    ts = np.asarray(ts_ms, np.int64)
    if len(tt) > 1 and not (tt[1:] >= tt[:-1]).all():
        o = np.argsort(tt, kind="stable")
        i = np.searchsorted(tt[o], ts, side="left" if strict else "right") - 1
        return np.where(i >= 0, o[np.clip(i, 0, None)], -1)
    return np.searchsorted(tt, ts, side="left" if strict else "right") - 1


def _take(bt, col, idx):
    v = bt[col].to_numpy(float)
    if not len(v):
        return np.full(len(idx), np.nan)
    return np.where(idx >= 0, v[np.clip(idx, 0, None)], np.nan)


def _prior_mid(bt, ts_ms):
    """Strictly-prior mid for each timestamp (ms); NaN when no earlier book update exists."""
    return _take(bt, "mid", _asof_index(bt, ts_ms, strict=True))


def _mid_at(bt, ts_ms):
    """Last mid with transaction_time <= ts_ms (step-function mid at that instant)."""
    return _take(bt, "mid", _asof_index(bt, ts_ms, strict=False))


def _vw(x, w):
    x = np.asarray(x, float); w = np.asarray(w, float)
    ok = np.isfinite(x) & np.isfinite(w) & (w > 0)
    if not ok.any():
        return float(np.nanmean(x)) if np.isfinite(x).any() else float("nan")
    return float(np.average(x[ok], weights=w[ok]))


def huang_stoll(bt, tr, tau_s):
    """Huang-Stoll on signed trades, in bp normalised by the strictly-prior entry mid m.

    Returns per-trade arrays e, r, impact and their qty-VW means, plus the exact identity flag
    e == r + impact (impact is computed independently as d(m_tau - m)/m*1e4, so the identity is a
    real row-alignment check, not a tautology).
    """
    ts = tr["timestamp"].to_numpy(np.int64)
    p = tr["price"].to_numpy(float)
    d = tr["d"].to_numpy(float)
    q = tr["qty"].to_numpy(float)
    m = _prior_mid(bt, ts)
    m_fwd = _mid_at(bt, ts + int(tau_s) * 1000)
    ok = np.isfinite(m) & np.isfinite(m_fwd) & (m > 0) & (p > 0)
    if not ok.any():
        nan = np.full(len(tr), np.nan)
        return dict(e=nan, r=nan.copy(), impact=nan.copy(), identity=True,
                    e_vw=float("nan"), r_vw=float("nan"), impact_vw=float("nan"), n=0, m=nan.copy())
    e = np.full(len(tr), np.nan); r = e.copy(); impact = e.copy()
    mm, mf, pp, dd = m[ok], m_fwd[ok], p[ok], d[ok]
    e[ok] = dd * (pp - mm) / mm * 1e4
    r[ok] = dd * (pp - mf) / mm * 1e4
    impact[ok] = dd * (mf - mm) / mm * 1e4                        # independent form
    ident = bool(np.allclose(e[ok], r[ok] + impact[ok], rtol=0, atol=1e-9))
    return dict(e=e, r=r, impact=impact, identity=ident,
                e_vw=_vw(e[ok], q[ok]), r_vw=_vw(r[ok], q[ok]), impact_vw=_vw(impact[ok], q[ok]),
                n=int(ok.sum()), m=m)


# ------------------------------------------------------------------ 3 maker mark-out
def maker_markout(bt, tr, taus):
    """Gatto entry decomposition for the passive side of each trade at the touch.

    s = -d is the maker side; f is that side's touch from the strictly-prior book
    (maker buy -> best_bid, maker sell -> best_ask); m0 is the strictly-prior mid.
    All quantities in bp normalised by m0. Identity (checked exactly): capture + adverse == net.
    """
    ts = tr["timestamp"].to_numpy(np.int64)
    q = tr["qty"].to_numpy(float)
    s = -tr["d"].to_numpy(float)
    p = tr["price"].to_numpy(float)
    m0 = _prior_mid(bt, ts)
    j = _prior_book(bt, ts)
    bid, ask = j["bid"], j["ask"]
    # the maker's fill: a passive buyer fills at the bid, a passive seller at the ask
    f = np.where(s > 0, bid, ask)
    ok = np.isfinite(m0) & np.isfinite(f) & (m0 > 0)
    capture = np.where(ok, s * (m0 - f) / np.where(m0 > 0, m0, 1) * 1e4, np.nan)
    adverse, net = {}, {}
    ident_all = True
    for tau in taus:
        m_tau = _mid_at(bt, ts + int(tau) * 1000)
        a = np.where(ok & np.isfinite(m_tau), s * (m_tau - m0) / np.where(m0 > 0, m0, 1) * 1e4, np.nan)
        n_indep = np.where(ok & np.isfinite(m_tau), s * (m_tau - f) / np.where(m0 > 0, m0, 1) * 1e4, np.nan)
        got = ok & np.isfinite(m_tau)
        if got.any():
            ident_all = ident_all and bool(np.allclose(capture[got] + a[got], n_indep[got], rtol=0, atol=1e-9))
        adverse[str(tau)] = a
        net[str(tau)] = n_indep
    out = dict(capture=capture, adverse=adverse, net=net, identity=bool(ident_all),
               capture_vw=_vw(capture[ok], q[ok]), n=int(ok.sum()),
               adverse_vw={}, net_vw={}, net_after_fee_vw={})
    fee = MAKER_BP - REBATE_BP
    for tau in taus:
        k = str(tau)
        out["adverse_vw"][k] = _vw(adverse[k][ok], q[ok])
        out["net_vw"][k] = _vw(net[k][ok], q[ok])
        out["net_after_fee_vw"][k] = out["net_vw"][k] - fee
    return out


def _prior_book(bt, ts_ms):
    """Strictly-prior (bid, ask) touch for each timestamp; NaN when no earlier book update exists."""
    i = _asof_index(bt, ts_ms, strict=True)
    return dict(bid=_take(bt, "best_bid_price", i), ask=_take(bt, "best_ask_price", i))


# ------------------------------------------------------------------ 4 OFI
def ofi_l1(bt) -> pd.Series:
    """Cont-Kukanov-Stoikov L1 order-flow imbalance per book update (first update contributes 0)."""
    pb = bt["best_bid_price"].to_numpy(float)
    qb = bt["best_bid_qty"].to_numpy(float)
    pa = bt["best_ask_price"].to_numpy(float)
    qa = bt["best_ask_qty"].to_numpy(float)
    n = len(pb)
    e = np.zeros(n)
    if n > 1:
        pb0, pb1 = pb[:-1], pb[1:]
        qb0, qb1 = qb[:-1], qb[1:]
        pa0, pa1 = pa[:-1], pa[1:]
        qa0, qa1 = qa[:-1], qa[1:]
        e[1:] = ((pb1 >= pb0) * qb1 - (pb1 <= pb0) * qb0
                 - (pa1 <= pa0) * qa1 + (pa1 >= pa0) * qa0)
    return pd.Series(e, index=bt.index, name="ofi")


def _bins(ts_ms, step_ms):
    return np.asarray(ts_ms, np.int64) // int(step_ms)


def ofi_predictive(bt, step_s=1.0, max_lag_s=None) -> dict:
    """Regress the step_s-grid mid change (bp) on the binned L1 OFI. OLS via numpy.

    Grid: bin_mid[i] = last mid in bin i, bin_ofi[i] = sum of the per-update OFI in bin i.
    max_lag_s=None (default) -> the canonical Cont-Kukanov-Stoikov contemporaneous regression
        dmid[i] = (bin_mid[i] - bin_mid[i-1]) / bin_mid[i-1] * 1e4  on  bin_ofi[i], i = 1..n-1.
    max_lag_s=L > 0 -> the forward-horizon variant (the "predictive" use): the mid change from the
        end of bin i to the end of bin i + round(L/step_s), on bin_ofi[i].
    Returns {r2, beta, n, intercept}. Deterministic.
    """
    step_ms = int(round(float(step_s) * 1000))
    if step_ms <= 0:
        raise ValueError("step_s must be > 0 (millisecond grid resolution)")
    ts = bt["transaction_time"].to_numpy(np.int64)
    mid = bt["mid"].to_numpy(float)
    ofi = ofi_l1(bt).to_numpy(float)
    b = _bins(ts, step_ms)
    order = np.argsort(b, kind="stable")
    b, mid, ofi = b[order], mid[order], ofi[order]
    uniq, start = np.unique(b, return_index=True)
    last_idx = np.r_[start[1:] - 1, len(b) - 1]
    bin_mid = mid[last_idx]
    bin_ofi = np.add.reduceat(ofi, start)
    n = len(uniq)
    if max_lag_s is None:
        if n < 3:
            return dict(r2=np.nan, beta=np.nan, n=int(max(n - 1, 0)), intercept=np.nan)
        y = (bin_mid[1:] - bin_mid[:-1]) / np.where(bin_mid[:-1] > 0, bin_mid[:-1], np.nan) * 1e4
        x = bin_ofi[1:]
    else:
        h = max(1, int(round(float(max_lag_s) * 1000 / step_ms)))
        if n < h + 3:
            return dict(r2=np.nan, beta=np.nan, n=int(max(n - h, 0)), intercept=np.nan)
        y = (bin_mid[h:] - bin_mid[: n - h]) / np.where(bin_mid[: n - h] > 0, bin_mid[: n - h], np.nan) * 1e4
        x = bin_ofi[: n - h]
    ok = np.isfinite(y) & np.isfinite(x)
    x, y = x[ok], y[ok]
    if len(y) < 3:
        return dict(r2=np.nan, beta=np.nan, n=int(len(y)), intercept=np.nan)
    X = np.column_stack([np.ones(len(x)), x])
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    yhat = X @ coef
    ss_res = float(((y - yhat) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan
    return dict(r2=float(r2), beta=float(coef[1]), n=int(len(y)), intercept=float(coef[0]))


# ------------------------------------------------------------------ universe
def _is_perp(sym):
    return isinstance(sym, str) and sym.endswith("USDT") and sym[:-4] not in EXCLUDE_BASES


def _fallback_universe(root):
    """Documented no-parquet fallback: rank kept USDT perps by a min1 activity proxy."""
    sp = os.path.join(root, "symbols.json")
    kept = []
    if os.path.exists(sp):
        d = json.load(open(sp))
        kept = [s for s in d.get("kept", []) if _is_perp(s)]
    min1 = os.path.join(root, "min1")
    counts = {}
    if os.path.isdir(min1):
        for s in os.listdir(min1):
            p = os.path.join(min1, s)
            if not os.path.isdir(p) or not _is_perp(s):
                continue
            counts[s] = sum(1 for f in os.listdir(p) if f.endswith((".parquet", ".csv", ".csv.gz")))
    if not kept:
        kept = sorted(counts)
    if counts:
        # activity proxy: more collected min1 days -> more actively traded; ties break by name
        order = sorted(kept, key=lambda s: (-counts.get(s, 0), s))
    else:
        order = list(kept)                                    # degenerate: symbols.json order
    order = [s for s in order if s != "BTCUSDT"]
    return order, counts


def _parquet_universe(root, day):
    """Point-in-time R4 eligibility + trailing-30-day quote-volume ranks (needs a parquet engine)."""
    import glob
    t_day = int(pd.Timestamp(day, tz="UTC").timestamp())
    files = sorted(glob.glob(os.path.join(root, "klines", "*.parquet")))
    syms = [os.path.basename(f)[:-8] for f in files]
    syms = [s for s in syms if _is_perp(s)]
    scores, keep = {}, []
    for s in syms:
        k = pd.read_parquet(os.path.join(root, "klines", f"{s}.parquet"), columns=["ts", "quote_volume"])
        k = k[(k.ts < t_day)]
        if len(k) == 0:
            continue
        first = int(k.ts.min())
        if t_day - first < 60 * 86400:                        # R4: >= 60 days listed
            continue
        w = k[k.ts >= t_day - 30 * 86400]
        if len(w) < 30 * 24 * 0.9:                            # R4: complete prior-30-day 1 h data
            continue
        scores[s] = float(w.quote_volume.sum())
        keep.append(s)
    keep.sort(key=lambda s: -scores[s])
    return keep, scores


def select_universe(day, root=DATA_ROOT, universe=None, band=BAND, comparator_n=COMPARATOR_N,
                    btc="BTCUSDT"):
    """Point-in-time universe for `day` -> (band_syms, comparator_syms).

    band = trailing-30-day quote-volume ranks BAND[0]..BAND[1]; comparator = btc + top COMPARATOR_N.
    universe={"band": [...], "comparator": [...]} injects a synthetic universe (tests). Falls back to
    the documented min1 activity proxy when no parquet engine is installed (see CONVENTIONS).
    """
    u = universe if universe is not None else SYNTHETIC_UNIVERSE
    if u is not None:
        return list(u["band"]), list(u["comparator"])
    order, meta = None, {}
    try:
        order, meta = _parquet_universe(root, day)
        meta = dict(source="parquet", **meta)
    except Exception:
        order, counts = _fallback_universe(root)
        meta = dict(source="fallback_min1_activity", counts=counts)
    lo, hi = band
    band_syms = order[max(0, lo - 1): hi]
    top = [s for s in order if s != btc][:comparator_n]
    comp = ([btc] if btc in order or btc == "BTCUSDT" else []) + top
    return band_syms, comp


# ------------------------------------------------------------------ coverage / day analysis
def coverage_fraction(bt) -> float:
    """Fraction of the UTC day spanned by transaction_time (partial-day files are common)."""
    ts = bt["transaction_time"].to_numpy(np.int64)
    if ts.size == 0:
        return 0.0
    span = float(ts.max() - ts.min())
    return float(min(1.0, max(0.0, span / DAY_MS)))


def analyse_day(bt, tr, taus=TAUS, symbol="", day="", cohort="band", hs_taus=HS_TAUS) -> dict:
    """One derived symbol-day row (CONVENTIONS schema) from a bookTicker + aggTrades pair."""
    qs = quoted_spread_stats(bt)
    hs = {str(t): huang_stoll(bt, tr, t) for t in hs_taus}
    mk = maker_markout(bt, tr, taus)
    ofi = ofi_predictive(bt, step_s=1.0)
    return dict(
        symbol=str(symbol), day=str(day), cohort=str(cohort),
        n_bt=int(len(bt)), n_tr=int(len(tr)), coverage=float(coverage_fraction(bt)),
        qs_median_bp=float(qs["median_bp"]), qs_mean_bp=float(qs["mean_bp"]),
        qs_d10_bp=float(qs["d10_bp"]), qs_d90_bp=float(qs["d90_bp"]),
        hs=dict(e_bp=float(hs[str(hs_taus[0])]["e_vw"]),
                r_bp={k: float(v["r_vw"]) for k, v in hs.items()},
                impact_bp={k: float(v["impact_vw"]) for k, v in hs.items()},
                identity=bool(all(v["identity"] for v in hs.values()))),
        mk=dict(capture_bp=float(mk["capture_vw"]),
                adverse_bp={k: float(v) for k, v in mk["adverse_vw"].items()},
                net_bp={k: float(v) for k, v in mk["net_vw"].items()},
                net_after_fee_bp={k: float(v) for k, v in mk["net_after_fee_vw"].items()},
                identity=bool(mk["identity"])),
        ofi=dict(r2=float(ofi["r2"]), beta=float(ofi["beta"]), n=int(ofi["n"])),
    )


# ------------------------------------------------------------------ pooling + criteria
def day_clustered_bootstrap(values, days, n_boot=2000, seed=0, alpha=0.05):
    """CI of the equal-weight mean from resampling whole UTC days (cluster bootstrap).

    values: per-symbol-day statistic; days: the day label of each row. Returns (lo, hi) percentile CI.
    """
    v = np.asarray(values, float); d = np.asarray(days)
    ok = np.isfinite(v)
    v, d = v[ok], d[ok]
    if v.size == 0:
        return (float("nan"), float("nan"))
    ud = pd.unique(d)
    rng = np.random.default_rng(seed)
    idx_by_day = {k: np.flatnonzero(d == k) for k in ud}
    means = np.empty(n_boot)
    for b in range(n_boot):
        pick = rng.choice(ud, size=len(ud), replace=True)
        take = np.concatenate([idx_by_day[k] for k in pick]) if len(ud) else np.array([], int)
        means[b] = v[take].mean() if take.size else np.nan
    means = means[np.isfinite(means)]
    if means.size == 0:
        return (float("nan"), float("nan"))
    lo, hi = np.percentile(means, [100 * alpha / 2, 100 * (1 - alpha / 2)])
    return (float(lo), float(hi))


def _get_net5(row):
    return float(row["mk"]["net_after_fee_bp"]["5"])


def pool(rows) -> dict:
    """Equal-weight pool across symbol-days + the pre-registered M1 PASS criteria.

    Criteria (all required, primary cell = mid-cap band):
      net5_mean_pos_and_ci_pos  band net_after_fee(5s) mean > 0 and day-clustered bootstrap CI95
                                lower bound > 0
      median_spread_gt_2x_fee   band median relative quoted spread > 2 x MAKER_BP
      band_gt_comparator        band mean net_after_fee(5s) > comparator mean
      same_sign_median_mean     band net_after_fee(5s) median and mean share a sign
      days_pos_ge_3             band day-mean net_after_fee(5s) > 0 on >= 3 of the sampled days
    """
    rows = list(rows)
    band = [r for r in rows if r.get("cohort") == "band"]
    comp = [r for r in rows if r.get("cohort") == "comparator"]
    out = dict(n_rows=len(rows), n_band=len(band), n_comparator=len(comp),
               days=sorted({r["day"] for r in rows}), metrics={}, criteria={}, pass_=False)

    def _mean(vals):
        v = np.asarray(vals, float); v = v[np.isfinite(v)]
        return float(v.mean()) if v.size else float("nan")

    bnet = [_get_net5(r) for r in band]
    cnet = [_get_net5(r) for r in comp]
    bspread = [float(r["qs_median_bp"]) for r in band if np.isfinite(r.get("qs_median_bp", np.nan))]
    metrics = dict(
        band_net_after_fee_5s_mean=_mean(bnet),
        band_net_after_fee_5s_median=float(np.median(bnet)) if bnet else float("nan"),
        comparator_net_after_fee_5s_mean=_mean(cnet),
        band_median_rel_spread_bp=float(np.median(bspread)) if bspread else float("nan"),
        band_mean_rel_spread_bp=_mean(bspread),
    )
    # day-clustered bootstrap on the band's net_after_fee(5s)
    bdays = [r["day"] for r in band]
    if band:
        lo, hi = day_clustered_bootstrap(bnet, bdays)
    else:
        lo = hi = float("nan")
    metrics["band_net_after_fee_5s_ci95"] = [lo, hi]
    # day means of the band
    day_map = {}
    for r in band:
        day_map.setdefault(r["day"], []).append(_get_net5(r))
    day_means = {d: _mean(v) for d, v in sorted(day_map.items())}
    metrics["band_day_mean_net_after_fee_5s"] = day_means
    days_pos = sum(1 for v in day_means.values() if np.isfinite(v) and v > 0)
    metrics["band_days_positive"] = int(days_pos)
    metrics["band_days"] = int(len(day_means))
    ofis = [float(r["ofi"]["r2"]) for r in band if np.isfinite(r.get("ofi", {}).get("r2", np.nan))]
    metrics["band_ofi_r2_mean"] = _mean(ofis)

    net_mean = metrics["band_net_after_fee_5s_mean"]
    net_med = metrics["band_net_after_fee_5s_median"]
    c1 = bool(np.isfinite(net_mean) and net_mean > 0 and np.isfinite(lo) and lo > 0)
    c2 = bool(np.isfinite(metrics["band_median_rel_spread_bp"])
              and metrics["band_median_rel_spread_bp"] > 2 * MAKER_BP)
    c3 = bool(np.isfinite(net_mean) and np.isfinite(metrics["comparator_net_after_fee_5s_mean"])
              and net_mean > metrics["comparator_net_after_fee_5s_mean"])
    c4 = bool(np.isfinite(net_mean) and np.isfinite(net_med) and (net_mean > 0) == (net_med > 0))
    c5 = bool(days_pos >= 3)

    def det(ok, s):
        return dict(ok=bool(ok), detail=s)

    criteria = {
        "net5_mean_pos_and_ci_pos": det(c1,
            f"band net_after_fee(5s) mean {net_mean:+.3f} bp, day-clustered CI95 [{lo:+.3f}, {hi:+.3f}]"),
        "median_spread_gt_2x_fee": det(c2,
            f"band median rel spread {metrics['band_median_rel_spread_bp']:.3f} bp vs 2x maker fee "
            f"{2 * MAKER_BP:.1f} bp"),
        "band_gt_comparator": det(c3,
            f"band {net_mean:+.3f} bp vs comparator {metrics['comparator_net_after_fee_5s_mean']:+.3f} bp"),
        "same_sign_median_mean": det(c4,
            f"median {net_med:+.3f} bp, mean {net_mean:+.3f} bp"),
        "days_pos_ge_3": det(c5,
            f"positive day-mean net_after_fee(5s) on {days_pos} of {len(day_means)} days "
            f"({', '.join(f'{d}:{v:+.2f}' for d, v in day_means.items())})"),
    }
    out["metrics"] = metrics
    out["criteria"] = criteria
    out["pass_"] = bool(all(c["ok"] for c in criteria.values()))
    out["pass"] = out["pass_"]                       # alias; JSON-safe
    return out


__all__ = [
    "COST_BP", "MAKER_BP", "TAKER_BP", "REBATE_BP", "BAND", "COMPARATOR_N", "TAUS", "HS_TAUS",
    "SAMPLE_DAYS", "EXCLUDE_BASES", "SYNTHETIC_UNIVERSE", "DATA_ROOT",
    "read_bookticker_zip", "read_aggtrades_zip", "quoted_spread_stats", "huang_stoll",
    "maker_markout", "ofi_l1", "ofi_predictive", "select_universe", "coverage_fraction",
    "analyse_day", "pool", "day_clustered_bootstrap",
]
