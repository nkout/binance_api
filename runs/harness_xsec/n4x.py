"""N4-X minute-scale execution check — library (pre-registered in next_signal_ideas.md, "N4 follow-up").

Frozen POST rule from N4, re-priced on Binance daily 1-minute futures klines for a random sample of registered events.

Conventions (minute bars indexed by their open time m, seconds UTC)
  O(m), Cm(m)      open / close of the minute bar opening at m
  P(t)             price at instant t = close of the minute bar opening t - 60 (last trade before t)
  entry, k min     O(s + k*60): first trade of the minute starting k minutes after the settlement (registered k = 2;
                   information k = 1, 5, 15)
  exit             P(s + 1h)
  trade            -q * (exit / entry - 1), bp;  net = gross - COST_ALL_BP (9 bp fee + 10 bp spread / slippage)
  path(k)          -q * (P(s + k*60) / P(s) - 1), k = 0 .. 120: the mean signed path around the settlement
"""
import io, os, sys, zipfile
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import settle as S

MIN, DAY = 60, 86400
COST_ALL_BP = 19.0
N_SAMPLE, SEED = 2000, 0
ENTRY_K, INFO_K = 2, (1, 5, 15)
PATH_K = 120
DL = "https://data.binance.vision/data/futures/um/daily/klines/{s}/1m/{s}-1m-{d}.zip"
KC = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "count",
      "taker_buy_volume", "taker_buy_quote_volume", "ignore"]


# ------------------------------------------------------------------ sample
def sample_events(e, lo, hi, n=N_SAMPLE, seed=SEED):
    """Registered events with lo <= s < hi, n drawn without replacement (rng seed), sorted by time."""
    pool = e[(e.s >= lo) & (e.s < hi)].sort_values(["s", "j"]).reset_index(drop=True)
    idx = np.sort(np.random.default_rng(seed).choice(len(pool), min(n, len(pool)), replace=False))
    return pool.iloc[idx].reset_index(drop=True)


def needed_days(sample):
    """(sym, 'YYYY-MM-DD') pairs covering every instant from s - 1 min to s + PATH_K min."""
    out = set()
    for sym, s in zip(sample.sym, sample.s):
        for t in (s - MIN, s + PATH_K * MIN):
            out.add((sym, pd.to_datetime(t // DAY * DAY, unit="s").strftime("%Y-%m-%d")))
    return sorted(out)


# ------------------------------------------------------------------ data
def fetch_days(pairs, root, workers=16):
    """Download daily 1 m klines to root/<SYM>/<date>.parquet (ts, open, close, quote_volume). Resumable."""
    from concurrent.futures import ThreadPoolExecutor
    from fetch_archive import get

    def one(pair):
        sym, d = pair
        fp = os.path.join(root, sym, f"{d}.parquet")
        if os.path.exists(fp) or os.path.exists(fp + ".missing"):
            return "cached"
        os.makedirs(os.path.join(root, sym), exist_ok=True)
        b = get(DL.format(s=sym, d=d))
        if b is None:
            open(fp + ".missing", "w").close(); return "missing"
        with zipfile.ZipFile(io.BytesIO(b)) as z:
            raw = z.read(z.namelist()[0]).decode()
        hdr = 0 if not raw[:1].isdigit() else None
        k = pd.read_csv(io.StringIO(raw), header=hdr)
        if hdr is None:
            k.columns = KC[:k.shape[1]]
        t = pd.to_numeric(k.open_time).astype(np.int64).to_numpy()
        ts = np.where(t > 10 ** 14, t // 10 ** 6, t // 1000)
        pd.DataFrame({"ts": ts, "open": pd.to_numeric(k.open).astype(float), "close": pd.to_numeric(k.close).astype(float),
                      "quote_volume": pd.to_numeric(k.quote_volume).astype(float)}).to_parquet(fp, index=False)
        return "ok"
    with ThreadPoolExecutor(workers) as ex:
        st = list(ex.map(one, pairs))
    return {k: st.count(k) for k in set(st)}


def load_minutes(root, sym, days):
    parts = [pd.read_parquet(os.path.join(root, sym, f"{d}.parquet")) for d in days
             if os.path.exists(os.path.join(root, sym, f"{d}.parquet"))]
    if not parts:
        return None
    return pd.concat(parts).drop_duplicates("ts").set_index("ts").sort_index()


# ------------------------------------------------------------------ trades
def event_minutes(m, s):
    """Prices for one event from a minute frame m (index = open ts): entries, exit, path. NaN when a bar is missing."""
    O = lambda t: m.open.get(t, np.nan)
    P = lambda t: m.close.get(t - MIN, np.nan)
    path = np.array([P(s + k * MIN) for k in range(PATH_K + 1)])
    ent = {k: O(s + k * MIN) for k in (ENTRY_K,) + INFO_K}
    return ent, P(s + 3600), path


def trade_table(sample, minutes):
    """minutes: dict sym -> minute frame. Returns per-event gross for each entry k, the registered net, and the path matrix."""
    rows, paths = [], []
    for r in sample.itertuples():
        m = minutes.get(r.sym)
        if m is None:
            ent, ex, path = {k: np.nan for k in (ENTRY_K,) + INFO_K}, np.nan, np.full(PATH_K + 1, np.nan)
        else:
            ent, ex, path = event_minutes(m, r.s)
        row = {"sym": r.sym, "s": r.s, "q": r.q, "exit": ex, "p_s": path[0], "post_1h": r.post}
        for k, v in ent.items():
            row[f"g{k}"] = -r.q * (ex / v - 1) * 1e4
        rows.append(row)
        paths.append(-r.q * (path / path[0] - 1) * 1e4)
    t = pd.DataFrame(rows)
    t["net"] = t[f"g{ENTRY_K}"] - COST_ALL_BP
    return t, np.array(paths)


def verdict(t, seed=0):
    ok = np.isfinite(t.net)
    x, cl = t.net[ok].to_numpy(), t.s[ok].to_numpy()
    b = S.cluster_boot(x, cl, seed=seed)
    crit = {"net mean > 0": b["mean"] > 0, "net CI lo > 0": b["lo"] > 0, "median > 0": float(np.median(x)) > 0}
    return dict(boot=b, desc=S.describe(x), criteria=crit, pass_=bool(all(crit.values())), n_used=int(ok.sum()),
                n_missing=int((~ok).sum()))
