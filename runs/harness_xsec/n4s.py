"""N4-S — the funding-settlement move at sub-second resolution (next_signal_ideas.md, Round 10). Library.

Frozen N4 POST rule (d = sign(r_prev), the paying side) re-priced on Binance aggTrades with taker-side fills.

Conventions (times in ms; s = settlement, seconds UTC in the sample)
  trades           t (ms), p, ibm (is_buyer_maker: True = the seller was the taker, a market sell at the bid)
  fill(target, side)  first trade at or after target on the taker side ('buy': ibm False, 'sell': ibm True), within WAIT_MS; else NaN
  entry(Δ)         fill(s + Δ, buy if d > 0 else sell)
  exit             fill(s + EXIT_S, sell if d > 0 else buy)
  gross(Δ)         d * (exit / entry(Δ) - 1) * 1e4;  net = gross - FEE_BP
  path(Δ)          d * (last trade at or before s + Δ / P(s-) - 1) * 1e4, P(s-) = N4-X p_s
  pre              enter on the taker side at the last trade before s - 1 s, hold through s (funding -d * r_s * 1e4), same exit
  n4x replication  entry = first trade (any side) at or after s + 120 s, exit = last trade before s + 3600 s (N4-X g2 before costs)
"""
import io, json, os, time, urllib.error, urllib.request, zipfile
import numpy as np
try:                                                    # local harness; in the Colab notebook these are defined in an earlier cell
    from settle import cluster_boot, describe
except ImportError:
    pass

FEE_BP = 9.0
EXTRA_BP = 10.0                                     # information: +5 bp per side
DELTAS = (0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120)
PRIMARY = 0.5
EXIT_S = 3595
WAIT_MS = 60_000
PRE_S = 1
WINDOWS = ((-60, 0), (0, 180 + 60), (3300, 3600 + 60))   # seconds relative to s: pre, entry (+ wait), exit / replication
COLS = ("agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "transact_time", "is_buyer_maker")
URL = "https://data.binance.vision/data/futures/um/daily/aggTrades/{s}/{s}-aggTrades-{d}.zip"


def get(url, tries=5):
    """Bytes of url, None on 404; retries other errors with backoff."""
    for k in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if k == tries - 1:
                raise
        except Exception:
            if k == tries - 1:
                raise
        time.sleep(2 ** k)


def read_agg(blob):
    """aggTrades zip bytes -> (t ms int64, price float64, ibm bool), sorted by time; header and no-header layouts."""
    import pyarrow.csv as pc
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            head = f.read(64)
        has_header = head.startswith(b"agg_trade_id")
        with z.open(name) as f:
            ro = pc.ReadOptions(column_names=None if has_header else list(COLS), block_size=1 << 24)
            co = pc.ConvertOptions(include_columns=["price", "transact_time", "is_buyer_maker"],
                                   column_types={"price": "float64", "transact_time": "int64", "is_buyer_maker": "bool"})
            tb = pc.read_csv(f, read_options=ro, convert_options=co)
    t = tb["transact_time"].to_numpy(); p = tb["price"].to_numpy(); ibm = tb["is_buyer_maker"].to_numpy(zero_copy_only=False)
    if len(t) and np.any(np.diff(t) < 0):
        o = np.argsort(t, kind="stable"); t, p, ibm = t[o], p[o], ibm[o]
    return t, p, ibm


def extract(t, p, ibm, s_list):
    """Keep only the trades inside the union of each event's windows. Returns {s: (t, p, ibm)}."""
    out = {}
    for s in s_list:
        keep = np.zeros(len(t), bool)
        for a, b in WINDOWS:
            lo, hi = np.searchsorted(t, (s + a) * 1000, "left"), np.searchsorted(t, (s + b) * 1000, "left")
            keep[lo:hi] = True
        out[int(s)] = (t[keep], p[keep], ibm[keep])
    return out


def fill(tr, target_ms, side, wait_ms=WAIT_MS):
    """First trade at or after target_ms on the taker side ('buy' -> ibm False, 'sell' -> ibm True), within wait_ms."""
    t, p, ibm = tr
    i = np.searchsorted(t, target_ms, "left")
    m = ~ibm[i:] if side == "buy" else ibm[i:]
    k = np.flatnonzero(m)
    if len(k) == 0 or t[i + k[0]] > target_ms + wait_ms:
        return np.nan
    return float(p[i + k[0]])


def last_before(tr, target_ms, side=None, inclusive=False, floor_ms=None):
    """Last trade before (or at, if inclusive) target_ms, optionally on one taker side, not earlier than floor_ms."""
    t, p, ibm = tr
    i = np.searchsorted(t, target_ms, "right" if inclusive else "left")
    m = np.ones(i, bool) if side is None else (~ibm[:i] if side == "buy" else ibm[:i])
    k = np.flatnonzero(m)
    if len(k) == 0 or (floor_ms is not None and t[k[-1]] < floor_ms):
        return np.nan
    return float(p[k[-1]])


def first_any(tr, target_ms, wait_ms=WAIT_MS):
    t, p, _ = tr
    i = np.searchsorted(t, target_ms, "left")
    return float(p[i]) if i < len(t) and t[i] <= target_ms + wait_ms else np.nan


def event_result(s, d, p_s, r_s, tr):
    """All entries, the exit, gross per Δ, path, pre arm and the N4-X replication for one event."""
    S = int(s) * 1000
    buy, sell = ("buy", "sell") if d > 0 else ("sell", "buy")
    ex = fill(tr, S + EXIT_S * 1000, sell)
    row = {"exit": ex}
    for D in DELTAS:
        en = fill(tr, S + int(round(D * 1000)), buy)
        row[f"entry_{D}"] = en
        row[f"gross_{D}"] = d * (ex / en - 1) * 1e4
        lp = last_before(tr, S + int(round(D * 1000)), inclusive=True, floor_ms=S - 60_000)
        row[f"path_{D}"] = d * (lp / p_s - 1) * 1e4
        row[f"ntr_{D}"] = int(np.searchsorted(tr[0], S + int(round(D * 1000)), "right") - np.searchsorted(tr[0], S, "left"))
    pre = last_before(tr, S - PRE_S * 1000, side=buy, floor_ms=S - 60_000)
    row["pre_gross"] = d * (ex / pre - 1) * 1e4
    row["pre_funding"] = -d * r_s * 1e4
    e2 = first_any(tr, S + 120_000)
    x2 = last_before(tr, S + 3_600_000, floor_ms=S + 3_300_000)
    row["n4x_g2"] = d * (x2 / e2 - 1) * 1e4
    return row


# ------------------------------------------------------------------ fetch one coin-day (local runner and notebook)
def extract_path(out_dir, sym, day):
    return os.path.join(out_dir, f"{sym}_{day}.npz")


def fetch_coin_day(sym, day, s_list, out_dir, getter=get):
    """Download one aggTrades day, keep only the event windows, write {sym}_{day}.npz (atomic). Returns (MB downloaded, status)."""
    f = extract_path(out_dir, sym, day)
    if os.path.exists(f):
        return 0.0, "cached"
    blob = getter(URL.format(s=sym, d=day))
    if blob is None:
        np.savez_compressed(f, missing=np.array(True)); return 0.0, "missing"
    t, p, ibm = read_agg(blob)
    arrs = {"missing": np.array(False)}
    for s_, (a, b, c) in extract(t, p, ibm, s_list).items():
        arrs[f"{s_}_t"], arrs[f"{s_}_p"], arrs[f"{s_}_i"] = a, b, c
    tmp = f[:-4] + ".tmp.npz"
    np.savez_compressed(tmp, **arrs); os.replace(tmp, f)
    return len(blob) / 1e6, f"{len(t):,} trades"


def event_rows(e, out_dir):
    """Per-event results from the extracts; {} for an event without data."""
    rows, z, zf = [], None, None
    for r in e.itertuples():
        f = extract_path(out_dir, r.sym, r.day)
        if f != zf:
            zf = f; z = np.load(f) if os.path.exists(f) else None
        if z is None or bool(z["missing"]) or f"{r.s}_t" not in z.files:
            rows.append({}); continue
        tr = (z[f"{r.s}_t"], z[f"{r.s}_p"], z[f"{r.s}_i"].astype(bool))
        rows.append(event_result(r.s, r.d, r.p_s, r.r_s, tr))
    return rows


# ------------------------------------------------------------------ scoring (registered cell, information tables)
def _boot(x, cl):
    ok = np.isfinite(x)
    return cluster_boot(x[ok], cl[ok]) if ok.sum() > 1 else dict(mean=np.nan, lo=np.nan, hi=np.nan, p=np.nan, n=int(ok.sum()))


def score_events(e, rows):
    """e: the sample (sym, s, d, r_prev, r_s, interval_h, p_s, g2, year, ...); rows: event_rows output. Returns (table, results)."""
    import pandas as pd
    t = pd.concat([e.reset_index(drop=True), pd.DataFrame(rows)], axis=1)
    for c in ["n4x_g2", "pre_gross", "pre_funding", "exit"] + [f"{k}_{D}" for D in DELTAS for k in ("gross", "path", "ntr", "entry")]:
        if c not in t:
            t[c] = np.nan
    cl = t["s"].to_numpy()
    both = np.isfinite(t["n4x_g2"]) & np.isfinite(t["g2"])
    dev = (t["n4x_g2"] - t["g2"]).abs()[both]
    rep_share = float((dev <= 0.5).mean()) if both.any() else float("nan")
    rep_ok = bool(both.any() and rep_share >= 0.99)
    out = dict(n_events=len(t), no_data=int(t["exit"].isna().sum() if "exit" in t else len(t)),
               replication=dict(n=int(both.sum()), share_within_0p5bp=rep_share, max_dev=float(dev.max()) if both.any() else None,
                                median_dev=float(dev.median()) if both.any() else None, ok=rep_ok))
    cells = {}
    for D in DELTAS:
        g = t[f"gross_{D}"].to_numpy(float); net = g - FEE_BP
        b = _boot(net, cl); x = net[np.isfinite(net)]
        cells[str(D)] = dict(n=int(np.isfinite(net).sum()), gross=float(np.nanmean(g)) if len(x) else np.nan, net=b["mean"], lo=b["lo"], hi=b["hi"],
                             median=float(np.median(x)) if len(x) else np.nan, net_extra=float(np.nanmean(net - EXTRA_BP)) if len(x) else np.nan,
                             path=float(np.nanmean(t[f"path_{D}"])), path_median=float(np.nanmedian(t[f"path_{D}"])),
                             trades_median=float(np.nanmedian(t[f"ntr_{D}"])))
    out["cells"] = cells
    P = f"gross_{PRIMARY}"; net = t[P] - FEE_BP; ok = np.isfinite(net)
    b = _boot(net.to_numpy(float), cl); desc = describe(net[ok].to_numpy(float))
    by_year = {int(y): float((g[P] - FEE_BP).mean()) for y, g in t[ok].groupby("year")}
    terc = pd.qcut(t.loc[ok, "r_prev"].abs(), 3, labels=["low", "mid", "high"]) if ok.sum() >= 3 else None
    by_terc = {str(k): float(v) for k, v in net[ok].groupby(terc, observed=True).mean().items()} if terc is not None else {}
    by_int = {int(k): [int(len(g)), float((g[P] - FEE_BP).mean())] for k, g in t[ok].groupby("interval_h")}
    by_side = {("long (r>0)" if k > 0 else "short (r<0)"): [int(len(g)), float((g[P] - FEE_BP).mean())] for k, g in t[ok].groupby("d")}
    pre = t["pre_gross"] + t["pre_funding"] - FEE_BP
    out["primary"] = dict(delta=PRIMARY, boot=b, desc=desc, by_year=by_year, by_abs_rate_tercile=by_terc, by_interval=by_int, by_side=by_side,
                          pre_arm=dict(n=int(np.isfinite(pre).sum()), net=float(np.nanmean(pre)) if np.isfinite(pre).any() else np.nan,
                                       gross=float(np.nanmean(t["pre_gross"])) if np.isfinite(t["pre_gross"]).any() else np.nan,
                                       funding=float(np.nanmean(t["pre_funding"])) if np.isfinite(t["pre_funding"]).any() else np.nan))
    crit = {"net mean > 0, CI lo > 0": bool(b["mean"] > 0 and b["lo"] > 0), "median > 0": bool(desc.get("median", -1) > 0),
            "2025 > 0 and 2026 > 0": bool(by_year.get(2025, -1) > 0 and by_year.get(2026, -1) > 0), "delta 1 s mean > 0": bool(cells["1"]["net"] > 0),
            "n >= 1800": bool(int(ok.sum()) >= 1800)}
    out["criteria"] = crit
    out["verdict"] = ("PASS" if all(crit.values()) else "FAIL") if rep_ok else "STOPPED (replication check failed)"
    return t, out


def report(out):
    r = out["replication"]; L = []
    L.append(f"N4-S: {out['n_events']} events, {out['no_data']} without an exit fill / data | replication of N4-X g2: "
             f"{r['share_within_0p5bp'] * 100:.1f} % within 0.5 bp of {r['n']} (median dev {r['median_dev']}, max {r['max_dev']}) -> {'ok' if r['ok'] else 'FAIL'}")
    L.append("  delta     n    path mean / median   trades   gross   net(9bp)  CI                 median   net(+10bp)")
    for D, c in out["cells"].items():
        L.append(f"  {D:>5}s {c['n']:5d}  {c['path']:+7.1f} {c['path_median']:+7.1f}     {c['trades_median']:6.0f}  {c['gross']:+7.1f}  {c['net']:+7.1f}"
                 f"  [{c['lo']:+6.1f}, {c['hi']:+6.1f}]  {c['median']:+7.1f}  {c['net_extra']:+7.1f}")
    p = out["primary"]; b, d = p["boot"], p["desc"]
    L.append(f"primary delta {p['delta']} s: mean {b['mean']:+.2f} CI [{b['lo']:+.2f}, {b['hi']:+.2f}] p {b.get('p', float('nan')):.3f} | median {d.get('median', float('nan')):+.2f}"
             f" trimmed {d.get('trimmed', float('nan')):+.2f} worst {d.get('worst', float('nan')):+.0f} worst 1 % {d.get('worst1pct_mean', float('nan')):+.0f}")
    L.append(f"  by year {p['by_year']} | |r_prev| tercile {p['by_abs_rate_tercile']}")
    L.append(f"  by interval (n, net) {p['by_interval']} | by side {p['by_side']}")
    L.append(f"  pre-positioned (enter before s - 1 s, pay the settled rate): {p['pre_arm']}")
    L.append(f"criteria: {out['criteria']}")
    L.append(f"VERDICT: {out['verdict']}")
    return "\n".join(L)
