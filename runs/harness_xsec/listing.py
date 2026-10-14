"""N5 drift after a new token's Binance perp listing — library (pre-registered in next_signal_ideas.md, Round 7 / N5).

Hourly panel p as xsec.load_panels: grid of 1 h bar opens, close / quote volume (T, N), per-symbol funding events.
  L             listing instant: open of the first 1 h bar with a close in the archive
  class         'pre2020' (L before 2020-02-01: existed when the archive starts), 'tradfi' (exchangeInfo underlyingSubType has TradFi
                or contractType TRADIFI_PERPETUAL), 'index' (underlyingType INDEX), 'relisted' (its base without a 1000 / 1M multiplier
                prefix was listed earlier under another symbol; the registered counts drop BOBUSDT this way), else 'new' (no Binance spot
                pair, or the first spot month is <= NEW_MAX_MONTHS before L's month) or 'seasoned'
  entry         first 00:00 UTC >= L + gap_h (registered 48 h; information arm 24 h); the 1 h lag row moves entry and exit by + 1 h
  liquidity     perp quote volume of the 24 1 h bars before entry >= MIN_QV
  P(t)          close of the 1 h bar that ends at t (opens t - 1 h), forward-filled
  end           min(exit, death); death = the instant after the coin's last traded bar when its file ends in zero-volume bars
                (xfund.death_hour) or the instant after its last bar when the file stops early; the BTC leg closes at the same instant
  event P&L     buy-and-hold per unit entry notional, funding settlements s in (entry, end] on the position's value at s:
                  short coin  -(P(end) / P(entry) - 1) + sum r_s * P(s) / P(entry)
                  long BTC     (B(end) / B(entry) - 1) - sum rb_s * B(s) / B(entry)
                  net = short + long - COST_PAIR (4 sides x 4.5 bp); blind = short - COST_BLIND (2 sides x 4.5 bp)
  control       same dates: equal-weight short of the point-in-time top 100 (R4 eligibility, 30-day quote volume) aged >= 365 days,
                crypto only, ex-BTC, each coin buy-and-hold with its own death closure, plus the event's long BTC leg and the same 18 bp;
                defined when >= CTRL_MIN_N coins qualify, else NaN (the event is left out of the control criterion, counted)
  portfolio     SLOT of capital on each leg per event, at most MAX_OPEN open (later entries skipped, counted), marked daily on constant
                notional: day d earns SLOT * (-(c1 / c0 - 1) + Fc + (b1 / b0 - 1) - Fb) over [max(d, entry), min(d + 1 day, end)],
                funding unweighted; SLOT * 9 bp on the first held day and again on the last; idle capital earns 0
  inference     percentile bootstrap of the event mean, clustered by the entry ISO week
"""
import json, os, re
import numpy as np, pandas as pd
import xsec as X
import xfund as XF

DAY, HOUR = 86400, 3600
ENTRY_GAP_H, GAP_INFO_H = 48, 24
H_PRIMARY, H_INFO = 14, (7, 28)
MIN_QV, QV_INFO = 5e6, (2e6, 10e6)
NEW_MAX_MONTHS = 3
COST_PAIR = 4 * X.COST_BP / 1e4
COST_BLIND = 2 * X.COST_BP / 1e4
SLOT, MAX_OPEN = 0.02, 25
CTRL_TOP, CTRL_MIN_AGE_D, CTRL_MIN_N = 100, 365, 10
TRIM = 0.01
N_BOOT = 10000
MIN_YEAR_N = 20
MIN_N = 300
RF_PCT = 4.5
T_ARCHIVE = int(pd.Timestamp("2020-02-01", tz="UTC").timestamp())
META = os.path.join(os.path.dirname(os.path.abspath(__file__)), "n5_meta")


# ------------------------------------------------------------------ metadata and the listing table
def load_meta(meta_dir=META):
    """spot first month per symbol and the exchangeInfo snapshot keyed by symbol."""
    spot = json.load(open(os.path.join(meta_dir, "spot_first_month.json")))
    ei = json.load(open(os.path.join(meta_dir, "exchangeinfo_2026-10-12.json")))
    ei = {x["symbol"]: x for x in (ei["symbols"] if isinstance(ei, dict) else ei)}
    return dict(spot=spot, ei=ei)


def _months(ts):
    t = pd.to_datetime(ts, unit="s", utc=True)
    return t.year * 12 + t.month - 1


def classify(sym, L, meta):
    x = meta["ei"].get(sym, {})
    if L < T_ARCHIVE:
        return "pre2020"
    if "TradFi" in (x.get("underlyingSubType") or []) or x.get("contractType") == "TRADIFI_PERPETUAL":
        return "tradfi"
    if x.get("underlyingType") == "INDEX":
        return "index"
    m = meta["spot"].get(sym)
    if m is None:
        return "new"
    y, mo = (int(v) for v in m.split("-"))
    return "new" if _months(L) - (y * 12 + mo - 1) <= NEW_MAX_MONTHS else "seasoned"


def subtype(sym, meta):
    x = meta["ei"].get(sym)
    if x is None:
        return "delisted"
    s = [v for v in (x.get("underlyingSubType") or []) if v != "Crypto"]
    return s[0] if s else "none"


def base(sym):
    return re.sub(r"^(1000000|100000|10000|1000|1M)", "", sym[:-4])


def listing_table(p, meta):
    """One row per archive symbol with a listing instant: sym, j, L, class, subtype (in listing order)."""
    rows = []
    for j, s in enumerate(p["syms"]):
        ok = np.flatnonzero(np.isfinite(p["close"][:, j]))
        if not len(ok):
            continue
        L = int(p["grid"][ok[0]])
        rows.append(dict(sym=s, j=j, L=L, cls=classify(s, L, meta), sub=subtype(s, meta)))
    tab = pd.DataFrame(rows).sort_values(["L", "sym"], kind="stable").reset_index(drop=True)
    seen = set()
    for i, r in tab.iterrows():
        b = base(r.sym)
        if b in seen and r.cls in ("new", "seasoned"):
            tab.at[i, "cls"] = "relisted"
        seen.add(b)
    return tab


# ------------------------------------------------------------------ price / funding access
def prep(p):
    """Forward-filled closes, death instants and cumulative funding per symbol (everything the P&L functions read)."""
    grid, close, qv = p["grid"], p["close"], p["qv"]
    T, N = close.shape
    cf = np.empty_like(close)
    death = np.full(N, np.iinfo(np.int64).max, np.int64)
    fund = []
    for j, s in enumerate(p["syms"]):
        cf[:, j] = X._ffill_col(close[:, j], T)
        pres = np.flatnonzero(np.isfinite(close[:, j]))
        if len(pres):
            h = XF.death_hour(qv[:, j], close[:, j])
            if h is None and pres[-1] < T - 1:
                h = int(pres[-1] + 1)
            if h is not None:
                death[j] = int(grid[0] + h * HOUR)                     # the close instant of the last traded bar
        ft, fr = p["fund"][s]
        i = (ft - grid[0]) // HOUR - 1
        ps = np.where((i >= 0) & (i < T), cf[np.clip(i, 0, T - 1), j], np.nan)
        fund.append((ft, np.r_[0.0, np.cumsum(fr)], np.r_[0.0, np.cumsum(np.nan_to_num(fr * ps))]))
    cq = np.vstack([np.zeros((1, N)), np.cumsum(np.nan_to_num(qv.astype(np.float64)), 0)])
    return dict(grid=grid, cf=cf, death=death, fund=fund, cq=cq, syms=p["syms"])


def price(pp, j, t):
    """P(t): close of the bar ending at t, forward-filled; NaN before the first close or outside the grid."""
    i = (np.asarray(t, np.int64) - pp["grid"][0]) // HOUR - 1
    T = len(pp["grid"])
    ok = (i >= 0) & (i < T)
    return np.where(ok, pp["cf"][np.clip(i, 0, T - 1), j], np.nan)


def fund_sum(pp, j, t0, t1, weighted):
    """Sum of settlement rates in (t0, t1]; weighted: each rate times the coin's price at the settlement."""
    ft, cu, cw = pp["fund"][j]
    a, b = np.searchsorted(ft, t0, side="right"), np.searchsorted(ft, t1, side="right")
    c = cw if weighted else cu
    return c[b] - c[a]


def qv_before(pp, j, t, hours=24):
    """Quote volume of the `hours` 1 h bars before instant t."""
    i = int((t - pp["grid"][0]) // HOUR)
    T = len(pp["grid"])
    if i - hours < 0 or i > T:
        return np.nan
    return float(pp["cq"][i, j] - pp["cq"][i - hours, j])


def leg(pp, j, t0, t1):
    """Buy-and-hold long leg over (t0, min(t1, death)]: price return, value-weighted funding paid, end instant."""
    end = min(int(t1), int(pp["death"][j]))
    p0, p1 = price(pp, j, t0), price(pp, j, end)
    if end <= t0 or not (np.isfinite(p0) and np.isfinite(p1)) or p0 <= 0:
        return np.nan, np.nan, end
    return float(p1 / p0 - 1), float(fund_sum(pp, j, t0, end, True) / p0), end


def event_pnl(pp, j, btc, t0, t1):
    """One event: short coin j / long BTC over (t0, t1], both closed at the coin's end. Gross pieces; costs added by the caller."""
    r, f, end = leg(pp, j, t0, t1)
    if not np.isfinite(r):
        return None
    rb, fb, _ = leg(pp, btc, t0, end)
    return dict(short=-r + f, coin_ret=r, coin_fund=f, btc=rb - fb, btc_ret=rb, btc_fund=fb, end=end, died=end < t1)


# ------------------------------------------------------------------ events
def entry_instant(L, gap_h):
    t = L + gap_h * HOUR
    return int(-(-t // DAY) * DAY)


def build_events(pp, table, btc, H, cls="new", gap_h=ENTRY_GAP_H, min_qv=MIN_QV, lag_h=0):
    """Event rows for one construction: entry / exit, liquidity, P&L pieces (gross), net, blind. Decisions use data <= entry only."""
    out = []
    for r in table[table.cls == cls].itertuples():
        te = entry_instant(r.L, gap_h)
        q = qv_before(pp, r.j, te)
        if not (np.isfinite(q) and q >= min_qv):
            continue
        t0, t1 = te + lag_h * HOUR, te + H * DAY + lag_h * HOUR
        if t1 > pp["grid"][-1] + HOUR:
            continue
        e = event_pnl(pp, r.j, btc, t0, t1)
        if e is None:
            continue
        e.update(sym=r.sym, j=r.j, L=r.L, entry=te, exit=te + H * DAY, t0=t0, t1=t1, qv24=q, sub=r.sub,
                 net=e["short"] + e["btc"] - COST_PAIR, blind=e["short"] - COST_BLIND)
        out.append(e)
    ev = pd.DataFrame(out)
    if len(ev):
        ev = ev.sort_values(["entry", "L", "sym"], kind="stable").reset_index(drop=True)
    return ev


def control_sets(p, crypto):
    """(days, sets): per 00:00 day the column indices of the R4-eligible top CTRL_TOP by 30-day quote volume, aged >= CTRL_MIN_AGE_D,
    crypto only (bool mask over symbols), BTC excluded by the caller."""
    dp = X.daily_panels(p, 0)
    fi = X.factor_inputs(p, dp)
    days = dp["days"]
    first = np.array([p["grid"][np.flatnonzero(np.isfinite(p["close"][:, j]))[0]] if np.isfinite(p["close"][:, j]).any()
                      else np.iinfo(np.int64).max for j in range(len(p["syms"]))], np.int64)
    sets = []
    for d in range(len(days)):
        ok = np.flatnonzero(fi["elig"][d] & crypto)
        top = ok[np.argsort(-fi["qv30"][d, ok], kind="stable")[:CTRL_TOP]] if len(ok) else ok
        sets.append(top[first[top] <= days[d] - CTRL_MIN_AGE_D * DAY])
    return days, sets


def add_control(pp, ev, btc, ctrl_days, ctrl_sets):
    """Control net per event: EW short of the seasoned set at the entry day, buy-and-hold with death closure, long BTC, 18 bp."""
    c_short, c_n, c_net = [], [], []
    for e in ev.itertuples():
        d = int(np.searchsorted(ctrl_days, e.entry))
        s = [j for j in (ctrl_sets[d] if d < len(ctrl_days) and ctrl_days[d] == e.entry else []) if j != btc]
        vals = []
        for j in s:
            r, f, _ = leg(pp, j, e.t0, e.t1)
            if np.isfinite(r):
                vals.append(-r + f)
        rb, fb, _ = leg(pp, btc, e.t0, e.t1)
        ok = len(vals) >= CTRL_MIN_N
        c_short.append(float(np.mean(vals)) if ok else np.nan)
        c_n.append(len(vals))
        c_net.append(float(np.mean(vals)) + rb - fb - COST_PAIR if ok else np.nan)
    return ev.assign(ctrl_short=c_short, ctrl_n=c_n, ctrl_net=c_net, minus_ctrl=ev["net"].to_numpy() - np.array(c_net))


# ------------------------------------------------------------------ statistics
def iso_week(ts):
    t = pd.to_datetime(np.asarray(ts), unit="s", utc=True)
    iso = t.isocalendar()
    return (iso["year"].to_numpy() * 100 + iso["week"].to_numpy()).astype(np.int64)


def cluster_ci(x, cl, n_boot=N_BOOT, seed=0, alpha=0.05):
    """Percentile CI of the mean, resampling whole clusters with replacement."""
    x = np.asarray(x, float); cl = np.asarray(cl)
    ok = np.isfinite(x); x, cl = x[ok], cl[ok]
    if not len(x):
        return (np.nan, np.nan)
    u, inv = np.unique(cl, return_inverse=True)
    s = np.bincount(inv, weights=x, minlength=len(u)); n = np.bincount(inv, minlength=len(u)).astype(float)
    rng = np.random.default_rng(seed)
    pick = rng.integers(0, len(u), (n_boot, len(u)))
    m = s[pick].sum(1) / n[pick].sum(1)
    lo, hi = np.percentile(m, [100 * alpha / 2, 100 * (1 - alpha / 2)])
    return (float(lo), float(hi))


def describe(x, cl=None):
    """Mean, median, 1 % trimmed mean, worst, mean of the worst 1 %, n, and the clustered CI (percent units)."""
    x = np.asarray(x, float); x = x[np.isfinite(x)]
    n = len(x)
    if not n:
        return dict(n=0)
    s = np.sort(x); c = int(np.floor(TRIM * n)); k1 = max(1, c)
    out = dict(n=n, mean=float(x.mean() * 100), median=float(np.median(x) * 100), trimmed=float(s[c:n - c].mean() * 100),
               worst=float(s[0] * 100), worst1=float(s[:k1].mean() * 100), best=float(s[-1] * 100), pos_share=float((x > 0).mean()))
    return out


def stats_with_ci(x, ts):
    x = np.asarray(x, float)
    d = describe(x)
    lo, hi = cluster_ci(x, iso_week(ts))
    d["ci"] = [lo * 100, hi * 100]
    return d


# ------------------------------------------------------------------ portfolio
def portfolio(pp, ev, btc, days, max_open=MAX_OPEN, slot=SLOT):
    """Daily net P&L on capital (len(days)) from the events in entry order, at most max_open open. Returns (net, taken mask, open count)."""
    D = len(days)
    net = np.zeros(D); nopen = np.zeros(D, np.int64)
    taken = np.zeros(len(ev), bool)
    for k, e in enumerate(ev.itertuples()):
        a = int(np.searchsorted(days, e.t0 - e.t0 % DAY))
        end = int(e.end)
        b = int(np.searchsorted(days, end - 1 - (end - 1) % DAY))       # day holding the last instant before end
        if a >= D:
            continue
        if nopen[a] >= max_open:
            continue
        taken[k] = True
        for d in range(a, min(b, D - 1) + 1):
            s0, s1 = max(int(days[d]), int(e.t0)), min(int(days[d]) + DAY, end)
            if s1 <= s0:
                continue
            c0, c1 = price(pp, e.j, s0), price(pp, e.j, s1)
            b0, b1 = price(pp, btc, s0), price(pp, btc, s1)
            v = -(c1 / c0 - 1) + fund_sum(pp, e.j, s0, s1, False) + (b1 / b0 - 1) - fund_sum(pp, btc, s0, s1, False)
            net[d] += slot * float(np.nan_to_num(v))
            nopen[d] += 1
        net[a] -= slot * 2 * X.COST_BP / 1e4
        net[min(b, D - 1)] -= slot * 2 * X.COST_BP / 1e4
    return net, taken, nopen


def portfolio_stats(net, days, idx):
    from carry import summarize
    s = summarize(net, days, idx)
    s["worst_day_pct"] = float(net[idx].min() * 100)
    return s


# ------------------------------------------------------------------ verdict
def by_year(ev, col="net"):
    y = pd.to_datetime(ev["entry"], unit="s", utc=True).dt.year
    g = ev.assign(y=y.to_numpy()).groupby("y")[col]
    return {int(k): dict(n=int(v.notna().sum()), mean=float(v.mean() * 100), median=float(v.median() * 100)) for k, v in g}


def verdict(prim, ctrl, years, lag_mean, port):
    """Registered PASS criteria (all required) for the H = 14 cell."""
    big = {y: v for y, v in years.items() if v["n"] >= MIN_YEAR_N}
    neg = [y for y, v in big.items() if not v["mean"] > 0]
    crit = {
        "1 event net mean > 0, clustered CI lower > 0": prim["mean"] > 0 and prim["ci"][0] > 0,
        "2 median > 0 and trimmed mean > 0": prim["median"] > 0 and prim["trimmed"] > 0,
        "3 event - seasoned control > 0, CI lower > 0": ctrl.get("n", 0) > 0 and ctrl["mean"] > 0 and ctrl["ci"][0] > 0,
        f"4 mean > 0 in every year with >= {MIN_YEAR_N} events except at most one": len(big) > 0 and len(neg) <= 1,
        "5 1 h lag row net mean > 0": lag_mean > 0,
        "6a portfolio annualised net >= 4.5 %": port["ann_pct"] >= RF_PCT,
        "6b portfolio NW t >= 2": port["nw_t"] >= 2,
        "6c worst month > -5 %": port["worst_month_pct"] > -5,
        "6d worst day > -5 %": port["worst_day_pct"] > -5,
        f"7 n >= {MIN_N}": prim["n"] >= MIN_N,
    }
    return ("PASS" if all(crit.values()) else "DEAD"), crit
