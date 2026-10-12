"""N4 funding-settlement event study — library (pre-registered in next_signal_ideas.md, Round 6 / N4).

Conventions
  s                a funding settlement time (archive calc_time rounded to the hour), seconds UTC
  C(t)             1 h close at instant t = close of the bar opening t - 1 h (raw, not forward-filled)
  r_prev           the coin's previous settlement rate (known before s) -> the signal
  r_s              the rate settled at s (finalised only at s) -> information arm only
  q                -sign(signal rate): the receiving side's direction
  PRE              q * (C(s) / C(s - 1h) - 1); exit treated as before the settlement, no funding credited
  POST             -q * (C(s + 1h) / C(s) - 1)
  placebo PRE      the same signed trade over [s - 3h, s - 2h]; placebo POST over [s + 2h, s + 3h]
  membership       point-in-time top-`top` universe (R4 eligibility, 30-day volume) of the day starting at or
                   before s - 3h, i.e. known before the earliest entry of the event and its placebo
  net              gross - COST_RT_BP (VIP0 + BNB taker round trip)
"""
import numpy as np, pandas as pd

HOUR, DAY = 3600, 86400
COST_RT_BP = 9.0
EXTREME = 0.0010                                  # |rate| >= 0.10 % per settlement
TOP_EV = 100
B_BOOT = 2000


# ------------------------------------------------------------------ events
def settlement_events(p, U, days, extreme=EXTREME, sign_by="prev"):
    """One row per extreme settlement of a universe coin. sign_by 'prev' (registered) or 'settled' (info)."""
    grid, syms = p["grid"], p["syms"]
    member = np.zeros((len(days), len(syms)), bool)
    rows = np.flatnonzero(U[:, 0] >= 0)
    member[np.repeat(rows, U.shape[1]), U[rows].ravel()] = True
    out = []
    for j, s in enumerate(syms):
        ft, fr = p["fund"][s]
        if len(ft) < 2:
            continue
        st = (ft + HOUR // 2) // HOUR * HOUR                              # calc_time -> settlement hour
        o = np.argsort(st, kind="stable"); st, fr = st[o], fr[o]
        keep = np.r_[True, np.diff(st) > 0]; st, fr = st[keep], fr[keep]
        prev, cur, sp = fr[:-1], fr[1:], st[1:]
        interval = (st[1:] - st[:-1]) // HOUR
        sig = prev if sign_by == "prev" else cur
        ext = np.abs(sig) >= extreme - 1e-12
        di = np.searchsorted(days, sp - 3 * HOUR, side="right") - 1         # day start <= s - 3h
        ok = ext & (di >= 0) & (di < len(days))
        ok[ok] = member[di[ok], j]
        if not ok.any():
            continue
        out.append(pd.DataFrame({"j": j, "sym": s, "s": sp[ok], "r_prev": prev[ok], "r_s": cur[ok],
                                 "interval_h": interval[ok], "q": -np.sign(sig[ok])}))
    if not out:
        return pd.DataFrame(columns=["j", "sym", "s", "r_prev", "r_s", "interval_h", "q"])
    return pd.concat(out, ignore_index=True).sort_values(["s", "j"], ignore_index=True)


def close_at(p, j, t):
    """C(t) for coin index j at instants t (NaN outside the grid or when the bar is missing)."""
    grid, close = p["grid"], p["close"]
    i = (np.asarray(t) - HOUR - grid[0]) // HOUR
    ok = (i >= 0) & (i < len(grid))
    out = np.full(len(i), np.nan)
    out[ok] = close[i[ok], np.asarray(j)[ok]]
    return out


def event_returns(ev, p):
    """Adds raw bar returns and signed gross / net / placebo columns (bp). Drops events with a missing bar."""
    j, s, q = ev["j"].to_numpy(), ev["s"].to_numpy(np.int64), ev["q"].to_numpy(float)
    C = {k: close_at(p, j, s + k * HOUR) for k in (-3, -2, -1, 0, 1, 2, 3)}
    raw = lambda a, b: (C[b] / C[a] - 1) * 1e4
    e = ev.copy()
    e["raw_pre"], e["raw_post"] = raw(-1, 0), raw(0, 1)
    e["raw_plc_pre"], e["raw_plc_post"] = raw(-3, -2), raw(2, 3)
    e["pre"], e["post"] = q * e.raw_pre, -q * e.raw_post
    e["plc_pre"], e["plc_post"] = q * e.raw_plc_pre, -q * e.raw_plc_post
    e["pre_net"], e["post_net"] = e.pre - COST_RT_BP, e.post - COST_RT_BP
    e["year"] = pd.to_datetime(e.s, unit="s").dt.year
    full = np.isfinite(e[["raw_pre", "raw_post", "raw_plc_pre", "raw_plc_post"]].to_numpy()).all(1)
    return e[full].reset_index(drop=True), int((~full).sum())


# ------------------------------------------------------------------ statistics
def cluster_boot(x, cl, B=B_BOOT, seed=0, chunk=100):
    """Mean of x with a bootstrap that resamples whole clusters. Returns mean, 2.5 / 97.5 %, two-sided p."""
    x = np.asarray(x, float)
    _, inv = np.unique(np.asarray(cl), return_inverse=True)
    S, C = np.bincount(inv, weights=x), np.bincount(inv).astype(float)
    K = len(S); rng = np.random.default_rng(seed); m = []
    for b in range(0, B, chunk):
        idx = rng.integers(0, K, (min(chunk, B - b), K))
        m.append(S[idx].sum(1) / C[idx].sum(1))
    m = np.concatenate(m)
    p = min(1.0, 2 * min((m <= 0).mean(), (m >= 0).mean()))
    return dict(mean=float(x.mean()), lo=float(np.percentile(m, 2.5)), hi=float(np.percentile(m, 97.5)),
                p=max(p, 1.0 / B), n=len(x), clusters=K)


def describe(x):
    x = np.sort(np.asarray(x, float)); n = len(x)
    if n == 0:
        return dict(n=0)
    c = int(np.floor(0.01 * n)); k = max(1, c)
    return dict(n=n, mean=float(x.mean()), median=float(np.median(x)), trimmed=float(x[c:n - c].mean()),
                worst=float(x[0]), q01=float(np.percentile(x, 1)), worst1pct_mean=float(x[:k].mean()),
                best=float(x[-1]))


def arm_stats(e, arm, seed=0):
    """Registered criteria for one arm ('pre' or 'post') of an event table from event_returns."""
    net, plc = e[f"{arm}_net"].to_numpy(), e[f"plc_{arm}"].to_numpy()
    diff = e[arm].to_numpy() - plc
    bn, bd = cluster_boot(net, e.s, seed=seed), cluster_boot(diff, e.s, seed=seed + 1)
    yr = e.groupby("year")[f"{arm}_net"].agg(["mean", "size"])
    pos_y, n_y = int((yr["mean"] > 0).sum()), len(yr)
    sgn = e.q < 0                                                         # q = -sign(rate): q < 0 <=> rate > 0
    legs = {"rate>0": float(e.loc[sgn, f"{arm}_net"].mean()) if sgn.any() else np.nan,
            "rate<0": float(e.loc[~sgn, f"{arm}_net"].mean()) if (~sgn).any() else np.nan}
    crit = {"net mean > 0": bn["mean"] > 0, "net CI lo > 0": bn["lo"] > 0,
            "event - placebo > 0": bd["mean"] > 0, "event - placebo CI lo > 0": bd["lo"] > 0,
            "years positive >= 4/6": n_y > 0 and pos_y / n_y >= 4 / 6 - 1e-9,
            "both sign legs >= 0": all(np.isfinite(v) and v >= 0 for v in legs.values()),
            "n >= 300": len(e) >= 300}
    return dict(arm=arm, boot_net=bn, boot_diff=bd, desc_net=describe(net), desc_placebo=describe(plc - COST_RT_BP),
                years={int(y): dict(mean=float(r["mean"]), n=int(r["size"])) for y, r in yr.iterrows()},
                pos_years=pos_y, n_years=n_y, sign_legs=legs, criteria=crit,
                arm_p=max(bn["p"], bd["p"]))


def verdict(e, seed=0):
    """Both arms with Holm across them on the arm p-value (max of the net and event-minus-placebo bootstrap p)."""
    from xsec import holm
    arms = [arm_stats(e, a, seed) for a in ("pre", "post")]
    adj = holm([a["arm_p"] for a in arms])
    for a, pa in zip(arms, adj):
        a["holm_p"] = float(pa)
        a["criteria"]["Holm p < 0.05"] = pa < 0.05
        a["pass"] = bool(all(a["criteria"].values()))
    return arms


def split_table(e, arm, by):
    g = e.groupby(by)
    return pd.DataFrame({"n": g.size(), "gross": g[arm].mean(), "net": g[f"{arm}_net"].mean(),
                         "placebo": g[f"plc_{arm}"].mean(), "diff": g[arm].mean() - g[f"plc_{arm}"].mean()})


def build_events(p, top=TOP_EV, sign_by="prev", extreme=EXTREME):
    """Hourly panels -> R4 eligibility -> point-in-time universe -> extreme settlement events with returns."""
    import xsec as X
    dp = X.daily_panels(p); fi = X.factor_inputs(p, dp)
    U = X.universe(fi["elig"], fi["qv30"], top=top)
    ev = settlement_events(p, U, dp["days"], extreme=extreme, sign_by=sign_by)
    e, dropped = event_returns(ev, p)
    return e, dropped, U, dp["days"]
