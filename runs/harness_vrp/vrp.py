"""V1 — is 30-day forward realised BTC volatility predictably different from Deribit DVOL?  (next_signal_ideas.md, Round 5)

Conventions (day t = 00:00 UTC):
  IV_t       DVOL close of the daily bar of day t - 1 (the value at 00:00 of t), annualised %
  RV_t       100 * sqrt(365 * mean daily variance over days t .. t + 29)  (daily variance = sum of squared 5-min log returns)
  F_t        HAR-RV forecast of RV_t from data before t (expanding OLS refit monthly, purged 30 d), exp(pred + s2 / 2)
  naive_t    100 * sqrt(365 * mean daily variance over days t - 30 .. t - 1)
  P&L        a position entered at t earns pos * (RV_t - IV_t) - f * |pos| in annualised vol points (long vol pos +1 earns RV - IV;
             short vol pos -1 earns IV - RV)
"""
import numpy as np, pandas as pd

ANN, BARS = 365, 288
HORIZON, PURGE = 30, 30


# ------------------------------------------------------------------ realised variance and features
def daily_variance(ts, close):
    """Daily sum of squared 5-minute log returns on the UTC day grid; NaN unless all 288 returns of the day exist."""
    ts = np.asarray(ts, np.int64); close = np.asarray(close, float)
    r = np.diff(np.log(close)); t = ts[1:]
    day = t // 86400
    df = pd.DataFrame({"day": day, "r2": r * r})
    g = df.groupby("day").r2
    v = g.sum(); n = g.count()
    v[n != BARS] = np.nan
    idx = pd.to_datetime(v.index.to_numpy() * 86400, unit="s")
    out = pd.Series(v.to_numpy(), index=idx)
    return out.asfreq("D")


def ann_vol(var):
    return 100.0 * np.sqrt(ANN * var)


def features(var):
    """DataFrame indexed by day t: HAR inputs (known at t), forward target, naive forecast."""
    v = var.copy()
    f = pd.DataFrame(index=v.index)
    f["h1"] = np.log(ann_vol(v.shift(1)))
    f["h5"] = np.log(ann_vol(v.rolling(5).mean().shift(1)))
    f["h22"] = np.log(ann_vol(v.rolling(22).mean().shift(1)))
    f["rv"] = ann_vol(v.rolling(HORIZON).mean().shift(-(HORIZON - 1)))
    f["naive"] = ann_vol(v.rolling(HORIZON).mean().shift(1))
    return f


def har_forecast(f, first_fit="2020-09-01", min_obs=250):
    """Expanding-window HAR on log RV, refit on the first day of each month with samples whose forward window ended before it."""
    X = np.column_stack([np.ones(len(f)), f.h1, f.h5, f.h22]); y = np.log(f.rv.to_numpy())
    idx = f.index; fc = np.full(len(f), np.nan)
    months = pd.date_range(pd.Timestamp(first_fit), idx[-1] + pd.offsets.MonthBegin(1), freq="MS")
    for k, m in enumerate(months):
        fit_pos = idx.searchsorted(m)
        if fit_pos >= len(idx):
            break
        train = np.arange(0, max(fit_pos - PURGE + 1, 0))                    # sample s needs s + 29 <= fit day - 1  ->  s <= fit - 30
        ok = train[np.isfinite(X[train]).all(1) & np.isfinite(y[train])]
        if len(ok) < min_obs:
            continue
        b, *_ = np.linalg.lstsq(X[ok], y[ok], rcond=None)
        s2 = float(np.mean((y[ok] - X[ok] @ b) ** 2))
        nxt = idx.searchsorted(months[k + 1]) if k + 1 < len(months) else len(idx)
        seg = np.arange(fit_pos, min(nxt, len(idx)))
        fc[seg] = np.exp(X[seg] @ b + 0.5 * s2)
    return pd.Series(fc, index=idx)


def align_iv(dvol_ts, dvol_close, index):
    """IV_t = close of the DVOL bar of day t - 1 on the day grid `index`."""
    s = pd.Series(np.asarray(dvol_close, float), index=pd.to_datetime(np.asarray(dvol_ts, np.int64), unit="s"))
    return s.shift(1, freq="D").reindex(index)


# ------------------------------------------------------------------ statistics
def ols_nw(y, X, lags=30):
    """OLS with Newey-West (Bartlett) standard errors; returns coef, se, t."""
    y = np.asarray(y, float); X = np.asarray(X, float); n, k = X.shape
    XtXi = np.linalg.inv(X.T @ X); b = XtXi @ X.T @ y; e = y - X @ b
    Xe = X * e[:, None]; S = Xe.T @ Xe
    for l in range(1, lags + 1):
        G = Xe[l:].T @ Xe[:-l]; S += (1 - l / (lags + 1)) * (G + G.T)
    V = XtXi @ S @ XtXi
    se = np.sqrt(np.diag(V)); return b, se, b / se


def nw_t_mean(x, lags=30):
    b, se, t = ols_nw(x, np.ones((len(x), 1)), lags); return float(b[0]), float(t[0])


def mbb_ci(x, block=30, B=2000, seed=0, stat=np.mean):
    """Moving-block bootstrap 95 % CI of stat(x)."""
    x = np.asarray(x, float); n = len(x); rng = np.random.default_rng(seed)
    nb = int(np.ceil(n / block)); out = np.empty(B)
    for i in range(B):
        st = rng.integers(0, n - block + 1, nb)
        out[i] = stat(np.concatenate([x[s:s + block] for s in st])[:n])
    return [float(np.percentile(out, 2.5)), float(np.percentile(out, 97.5))]


# ------------------------------------------------------------------ the tests
def arm_pnl(pos, iv, rv, friction):
    return pos * (rv - iv) - friction * np.abs(pos)


def analyse(dates, iv, rv, F, naive, frictions=(0.0, 1.0, 2.0, 3.0), seed=0, boot=2000):
    """All P0 / P1 / P2 numbers on aligned arrays (finite rows only are used)."""
    dates = pd.DatetimeIndex(dates)
    ok = np.isfinite(iv) & np.isfinite(rv) & np.isfinite(F) & np.isfinite(naive) & (iv > 0) & (rv > 0)
    d, iv, rv, F, nv = dates[ok], iv[ok], rv[ok], F[ok], naive[ok]; n = len(iv)
    out = dict(n=int(n), first=str(d[0].date()), last=str(d[-1].date()))
    # P0 premium
    prem = iv - rv; m, t = nw_t_mean(prem)
    off = prem[::HORIZON]; out["P0"] = dict(mean_iv=float(iv.mean()), mean_rv=float(rv.mean()), vrp_mean=m, vrp_nw_t=t,
                                           vrp_median=float(np.median(prem)), frac_positive=float((prem > 0).mean()),
                                           nonoverlap_n=len(off), nonoverlap_mean=float(off.mean()),
                                           nonoverlap_t=float(off.mean() / (off.std(ddof=1) / np.sqrt(len(off)))),
                                           worst_1pct=float(np.percentile(prem, 1)))
    # P1 log-ratio regression
    for name, fc in (("har", F), ("naive", nv)):
        yv = np.log(rv / iv); xv = np.log(fc / iv)
        b, se, tt = ols_nw(yv, np.column_stack([np.ones(n), xv]), 30)
        out[f"P1_{name}"] = dict(intercept=float(b[0]), slope=float(b[1]), slope_se=float(se[1]), slope_nw_t=float(tt[1]),
                                 corr=float(np.corrcoef(xv, yv)[0, 1]), x_sd=float(xv.std()), pass_=bool(b[1] > 0 and tt[1] >= 2))
    # P2 arms
    years = d.year.to_numpy(); uy = sorted(set(years))
    pos = dict(veto_short=np.where(F < iv, -1.0, 0.0), long_timed=np.where(F > iv, 1.0, 0.0), always_short=-np.ones(n),
               veto_short_naive=np.where(nv < iv, -1.0, 0.0))
    arms = {}
    for name, p in pos.items():
        row = {}
        for f in frictions:
            x = arm_pnl(p, iv, rv, f)
            row[f"f{f:g}"] = dict(mean=float(x.mean()), nw_t=nw_t_mean(x)[1])
        x2 = arm_pnl(p, iv, rv, 2.0); act = p != 0
        row.update(active_pct=float(act.mean() * 100), mean_active_f2=float(x2[act].mean()) if act.any() else float("nan"),
                   median_active_f2=float(np.median(x2[act])) if act.any() else float("nan"),
                   worst_1pct_f2=float(np.percentile(x2, 1)), worst=float(x2.min()),
                   years_f2={int(y): float(x2[years == y].mean()) for y in uy},
                   ci_f2=mbb_ci(x2, 30, boot, seed))
        # entry one day later: decided at t, executed at t + 1
        x2l = np.full(n, np.nan); x2l[:-1] = (p[:-1] * (rv[1:] - iv[1:]) - 2.0 * np.abs(p[:-1])); x2l = x2l[~np.isnan(x2l)]
        row["lag1_f2"] = float(x2l.mean())
        arms[name] = row
    out["P2"] = arms
    v = arms["veto_short"]; yrs = list(v["years_f2"].values())
    need = int(np.ceil(0.75 * len(yrs)))
    diff = arm_pnl(pos["veto_short"], iv, rv, 2.0) - arm_pnl(pos["always_short"], iv, rv, 2.0)
    out["veto_minus_always_f2"] = dict(mean=float(diff.mean()), ci=mbb_ci(diff, 30, boot, seed + 1))
    out["criteria"] = dict(
        P1=dict(ok=out["P1_har"]["pass_"], detail=f"slope {out['P1_har']['slope']:+.3f}, NW t {out['P1_har']['slope_nw_t']:.2f}"),
        P2=dict(ok=bool(v["f2"]["mean"] > 0 and v["ci_f2"][0] > 0 and sum(y > 0 for y in yrs) >= need),
                detail=f"veto-short at 2 vol pts: mean {v['f2']['mean']:+.2f}, CI {np.round(v['ci_f2'], 2).tolist()}, years positive {sum(y > 0 for y in yrs)}/{len(yrs)} (need {need})"))
    return out


def run(kl_ts, kl_close, dvol_ts, dvol_close, seed=0, boot=2000):
    var = daily_variance(kl_ts, kl_close)
    f = features(var)
    fc = har_forecast(f)
    iv = align_iv(dvol_ts, dvol_close, f.index)
    res = analyse(f.index, iv.to_numpy(), f.rv.to_numpy(), fc.to_numpy(), f.naive.to_numpy(), seed=seed, boot=boot)
    return res
