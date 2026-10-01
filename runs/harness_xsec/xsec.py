"""R4 cross-sectional perp factor backtest — library (pre-registered in next_signal_ideas.md, R4).

Pipeline: hourly panels -> daily 00:00 UTC panels -> point-in-time top-40 universe -> 6 factors ->
quintile long/short, H staggered books -> daily net P&L (price + funding - costs) -> metrics / null.

Conventions
  day d            the instant 00:00 UTC of day d; everything known at d uses data up to that instant
  P0[d]            last 1 h close at or before d (bar opening d - 1 h), forward-filled <= 24 h
  P1[d]            the same one hour later (1 h entry-lag control)
  TR[d, i]         simple return of coin i from d to d + 1 minus funding paid by a long in (d, d + 1]
                   (a short earns -TR). A coin that delists inside the day is closed at its last close;
                   after that its return is 0 (position gone).
  cohort s         weights formed at day s, held for days s .. s + H - 1; the book on day t is the mean of
                   the H cohorts formed at t - H + 1 .. t (Jegadeesh-Titman staggering, no overlap in P&L)
  turnover[t]      sum_i |w_t - w_{t-1}| of the combined book; cost = COST_BP per unit traded
"""
import glob, os
import numpy as np, pandas as pd

DAY, HOUR = 86400, 3600
TOP_N, MIN_AGE_D, HIST_D, QUANT = 40, 60, 30, 5
COST_BP = 4.5
EXCLUDE_BASES = {"USDC", "BUSD", "TUSD", "FDUSD", "USDP", "DAI", "EUR", "BTCDOM", "DEFI", "USDE", "PYUSD", "AEUR"}
MIN_RVOL_ANN = 0.01
FACTORS = ["MOM28", "MOM7", "REV1", "FUND7", "VSHOCK", "RVOL30"]
HOLDS = [1, 3, 7]
IS_DAYS = 730


# ------------------------------------------------------------------ loading
def load_panels(root, t_start="2020-01-01", t_end="2026-09-01"):
    """Hourly close / quote-volume matrices on one grid + per-symbol funding events."""
    t0 = int(pd.Timestamp(t_start, tz="UTC").timestamp()); t1 = int(pd.Timestamp(t_end, tz="UTC").timestamp())
    grid = np.arange(t0, t1, HOUR, dtype=np.int64); T = len(grid)
    files = sorted(glob.glob(os.path.join(root, "klines", "*.parquet")))
    syms = [os.path.basename(f)[:-8] for f in files]
    syms = [s for s in syms if s[:-4] not in EXCLUDE_BASES]
    N = len(syms)
    close = np.full((T, N), np.nan); qv = np.full((T, N), np.nan, np.float32)
    fund = {}
    for j, s in enumerate(syms):
        k = pd.read_parquet(os.path.join(root, "klines", f"{s}.parquet"), columns=["ts", "close", "quote_volume"])
        k = k[(k.ts >= t0) & (k.ts < t1)]
        ii = ((k.ts.to_numpy() - t0) // HOUR).astype(np.int64)
        close[ii, j] = k.close.to_numpy(); qv[ii, j] = k.quote_volume.to_numpy(np.float32)
        fp = os.path.join(root, "funding", f"{s}.parquet")
        f = pd.read_parquet(fp) if os.path.exists(fp) else pd.DataFrame({"ts": [], "rate": []})
        fund[s] = (f.ts.to_numpy(np.int64), np.nan_to_num(f.rate.to_numpy(np.float64)))
    return dict(grid=grid, syms=syms, close=close, qv=qv, fund=fund)


# ------------------------------------------------------------------ daily panels
def _ffill_col(x, limit):
    """Forward-fill NaN in a 1-D array, at most `limit` steps."""
    idx = np.where(np.isfinite(x), np.arange(len(x)), -1)
    last = np.maximum.accumulate(idx)
    ok = (last >= 0) & (np.arange(len(x)) - last <= limit)
    out = np.full_like(x, np.nan); out[ok] = x[last[ok]]
    return out


def _days(grid):
    return np.arange(grid[0] // DAY * DAY + DAY, grid[-1] + 1, DAY, dtype=np.int64)


def daily_panels(p, lag_h=0):
    """Daily price at 00:00 (+ lag_h) and total return incl. funding, one symbol at a time."""
    grid, close = p["grid"], p["close"]
    T, N = close.shape
    days = _days(grid); D = len(days)
    hi = ((days + lag_h * HOUR - HOUR - grid[0]) // HOUR).astype(np.int64)  # bar closing at d + lag
    okh = (hi >= 0) & (hi < T)
    P = np.full((D, N), np.nan); F = np.zeros((D, N))
    for j, s in enumerate(p["syms"]):
        cf = _ffill_col(close[:, j], 24)
        P[okh, j] = cf[hi[okh]]
        ft, fr = p["fund"][s]
        if len(ft):
            b = np.searchsorted(days + lag_h * HOUR, ft, side="left") - 1      # ft in (d + lag, d + 1 + lag]
            ok = (b >= 0) & (b < D)
            np.add.at(F[:, j], b[ok], fr[ok])
    R = np.where(np.isfinite(P[1:]) & np.isfinite(P[:-1]), P[1:] / np.where(np.isfinite(P[:-1]), P[:-1], 1) - 1, 0.0)
    R = np.vstack([R, np.zeros((1, N))])
    TR = np.where(np.isfinite(P), R - F, 0.0)
    return dict(days=days, P=P, TR=TR, F=F, hi=hi)


def factor_inputs(p, dp):
    """Universe filters and the six factors at each day, from data up to 00:00 only (per symbol)."""
    grid, close, qv = p["grid"], p["close"], p["qv"]
    days, P, hi = dp["days"], dp["P"], dp["hi"]
    D, N = P.shape; T = len(grid); H30 = HIST_D * 24
    end = np.clip(hi + 1, 0, T)                                          # bars [end - k, end) known at d
    def win(c, k):
        return c[end] - c[np.clip(end - k, 0, T)]
    elig = np.zeros((D, N), bool); qv30 = np.zeros((D, N)); rvol = np.full((D, N), np.nan)
    vshock = np.full((D, N), np.nan); fz = np.zeros((D, N))
    for j, s in enumerate(p["syms"]):
        c = close[:, j]; pres = np.isfinite(c)
        if not pres.any():
            continue
        first_ts = grid[pres.argmax()]
        cp = np.r_[0, np.cumsum(pres)]
        lr = np.r_[np.nan, np.diff(np.log(c))]; ok = np.isfinite(lr); l0 = np.where(ok, lr, 0.0)
        c1, c2, cn = np.r_[0.0, np.cumsum(l0)], np.r_[0.0, np.cumsum(l0 ** 2)], np.r_[0, np.cumsum(ok)]
        cq = np.r_[0.0, np.cumsum(np.where(np.isfinite(qv[:, j]), qv[:, j], 0.0))]
        n = np.maximum(win(cn, H30), 1); m1 = win(c1, H30) / n
        rv = np.sqrt(np.maximum(win(c2, H30) / n - m1 ** 2, 0)); rvol[:, j] = rv
        q24 = win(cq, 24); q30p = win(cq, H30 + 24) - q24
        qv30[:, j] = win(cq, H30)
        vshock[:, j] = np.log(np.maximum(q24, 1e-9) / np.maximum(q30p / HIST_D, 1e-9))
        elig[:, j] = ((days - MIN_AGE_D * DAY >= first_ts) & (win(cp, H30) == H30)
                      & (rv * np.sqrt(24 * 365) >= MIN_RVOL_ANN) & np.isfinite(P[:, j]))
        ft, fr = p["fund"][s]
        if len(ft):
            cs = np.r_[0.0, np.cumsum(fr)]
            fz[:, j] = (cs[np.searchsorted(ft, days, side="right")] - cs[np.searchsorted(ft, days - 7 * DAY, side="right")]) / 21.0
    lP = np.log(P)
    def lagd(x, k):
        out = np.full_like(x, np.nan); out[k:] = x[:-k]; return out
    fac = dict(MOM28=lagd(lP, 1) - lagd(lP, 28), MOM7=lagd(lP, 1) - lagd(lP, 7), REV1=lP - lagd(lP, 1),
               FUND7=fz, VSHOCK=vshock, RVOL30=rvol)
    return dict(elig=elig, qv30=qv30, fac=fac)


def universe(elig, qv30, top=TOP_N):
    """(D, top) coin indices of the point-in-time top-`top` by trailing 30-day quote volume; -1 if short."""
    D, N = elig.shape
    U = np.full((D, top), -1, np.int64)
    score = np.where(elig, qv30, -np.inf)
    for d in range(D):
        k = int(elig[d].sum())
        if k >= top:
            U[d] = np.argpartition(-score[d], top - 1)[:top]
    return U


# ------------------------------------------------------------------ portfolios
def legs(U, F, sign, q=QUANT):
    """Long / short coin indices per day from factor F restricted to the universe; sign +1 longs high F."""
    D, top = U.shape; k = top // q
    L = np.full((D, k), -1, np.int64); S = np.full((D, k), -1, np.int64)
    for d in range(D):
        u = U[d]
        if u[0] < 0:
            continue
        f = F[d, u] * sign; ok = np.isfinite(f)
        if ok.sum() < int(0.9 * top):                                  # rank only with >= 90 % of the universe
            continue
        uu, ff = u[ok], f[ok]
        o = np.argsort(ff, kind="stable")
        S[d], L[d] = uu[o[:k]], uu[o[-k:]]
    return L, S


def book_returns(L, S, TR, H):
    """Daily gross P&L, leg contributions and turnover of the H-staggered book (weights +-1/k per leg)."""
    D, k = L.shape; N = TR.shape[1]
    active = L[:, 0] >= 0
    coh_l = np.zeros(D); coh_s = np.zeros(D)
    W = np.zeros((D, N))
    for s in range(D):
        if not active[s]:
            continue
        w = np.zeros(N); w[L[s]] += 1.0 / k; w[S[s]] -= 1.0 / k
        W[s:min(D, s + H)] += w / H
    gross = np.einsum("dn,dn->d", W, TR)
    long_c = np.einsum("dn,dn->d", np.where(W > 0, W, 0), TR)
    turnover = np.r_[np.abs(W[0]).sum(), np.abs(np.diff(W, axis=0)).sum(1)]
    return dict(gross=gross, long=long_c, short=gross - long_c, turnover=turnover, W=W)


def cohort_gross(L, S, TR, H):
    """Gross daily P&L only, without the dense weight matrix (fast path for the permutation null)."""
    D, k = L.shape
    act = L[:, 0] >= 0
    out = np.zeros(D)
    for h in range(H):                                                  # cohort formed at t - h, held on t
        s = np.arange(D) - h; ok = (s >= 0)
        ss = np.where(ok, s, 0)
        a = act[ss] & ok
        lt = TR[np.arange(D)[:, None], np.where(a[:, None], L[ss], 0)].mean(1)
        st = TR[np.arange(D)[:, None], np.where(a[:, None], S[ss], 0)].mean(1)
        out += np.where(a, lt - st, 0.0) / H
    return out


# ------------------------------------------------------------------ statistics
def rank_ic(U, F, TR):
    """Daily Spearman IC between F and next-day TR over the universe."""
    D = U.shape[0]; out = np.full(D, np.nan)
    for d in range(D):
        u = U[d]
        if u[0] < 0:
            continue
        f, r = F[d, u], TR[d, u]
        ok = np.isfinite(f) & np.isfinite(r)
        if ok.sum() > 5:
            a, b = pd.Series(f[ok]).rank().to_numpy(), pd.Series(r[ok]).rank().to_numpy()
            if a.std() > 0 and b.std() > 0:
                out[d] = np.corrcoef(a, b)[0, 1]
    return out


def nw_t(x, lags=10):
    x = np.asarray(x, float); x = x[np.isfinite(x)]; n = len(x)
    if n < 20:
        return np.nan
    e = x - x.mean(); v = e @ e / n
    for l in range(1, lags + 1):
        v += 2 * (1 - l / (lags + 1)) * (e[l:] @ e[:-l]) / n
    return x.mean() / np.sqrt(v / n) if v > 0 else np.nan


def holm(pvals):
    p = np.asarray(pvals, float); m = len(p); o = np.argsort(p)
    adj = np.empty(m); run = 0.0
    for r, i in enumerate(o):
        run = max(run, (m - r) * p[i]); adj[i] = min(1.0, run)
    return adj


def max_drawdown(x):
    c = np.cumsum(np.nan_to_num(x)); return float((c - np.maximum.accumulate(c)).min())
