"""R4b low-volatility factor with a bounded tail — library (pre-registered in next_signal_ideas.md, R4b + details).

Long the lowest-RVOL30 quintile of a point-in-time universe (inverse-vol weights, capped), hedged with a short BTC
perp sized to the trailing 60-day beta of the alt book. Same daily conventions as xsec.py:
  weights at day d use data up to 00:00 of d and are held d -> d + 1, earning W[d] . TR[d] (TR includes funding).
  staggering H: the book on day d is the mean of the last H days' target weights (no overlap in P&L).
  hedge[d]      = beta[d] shorted in BTC, beta[d] = OLS slope of the alt book's daily return on BTC's over d-60 .. d-1.
  cost          = COST_BP per side on the daily turnover of the whole book including the hedge.
"""
import numpy as np, pandas as pd
import xsec as X

COST = X.COST_BP * 1e-4
CAP_LONG, SHORT_GROSS, CAP_SHORT = 0.20, 0.30, 0.025
BETA_WIN, ANN = 60, 365


# ------------------------------------------------------------------ universe and weights
def universe_band(elig, qv30, lo, hi):
    """(D, hi - lo + 1) coin indices of ranks lo..hi (1-based) by trailing 30-day volume; -1 rows if < hi eligible."""
    D, N = elig.shape; U = np.full((D, hi - lo + 1), -1, np.int64)
    score = np.where(elig, qv30, -np.inf)
    for d in range(D):
        if elig[d].sum() >= hi:
            U[d] = np.argsort(-score[d], kind="stable")[:hi][lo - 1:hi]
    return U


def inv_vol_weights(sets, V, total=1.0, cap=CAP_LONG):
    """(D, N) weights proportional to 1 / V on `sets` (D, m; -1 rows empty), summing to `total` unless the cap
    forces less; each weight <= cap, excess redistributed to uncapped names."""
    D, m = sets.shape; N = V.shape[1]
    ok = sets[:, 0] >= 0; Ic = np.where(sets < 0, 0, sets)
    v = np.take_along_axis(V, Ic, 1)
    good = np.isfinite(v) & (v > 0) & ok[:, None]
    w = np.where(good, 1.0 / np.where(good, v, 1.0), 0.0)
    s = w.sum(1, keepdims=True); w = np.where(s > 0, w / np.where(s > 0, s, 1.0), 0.0) * total
    for _ in range(20):
        over = w > cap + 1e-15
        if not over.any():
            break
        w = np.where(over, cap, w)
        free = good & (w < cap - 1e-15)
        deficit = (total - w.sum(1, keepdims=True)) * (s > 0)
        fs = np.where(free, w, 0.0).sum(1, keepdims=True)
        add = np.where(fs > 0, deficit * np.where(free, w, 0.0) / np.where(fs > 0, fs, 1.0), 0.0)
        w = w + np.where(deficit > 1e-15, add, 0.0)
    out = np.zeros((D, N)); np.put_along_axis(out, Ic, w, 1)
    return out


def ew_matrix(U, N, total=1.0):
    D, k = U.shape; out = np.zeros((D, N)); ok = U[:, 0] >= 0
    np.put_along_axis(out, np.where(U < 0, 0, U), np.where(ok[:, None], total / k, 0.0), 1)
    return out


def rolling_mean_rows(W, H):
    """Mean of the last H rows (rows before the start count as zero weight)."""
    if H == 1:
        return W
    c = np.vstack([np.zeros((1, W.shape[1])), np.cumsum(W, 0)])
    idx = np.arange(1, len(W) + 1)
    return (c[idx] - c[np.maximum(idx - H, 0)]) / H


# ------------------------------------------------------------------ hedge and accounting
def rolling_beta(book_ret, btc_ret, active, win=BETA_WIN):
    """beta known at day d from days d - win .. d - 1 (NaN until `win` active days)."""
    x = np.where(active, book_ret, np.nan)
    df = pd.DataFrame({"y": x, "b": btc_ret})
    b = df.y.rolling(win, min_periods=win).cov(df.b) / df.b.rolling(win, min_periods=win).var()
    return b.shift(1).to_numpy()


def hedged_book(Wl, Ws, TR, btc, H=1):
    """Full position matrix (D, N) with the BTC hedge in column `btc`, plus beta and the pre-hedge book return."""
    W = rolling_mean_rows(Wl - Ws, H)
    book = (W * TR).sum(1)
    active = np.abs(W).sum(1) > 0
    beta = rolling_beta(book, TR[:, btc], active)
    full = W.copy()
    full[:, btc] = np.where(np.isfinite(beta), -beta, np.nan)
    live = np.isfinite(beta) & active
    full = np.where(live[:, None], np.nan_to_num(full), 0.0)
    return full, beta, live, book


def day_costs(W):
    prev = np.vstack([np.zeros((1, W.shape[1])), W[:-1]])
    return COST * np.abs(W - prev).sum(1)


def net_series(full, TR, idx):
    Wi, Ti = full[idx], TR[idx]
    gross = (Wi * Ti).sum(1)
    return gross - day_costs(Wi), gross


# ------------------------------------------------------------------ statistics
def nw_p(t):
    from math import erf, sqrt
    return 0.5 * (1 - erf(t / sqrt(2))) if np.isfinite(t) else 1.0


def block_years(net, size=365):
    nb = len(net) // size
    return [float(net[i * size:(i + 1) * size].sum()) for i in range(nb)]


def cell_stats(net, gross, full, beta, TR, btc, idx, days):
    n = len(net); mu = net.mean()
    bt = TR[idx, btc]
    blocks = block_years(net)
    wd = pd.Series(net).sort_values()
    return dict(
        n=n, net_bp=float(mu * 1e4), gross_bp=float(gross.mean() * 1e4), cost_bp=float((gross - net).mean() * 1e4),
        ann_pct=float(mu * ANN * 100), sharpe=float(mu / net.std(ddof=1) * np.sqrt(ANN)), nw_t=float(X.nw_t(net)),
        median_bp=float(np.median(net) * 1e4), mean_trim_bp=float(net[(net > np.percentile(net, 1)) & (net < np.percentile(net, 99))].mean() * 1e4),
        daily_std_pct=float(net.std(ddof=1) * 100), skew=float(pd.Series(net).skew()),
        worst_day_pct=float(net.min() * 100), best_day_pct=float(net.max() * 100),
        max_dd_pct=float(X.max_drawdown(net) * 100), beta_to_btc=float(np.cov(net, bt)[0, 1] / bt.var(ddof=1)),
        mean_hedge=float(-full[idx, btc].mean()), mean_gross=float(np.abs(full[idx]).sum(1).mean()),
        turnover=float(np.abs(np.diff(np.vstack([np.zeros((1, full.shape[1])), full[idx]]), axis=0)).sum(1).mean()),
        blocks_bp_per_day=[b / 365 * 1e4 for b in blocks], blocks_pos=int(sum(b > 0 for b in blocks)), blocks_n=len(blocks),
    )


# ------------------------------------------------------------------ the study
def evaluate(U, V, F, TR0, TR1, btc, days, Hs=(1, 7), perms=300, seed=0, ls_arm=True):
    """U (D, k) universe; V (D, N) vol used for weights (RVOL30); F factor for ranking (RVOL30); TR0 / TR1 returns
    from 00:00 / 01:00 with funding; btc column. Returns per-H cells for primary / control / info and the criteria."""
    D, N = TR0.shape; k = U.shape[1]; m = k // X.QUANT
    Lq, Sq = X.legs(U, F, -1.0)                                      # long = lowest RVOL, short = highest
    Wl = inv_vol_weights(Lq, V, 1.0, CAP_LONG)
    Ws = inv_vol_weights(Sq, V, SHORT_GROSS, CAP_SHORT)
    Wc = ew_matrix(U, N, 1.0)
    zero = np.zeros_like(Wl)
    first = int(np.flatnonzero((U[:, 0] >= 0) & (Lq[:, 0] >= 0))[0])
    idx = np.arange(first + BETA_WIN + 1, D - 1)
    rng = np.random.default_rng(seed)
    cells, series, gross_mean = {}, {}, {}
    for H in Hs:
        row = {}
        for name, (a, b) in dict(primary=(Wl, zero), control=(Wc, zero)).items():
            full, beta, live, _ = hedged_book(a, b, TR0, btc, H)
            net, gross = net_series(full, TR0, idx); net1, _ = net_series(full, TR1, idx)
            st = cell_stats(net, gross, full, beta, TR0, btc, idx, days)
            st["lag_net_bp"] = float(net1.mean() * 1e4)
            row[name] = st; series[(name, H)] = net; gross_mean[(name, H)] = gross.mean()
        if ls_arm:
            full, beta, live, _ = hedged_book(Wl, Ws, TR0, btc, H)
            net, gross = net_series(full, TR0, idx); net1, _ = net_series(full, TR1, idx)
            st = cell_stats(net, gross, full, beta, TR0, btc, idx, days); st["lag_net_bp"] = float(net1.mean() * 1e4)
            row["info_ls"] = st; series[("info_ls", H)] = net
        # permutation: random quintile of the same universe, same construction
        null = np.empty(perms)
        for r in range(perms):
            o = np.argsort(rng.random(U.shape), 1)
            Lr = np.take_along_axis(U, o[:, :m], 1).copy(); Lr[U[:, 0] < 0] = -1
            Wr = inv_vol_weights(Lr, V, 1.0, CAP_LONG)
            fr, _, _, _ = hedged_book(Wr, zero, TR0, btc, H)
            null[r] = net_series(fr, TR0, idx)[1].mean()            # gross: a daily-redrawn random quintile pays far more turnover than a persistent rank
        row["perm_pct"] = float((null < gross_mean[("primary", H)]).mean() * 100)
        row["perm_q975_bp"] = float(np.percentile(null, 97.5) * 1e4)
        diff = series[("primary", H)] - series[("control", H)]
        row["diff_bp"] = float(diff.mean() * 1e4); row["diff_nw_t"] = float(X.nw_t(diff))
        cells[H] = row
    # Holm across the cells
    ps = np.array([nw_p(cells[H]["primary"]["nw_t"]) for H in Hs]); adj = X.holm(ps)
    out = []
    for H, pa in zip(Hs, adj):
        c = cells[H]; p = c["primary"]
        crit = dict(
            net_t=(p["net_bp"] > 0 and p["nw_t"] >= 2 and pa < 0.05, f"net {p['net_bp']:+.1f} bp/day, NW t {p['nw_t']:.2f}, Holm p {pa:.3f}"),
            beats_control=(c["diff_bp"] > 0 and c["diff_nw_t"] >= 2, f"diff {c['diff_bp']:+.1f} bp/day, NW t {c['diff_nw_t']:.2f}"),
            blocks=(p["blocks_pos"] >= int(np.ceil(0.75 * p["blocks_n"])), f"{p['blocks_pos']} of {p['blocks_n']} 365-day blocks"),
            perm=(c["perm_pct"] >= 97.5, f"{c['perm_pct']:.1f} th percentile of gross"),
            lag=(p["lag_net_bp"] > 0, f"{p['lag_net_bp']:+.1f} bp/day"),
            median_mean_sign=(np.sign(p["median_bp"]) == np.sign(p["net_bp"]) and p["net_bp"] > 0, f"median {p['median_bp']:+.1f}, mean {p['net_bp']:+.1f}"),
        )
        c["holm_p"] = float(pa)
        c["criteria"] = {a: dict(ok=bool(b[0]), detail=b[1]) for a, b in crit.items()}
        c["pass"] = bool(all(v[0] for v in crit.values()))
    return dict(cells={int(H): cells[H] for H in Hs}, passed=bool(any(cells[H]["pass"] for H in Hs)),
                first_day=str(pd.to_datetime(days[idx[0]], unit="s").date()), last_day=str(pd.to_datetime(days[idx[-1]], unit="s").date()),
                days=len(idx), quintile=m, universe=k)
