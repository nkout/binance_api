"""R6 daily trend-following + volatility targeting — library (pre-registered in next_signal_ideas.md, R6).

Everything is on the daily 00:00 UTC panels of xsec.py. Conventions (same as R4 / R5):
  day d          the instant 00:00 UTC of day d; every signal at d uses prices at or before that instant
  P[d]           price at d; TR[d] = simple return d -> d + 1 minus funding paid by a long (a short earns -TR)
  position[d]    notional / capital held from d to d + 1 (earns position[d] * TR[d])
  cost           COST_BP per side on |position[d] - position[d - 1]|, charged on the day the position changes
  idle capital   earns 0 in the main rows (as in R5); one information row adds the risk-free rate on the unused part

Arms
  BTC  A      signal = mean sign(trailing return over 20 / 60 / 120 d) in [-1, 1] (long / short),
              position = signal * 40 % / trailing-30-day realised vol, |position| <= 2, rebalanced only when the
              target moves more than 0.10 (absolute, in units of capital) from the held position
  BTC  A_long the same with the signal clipped at 0 (information)
  BTC  B      signal = +1: vol-targeted hold (a sizing rule, not alpha)
  BTC  BH     buy and hold, position 1
  XS   1C     R4 point-in-time top-40; signal clipped to [0, 1] per coin (long / flat); weights proportional to
              signal / vol, normalised by the sum of 1 / vol over the universe (fully invested when every coin trends)
  XS   1C_ls  the signed signal (information);  IV_long = signal 1 (control);  EW = equal weight (benchmark)
"""
import numpy as np, pandas as pd
import xsec as X

LOOKBACKS = (20, 60, 120)
TARGET_VOL, CAP, BAND, VOL_WIN = 0.40, 2.0, 0.10, 30
RF = 0.045
COST = X.COST_BP * 1e-4
ANN = 365


# ------------------------------------------------------------------ signals
def trend_signal(P, long_only=False):
    """(D, N) mean of sign(P[d] / P[d - L] - 1) over the lookbacks; NaN unless all three exist."""
    P = np.asarray(P, float)
    out = []
    for L in LOOKBACKS:
        r = np.full(P.shape, np.nan); r[L:] = P[L:] / P[:-L] - 1.0
        out.append(np.sign(r))
    S = np.mean(out, axis=0)
    if long_only:
        S = np.where(np.isfinite(S), np.maximum(S, 0.0), np.nan)
    return S


def realised_vol(P, win=VOL_WIN):
    """(D, N) annualised std of the last `win` daily simple returns ending at d."""
    P = np.asarray(P, float)
    r = np.full(P.shape, np.nan); r[1:] = P[1:] / P[:-1] - 1.0
    return pd.DataFrame(r).rolling(win, min_periods=win).std().to_numpy() * np.sqrt(ANN)


def vol_target(sig, vol, target=TARGET_VOL, cap=CAP):
    return np.clip(sig * target / np.maximum(vol, 1e-6), -cap, cap)


def apply_band(tgt, band=BAND):
    """Held position: jump to the target only when it is more than `band` away; 0 before the first target."""
    held = np.zeros(len(tgt)); cur = 0.0
    for d, t in enumerate(tgt):
        if np.isfinite(t) and abs(t - cur) > band:
            cur = t
        held[d] = cur
    return held


def xs_weights(U, S, V, equal=False):
    """(D, N) weights on the point-in-time universe U (D, k; -1 rows = no universe that day)."""
    D, N = S.shape; k = U.shape[1]
    ok = U[:, 0] >= 0; Uc = np.where(U < 0, 0, U)
    W = np.zeros((D, N))
    if equal:
        np.put_along_axis(W, Uc, np.where(ok[:, None], 1.0 / k, 0.0), 1)
        return W
    Su = np.take_along_axis(S, Uc, 1); Vu = np.take_along_axis(V, Uc, 1)
    valid = np.isfinite(Vu) & (Vu > 0) & ok[:, None]
    inv = np.where(valid, 1.0 / np.where(valid, Vu, 1.0), 0.0)
    den = inv.sum(1, keepdims=True)
    raw = np.where(np.isfinite(Su), Su, 0.0) * inv
    Wu = np.where(den > 0, raw / np.where(den > 0, den, 1.0), 0.0)
    np.put_along_axis(W, Uc, Wu, 1)
    return W


# ------------------------------------------------------------------ accounting and statistics
def _2d(W):
    W = np.asarray(W, float)
    return W[:, None] if W.ndim == 1 else W


def day_costs(W):
    W = _2d(W)
    prev = np.vstack([np.zeros((1, W.shape[1])), W[:-1]])
    return COST * np.abs(W - prev).sum(1)


def lagged(W, k=1):
    W = _2d(W)
    return np.vstack([np.zeros((k, W.shape[1])), W[:-k]])


def max_dd(net):
    eq = np.cumprod(1.0 + net)
    return float((eq / np.maximum.accumulate(eq) - 1.0).min())


def yearly(net, days):
    yr = pd.to_datetime(days, unit="s").year
    s = pd.Series(net, index=yr)
    g = s.groupby(level=0)
    return {int(y): dict(ret=float((1 + x).prod() - 1), cash=float(RF * len(x) / ANN), n=int(len(x)))
            for y, x in g}


def sharpe(net):
    sd = net.std(ddof=1)
    return float(net.mean() / sd * np.sqrt(ANN)) if sd > 0 else float("nan")


def summarise(W, TR, idx, days):
    """Net daily series and statistics of position matrix W (D, m) against returns TR (D, m) on days idx."""
    W, TR = _2d(W), _2d(TR)
    Wi, Ti = W[idx], TR[idx]
    gross = (Wi * Ti).sum(1); cost = day_costs(Wi); net = gross - cost
    n = len(net); yrs = n / ANN
    pos_sum = Wi.sum(1); idle = np.maximum(0.0, 1.0 - np.abs(Wi).sum(1))
    net_rf = net + RF / ANN * idle
    ye = yearly(net, days[idx])
    return dict(
        net=net, n=n,
        ann_pct=float(net.mean() * ANN * 100), cagr_pct=float((np.prod(1 + net) ** (1 / yrs) - 1) * 100),
        gross_ann_pct=float(gross.mean() * ANN * 100), cost_ann_pct=float(cost.mean() * ANN * 100),
        sharpe=sharpe(net), nw_t=float(X.nw_t(net)), maxdd_pct=max_dd(net) * 100,
        vol_ann_pct=float(net.std(ddof=1) * np.sqrt(ANN) * 100),
        skew=float(pd.Series(net).skew()), worst_day_pct=float(net.min() * 100),
        mean_abs_exposure=float(np.abs(Wi).sum(1).mean()), mean_net_exposure=float(pos_sum.mean()),
        days_nonflat_pct=float((np.abs(Wi).sum(1) > 1e-9).mean() * 100),
        turnover_per_year=float(np.abs(np.diff(np.vstack([np.zeros((1, Wi.shape[1])), Wi]), axis=0)).sum() / yrs),
        ann_with_rf_pct=float(net_rf.mean() * ANN * 100), sharpe_with_rf=sharpe(net_rf),
        years={y: v for y, v in ye.items()},
    )


def years_beating_cash(st):
    return int(sum(v["ret"] > v["cash"] for v in st["years"].values())), len(st["years"])


# ------------------------------------------------------------------ nulls
def shift_null(Wi, Ti, cost_i, nshift=2000, min_shift=90, seed=0):
    """Net Sharpe of the same position path circularly shifted against returns (timing destroyed, exposure,
    turnover and autocorrelation kept). Wi, Ti: (n, m) on the live window; cost_i: (n,) cost of the true path."""
    rng = np.random.default_rng(seed); n = len(Wi)
    ks = rng.integers(min_shift, n - min_shift, nshift)
    out = np.empty(nshift)
    for i, k in enumerate(ks):
        g = (np.roll(Wi, k, axis=0) * Ti).sum(1) - np.roll(cost_i, k)
        out[i] = sharpe(g)
    return out


def stationary_boot_sharpe_diff(a, b, B=1000, mean_block=20, seed=0):
    """95 % CI of Sharpe(a) - Sharpe(b) with paired stationary-bootstrap resampling of days."""
    rng = np.random.default_rng(seed); n = len(a); p = 1.0 / mean_block
    d = np.empty(B)
    for i in range(B):
        idx = np.empty(n, np.int64); j = 0
        while j < n:
            s = rng.integers(0, n); L = min(rng.geometric(p), n - j)
            idx[j:j + L] = (s + np.arange(L)) % n; j += L
        d[i] = sharpe(a[idx]) - sharpe(b[idx])
    return [float(np.percentile(d, 2.5)), float(np.percentile(d, 97.5))]


# ------------------------------------------------------------------ evaluations
def _criteria(st, ref, st_lag, null_pct):
    nb, ny = years_beating_cash(st)
    need = int(np.ceil(5 / 7 * ny))
    c = dict(
        sharpe_vs_ref=(st["sharpe"] - ref["sharpe"] >= 0.3, f"{st['sharpe']:.2f} vs {ref['sharpe']:.2f} + 0.3"),
        maxdd_half=(st["maxdd_pct"] >= 0.5 * ref["maxdd_pct"], f"{st['maxdd_pct']:.1f} % vs 0.5 x {ref['maxdd_pct']:.1f} %"),
        years_beat_cash=(nb >= need, f"{nb} of {ny} (need {need})"),
        shift_null=(null_pct >= 97.5, f"{null_pct:.1f} th percentile"),
        lag1_positive=(st_lag["ann_pct"] > 0, f"{st_lag['ann_pct']:+.2f} %/yr"),
    )
    return {k: dict(ok=bool(v[0]), detail=v[1]) for k, v in c.items()}


def _pack(st):
    return {k: v for k, v in st.items() if k != "net"}


def evaluate_btc(P, TR, days, nshift=2000, seed=0):
    """BTC arms on one price / return series. P, TR: (D,), days: (D,) epoch seconds; the last day is dropped."""
    D = len(P); P2 = P[:, None]
    vol = realised_vol(P2)[:, 0]
    sLS = trend_signal(P2)[:, 0]; sL = trend_signal(P2, True)[:, 0]
    live = np.isfinite(sLS) & np.isfinite(vol)
    sB = np.where(live, 1.0, np.nan)
    tgt = lambda s: vol_target(s, vol)
    held = dict(A=apply_band(tgt(sLS)), A_long=apply_band(tgt(sL)), B=apply_band(tgt(sB)))
    start = int(np.flatnonzero(live)[0]); idx = np.arange(start, D - 1)
    held["BH"] = np.where(np.arange(D) >= start, 1.0, 0.0)
    st = {k: summarise(h, TR, idx, days) for k, h in held.items()}
    lag = {k: summarise(lagged(held[k]), TR, idx, days) for k in ("A", "A_long", "B", "BH")}
    Wi, Ti = _2d(held["A"])[idx], _2d(TR)[idx]
    null = shift_null(Wi, Ti, day_costs(Wi), nshift, seed=seed)
    pct = float((null < st["A"]["sharpe"]).mean() * 100)
    crit = _criteria(st["A"], st["BH"], lag["A"], pct)
    held_A = held["A"][idx]
    pnl_long = float(((np.clip(held_A, 0, None)) * TR[idx]).mean() * ANN * 100)
    pnl_short = float(((np.clip(held_A, None, 0)) * TR[idx]).mean() * ANN * 100)
    return dict(
        start=str(pd.to_datetime(days[start], unit="s").date()), end=str(pd.to_datetime(days[D - 2], unit="s").date()),
        days=len(idx), arms={k: _pack(v) for k, v in st.items()}, lag1={k: _pack(v) for k, v in lag.items()},
        null=dict(pct=pct, q50=float(np.median(null)), q975=float(np.percentile(null, 97.5)), n=len(null)),
        sharpe_diff_vs_bh_ci=stationary_boot_sharpe_diff(st["A"]["net"], st["BH"]["net"], seed=seed),
        leg_split_ann_pct=dict(long=pnl_long, short=pnl_short),
        criteria=crit, passed=bool(all(v["ok"] for v in crit.values())),
    ), st


def evaluate_xs(U, P, TR, days, nshift=500, seed=0):
    """Per-coin trend book on the point-in-time universe U (D, k). P, TR: (D, N)."""
    D = P.shape[0]
    vol = realised_vol(P)
    sLS = trend_signal(P); sL = trend_signal(P, True)
    s1 = np.where(np.isfinite(sLS), 1.0, np.nan)
    W = dict(C1=xs_weights(U, sL, vol), C1_ls=xs_weights(U, sLS, vol), IV_long=xs_weights(U, s1, vol),
             EW=xs_weights(U, sLS, vol, equal=True))
    first = int(np.flatnonzero(U[:, 0] >= 0)[0]); start = first + max(LOOKBACKS)
    idx = np.arange(start, D - 1)
    st = {k: summarise(w, TR, idx, days) for k, w in W.items()}
    lag = {k: summarise(lagged(w), TR, idx, days) for k, w in W.items()}
    Wi, Ti = W["C1"][idx], TR[idx]
    null = shift_null(Wi, Ti, day_costs(Wi), nshift, seed=seed)
    pct = float((null < st["C1"]["sharpe"]).mean() * 100)
    crit = _criteria(st["C1"], st["IV_long"], lag["C1"], pct)
    return dict(
        start=str(pd.to_datetime(days[start], unit="s").date()), end=str(pd.to_datetime(days[D - 2], unit="s").date()),
        days=len(idx), arms={k: _pack(v) for k, v in st.items()}, lag1={k: _pack(v) for k, v in lag.items()},
        null=dict(pct=pct, q50=float(np.median(null)), q975=float(np.percentile(null, 97.5)), n=len(null)),
        sharpe_diff_vs_iv_ci=stationary_boot_sharpe_diff(st["C1"]["net"], st["IV_long"]["net"], seed=seed),
        sharpe_diff_vs_ew_ci=stationary_boot_sharpe_diff(st["C1"]["net"], st["EW"]["net"], seed=seed),
        criteria=crit, passed=bool(all(v["ok"] for v in crit.values())),
    ), st
