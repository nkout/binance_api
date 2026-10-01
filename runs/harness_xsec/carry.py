"""R5 delta-neutral funding carry — library (pre-registered in next_signal_ideas.md, R5).

Builds on xsec.py (R4): same hourly panels, daily 00:00 grid, point-in-time top-40 universe and FUND7.
A position on coin j held over (d, d+1] earns, per unit notional:
    CARRY+  (long spot, short perp):  Rs - Rp + F      F = funding paid by a perp long in (d, d+1]
    CARRY-  (long perp, short spot):  Rp - Rs - F      (gross of spot borrow cost)
Costs: SPOT_BP + PERP_BP per unit notional on every entry and every exit (taker, both legs).
Capital: MAX_POS slots; a slot is 1 / MAX_POS of capital = spot notional n + perp margin n / LEV.
"""
import os
import numpy as np, pandas as pd
import xsec as X

ENTER, EXIT = 0.0003, 0.0001             # FUND7 per 8 h
MAX_POS, LEV = 10, 3.0
SPOT_BP, PERP_BP = 7.5, 4.5
MIN_SPOT_QV = 2e6                        # USD / day, 30-day mean
STRESS = 0.25                            # perp high > entry * (1 + STRESS) while short -> margin top-up day
N_SLOT = 1.0 / MAX_POS / (1 + 1 / LEV)   # notional per position as a share of capital


def load_spot(root, syms, grid):
    """Spot 1 h close / quote volume aligned to the perp grid and symbol order (NaN when no spot pair)."""
    T, N = len(grid), len(syms)
    close = np.full((T, N), np.nan); qv = np.full((T, N), np.nan, np.float32)
    for j, s in enumerate(syms):
        f = os.path.join(root, "spot", f"{s}.parquet")
        if not os.path.exists(f):
            continue
        k = pd.read_parquet(f, columns=["ts", "close", "quote_volume"])
        k = k[(k.ts >= grid[0]) & (k.ts <= grid[-1]) & (k.ts % 3600 == 0)]
        ii = ((k.ts.to_numpy() - grid[0]) // 3600).astype(np.int64)
        close[ii, j] = k.close.to_numpy(); qv[ii, j] = k.quote_volume.to_numpy(np.float32)
    return close, qv


def spot_panels(p, close_s, qv_s, lag_h=0):
    """Daily spot price / return on the same grid (xsec.daily_panels with no funding) + eligibility."""
    ps = dict(grid=p["grid"], syms=p["syms"], close=close_s, qv=qv_s,
              fund={s: (np.zeros(0, np.int64), np.zeros(0)) for s in p["syms"]})
    dps = X.daily_panels(ps, lag_h)
    T = len(p["grid"]); H30 = X.HIST_D * 24
    end = np.clip(dps["hi"] + 1, 0, T)
    D, N = dps["P"].shape
    ok = np.zeros((D, N), bool)
    for j in range(N):
        c = close_s[:, j]
        if not np.isfinite(c).any():
            continue
        cp = np.r_[0, np.cumsum(np.isfinite(c))]
        cq = np.r_[0.0, np.cumsum(np.where(np.isfinite(qv_s[:, j]), qv_s[:, j], 0.0))]
        s = np.clip(end - H30, 0, T)
        ok[:, j] = ((cp[end] - cp[s]) == H30) & ((cq[end] - cq[s]) / X.HIST_D >= MIN_SPOT_QV) & np.isfinite(dps["P"][:, j])
    return dps, ok


def simulate(elig, fz, Rp, Rs, F, mirror=False, enter=ENTER, exit_=EXIT, max_pos=MAX_POS):
    """Daily positions (D, N) bool held over (d, d+1], and per-day P&L on capital.
    Decisions at d use only elig[d] and fz[d] (known at 00:00 of d)."""
    D, N = elig.shape
    sig = -fz if mirror else fz
    pos = np.zeros((D, N), bool); cur = np.zeros(N, bool)
    for d in range(D):
        keep = cur & elig[d] & (sig[d] >= exit_)
        room = max_pos - int(keep.sum())
        cand = np.flatnonzero(elig[d] & ~keep & (sig[d] >= enter))
        if room > 0 and len(cand):
            cand = cand[np.argsort(-sig[d, cand], kind="stable")][:room]
            keep[cand] = True
        pos[d] = cur = keep
    unit = (Rp - Rs - F) if mirror else (Rs - Rp + F)
    gross = N_SLOT * np.where(pos, unit, 0.0).sum(1)
    prev = np.vstack([np.zeros((1, N), bool), pos[:-1]])
    trades = (pos & ~prev).sum(1) + (~pos & prev).sum(1)
    cost = N_SLOT * trades * (SPOT_BP + PERP_BP) / 1e4
    fund = N_SLOT * np.where(pos, -F if mirror else F, 0.0).sum(1)
    return dict(pos=pos, gross=gross, net=gross - cost, cost=cost, funding=fund, trades=trades)


def episodes(pos):
    """(coin, first day, last day held) for every contiguous holding."""
    out = []
    D, N = pos.shape
    for j in np.flatnonzero(pos.any(0)):
        x = np.r_[False, pos[:, j], False].astype(int); dx = np.diff(x)
        for a, b in zip(np.flatnonzero(dx == 1), np.flatnonzero(dx == -1)):
            out.append((int(j), int(a), int(b - 1)))
    return out


def margin_stress(eps, days, grid, high_by_coin, entry_px, lag_h=0):
    """Count episodes whose perp high exceeded entry * (1 + STRESS) while the short was open."""
    n = 0
    for j, a, b in eps:
        h = high_by_coin.get(j)
        if h is None:
            continue
        i0 = (days[a] + lag_h * 3600 - grid[0]) // 3600; i1 = (days[b + 1] + lag_h * 3600 - grid[0]) // 3600 if b + 1 < len(days) else len(grid)
        hh = h[int(i0):int(i1)]
        if len(hh) and np.nanmax(hh) > entry_px[a, j] * (1 + STRESS):
            n += 1
    return n


def summarize(net, days, idx, nw_lags=10):
    """Annualised return, calendar-year sums, worst calendar month, NW t, Sharpe over day indices idx."""
    x = net[idx]; dt = pd.to_datetime(days[idx], unit="s", utc=True)
    s = pd.Series(x, index=dt)
    yrs = s.groupby(s.index.year).sum()
    mon = s.groupby([s.index.year, s.index.month]).sum()
    cum = np.cumsum(x)
    return dict(ann_pct=float(x.mean() * 365 * 100), years_pct={int(k): float(v * 100) for k, v in yrs.items()},
                worst_month_pct=float(mon.min() * 100), nw_t=float(X.nw_t(x, nw_lags)),
                sharpe=float(x.mean() / x.std() * np.sqrt(365)) if x.std() > 0 else np.nan,
                maxdd_pct=float((cum - np.maximum.accumulate(cum)).min() * 100))
