"""N2 XFUND cross-venue funding spread, Binance vs Bybit perps — library (pre-registered in next_signal_ideas.md, Round 6 / N2).

Daily 00:00 UTC grid as xsec.py. Both venues' hourly data are aligned to the Binance grid and symbol order.
  F7_v[d, j]   sum of venue v's settlement rates in (d - 7 days, d] / 21  (mean per 8 h, any interval)
  delta[d, j]  F7_bybit - F7_binance
  set[d, j]    Binance point-in-time top 100 (R4 eligibility) & Bybit perp with 720 complete 1 h bars in the prior 30 days
               & mean Bybit turnover >= 2 M USD / day & a settlement on both venues in the prior 7 days & both prices known
  direction    +1 when delta > 0 (Bybit pays more): short Bybit, long Binance; -1 the reverse
  enter        |delta| >= 0.03 %; at most 10 positions, ranked by |delta|
  exit         |delta| < 0.01 %, or sign(delta) != direction (the higher venue changed), or the coin leaves the set
  P&L          N_SLOT x direction x (TR_binance - TR_bybit) per day, TR = price return - funding paid by a long, so
               funding part = direction x (F_bybit - F_binance), basis part = direction x (R_binance - R_bybit)
  cost         N_SLOT x (4.5 + 5.5) bp per entry and per exit (taker, both legs); a flip is an exit and an entry
  capital      10 slots; a slot is 1/10 of capital as margin for both legs at 3x, so each leg's notional N_SLOT = 0.15
"""
import os
import numpy as np, pandas as pd
import xsec as X

ENTER, EXIT = 0.0003, 0.0001
MAX_POS = 10
N_SLOT = 0.15
BIN_BP, BYB_BP = 4.5, 5.5
MIN_BYBIT_QV = 2e6
MIN_SET = 20
STRESS = 0.25
RF_PCT = 4.5


# ------------------------------------------------------------------ data
def load_bybit(root, p):
    """Bybit hourly close / turnover / funding aligned to the Binance panel p (NaN / empty where no Bybit perp)."""
    grid, syms = p["grid"], p["syms"]
    T, N = len(grid), len(syms)
    close = np.full((T, N), np.nan); qv = np.full((T, N), np.nan, np.float32); fund = {}
    for j, s in enumerate(syms):
        kp, fp = os.path.join(root, "klines", f"{s}.parquet"), os.path.join(root, "funding", f"{s}.parquet")
        fund[s] = (np.zeros(0, np.int64), np.zeros(0))
        if not (os.path.exists(kp) and os.path.exists(fp)):
            continue
        k = pd.read_parquet(kp)
        k = k[(k.ts >= grid[0]) & (k.ts <= grid[-1]) & (k.ts % 3600 == 0)]
        ii = ((k.ts.to_numpy() - grid[0]) // 3600).astype(np.int64)
        close[ii, j] = k.close.to_numpy(); qv[ii, j] = k.turnover.to_numpy(np.float32)
        f = pd.read_parquet(fp)
        fund[s] = (f.ts.to_numpy(np.int64), np.nan_to_num(f.rate.to_numpy(np.float64)))
    return dict(grid=grid, syms=syms, close=close, qv=qv, fund=fund)


def fund7(p, days):
    """(D, N) F7 and the count of settlements in (d - 7 days, d]."""
    D, N = len(days), len(p["syms"])
    f7 = np.zeros((D, N)); cnt = np.zeros((D, N), np.int64)
    for j, s in enumerate(p["syms"]):
        ft, fr = p["fund"][s]
        if len(ft):
            cs = np.r_[0.0, np.cumsum(fr)]
            a, b = np.searchsorted(ft, days - 7 * 86400, side="right"), np.searchsorted(ft, days, side="right")
            f7[:, j] = (cs[b] - cs[a]) / 21.0; cnt[:, j] = b - a
    return f7, cnt


def bybit_ok(p_by, dp_by):
    """Bybit side of the set: complete 30-day 1 h history, mean turnover >= MIN_BYBIT_QV, price known at d."""
    T = len(p_by["grid"]); H30 = X.HIST_D * 24
    end = np.clip(dp_by["hi"] + 1, 0, T)
    D, N = dp_by["P"].shape
    ok = np.zeros((D, N), bool); qv30 = np.zeros((D, N))
    for j in range(N):
        c = p_by["close"][:, j]
        if not np.isfinite(c).any():
            continue
        cp = np.r_[0, np.cumsum(np.isfinite(c))]
        cq = np.r_[0.0, np.cumsum(np.where(np.isfinite(p_by["qv"][:, j]), p_by["qv"][:, j], 0.0))]
        s = np.clip(end - H30, 0, T)
        qv30[:, j] = (cq[end] - cq[s]) / X.HIST_D
        ok[:, j] = ((cp[end] - cp[s]) == H30) & (qv30[:, j] >= MIN_BYBIT_QV) & np.isfinite(dp_by["P"][:, j])
    return ok, qv30


def death_hour(qv, close, min_tail_h=24):
    """Index of the first hour after the last traded bar when a coin's file ends in >= min_tail_h zero-volume bars
    (the Binance archive keeps printing flat zero-volume bars after a delisting), else None."""
    present = np.flatnonzero(np.isfinite(close))
    if not len(present):
        return None
    traded = np.flatnonzero(np.isfinite(close) & (np.nan_to_num(qv) > 0))
    if not len(traded):
        return None
    last = traded[-1]
    return int(last + 1) if present[-1] - last >= min_tail_h else None


def close_at_common_death(p_bin, p_by):
    """Registered rule 'delisting is closed at the last common price': from the first hour either venue is dead
    (file ended or only zero-volume bars), both venues' closes are NaN, so both legs exit at their last common prices."""
    cb, cy = p_bin["close"].copy(), p_by["close"].copy()
    T = len(p_bin["grid"]); cut = {}
    for j, s in enumerate(p_bin["syms"]):
        if not np.isfinite(cy[:, j]).any():
            continue
        hb = death_hour(p_bin["qv"][:, j], cb[:, j]); hy = death_hour(p_by["qv"][:, j], cy[:, j])
        ends = [h for h in (hb, hy) if h is not None]
        py_last = np.flatnonzero(np.isfinite(cy[:, j]))[-1]
        pb_last = np.flatnonzero(np.isfinite(cb[:, j]))[-1] if np.isfinite(cb[:, j]).any() else -1
        if py_last < T - 1:
            ends.append(py_last + 1)                                    # Bybit file ends (closed perp)
        if 0 <= pb_last < T - 1:
            ends.append(pb_last + 1)
        if ends:
            h = min(ends); cb[h:, j] = np.nan; cy[h:, j] = np.nan; cut[s] = h
    return dict(p_bin, close=cb), dict(p_by, close=cy), cut


def build(p_bin, p_by, top=100, common_close=True):
    """Everything the simulation needs, from data known at 00:00 (signals) and the d -> d+1 returns at 00:00 and 01:00.
    common_close: apply close_at_common_death to the price panels used for returns and the set (registered rule)."""
    if common_close:
        p_bin, p_by, _ = close_at_common_death(p_bin, p_by)
    db0, db1 = X.daily_panels(p_bin, 0), X.daily_panels(p_bin, 1)
    dy0, dy1 = X.daily_panels(p_by, 0), X.daily_panels(p_by, 1)
    fi = X.factor_inputs(p_bin, db0)
    days = db0["days"]
    U = X.universe(fi["elig"], fi["qv30"], top=top)
    member = np.zeros_like(fi["elig"])
    rows = np.flatnonzero(U[:, 0] >= 0)
    member[np.repeat(rows, U.shape[1]), U[rows].ravel()] = True
    ok_by, qv30_by = bybit_ok(p_by, dy0)
    f7b, nb = fund7(p_bin, days); f7y, ny = fund7(p_by, days)
    has_by = np.array([np.isfinite(p_by["close"][:, j]).any() for j in range(len(p_bin["syms"]))])
    elig = member & ok_by & (nb > 0) & (ny > 0) & np.isfinite(db0["P"])
    return dict(days=days, elig=elig, member=member, has_by=has_by, delta=f7y - f7b, f7b=f7b, f7y=f7y,
                TRb=db0["TR"], TRy=dy0["TR"], Fb=db0["F"], Fy=dy0["F"], TRb1=db1["TR"], TRy1=dy1["TR"],
                Pb=db0["P"], Py=dy0["P"], qv30_bin=fi["qv30"] / X.HIST_D, qv30_by=qv30_by)


# ------------------------------------------------------------------ simulation
def simulate(elig, delta, enter=ENTER, exit_=EXIT, max_pos=MAX_POS):
    """(D, N) int8 direction held over (d, d+1]; decisions at d use elig[d] and delta[d] only."""
    D, N = elig.shape
    pos = np.zeros((D, N), np.int8); cur = np.zeros(N, np.int8)
    for d in range(D):
        a = np.abs(delta[d]); sg = np.sign(delta[d]).astype(np.int8)
        keep = (cur != 0) & elig[d] & (a >= exit_) & (sg == cur)
        nxt = np.where(keep, cur, 0).astype(np.int8)
        room = max_pos - int(keep.sum())
        cand = np.flatnonzero(elig[d] & ~keep & (a >= enter) & np.isfinite(a))
        if room > 0 and len(cand):
            cand = cand[np.argsort(-a[cand], kind="stable")][:room]
            nxt[cand] = sg[cand]
        pos[d] = cur = nxt
    return pos


def pnl(pos, TRb, TRy, Fb, Fy):
    """Daily P&L on capital: gross, funding part, basis part, cost, net, trades."""
    dirn = pos.astype(float)
    gross = N_SLOT * (dirn * (TRb - TRy)).sum(1)
    funding = N_SLOT * (dirn * (Fy - Fb)).sum(1)
    prev = np.vstack([np.zeros((1, pos.shape[1]), np.int8), pos[:-1]])
    trades = ((pos != prev) & (prev != 0)).sum(1) + ((pos != prev) & (pos != 0)).sum(1)
    cost = N_SLOT * trades * (BIN_BP + BYB_BP) / 1e4
    return dict(gross=gross, funding=funding, basis=gross - funding, cost=cost, net=gross - cost, trades=trades)


def episodes(pos):
    """(coin, direction, first day, last day) for every contiguous same-direction holding."""
    out = []
    for j in np.flatnonzero((pos != 0).any(0)):
        for dirn in (1, -1):
            x = np.r_[False, pos[:, j] == dirn, False].astype(int); dx = np.diff(x)
            for a, b in zip(np.flatnonzero(dx == 1), np.flatnonzero(dx == -1)):
                out.append((int(j), dirn, int(a), int(b - 1)))
    return out


def margin_stress(eps, days, grid, close_b, close_y, Pb, Py):
    """Episodes in which either leg's hourly close went > STRESS against its entry price."""
    n = 0
    for j, dirn, a, b in eps:
        i0 = int((days[a] - grid[0]) // 3600); i1 = int((days[b] + 86400 - grid[0]) // 3600)
        cb, cy = close_b[i0:i1, j], close_y[i0:i1, j]
        long_c, long_e, short_c, short_e = (cb, Pb[a, j], cy, Py[a, j]) if dirn > 0 else (cy, Py[a, j], cb, Pb[a, j])
        bad = (np.nanmin(long_c) < long_e * (1 - STRESS) if np.isfinite(long_c).any() else False) or \
              (np.nanmax(short_c) > short_e * (1 + STRESS) if np.isfinite(short_c).any() else False)
        n += bool(bad)
    return n


def window(elig, days, t_hi, min_set=MIN_SET):
    first = int(np.flatnonzero(elig.sum(1) >= min_set)[0])
    last = min(int(np.searchsorted(days, t_hi, side="left")), len(days) - 1)
    return np.arange(first, last)


def criteria(net, net_lag, days, idx, mean_pos):
    """Registered PASS criteria."""
    from carry import summarize
    s = summarize(net, days, idx)
    dt = pd.to_datetime(days[idx], unit="s")
    ndays = pd.Series(1, index=dt).groupby(dt.year).sum()
    full_years = {y: v for y, v in s["years_pct"].items() if ndays.get(y, 0) >= 182}
    crit = {"annualised net >= 4.5 %": s["ann_pct"] >= RF_PCT,
            "every year (>= 6 months) positive": len(full_years) > 0 and all(v > 0 for v in full_years.values()),
            "worst month > -5 %": s["worst_month_pct"] > -5,
            "NW t >= 2": s["nw_t"] >= 2,
            "1 h lag net > 0": float(net_lag[idx].mean()) > 0,
            "mean open positions >= 2": mean_pos >= 2}
    return s, full_years, crit
