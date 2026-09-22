"""§8.5 — run the UNTUNED §8.3 rule on 5+ years of public 5-min klines.

Same instrument, same cadence, same rule, same accounting. No tuning (§8.2: tuning
cost 7 bp/trade). The only change is the data source and the length of the sample,
which is the entire point: 202 trades could not resolve a 10 bp edge.
"""
import sys, warnings, numpy as np, pandas as pd
warnings.filterwarnings('ignore')
sys.path.insert(0, '/home/nkout/projects/binance_api/runs/harness_v1_feas')
from strat_4h import backtest, H4, FEES
from feas_4h import daily_ic_t

KL = "/home/nkout/projects/binance_api/data/btcusdt_5m_klines.pkl"
BH = 12                       # 12 five-min bars = 1 h
K_FOLDS = 8
T0_FRAC = 0.30


def zs(s, w):
    return (s - s.rolling(w, min_periods=w // 4).mean()) / \
           (s.rolling(w, min_periods=w // 4).std() + 1e-12)


def build_kl(df):
    """The klines-computable subset of the feas_4h feature set, same definitions."""
    d = pd.DataFrame(index=df.index)
    c = df['close']
    lm = np.log(c)
    d['_mid'] = c
    r1 = lm.diff()
    rv1h, rv4h, rv24h = (r1.rolling(w, min_periods=w // 2).std()
                         for w in (BH, 4 * BH, 24 * BH))
    d['vol_ratio_1h_24h'] = rv1h / (rv24h + 1e-12)
    d['vol_ratio_4h_24h'] = rv4h / (rv24h + 1e-12)
    d['rv24h_z'] = zs(rv24h, 24 * BH * 7)
    for h, nm in [(BH, '1h'), (4 * BH, '4h'), (24 * BH, '24h')]:
        d[f'ret_norm_{nm}'] = lm.diff(h) / (rv24h * np.sqrt(h) + 1e-12)
    hi = c.rolling(24 * BH, min_periods=BH).max()
    lo = c.rolling(24 * BH, min_periods=BH).min()
    d['range_pos_24h'] = (c - lo) / (hi - lo + 1e-12)
    buy = df['taker_buy_base']; sell = df['volume'] - buy
    d['vol_imb_1h'] = ((buy - sell).rolling(BH).sum() /
                       ((buy + sell).rolling(BH).sum() + 1e-12))
    d['vol_imb_4h'] = ((buy - sell).rolling(4 * BH).sum() /
                       ((buy + sell).rolling(4 * BH).sum() + 1e-12))
    d['vol_z'] = zs(np.log(df['volume'] + 1e-9), 24 * BH).clip(-8, 8)
    d['trades_z'] = zs(np.log(df['trades'] + 1), 24 * BH).clip(-8, 8)
    dt = pd.to_datetime(df.index, unit='s', utc=True)
    hod = dt.hour + dt.minute / 60
    d['hour_sin'], d['hour_cos'] = np.sin(2*np.pi*hod/24), np.cos(2*np.pi*hod/24)
    d['dow_sin'], d['dow_cos'] = np.sin(2*np.pi*dt.dayofweek/7), np.cos(2*np.pi*dt.dayofweek/7)

    fwd = lm.shift(-H4) - lm
    vol4 = lm.diff(H4).rolling(24 * BH * 7, min_periods=24 * BH).std()
    d['y_norm'] = (fwd / (vol4 + 1e-12)).clip(-5, 5)
    ts = df.index.values
    ok = np.zeros(len(df), bool)
    ok[:len(df) - H4] = (ts[H4:] - ts[:len(df) - H4]) == H4 * 300
    d['_valid'] = ok & np.isfinite(d['y_norm'])
    d['_day'] = ts // 86400
    return d


def main():
    df = pd.read_pickle(KL)
    d = build_kl(df)
    feats = [c for c in d.columns if not c.startswith('_') and not c.startswith('y_')]
    sub = d[np.isfinite(d[feats]).all(axis=1) & np.isfinite(d['_mid'])].copy()
    n = len(sub)
    mid = sub['_mid'].values.astype(float); ts = sub.index.values.astype(np.int64)
    days = sub['_day'].values; y = sub['y_norm'].values; fin = np.isfinite(y)
    print(f"{n:,} bars  {pd.Timestamp(ts[0],unit='s'):%Y-%m-%d} -> "
          f"{pd.Timestamp(ts[-1],unit='s'):%Y-%m-%d}  ({(ts[-1]-ts[0])/86400:.0f} days)")

    t0 = int(n * T0_FRAC); blk = (n - t0) // K_FOLDS
    R, D, DR, TS, FD = [], [], [], [], []
    print(f"\nwalk-forward, {K_FOLDS} folds, UNTUNED rule (q95 / first entry / 4h hold)")
    for k in range(K_FOLDS):
        te_lo = t0 + k * blk
        te_hi = n if k == K_FOLDS - 1 else te_lo + blk
        va_lo = int(te_lo * 0.78)
        rows = []
        for f in feats:
            m_ = fin[:va_lo]
            t, m, _ = daily_ic_t(sub[f].values[:va_lo][m_], y[:va_lo][m_], days[:va_lo][m_])
            if np.isfinite(t): rows.append((f, t, m))
        rows.sort(key=lambda r: -abs(r[1]))
        sc = np.zeros(n)
        for f, t, m in rows[:3]:
            v = pd.Series(sub[f].values)
            sc += np.nan_to_num(np.sign(m) * np.clip(zs(v, 2016).values, -5, 5))
        thr = float(np.nanquantile(np.abs(sc[va_lo:te_lo]), 0.95))
        tr = backtest(sc[te_lo:te_hi], mid[te_lo:te_hi], ts[te_lo:te_hi], thr,
                      entry='first', exit_mode='time', max_hold=H4, fee=0.0)
        rr = np.array([x[2] for x in tr]) if tr else np.array([])
        print(f"  fold {k}  {pd.Timestamp(ts[te_lo],unit='s'):%Y-%m-%d}->"
              f"{pd.Timestamp(ts[te_hi-1],unit='s'):%Y-%m-%d}  top3="
              f"{','.join(f[0][:12] for f in rows[:3])}  n={len(tr):>4} "
              f"gross {rr.mean() if len(rr) else float('nan'):+7.2f}")
        for x in tr:
            R.append(x[2]); D.append(days[te_lo:te_hi][x[0]])
            DR.append(1 if sc[te_lo:te_hi][x[0]] > 0 else -1)
            TS.append(ts[te_lo:te_hi][x[0]]); FD.append(k)

    R = np.array(R); D = np.array(D); DR = np.array(DR); FD = np.array(FD)
    rng = np.random.default_rng(0); ud = np.unique(D)
    idx = {x: np.where(D == x)[0] for x in ud}
    bs = np.array([R[np.concatenate([idx[x] for x in rng.choice(ud, len(ud), True)])].mean()
                   for _ in range(6000)])
    lo, hi = np.percentile(bs, [2.5, 97.5])
    print(f"\n{'='*80}\nPOOLED  n={len(R)} trades over {len(ud)} days, win {100*(R>0).mean():.1f}%")
    print(f"  gross {R.mean():+.2f} bp   95% CI [{lo:+.2f}, {hi:+.2f}]   median {np.median(R):+.2f}")
    print(f"  leg split: long n={int((DR>0).sum())} {R[DR>0].mean():+.2f} | "
          f"short n={int((DR<0).sum())} {R[DR<0].mean():+.2f}")
    yr = pd.Series([pd.Timestamp(t, unit='s').year for t in TS])
    gy = pd.Series(R).groupby(yr).agg(['size', 'mean'])
    print("  by year: " + "  ".join(f"{p} n={int(x['size'])} {x['mean']:+.1f}"
                                    for p, x in gy.iterrows()))
    print(f"  years positive: {int((gy['mean']>0).sum())}/{len(gy)}   "
          f"folds positive: {sum(1 for k in range(K_FOLDS) if (FD==k).sum() and R[FD==k].mean()>0)}/{K_FOLDS}")
    nul = np.array([(rng.permutation(DR) * np.abs(R) * np.sign(R * DR)).mean()
                    for _ in range(4000)])
    print(f"  sign-permutation null: p97.5 {np.percentile(nul,97.5):+.2f} -> "
          f"model percentile {(nul<R.mean()).mean()*100:.1f}%")
    for kk, f in FEES.items():
        ok = (R.mean() - f > 0) and (lo - f > 0)
        print(f"  VERDICT @{kk} ({f:.0f}bp): net {R.mean()-f:+.2f} "
              f"CI [{lo-f:+.2f}, {hi-f:+.2f}] -> {'PASS' if ok else 'fail'}")


if __name__ == "__main__":
    main()
