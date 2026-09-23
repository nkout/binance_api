"""Zigzag continuation test conditioned on OI / funding / L-S. Descriptive; conditions are causal at the
trigger bar. Win = reach anchor±K·X before returning to anchor (cap TMAX). gross in bp from the actual
entry (close at trigger, or +delay bars). mart_p = martingale win prob given the actual entry distance."""
import numpy as np, pandas as pd, sys, json
f, BAR = sys.argv[1], int(sys.argv[2])
cols = ['ts','future_bid_close','future_ask_close','opt_open_interest_sample',
        'opt_funding_rate_sample','opt_long_short_ratio_sample']
d = pd.read_parquet(f, columns=cols).sort_values('ts').reset_index(drop=True)
for c in cols[3:]: d.loc[d[c] <= 0 if c != 'opt_funding_rate_sample' else d[c] == -1, c] = np.nan
ts = d.ts.values; lp = np.log((d.future_bid_close + d.future_ask_close).values / 2) * 1e4
seg = np.cumsum(np.r_[0, np.diff(ts) > 1.5 * BAR])
oi = d.opt_open_interest_sample.ffill().values
H1 = 3600 // BAR; D3 = 3 * 86400 // BAR
def tz(x):  # trailing 3-day z (causal)
    s = pd.Series(x).ffill(); m = s.rolling(D3, min_periods=D3 // 3).mean(); v = s.rolling(D3, min_periods=D3 // 3).std()
    return ((s - m) / v).values
fz = tz(d.opt_funding_rate_sample.values); lz = tz(d.opt_long_short_ratio_sample.values)
fund_valid = d.opt_funding_rate_sample.notna().values
TMAX = 3600 // BAR

def run(X, K, delay):
    out = []; n = len(lp); i = 0
    while i < n - 1:
        A = lp[i]; s0 = seg[i]; j = i + 1
        while j < n and seg[j] == s0 and abs(lp[j] - A) < X: j += 1
        if j >= n or seg[j] != s0: i = j; continue
        side = np.sign(lp[j] - A); e = j + delay
        if e >= n or seg[e] != s0: i = e; continue
        E = lp[e]; tgt = A + side * K * X; k = e; res = None
        while True:
            k += 1
            if k >= n or seg[k] != s0 or k - e > TMAX: break
            if side * (lp[k] - tgt) >= 0: res = 1; break
            if side * (lp[k] - A) <= 0: res = 0; break
        if res is not None:
            dstop = side * (E - A); dtgt = K * X - dstop
            out.append((i, j, side, res, side * (lp[k] - E), dstop / (dstop + dtgt),
                        (oi[j] / oi[i] - 1) * 1e4,                                  # ΔOI during leg, bp
                        (oi[j] / oi[j - H1] - 1) * 1e4 if j >= H1 and seg[j - H1] == s0 else np.nan,
                        side * fz[j] if fund_valid[j] else np.nan, side * lz[j]))
        i = min(k, n - 1) if k < n else n
    return pd.DataFrame(out, columns=['i','j','side','win','gross','mart_p','doi_leg','doi_1h','fz_side','lz_side'])

def boot(g, days, B=400):
    rng = np.random.default_rng(0); ud = np.unique(days); by = {u: g[days == u] for u in ud}
    bs = [np.concatenate([by[u] for u in rng.choice(ud, len(ud))]).mean() for _ in range(B)]
    return np.percentile(bs, [2.5, 97.5])

rows = []
def cell(o, od, label, cond):
    m = cond.values if hasattr(cond, 'values') else cond
    a, b = o[m], od[od.j.isin(o.j[m])]
    if len(a) < 40: return
    days = ts[a.j.values] // 86400; lo, hi = boot(a.gross.values, days)
    r = dict(cell=label, n=len(a), days=len(np.unique(days)), p=a.win.mean(), mart=a.mart_p.mean(),
             gross=a.gross.mean(), lo=lo, hi=hi, long=a.gross[a.side > 0].mean(), short=a.gross[a.side < 0].mean(),
             g_d1=b.gross.mean())
    rows.append(r)
    print(f"{label:40s} n={r['n']:6d} d={r['days']:3d}  P={r['p']:.3f} mart={r['mart']:.3f} exc={r['p']-r['mart']:+.3f}  "
          f"gross={r['gross']:+6.2f} [{lo:+6.2f},{hi:+6.2f}]  L={r['long']:+6.2f} S={r['short']:+6.2f}  delay1={r['g_d1']:+6.2f}")

for X, K in [(10, 2), (20, 2), (30, 2)]:
    o = run(X, K, 0); od = run(X, K, 1)
    print(f"\n=== {f.split('/')[-1]}  X={X} -> target {K*X}  (cap 60 min)")
    q = o.doi_leg.abs().quantile([1/3]).iloc[0]
    cell(o, od, 'ALL', np.ones(len(o), bool))
    cell(o, od, 'OI leg up   (dOI > 0)', o.doi_leg > 0)
    cell(o, od, 'OI leg down (dOI < 0)', o.doi_leg < 0)
    cell(o, od, 'OI leg strongly up', o.doi_leg > o.doi_leg.quantile(2/3))
    cell(o, od, 'OI leg strongly down', o.doi_leg < o.doi_leg.quantile(1/3))
    cell(o, od, 'OI prior 1h up', o.doi_1h > 0)
    cell(o, od, 'OI prior 1h down', o.doi_1h < 0)
    cell(o, od, 'funding: move AGAINST crowd (z<-1)', o.fz_side < -1)
    cell(o, od, 'funding: neutral', o.fz_side.abs() <= 1)
    cell(o, od, 'funding: move WITH crowd (z>+1)', o.fz_side > 1)
    cell(o, od, 'L/S: move AGAINST crowd (z<-1)', o.lz_side < -1)
    cell(o, od, 'L/S: neutral', o.lz_side.abs() <= 1)
    cell(o, od, 'L/S: move WITH crowd (z>+1)', o.lz_side > 1)
    cell(o, od, 'OI down + against funding crowd', (o.doi_leg < 0) & (o.fz_side < -1))
    cell(o, od, 'OI up + against funding crowd', (o.doi_leg > 0) & (o.fz_side < -1))
    for r in rows[-15:]: r.update(X=X, K=K)
json.dump(rows, open(sys.argv[3], 'w'), indent=1, default=float)
