"""Unconditioned zigzag continuation: after the first +-X bp from an anchor, does price reach +-K*X before
returning to the anchor? 5 s set, 30 min cap. Usage: python continuation_basic.py [w5_60d.parquet]"""
import numpy as np, pandas as pd, sys
d = pd.read_parquet(sys.argv[1] if len(sys.argv) > 1 else '../../data/w5_60d.parquet',
    columns=['ts','future_bid_close','future_ask_close'])
ts = d.ts.values; mid = ((d.future_bid_close+d.future_ask_close)/2).values
lp = np.log(mid)*1e4                     # log price in bp
seg = np.cumsum(np.r_[0, np.diff(ts) > 7])  # contiguous segments
r = np.r_[0, np.diff(lp)]; r[np.r_[False, np.diff(seg) != 0]] = 0
rv = pd.Series(np.abs(r)).rolling(60).sum().values   # trailing 5-min sum|r| (causal)
FEE = 9.0; TMAX = 360  # 30 min cap

def run(X, K, delay=0):
    """Zigzag: anchor A; first close >= X bp from A -> entry in that direction (after `delay` bars).
    Win if close reaches A +/- K*X (same side) before returning to A; else loss. Then re-anchor."""
    out = []
    n = len(lp); i = 0
    while i < n - 1:
        A = lp[i]; s0 = seg[i]; j = i + 1
        while j < n and seg[j] == s0 and abs(lp[j]-A) < X: j += 1
        if j >= n or seg[j] != s0: i = j; continue
        side = np.sign(lp[j]-A); e = j + delay
        if e >= n or seg[e] != s0: i = e; continue
        E = lp[e]; tgt = A + side*K*X; k = e
        res = None
        while k < n and seg[k] == s0 and k - e < TMAX:
            k += 1
            if k >= n or seg[k] != s0: break
            if side*(lp[k]-tgt) >= 0: res = 1; break
            if side*(lp[k]-A) <= 0: res = 0; break
        if res is None: i = k if k < n else n; out.append((j, side, np.nan, np.nan, rv[j])); continue
        out.append((j, side, res, side*(lp[k]-E), rv[j]))
        i = k
    o = pd.DataFrame(out, columns=['j','side','win','gross','rv']).dropna()
    return o

def summ(o, label):
    days = (ts[o.j.values.astype(int)]//86400)
    g = o.gross.values; bs=[]
    rng = np.random.default_rng(0); ud=np.unique(days)
    by = {u: g[days==u] for u in ud}
    for _ in range(500):
        pick = rng.choice(ud, len(ud)); bs.append(np.concatenate([by[u] for u in pick]).mean())
    lo, hi = np.percentile(bs, [2.5, 97.5])
    print(f"{label:34s} n={len(o):6d}  P(win)={o.win.mean():.3f}  gross={g.mean():+6.2f} [{lo:+.2f},{hi:+.2f}]  net@9={g.mean()-FEE:+6.2f}  long={g[o.side>0].mean():+.2f} short={g[o.side<0].mean():+.2f}")

for X in (5, 10, 15, 20):
    for K in (2, 3):
        for delay in (0, 1):
            o = run(X, K, delay)
            summ(o, f"X={X} tgt={K*X} delay={delay*5}s all")
            q = o.rv > np.nanpercentile(rv, 90)
            if q.sum() > 50: summ(o[q], f"X={X} tgt={K*X} delay={delay*5}s hiRV")
    print()
