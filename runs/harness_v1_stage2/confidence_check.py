"""Follow-up on the executed v1 stage-2 probe: is the high-confidence tail real and tradeable?
Controls: causal threshold (quantile of |p-0.5| on the PREVIOUS test month's triggers), one position
at a time, entry delay 0/1/2 bars, day-bootstrap CI, per-month, leg split, blind benchmark."""
import numpy as np, pandas as pd
z = np.load("v1_stage2_scores.npz")
raw = pd.read_parquet("../data/v1_year/v1_15s.parquet", columns=["ts", "future_bid_close", "future_ask_close"])
T0 = int(raw.ts.iloc[0]); slot = np.round((raw.ts.to_numpy() - T0) / 15).astype(np.int64)
G = slot[-1] + 1; lm = np.full(G, np.nan); lm[slot] = np.log((raw.future_bid_close + raw.future_ask_close).to_numpy() / 2)
ts = T0 + np.arange(G) * 15
idx = np.round((z["ts"] - T0) / 15).astype(np.int64)
p = np.full(G, np.nan); p[idx] = z["p_evt_6"]; trig = np.zeros(G, bool); trig[idx] = z["trig_0.05"]
month = pd.to_datetime(ts, unit="s", utc=True).strftime("%y%m").to_numpy(); day = ts // 86400
H = 6; rng = np.random.default_rng(0)
months = [m for m in dict.fromkeys(month[np.isfinite(p)])]

def fwd(t, d):  # bp return from close[t+d] to close[t+d+H]; NaN if gap
    a, b = lm[t + d], lm[t + d + H]
    seg = lm[t:t + d + H + 1]
    return np.where(np.isfinite(seg).all(), (b - a) * 1e4, np.nan)

def boot(v, dd):
    ud = np.unique(dd); s = np.array([v[dd == u].sum() for u in ud]); c = np.array([(dd == u).sum() for u in ud])
    bi = rng.integers(0, len(ud), (2000, len(ud))); m = s[bi].sum(1) / c[bi].sum(1)
    return np.percentile(m, [2.5, 97.5])

cand = trig & np.isfinite(p)
conf = np.abs(p - 0.5)
for q in (0.99, 0.98, 0.95, 0.90):
    for d in (0, 1, 2):
        take = []
        for i, m in enumerate(months[1:], 1):            # causal: threshold from previous month
            prev = cand & (month == months[i - 1]); th = np.quantile(conf[prev], q)
            ii = np.flatnonzero(cand & (month == m) & (conf >= th)); busy = -1
            for t in ii:
                if t > busy and t + d + H < G:
                    take.append(t); busy = t + d + H
        take = np.array(take); side = np.where(p[take] >= 0.5, 1, -1)
        r = np.array([fwd(t, d) for t in take]); ok = np.isfinite(r); take, side, r = take[ok], side[ok], r[ok]
        g = side * r; lo, hi = boot(g - 10, day[take])
        pm = pd.Series(g).groupby(month[take]).mean()
        print(f"top{100 - q * 100:4.0f}% delay{d}: n {len(g):5d} days {len(np.unique(day[take])):3d} acc {(g > 0).mean():.3f} "
              f"gross {g.mean():+6.2f} net@10 {g.mean() - 10:+6.2f} CI [{lo:+.2f},{hi:+.2f}] | L {g[side == 1].mean():+.2f} "
              f"S {g[side == -1].mean():+.2f} | blind {r.mean():+.2f} | months>0 {(pm > 0).sum()}/{len(pm)} "
              f"| max-day share {pd.Series(day[take]).value_counts().iloc[0] / len(g):.2f}")
