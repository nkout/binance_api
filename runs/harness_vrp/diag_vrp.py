"""V1 robustness diagnostics (run AFTER the pre-registered verdict; descriptive, they do not change it)."""
import os, sys
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import vrp as V
DATA = os.path.join(HERE, "..", "..", "data")
kl = pd.read_pickle(os.path.join(DATA, "btcusdt_5m_klines.pkl")); kl = kl[kl.index >= int(pd.Timestamp("2020-01-01", tz="UTC").timestamp())]
dv = pd.read_parquet(os.path.join(DATA, "dvol_daily.parquet"))
f = V.features(V.daily_variance(kl.index.to_numpy(), kl.close.to_numpy())); fc = V.har_forecast(f)
iv = V.align_iv(dv.ts.to_numpy(), dv.close.to_numpy(), f.index)
df = pd.DataFrame(dict(iv=iv, rv=f.rv, F=fc, naive=f.naive)).dropna(); df = df[(df.iv > 0) & (df.rv > 0)]
df["x"] = np.log(df.F / df.iv); df["y"] = np.log(df.rv / df.iv); df["yr"] = df.index.year

print("== P1 slope by period (log RV/IV on log F/IV, NW 30)")
for name, m in (("all", df.yr > 0), ("2021-2022", df.yr <= 2022), ("2023-2026", df.yr >= 2023), ("2024-2026", df.yr >= 2024)):
    d = df[m]; b, se, t = V.ols_nw(d.y.to_numpy(), np.column_stack([np.ones(len(d)), d.x.to_numpy()]), 30)
    print(f"  {name:10s} n {len(d):4d} slope {b[1]:+.3f} (NW t {t[1]:.2f})  mean log(F/IV) {d.x.mean():+.3f}  mean log(RV/IV) {d.y.mean():+.3f}  F<IV on {(d.F < d.iv).mean() * 100:.0f} % of days")
print("== bias: log(RV/F) mean by year (HAR forecast vs realised):", df.assign(b=np.log(df.rv / df.F)).groupby("yr").b.mean().round(3).to_dict())
print("== slope CI by moving-block bootstrap of (x, y) pairs")
rng = np.random.default_rng(0); n = len(df); xs, ys = df.x.to_numpy(), df.y.to_numpy(); sl = []
for _ in range(2000):
    st = rng.integers(0, n - 30 + 1, int(np.ceil(n / 30))); ix = np.concatenate([np.arange(s, s + 30) for s in st])[:n]
    sl.append(np.polyfit(xs[ix], ys[ix], 1)[0])
print("  slope 95 % CI", np.round(np.percentile(sl, [2.5, 97.5]), 3).tolist())
print("== P1 dropping the single worst-RV window (RV/IV max) +-30 days")
w = df.y.idxmax(); d = df[(df.index < w - pd.Timedelta(days=30)) | (df.index > w + pd.Timedelta(days=30))]
b, se, t = V.ols_nw(d.y.to_numpy(), np.column_stack([np.ones(len(d)), d.x.to_numpy()]), 30)
print(f"  worst window around {w.date()} (RV/IV {np.exp(df.y.max()):.2f}); slope {b[1]:+.3f} NW t {t[1]:.2f}")
print("== veto-short at f=2 by period (vol points / entry day) and the break-even friction")
pos = np.where(df.F < df.iv, -1.0, 0.0)
for name, m in (("2021-2022", df.yr <= 2022), ("2023-2026", df.yr >= 2023)):
    p, i, r = pos[m.to_numpy()], df.iv[m].to_numpy(), df.rv[m].to_numpy()
    gross = (p * (r - i)).mean(); act = (p != 0).mean()
    print(f"  {name}: gross {gross:+.2f}, active {act * 100:.0f} %, net@f=2 {gross - 2 * act:+.2f}, break-even f per active entry {gross / act:.2f}")
print("== variance-swap payoff version (convex): short var P&L per vega pt = (IV^2 - RV^2) / (2 IV) - f")
for nm, p in (("veto_short", np.where(df.F < df.iv, 1.0, 0.0)), ("always_short", np.ones(len(df)))):
    vs = (df.iv ** 2 - df.rv ** 2) / (2 * df.iv)
    for fr in (0.0, 2.0):
        x = p * vs.to_numpy() - fr * p
        print(f"  {nm:13s} f={fr:.0f}: mean {x.mean():+.2f}, worst 1 % {np.percentile(x, 1):+.1f}, worst {x.min():+.1f}, by year " + " ".join(f"{y} {v:+.1f}" for y, v in pd.Series(x, index=df.index).groupby(df.yr).mean().items()))
print("== the worst entry days under veto-short (date, IV, F, RV):")
x2 = (pos * (df.rv - df.iv) - 2 * np.abs(pos)).to_numpy(); wi = np.argsort(x2)[:3]
for k in wi: print("  ", df.index[k].date(), f"IV {df.iv.iloc[k]:.0f} F {df.F.iloc[k]:.0f} RV {df.rv.iloc[k]:.0f} P&L {x2[k]:+.1f}")
