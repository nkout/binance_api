"""N4 POST — post-hoc robustness diagnostics (NOT pre-registered; descriptive only, cannot change the verdict).

  a. entry at the OPEN of the bar starting at s (first trade after the settlement) instead of the last trade before it
  b. entry delayed: s+1h -> s+2h, and holding s -> s+2h / s+4h
  c. concentration: share of P&L from the top symbols, events per symbol, sorted by |r_prev| bucket
  d. capacity: quote volume traded in the POST hour
"""
import os, sys
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import settle as S
from run_n4 import ROOT, T_LO, T_HI, T_END

p = X.load_panels(ROOT, t_end=T_END)
e, *_ = S.build_events(p, top=S.TOP_EV)
e = e[(e.s >= T_LO) & (e.s < T_HI)].reset_index(drop=True)
j, s, q = e.j.to_numpy(), e.s.to_numpy(np.int64), e.q.to_numpy(float)
C = lambda k: S.close_at(p, j, s + k * 3600)
g0 = p["grid"][0]
opn = np.full(len(e), np.nan); qv_post = np.full(len(e), np.nan)
for sym, idx in e.groupby("sym").groups.items():
    k = pd.read_parquet(os.path.join(ROOT, "klines", f"{sym}.parquet"), columns=["ts", "open", "quote_volume"]).set_index("ts")
    opn[idx] = k.open.reindex(s[idx]).to_numpy(); qv_post[idx] = k.quote_volume.reindex(s[idx]).to_numpy()
bp = lambda a, b: (b / a - 1) * 1e4
rows = {"POST registered: C(s) -> C(s+1h)": -q * bp(C(0), C(1)),
        "a. entry OPEN(s) -> C(s+1h)": -q * bp(opn, C(1)),
        "   jump C(s) -> OPEN(s) (signed -q)": -q * bp(C(0), opn),
        "b. lag 1h: C(s+1h) -> C(s+2h)": -q * bp(C(1), C(2)),
        "   lag 2h: C(s+2h) -> C(s+3h)": -q * bp(C(2), C(3)),
        "   hold 2h: C(s) -> C(s+2h)": -q * bp(C(0), C(2)),
        "   hold 4h: C(s) -> C(s+4h)": -q * bp(C(0), C(4))}
print(f"n {len(e)} | gross bp (net = gross - 9)")
for k, x in rows.items():
    ok = np.isfinite(x); b = S.cluster_boot(x[ok], s[ok])
    d = S.describe(x[ok])
    print(f"  {k:40s} mean {b['mean']:7.2f} [{b['lo']:6.2f}, {b['hi']:6.2f}] | median {d['median']:7.2f} | trimmed {d['trimmed']:7.2f}")

e["post_open"] = -q * bp(opn, C(1))
for lab, sub in [("2023+", e.year >= 2023), ("2025+", e.year >= 2025)]:
    x = e.loc[sub, "post_open"]; print(f"  a. by period {lab}: mean {x.mean():.2f}, median {x.median():.2f}, n {sub.sum()}")
print("\nc. concentration (registered POST net)")
by = e.groupby("sym").post_net.agg(["sum", "size", "mean"]).sort_values("sum", ascending=False)
tot = e.post_net.sum()
print(f"  symbols {len(by)} | top 5 share of total net {by['sum'].head(5).sum() / tot:.1%} | top 20 {by['sum'].head(20).sum() / tot:.1%}"
      f" | symbols with net > 0: {(by['sum'] > 0).mean():.0%}")
print(f"  net excluding the top 10 symbols: mean {e[~e.sym.isin(by.index[:10])].post_net.mean():.2f} bp")
print(by.head(10).round(1).to_string())
print("\n  median-symbol view: mean of per-symbol mean net", round(by["mean"].mean(), 2), "| median", round(by["mean"].median(), 2))
e["absr"] = pd.cut(e.r_prev.abs() * 100, [0.1, 0.2, 0.5, 1.0, 100], right=False)
print("\n  by |r_prev| % bucket\n" + e.groupby("absr", observed=True).agg(n=("post", "size"), gross=("post", "mean"),
      gross_open=("post_open", "mean"), placebo=("plc_post", "mean")).round(2).to_string())
print("\nd. capacity: quote volume in the POST hour (USD)")
print("  quantiles 10/50/90 %:", np.nanpercentile(qv_post, [10, 50, 90]).round(-3))
c = e.assign(qv=qv_post).groupby("s").size()
print(f"  events per settlement timestamp: mean {c.mean():.1f}, median {c.median():.0f}, max {c.max()}")
print("  events per day by year:", (e.groupby("year").size() / e.groupby("year").s.apply(lambda x: (x.max() - x.min()) / 86400 + 1)).round(1).to_dict())
