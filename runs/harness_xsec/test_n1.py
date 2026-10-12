"""Tests for the N1 alt-short harness — real library functions on synthetic data.

  1. weights: inverse-vol, total 1, cap 2 % binds with redistribution, empty when < 90 % of the band has a vol, EW variant
  2. hedge: alts short, BTC long at the estimated beta, hedged beta ~ 0, nothing before 60 days of book history
  3. look-ahead: vols after d and returns from d on changed -> every position at <= d unchanged
  4. accounting: net == position . TR - 4.5 bp x turnover (hedge incl.); decomposition sums to net
  5. pump: one coin +500 % in a day costs at most cap x 500 % of the notional; worst_coins names it
  6. verdict logic: PASS / ALIVE / DEAD on hand-made stats
  7. planted alt underperformance passes; 10 null worlds pass at most once
  8. end-to-end on the on-disk market format (make_market): runs, window after warm-up, forward block reported
"""
import os, sys, tempfile
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import trend as T
import lowvol as LV
import altshort as A
import run_n1 as R
from synth import make_market
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


# ------------------------------------------------------------------ 1 weights
rng = np.random.default_rng(0)
U = np.tile(np.arange(1, 101), (3, 1)); U[2] = -1
V = np.c_[np.full((3, 1), 0.03), rng.uniform(0.02, 0.1, (3, 100))]
w = A.short_weights(U, V)
check("weights: sum 1, every weight <= 2 %, BTC column 0, empty row stays empty",
      np.allclose(w[:2].sum(1), 1) and w.max() <= 0.02 + 1e-12 and (w[:, 0] == 0).all() and (w[2] == 0).all())
Vc = V.copy(); Vc[0, 1] = 0.001                                        # a very low vol coin would get > 2 %
w = A.short_weights(U, Vc)
check("weights: the cap binds on a low-vol coin and the rest is redistributed", np.isclose(w[0, 1], 0.02) and np.isclose(w[0].sum(), 1))
V2 = V.copy(); V2[0, 1:12] = np.nan; V2[1, 1:10] = np.nan               # 89 vs 91 values
w = A.short_weights(U, V2)
check("weights: formed only when >= 90 of 100 have a vol (89 -> empty, 91 -> formed, missing names 0)",
      (w[0] == 0).all() and np.isclose(w[1].sum(), 1) and (w[1, 1:10] == 0).all())
we = A.short_weights(U, V, ew=True)
check("weights: equal-weight variant is 1 / 100 each", np.allclose(we[0, 1:], 0.01))


# ------------------------------------------------------------------ synthetic array world
def world(seed, premium, D=1100, k=100, beta=1.3):
    r = np.random.default_rng(seed)
    rb = 0.03 * r.standard_normal(D)
    vol = r.uniform(0.02, 0.08, k)
    R_ = np.empty((D, k + 1)); R_[:, 0] = rb
    R_[:, 1:] = beta * rb[:, None] + vol * r.standard_normal((D, k)) - premium
    F = np.zeros((D, k + 1)); F[:, 1:] = 0.0001 + 0.0002 * r.standard_normal((D, k))
    TR = R_ - F
    TR[-1] = 0.0; F[-1] = 0.0
    P = 100 * np.cumprod(1 + np.vstack([np.zeros((1, k + 1)), R_[:-1]]), 0)
    Vv = T.realised_vol(P)
    Uu = np.tile(np.arange(1, k + 1), (D, 1)); Uu[:40] = -1
    days = int(pd.Timestamp("2023-11-01", tz="UTC").timestamp()) + np.arange(D) * 86400
    return Uu, Vv, TR, F, days


Uw, Vw, TRw, Fw, days = world(1, 0.0)
Ws = A.short_weights(Uw, Vw)
full, beta, live = A.book(Ws, TRw, 0, 1)
li = np.flatnonzero(live)
check("hedge: nothing before 60 days of book history", li[0] >= np.flatnonzero(Ws.sum(1) > 0)[0] + LV.BETA_WIN)
check("hedge: alts short (<= 0), BTC long at ~1.3 on average", (full[live][:, 1:] <= 0).all() and abs(full[live, 0].mean() - 1.3) < 0.15,
      f"{full[live, 0].mean():.2f}")
g = (full * TRw).sum(1)[li[100:-1]]; b = TRw[li[100:-1], 0]
check("hedge: hedged book beta to BTC ~ 0", abs(np.cov(g, b)[0, 1] / b.var(ddof=1)) < 0.1, f"{np.cov(g, b)[0, 1] / b.var(ddof=1):+.3f}")

# ------------------------------------------------------------------ 3 look-ahead
d = 600
V3, T3 = Vw.copy(), TRw.copy()
V3[d + 1:] = np.random.default_rng(5).uniform(0.01, 0.2, V3[d + 1:].shape)
T3[d:] = 0.05 * np.random.default_rng(6).standard_normal(T3[d:].shape)
f3, b3, _ = A.book(A.short_weights(Uw, V3), T3, 0, 7)
f7, b7, _ = A.book(Ws, TRw, 0, 7)
check("look-ahead: positions (alts and hedge) at <= d unchanged", np.allclose(f7[:d + 1], f3[:d + 1]) and np.allclose(b7[:d + 1], b3[:d + 1], equal_nan=True))
check("look-ahead: later positions differ (the test can see a change)", not np.allclose(f7[d + 30:], f3[d + 30:]))

# ------------------------------------------------------------------ 4 accounting
idx = A.window(live, days, days[-1] + 1, len(days))
net, gross = LV.net_series(full, TRw, idx)
prev = np.vstack([np.zeros((1, full.shape[1])), full[idx][:-1]])
check("accounting: net == position . TR - 4.5 bp x |change| (hedge incl.)",
      np.allclose(net, (full[idx] * TRw[idx]).sum(1) - 4.5e-4 * np.abs(full[idx] - prev).sum(1)))
dec = A.decompose(full, TRw, Fw, 0, idx)
check("decomposition: alt price + alt funding + BTC price + BTC funding + cost == net", np.allclose(sum(dec.values()), net))
check("decomposition: shorts receive positive alt funding", dec["alt_funding"].mean() > 0)
check("window: never includes the final panel row, starts at the first live day", idx[-1] == len(days) - 2 and idx[0] == li[0])

# ------------------------------------------------------------------ 5 pump
Tp = TRw.copy(); dp = 700; jj = int(np.argmax(full[dp, 1:] * -1)) + 1     # most-shorted coin that day
Tp[dp, jj] = 5.0
fp, _, lp = A.book(Ws, Tp, 0, 1)
wc = A.worst_coins(fp, Tp, idx, 0, ["BTC"] + [f"A{i}" for i in range(100)], days)
check("pump: a +500 % coin costs <= 2 % x 500 % = 10 % of notional, and worst_coins names it",
      -10.01 <= wc["largest"]["pct"] < -5 and wc["largest"]["coin"] == f"A{jj - 1}", f"{wc['largest']['pct']:.2f} %, {wc['largest']['coin']}")

# ------------------------------------------------------------------ 6 verdict logic
base = dict(net_bp=10, nw_t=2.5, median_bp=3, trimmed_bp=8, blocks_n=5, blocks_pos=4, worst_day_pct=-8)
check("verdict: all criteria -> PASS", A.verdict(base, 5)[0] == "PASS")
check("verdict: only t short (1.4) -> ALIVE", A.verdict({**base, "nw_t": 1.4}, 5)[0] == "ALIVE")
check("verdict: t 0.8 -> DEAD", A.verdict({**base, "nw_t": 0.8}, 5)[0] == "DEAD")
check("verdict: t 1.4 but median < 0 -> DEAD", A.verdict({**base, "nw_t": 1.4, "median_bp": -1}, 5)[0] == "DEAD")
check("verdict: t 3 but worst day -25 % -> DEAD", A.verdict({**base, "nw_t": 3, "worst_day_pct": -25}, 5)[0] == "DEAD")
check("verdict: lag <= 0 -> DEAD", A.verdict(base, -0.1)[0] == "DEAD")
check("verdict: 3 of 5 blocks -> DEAD", A.verdict({**base, "blocks_pos": 3}, 5)[0] == "DEAD")

# ------------------------------------------------------------------ 7 planted / null
Up, Vp, TRp, Fp, dys = world(11, 0.0015)
a = A.arm(Up, Vp, TRp, TRp, Fp, 0, dys, dys[-1] + 1, 7)
vp, crit = A.verdict(a["stats"], a["stats"]["lag_net_bp"])
check("planted 15 bp/day alt underperformance: PASS", vp == "PASS",
      f"net {a['stats']['net_bp']:+.1f} t {a['stats']['nw_t']:.1f}, failed: " + ", ".join(k for k, v in crit.items() if not v))
res = []
for sd in range(10):
    Un, Vn, TRn, Fn, dn = world(100 + sd, 0.0)
    Fn[:] = 0; TRn = TRn                                                   # funding off: pure price null
    an = A.arm(Un, Vn, TRn, TRn, Fn, 0, dn, dn[-1] + 1, 7)
    res.append(A.verdict(an["stats"], an["stats"]["lag_net_bp"])[0])
check("null: 10 worlds -> at most 1 PASS", res.count("PASS") <= 1, str(res))

# ------------------------------------------------------------------ 8 end to end
root = tempfile.mkdtemp(prefix="n1_e2e_"); make_market(root, kappa=0.0, seed=3)
r = R.run(root, btc="C00USDT", band=(11, 40), t_hi=int(pd.Timestamp("2026-01-01", tz="UTC").timestamp()), info=False, quiet=True)
check("end-to-end: verdict produced, window after 60-day warm-up, forward block reported",
      r["primary"]["verdict"] in ("PASS", "ALIVE", "DEAD") and r["window"][2] > 800 and "forward" in r and r["forward"]["days"] > 50,
      f"{r['primary']['verdict']}, window {r['window']}, fwd {r.get('forward', {}).get('days')}")
check("end-to-end null market: not PASS", r["primary"]["verdict"] != "PASS")

print(f"\n{fails} failures"); sys.exit(fails > 0)
