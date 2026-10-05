"""Tests for the R4b low-volatility harness — real library functions on synthetic data.

  1. universe_band: ranks are ordered and disjoint, rank 1..k equals xsec.universe, short days give -1 rows
  2. weights: inverse-vol, sum to total, caps with redistribution, short-arm gross 0.30 at 2.5 % per coin
  3. staggering mean and the beta hedge (unhedged beta 1.5 -> hedged ~0), hedge sign, no position before 60 days
  4. look-ahead: changing returns / vols from day d on leaves every position at <= d unchanged
  5. accounting: net == position . TR - cost x turnover (hedge leg included)
  6. planted low-vol premium world PASSES every criterion; random worlds (no premium) almost never pass
  7. end-to-end on the on-disk market format: runs, structure complete, null world does not pass
"""
import os, sys, tempfile
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import trend as T
import lowvol as LV
import run_r4b as R
from synth import make_market
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


# ------------------------------------------------------------------ 1 universe_band
rng = np.random.default_rng(0)
D, N = 50, 12
elig = rng.random((D, N)) < 0.8; elig[:, 0] = True
qv = rng.lognormal(10, 1, (D, N))
Ub = LV.universe_band(elig, qv, 4, 6); Ua = LV.universe_band(elig, qv, 1, 3)
ok_rows = elig.sum(1) >= 6
check("band: ranks 4-6 disjoint from ranks 1-3 and ordered by volume",
      all(len(set(Ub[d]) & set(Ua[d])) == 0 and qv[d, Ua[d]].min() >= qv[d, Ub[d]].max() for d in np.flatnonzero(ok_rows)))
check("band: ranks 1-3 equal the set xsec.universe(top=3)",
      all(set(Ua[d]) == set(X.universe(elig, qv, top=3)[d]) for d in range(D) if elig[d].sum() >= 3))
check("band: days with fewer than `hi` eligible coins are -1 rows", (Ub[~ok_rows] == -1).all() and (Ub[ok_rows] >= 0).all())

# ------------------------------------------------------------------ 2 weights
V = np.array([[0.02, 0.04, 0.05, 0.1]] * 3); sets = np.array([[0, 1, 2, 3], [0, 1, 2, 3], [-1, -1, -1, -1]])
w = LV.inv_vol_weights(sets, V, 1.0, 1.0)
check("weights: proportional to 1 / vol, sum 1, empty rows zero",
      np.allclose(w[0] * V[0], (w[0] * V[0])[0]) and np.isclose(w[0].sum(), 1) and (w[2] == 0).all())
w2 = LV.inv_vol_weights(np.array([[0, 1, 2, 3, 4]]), np.array([[0.01, 0.1, 0.1, 0.1, 0.1]]), 1.0, 0.20)
check("weights: cap 0.20 binds on the low-vol coin, excess redistributed, sum stays 1", np.allclose(w2, 0.2) and np.isclose(w2.sum(), 1))
w3 = LV.inv_vol_weights(np.array([[0, 1, 2]]), np.array([[0.02, 0.03, 0.04]]), 1.0, 0.20)
check("weights: cap that cannot be met leaves the book under-invested (3 x 0.2)", np.allclose(w3, 0.2) and np.isclose(w3.sum(), 0.6))
w4 = LV.inv_vol_weights(np.tile(np.arange(12), (1, 1)), np.random.default_rng(1).uniform(0.02, 0.1, (1, 12)), LV.SHORT_GROSS, LV.CAP_SHORT)
check("weights: short arm 12 coins -> gross 0.30 at 0.025 each", np.allclose(w4, 0.025) and np.isclose(w4.sum(), 0.30))
w5 = LV.inv_vol_weights(np.array([[0, 1, 2, 3]]), np.array([[0.02, np.nan, 0.05, 0.1]]), 1.0, 1.0)
check("weights: a name without a vol gets zero and the rest renormalise", w5[0, 1] == 0 and np.isclose(w5.sum(), 1))
Wm = np.arange(10, dtype=float)[:, None] * np.ones((1, 2))
check("rolling mean rows: H=3 mean of last 3 rows, zero before the start",
      np.allclose(LV.rolling_mean_rows(Wm, 3)[:, 0], [0 / 3, 1 / 3, 3 / 3, 6 / 3, 9 / 3, 12 / 3, 15 / 3, 18 / 3, 21 / 3, 24 / 3]))


# ------------------------------------------------------------------ synthetic world on arrays
def world(seed, premium, Dd=2000, beta=1.5, k=40):
    r = np.random.default_rng(seed)
    rb = 0.03 * r.standard_normal(Dd)
    vol = r.permutation(np.linspace(0.02, 0.08, k))
    ret = np.empty((Dd, k + 1)); ret[:, 0] = rb
    for i in range(k):
        ret[:, i + 1] = beta * rb + vol[i] * r.standard_normal(Dd) + premium * (0.05 - vol[i])
    ret[-1] = 0.0                                                       # last day has no next-day return, as in the panels
    P = 100 * np.cumprod(1 + np.vstack([np.zeros((1, k + 1)), ret[:-1]]), 0)
    V = T.realised_vol(P)
    U = np.tile(np.arange(1, k + 1), (Dd, 1)); U[:100] = -1
    return U, V, ret, vol


U, Vw, TRw, vol = world(1, 0.0)
Lq, Sq = X.legs(U, Vw, -1.0)
Wl = LV.inv_vol_weights(Lq, Vw, 1.0, LV.CAP_LONG); zero = np.zeros_like(Wl)
full, beta, live, book = LV.hedged_book(Wl, zero, TRw, 0, 1)
live_idx = np.flatnonzero(live)
check("hedge: no position until the book has 60 days of history", live_idx[0] >= np.flatnonzero(np.abs(Wl).sum(1) > 0)[0] + LV.BETA_WIN)
check("hedge: BTC column is minus the estimated beta, alt columns untouched", np.allclose(full[live, 0], -beta[live]) and np.allclose(full[live][:, 1:], Wl[live][:, 1:]))
bi = live_idx[200:]; gross = (full * TRw).sum(1)
bu = np.cov(book[bi], TRw[bi, 0])[0, 1] / TRw[bi, 0].var(ddof=1); bh = np.cov(gross[bi], TRw[bi, 0])[0, 1] / TRw[bi, 0].var(ddof=1)
check("hedge: unhedged book beta ~ 1.5, hedged beta ~ 0", abs(bu - 1.5) < 0.2 and abs(bh) < 0.2, f"{bu:.2f} -> {bh:.2f}")
# 4 look-ahead
d = 1000
V2, T2 = Vw.copy(), TRw.copy(); V2[d + 1:] = np.random.default_rng(5).uniform(0.01, 0.2, V2[d + 1:].shape); T2[d:] = 0.05 * np.random.default_rng(6).standard_normal(T2[d:].shape)
Lq2, _ = X.legs(U, V2, -1.0); Wl2 = LV.inv_vol_weights(Lq2, V2, 1.0, LV.CAP_LONG)
full2, beta2, _, _ = LV.hedged_book(Wl2, zero, T2, 0, 1)
check("look-ahead: positions (alts and hedge) at <= d unchanged when vols after d and returns from d on change",
      np.allclose(full[:d + 1], full2[:d + 1]) and np.allclose(beta[:d + 1], beta2[:d + 1], equal_nan=True))
check("look-ahead: later positions do change (the test can detect a difference)", not np.allclose(full[d + 50:], full2[d + 50:]))
# 5 accounting
idx = np.arange(live_idx[0], len(TRw) - 1)
net, gr = LV.net_series(full, TRw, idx)
prev = np.vstack([np.zeros((1, full.shape[1])), full[idx][:-1]])
check("accounting: net == position . TR - 4.5 bp x |change| over the whole book incl. the hedge",
      np.allclose(net, (full[idx] * TRw[idx]).sum(1) - 4.5e-4 * np.abs(full[idx] - prev).sum(1)))
f7, _, l7, _ = LV.hedged_book(Wl, zero, TRw, 0, 7)
t1 = np.abs(np.diff(full[idx], axis=0)).sum(1).mean(); t7 = np.abs(np.diff(f7[idx], axis=0)).sum(1).mean()
check("staggering: H=7 cuts turnover well below H=1", t7 < 0.6 * t1, f"{t7 / t1:.2f}")

# ------------------------------------------------------------------ 6 planted / null worlds
days = int(pd.Timestamp("2020-01-01", tz="UTC").timestamp()) + np.arange(2000) * 86400
Up, Vp, TRp, _ = world(11, 0.06)
resP = LV.evaluate(Up, Vp, Vp, TRp, TRp, 0, days, perms=150)
c1 = resP["cells"][1]
check("planted low-vol premium: H=1 passes every criterion", c1["pass"],
      f"net {c1['primary']['net_bp']:+.1f} bp/d t {c1['primary']['nw_t']:.1f}, diff {c1['diff_bp']:+.1f}, perm {c1['perm_pct']:.0f}; failed: "
      + ", ".join(k for k, v in c1["criteria"].items() if not v["ok"]))
check("planted low-vol premium: control (EW whole universe, hedged) is ~ flat, factor return is primary - control",
      abs(c1["control"]["net_bp"]) < 0.5 * abs(c1["primary"]["net_bp"]), f"control {c1['control']['net_bp']:+.1f} vs primary {c1['primary']['net_bp']:+.1f}")
npass, pct = 0, []
for sd in range(10):
    Un, Vn, TRn, _ = world(200 + sd, 0.0)
    rn = LV.evaluate(Un, Vn, Vn, TRn, TRn, 0, days, perms=100, ls_arm=False)
    npass += rn["passed"]; pct += [rn["cells"][h]["perm_pct"] for h in (1, 7)]
check("null worlds (no vol premium): at most 1 of 10 passes", npass <= 1, f"{npass} passes")
check("null worlds: permutation percentiles spread (mean 25..75)", 25 < np.mean(pct) < 75, f"mean {np.mean(pct):.0f}")
# an alts-vs-BTC drift must not pass: all alts drift +20 bp/day vs BTC, no vol premium -> beats_control must fail
Ud, Vd, TRd, _ = world(300, 0.0); TRd[:, 1:] += 0.002; TRd[-1] = 0
rd = LV.evaluate(Ud, Vd, Vd, TRd, TRd, 0, days, perms=100, ls_arm=False)
check("alts-vs-BTC drift without a vol premium: primary earns it but does NOT beat the control",
      rd["cells"][1]["primary"]["net_bp"] > 5 and not rd["cells"][1]["criteria"]["beats_control"]["ok"],
      f"primary {rd['cells'][1]['primary']['net_bp']:+.1f}, diff {rd['cells'][1]['diff_bp']:+.1f} t {rd['cells'][1]['diff_nw_t']:.2f}")

# ------------------------------------------------------------------ 7 end to end
rootN = tempfile.mkdtemp(prefix="r4b_null_"); make_market(rootN, kappa=0.0, seed=3)
resN = R.run(rootN, btc="C00USDT", band=(11, 40), perms=40, quiet=True, info_top=False)
pr = resN["primary"]
check("end-to-end (make_market): runs, cells for H=1 and 7 with primary / control / info_ls",
      set(pr["cells"]) == {1, 7} and all(k in pr["cells"][1] for k in ("primary", "control", "info_ls", "criteria")))
check("end-to-end null world: does not pass", not pr["passed"])
check("end-to-end: study starts after the beta warm-up, quintile = 6", pr["days"] > 800 and pr["quintile"] == 6)
print("\nFAILS:", fails)
sys.exit(fails)
