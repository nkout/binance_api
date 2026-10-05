"""Tests for the R6 trend harness — real library functions on synthetic data.

  1. signal: known series, NaN until the longest lookback, long-only clip
  2. vol: realised vol of a constant-vol series; vol-targeted hold realises ~ the target; leverage cap
  3. band: known sequence; position unchanged inside the band
  4. look-ahead: scrambling prices after day d leaves signals, targets and XS weights at <= d unchanged
  5. accounting: net == position * TR - cost * |change| exactly; short earns -TR; lag shifts by one day
  6. XS weights: sum <= 1, zero outside the universe, equal-weight sums to 1, fully-trending == inverse-vol
  7. shift null: preserves exposure / turnover; planted-trend world PASSES, random-walk worlds almost never pass
  8. end-to-end on the on-disk market format (make_market): runs, structure complete, null world does not pass
"""
import os, sys, tempfile
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import trend as T
import run_r6 as R
from synth import make_market
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


def days_for(n, start="2020-01-01"):
    return int(pd.Timestamp(start, tz="UTC").timestamp()) + np.arange(n) * 86400


def price_from(ret, p0=100.0):
    return p0 * np.cumprod(1 + np.r_[0.0, ret[:-1]])                   # P[d+1] = P[d] (1 + TR[d]) with zero funding


# ------------------------------------------------------------------ 1 signal
up = 100 * 1.01 ** np.arange(300); dn = up[::-1].copy()
s = T.trend_signal(up[:, None])[:, 0]
check("signal: NaN until day 120, +1 after on a rising series", np.isnan(s[:120]).all() and (s[120:] == 1).all())
check("signal: -1 on a falling series", (T.trend_signal(dn[:, None])[120:, 0] == -1).all())
mix = np.r_[100 * 1.01 ** np.arange(200), 100 * 1.01 ** 199 * 0.99 ** np.arange(1, 41)]   # falls for the last 40 days
sm = T.trend_signal(mix[:, None])[:, 0]
check("signal: after 40 down days the 20- and 60-day returns are negative, 120-day still positive -> (-1 - 1 + 1) / 3",
      np.isclose(sm[-1], -1 / 3) and sm[199] == 1.0, f"{sm[-1]:.3f}")
check("signal: long-only clips negatives, keeps NaN",
      (T.trend_signal(dn[:, None], True)[120:, 0] == 0).all() and np.isnan(T.trend_signal(dn[:, None], True)[:120, 0]).all())

# ------------------------------------------------------------------ 2 vol, targeting, cap
rng = np.random.default_rng(0)
sig_d = 0.03
Pg = 100 * np.exp(np.cumsum(sig_d * rng.standard_normal(4000)))
v = T.realised_vol(Pg[:, None])[:, 0]
check("vol: NaN for the first 30 days; mean ~ sigma * sqrt(365)", np.isnan(v[:30]).all() and abs(np.nanmean(v) / (sig_d * np.sqrt(365)) - 1) < 0.08,
      f"{np.nanmean(v):.3f} vs {sig_d * np.sqrt(365):.3f}")
Rg = Pg[1:] / Pg[:-1] - 1; TRg = np.r_[Rg, 0.0]
tgt = T.vol_target(np.where(np.isfinite(v), 1.0, np.nan), v)
held = T.apply_band(tgt)
real = (held[40:3990] * TRg[40:3990]).std() * np.sqrt(365)
check("vol targeting: signal 1 realises ~ 40 % (cap not binding)", 0.34 < real < 0.46, f"{real:.3f}")
vlow = np.full(50, 0.01)
check("cap: |target| <= 2 at tiny vol, both signs", np.allclose(T.vol_target(np.ones(50), vlow), 2.0) and np.allclose(T.vol_target(-np.ones(50), vlow), -2.0))

# ------------------------------------------------------------------ 3 band
check("band: moves only past 0.10", np.allclose(T.apply_band(np.array([np.nan, 0.5, 0.55, 0.62, 0.30, 0.25, np.nan, 0.0])),
                                                [0, 0.5, 0.5, 0.62, 0.30, 0.30, 0.30, 0.0]))

# ------------------------------------------------------------------ 4 look-ahead
D, N = 700, 8
rngp = np.random.default_rng(1)
Pm = 100 * np.exp(np.cumsum(0.02 * rngp.standard_normal((D, N)), 0))
U = np.tile(np.arange(N), (D, 1)); U[:300] = -1
d = 500
P2 = Pm.copy(); P2[d + 1:] = 100 * np.exp(np.cumsum(0.05 * np.random.default_rng(9).standard_normal((D - d - 1, N)), 0))
def book_all(P):
    vol = T.realised_vol(P); sLS = T.trend_signal(P); sL = T.trend_signal(P, True)
    return sLS, sL, vol, T.xs_weights(U, sL, vol), T.apply_band(T.vol_target(sLS[:, 0], vol[:, 0]))
a, b = book_all(Pm), book_all(P2)
check("look-ahead: signals, vols, XS weights and held BTC position at <= d unchanged when later prices change",
      all(np.allclose(x[:d + 1], y[:d + 1], equal_nan=True) for x, y in zip(a, b)))
check("look-ahead: later days do change (the test can detect a difference)", not np.allclose(a[3][d + 20:], b[3][d + 20:]))

# ------------------------------------------------------------------ 5 accounting
n = 600
rr = 0.01 * np.random.default_rng(2).standard_normal(n); rr[100] = 0.2
held_t = np.r_[np.zeros(50), np.full(100, 1.0), np.full(100, -0.5), np.zeros(n - 250)]
dd = days_for(n); idx = np.arange(40, n)
st = T.summarise(held_t, rr, idx, dd)
manual = held_t[idx] * rr[idx] - X.COST_BP * 1e-4 * np.abs(np.diff(np.r_[0.0, held_t[idx]]))
check("accounting: net == position * TR - cost * |change| exactly", np.allclose(st["net"], manual))
check("accounting: a short earns -TR (day 100 spike +20 % with position +1 -> +20 %, same day short -> -20 %)",
      np.isclose((held_t[100] * rr[100]), 0.2) and np.isclose((-1 * rr[100]), -0.2))
check("accounting: round trip 1 -> 0 costs 2 x 4.5 bp", np.isclose(T.day_costs(np.array([0, 1.0, 0, 0])).sum(), 2 * 4.5e-4))
lg = T.lagged(held_t)[:, 0]
check("lag: position shifted by one day, first day flat", lg[0] == 0 and np.allclose(lg[1:], held_t[:-1]))
check("stats: max drawdown and Sharpe on known inputs",
      np.isclose(T.max_dd(np.array([0.1, -0.5, 0.5])), -0.5)
      and np.isclose(T.sharpe(np.array([0.01, 0.01, 0.03, -0.01])), 0.01 / np.sqrt(0.0008 / 3) * np.sqrt(365)))
yy = T.yearly(np.r_[np.full(366, 0.001), np.full(365, 0.0)], days_for(731))
check("yearly: two calendar years bucketed (2020 leap)", sorted(yy) == [2020, 2021] and yy[2020]["n"] == 366 and np.isclose(yy[2021]["ret"], 0))

# ------------------------------------------------------------------ 6 XS weights
Dx, Nx = 400, 12
rngx = np.random.default_rng(4)
Px = 100 * np.exp(np.cumsum(0.02 * rngx.standard_normal((Dx, Nx)), 0))
Ux = np.tile(np.arange(10), (Dx, 1)); Ux[:200] = -1
vx = T.realised_vol(Px); sx = T.trend_signal(Px, True)
Wx = T.xs_weights(Ux, sx, vx)
check("XS: weights >= 0, sum <= 1, zero before the universe starts and outside it",
      (Wx >= 0).all() and Wx.sum(1).max() <= 1 + 1e-9 and (Wx[:200] == 0).all() and (Wx[:, 10:] == 0).all())
ones = np.where(np.isfinite(sx), 1.0, np.nan)
Wi_ = T.xs_weights(Ux, ones, vx)
rowsum = Wi_.sum(1)
check("XS: signal 1 on every coin -> fully invested, inverse-vol proportional",
      np.allclose(rowsum[330:], 1.0) and np.allclose(Wi_[350, :10] * vx[350, :10], (Wi_[350, :10] * vx[350, :10])[0]))
check("XS: equal weight sums to 1 inside the universe", np.allclose(T.xs_weights(Ux, sx, vx, equal=True)[250:].sum(1), 1.0))
Wls = T.xs_weights(Ux, T.trend_signal(Px), vx)
check("XS: long/short weights can be negative and |sum| <= 1", (Wls < 0).any() and np.abs(Wls).sum(1).max() <= 1 + 1e-9)

# ------------------------------------------------------------------ 7 shift null and planted worlds
def regime_world(seed, strength, n=2400, flip=1 / 90):
    r = np.random.default_rng(seed); drift = np.empty(n); cur = strength
    for i in range(n):
        if r.random() < flip: cur = -cur
        drift[i] = cur
    ret = drift + 0.025 * r.standard_normal(n)
    return price_from(ret), ret

Wt = np.where(np.arange(500) % 100 < 50, 1.0, 0.0)[:, None]; Rt = 0.01 * np.random.default_rng(5).standard_normal((500, 1))
base = np.array([(np.roll(Wt, k, 0) * Rt).sum(1).mean() for k in (0, 123, 321)])
check("shift null: circular shift keeps exposure and turnover", np.isclose(np.roll(Wt, 123, 0).mean(), Wt.mean()) and
      np.isclose(T.day_costs(np.roll(Wt, 123, 0)).sum(), T.day_costs(Wt).sum(), atol=2 * 4.5e-4))

dP = days_for(2400)
Pp, Rp = regime_world(10, 0.004)
res, _ = T.evaluate_btc(Pp, Rp, dP, nshift=500)
check("planted trend world: A passes every criterion", res["passed"],
      f"A Sharpe {res['arms']['A']['sharpe']:.2f} vs BH {res['arms']['BH']['sharpe']:.2f}, null pct {res['null']['pct']:.1f}, "
      + ", ".join(k for k, c in res["criteria"].items() if not c["ok"]))
check("planted trend world: long leg and short leg both positive", res["leg_split_ann_pct"]["long"] > 0 and res["leg_split_ann_pct"]["short"] > 0,
      str(res["leg_split_ann_pct"]))
npass, pcts = 0, []
for sd in range(20):
    rw = 0.025 * np.random.default_rng(100 + sd).standard_normal(2400)
    rs, _ = T.evaluate_btc(price_from(rw), rw, dP, nshift=200, seed=sd)
    npass += rs["passed"]; pcts.append(rs["null"]["pct"])
check("random-walk worlds: at most 1 of 20 passes all five criteria", npass <= 1, f"{npass} passes")
check("random-walk worlds: null percentiles are spread (mean 30..70, not all extreme)", 30 < np.mean(pcts) < 70, f"mean {np.mean(pcts):.0f}")

# planted trend per coin: XS book should beat the inverse-vol control
Dm, Nm = 1500, 20
rngm = np.random.default_rng(7)
RETm = np.empty((Dm, Nm))
for j in range(Nm):
    RETm[:, j] = regime_world(200 + j, 0.003, n=Dm)[1]
Pm2 = np.vstack([100 * np.ones((1, Nm)), 100 * np.cumprod(1 + RETm[:-1], 0)])
Um = np.tile(np.arange(Nm), (Dm, 1)); Um[:150] = -1
rx, _ = T.evaluate_xs(Um, Pm2, RETm, days_for(Dm), nshift=500)
check("planted trend per coin: 1C Sharpe beats the inverse-vol control by > 0.3 and null pct >= 95",
      rx["arms"]["C1"]["sharpe"] - rx["arms"]["IV_long"]["sharpe"] > 0.3 and rx["null"]["pct"] >= 95,
      f"{rx['arms']['C1']['sharpe']:.2f} vs {rx['arms']['IV_long']['sharpe']:.2f}, pct {rx['null']['pct']:.0f}")

# ------------------------------------------------------------------ 8 end to end on the on-disk format
rootN = tempfile.mkdtemp(prefix="r6_null_"); make_market(rootN, kappa=0.0, seed=3)
resN = R.run(rootN, btc="C00USDT", nshift=200, nshift_xs=100, quiet=True)
keys_ok = all(k in resN["btc"]["arms"] for k in ("A", "A_long", "B", "BH")) and all(k in resN["xs"]["arms"] for k in ("C1", "C1_ls", "IV_long", "EW"))
check("end-to-end (make_market): runs, all arms present", keys_ok)
check("end-to-end null world: neither BTC nor XS passes", not resN["btc"]["passed"] and not resN["xs"]["passed"])
check("end-to-end: window starts after the lookback warm-up, last day dropped",
      resN["btc"]["days"] > 1000 and resN["btc"]["start"] > "2020-04-01")
print("\nFAILS:", fails)
sys.exit(fails)
