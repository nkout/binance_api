"""Tests for the V1 harness (DVOL vs forward realised vol) — real library functions on synthetic data.

  1. daily variance: known-sigma 5-min GBM recovers sigma; days with missing bars are NaN
  2. alignment: IV_t is the DVOL close of day t-1; RV_t covers days t..t+29; naive uses days t-30..t-1
  3. leak: scrambling prices after day T leaves IV, HAR features, HAR forecast and naive at <= T unchanged (forecast uses
     only samples whose forward window ended before the refit day)
  4. HAR recovers a planted log-vol AR structure
  5. statistics: Newey-West reduces to plain OLS at 0 lags, block bootstrap covers a known mean
  6. world tests on aligned arrays: efficient IV (proportional premium) -> slope ~ 0, P1 fails; sluggish / noisy IV -> slope ~ 1,
     P1 passes and veto-short beats always-short; a constant premium alone does not pass P1; P&L identity incl. friction
  7. end-to-end on synthetic klines + synthetic DVOL
"""
import os, sys
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import vrp as V
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


def make_klines(ndays, sig_ann, seed=0, start="2020-01-01"):
    """5-min closes with per-day annualised vol path sig_ann (array of length ndays, in %)."""
    rng = np.random.default_rng(seed)
    t0 = int(pd.Timestamp(start, tz="UTC").timestamp())
    sd = np.repeat(np.asarray(sig_ann) / 100 / np.sqrt(365 * BARS), BARS)
    r = sd * rng.standard_normal(ndays * BARS)
    ts = t0 + 300 * np.arange(ndays * BARS)
    return ts, 100 * np.exp(np.cumsum(r))


BARS = V.BARS
# ------------------------------------------------------------------ 1 daily variance
ts, cl = make_klines(400, np.full(400, 60.0), seed=1)
var = V.daily_variance(ts, cl)
vol = V.ann_vol(var.dropna()).mean()
check("daily variance: constant 60 % vol recovered within 3 %", abs(vol / 60 - 1) < 0.03, f"{vol:.1f}")
check("daily variance: first day has 287 returns in-day -> NaN, the rest complete", np.isnan(var.iloc[0]) and var.iloc[1:].notna().all(), f"{var.notna().sum()}")
keep = np.ones(len(ts), bool); keep[300 * 288 + 50] = False
var2 = V.daily_variance(ts[keep], cl[keep])
check("daily variance: a missing bar makes that day (and the next bar's return) NaN", var2.isna().sum() >= 2 and var2.iloc[300] != var2.iloc[300])

# ------------------------------------------------------------------ 2 alignment
path = np.r_[np.full(200, 40.0), np.full(200, 100.0)]
ts, cl = make_klines(400, path, seed=2)
var = V.daily_variance(ts, cl); f = V.features(var)
d150, d185, d210 = f.index[150], f.index[185], f.index[210]
check("RV_t: day 150 (window 150-179) ~ 40; day 185 (window 185-214) mixes 40 and 100; day 210 ~ 100",
      abs(f.rv[d150] / 40 - 1) < 0.1 and 55 < f.rv[d185] < 90 and abs(f.rv[d210] / 100 - 1) < 0.1, f"{f.rv[d150]:.0f} {f.rv[d185]:.0f} {f.rv[d210]:.0f}")
check("naive_t uses days t-30..t-1: day 215 sees 100, day 225 has the pure-100 window; day 205 mixes",
      abs(f.naive[f.index[235]] / 100 - 1) < 0.1 and f.naive[f.index[205]] < 90 and abs(f.naive[f.index[190]] / 40 - 1) < 0.1)
dv_ts = ((f.index - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")).to_numpy(); dv = np.arange(len(f), dtype=float) + 1000.0       # close of bar d = 1000 + d
iv = V.align_iv(dv_ts, dv, f.index)
check("IV_t = close of the DVOL bar of day t - 1", iv.iloc[10] == 1009.0 and np.isnan(iv.iloc[0]))
check("HAR h1 at t is the log vol of day t - 1", np.isclose(f.h1.iloc[50], np.log(V.ann_vol(var.iloc[49]))))

# ------------------------------------------------------------------ 3 leak test
rng = np.random.default_rng(3)
ndays = 1100
sig = 50 * np.exp(0.5 * np.cumsum(0.05 * rng.standard_normal(ndays)) )
ts, cl = make_klines(ndays, sig, seed=4)
var = V.daily_variance(ts, cl); f = V.features(var); fc = V.har_forecast(f, first_fit="2021-01-01", min_obs=100)
T = 800; cut = T * BARS
cl2 = cl.copy(); cl2[cut:] = cl2[cut - 1] * np.exp(np.cumsum(0.03 * np.random.default_rng(9).standard_normal(len(cl2) - cut)))
var2 = V.daily_variance(ts, cl2); f2 = V.features(var2); fc2 = V.har_forecast(f2, first_fit="2021-01-01", min_obs=100)
pre = f.index[:T]
check("leak: HAR inputs and naive at <= day T unchanged when prices after T change",
      np.allclose(f.loc[pre, ["h1", "h5", "h22", "naive"]].to_numpy(), f2.loc[pre, ["h1", "h5", "h22", "naive"]].to_numpy(), equal_nan=True))
check("leak: HAR forecast at <= day T unchanged", np.allclose(fc[pre].to_numpy(), fc2[pre].to_numpy(), equal_nan=True))
check("leak: the future does change after T (the test can detect a difference)", not np.allclose(fc.iloc[T + 40:T + 200].to_numpy(), fc2.iloc[T + 40:T + 200].to_numpy(), equal_nan=True))
rv_pre_ok = np.allclose(f.rv.iloc[:T - 29].to_numpy(), f2.rv.iloc[:T - 29].to_numpy(), equal_nan=True)
check("leak: forward targets whose window ends by day T are unchanged; later ones differ", rv_pre_ok and not np.allclose(f.rv.iloc[T:T + 20], f2.rv.iloc[T:T + 20]))
first = fc.first_valid_index()
check("HAR: first forecast is on/after the first refit day, never before enough history", first >= pd.Timestamp("2021-01-01"), str(first.date()))

# ------------------------------------------------------------------ 4 HAR recovers planted structure
n4 = 2400; r4 = np.random.default_rng(5)
x = np.zeros(n4); phi = 0.985
for i in range(1, n4):
    x[i] = phi * x[i - 1] + 0.06 * r4.standard_normal()
sig4 = 55 * np.exp(x)
ts4, cl4 = make_klines(n4, sig4, seed=6)
f4 = V.features(V.daily_variance(ts4, cl4)); fc4 = V.har_forecast(f4, first_fit="2021-06-01")
okk = np.isfinite(fc4) & np.isfinite(f4.rv)
corr = np.corrcoef(np.log(fc4[okk]), np.log(f4.rv[okk]))[0, 1]
orc = np.corrcoef(np.log(sig4[okk.to_numpy()]), np.log(f4.rv[okk]))[0, 1]
check("HAR: out-of-sample forecast reaches >= 90 % of the oracle correlation (true latent vol vs forward RV)", corr >= 0.9 * orc, f"{corr:.2f} vs oracle {orc:.2f}")

# ------------------------------------------------------------------ 5 statistics
r5 = np.random.default_rng(7)
y = r5.normal(0.3, 1, 3000); X1 = np.column_stack([np.ones(3000), r5.normal(size=3000)])
yy = 1.0 + 2.0 * X1[:, 1] + r5.normal(size=3000)
b0, se0, _ = V.ols_nw(yy, X1, 0)
bols = np.linalg.lstsq(X1, yy, rcond=None)[0]
check("NW at 0 lags equals OLS coefficients and White-type se", np.allclose(b0, bols) and 0.015 < se0[1] < 0.03)
ci = V.mbb_ci(y, 30, 1000, 1)
check("block bootstrap CI covers the true mean 0.3 and is narrow", ci[0] < 0.3 < ci[1] and ci[1] - ci[0] < 0.2, str(np.round(ci, 3)))
b_ar = []
for sd in range(30):
    e = np.convolve(np.random.default_rng(100 + sd).standard_normal(3030), np.ones(30) / 30, "valid")
    b_ar.append(V.nw_t_mean(e, 30)[1])
check("NW(30) on an overlapping series: |t| of a zero-mean MA(29) is mostly < 2 (naive OLS would reject far more)", np.mean(np.abs(b_ar) > 2) <= 0.2, f"{np.mean(np.abs(b_ar) > 2):.2f}")


# ------------------------------------------------------------------ 6 worlds on aligned arrays
def world(seed, kind, n=1800):
    r = np.random.default_rng(seed)
    x = np.zeros(n + 60); phi = 0.985
    for i in range(1, n + 60):
        x[i] = phi * x[i - 1] + 0.05 * r.standard_normal()
    E = 55 * np.exp(x[:n])                                              # the predictable level
    eps = np.convolve(r.standard_normal(n + 29), np.ones(30) / 30, "valid") * np.sqrt(30) * 0.15   # overlapping shock, sd ~ 0.15
    rv = E * np.exp(eps)
    F = E * np.exp(0.05 * r.standard_normal(n))                         # HAR: close to the truth with a little error
    naive = np.roll(E, 30) * np.exp(0.08 * r.standard_normal(n))
    if kind == "efficient":
        iv = 1.10 * E * np.exp(0.0 * r.standard_normal(n))              # proportional premium, uses the full information
    elif kind == "noisy":
        eta = np.convolve(r.standard_normal(n + 19), np.ones(20) / 20, "valid") * np.sqrt(20) * 0.20
        iv = 1.10 * E * np.exp(eta)                                     # IV carries a persistent error HAR does not share
    return pd.date_range("2021-04-01", periods=n), iv, rv, F, naive


d, iv, rv, F, nv = world(1, "efficient")
res = V.analyse(d, iv, rv, F, nv, boot=300)
check("efficient world (IV = 1.10 x E, a pure level premium): P1 slope near 0 / fails, VRP is positive but is not information",
      not res["criteria"]["P1"]["ok"] and res["P0"]["vrp_mean"] > 0, f"{res['criteria']['P1']['detail']}, vrp {res['P0']['vrp_mean']:+.1f}")
fp = []
for sd in range(10):
    dd, iv_, rv_, F_, nv_ = world(10 + sd, "efficient"); fp.append(V.analyse(dd, iv_, rv_, F_, nv_, boot=100)["criteria"]["P1"]["ok"])
check("efficient worlds: P1 passes at most 1 of 10", sum(fp) <= 1, f"{sum(fp)}")
d, iv, rv, F, nv = world(2, "noisy")
res = V.analyse(d, iv, rv, F, nv, boot=300)
check("noisy-IV world: P1 passes, slope near 1", res["criteria"]["P1"]["ok"] and 0.5 < res["P1_har"]["slope"] < 1.5, res["criteria"]["P1"]["detail"])
check("noisy-IV world: veto-short beats always-short (mean at f=2) and P2 passes",
      res["P2"]["veto_short"]["f2"]["mean"] > res["P2"]["always_short"]["f2"]["mean"] and res["criteria"]["P2"]["ok"], res["criteria"]["P2"]["detail"])
check("noisy-IV world: long-timed earns when F > IV", res["P2"]["long_timed"]["f0"]["mean"] > 0)
# P&L identity
iv_t = np.array([60.0, 50.0, 70.0]); rv_t = np.array([50.0, 55.0, 70.0])
check("P&L identity: short earns IV - RV, long earns RV - IV, friction per active entry, flat earns 0",
      np.allclose(V.arm_pnl(np.array([-1.0, -1.0, 0.0]), iv_t, rv_t, 2.0), [8.0, -7.0, 0.0]) and np.allclose(V.arm_pnl(np.array([1.0, 1.0, 1.0]), iv_t, rv_t, 0.0), [-10, 5, 0]))
# veto never vetoes when F tracks E and IV = c E, c > 1 -> equals always short
d, iv, rv, F, nv = world(3, "efficient")
res = V.analyse(d, iv, rv, F, nv, boot=100)
check("level premium only: veto-short is active almost always (>= 95 %), equal to always-short up to the vetoed days",
      res["P2"]["veto_short"]["active_pct"] >= 95, f"{res['P2']['veto_short']['active_pct']:.0f} %")

# ------------------------------------------------------------------ 7 end to end
nd = 1500
rr = np.random.default_rng(11); xs = np.zeros(nd); 
for i in range(1, nd):
    xs[i] = 0.985 * xs[i - 1] + 0.05 * rr.standard_normal()
sigd = 55 * np.exp(xs)
tsE, clE = make_klines(nd, sigd, seed=12)
idxE = pd.date_range("2020-01-01", periods=nd)
dts = ((idxE - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")).to_numpy()
ivs = 1.1 * sigd * np.exp(0.05 * np.random.default_rng(13).standard_normal(nd))
dcl = np.r_[ivs]                                                        # DVOL close of bar d, used as IV at day d + 1
resE = V.run(tsE, clE, dts, dcl, boot=100)
check("end-to-end: runs and yields every block", all(k in resE for k in ("P0", "P1_har", "P1_naive", "P2", "criteria", "veto_minus_always_f2")))
check("end-to-end: sample starts after the first HAR refit (>= 2020-09-01) and ends before the last 29 days", resE["first"] >= "2020-09-01" and resE["n"] > 500, f"{resE['first']} {resE['last']} n {resE['n']}")
check("end-to-end: IV close of bar d feeds day d + 1 (shifting DVOL by one day changes P0 mean)", abs(resE["P0"]["mean_iv"] - np.mean(ivs[1:])) < 40)
print("\nFAILS:", fails)
sys.exit(fails)
