"""Tests for the R4 cross-sectional harness — runs the real loader and backtest on synthetic markets.

  1. look-ahead: factors / eligibility at day d unchanged when every hour after 00:00 of d is scrambled
  2. prices at 00:00 are the close of the bar opening 23:00; funding is paid by longs (TR = R - F)
  3. delisting: the coin is closed at its last close, then contributes 0; never in the universe again
  4. universe: listing age >= 60 d, stable bases excluded at load, size 40
  5. books: dense and fast P&L paths agree; constant legs -> zero turnover; staggering cuts turnover ~1/H
  6. stats: Holm and Newey-West on known inputs
  7. end-to-end, planted reversal world: REV1 gets sign -1 in-sample and PASSES out of sample
  8. end-to-end, null world (pure random walk): nothing passes, permutation percentiles look uniform
"""
import os, sys, tempfile
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import run_r4 as R
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


def make_market(root, n=60, days=2300, kappa=0.0, seed=0, start="2020-03-01"):
    """Hourly random walks with an optional planted daily reversal: drift of day d+1 = -kappa * return of day d."""
    rng = np.random.default_rng(seed)
    t0 = int(pd.Timestamp(start, tz="UTC").timestamp())
    sig_h = rng.uniform(0.004, 0.009, n)
    lvl = rng.lognormal(16, 1.0, n)
    listed = np.zeros(n, int); listed[50:] = rng.integers(200, 500, n - 50)
    delist = np.full(n, days * 24); delist[5] = 600 * 24 + 13              # coin 5 dies at 13:00 on day 600
    R = sig_h[None, None, :] * rng.standard_normal((days, 24, n))
    prev = np.zeros(n)
    for d in range(days):                                                  # planted reversal, day by day
        R[d] += -kappa * prev / 24
        prev = R[d].sum(0)
    path = np.cumsum(R.reshape(days * 24, n), 0)
    hours = np.arange(days * 24)
    os.makedirs(os.path.join(root, "klines"), exist_ok=True); os.makedirs(os.path.join(root, "funding"), exist_ok=True)
    names = [f"C{i:02d}USDT" for i in range(n)]
    for i, s in enumerate(names):
        m = (hours >= listed[i] * 24) & (hours < delist[i]); c = 100 * np.exp(path[m, i])
        k = pd.DataFrame({"ts": t0 + hours[m] * 3600, "open": c, "high": c, "low": c, "close": c,
                          "quote_volume": lvl[i] * rng.lognormal(0, 0.3, m.sum()), "taker_buy_quote": 0.0, "trades": 100})
        k.to_parquet(os.path.join(root, "klines", f"{s}.parquet"), index=False)
        ft = np.arange(t0 + listed[i] * 86400, t0 + min(days * 86400, delist[i] * 3600), 8 * 3600)
        rate = np.full(len(ft), 0.001) if i == 7 else 0.0001 + 0.00005 * rng.standard_normal(len(ft))
        pd.DataFrame({"ts": ft, "rate": rate}).to_parquet(os.path.join(root, "funding", f"{s}.parquet"), index=False)
    # a stablecoin perp that must be excluded at load
    k = pd.DataFrame({"ts": t0 + hours * 3600, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
                      "quote_volume": 1e12, "taker_buy_quote": 0.0, "trades": 1})
    k.to_parquet(os.path.join(root, "klines", "USDCUSDT.parquet"), index=False)
    pd.DataFrame({"ts": [], "rate": []}).to_parquet(os.path.join(root, "funding", "USDCUSDT.parquet"), index=False)
    return t0, names, listed


# ------------------------------------------------------------------ unit checks on one synthetic market
root = tempfile.mkdtemp(prefix="r4_unit_")
t0, names, listed = make_market(root, kappa=0.0, seed=1)
p = X.load_panels(root)
check("load: stablecoin base excluded", "USDCUSDT" not in p["syms"] and len(p["syms"]) == 60)
dp = X.daily_panels(p); fi = X.factor_inputs(p, dp)
days = dp["days"]; j0 = p["syms"].index("C00USDT")
o = int(np.searchsorted(days, t0))                                     # synthetic day 0 on the daily grid
d = o + 300; hbar = (days[d] - 3600 - p["grid"][0]) // 3600
check("P[d] = close of the bar opening at d - 1 h", np.isclose(dp["P"][d, j0], p["close"][hbar, j0]))
j7 = p["syms"].index("C07USDT")
check("funding: 3 events/day at 0.1 % -> F = 0.003, TR = R - F",
      np.isclose(dp["F"][d, j7], 0.003) and np.isclose(dp["TR"][d, j7], dp["P"][d + 1, j7] / dp["P"][d, j7] - 1 - 0.003))
# look-ahead: scramble every hour from 00:00 of day d on
p2 = dict(p); cut = (days[d] - p["grid"][0]) // 3600
p2["close"] = p["close"].copy(); p2["qv"] = p["qv"].copy()
rng = np.random.default_rng(9)
p2["close"][cut:] *= rng.uniform(0.5, 1.5, p2["close"][cut:].shape); p2["qv"][cut:] *= 7
dp2 = X.daily_panels(p2); fi2 = X.factor_inputs(p2, dp2)
same = all(np.allclose(fi["fac"][f][:d + 1], fi2["fac"][f][:d + 1], equal_nan=True) for f in X.FACTORS)
check("look-ahead: all six factors at <= d unchanged when hours >= d scrambled", same,
      str([f for f in X.FACTORS if not np.allclose(fi["fac"][f][:d + 1], fi2["fac"][f][:d + 1], equal_nan=True)]))
check("look-ahead: eligibility and volume rank inputs unchanged",
      np.array_equal(fi["elig"][:d + 1], fi2["elig"][:d + 1]) and np.allclose(fi["qv30"][:d + 1], fi2["qv30"][:d + 1]))
# delisting
j5 = p["syms"].index("C05USDT")
check("delist: closed at last close on the delisting day", np.isclose(dp["P"][o + 601, j5], p["close"][(t0 - p["grid"][0]) // 3600 + 600 * 24 + 12, j5]))
check("delist: zero return afterwards and never eligible again",
      (dp["TR"][o + 601:, j5] == 0).all() and not fi["elig"][o + 601:, j5].any())
# universe / listing age
U = X.universe(fi["elig"], fi["qv30"])
late = [p["syms"].index(f"C{i:02d}USDT") for i in range(50, 60)]
ok_age = all(not fi["elig"][:o + listed[int(p["syms"][j][1:3])] + 60, j].any() for j in late)
check("universe: no coin eligible before 60 days after listing", ok_age)
check("universe: 40 coins once enough are eligible", (U[o + 100:o + 2290] >= 0).all() and len(set(U[o + 200].tolist())) == 40)
# books
L, S = X.legs(U, fi["fac"]["MOM7"], 1.0)
for H in (1, 3, 7):
    b = X.book_returns(L, S, dp["TR"], H); g = X.cohort_gross(L, S, dp["TR"], H)
    check(f"books: dense and fast gross agree (H={H})", np.allclose(b["gross"], g))
Lc, Sc = L.copy(), S.copy(); Lc[o + 100:] = L[o + 100]; Sc[o + 100:] = S[o + 100]
bc = X.book_returns(Lc, Sc, dp["TR"], 1)
check("books: constant legs -> zero turnover", np.allclose(bc["turnover"][o + 101:], 0))
Lr, Sr = X.legs(U, np.random.default_rng(3).random(fi["elig"].shape), 1.0)
t1 = X.book_returns(Lr, Sr, dp["TR"], 1)["turnover"][o + 200:].mean(); t3 = X.book_returns(Lr, Sr, dp["TR"], 3)["turnover"][o + 200:].mean()
check("books: H=3 staggering cuts random-ranking turnover ~1/3", 0.25 < t3 / t1 < 0.42, f"{t3 / t1:.3f}")
# stats
check("holm: known example", np.allclose(X.holm([0.01, 0.04, 0.03, 0.005]), [0.03, 0.06, 0.06, 0.02]))
x = np.random.default_rng(0).normal(0.1, 1, 5000)
check("nw_t(lags=0) == plain t", np.isclose(X.nw_t(x, 0), x.mean() / (x.std() / np.sqrt(len(x))), rtol=1e-3))

# ------------------------------------------------------------------ end-to-end
rootP = tempfile.mkdtemp(prefix="r4_plant_"); make_market(rootP, kappa=0.08, seed=2)
resP = R.run(rootP, perms=200, quiet=True)
rev = {c["H"]: c for c in resP["cells"] if c["factor"] == "REV1"}
check("planted reversal: REV1 sign fixed to -1 in-sample", resP["factors"]["REV1"]["sign"] == -1.0)
check("planted reversal: REV1 H1 passes every criterion", rev[1]["pass"],
      f"net {rev[1]['net_bp']:.1f} bp, c* {rev[1]['c_star_bp']:.1f}, p_holm {rev[1]['p_holm']:.3g}, perm {rev[1]['perm_pct']:.0f}")
rootN = tempfile.mkdtemp(prefix="r4_null_"); make_market(rootN, kappa=0.0, seed=3)
resN = R.run(rootN, perms=200, quiet=True)
pp = np.array([c["perm_pct"] for c in resN["cells"]])
check("null world: no cell passes", not resN["passed"])
check("null world: |gross| small and permutation pcts spread (not all extreme)",
      max(abs(c["gross_bp"]) for c in resN["cells"]) < 15 and ((pp > 2.5) & (pp < 97.5)).mean() >= 0.6,
      f"pcts {np.round(pp).astype(int).tolist()}")
print("\nFAILS:", fails)
sys.exit(fails)
