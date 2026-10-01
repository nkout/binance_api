"""Tests for the R5 funding-carry harness.

  1. simulate(): hysteresis (enter >= 0.03 %, hold until < 0.01 %), 10-position cap keeps the highest
     FUND7, decisions at d use nothing after d, costs on entries and exits only
  2. accounting: CARRY+ = n (Rs - Rp + F) per held day; CARRY- is the mirror
  3. spot eligibility: no spot pair -> never eligible; spot volume floor applies
  4. archive timestamps: milliseconds and microseconds both map to seconds
  5. end-to-end on a synthetic market with spot == perp and one coin at a constant 0.1 % per 8 h:
     CARRY+ holds exactly that coin and earns n * 0.3 % a day minus one round trip
"""
import os, sys, tempfile
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X, carry as C, run_r5 as R5, fetch_archive as FA
from synth import make_market
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


# ------------------------------------------------------------------ 1. simulate()
D = 6; el = np.ones((D, 1), bool); z = np.zeros((D, 1))
fz = np.array([[0.0002], [0.0004], [0.0002], [0.00012], [0.00005], [0.0004]])
r = C.simulate(el, fz, z, z, z)
check("hysteresis: enter >= 0.03 %, hold while >= 0.01 %, exit below", r["pos"][:, 0].tolist() == [False, True, True, True, False, True])
check("costs only on entry / exit days", r["trades"].tolist() == [0, 1, 0, 0, 1, 1]
      and np.allclose(r["cost"], C.N_SLOT * r["trades"] * 12e-4))
rng = np.random.default_rng(0)
el = np.ones((3, 15), bool); fz = np.tile(0.0003 + 0.0001 * rng.permutation(15), (3, 1))
r = C.simulate(el, fz, np.zeros((3, 15)), np.zeros((3, 15)), np.zeros((3, 15)))
top10 = set(np.argsort(-fz[0])[:10].tolist())
check("cap: 10 positions, the 10 highest FUND7", r["pos"][0].sum() == 10 and set(np.flatnonzero(r["pos"][0])) == top10)
el = rng.random((50, 30)) > 0.3; fz = rng.normal(0.0002, 0.0003, (50, 30))
r1 = C.simulate(el, fz, *(np.zeros((50, 30)),) * 3)
el2, fz2 = el.copy(), fz.copy(); el2[26:] = rng.random((24, 30)) > 0.5; fz2[26:] = rng.normal(0, 0.001, (24, 30))
r2 = C.simulate(el2, fz2, *(np.zeros((50, 30)),) * 3)
check("look-ahead: positions at <= d unchanged when inputs after d change", np.array_equal(r1["pos"][:26], r2["pos"][:26]))

# ------------------------------------------------------------------ 2. accounting
D, N = 4, 2
el = np.ones((D, N), bool); fz = np.array([[0.0005, -0.0005]] * D)
Rp = rng.normal(0, 0.02, (D, N)); Rs = Rp + rng.normal(0, 0.001, (D, N)); F = np.array([[0.0015, -0.0015]] * D)
rp = C.simulate(el, fz, Rp, Rs, F); rm = C.simulate(el, fz, Rp, Rs, F, mirror=True)
check("CARRY+ gross = n * (Rs - Rp + F) on the held coin", np.allclose(rp["gross"], C.N_SLOT * (Rs[:, 0] - Rp[:, 0] + F[:, 0])))
check("CARRY- gross = n * (Rp - Rs - F) on the negative-funding coin", np.allclose(rm["gross"], C.N_SLOT * (Rp[:, 1] - Rs[:, 1] - F[:, 1])))
check("funding component: + n F for CARRY+, - n F for CARRY-", np.allclose(rp["funding"], C.N_SLOT * 0.0015) and np.allclose(rm["funding"], C.N_SLOT * 0.0015))

# ------------------------------------------------------------------ 3 / 4. spot eligibility, timestamps
check("timestamps: ms and us -> s", FA.to_seconds(pd.Series([1740787200000, 1740787200000000])).tolist() == [1740787200, 1740787200])
root = tempfile.mkdtemp(prefix="r5_")
t0, names, listed = make_market(root, spot=True, funding={7: 0.001, 3: 0.0001})
p = X.load_panels(root)
cs, qs = C.load_spot(root, p["syms"], p["grid"])
dps, sok = C.spot_panels(p, cs, qs)
j9 = p["syms"].index("C09USDT")
check("spot: a coin without a spot pair is never eligible", not sok[:, j9].any())
qs2 = qs.copy(); qs2[:] = 1e3
_, sok2 = C.spot_panels(p, cs, qs2)
check("spot: 30-day volume floor of 2 M USD / day applies", not sok2.any() and sok.any())

# ------------------------------------------------------------------ 5. end-to-end
res = R5.run(root, quiet=True, bench_syms=("C00USDT", "C01USDT"))
cp = res["CARRY+"]; n = C.N_SLOT
exp_ann = n * 0.003 * 365 * 100
check("end-to-end: CARRY+ uses only the 0.1 %-funding coin", list(cp["top_coins"]) == ["C07USDT"], str(cp["top_coins"]))
check("end-to-end: funding = n * 0.3 %/day, basis = 0", abs(cp["funding_ann_pct"] - exp_ann * cp["days_with_position_pct"] / 100) < 0.05
      and abs(cp["basis_ann_pct"]) < 1e-6, f"funding {cp['funding_ann_pct']:.3f} vs {exp_ann:.3f}, basis {cp['basis_ann_pct']:.2e}")
check("end-to-end: one round trip of cost in total", cp["episodes"] == 1)
b = res["BTCETH"]
check("end-to-end: benchmark = two coins at ~0.01 % funding, hedged -> ~n2 * 0.03 %/day",
      abs(b["funding_ann_pct"] - 2 * 0.5 / (1 + 1 / C.LEV) * 0.0003 * 365 * 100) < 0.6, f"{b['funding_ann_pct']:.2f} %/yr")
print("\nFAILS:", fails)
sys.exit(fails)
