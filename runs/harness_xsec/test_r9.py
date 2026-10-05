"""Tests for the R9 harness — pure functions and the re-pricing on synthetic arrays."""
import os, sys
import numpy as np
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import r9_borrow as R
import carry as C
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


check("base asset: multiplier prefixes stripped, 1INCH kept",
      [R.base_asset(s) for s in ("BTCUSDT", "1000SHIBUSDT", "1000000MOGUSDT", "1MBABYDOGEUSDT", "10000LADYSUSDT", "1INCHUSDT")] == ["BTC", "SHIB", "MOG", "BABYDOGE", "LADYS", "1INCH"])
check("annualise: daily 0.001 -> 0.365", np.isclose(R.annualise_daily(0.001), 0.365))
check("amortised cost: 24 bp over 15 days = 5.84 %/yr", np.isclose(R.AMORT, 0.0024 * 365 / 15))
now = 10 * 86400 * 1000
ev = [(now - d * 3 * 3600 * 1000, -0.0006) for d in range(0, 80)]            # 3-hourly events of -0.06 % for 10 days
check("FUND7: sums the last 7 days only and divides by 21", np.isclose(R.fund7(ev, now), -0.0006 * 57 / 21))             # events at 0, 3, ..., 168 h back (the 168 h one is on the cut)
borrow = {"AAA": dict(daily=0.0005, limit=1000.0), "BBB": dict(daily=0.01, limit=5000.0), "CCC": dict(daily=0.0001, limit=10.0)}
cands = [dict(symbol="AAAUSDT", base="AAA", mark=100.0, last_funding=-0.001, fund7=-0.0012), dict(symbol="BBBUSDT", base="BBB", mark=10.0, last_funding=-0.001, fund7=-0.0012),
         dict(symbol="CCCUSDT", base="CCC", mark=1.0, last_funding=-0.001, fund7=-0.0012), dict(symbol="DDDUSDT", base="DDD", mark=1.0, last_funding=-0.001, fund7=-0.0012),
         dict(symbol="EEEUSDT", base="AAA", mark=100.0, last_funding=-0.0001, fund7=-0.0001)]
rows, v = R.score_s1(cands, borrow)
byb = {r["symbol"]: r for r in rows}
check("S1: coins above the FUND7 threshold are dropped", "EEEUSDT" not in byb and len(rows) == 4)
check("S1: net = income - borrow - amortised cost; limit in USD", np.isclose(byb["AAAUSDT"]["net_ann"], 0.0012 * 3 * 365 - 0.0005 * 365 - R.AMORT) and np.isclose(byb["AAAUSDT"]["limit_usd"], 100000.0))
check("S1: unlisted coin is not borrowable", not byb["DDDUSDT"]["borrowable"] and "net_ann" not in byb["DDDUSDT"])
check("S1: only AAA qualifies (BBB borrow too dear, CCC limit < $50k) -> DEAD with 1 < 3", v["n_qualifying"] == 1 and not v["alive"], str(v))

# re-pricing on synthetic arrays
rng = np.random.default_rng(0); D, N = 300, 6
F = rng.normal(0.0004, 0.0003, (D, N)); Rp = rng.normal(0, 0.02, (D, N)); Rs = Rp + rng.normal(0, 0.002, (D, N))
fz = np.where(rng.random((D, N)) < 0.5, -0.0006, 0.0); elig = np.ones((D, N), bool)
bor = np.array([True, True, True, False, True, False]); rate = np.array([0.001, 0.002, 0.0005, 0.0, 0.01, 0.0])
res0 = R.reprice(elig, fz, Rp, Rs, F, np.ones(N, bool), np.zeros(N))
base = C.simulate(elig, fz, Rp, Rs, F, mirror=True)
check("reprice: all borrowable at zero rate equals the R5 CARRY- net exactly", np.allclose(res0["net"], base["net"]))
res = R.reprice(elig, fz, Rp, Rs, F, bor, rate)
check("reprice: coins that are not borrowable are never held", not res["restricted"]["pos"][:, ~bor].any())
check("reprice: net = restricted net - N_SLOT x sum(pos x rate) every day", np.allclose(res["net"], res["restricted"]["net"] - C.N_SLOT * (res["restricted"]["pos"] * rate[None, :]).sum(1)))
nb = res["restricted"]["net"].mean() * 365; ba = res["borrow"].mean() * 365
k = R.breakeven_multiple(nb, ba, 0.045)
check("break-even multiple: net at k x rates equals risk-free", np.isclose(nb - k * ba, 0.045) if nb > 0.045 else k == 0.0, f"k {k:.2f}")
check("break-even multiple: closed form (0.30 - 0.045) / 0.10 = 2.55, and 0 when net is below risk-free", np.isclose(R.breakeven_multiple(0.30, 0.10, 0.045), 2.55) and R.breakeven_multiple(0.02, 0.10, 0.045) == 0.0)
print("\nFAILS:", fails)
sys.exit(fails)
