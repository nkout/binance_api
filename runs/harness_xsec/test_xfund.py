"""Tests for N2 (XFUND) — the Bybit fetcher with a fake API, and the strategy on synthetic two-venue worlds.

  1. fetcher: instruments pagination (Trading + Closed, perps only), backward paging of funding / klines, window limits,
     closed perp's kline window ends at its last settlement, symbol mapping (exact, 10^k multipliers, ambiguity)
  2. F7: per-8 h normalisation makes 4 h and 8 h venues with the same economic rate give delta ~ 0
  3. simulation: <= 10 positions, ranked by |delta|, direction = sign(delta), exit below 0.01 % / on a sign flip /
     leaving the set, flip = exit + entry
  4. accounting: gross = N_SLOT * dir * (TR_bin - TR_byb), funding + basis = gross, cost = 20 bp of N_SLOT per round trip
  5. look-ahead: data from a cut on changed -> every position on days before the cut unchanged
  6. planted persistent spread passes every criterion; 10 iid-spread null worlds pass at most once
  7. margin stress counts an episode whose long leg falls > 25 %
"""
import os, sys, tempfile
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fetch_bybit as FB
import xfund as XF
fails = 0
HOUR, DAY = 3600, 86400


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


# ------------------------------------------------------------------ 1 fetcher with a fake API
class FakeAPI:
    def __init__(self):
        self.ins = {"Trading": [dict(symbol=f"T{i}USDT", contractType="LinearPerpetual", quoteCoin="USDT", status="Trading") for i in range(3)]
                    + [dict(symbol="BTC-26DEC25", contractType="LinearFutures", quoteCoin="USDT", status="Trading")],
                    "Closed": [dict(symbol="DEADUSDT", contractType="LinearPerpetual", quoteCoin="USDT", status="Closed"),
                               dict(symbol="USDCPERP", contractType="LinearPerpetual", quoteCoin="USDC", status="Closed")]}
        t0 = 1_600_000_000_000
        self.fts = t0 + np.arange(1000) * 8 * HOUR * 1000          # 1000 settlements
        self.kts = t0 + np.arange(5000) * HOUR * 1000
        self.calls = 0

    def __call__(self, path):
        self.calls += 1
        q = dict(x.split("=", 1) for x in path.split("?", 1)[1].split("&"))
        if path.startswith("/instruments-info"):
            lst = self.ins[q["status"]]
            if q["cursor"] == "":                                   # two pages for Trading
                return {"list": lst[:2], "nextPageCursor": "p2" if len(lst) > 2 else ""}
            return {"list": lst[2:], "nextPageCursor": ""}
        if path.startswith("/funding"):
            e = int(q["endTime"]); ts = self.fts[self.fts <= e][-int(q["limit"]):][::-1]
            return {"list": [{"fundingRateTimestamp": str(t), "fundingRate": "0.0001"} for t in ts]}
        if path.startswith("/kline"):
            e, s = int(q["end"]), int(q["start"]); ts = self.kts[(self.kts <= e) & (self.kts >= s)][-int(q["limit"]):][::-1]
            return {"list": [[str(t), "1", "1", "1", "2.5", "10", "25"] for t in ts]}


api = FakeAPI()
ins = FB.instruments(api)
check("fetcher: instruments = USDT perps from both statuses, pagination followed",
      sorted(x["symbol"] for x in ins) == ["DEADUSDT", "T0USDT", "T1USDT", "T2USDT"])
with tempfile.TemporaryDirectory() as d:
    end = int(api.fts[600]) + 1; start = int(api.kts[0])
    s, st, n = FB.fetch_one("DEADUSDT", "DEADUSDT", end, start, getter=api, out=d)
    f = pd.read_parquet(os.path.join(d, "funding", "DEADUSDT.parquet")); k = pd.read_parquet(os.path.join(d, "klines", "DEADUSDT.parquet"))
    check("fetcher: funding paged backwards to the start, nothing at / after `end`, sorted, unique",
          len(f) == 601 and f.ts.is_monotonic_increasing and f.ts.is_unique and f.ts.max() * 1000 < end)
    check("fetcher: klines paged in 1000-bar pages, every bar before `end`, values parsed",
          len(k) == 4801 and k.ts.max() * 1000 < end and (np.diff(k.ts) == HOUR).all()
          and (k.close == 2.5).all() and (k.turnover == 25).all(), f"{len(k)} bars")
    api2 = FakeAPI(); api2.fts = api2.fts[:300]                       # a perp closed after 300 settlements
    FB.fetch_one("GONEUSDT", "GONEUSDT", int(api2.kts[-1]) + 10 ** 9, start, getter=api2, out=d)
    k2 = pd.read_parquet(os.path.join(d, "klines", "GONEUSDT.parquet"))
    check("fetcher: closed perp -> kline window ends at its last settlement + 8 h",
          len(k2) == 300 * 8 and k2.ts.max() * 1000 < int(api2.fts[-1]) + 8 * HOUR * 1000, f"{len(k2)} bars")
    c0 = api.calls; FB.fetch_one("DEADUSDT", "DEADUSDT", end, start, getter=api, out=d)
    check("fetcher: resumable (second call makes no request)", api.calls == c0)
m = FB.map_symbols(["BTCUSDT", "1000SHIBUSDT", "1000PEPEUSDT", "XUSDT", "1INCHUSDT"],
                   ["BTCUSDT", "SHIB1000USDT", "1000PEPEUSDT", "10000PEPEUSDT", "1INCHUSDT", "X1000USDT", "10000XUSDT"])
check("mapping: exact first, normalised multiplier match, ambiguous normalised names dropped",
      m == {"BTCUSDT": "BTCUSDT", "1000SHIBUSDT": "SHIB1000USDT", "1000PEPEUSDT": "1000PEPEUSDT", "1INCHUSDT": "1INCHUSDT"}, str(m))


# ------------------------------------------------------------------ synthetic two-venue world
def world(seed, spread=0.0, n=30, days=1000, noise=0.00005, persist=True, by4h=True):
    """Same coin on two venues. Binance funding 8 h; Bybit funding 8 h then 4 h (rate halved) from mid-sample.
    Planted: coins 0..7 carry a persistent Bybit-minus-Binance spread of +-spread per 8 h, sign switching every 150 days.
    persist False: the spread is iid per settlement with sd = spread (no persistence)."""
    rng = np.random.default_rng(seed)
    t0 = int(pd.Timestamp("2021-01-01", tz="UTC").timestamp())
    T = days * 24; grid = t0 + np.arange(T, dtype=np.int64) * HOUR
    lr = 0.008 * rng.standard_normal((T, n)); cb = 100 * np.exp(np.cumsum(lr, 0))
    cy = cb * np.exp(0.0002 * rng.standard_normal((T, n)))
    syms = [f"C{i:02d}USDT" for i in range(n)]
    fb, fy = {}, {}
    hb = np.arange(8, T, 8)
    half = T // 2
    hy = np.r_[np.arange(8, half, 8), np.arange(half, T, 4)] if by4h else hb
    for i in range(n):
        base = 0.0001 + 0.00005 * rng.standard_normal(len(hb))
        fb[syms[i]] = (t0 + hb * HOUR, base)
        b_at = np.interp(hy, hb, base)
        if persist:
            sp = (spread * np.where((hy // (150 * 24)) % 2 == 0, 1, -1)) if i < 8 else np.zeros(len(hy))
        else:
            sp = spread * rng.standard_normal(len(hy))
        r = b_at + sp + noise * rng.standard_normal(len(hy))
        scale = np.where(np.r_[np.diff(hy), 8] == 4, 0.5, 1.0) if by4h else 1.0
        fy[syms[i]] = (t0 + hy * HOUR, r * scale)
    qv = rng.lognormal(16, 0.5, (1, n)) * rng.lognormal(0, 0.2, (T, n))
    pb = dict(grid=grid, syms=syms, close=cb, qv=qv.astype(np.float32), fund=fb)
    py = dict(grid=grid, syms=syms, close=cy, qv=(qv * 0.5).astype(np.float32), fund=fy)
    return pb, py


TOP = 20
pb, py = world(1, spread=0.0004)
B = XF.build(pb, py, top=TOP)
# ------------------------------------------------------------------ 2 F7 normalisation
late = np.arange(600, 990)
d_other = B["delta"][late][:, 8:]
check("F7: Bybit at 4 h with half rates vs Binance at 8 h -> delta ~ 0 for unplanted coins (per-8 h normalisation)",
      np.abs(d_other).mean() < 0.00003, f"mean |delta| {np.abs(d_other).mean():.6f}")
check("F7: planted coins show |delta| ~ 0.04 %", abs(np.abs(B['delta'][late][:, :8]).mean() - 0.0004) < 0.00006,
      f"{np.abs(B['delta'][late][:, :8]).mean():.5f}")

# ------------------------------------------------------------------ 3 simulation rules
rng = np.random.default_rng(3)
D, N = 40, 15
el = np.ones((D, N), bool); dl = rng.normal(0, 0.0004, (D, N))
pos = XF.simulate(el, dl)
check("sim: never more than 10 positions", (np.abs(pos).sum(1) <= 10).all())
ok = True
for d in range(D):
    held = pos[d] != 0
    ok &= ((pos[d][held] == np.sign(dl[d][held])).all() and (np.abs(dl[d][held]) >= XF.EXIT).all())
check("sim: every held coin has direction = sign(delta) and |delta| >= exit", ok)
el2 = np.ones((3, 2), bool); dl2 = np.array([[0.0005, 0.0002], [-0.0004, 0.0002], [-0.00005, 0.0002]])
p2 = XF.simulate(el2, dl2)
check("sim: enter at 0.03 %, flip on sign change, exit below 0.01 %; 0.02 % never enters",
      p2[:, 0].tolist() == [1, -1, 0] and (p2[:, 1] == 0).all())
dl3 = np.full((2, 12), 0.0005) + np.arange(12) * 1e-5; dl3[1, 11] = 0.0005
p3 = XF.simulate(np.ones((2, 12), bool), dl3)
check("sim: with 12 candidates the 10 largest |delta| are taken; held positions keep their slot",
      set(np.flatnonzero(p3[0])) == set(range(2, 12)) and (p3[1] == p3[0]).all())
el4 = np.ones((3, 1), bool); el4[1] = False
check("sim: leaving the set closes the position", XF.simulate(el4, np.full((3, 1), 0.0005))[:, 0].tolist() == [1, 0, 1])

# ------------------------------------------------------------------ 4 accounting
pos = XF.simulate(B["elig"], B["delta"])
P = XF.pnl(pos, B["TRb"], B["TRy"], B["Fb"], B["Fy"])
d, j = np.argwhere(pos != 0)[[5, 50, 500]].T
hand = [XF.N_SLOT * pos[a, b] * (B["TRb"][a, b] - B["TRy"][a, b]) for a, b in zip(d, j)]
check("accounting: per-coin gross = 0.15 x dir x (TR_binance - TR_bybit)",
      all(np.isclose(h, XF.N_SLOT * pos[a, b] * (B["TRb"][a, b] - B["TRy"][a, b])) for h, a, b in zip(hand, d, j))
      and np.allclose(P["gross"], XF.N_SLOT * (pos * (B["TRb"] - B["TRy"])).sum(1)))
check("accounting: funding + basis == gross, net == gross - cost", np.allclose(P["funding"] + P["basis"], P["gross"]) and np.allclose(P["net"], P["gross"] - P["cost"]))
ent = ((pos != 0) & (np.vstack([np.zeros((1, pos.shape[1])), pos[:-1]]) == 0)).sum()
check("accounting: total cost = 0.15 x 10 bp x (entries + exits)", np.isclose(P["cost"].sum(), XF.N_SLOT * 10e-4 * P["trades"].sum())
      and P["trades"].sum() >= 2 * ent - (pos[-1] != 0).sum())

# ------------------------------------------------------------------ 5 look-ahead
cut_day = 600; cut = int(B["days"][cut_day]) + 12 * HOUR; ci = (cut - pb["grid"][0]) // HOUR
pb2 = dict(pb, close=pb["close"].copy(), qv=pb["qv"].copy(), fund=dict(pb["fund"]))
py2 = dict(py, close=py["close"].copy(), fund=dict(py["fund"]))
r2 = np.random.default_rng(9)
pb2["close"][ci:] *= np.exp(r2.normal(0, 0.05, pb2["close"][ci:].shape)); py2["close"][ci:] *= 1.3
pb2["qv"][ci:] = r2.lognormal(10, 3, pb2["qv"][ci:].shape)
for s, (ft, fr) in py["fund"].items():
    py2["fund"][s] = (ft, np.where(ft >= cut, -0.01, fr))
B2 = XF.build(pb2, py2, top=TOP)
pos2 = XF.simulate(B2["elig"], B2["delta"])
check("look-ahead: positions on every day up to the cut day unchanged", (pos[:cut_day + 1] == pos2[:cut_day + 1]).all())
check("look-ahead: later positions change (the test sees a difference)", not (pos[cut_day + 2:] == pos2[cut_day + 2:]).all())

# ------------------------------------------------------------------ 6 planted / null
def evaluate(B, min_set=10):
    pos = XF.simulate(B["elig"], B["delta"])
    P0 = XF.pnl(pos, B["TRb"], B["TRy"], B["Fb"], B["Fy"]); P1 = XF.pnl(pos, B["TRb1"], B["TRy1"], B["Fb"], B["Fy"])
    idx = XF.window(B["elig"], B["days"], B["days"][-1] + 1, min_set)
    s, fy, crit = XF.criteria(P0["net"], P1["net"], B["days"], idx, float((pos[idx] != 0).sum(1).mean()))
    return s, crit, pos, idx


s, crit, _, _ = evaluate(B)
check("planted 0.04 % persistent spread on 8 coins: PASS", all(crit.values()),
      f"ann {s['ann_pct']:.1f} %, t {s['nw_t']:.1f}; failed: " + ", ".join(k for k, v in crit.items() if not v))
npass = 0; anns = []
for sd in range(10):
    s, crit, _, _ = evaluate(XF.build(*world(100 + sd, spread=0.0015, persist=False), top=TOP))
    npass += all(crit.values()); anns.append(round(s["ann_pct"], 1))
check("null: iid spread (sd 0.15 % per settlement, no persistence) -> at most 1 of 10 pass", npass <= 1, f"{npass}/10, ann {anns}")

# ------------------------------------------------------------------ 7 margin stress
pbS = dict(pb, close=pb["close"].copy())
eps = [(0, 1, 100, 120)]
i0 = int((B["days"][100] - pb["grid"][0]) // HOUR)
pbS["close"][i0 + 200, 0] = B["Pb"][100, 0] * 0.7                      # long Binance leg -30 % intraday
check("margin stress: a 30 % adverse move on the long leg is counted; untouched episode is not",
      XF.margin_stress(eps, B["days"], pb["grid"], pbS["close"], py["close"], B["Pb"], B["Py"]) == 1
      and XF.margin_stress([(1, 1, 100, 101)], B["days"], pb["grid"], pb["close"], py["close"], B["Pb"], B["Py"]) == 0)

# ------------------------------------------------------------------ 8 delisting: last common price
pbD = dict(pb, close=pb["close"].copy(), qv=pb["qv"].copy())
hD = 500 * 24 + 9                                                      # coin 0 delists on Binance at 09:00 of day 500
pbD["close"][hD:, 0] = pbD["close"][hD - 1, 0]; pbD["qv"][hD:, 0] = 0  # archive keeps flat zero-volume bars
_, _, cut = XF.close_at_common_death(pbD, py)
check("delisting: flat zero-volume tail detected; both venues closed from the same hour", cut.get("C00USDT") == hD, str(cut.get("C00USDT")))
BD = XF.build(pbD, py, top=TOP)
dd = 500
check("delisting: on the delisting day both legs' returns stop at the last common prices; the coin leaves the set next day",
      np.isclose(BD["TRb"][dd, 0] + BD["Fb"][dd, 0], pb["close"][hD - 1, 0] / BD["Pb"][dd, 0] - 1)
      and np.isclose(BD["TRy"][dd, 0] + BD["Fy"][dd, 0], py["close"][hD - 1, 0] / BD["Py"][dd, 0] - 1)
      and not BD["elig"][dd + 1:, 0].any())
BN = XF.build(pbD, py, top=TOP, common_close=False)
check("delisting: without the rule the Bybit leg keeps moving while Binance is frozen (the bug the rule removes)",
      np.allclose(BN["TRb"][dd + 1:dd + 20, 0] + BN["Fb"][dd + 1:dd + 20, 0], 0) and (BN["TRy"][dd + 1:dd + 20, 0] != 0).any())

print(f"\n{fails} failures"); sys.exit(fails > 0)
