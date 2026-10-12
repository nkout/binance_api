"""Tests for the N4-X minute execution harness — real library functions on synthetic minute data.

  1. identity: entry O(s + 2 min), exit P(s + 1h), signed gross, net = gross - 19, path by hand
  2. leak: bars before the entry minute never change the registered trade; bars after s + 1h never change it
  3. needed_days: s - 1 min and s + 120 min both covered (midnight crossings both ways)
  4. sample: deterministic for the seed, inside the window, sorted, without replacement
  5. planted world (30 bp linear drift in the paying direction over the hour) passes; path(60) ~ 30 bp
  6. 10 null worlds pass at most once; a world whose whole move happens in the first 2 minutes fails (not capturable)
  7. download: daily file parsed and written by fetch_days (one real BTCUSDT day; skipped when offline)
"""
import os, sys, tempfile
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import n4x as N
fails = 0
MIN = 60


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


T0 = int(pd.Timestamp("2025-03-01", tz="UTC").timestamp())


def world(seed, drift_bp=0.0, n=2000, early=False, sd=0.001):
    """n events at distinct settlement hours; per event a 1 m random walk from s - 10 min to s + 130 min.
    The paying-side move (-q direction) of drift_bp accrues linearly over [s, s + 60 min] (or over the first 2 min if early)."""
    rng = np.random.default_rng(seed)
    s = T0 + np.arange(n) * 4 * 3600
    q = np.where(rng.random(n) < 0.9, 1.0, -1.0)
    sample = pd.DataFrame({"sym": [f"E{i:04d}USDT" for i in range(n)], "s": s, "q": q, "post": 0.0, "j": 0})
    minutes = {}
    for i in range(n):
        ts = s[i] + np.arange(-10, 131) * MIN
        k = (ts - s[i]) // MIN + 1                  # close of the bar opening s + (k-1) min is the price at s + k min
        ramp = np.clip(k, 0, 2 if early else 60) / (2 if early else 60) * drift_bp / 1e4
        lr = sd * rng.standard_normal(len(ts))
        c = 100 * np.exp(np.cumsum(lr) - q[i] * ramp)
        o = np.r_[c[0], c[:-1]]                     # open = previous close
        minutes[sample.sym[i]] = pd.DataFrame({"open": o, "close": c, "quote_volume": 1e5}, index=pd.Index(ts, name="ts"))
    return sample, minutes


# ------------------------------------------------------------------ 1 identity
sample, minutes = world(1, drift_bp=30)
t, paths = N.trade_table(sample, minutes)
ok = True
for i in [0, 7, 500, 1999]:
    m, s, q = minutes[sample.sym[i]], sample.s[i], sample.q[i]
    ent = m.open[s + 2 * MIN]; ex = m.close[s + 60 * MIN - MIN]; p0 = m.close[s - MIN]
    ok &= np.isclose(t.g2[i], -q * (ex / ent - 1) * 1e4) and np.isclose(t.net[i], t.g2[i] - 19.0)
    ok &= np.isclose(paths[i, 45], -q * (m.close[s + 44 * MIN] / p0 - 1) * 1e4) and paths[i, 0] == 0
check("identity: entry O(s+2m), exit P(s+1h), net = gross - 19, path by hand", ok)

# ------------------------------------------------------------------ 2 leak
m2 = {k: v.copy() for k, v in minutes.items()}
for k, v in m2.items():
    s = sample.s[sample.sym == k].iloc[0]
    pre = v.index < s + 2 * MIN; post = v.index >= s + 60 * MIN
    v.loc[pre, ["open", "close"]] *= 1.37; v.loc[post, ["open", "close"]] *= 0.61
t2, _ = N.trade_table(sample, m2)
check("leak: bars before the entry minute and after the exit leave the registered trade unchanged",
      np.allclose(t.g2, t2.g2) and not np.allclose(t.g1, t2.g1))

# ------------------------------------------------------------------ 3 needed days
mid = int(pd.Timestamp("2025-05-10", tz="UTC").timestamp())
sm = pd.DataFrame({"sym": ["AUSDT", "BUSDT", "CUSDT"], "s": [mid, mid - 3600, mid + 8 * 3600]})
nd = N.needed_days(sm)
check("needed_days: 00:00 settlement also needs the previous day; 23:00 needs the next day; mid-day one file",
      ("AUSDT", "2025-05-09") in nd and ("AUSDT", "2025-05-10") in nd and ("BUSDT", "2025-05-10") in nd
      and ("BUSDT", "2025-05-09") in nd and [p for p in nd if p[0] == "CUSDT"] == [("CUSDT", "2025-05-10")], str(nd))

# ------------------------------------------------------------------ 4 sample
e = pd.DataFrame({"s": np.arange(10000) * 3600 + T0 - 2000 * 3600, "j": 0, "sym": "X"})
a, b = N.sample_events(e, T0, T0 + 9000 * 3600), N.sample_events(e, T0, T0 + 9000 * 3600)
check("sample: deterministic, 2000 unique events inside the window, sorted",
      a.equals(b) and len(a) == 2000 and a.s.is_unique and a.s.is_monotonic_increasing
      and a.s.min() >= T0 and a.s.max() < T0 + 9000 * 3600)

# ------------------------------------------------------------------ 5 planted
v = N.verdict(t)
check("planted 30 bp: passes", v["pass_"], f"net {v['boot']['mean']:.1f} [{v['boot']['lo']:.1f}, {v['boot']['hi']:.1f}]")
check("planted: path(60) ~ 30 bp and path(2) ~ 1 bp", abs(np.nanmean(paths[:, 60]) - 30) < 4 and abs(np.nanmean(paths[:, 2]) - 1) < 3,
      f"{np.nanmean(paths[:, 60]):.1f}, {np.nanmean(paths[:, 2]):.1f}")

# ------------------------------------------------------------------ 6 null / early
npass = sum(N.verdict(N.trade_table(*world(100 + k))[0], seed=k)["pass_"] for k in range(10))
check("null: 10 worlds pass at most once", npass <= 1, f"{npass}/10")
te, pe = N.trade_table(*world(300, drift_bp=40, early=True))
check("early world: 40 bp move in the first 2 minutes is visible in the path but not capturable",
      np.nanmean(pe[:, 60]) > 30 and abs(te.g2.mean()) < 3 and not N.verdict(te)["pass_"], f"path60 {np.nanmean(pe[:, 60]):.1f}, g2 {te.g2.mean():.1f}")

# ------------------------------------------------------------------ 7 download
with tempfile.TemporaryDirectory() as d:
    try:
        st = N.fetch_days([("BTCUSDT", "2025-01-15"), ("NOSUCHCOINUSDT", "2025-01-15")], d, workers=2)
        m = N.load_minutes(d, "BTCUSDT", ["2025-01-15"])
        check("download: 1440 minute bars, seconds, a missing file is marked not fatal",
              len(m) == 1440 and m.index[0] == int(pd.Timestamp("2025-01-15", tz="UTC").timestamp())
              and st.get("missing") == 1 and N.load_minutes(d, "NOSUCHCOINUSDT", ["2025-01-15"]) is None, str(st))
    except Exception as ex:
        print(f"SKIP download test (offline?): {ex}")

print(f"\n{fails} failures"); sys.exit(fails > 0)
