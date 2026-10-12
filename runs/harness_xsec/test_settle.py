"""Tests for the N4 settlement-event harness — real library functions on synthetic hourly worlds.

  1. events: settlement hours rounded from jittered calc_time, signal = previous rate, extreme threshold,
     8 h -> 4 h interval change classified, membership = universe of the day starting <= s - 3h
  2. P&L identity: pre / post / placebo / net recomputed by hand from the close matrix
  3. leak: changing every settled rate r_s leaves the registered event set and q unchanged; changing prices
     and volumes after a cut leaves every event with s + 3h <= cut unchanged
  4. planted world (push before an extreme settlement, reversion after) -> both arms PASS, placebo ~ 0
  5. drift world (signed drift over the whole funding interval, not at the settlement) -> placebo kills both arms
  6. world planted on the SETTLED rate, independent of the previous one -> registered arms fail, info arm sees it
  7. 10 null worlds -> at most 1 pass
  8. cluster bootstrap: CI covers 0 on iid noise, and clustering widens the CI when events share a shock
"""
import os, sys
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import settle as S
fails = 0
HOUR, DAY = 3600, 86400


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


def world(seed, k_pre=0.0, k_post=0.0, drift=0.0, plant_on="prev", rho=0.9, n=30, days=1500, sd_r=6e-4):
    """Hourly random walks; funding every 8 h, then every 4 h from mid-sample; AR(1) rates per settlement.
    Plant: bar [s-1h, s] gets q*k_pre, bar [s, s+1h] gets -q*k_post for extreme settlements (q from plant_on).
    Drift: every hour of (s_prev, s] gets q(r_prev)*drift while r_prev is extreme."""
    rng = np.random.default_rng(seed)
    t0 = int(pd.Timestamp("2021-01-01", tz="UTC").timestamp())
    T = days * 24; grid = t0 + np.arange(T, dtype=np.int64) * HOUR
    lr = 0.006 * rng.standard_normal((T, n))
    listed = np.zeros(n, int); listed[25:] = rng.integers(100, 400, n - 25)
    sched = np.r_[np.arange(8, days // 2 * 24, 8), np.arange(days // 2 * 24, T - 24, 4)]   # settlement hour offsets
    fund, sd_e = {}, sd_r * np.sqrt(1 - rho ** 2)
    syms = [f"C{i:02d}USDT" for i in range(n)]
    for i in range(n):
        h = sched[sched >= listed[i] * 24]
        r = np.zeros(len(h)); r[0] = sd_r * rng.standard_normal()
        for t in range(1, len(h)):
            r[t] = rho * r[t - 1] + sd_e * rng.standard_normal()
        sig = r[:-1] if plant_on == "prev" else r[1:]
        q = -np.sign(sig) * (np.abs(sig) >= S.EXTREME)
        hs = h[1:]
        lr[hs - 1, i] += q * k_pre                     # row hs-1 = bar opening s-1h = move from C(s-1h) to C(s)
        ok = hs < T; lr[hs[ok], i] -= q[ok] * k_post   # bar [s, s+1h]
        if drift:
            qp = -np.sign(r[:-1]) * (np.abs(r[:-1]) >= S.EXTREME)
            for a, b, qq in zip(h[:-1], h[1:], qp):
                if qq:
                    lr[a:b, i] += qq * drift
        jitter = rng.integers(0, 3, len(h))            # calc_time a few seconds after the hour
        fund[syms[i]] = ((t0 + h * HOUR + jitter).astype(np.int64), r)
    close = 100 * np.exp(np.cumsum(lr, 0))
    for i in range(n):
        close[:listed[i] * 24, i] = np.nan
    qv = rng.lognormal(15, 1, n)[None, :] * rng.lognormal(0, 0.3, (T, n))
    return dict(grid=grid, syms=syms, close=close, qv=qv.astype(np.float32), fund=fund)


TOP = 20
# ------------------------------------------------------------------ 1-2 events and identity
p = world(1, k_pre=0.0015, k_post=0.0015)
e, dropped, U, days = S.build_events(p, top=TOP)
check("events: every settlement time is on the hour", (e.s % HOUR == 0).all())
check("events: extreme by the previous rate, q = -sign(r_prev)",
      (np.abs(e.r_prev) >= S.EXTREME - 1e-12).all() and (e.q == -np.sign(e.r_prev)).all())
check("events: 8 h and 4 h intervals both present", set(e.interval_h.unique()) >= {8, 4}, str(sorted(e.interval_h.unique())))
di = np.searchsorted(days, e.s - 3 * HOUR, side="right") - 1
check("events: coin is in the universe of the day starting <= s - 3h",
      all(j in U[d] for j, d in zip(e.j, di)) and (days[di] <= e.s - 3 * HOUR).all())
check("events: no event before the universe exists", (e.s - 3 * HOUR >= days[np.flatnonzero(U[:, 0] >= 0)[0]]).all())
rng = np.random.default_rng(5); pick = rng.choice(len(e), 25, replace=False)
g0 = p["grid"][0]; c = lambda j, t: p["close"][(t - HOUR - g0) // HOUR, j]
ok = True
for r in e.iloc[pick].itertuples():
    pre = r.q * (c(r.j, r.s) / c(r.j, r.s - HOUR) - 1) * 1e4
    post = -r.q * (c(r.j, r.s + HOUR) / c(r.j, r.s) - 1) * 1e4
    pp = r.q * (c(r.j, r.s - 2 * HOUR) / c(r.j, r.s - 3 * HOUR) - 1) * 1e4
    po = -r.q * (c(r.j, r.s + 3 * HOUR) / c(r.j, r.s + 2 * HOUR) - 1) * 1e4
    ok &= np.allclose([r.pre, r.post, r.plc_pre, r.plc_post, r.pre_net, r.post_net],
                      [pre, post, pp, po, pre - 9.0, post - 9.0])
check("identity: pre / post / placebos / net recomputed by hand from closes", ok)

# ------------------------------------------------------------------ 3 leak
# change only the LAST settled rate of each coin: every event at an earlier settlement must be untouched, and the
# event AT the last settlement too (its signal is the previous rate) -- only events after it could move (none exist)
p2 = dict(p); p2["fund"] = {s: (ft, np.r_[fr[:-1], -fr[-1] * 50]) for s, (ft, fr) in p["fund"].items()}
e2, *_ = S.build_events(p2, top=TOP)
last = {s: (ft[-1] + HOUR // 2) // HOUR * HOUR for s, (ft, fr) in p["fund"].items()}
keep = lambda x: x[x.s <= x.sym.map(last)].reset_index(drop=True)[["sym", "s", "q", "pre", "post"]]
check("leak: changing the settled rate at s never changes the event at s (signal = r_prev)",
      keep(e).equals(keep(e2)), f"{len(keep(e))} events compared")
cut = int(p["grid"][len(p["grid"]) * 2 // 3]); ci = (cut - p["grid"][0]) // HOUR
p3 = dict(p); p3["close"] = p["close"].copy(); p3["qv"] = p["qv"].copy()
p3["close"][ci:] *= np.exp(np.random.default_rng(9).normal(0, 0.05, p3["close"][ci:].shape))
p3["qv"][ci:] = np.random.default_rng(10).lognormal(15, 3, p3["qv"][ci:].shape)
e3, *_ = S.build_events(p3, top=TOP)
sel = lambda x: x[x.s + 3 * HOUR <= cut - HOUR].reset_index(drop=True)[["sym", "s", "q", "pre", "post", "plc_pre", "plc_post"]]
check("leak: changing prices / volumes after a cut leaves every earlier event unchanged",
      sel(e).equals(sel(e3)), f"{len(sel(e))} events")

# ------------------------------------------------------------------ 4 planted world
arms = S.verdict(e)
for a in arms:
    check(f"planted: {a['arm'].upper()} passes every criterion", a["pass"],
          f"net {a['boot_net']['mean']:.1f} [{a['boot_net']['lo']:.1f}, {a['boot_net']['hi']:.1f}] bp, "
          f"diff {a['boot_diff']['mean']:.1f}, n {a['boot_net']['n']}, years {a['pos_years']}/{a['n_years']}")
check("planted: placebo bars carry no planted effect (|mean| < 2 bp)",
      abs(e.plc_pre.mean()) < 2 and abs(e.plc_post.mean()) < 2, f"{e.plc_pre.mean():.2f}, {e.plc_post.mean():.2f}")
check("planted: gross ~ planted 15 bp in both arms", abs(e.pre.mean() - 15) < 3 and abs(e.post.mean() - 15) < 3,
      f"{e.pre.mean():.1f}, {e.post.mean():.1f}")

# ------------------------------------------------------------------ 5 drift world
ed, *_ = S.build_events(world(2, drift=0.0015), top=TOP)
ad = S.verdict(ed)
check("drift: PRE gross > 9 bp (would pass on net alone) but fails on event - placebo",
      ed.pre.mean() > 9 and not ad[0]["pass"] and not ad[0]["criteria"]["event - placebo CI lo > 0"],
      f"pre {ed.pre.mean():.1f}, placebo {ed.plc_pre.mean():.1f}")
check("drift: POST does not pass", not ad[1]["pass"])

# ------------------------------------------------------------------ 6 settled-rate world
ps = world(3, k_pre=0.0015, k_post=0.0015, plant_on="settled", rho=0.0)
es, *_ = S.build_events(ps, top=TOP)
ei, *_ = S.build_events(ps, top=TOP, sign_by="settled")
as_ = S.verdict(es)
check("settled-rate world: registered arms (signed by r_prev) do not pass", not any(a["pass"] for a in as_),
      f"pre gross {es.pre.mean():.1f}, post {es.post.mean():.1f}")
check("settled-rate world: info arm signed by r_s sees the planted 15 bp", abs(ei.pre.mean() - 15) < 3,
      f"{ei.pre.mean():.1f}")

# ------------------------------------------------------------------ 7 null worlds
npass = 0
for sd in range(10):
    en, *_ = S.build_events(world(100 + sd), top=TOP)
    npass += any(a["pass"] for a in S.verdict(en, seed=sd))
check("null: 10 worlds with no effect pass at most once", npass <= 1, f"{npass}/10")
npass = 0
for sd in range(10):                                # cost-free null: a 9 bp effect with zero cost must still be rare
    en, *_ = S.build_events(world(200 + sd), top=TOP)
    en = en.assign(pre_net=en.pre, post_net=en.post)
    npass += any(all(v for k, v in a["criteria"].items() if k != "Holm p < 0.05") and a["criteria"]["net CI lo > 0"]
                 for a in S.verdict(en, seed=sd))
check("null without costs: CI lower bound > 0 in at most 1 of 10 worlds", npass <= 1, f"{npass}/10")

# ------------------------------------------------------------------ 8 bootstrap
r = np.random.default_rng(0)
x = r.standard_normal(4000); cl = np.arange(4000)
b = S.cluster_boot(x, cl)
check("boot: iid noise CI covers 0", b["lo"] < 0 < b["hi"])
shock = r.standard_normal(200); clc = np.repeat(np.arange(200), 20); xc = shock[clc] + 0.2 * r.standard_normal(4000)
w_iid = np.subtract(*[S.cluster_boot(xc, np.arange(4000))[k] for k in ("hi", "lo")])
w_cl = np.subtract(*[S.cluster_boot(xc, clc)[k] for k in ("hi", "lo")])
check("boot: shared shocks widen the clustered CI (> 3x iid)", w_cl > 3 * w_iid, f"{w_cl:.3f} vs {w_iid:.3f}")

print(f"\n{fails} failures"); sys.exit(fails > 0)
