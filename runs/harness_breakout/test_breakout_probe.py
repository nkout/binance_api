"""Sanity tests for breakout_probe.one_trade / run_rule (no npz needed)."""
import numpy as np
import breakout_probe as bp

rng = np.random.default_rng(1)
fails = 0
def check(name, cond):
    global fails
    print(("PASS " if cond else "FAIL ") + name); fails += (not cond)

def world(n, drift_bp=0.0, sd_bp=1.5):
    r = rng.normal(drift_bp, sd_bp, n) * 1e-4
    close = 60000 * np.exp(np.cumsum(r))
    return dict(close=close, seg_end=np.full(n, n - 1), day=np.arange(n) // 17280,
                fold=np.zeros(n, int))

# 1. hand case: up-cross on bar 3, hold 2
c = np.array([100, 100.05, 100.08, 100.2, 100.3, 100.25, 100.0])
f, side, g, e, x = bp.one_trade(c, 0, 10.0, 4, 2, "hold")
check("hand long entry at first close >= +10bp", f and side == 1 and e == 3 and x == 5)
check("hand gross = close[x]/close[e]-1", abs(g - (100.25 / 100.2 - 1) * 1e4) < 1e-9)
f, *_ = bp.one_trade(np.array([100, 100.01, 99.99, 100.0, 100.0, 100, 100]), 0, 10.0, 4, 2, "hold")
check("no cross -> not filled", not f)

# 2. martingale: breakout on random bars has ~0 expected gross (the probe's null hypothesis)
d = world(400_000)
trig = np.sort(rng.choice(np.arange(399_000), 20_000, replace=False))
res = bp.run_rule(d, trig, 5.0, 60, 180, "hold")
g = np.array([r[3] for r in res if r[1]])
se = g.std() / np.sqrt(len(g))
check(f"martingale gross ~0: {g.mean():+.3f} +- {se:.3f} bp (n={len(g)})", abs(g.mean()) < 3 * se)
res = bp.run_rule(d, trig, 5.0, 60, 180, "trail")
g = np.array([r[3] for r in res if r[1]])
se = g.std() / np.sqrt(len(g))
check(f"martingale trail gross ~0: {g.mean():+.3f} +- {se:.3f}", abs(g.mean()) < 3 * se)

# 3. one position at a time: no trade starts before the previous exit
starts = np.array([r[0] for r in res]); exits = np.array([r[5] for r in res])
check("non-overlapping trades", np.all(starts[1:] > exits[:-1]))

# 4. momentum world (AR(1) positive returns) -> positive gross
n = 200_000; eps = rng.normal(0, 1.5, n); r = np.zeros(n)
for i in range(1, n): r[i] = 0.3 * r[i - 1] + eps[i]
dm = dict(close=60000 * np.exp(np.cumsum(r * 1e-4)), seg_end=np.full(n, n - 1),
          day=np.arange(n) // 17280, fold=np.zeros(n, int))
res = bp.run_rule(dm, np.arange(0, n - 2000, 50), 5.0, 60, 180, "hold")
g = np.array([r[3] for r in res if r[1]])
check(f"momentum world gross > 0: {g.mean():+.2f}", g.mean() > 0.5)

# 5. segment guard: trigger whose window crosses a segment end is skipped
d2 = world(1000); d2["seg_end"][:] = 500; d2["seg_end"][501:] = 999
res = bp.run_rule(d2, np.array([400, 600]), 5.0, 60, 180, "hold")
check("segment-crossing trigger skipped", [r[0] for r in res] == [600])

print("FAILS:", fails)
raise SystemExit(fails)
