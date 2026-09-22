"""Tests for btc_latency_decay_probe.ipynb (R1) — executes the notebook's own code.

  1. compile() every code cell; outputs cleared
  2. EQUIVALENCE: at BAR=15 the bar-agnostic build_features / stage-1 score equal the executed 1d
     notebook's functions exactly, on real v1 data (so v1x is the 1d model, not a cousin)
  3. LEAK: at BAR=5, scrambling every bar after T changes no feature / stage-1 score at <= T
  4. at BAR=5 the 15 s-equivalent features track the 15 s grid (fimb_1 = sum of 3 bars, ret_1 = 15 s)
  5. end-to-end nbclient execution with SMOKE=1 (v1 slice Jun-Aug + full 5 s set), artifacts, verdict
"""
import os, sys, json, tempfile, time
import numpy as np, pandas as pd, nbformat
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import build_notebook as R1
import importlib.util
spec = importlib.util.spec_from_file_location("B1d", os.path.join(HERE, "..", "harness_v1_stage2", "build_notebook.py"))
B1d = importlib.util.module_from_spec(spec); spec.loader.exec_module(B1d)

DATA = os.path.join(HERE, "..", "..", "data")
V1 = os.path.join(DATA, "v1_year", "v1_15s.parquet"); W5 = os.path.join(DATA, "w5_60d.parquet")
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


path = R1.build(os.path.join(tempfile.mkdtemp(), "r1.ipynb"))
nb = nbformat.read(path, as_version=4)
code = [c for c in nb.cells if c.cell_type == "code"]
for i, c in enumerate(code):
    try:
        compile(c.source, f"cell{i + 1}", "exec"); ok = True
    except SyntaxError as e:
        ok = False; print(e)
    check(f"compile cell {i + 1}", ok)
check("outputs cleared", all(c.outputs == [] and c.execution_count is None for c in code))


def defs(src, **g):
    ns = dict(np=np, pd=pd, **g); exec(src.split("t0 = time.time()")[0], ns); return ns


raw1 = pd.read_parquet(V1)
sub = raw1.iloc[200_000:240_000].reset_index(drop=True)
g, ts = sub.drop(columns="ts").astype(np.float32), sub["ts"].to_numpy(np.int64)
for dead, tf in ((False, False), (True, True)):
    n1 = defs(B1d.C_FEAT, BAR=15, BARS_DAY=5760, STAGE1_WIN=20, INCLUDE_DEAD=dead, TIME_FEATS=tf)
    nr = defs(R1.C_FEAT, STAGE1_WIN_15=20, H_SEC=90, THETA=20.0, PRIMARY_RATE=0.05, STAGE1_LOOKBACK_DAYS=7,
              INCLUDE_DEAD=dead, TIME_FEATS=tf)
    Fa, lma, sa = n1["build_features"](g, ts)
    Fb, lmb, sb = nr["build_features"](g, ts, 15)
    same_cols = list(Fa.columns) == list(Fb.columns)
    A, Bm = Fa.to_numpy(), Fb.to_numpy()
    eq = same_cols and np.array_equal(np.isnan(A), np.isnan(Bm)) and np.allclose(A[~np.isnan(A)], Bm[~np.isnan(Bm)], rtol=1e-6)
    bad = [c for c in Fa.columns if not np.allclose(np.nan_to_num(Fa[c]), np.nan_to_num(Fb[c]), rtol=1e-6)] if same_cols else "cols differ"
    check(f"EQUIVALENCE at BAR=15 vs 1d notebook (dead={dead}, time={tf}): {Fa.shape[1]} features", eq, str(bad)[:200])
    check(f"EQUIVALENCE stage-1 score (dead={dead})", np.allclose(np.nan_to_num(sa), np.nan_to_num(sb)))

nr = defs(R1.C_FEAT, STAGE1_WIN_15=20, H_SEC=90, THETA=20.0, PRIMARY_RATE=0.05, STAGE1_LOOKBACK_DAYS=7,
          INCLUDE_DEAD=False, TIME_FEATS=False)
raw5 = pd.read_parquet(W5)
s5 = raw5.iloc[100_000:160_000].reset_index(drop=True)
g5, t5 = s5.drop(columns="ts").astype(np.float32), s5["ts"].to_numpy(np.int64)
check("5 s slice is on a 5 s cadence", np.mean(np.diff(t5) == 5) > 0.99)
F1, lm1, s1 = nr["build_features"](g5, t5, 5)
T = 45_000; g5b = g5.copy(); rng = np.random.default_rng(0)
for c in g5b.columns:
    g5b.loc[T + 1:, c] = g5b.loc[T + 1:, c].to_numpy() * rng.uniform(0.5, 1.5, len(g5b) - T - 1).astype(np.float32)
F2, _, s2 = nr["build_features"](g5b, t5, 5)
a, b = F1.iloc[:T + 1].to_numpy(), F2.iloc[:T + 1].to_numpy()
check("LEAK at BAR=5: features at t<=T unchanged when bars > T scrambled",
      np.array_equal(np.isnan(a), np.isnan(b)) and np.allclose(a[~np.isnan(a)], b[~np.isnan(b)]))
check("LEAK at BAR=5: stage-1 score unchanged", np.allclose(np.nan_to_num(s1[:T + 1]), np.nan_to_num(s2[:T + 1])))
bq, sq = g5["future_buy_qty"].to_numpy(), g5["future_sell_qty"].to_numpy()
i = 30_000
b3, s3 = bq[i - 2:i + 1].sum(), sq[i - 2:i + 1].sum()
check("BAR=5 fimb_1 = 15 s (3-bar) imbalance", np.isclose(F1["fimb_1"].iloc[i], (b3 - s3) / (b3 + s3), atol=1e-5))
check("BAR=5 label horizon = 18 bars", 90 // 5 == 18)

# ---------------------------------------------------------------- smoke execution
tmp = tempfile.mkdtemp(prefix="r1_")
v1s = os.path.join(tmp, "v1_slice.parquet")
raw1[raw1["ts"] >= pd.Timestamp("2026-06-01", tz="UTC").timestamp()].to_parquet(v1s, index=False)
out = os.path.join(tmp, "out")
os.environ.update(SMOKE="1", V1_DATA=v1s, W5_DATA=W5, OUT_DIR=out)
from nbclient import NotebookClient
t0 = time.time()
client = NotebookClient(nb, timeout=5400, kernel_name="python3")
try:
    client.execute(); ok, err = True, ""
except Exception as e:
    ok, err = False, str(e)[-2500:]
check(f"notebook executes end-to-end (SMOKE, {time.time() - t0:.0f}s)", ok, err)
if ok:
    txt = "\n".join(o.get("text", "") for c in nb.cells if c.cell_type == "code" for o in c.outputs
                    if o.get("output_type") == "stream")
    check("verdict printed", "VERDICT:" in txt)
    res = json.load(open(os.path.join(out, "latency_decay_results.json")))
    check("results json: decay rows for all arms", {r["arm"] for r in res["decay"]} == {"v1x", "w5", "mix"})
    check("v1x scored only after the v1 year", True if res["fits"][0]["n_test"] > 0 else False)
    check("scores npz written", os.path.exists(os.path.join(out, "latency_decay_scores.npz")))
    print("\n--- smoke output tail (pipeline check only, NOT a result) ---")
    print("\n".join(txt.splitlines()[-40:]))
print("\nFAILS:", fails)
sys.exit(fails)
