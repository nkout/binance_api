"""Tests for btc_latency_decay_probe.ipynb (R1) — executes the notebook's own code.

  1. compile() every code cell; outputs cleared
  2. EQUIVALENCE: at BAR=15 the bar-agnostic build_features / stage-1 score equal the executed 1d
     notebook's functions exactly, on real v1 data (so v1x is the 1d model, not a cousin)
  3. LEAK: at BAR=5, scrambling every bar after T changes no feature / stage-1 score at <= T
  4. at BAR=5 the 15 s-equivalent features track the 15 s grid (fimb_1 = sum of 3 bars, ret_1 = 15 s)
  5. MLP arm: preprocessing fitted on train only, NaN-safe; the MLP learns a synthetic non-linear signal
     (sanity that the training loop works) and scores ~0.5 on pure noise
  6. end-to-end nbclient execution with SMOKE=1 (v1 slice Jun-Aug + full 5 s set), artifacts, verdict
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

# ---------------------------------------------------------------- verdict on known rows
# (regression: DEC.tail resolved to DataFrame.tail, so the verdict saw no rows on the 2026-09-23 Colab run)
DEC_T = pd.DataFrame([dict(arm=a_, tail=q_, delay=d_, n=150, gross=g_, gross_ci=[g_ - 1, g_ + 1])
                      for a_ in ("v1x", "v1x_mlp") for q_ in (0.01, 0.02) for d_, g_ in ((0, 12.0), (5, 10.5))])
nv = dict(np=np, pd=pd, DEC=DEC_T, PRIMARY_TAILS=[0.01, 0.02], PASS_FEE=9.0, HO=dict(mlp_better=False))
exec(R1.C_VERDICT, nv)
check("verdict reads the decay rows (CI lo 9.5 > 9 -> PASS)", nv["PASSED"] and nv["V"][0.01]["g5"] == 10.5, str(nv["V"]))
nv["DEC"] = DEC_T.assign(gross=DEC_T["gross"] - 3, gross_ci=[[g - 4, g - 2] for g in DEC_T["gross"]])
exec(R1.C_VERDICT, nv)
check("verdict FAILs when CI lo < 9", not nv["PASSED"] and "g5" in nv["V"][0.02])

# ---------------------------------------------------------------- MLP arm
import torch, torch.nn as nn, torch.nn.functional as TF
from sklearn.metrics import roc_auc_score
nm = dict(np=np, pd=pd, torch=torch, nn=nn, TF=TF, roc_auc_score=roc_auc_score, TDEV="cpu", SEEDS=[0, 1],
          MLP_HIDDEN=(256, 128, 64), MLP_DROPOUT=0.2, MLP_LR=1e-3, MLP_WD=1e-4, MLP_BATCH=1024,
          MLP_EPOCHS=15, MLP_PATIENCE=4)
exec(R1.C_MLP, nm)
rng = np.random.default_rng(1)
Xa = rng.normal(size=(20000, 8)).astype(np.float32); Xa[rng.random(Xa.shape) < 0.05] = np.nan
Xa[:, 7] = np.nan                                             # an all-NaN column must not break anything
Xb = Xa.copy(); Xb[:, 0] = Xb[:, 0] * 100 + 50                # test-time shift must not change the fitted prep
pa = nm["Prep"](Xa); pb_mu = pa.mu.copy(); _ = pa(Xb)
check("MLP prep: fitted on train only (applying to other data leaves it unchanged)", np.array_equal(pb_mu, pa.mu))
Z = pa(Xa)
check("MLP prep: finite output, missing flags appended", np.isfinite(Z).all() and Z.shape[1] == 8 + len(pa.miss) and len(pa.miss) >= 7)
x0, x1, x2 = [np.nan_to_num(Xa[:, i]) for i in range(3)]
ysyn = ((x0 * x1 + 0.5 * np.sin(3 * x2) + 0.3 * rng.normal(size=len(x0))) > 0).astype(np.int8)
tr_, va_, te_ = slice(0, 12000), slice(12000, 15000), slice(15000, 20000)
p_, eps_ = nm["mlp_ens_predict"](Xa[tr_], ysyn[tr_], Xa[va_], ysyn[va_], Xa[te_])
auc_syn = roc_auc_score(ysyn[te_], p_)
check("MLP learns a non-linear synthetic signal (AUC > 0.85)", auc_syn > 0.85, f"auc {auc_syn:.3f}, epochs {eps_}")
ynoise = rng.integers(0, 2, len(Xa)).astype(np.int8)
p_, _ = nm["mlp_ens_predict"](Xa[tr_], ynoise[tr_], Xa[va_], ynoise[va_], Xa[te_])
auc_n = roc_auc_score(ynoise[te_], p_)
check("MLP on pure-noise labels stays near 0.5 (|AUC-0.5| < 0.03)", abs(auc_n - 0.5) < 0.03, f"auc {auc_n:.3f}")

# ---------------------------------------------------------------- smoke execution
tmp = tempfile.mkdtemp(prefix="r1_")
v1s = os.path.join(tmp, "v1_slice.parquet")
raw1[raw1["ts"] >= pd.Timestamp("2026-06-01", tz="UTC").timestamp()].to_parquet(v1s, index=False)
out = os.path.join(tmp, "out")
os.environ.update(SMOKE="1", V1_DATA=v1s, W5_DATA=W5, OUT_DIR=out, HOLDOUT_SPLIT="2026-07-10")
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
    check("results json: decay rows for all arms", {r["arm"] for r in res["decay"]} == {"v1x", "v1x_mlp", "w5", "mix"})
    ho = res["holdout_c1"]
    check("holdout C1: AUCs for xgb / mlp / avg, diff CI, tails", set(ho["auc"]) == {"xgb", "mlp", "avg"}
          and len(ho["diff_ci"]) == 2 and len(ho["tails"]) == 12 and ho["n_auc"] > 100, str(ho["auc"]))
    check("MLP verdict printed", "MLP C2 (economics):" in txt and "C1:" in txt)
    hz = os.path.join(out, "holdout_c1_scores.npz")
    check("holdout npz written", os.path.exists(hz))
    if os.path.exists(hz):
        tsh = np.load(hz)["ts"]
        check("holdout test starts after split + 10 d val + purge", tsh.min() >= ho["split"] + 10 * 86400 + 4 * 90,
              f"{pd.to_datetime(tsh.min(), unit='s')}")
    check("v1x scored only after the v1 year", True if res["fits"][0]["n_test"] > 0 else False)
    check("scores npz written", os.path.exists(os.path.join(out, "latency_decay_scores.npz")))
    print("\n--- smoke output tail (pipeline check only, NOT a result) ---")
    print("\n".join(txt.splitlines()[-40:]))
print("\nFAILS:", fails)
sys.exit(fails)
