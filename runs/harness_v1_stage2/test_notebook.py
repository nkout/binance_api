"""Tests for btc_v1_stage2_probe.ipynb — execute the notebook's OWN code, not re-implementations.

  1. compile() every code cell (not ast.parse — that let run.009f ship a 'break outside loop')
  2. outputs cleared in the built notebook
  3. unit tests on the notebook's own first_touch / build_features / stage1_triggers:
     hand-computed labels, and a LEAK test — perturbing every bar after T must not change any
     feature or trigger at or before T
  4. fold purge: train labels never reach val, val labels never reach test
  5. full end-to-end execution (nbclient) on a 4.5-month slice with SMOKE=1, zero errors,
     artifacts written, verdict printed

  python test_notebook.py            (needs data/v1_year/v1_15s.parquet)
"""
import os, sys, re, json, tempfile, time
import numpy as np, pandas as pd
import nbformat
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_notebook as B

HERE = os.path.dirname(os.path.abspath(__file__))
FULL = "/home/nkout/projects/binance_api/data/v1_year/v1_15s.parquet"
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))
    fails += (not cond)


# ---------------------------------------------------------------- 1-2 build + compile
path = B.build(os.path.join(tempfile.mkdtemp(), 'nb.ipynb'))
nb = nbformat.read(path, as_version=4)
code = [c for c in nb.cells if c.cell_type == "code"]
for i, c in enumerate(code):
    try:
        compile(c.source, f"cell{i + 1}", "exec"); ok = True
    except SyntaxError as e:
        ok = False; print(e)
    check(f"compile cell {i + 1}", ok)
check("outputs cleared", all(c.outputs == [] and c.execution_count is None for c in code))

# ---------------------------------------------------------------- 3 unit tests on notebook code
ns = dict(np=np, pd=pd, BAR=15, BARS_DAY=5760, STAGE1_WIN=20, INCLUDE_DEAD=True, TIME_FEATS=True)
src = B.C_FEAT.split("t0 = time.time()")[0]          # function defs only
exec(src, ns)
first_touch, build_features, stage1_triggers = ns["first_touch"], ns["build_features"], ns["stage1_triggers"]

lm = np.log(np.array([100, 100.1, 100.25, 100.1, 99.7, 99.6, 99.6, 99.6], float))
L = first_touch(lm, 3, 20.0)
check("first_touch: +25 bp at t+2 -> up first", L["touched"][0] and L["up"][0])
check("first_touch: t=2 drops to -40 bp at t+2 -> dn first", L["touched"][2] and not L["up"][2])
check("first_touch: last h bars invalid", not L["valid"][-3:].any() and L["valid"][:5].all())
lm2 = lm.copy(); lm2[3] = np.nan
check("first_touch: missing bar in path -> invalid", not first_touch(lm2, 3, 20.0)["valid"][0])

raw = pd.read_parquet(FULL)
sub = raw.iloc[:40_000].reset_index(drop=True)          # ~7 days, contiguous grid assumed below
g = sub.drop(columns="ts").astype(np.float32)
ts = sub["ts"].to_numpy(np.int64)
F1, lm1, s1 = build_features(g, ts)
T = 30_000
g2 = g.copy()
rng = np.random.default_rng(0)
for c in g2.columns:                                     # scramble the future hard
    g2.loc[T + 1:, c] = g2.loc[T + 1:, c].to_numpy() * rng.uniform(0.5, 1.5, len(g2) - T - 1).astype(np.float32)
F2, lm2_, s2 = build_features(g2, ts)
a, b = F1.iloc[:T + 1].to_numpy(), F2.iloc[:T + 1].to_numpy()
same = np.array_equal(np.isnan(a), np.isnan(b)) and np.allclose(a[~np.isnan(a)], b[~np.isnan(b)])
check("LEAK: features at t<=T unchanged when bars > T are scrambled", same)
check("LEAK: stage-1 score at t<=T unchanged", np.allclose(np.nan_to_num(s1[:T + 1]), np.nan_to_num(s2[:T + 1])))
check("features scramble actually changed the future (test is live)",
      not np.allclose(np.nan_to_num(F1.iloc[T + 10:].to_numpy()), np.nan_to_num(F2.iloc[T + 10:].to_numpy())))
check("no inf in features", np.isfinite(F1.to_numpy()[~np.isnan(F1.to_numpy())]).all())
print(f"     {F1.shape[1]} features, NaN share after 1-day warm-up "
      f"{np.isnan(F1.iloc[6000:].to_numpy()).mean() * 100:.1f} %")

day = ts // 86400
tr1 = stage1_triggers(s1, day, 0.05, 2)
s_fut = s1.copy(); s_fut[day >= day[T]] *= 3.0
tr2 = stage1_triggers(s_fut, day, 0.05, 2)
first_T_day = np.searchsorted(day, day[T])
check("LEAK: stage-1 triggers before day(T) unchanged by future scores",
      np.array_equal(tr1[:first_T_day], tr2[:first_T_day]))
rate = tr1[day >= day[0] + 2].mean()
check("stage-1 realised rate in a sane band", 0.01 < rate < 0.15, f"{rate:.3%}")

# ---------------------------------------------------------------- 4-5 end-to-end smoke execution
slice_path = os.path.join(tempfile.gettempdir(), "v1_15s_smoke.parquet")
cut = pd.Timestamp("2026-02-01", tz="UTC").timestamp()
raw[raw["ts"] < cut].to_parquet(slice_path, index=False)
out_dir = tempfile.mkdtemp(prefix="v1s2_")
os.environ.update(SMOKE="1", V1_DATA=slice_path, OUT_DIR=out_dir)
from nbclient import NotebookClient
t0 = time.time()
client = NotebookClient(nb, timeout=3600, kernel_name="python3", allow_errors=False)
try:
    client.execute(); ok = True; err = ""
except Exception as e:
    ok, err = False, str(e)[-1500:]
check(f"notebook executes end-to-end on SMOKE slice ({time.time() - t0:.0f}s)", ok, err)

if ok:
    txt = "\n".join(o.get("text", "") for c in nb.cells if c.cell_type == "code" for o in c.outputs
                    if o.get("output_type") == "stream")
    check("verdict printed", "VERDICT:" in txt)
    res = json.load(open(os.path.join(out_dir, "v1_stage2_results.json")))
    check("results json has primary AUC", "evt|6|0.05" in res["auc"])
    check("scores npz written", os.path.exists(os.path.join(out_dir, "v1_stage2_scores.npz")))
    # purge check, recomputed from the executed kernel's own split()
    kc = client.kc
    probe = ("import json as _j\nout=[]\nfor m in TEST_MONTHS:\n (tl,th),(vl,vh),(a,b)=split(m)\n"
             " out.append([int(th+max(h for h,_ in HORIZONS)<=vl), int(vh+max(h for h,_ in HORIZONS)<=a),"
             " int(th>0), int(vl>th)])\nprint('PURGE', _j.dumps(out))")
    nb2 = nbformat.v4.new_notebook(); nb2.cells = nb.cells + [nbformat.v4.new_code_cell(probe)]
    c2 = NotebookClient(nb2, timeout=3600, kernel_name="python3")
    c2.execute()
    ptxt = "".join(o.get("text", "") for o in nb2.cells[-1].outputs if o.get("output_type") == "stream")
    rows = json.loads(re.search(r"PURGE (.*)", ptxt).group(1))
    check("purge: train labels end before val, val labels end before test", all(all(r) for r in rows), str(rows))
    print("\n--- smoke output tail (pipeline check only, NOT a result) ---")
    print("\n".join(txt.splitlines()[-25:]))

print("\nFAILS:", fails)
sys.exit(fails)
