"""Tests for the W1 wide-input probe — executes the extractor's and the notebook's own code.

  1. extractor: trunc16 precision; per-file transform is continuous across file boundaries (ETH / OI
     per-bar changes); price columns are bp vs mid; no timestamp / time-proxy column survives
  2. w5_wide.parquet (if built): ts strictly increasing, columns match the spec, aligned to w5_60d
  3. notebook cells compile; outputs cleared
  4. notebook functions: rolling_at == pandas rolling mean and is causal; label D == first touch
     measured from t+1; CNN windows never read bars after t; MLP and CNN learn a synthetic signal
     (and stay at chance on noise)
  5. end-to-end nbclient execution with SMOKE=1 on the first 35 days of the real data
"""
import io, json, os, sys, tarfile, tempfile, time
import numpy as np, pandas as pd, nbformat
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import build_notebook as W
import extract_w5_wide as X

DATA = os.path.join(HERE, "..", "..", "data")
TAR, W5, WIDE = os.path.join(DATA, "60days_data.tar"), os.path.join(DATA, "w5_60d.parquet"), os.path.join(DATA, "w5_wide.parquet")
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


# ------------------------------------------------------------------ 1. extractor
rng = np.random.default_rng(0)
v = (rng.normal(size=100000) * 10 ** rng.uniform(-3, 4, 100000)).astype(np.float32); v[::97] = np.nan
t = X.trunc16(v)
rel = np.abs(t - v) / np.maximum(np.abs(v), 1e-30)
check("trunc16: relative error < 2^-10, NaN kept", np.nanmax(rel) < 2 ** -10 and np.array_equal(np.isnan(t), np.isnan(v)),
      f"max rel {np.nanmax(rel):.2e}")
with tarfile.open(TAR) as tf:
    names = sorted(n for n in tf.getnames() if n.endswith(".csv.gz"))
    df = X.read_member(tf, names[10])
spec = json.load(open(os.path.join(HERE, "wide_spec.json")))["columns"] if os.path.exists(os.path.join(HERE, "wide_spec.json")) \
    else X.classify(list(df.columns), df)
_, whole = X.transform(df, spec, {})
prev = {}
_, a1 = X.transform(df.iloc[:1000].reset_index(drop=True), spec, prev)
_, a2 = X.transform(df.iloc[1000:].reset_index(drop=True), spec, prev)
same = all(np.array_equal(np.r_[a1[k], a2[k]], whole[k], equal_nan=True) for k in whole)
bad = [k for k in whole if not np.array_equal(np.r_[a1[k], a2[k]], whole[k], equal_nan=True)]
check("transform: split file == whole file (per-bar ETH / OI changes carried across files)", same, str(bad[:5]))
mid = (df["future_bid_close"] + df["future_ask_close"]) / 2
exp = ((df["future_bid_close"] / mid - 1) * 1e4).to_numpy(np.float32)
check("transform: future_bid_close -> bp vs mid", np.allclose(whole["w_future_bid_close"], exp, atol=1e-3))
eth = np.log(df["opt_eth_mid_close"].to_numpy(np.float64)) * 1e4
check("transform: ETH close -> 5 s log return", np.allclose(whole["w_opt_eth_mid_close"][1:], np.diff(eth), atol=0.05))
kept = [c for c, a in spec.items() if not a.startswith("drop") and a != "ts"]
check("spec: no timestamp / datetime / time-to-funding / liquidation column kept",
      not [c for c in kept if "time" in c or "force_exit" in c], str([c for c in kept if "time" in c][:5]))
check("spec: every raw price level is transformed (none left as log1p/raw)",
      all(spec[c] == "price_bp" for c in kept if c.endswith(("_bid_close", "_ask_close", "_vwap"))))

# ------------------------------------------------------------------ 2. the built parquet
if os.path.exists(WIDE):
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(WIDE)
    tsw = pq.read_table(WIDE, columns=["ts"]).column("ts").to_numpy()
    check("w5_wide: ts strictly increasing", bool((np.diff(tsw) > 0).all()))
    check("w5_wide: columns == spec kept + ts", pf.schema_arrow.names == ["ts"] + [f"w_{c}" for c in kept],
          f"{len(pf.schema_arrow.names)} vs {len(kept) + 1}")
    ts5 = pd.read_parquet(W5, columns=["ts"])["ts"].to_numpy()
    cover = np.isin(ts5, tsw).mean()
    check("w5_wide: covers >= 99 % of w5_60d bars", cover >= 0.99, f"{cover * 100:.2f} %")
    print(f"      w5_wide: {len(tsw):,} rows x {len(pf.schema_arrow.names)} cols, {os.path.getsize(WIDE) / 1e9:.2f} GB")
else:
    check("w5_wide.parquet exists (run extract_w5_wide.py first)", False)

# ------------------------------------------------------------------ 3. cells
path = W.build(os.path.join(tempfile.mkdtemp(), "w1.ipynb"))
nb = nbformat.read(path, as_version=4)
code = [c for c in nb.cells if c.cell_type == "code"]
for i, c in enumerate(code):
    try:
        compile(c.source, f"cell{i + 1}", "exec"); ok = True
    except SyntaxError as e:
        ok = False; print(e)
    check(f"compile cell {i + 1}", ok)
check("outputs cleared", all(c.outputs == [] and c.execution_count is None for c in code))

# ------------------------------------------------------------------ 4. notebook functions
os.environ["OUT_DIR"] = tempfile.mkdtemp(prefix="w1_unit_")
ns = {}
exec(W.C_SETUP, ns); exec(W.C_CONFIG, ns)
exec(W.C_BASE.split("t0 = time.time()")[0], ns)
exec(W.C_WIDE.split("t0 = time.time()")[0], ns)
x = rng.normal(size=5000); x[rng.random(5000) < 0.1] = np.nan
rows = np.arange(5000)
for w in (12, 60):
    ref = pd.Series(x).rolling(w, min_periods=1).mean().to_numpy()
    check(f"rolling_at(w={w}) == pandas causal rolling mean", np.allclose(ns["rolling_at"](x, rows, w), ref, equal_nan=True))
xs = x.copy(); xs[3001:] = rng.normal(size=1999) * 100
check("rolling_at is causal (bars > t scrambled -> values at <= t unchanged)",
      np.allclose(ns["rolling_at"](x, rows[:3001], 60), ns["rolling_at"](xs, rows[:3001], 60), equal_nan=True))
lm_ = np.cumsum(rng.normal(size=20000)) * 2.0
L0 = ns["first_touch"](lm_, 18, 10.0); LD = ns["shift_lab"](L0, 1); L1 = ns["first_touch"](lm_[1:], 18, 10.0)
check("label D at t == first touch measured from t+1",
      all(np.array_equal(LD[k][:-1], L1[k], equal_nan=True) for k in ("valid", "touched", "up", "ret")))

# models on synthetic data (CPU)
G_, K_ = 30000, 16
GW_ = rng.normal(size=(G_, K_)).astype(np.float16); GW_[rng.random(GW_.shape) < 0.02] = np.nan
g0 = np.nan_to_num(GW_.astype(np.float32))
sig = g0[:, 3] + np.r_[np.zeros(4), g0[:-4, 5]] * g0[:, 7]           # depends on the bar and on 4 bars back
ysyn = ((sig + 0.5 * rng.normal(size=G_)) > 0).astype(np.int8)
ROWS_ = np.arange(G_); row_of_ = np.arange(G_)
XW_ = np.concatenate([g0] + [np.stack([ns["rolling_at"](g0[:, j], ROWS_, w) for j in range(K_)], 1) for w in (12, 60)], 1).astype(np.float32)
ns.update(GW=GW_, K=K_, XW=XW_, row_of=row_of_, X60=g0, EPOCHS=12, PATIENCE=3, SEEDS=[0])
exec(W.C_MODELS, ns)
tr, va, te = np.arange(100, 20000), np.arange(20000, 24000), np.arange(24000, 30000)
p, ep = ns["fit_cnn"](tr, ysyn[tr], va, ysyn[va], te, 0)
from sklearn.metrics import roc_auc_score
auc_c = roc_auc_score(ysyn[te], p)
check("CNN learns a signal that needs the 4-bars-back input (AUC > 0.75)", auc_c > 0.75, f"auc {auc_c:.3f} ep {ep}")
p, ep = ns["fit_mlp"](tr, ysyn[tr], va, ysyn[va], te, 0)
auc_m = roc_auc_score(ysyn[te], p)
check("MLP learns the same-bar part of the signal (AUC > 0.65)", auc_m > 0.65, f"auc {auc_m:.3f} ep {ep}")
yn = rng.integers(0, 2, G_).astype(np.int8)
p, _ = ns["fit_cnn"](tr, yn[tr], va, yn[va], te, 0)
check("CNN on noise labels stays near chance (|AUC - 0.5| < 0.03)", abs(roc_auc_score(yn[te], p) - 0.5) < 0.03,
      f"{roc_auc_score(yn[te], p):.3f}")
stats = ns["Prep"](GW_[tr].astype(np.float32)).torch()
w1 = ns["cnn_window_batch"](np.array([5000]), stats).numpy()
ns["GW"] = GW_.copy(); ns["GW"][5001:] = np.float16(999); ns["GWT"] = None
w2 = ns["cnn_window_batch"](np.array([5000]), stats).numpy()
check("CNN window for bar t never reads bars after t", np.array_equal(w1, w2) and w1.shape == (1, 12, K_))

# ------------------------------------------------------------------ 5. smoke execution
if os.path.exists(WIDE):
    out = tempfile.mkdtemp(prefix="w1_smoke_")
    os.environ.update(SMOKE="1", W5_DATA=W5, WIDE_DATA=WIDE, OUT_DIR=out, LIMIT_DAYS="35")
    from nbclient import NotebookClient
    t0 = time.time()
    try:
        NotebookClient(nb, timeout=7200, kernel_name="python3").execute(); ok, err = True, ""
    except Exception as e:
        ok, err = False, str(e)[-3000:]
    check(f"notebook executes end-to-end (SMOKE, 35 days, {time.time() - t0:.0f}s)", ok, err)
    if ok:
        txt = "\n".join(o.get("text", "") for c in nb.cells if c.cell_type == "code" for o in c.outputs
                        if o.get("output_type") == "stream")
        check("verdicts printed", all(s in txt for s in ("VERDICT P1", "VERDICT P2", "VERDICT P3")))
        res = json.load(open(os.path.join(out, "wide_probe_results.json")))
        arms = {(r["label"], r["model"]) for r in res["decay"]}
        check("decay rows for 2 labels x 4 models", arms == {(L, m) for L in "DZ" for m in ("xgb60", "xgbw", "mlp", "cnn")})
        check("trade path exercised (some cell has >= 20 trades)", any(r.get("gross") is not None for r in res["decay"]))
        check("AUC for every arm", len(res["auc"]) == 8 and all(v["n"] > 50 for v in res["auc"].values()), str(res["auc"])[:300])
        check("P1 / P2 / P3 recorded", set(res["passed"]) == {"P1", "P2", "P3"} and len(res["p1"]) == 6)
        check("scores npz written", os.path.exists(os.path.join(out, "wide_probe_scores.npz")))
        print("\n--- smoke output tail (pipeline check only, NOT a result) ---")
        print("\n".join(txt.splitlines()[-30:]))
print("\nFAILS:", fails)
sys.exit(fails)
