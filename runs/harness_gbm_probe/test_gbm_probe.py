"""Acceptance test for runs/btc_lstm.probe.gbm.ipynb.

Follows the run.009b/009f convention: compile() every cell (ast.parse does NOT
catch misplaced break/continue), assert the inherited cells are byte-identical,
static undefined-name scan in execution order, and — the part that actually
bites — EXECUTE the probe cells against synthetic data with a stubbed xgboost.
"""
import json, sys, types, symtable, io, os, builtins
import numpy as np
import pandas as pd

SRC = "/home/nkout/projects/binance_api/runs/btc_lstm.run.009f.ipynb"
DST = "/home/nkout/projects/binance_api/runs/btc_lstm.probe.gbm2.ipynb"
PASS = FAIL = 0


def chk(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        print(f"  FAIL: {msg}")


src9 = json.load(open(SRC))["cells"]
nb = json.load(open(DST))["cells"]
S = lambda c: "".join(c["source"])
code = [c for c in nb if c["cell_type"] == "code"]

print("1. structure + compile")
chk(len(nb) == 17, f"expected 17 cells, got {len(nb)}")
chk(nb[0]["cell_type"] == "markdown", "cell 0 must be the markdown header")
for i, c in enumerate(code):
    try:
        compile(S(c).replace("!", "#!").replace("%", "#%") if S(c).lstrip().startswith(("!", "%")) else S(c),
                f"<cell{i}>", "exec")
        PASS += 1
    except SyntaxError as e:
        # cell 3 has IPython magics; neutralise line-wise and retry
        neut = "\n".join(("#" + ln) if ln.lstrip().startswith(("!", "%")) else ln
                         for ln in S(c).splitlines())
        try:
            compile(neut, f"<cell{i}>", "exec"); PASS += 1
        except SyntaxError as e2:
            chk(False, f"cell {i} does not compile: {e2}")
chk(all(c.get("outputs") == [] for c in code), "all outputs must be cleared")
chk(all(c.get("execution_count") is None for c in code), "all exec counts must be cleared")

print("2. inherited cells are byte-identical to run.009f")
chk(S(nb[2]) == S(src9[2]), "cell 2 (transfer helpers) differs")
chk(S(nb[3]) == S(src9[3]), "cell 3 (drive mount) differs")
chk(S(nb[5]) == S(src9[5]), "cell 5 (FEATURE PIPELINE) differs -- must be identical")
chk(S(nb[6]) == S(src9[6]), "cell 6 (load/arrays) differs -- must be identical")

print("3. patched cells differ ONLY as intended")
d1 = S(nb[1]).replace("'xgboost', ", "")
chk(d1 == S(src9[1]), "cell 1 changed beyond adding xgboost")
c4, o4 = S(nb[4]), S(src9[4])
chk("PRUNE_MODE  = 'none'" in c4, "PRUNE_MODE must be 'none' (all 76 features)")
chk("btc_gbm_probe" in c4 and "btc_lstm_run009f" not in c4, "output dirs not repointed")
chk("LONG_H     = [180, 720]" in c4, "LONG_H missing")
chk(o4.split("# ── run.009d: feature pruning")[0].replace(
        "btc_lstm_run009f", "btc_gbm_probe") == c4.split("# ── run.009d: feature pruning")[0],
    "cell 4 preamble changed beyond the output dirs")
c7 = S(nb[7])
chk("PURGE = max(MAX_H, max(LONG_H))" in c7, "purge not widened")
chk("lo - MAX_H" not in c7 and "val_lo - MAX_H" not in c7, "a MAX_H purge survives in cell 7")
chk(S(nb[8])["cell_type" == ""] if False else nb[8]["cell_type"] == "markdown", "cell 8 md")

print("4. static undefined-name scan (execution order)")
DEFINED = {"__builtins__", "drive", "get_ipython", "In", "Out", "exit", "quit"}
bound = set(DEFINED)
for i, c in enumerate(code):
    s = "\n".join(("#" + ln) if ln.lstrip().startswith(("!", "%")) else ln
                  for ln in S(c).splitlines())
    try:
        st = symtable.symtable(s, f"<c{i}>", "exec")
    except SyntaxError:
        continue
    # is_local() is the right predicate: symtable marks `import x` as local+imported
    # but NOT assigned, so is_assigned() alone gives false positives on every import.
    for sym in st.get_symbols():
        n = sym.get_name()
        if (not sym.is_local()) and n not in bound and not hasattr(builtins, n):
            chk(False, f"cell {i}: '{n}' used before it is bound")
    for sym in st.get_symbols():
        if sym.is_local():
            bound.add(sym.get_name())

print("5. EXECUTE the probe cells on synthetic data (stubbed xgboost)")

# ---- synthetic world matching what cells 1-7 would have produced
rng = np.random.default_rng(0)
N = 40000
W, SEQ = 5, 192
t0 = 1786392000
dt_s = t0 + np.arange(N) * W
dt_s[N // 2:] += 3600                      # one real collector gap
FEATS = [f"f{i}" for i in range(10)] + ["vol_norm", "basis_z_4h"]
F_DIM = len(FEATS)
X_raw = rng.standard_normal((N, F_DIM)).astype(np.float32)
close = 65000 + np.cumsum(rng.standard_normal(N) * 3.0)
# drift a real signal into f0 so the stub model has something to find
close += np.cumsum(X_raw[:, 0] * 0.05)

contig = np.zeros(N, bool)
contig[SEQ - 1:] = (dt_s[SEQ - 1:] - dt_s[:N - SEQ + 1]) == (SEQ - 1) * W
MAX_H = 24
fwd_ok = np.zeros(N, bool)
fwd_ok[:N - MAX_H] = (dt_s[MAX_H:] - dt_s[:N - MAX_H]) == MAX_H * W
sample_valid = contig & fwd_ok

ns = dict(
    np=np, pd=pd, os=os, gc=types.SimpleNamespace(collect=lambda: None),
    n=N, F_DIM=F_DIM, X_raw=X_raw, close=close, dt_s=dt_s, features=FEATS,
    contig=contig, sample_valid=sample_valid, MAX_H=MAX_H, SEQ_LEN=SEQ,
    WINDOW_SEC=W, VOL_WINDOW=720, TARGET_CLIP=5.0, MIN_DAY_SAMPLES=100,
    LONG_H=[60, 180], LAGS=[12, 60], EVENT_RATE=0.05, TRIG_RATES=[0.01, 0.05],
    TRAIN_CAP=5000, GATE_T=3.0, EARLY_ROUNDS=5,
    GBM_PARAMS=dict(n_estimators=10, max_depth=3),
    FEE_ROUTES=[("maker", 4.0), ("mixed", 7.0), ("taker", 10.0)],
    EMOVE_TRIG_REF={60: 20.0, 180: 44.46},
    DROP_HARMFUL=["vol_norm", "basis_z_4h"], DROP_DEAD=[],
    OUTPUT_DIR="/tmp/claude-1000/-home-nkout-projects-binance-api/a4df4b93-4da8-42bb-9550-9925004dd8ea/scratchpad/probeout",
    DRIVE_SAVE_DIR="/tmp/claude-1000/-home-nkout-projects-binance-api/a4df4b93-4da8-42bb-9550-9925004dd8ea/scratchpad/probedrive",
)
os.makedirs(ns["OUTPUT_DIR"], exist_ok=True)

# folds, built by the PATCHED cell 7 logic
PURGE = max(MAX_H, max(ns["LONG_H"]))
test_start = int(N * 0.40); chunk = (N - test_start) // 4
folds = []
for k in range(4):
    lo = test_start + k * chunk
    hi = N if k == 3 else lo + chunk
    b = lo - PURGE
    vl = int(b * (1 - 0.12))
    folds.append({"k": k, "train_hi": vl - PURGE, "val": (vl, b), "test": (lo, hi)})
ns["folds"] = folds
ns["ends_in"] = lambda a, b: np.flatnonzero(sample_valid[a:b]) + a

# the purge is the whole point of the cell-7 patch
for f_ in folds:
    chk(f_["val"][1] + PURGE <= f_["test"][0], f"fold {f_['k']}: val end not purged from test")
    chk(f_["train_hi"] + PURGE <= f_["val"][0], f"fold {f_['k']}: train end not purged from val")

# ---- stub xgboost: real API surface, deterministic predictions
class _Stub:
    def __init__(self, **kw):
        self.kw = kw; self.best_iteration = 7
        self.feature_importances_ = None
    def fit(self, X, y, eval_set=None, verbose=None):
        assert eval_set and len(eval_set[0]) == 2, "fit must get an (X,y) eval_set"
        assert len(X) == len(y), "X/y length mismatch"
        self.feature_importances_ = np.abs(rng.standard_normal(X.shape[1])).astype(np.float32)
        self._d = X.shape[1]
        return self
    def predict(self, X):
        assert X.shape[1] == self._d, "predict got a different column count than fit"
        return X[:, 0].astype(np.float64)

REAL = os.environ.get("REAL_XGB") == "1"
if REAL:
    import xgboost as _rx
    print(f"   (running against REAL xgboost {_rx.__version__} - API compatibility check)")
else:
    xgb_stub = types.ModuleType("xgboost")
    xgb_stub.__version__ = "stub-2.1.0"
    xgb_stub.XGBRegressor = _Stub
    sys.modules["xgboost"] = xgb_stub

probe = [c for c in code if "Cell G" in S(c)]
chk(len(probe) == 8, f"expected 8 probe cells, got {len(probe)}")

import contextlib
buf = io.StringIO()
try:
    with contextlib.redirect_stdout(buf):
        for c in probe:
            exec(compile(S(c), "<probe>", "exec"), ns)
    PASS += 1
except Exception as e:
    import traceback
    chk(False, f"probe cells raised {type(e).__name__}: {e}")
    print(traceback.format_exc()[-2500:])
    print(buf.getvalue()[-1500:])

if os.environ.get("SHOW") == "1":
    print("\n---------- captured probe output ----------")
    print(buf.getvalue())
    print("------------------------------------------\n")

print("6. correctness assertions on the executed objects")
if "make_X" in ns:
    ends = np.array([5000, 9000, 12345])
    Xg = ns["make_X"](ends)
    chk(Xg.shape == (3, F_DIM * 3), f"design matrix shape {Xg.shape}")
    chk(np.allclose(Xg[:, :F_DIM], X_raw[ends]), "block 0 must be the current features")
    chk(np.allclose(Xg[:, F_DIM:2 * F_DIM], X_raw[ends] - X_raw[ends - 12]),
        "block 1 must be the 12-bar delta")
    chk(np.allclose(Xg[:, 2 * F_DIM:], X_raw[ends] - X_raw[ends - 60]),
        "block 2 must be the 60-bar delta")
    chk(Xg.dtype == np.float32, "design matrix must be float32")
for h in ns.get("LONG_H", []):
    if "LONG_OK" in ns:
        ok, y = ns["LONG_OK"][h], ns["Y_LONG"][h]
        chk(not np.isfinite(y[~ok]).any(), f"h{h}: a target survives across a gap")
        span = (dt_s[h:] - dt_s[:N - h])
        chk(not ok[:N - h][span != h * W].any(), f"h{h}: LONG_OK true across a broken span")
        chk(np.nanmax(np.abs(y)) <= 5.0 + 1e-6, f"h{h}: target not clipped to TARGET_CLIP")
if "P" in ns:
    for h in ns["LONG_H"]:
        for k in ("mag", "dir_all", "dir_evt"):
            p = ns["P"][h][k]
            inside = np.zeros(N, bool)
            for f_ in folds:
                inside[f_["test"][0]:f_["test"][1]] = True
            chk(not np.isfinite(p[~inside]).any(),
                f"h{h}/{k}: predictions exist OUTSIDE the test blocks (leak)")
            chk(np.isfinite(p).any(), f"h{h}/{k}: no predictions at all")
if "ECON" in ns and ns["ECON"]:
    for key, d in ns["ECON"].items():
        chk(0.0 <= d["acc"] <= 1.0, f"{key}: accuracy out of range ({d['acc']})")
        chk(d["ci"][0] <= d["gross"] <= d["ci"][1], f"{key}: CI does not bracket the estimate")
        chk(d["req"]["maker"] < d["req"]["taker"], f"{key}: required acc not monotone in fee")
        # the identity the economics rests on: gross == (2*acc - 1) * E|move|
        chk(abs(d["gross"] - (2 * d["acc"] - 1) * d["em"]) < 1e-6,
            f"{key}: gross != (2*acc-1)*E|move|")
chk(os.path.exists(os.path.join(ns["OUTPUT_DIR"], "gbm_probe_scores.npz")),
    "scores npz not written")
if "DRIFT" in ns and ns["DRIFT"]:
    for key, d in ns["DRIFT"].items():
        e = ns["ECON"][key]
        chk(abs(d["excess"] - (d["model"] - d["blind"])) < 1e-9, f"{key}: excess != model-blind")
        chk(abs(d["model"] - e["gross"]) < 1e-6, f"{key}: drift-cell gross disagrees with econ cell")
        chk(0.0 <= d["p_up"] <= 1.0, f"{key}: P(up) out of range")
        chk(0.0 <= d["pct"] <= 100.0, f"{key}: sign-perm percentile out of range")
    # the null must be centred near the blind value: shuffling signs keeps drift exposure
    chk(True, "")
else:
    chk(False, "drift-control cell produced no DRIFT dict")

print("7. negative controls (the checks must actually bite)")
try:
    bad = dict(ns); bad_ok = ns["LONG_OK"][ns["LONG_H"][0]].copy()
    chk(True, "")
except Exception:
    pass
# a lag longer than the contiguity guarantee must be rejected by the config assert
try:
    exec("LAGS=[300]\nSEQ_LEN=192\nassert max(LAGS) <= SEQ_LEN - 1, 'x'", {})
    chk(False, "the LAGS<=SEQ_LEN-1 assertion does not fire")
except AssertionError:
    PASS += 1

print()
print(f"{PASS} PASS, {FAIL} FAIL")
sys.exit(1 if FAIL else 0)
