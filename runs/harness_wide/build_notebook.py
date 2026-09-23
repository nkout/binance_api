"""Build runs/btc_wide_probe.ipynb (W1, next_signal_ideas.md Round 3) — Colab, GPU xgboost + torch.

    python build_notebook.py [out.ipynb]    (default ../btc_wide_probe.ipynb; refuses to overwrite an
                                             executed notebook)

The grid loader and the 60 engineered features are copied verbatim from the R1 builder
(harness_latency/build_notebook.py), whose tests prove them equal to the 1d notebook's features.
"""
import importlib.util, os, sys
import nbformat as nbf

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "btc_wide_probe.ipynb")
_spec = importlib.util.spec_from_file_location("R1b", os.path.join(HERE, "..", "harness_latency", "build_notebook.py"))
R1 = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(R1)
R1_LOAD_DEFS = R1.C_LOAD.split("t0 = time.time()")[0].split("\n", 1)[1]
R1_FEAT_DEFS = R1.C_FEAT.split("t0 = time.time()")[0].split("\n", 1)[1]

MD0 = r"""# W1 — wide raw-input 5 s direction probe (MLP / CNN on all collector columns), two label arms

**Question.** Every model so far saw at most 76 engineered features. The 5 s collector writes **825
columns** per bar (full spot + futures ladders, OFI, add/cancel flow, early/late trade counts, walls,
bursts, ETH mid). Can a network that reads all of them find direction that **survives entry latency**?

**Design** (pre-registered in `next_signal_ideas.md`, Round 3, W1):
- Inputs: `w5_wide.parquet`, ~664 columns after mechanical transforms only (`harness_wide/extract_w5_wide.py`,
  spec in `wide_spec.json`).
- **Label D (primary):** first touch of ±10 bp within 90 s measured from the bar **after** the signal
  (t + 5 s). The model cannot earn the first-seconds move R1 showed is uncatchable.
  **Label Z (secondary):** the same from t.
- Models per label: `mlp` (wide MLP on bar + 1-min mean + 5-min mean), `cnn` (1-D CNN over the last
  12 bars × all channels), controls `xgbw` (xgboost, same flat wide inputs) and `xgb60` (xgboost, the
  60 engineered features). Training rows = every bar whose barrier was touched. Weekly walk-forward
  from day 21, 7-day val, purge 4 × 90 s.
- Trades as in R1: stage-1 triggers, confident tail with an expanding causal threshold, one position,
  hold 90 s from entry, delay 0 / 5 / 10 / 15 / 30 s.

**Verdict.** P1 (primary): any of `mlp`, `cnn`, `xgbw` under label D has top 1 % or top 2 % gross at a
**5 s** delay with day-bootstrap CI lower bound **> 9.0 bp**, n ≥ 100. P2: `mlp` or `cnn` AUC − `xgbw`
AUC ≥ 0.02 with CI lower bound > 0. P3: best wide AUC − `xgb60` AUC ≥ 0.02 with CI lower bound > 0.

**How to run.** Upload `data/w5_wide.parquet` (~1.5 GB) to `MyDrive/w5_wide.parquet`; `w5_60d.parquet`
is already there. Colab → Runtime → **GPU** (T4 is enough; High-RAM helps) → Run all.
Outputs: `MyDrive/btc_wide_probe/`.
"""

C_SETUP = r"""# Cell 1 — environment
import os, sys, json, time, subprocess, warnings
IN_COLAB = 'google.colab' in sys.modules
if IN_COLAB:
    subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', 'xgboost>=2.0'], check=False)
import numpy as np, pandas as pd, xgboost as xgb, pyarrow.parquet as pq
import torch, torch.nn as nn, torch.nn.functional as TF
from sklearn.metrics import roc_auc_score
warnings.filterwarnings('ignore', category=RuntimeWarning)

def _has_gpu():
    try:
        return subprocess.run(['nvidia-smi'], capture_output=True).returncode == 0
    except FileNotFoundError:
        return False

DEVICE = 'cuda' if _has_gpu() else 'cpu'
TDEV = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f'xgboost {xgb.__version__} on {DEVICE} | torch {torch.__version__} on {TDEV} | colab {IN_COLAB}')
if IN_COLAB:
    from google.colab import drive
    drive.mount('/content/drive')
"""

C_CONFIG = r"""# Cell 2 — config
SMOKE = os.environ.get('SMOKE') == '1'
W5_DATA = os.environ.get('W5_DATA', '/content/drive/MyDrive/w5_60d.parquet')
WIDE_DATA = os.environ.get('WIDE_DATA', '/content/drive/MyDrive/w5_wide.parquet')
OUT_DIR = os.environ.get('OUT_DIR', '/content/drive/MyDrive/btc_wide_probe')
LIMIT_DAYS = float(os.environ.get('LIMIT_DAYS', '0'))        # > 0: first N days only (tests)

BAR = 5
H_SEC, THETA_TRAIN = 90, 10.0
HB = H_SEC // BAR                                           # 18 bars
LABELS = {'D': 1, 'Z': 0}                                   # label starts this many bars after the signal
STAGE1_WIN_15, STAGE1_LOOKBACK_DAYS, PRIMARY_RATE = 20, 7, 0.05
TAILS, PRIMARY_TAILS = [0.01, 0.02, 0.05, 0.10], [0.01, 0.02]
DELAYS_SEC = [0, 5, 10, 15, 30]
PATH_SEC = [5, 10, 15, 30, 60, 90, 120]
MIN_TAIL_DAYS, MIN_TRAIN_DAYS, VAL_DAYS = 3, 21, 7
PURGE = 4 * HB + 1
MEAN_WINS = (12, 60)                                        # 1 min, 5 min causal means for the flat input
CNN_WIN, CNN_CH = 12, 128
PASS_FEE, P2_MIN_DIFF = 9.0, 0.02
MODELS = ['xgb60', 'xgbw', 'mlp', 'cnn']
WIDE_MODELS = ['xgbw', 'mlp', 'cnn']
SEEDS = [0] if SMOKE else [0, 1]
N_ROUNDS = 60 if SMOKE else 3000
EARLY = 10 if SMOKE else 150
XGB_PARAMS = dict(max_depth=4, learning_rate=0.03, subsample=0.8, colsample_bytree=0.8,
                  min_child_weight=50, reg_lambda=5.0, tree_method='hist', device=DEVICE)
MLP_HIDDEN, DROPOUT = (512, 256, 128), 0.3
LR, WD, BATCH = 1e-3, 1e-3, 1024
EPOCHS = 2 if SMOKE else 40
PATIENCE = 1 if SMOKE else 5
INCLUDE_DEAD, TIME_FEATS = False, False
N_BOOT = 300 if SMOKE else 2000
RNG = np.random.default_rng(0)
os.makedirs(OUT_DIR, exist_ok=True)
print(f'SMOKE={SMOKE} LIMIT_DAYS={LIMIT_DAYS} | labels {LABELS} theta {THETA_TRAIN} bp / {H_SEC} s | out {OUT_DIR}')
"""

C_BASE = r"""# Cell 3 — 5 s grid, 60 engineered features, labels D / Z, stage-1 triggers (code verbatim from R1)
""" + R1_LOAD_DEFS + R1_FEAT_DEFS + r"""
def shift_lab(L, k):
    # label measured from bar t + k, stored at t (k = 1: entry one bar after the signal)
    if k == 0:
        return L
    return {key: np.r_[v[k:], np.zeros(k, bool) if v.dtype == bool else np.full(k, np.nan, v.dtype)]
            for key, v in L.items()}

t0 = time.time()
g5, ts5 = load_grid(W5_DATA, BAR)
if LIMIT_DAYS > 0:
    G_ = int(LIMIT_DAYS * 86400 // BAR)
    g5, ts5 = g5.iloc[:G_].reset_index(drop=True), ts5[:G_]
F60, lm, s1 = build_features(g5, ts5, BAR)
X60, FEATS60 = F60.to_numpy(np.float32), list(F60.columns)
G = len(ts5); day = ts5 // 86400; bday = 86400 // BAR
trig = stage1_triggers(s1, day, PRIMARY_RATE, STAGE1_LOOKBACK_DAYS, bday)
L0 = first_touch(lm, HB, THETA_TRAIN)
LAB = {L: shift_lab(L0, k) for L, k in LABELS.items()}
ROWS = np.flatnonzero((LAB['D']['touched'] | LAB['Z']['touched'] | trig) & np.isfinite(lm))
del g5
for L in LABELS:
    v = LAB[L]['valid']
    print(f'label {L}: touched {LAB[L]["touched"][v].mean() * 100:.2f} % of valid bars, '
          f'P(up | touched) {LAB[L]["up"][LAB[L]["touched"]].mean():.3f}')
print(f'grid {G:,} bars ({G / bday:.1f} d) | stage-1 {trig.mean() * 100:.2f} % | rows needed {len(ROWS):,} | '
      f'{len(FEATS60)} engineered features | {time.time() - t0:.0f}s')
"""

C_WIDE = r"""# Cell 4 — wide inputs onto the same grid (fp16), flat [bar, 1-min mean, 5-min mean] for the rows needed
def rolling_at(col, rows, w):
    # causal nan-mean of col over [t - w + 1, t], evaluated at `rows`
    x = col.astype(np.float64); m = np.isfinite(x)
    cs = np.r_[0.0, np.cumsum(np.where(m, x, 0.0))]; cn = np.r_[0, np.cumsum(m)]
    lo = np.maximum(rows - w + 1, 0)
    s, n = cs[rows + 1] - cs[lo], cn[rows + 1] - cn[lo]
    return np.where(n > 0, s / np.maximum(n, 1), np.nan)

t0 = time.time()
tab = pq.read_table(WIDE_DATA, filters=[('ts', '<=', int(ts5[-1]))] if LIMIT_DAYS > 0 else None)
WCOLS = [c for c in tab.column_names if c != 'ts']; K = len(WCOLS)
tsw = tab.column('ts').to_numpy().astype(np.int64)
slot = np.round((tsw - ts5[0]) / BAR).astype(np.int64)
okw = (slot >= 0) & (slot < G)
okw[okw] &= np.abs(tsw[okw] - ts5[slot[okw]]) <= 1
GW = np.full((G, K), np.nan, np.float16)
for j, c in enumerate(WCOLS):
    GW[slot[okw], j] = np.clip(tab.column(c).to_numpy()[okw], -6e4, 6e4).astype(np.float16)
del tab
present = np.zeros(G, bool); present[slot[okw]] = True
print(f'wide: {K} columns | grid rows present {present.mean() * 100:.1f} % | needed rows present '
      f'{present[ROWS].mean() * 100:.1f} % | GW {GW.nbytes / 1e9:.2f} GB | {time.time() - t0:.0f}s')

t0 = time.time()
nR = len(ROWS)
row_of = np.full(G, -1, np.int64); row_of[ROWS] = np.arange(nR)
XW = np.empty((nR, K * (1 + len(MEAN_WINS))), np.float32)
for j in range(K):
    col = GW[:, j]
    XW[:, j] = col[ROWS]
    for i, w in enumerate(MEAN_WINS):
        XW[:, K * (i + 1) + j] = rolling_at(col, ROWS, w)
print(f'flat wide input {XW.shape} ({XW.nbytes / 1e9:.2f} GB) | {time.time() - t0:.0f}s')
"""

C_MODELS = r"""# Cell 5 — models. Preprocessing statistics come from the training rows of each fit only.
class Prep:
    def __init__(self, X):
        X = np.where(np.isfinite(X), X, np.nan)
        self.lo = np.nan_to_num(np.nanpercentile(X, 0.5, 0)); self.hi = np.nan_to_num(np.nanpercentile(X, 99.5, 0))
        Xc = np.clip(X, self.lo, self.hi)
        self.mu = np.nan_to_num(np.nanmean(Xc, 0)); sd = np.nan_to_num(np.nanstd(Xc, 0))
        self.sd = np.where(sd > 1e-9, sd, 1.0).astype(np.float32)
    def __call__(self, X):
        X = np.where(np.isfinite(X), X, np.nan)
        return np.nan_to_num((np.clip(X, self.lo, self.hi) - self.mu) / self.sd, nan=0.0).astype(np.float32)
    def torch(self):
        return [torch.as_tensor(np.asarray(a, np.float32), device=TDEV) for a in (self.lo, self.hi, self.mu, self.sd)]

def make_mlp(d_in):
    layers, d = [], d_in
    for h in MLP_HIDDEN:
        layers += [nn.Linear(d, h), nn.GELU(), nn.Dropout(DROPOUT)]; d = h
    return nn.Sequential(*layers, nn.Linear(d, 1)).to(TDEV)

class CNN(nn.Module):
    # 1x1 conv = learned features across all ~664 channels, then two temporal convs over 12 bars
    def __init__(self, k):
        super().__init__()
        self.mix = nn.Sequential(nn.Conv1d(k, CNN_CH, 1), nn.GELU(), nn.Dropout(DROPOUT))
        self.tconv = nn.Sequential(nn.Conv1d(CNN_CH, CNN_CH, 3, padding=1), nn.GELU(),
                                   nn.Conv1d(CNN_CH, CNN_CH, 3, padding=1), nn.GELU())
        self.head = nn.Sequential(nn.Linear(2 * CNN_CH, 64), nn.GELU(), nn.Dropout(DROPOUT), nn.Linear(64, 1))
    def forward(self, x):                                   # x (B, CNN_WIN, K), last step = bar t
        h = self.tconv(self.mix(x.transpose(1, 2)))
        return self.head(torch.cat([h.mean(2), h[:, :, -1]], 1))

WIN_OFF = np.arange(-CNN_WIN + 1, 1)
GWT = None
def cnn_window_batch(rows, stats):
    # (B, CNN_WIN, K) normalised windows ending at each row; never reads bars after the row
    global GWT
    if GWT is None:
        GWT = torch.as_tensor(GW, device=TDEV)
    lo, hi, mu, sd = stats
    idx = torch.as_tensor(np.maximum(rows[:, None] + WIN_OFF, 0), device=TDEV)
    x = GWT[idx].float()
    x = torch.where(torch.isfinite(x), x, torch.full_like(x, float('nan')))
    x = (torch.clamp(x, lo, hi) - mu) / sd
    return torch.nan_to_num(x, nan=0.0)

def _predict(net, fn, n, bs=8192):
    net.eval(); out = []
    with torch.no_grad():
        for i in range(0, n, bs):
            out.append(torch.sigmoid(net(fn(slice(i, min(n, i + bs)))).squeeze(1)).float().cpu().numpy())
    return np.concatenate(out) if out else np.zeros(0, np.float32)

def _train(net, fn_tr, ytr, fn_va, yva, seed):
    opt = torch.optim.AdamW(net.parameters(), lr=LR, weight_decay=WD)
    gen = torch.Generator().manual_seed(seed)
    yt = torch.as_tensor(np.asarray(ytr, np.float32), device=TDEV)
    best, best_ep, bad, state = -np.inf, -1, 0, None
    for ep in range(EPOCHS):
        net.train(); perm = torch.randperm(len(ytr), generator=gen).numpy()
        for i in range(0, len(perm), BATCH):
            b = perm[i:i + BATCH]
            loss = TF.binary_cross_entropy_with_logits(net(fn_tr(b)).squeeze(1), yt[torch.as_tensor(b, device=TDEV)])
            opt.zero_grad(); loss.backward(); opt.step()
        pv = _predict(net, fn_va, len(yva))
        auc = roc_auc_score(yva, pv) if len(np.unique(yva)) == 2 else 0.5
        if auc > best + 1e-4:
            best, best_ep, bad = auc, ep, 0; state = {k: v.detach().clone() for k, v in net.state_dict().items()}
        else:
            bad += 1
            if bad >= PATIENCE: break
    net.load_state_dict(state)
    return net, best_ep

def _rows(r):
    i = row_of[r]
    assert (i >= 0).all(), 'a fit row is missing from ROWS'
    return i

def fit_mlp(tr, ytr, va, yva, te, seed):
    torch.manual_seed(seed)
    prep = Prep(XW[_rows(tr)])
    A, B, C = (torch.as_tensor(prep(XW[_rows(r)]), device=TDEV) for r in (tr, va, te))
    net = make_mlp(A.shape[1])
    net, ep = _train(net, lambda b: A[torch.as_tensor(b, device=TDEV)] if not isinstance(b, slice) else A[b],
                     ytr, lambda s: B[s], yva, seed)
    return _predict(net, lambda s: C[s], len(te)), ep

def fit_cnn(tr, ytr, va, yva, te, seed):
    torch.manual_seed(seed)
    stats = Prep(GW[tr].astype(np.float32)).torch()
    net = CNN(K).to(TDEV)
    net, ep = _train(net, lambda b: cnn_window_batch(tr[b], stats), ytr,
                     lambda s: cnn_window_batch(va[s], stats), yva, seed)
    return _predict(net, lambda s: cnn_window_batch(te[s], stats), len(te)), ep

def _fit_xgb(Xtr, ytr, Xva, yva, Xte, seed):
    clf = xgb.XGBClassifier(n_estimators=N_ROUNDS, early_stopping_rounds=EARLY, eval_metric='auc',
                            random_state=seed, **XGB_PARAMS)
    clf.fit(Xtr, ytr, eval_set=[(Xva, yva)], verbose=False)
    return clf.predict_proba(Xte)[:, 1], clf.best_iteration

def fit_xgb60(tr, ytr, va, yva, te, seed):
    return _fit_xgb(X60[tr], ytr, X60[va], yva, X60[te], seed)

def fit_xgbw(tr, ytr, va, yva, te, seed):
    return _fit_xgb(XW[_rows(tr)], ytr, XW[_rows(va)], yva, XW[_rows(te)], seed)

FIT = dict(xgb60=fit_xgb60, xgbw=fit_xgbw, mlp=fit_mlp, cnn=fit_cnn)
print(f'models {MODELS} | MLP {MLP_HIDDEN} on {XW.shape[1]} inputs | CNN {CNN_WIN} bars x {K} ch -> {CNN_CH}')
"""

C_TRAIN = r"""# Cell 6 — weekly walk-forward: 2 labels x 4 models; scored on stage-1 trigger bars of each test week
P = {(L, m): np.full(G, np.nan, np.float32) for L in LABELS for m in MODELS}
LOG, blocks = [], []
b = int(np.searchsorted(ts5, ts5[0] + MIN_TRAIN_DAYS * 86400))
while b < G:
    e = min(G, b + 7 * bday); blocks.append((b, e)); b = e
if SMOKE:
    blocks = blocks[-2:]
t0 = time.time()
for (a, e) in blocks:
    va_hi = a - PURGE; va_lo = va_hi - VAL_DAYS * bday
    te = np.arange(a, e)[trig[a:e] & np.isfinite(lm[a:e])]
    for L in LABELS:
        Lb = LAB[L]; y = Lb['up'].astype(np.int8)
        ok = Lb['touched'] & np.isfinite(lm)                # the signal bar itself must exist (it is in ROWS)
        tr = np.arange(0, va_lo - PURGE)[ok[:va_lo - PURGE]]
        va = np.arange(va_lo, va_hi)[ok[va_lo:va_hi]]
        if len(tr) < 500 or len(va) < 100 or len(te) == 0:
            print(f'  {L} block {a}: skipped (train {len(tr)}, val {len(va)}, test {len(te)})'); continue
        for m in MODELS:
            ps, its = [], []
            for sd in SEEDS:
                p, it = FIT[m](tr, y[tr], va, y[va], te, sd); ps.append(p); its.append(int(it))
            P[(L, m)][te] = np.mean(ps, 0)
            LOG.append(dict(label=L, model=m, block=str(pd.to_datetime(ts5[a], unit='s').date()),
                            n_train=len(tr), n_val=len(va), n_test=len(te), best=its))
    print(f'block {pd.to_datetime(ts5[a], unit="s").date()}: train {len(tr):,} val {len(va):,} '
          f'test {len(te):,} | {time.time() - t0:.0f}s', flush=True)
print(pd.DataFrame(LOG).to_string(index=False))
"""

C_AUC = r"""# Cell 7 — AUC on touched stage-1 triggers, and the paired day-bootstrap tests P2 / P3
def auc_(y, p):
    return float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else np.nan

def auc_mask(L):
    return trig & LAB[L]['touched'] & np.isfinite(P[(L, 'xgb60')])

def paired_diff(L, m1, m2):
    msk = auc_mask(L) & np.isfinite(P[(L, m1)]) & np.isfinite(P[(L, m2)])
    idx = np.flatnonzero(msk); y = LAB[L]['up'][idx]; p1, p2 = P[(L, m1)][idx], P[(L, m2)][idx]
    d0 = auc_(y, p1) - auc_(y, p2)
    dd = day[idx]; ud = np.unique(dd); by = {u: np.flatnonzero(dd == u) for u in ud}
    bs = []
    for _ in range(N_BOOT // 2):
        ii = np.concatenate([by[u] for u in RNG.choice(ud, len(ud))])
        bs.append(auc_(y[ii], p1[ii]) - auc_(y[ii], p2[ii]))
    return float(d0), [float(x) for x in np.nanpercentile(bs, [2.5, 97.5])], int(len(idx)), int(len(ud))

AUC = {}
for L in LABELS:
    msk = auc_mask(L)
    for m in MODELS:
        AUC[f'{L}|{m}'] = dict(n=int(msk.sum()), auc=auc_(LAB[L]['up'][msk], P[(L, m)][msk]))
print(pd.DataFrame(AUC).T.to_string())
TESTS = {}
for m in ('mlp', 'cnn'):
    d0, ci, n, nd = paired_diff('D', m, 'xgbw')
    TESTS[f'P2 {m} - xgbw'] = dict(diff=d0, ci=ci, n=n, days=nd, pass_=bool(d0 >= P2_MIN_DIFF and ci[0] > 0))
best_w = max(WIDE_MODELS, key=lambda m: AUC[f'D|{m}']['auc'] if np.isfinite(AUC[f'D|{m}']['auc']) else -1)
d0, ci, n, nd = paired_diff('D', best_w, 'xgb60')
TESTS[f'P3 {best_w} - xgb60'] = dict(diff=d0, ci=ci, n=n, days=nd, pass_=bool(d0 >= P2_MIN_DIFF and ci[0] > 0))
for L in LABELS:                                            # information: every wide model vs xgb60
    for m in WIDE_MODELS:
        d0, ci, n, nd = paired_diff(L, m, 'xgb60')
        TESTS[f'info {L} {m} - xgb60'] = dict(diff=d0, ci=ci, n=n, days=nd)
for k, v in TESTS.items():
    print(f'{k:24s} diff {v["diff"]:+.4f} CI [{v["ci"][0]:+.4f}, {v["ci"][1]:+.4f}] n {v["n"]} days {v["days"]}'
          + (f'  -> {"PASS" if v["pass_"] else "fail"}' if 'pass_' in v else ''))
"""

C_DECAY = r"""# Cell 8 — confident tail x entry delay, one position at a time, hold 90 s after entry
def boot(v, days):
    ud = np.unique(days)
    s = np.array([v[days == u].sum() for u in ud]); c = np.array([(days == u).sum() for u in ud])
    bi = RNG.integers(0, len(ud), (N_BOOT, len(ud)))
    mm = s[bi].sum(1) / c[bi].sum(1)
    return [float(np.percentile(mm, 2.5)), float(np.percentile(mm, 97.5))]

def tail_candidates(p, q):
    cand = trig & np.isfinite(p); conf = np.abs(p - 0.5)
    sel = np.zeros(G, bool)
    for i, d in enumerate(np.unique(day[cand])):
        if i < MIN_TAIL_DAYS:
            continue
        cur = cand & (day == d)
        sel[cur] = conf[cur] >= np.quantile(conf[cand & (day < d)], 1 - q)
    return np.flatnonzero(sel)

def trades(p, q, dsec):
    d = dsec // BAR; take, busy = [], -1
    for t in tail_candidates(p, q):
        if t > busy and t + d + HB < G and np.isfinite(lm[t:t + d + HB + 1]).all():
            take.append(t); busy = t + d + HB
    take = np.array(take, np.int64)
    side = np.where(p[take] >= 0.5, 1, -1) if len(take) else np.array([], int)
    ret = (lm[take + d + HB] - lm[take + d]) * 1e4 if len(take) else np.array([])
    return take, side, ret

ROWS_D = []
for (L, m), p in P.items():
    for q in TAILS:
        for dsec in DELAYS_SEC:
            take, side, ret = trades(p, q, dsec)
            r = dict(label=L, model=m, tail=q, delay=dsec, n=int(len(take)))
            if len(take) >= 20:
                g_ = side * ret
                r.update(days=int(len(np.unique(day[take]))), acc=float((g_ > 0).mean()), gross=float(g_.mean()),
                         gross_ci=boot(g_, day[take]), long=float(g_[side == 1].mean()) if (side == 1).any() else np.nan,
                         short=float(g_[side == -1].mean()) if (side == -1).any() else np.nan, blind=float(ret.mean()))
            ROWS_D.append(r)
DEC = pd.DataFrame(ROWS_D)
show = DEC[DEC['tail'].isin(PRIMARY_TAILS)].copy()
if 'gross_ci' in show:
    show['gross_ci'] = show['gross_ci'].apply(lambda c: f'[{c[0]:+.2f},{c[1]:+.2f}]' if isinstance(c, list) else '')
print(show.round(3).to_string(index=False))

PATHS = {}                                                   # accrual: mean signed move since signal close
for (L, m), p in P.items():
    take, side, _ = trades(p, 0.01, 0)
    take = take[take + max(PATH_SEC) // BAR < G]; side = np.where(p[take] >= 0.5, 1, -1)
    if len(take) >= 20:
        PATHS[f'{L}|{m}'] = {f'+{s}s': float(np.nanmean(side * (lm[take + s // BAR] - lm[take]) * 1e4)) for s in PATH_SEC}
print('\nmean signed move since the signal close, top 1 %, bp:')
print(pd.DataFrame(PATHS).T.round(2).to_string())
"""

C_VERDICT = r"""# Cell 9 — pre-registered verdict
P1 = {}
for m in WIDE_MODELS:
    for q in PRIMARY_TAILS:
        r = DEC[(DEC['label'] == 'D') & (DEC['model'] == m) & (DEC['tail'] == q) & (DEC['delay'] == 5)]
        if r.empty or 'gross' not in r or pd.isna(r['gross'].iloc[0]):
            P1[f'{m}|{q}'] = dict(pass_=False, n=int(r['n'].iloc[0]) if len(r) else 0, note='too few trades'); continue
        n, g, ci = int(r['n'].iloc[0]), float(r['gross'].iloc[0]), r['gross_ci'].iloc[0]
        P1[f'{m}|{q}'] = dict(n=n, gross=g, ci=ci, pass_=bool(n >= 100 and ci[0] > PASS_FEE))
for k, v in P1.items():
    print(f'P1 D {k:10s}: ' + (f'gross @5 s {v["gross"]:+.2f} CI [{v["ci"][0]:+.2f},{v["ci"][1]:+.2f}] n {v["n"]}'
                                if 'gross' in v else v['note']) + f'  -> {"PASS" if v["pass_"] else "fail"}')
PASS_P1 = any(v['pass_'] for v in P1.values())
PASS_P2 = any(v.get('pass_') for k, v in TESTS.items() if k.startswith('P2'))
PASS_P3 = any(v.get('pass_') for k, v in TESTS.items() if k.startswith('P3'))
print('VERDICT P1 (economics):', 'PASS — a wide-input tail survives a 5 s delay at the reachable fee' if PASS_P1
      else 'FAIL — wide inputs do not produce a tradeable 5 s-delayed signal')
print('VERDICT P2 (DNN vs trees on wide inputs):', 'PASS' if PASS_P2 else 'FAIL')
print('VERDICT P3 (wide vs 60 engineered):', 'PASS' if PASS_P3 else 'FAIL')
"""

C_SAVE = r"""# Cell 10 — save
def _j(o):
    if isinstance(o, dict): return {str(k): _j(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)): return [_j(v) for v in o]
    if isinstance(o, np.ndarray): return o.tolist()
    if isinstance(o, (np.floating, np.integer, np.bool_)): return o.item()
    return o
res = dict(config=dict(LABELS=LABELS, THETA_TRAIN=THETA_TRAIN, H_SEC=H_SEC, TAILS=TAILS, DELAYS_SEC=DELAYS_SEC,
                       PASS_FEE=PASS_FEE, SEEDS=SEEDS, SMOKE=SMOKE, LIMIT_DAYS=LIMIT_DAYS, K=K, MODELS=MODELS),
           fits=LOG, auc=AUC, tests=TESTS, decay=DEC.to_dict('records'), paths=PATHS, p1=P1,
           passed=dict(P1=PASS_P1, P2=PASS_P2, P3=PASS_P3))
json.dump(_j(res), open(os.path.join(OUT_DIR, 'wide_probe_results.json'), 'w'), indent=1, default=float)
keep = np.isfinite(P[('D', 'xgb60')])
np.savez_compressed(os.path.join(OUT_DIR, 'wide_probe_scores.npz'), ts=ts5[keep], lm=lm[keep], trig=trig[keep],
                    **{f'p_{L}_{m}': P[(L, m)][keep] for (L, m) in P},
                    **{f'up_{L}': LAB[L]['up'][keep] for L in LABELS},
                    **{f'touched_{L}': LAB[L]['touched'][keep] for L in LABELS})
print('saved to', OUT_DIR, os.listdir(OUT_DIR))
"""

CELLS = [("md", MD0), ("code", C_SETUP), ("code", C_CONFIG), ("code", C_BASE), ("code", C_WIDE),
         ("code", C_MODELS), ("code", C_TRAIN), ("code", C_AUC), ("code", C_DECAY), ("code", C_VERDICT),
         ("code", C_SAVE)]


def build(path=OUT):
    if os.path.exists(path):
        old = nbf.read(path, as_version=4)
        if any(c.get("outputs") for c in old.cells if c.cell_type == "code"):
            raise SystemExit(f"refusing to overwrite executed notebook {path}")
    nb = nbf.v4.new_notebook()
    nb.cells = [nbf.v4.new_markdown_cell(s) if k == "md" else nbf.v4.new_code_cell(s) for k, s in CELLS]
    nb.metadata = {"accelerator": "GPU", "colab": {"gpuType": "T4", "provenance": []},
                   "kernelspec": {"display_name": "Python 3", "name": "python3"},
                   "language_info": {"name": "python"}}
    nbf.write(nb, path)
    return path


if __name__ == "__main__":
    print("wrote", build(sys.argv[1] if len(sys.argv) > 1 else OUT))
