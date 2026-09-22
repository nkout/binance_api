"""Build runs/btc_lstm.probe.gbm.ipynb from runs/btc_lstm.run.009f.ipynb.

Cells 0-6 (markdown header, install, transfer, drive, config, feature pipeline,
load/arrays) are inherited; only the config cell is patched. Cell 7 (folds) is
patched for a wider purge. Cells 8+ (the LSTM programme) are replaced by the
GBM probe.
"""
import json, copy, io, os

SRC = "/home/nkout/projects/binance_api/runs/btc_lstm.run.009f.ipynb"
DST = "/home/nkout/projects/binance_api/runs/btc_lstm.probe.gbm2.ipynb"

nb = json.load(open(SRC))
cells = nb["cells"]


def src(c):
    return "".join(c["source"])


def mkcode(s):
    return {"cell_type": "code", "execution_count": None, "metadata": {},
            "outputs": [], "source": s.splitlines(keepends=True)}


def mkmd(s):
    return {"cell_type": "markdown", "metadata": {}, "source": s.splitlines(keepends=True)}


# ───────────────────────────────────────────── header
HEADER = """# GBM long-horizon probe — do the EXISTING features carry direction at 15 min / 1 h?

**This is a probe, not a gated run.** It exists to answer one question cheaply,
before any two-stage notebook is built:

> The 90 s programme (runs 007-013) failed because the fee is larger than the move.
> On the stage-1 trigger set the required directional accuracy is **0.77 at 90 s**
> but only **0.61 at 15 min** and **0.59 at 1 h** (taker), because `E|move|` on
> triggers grows 18.6 -> 44.5 -> 55.8 bp while the fee stays at 10 bp.
> The volatility detector still concentrates magnitude 3.6x / 2.3x at those horizons.
> **So: is there any directional signal at 15 min - 1 h in the 76 existing features?**

Measured context (`runs/horizon_economics_and_next_ideas.md`, `run009f_scores.npz`):

| | 90 s | 5 min | 15 min | 1 h |
|---|---|---|---|---|
| IC of the *existing* LSTM `pred` | +0.046 | +0.034 | +0.025 | +0.006 |
| daily-IC t | +4.1 | +1.6 | +0.2 | -0.9 |
| stage-1 \\|move\\| lift | 4.63x | 4.33x | 3.59x | 2.27x |
| E\\|move\\| on triggers | 18.6 bp | 31.7 bp | 44.5 bp | 55.8 bp |
| **required accuracy @ taker 10 bp** | 0.769 | 0.658 | **0.612** | **0.590** |
| **required accuracy @ maker 4 bp** | 0.607 | 0.563 | **0.545** | **0.536** |

The existing LSTM's direction does **not** transfer past ~5 min, so a long-horizon
model has to be trained fresh. This notebook does that with gradient boosting
(fast to iterate; the LSTM overfits at epoch 1 in 10/12 seed-folds anyway).

## What it does

1. Inherits run.009f's **exact** feature pipeline — cells 1-5 are byte-identical,
   so the 76 features are the same objects, built the same way.
2. Builds **long-horizon targets** (15 min, 1 h) with a causal, horizon-scaled
   volatility normalisation.
3. Walk-forward over the **same 4 expanding folds**, with the purge widened from
   `MAX_H`(24) to `max(LONG_H)` so no long label reaches across a fold boundary.
4. Trains three GBMs per fold per horizon:
   - **stage 1 / `mag`** — predicts `|y|` (the big-move detector)
   - **stage 2a / `dir_all`** — predicts signed `y` on all bars
   - **stage 2b / `dir_evt`** — predicts signed `y` trained **only on eventful bars**
     (the "train direction on big moves only" hypothesis)
5. Reports IC, daily-IC t, directional accuracy **on the stage-1 trigger set**,
   net bp at 4 / 7 / 10 bp with day-clustered CIs, and gain importance with the
   13 `DROP_HARMFUL` + 5 `DROP_DEAD` features **put back in** (`PRUNE_MODE='none'`)
   so the prune can be re-judged on a direction objective instead of h18 AP.

## Pre-registered read-out

- **A (signal)** — `dir` daily-IC t >= `GATE_T` (3.0) at 15 min or 1 h, IC > 0 in >= 3/4 folds.
- **B (economics)** — directional accuracy on the stage-1 top-0.1% >= the required
  accuracy for the **maker** route at that horizon (0.545 @15 min, 0.536 @1 h).
- **C (net)** — net bp after 7 bp (realistic maker-in/taker-out) > 0 with a
  day-clustered CI excluding 0.
- **Falsification** — A fails at both horizons -> the existing bar features carry no
  usable long-horizon direction; do **not** build the two-stage notebook. Move to
  new inputs (run.013 price panel) or close the direction line.

A **pass on A but not B** is the informative middle: signal exists but is too weak
to trade, which is the run.009d-f pattern and must not be read as success.

## Runtime

GPU (T4) ~10-20 min, CPU ~40-80 min. Set `TRAIN_CAP` lower to trade accuracy for speed.
Needs `60days_data.tar` on Drive, same as run.009f.
"""

out = [mkmd(HEADER)]

# ───────────────────────────────────────────── cell 1: imports (+xgboost)
c1 = copy.deepcopy(cells[1])
s = src(c1)
assert "'scikit-learn'" in s
s = s.replace("'torch',\n                'scikit-learn'", "'torch',\n                'xgboost', 'scikit-learn'")
assert "'xgboost'" in s
c1["source"] = s.splitlines(keepends=True)
out.append(c1)

# ───────────────────────────────────────────── cells 2,3 verbatim
out.append(copy.deepcopy(cells[2]))
out.append(copy.deepcopy(cells[3]))

# ───────────────────────────────────────────── cell 4: config patch
c4 = copy.deepcopy(cells[4])
s = src(c4)
rep = [
    ("OUTPUT_DIR = '/root/btc_lstm_run009f'", "OUTPUT_DIR = '/root/btc_gbm_probe'"),
    ("DRIVE_SAVE_DIR = '/content/drive/MyDrive/btc_lstm_run009f'",
     "DRIVE_SAVE_DIR = '/content/drive/MyDrive/btc_gbm_probe'"),
    ("PRUNE_MODE  = 'harmful+dead'   # 'none' | 'harmful' | 'harmful+dead'",
     "PRUNE_MODE  = 'none'           # PROBE: keep all 76 - the prune was measured on\n"
     "                               # h18 AP (a MAGNITUDE metric); this run re-judges it\n"
     "                               # on a long-horizon DIRECTION metric."),
]
for a, b in rep:
    assert s.count(a) == 1, ("config patch miss", a)
    s = s.replace(a, b)

PROBE_CFG = '''

# ══════════════════════════════════════════════════════════════════════════
# GBM long-horizon probe — config
# ══════════════════════════════════════════════════════════════════════════
LONG_H     = [180, 720]        # 15 min and 1 h at 5 s bars
LAGS       = [12, 60, 180]     # feature deltas: 1 min / 5 min / 15 min
EVENT_RATE = 0.05              # 'eventful' share for the dir_evt training subset
TRIG_RATES = [0.001, 0.01]     # stage-1 trigger rates to evaluate
TRAIN_CAP  = 400_000           # max training rows per fold (RAM/speed); 0 = no cap
GATE_T     = 3.0               # criterion A: daily-IC t threshold
EARLY_ROUNDS = 50

GBM_PARAMS = dict(
    n_estimators=3000,         # capped by early stopping on the fold's val split
    max_depth=5,               # shallow on purpose: the signal is weak and noisy
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.5,
    min_child_weight=200,      # heavy leaf smoothing - financial targets are ~noise
    reg_lambda=2.0,
)

# Required directional accuracy on the stage-1 trigger set, p = (1 + F/E|move|)/2.
# E|move| measured from run009f_scores.npz (horizon_economics_and_next_ideas.md
# section 1 / section 6); recomputed from this run's own data in the economics cell.
EMOVE_TRIG_REF = {180: 44.46, 720: 55.82}   # bp, top-0.1% of the run.009f vol detector
FEE_ROUTES = [('maker', 4.0), ('mixed', 7.0), ('taker', 10.0)]

assert max(LAGS) <= SEQ_LEN - 1, 'a lag reaches past the SEQ_LEN contiguity guarantee'
print(f'\\nPROBE: long horizons ' +
      '  '.join(f'h{h} ({h*WINDOW_SEC/60:.0f} min)' for h in LONG_H))
print(f'       lags {LAGS}  event rate {EVENT_RATE:.0%}  train cap {TRAIN_CAP:,}')
'''
s = s.rstrip("\n") + "\n" + PROBE_CFG
c4["source"] = s.splitlines(keepends=True)
out.append(c4)

# ───────────────────────────────────────────── cells 5,6 verbatim (pipeline, load)
out.append(copy.deepcopy(cells[5]))
out.append(copy.deepcopy(cells[6]))

# ───────────────────────────────────────────── cell 7: folds, widened purge
c7 = copy.deepcopy(cells[7])
s = src(c7)
old_hdr = "def ends_in(a, b):"
assert s.count(old_hdr) == 1
s = s.replace(old_hdr, """# PROBE: the purge must cover the LONGEST label, not MAX_H(=24). A 1 h label
# started just before a fold boundary resolves 720 bars later, i.e. inside the
# next split — with the run.009f purge that is direct label leakage into test.
PURGE = max(MAX_H, max(LONG_H))
print(f'purge = {PURGE} bars ({PURGE*WINDOW_SEC/60:.0f} min)  '
      f'[run.009f used MAX_H = {MAX_H}]')

def ends_in(a, b):""")
for a, b in [("boundary = lo - MAX_H ", "boundary = lo - PURGE "),
             ("'train_hi': val_lo - MAX_H,", "'train_hi': val_lo - PURGE,")]:
    assert s.count(a) == 1, ("fold patch miss", a)
    s = s.replace(a, b)
c7["source"] = s.splitlines(keepends=True)
out.append(c7)

# ═════════════════════════════════════════════ NEW CELLS
out.append(mkmd("## Probe — long-horizon targets, GBM walk-forward, economics"))

G1 = '''# ── Cell G1: long-horizon targets ─────────────────────────────────────────
# Built here rather than inside prepare() so cells 1-5 stay byte-identical to
# run.009f and MAX_H / sample_valid / contig keep exactly their run.009f meaning.
#
#   Y_LONG[h][e] = (close[e+h] - close[e]) / vol_h[e]   vol_h uses PAST bars only
#   RAW_BP[h][e] = (close[e+h]/close[e] - 1) * 1e4      for the economics cell
#
# vol_h mirrors prepare()'s own formula (rolling std of the h-bar price change)
# with a horizon-scaled window, because a 720-bar diff inside a 720-bar window
# would be a single overlapping observation.

cser = pd.Series(close)
Y_LONG, RAW_BP, LONG_OK = {}, {}, {}

print(f'{"h":>6} {"wall":>8} {"vol win":>9} {"valid":>12} {"E|move|":>9} {"sd(y)":>7} {"up%":>6}')
for h in LONG_H:
    vol_n = max(VOL_WINDOW, 8 * h)
    vol = cser.diff(h).rolling(vol_n, min_periods=vol_n // 4).std().values

    ok = np.zeros(n, dtype=bool)
    ok[:n - h] = (dt_s[h:] - dt_s[:n - h]) == h * WINDOW_SEC   # no label across a gap

    dpx = np.full(n, np.nan); dpx[:n - h] = close[h:] - close[:n - h]
    rbp = np.full(n, np.nan); rbp[:n - h] = (close[h:] / close[:n - h] - 1.0) * 1e4
    y = np.clip(dpx / (vol + 1e-12), -TARGET_CLIP, TARGET_CLIP)

    good = ok & np.isfinite(y) & np.isfinite(vol) & (vol > 0)
    y[~good] = np.nan; rbp[~good] = np.nan
    Y_LONG[h], RAW_BP[h], LONG_OK[h] = y, rbp, good

    print(f'{h:>6} {h*WINDOW_SEC/60:>7.0f}m {vol_n:>9,} {good.sum():>12,} '
          f'{np.nanmean(np.abs(rbp)):>8.2f}b {np.nanstd(y[good]):>7.3f} '
          f'{(rbp[good] > 0).mean()*100:>5.1f}%')

# Sanity: a long target must never be finite where the forward span is broken.
for h in LONG_H:
    assert not np.isfinite(Y_LONG[h][~LONG_OK[h]]).any(), f'h{h}: target survives a gap'
print('\\ngap-validity assertion passed for all long horizons')
'''
out.append(mkcode(G1))

G2 = '''# ── Cell G2: GBM design matrix ────────────────────────────────────────────
# A GBM cannot consume a (SEQ_LEN x F_DIM) window, so each sample end e is encoded
# as the current feature vector plus its change over each lag. contig[e] already
# guarantees SEQ_LEN-1 = 191 contiguous bars behind e, so every lag (<= 180) is a
# real bar rather than one across a collector gap — asserted in the config cell.

GBM_COLS = list(features) + [f'{f_}_d{L}' for L in LAGS for f_ in features]
N_COL = len(GBM_COLS)

def make_X(ends):
    """(len(ends), F_DIM*(1+len(LAGS))) float32 design matrix for the given ends."""
    ends = np.asarray(ends)
    out = np.empty((len(ends), N_COL), dtype=np.float32)
    cur = X_raw[ends]
    out[:, :F_DIM] = cur
    for k, L in enumerate(LAGS):
        out[:, F_DIM * (k + 1):F_DIM * (k + 2)] = cur - X_raw[ends - L]
    return out

def split_ends(f_, h):
    """train / val / test ends for fold f_ at horizon h, all long-target-valid."""
    v = contig & LONG_OK[h]
    tr = ends_in(0, f_['train_hi']);  tr = tr[v[tr]]
    va = ends_in(*f_['val']);         va = va[v[va]]
    te = ends_in(*f_['test']);        te = te[v[te]]
    return tr, va, te

print(f'design matrix: {F_DIM} features x (1 + {len(LAGS)} lags) = {N_COL} columns')
for h in LONG_H:
    tr, va, te = split_ends(folds[0], h)
    print(f'  h{h:<5} fold 0:  train {len(tr):>9,}   val {len(va):>8,}   test {len(te):>8,}')
'''
out.append(mkcode(G2))

G3 = '''# ── Cell G3: walk-forward GBM (stage 1 = mag, stage 2 = dir_all / dir_evt) ──
import xgboost as xgb

_gpu = False
try:
    import torch
    _gpu = torch.cuda.is_available()
except Exception:
    pass
DEV = 'cuda' if _gpu else 'cpu'
print(f'xgboost {xgb.__version__}   device = {DEV}\\n')

def fit_gbm(Xtr, ytr, Xva, yva, seed=0):
    m = xgb.XGBRegressor(tree_method='hist', device=DEV, random_state=seed,
                         early_stopping_rounds=EARLY_ROUNDS, eval_metric='rmse',
                         **GBM_PARAMS)
    m.fit(Xtr, ytr, eval_set=[(Xva, yva)], verbose=False)
    return m

def cap(ix, rng):
    if TRAIN_CAP and len(ix) > TRAIN_CAP:
        return np.sort(rng.choice(ix, TRAIN_CAP, replace=False))
    return ix

# out-of-fold prediction stores, NaN outside the test blocks
P = {h: {k: np.full(n, np.nan) for k in ('mag', 'dir_all', 'dir_evt')} for h in LONG_H}
IMP = {h: {k: np.zeros(N_COL) for k in ('mag', 'dir_all', 'dir_evt')} for h in LONG_H}
BEST_IT = {h: {k: [] for k in ('mag', 'dir_all', 'dir_evt')} for h in LONG_H}

for h in LONG_H:
    y = Y_LONG[h]
    print(f'═══ h{h} ({h*WINDOW_SEC/60:.0f} min) ' + '═' * 46)
    for f_ in folds:
        rng = np.random.default_rng(1234 + f_['k'])
        tr, va, te = split_ends(f_, h)
        tr = cap(tr, rng)
        Xtr, Xva, Xte = make_X(tr), make_X(va), make_X(te)
        ytr, yva = y[tr], y[va]

        # stage 1 — magnitude (the big-move detector)
        m_mag = fit_gbm(Xtr, np.abs(ytr), Xva, np.abs(yva))
        # stage 2a — direction on everything
        m_dall = fit_gbm(Xtr, ytr, Xva, yva)
        # stage 2b — direction trained ONLY on eventful bars (train/val only)
        thr_tr = np.quantile(np.abs(ytr), 1 - EVENT_RATE)
        thr_va = np.quantile(np.abs(yva), 1 - EVENT_RATE)
        etr, eva = np.abs(ytr) >= thr_tr, np.abs(yva) >= thr_va
        m_devt = fit_gbm(Xtr[etr], ytr[etr], Xva[eva], yva[eva])

        for key, mdl in (('mag', m_mag), ('dir_all', m_dall), ('dir_evt', m_devt)):
            P[h][key][te] = mdl.predict(Xte)
            IMP[h][key] += mdl.feature_importances_
            BEST_IT[h][key].append(getattr(mdl, 'best_iteration', -1))

        print(f'  fold {f_["k"]}  train {len(tr):>8,} (evt {etr.sum():>7,})  '
              f'test {len(te):>8,}   best_iter mag/dir/evt = '
              f'{BEST_IT[h]["mag"][-1]}/{BEST_IT[h]["dir_all"][-1]}/{BEST_IT[h]["dir_evt"][-1]}')
        del Xtr, Xva, Xte; gc.collect()

for h in LONG_H:
    for k in IMP[h]:
        IMP[h][k] /= len(folds)
print('\\nwalk-forward complete')
'''
out.append(mkcode(G3))

G4 = '''# ── Cell G4: CRITERION A — is there any signal? ───────────────────────────
from scipy.stats import spearmanr

def daily_ic_t(pred_, y_, ends):
    """t-stat of the mean daily Spearman IC over test days (the project's metric)."""
    d = dt_s[ends] // 86400
    ics = []
    for u in np.unique(d):
        m = d == u
        if m.sum() < MIN_DAY_SAMPLES:
            continue
        r = spearmanr(pred_[ends][m], y_[ends][m]).statistic
        if np.isfinite(r):
            ics.append(r)
    ics = np.asarray(ics)
    if len(ics) < 5:
        return np.nan, np.nan, len(ics)
    return ics.mean() / (ics.std(ddof=1) / np.sqrt(len(ics))), ics.mean(), len(ics)

print(f'{"h":>5} {"model":>8} | {"pooled IC":>10} {"daily t":>8} {"days":>5} | '
      + '  '.join(f'{"f"+str(f_["k"]):>7}' for f_ in folds) + f' {"folds>0":>8}')
SIG = {}
for h in LONG_H:
    y = Y_LONG[h]
    for key in ('dir_all', 'dir_evt'):
        p = P[h][key]
        ok = np.isfinite(p) & np.isfinite(y)
        e = np.flatnonzero(ok)
        ic = spearmanr(p[e], y[e]).statistic
        t, icm, nday = daily_ic_t(p, y, e)
        per = []
        for f_ in folds:
            lo, hi = f_['test']
            m = e[(e >= lo) & (e < hi)]
            per.append(spearmanr(p[m], y[m]).statistic if len(m) > 100 else np.nan)
        SIG[(h, key)] = dict(ic=ic, t=t, per=per)
        print(f'{h:>5} {key:>8} | {ic:>+10.4f} {t:>+8.2f} {nday:>5} | '
              + '  '.join(f'{v:>+7.4f}' for v in per)
              + f' {sum(1 for v in per if v > 0):>6}/{len(folds)}')

print(f'\\nCRITERION A: daily-IC t >= {GATE_T} and IC > 0 in >= {len(folds)-1}/{len(folds)} folds')
for (h, key), d in SIG.items():
    ok = (d['t'] >= GATE_T) and sum(1 for v in d['per'] if v > 0) >= len(folds) - 1
    print(f'  h{h:<5} {key:<8}  t={d["t"]:+6.2f}  folds>0={sum(1 for v in d["per"] if v>0)}/{len(folds)}'
          f'   -> {"PASS" if ok else "fail"}')
'''
out.append(mkcode(G4))

G5 = '''# ── Cell G5: CRITERIA B & C — economics on the stage-1 trigger set ────────
# Stage 1 selects the top TRIG_RATE by predicted |y| within each fold; stage 2
# supplies the sign. Net bp = (2*acc - 1) * E|move| - fee, measured directly as
# mean(sign(dir) * raw_bp) - fee, with a day-clustered bootstrap CI.

def day_boot_ci(vals, days, n_boot=3000, seed=0):
    rng = np.random.default_rng(seed)
    ud = np.unique(days)
    idx = {d: np.where(days == d)[0] for d in ud}
    out = np.empty(n_boot)
    for i in range(n_boot):
        sel = np.concatenate([idx[d] for d in rng.choice(ud, len(ud), replace=True)])
        out[i] = vals[sel].mean()
    return tuple(np.percentile(out, [2.5, 97.5]))

def fold_top(score, rate, valid):
    """top-`rate` of `score` within each fold's test block (per-fold threshold)."""
    t = np.zeros(n, dtype=bool)
    for f_ in folds:
        lo, hi = f_['test']
        m = valid.copy(); m[:lo] = False; m[hi:] = False
        k = int(round(rate * m.sum()))
        if k < 1:
            continue
        ii = np.flatnonzero(m)
        s = score[ii]
        t[ii[s >= np.partition(s, -k)[-k]]] = True
    return t

ECON = {}
for h in LONG_H:
    rb, y = RAW_BP[h], Y_LONG[h]
    print(f'\\n═══ h{h} ({h*WINDOW_SEC/60:.0f} min) ' + '═' * 62)
    for rate in TRIG_RATES:
        valid = np.isfinite(P[h]['mag']) & np.isfinite(rb)
        trig = fold_top(P[h]['mag'], rate, valid)
        em = np.abs(rb[trig]).mean()
        lift = em / np.abs(rb[np.isfinite(rb)]).mean()
        req = {nm: (1 + F / em) / 2 for nm, F in FEE_ROUTES}
        print(f'\\n  rate {rate:.2%}   n={trig.sum():,}   E|move| {em:.2f} bp '
              f'(lift {lift:.2f}x)   required acc: '
              + '  '.join(f'{nm} {p:.3f}' for nm, p in req.items()))
        print(f'    {"stage2":>8} {"acc":>7} {"gross":>8} '
              + ' '.join(f'{"net "+nm:>9}' for nm, _ in FEE_ROUTES) + f'   {"95% CI (gross)":>22}')
        for key in ('dir_all', 'dir_evt'):
            m = trig & np.isfinite(P[h][key])
            sgn = np.sign(P[h][key][m])
            v = sgn * rb[m]
            acc = (v.mean() / np.abs(rb[m]).mean() + 1) / 2
            lo, hi = day_boot_ci(v, dt_s[m] // 86400)
            ECON[(h, rate, key)] = dict(n=int(m.sum()), em=em, acc=acc,
                                        gross=v.mean(), ci=(lo, hi), req=req)
            print(f'    {key:>8} {acc:>7.3f} {v.mean():>+8.2f} '
                  + ' '.join(f'{v.mean()-F:>+9.2f}' for _, F in FEE_ROUTES)
                  + f'   [{lo:>+8.2f}, {hi:>+8.2f}]')

print(f'\\n\\nCRITERION B: accuracy on top-{TRIG_RATES[0]:.1%} >= required (maker route)')
print(f'CRITERION C: net at mixed 7 bp > 0 with the day-clustered CI excluding 0')
for (h, rate, key), d in ECON.items():
    if rate != TRIG_RATES[0]:
        continue
    b = d['acc'] >= d['req']['maker']
    c = (d['gross'] - 7.0 > 0) and (d['ci'][0] - 7.0 > 0)
    print(f'  h{h:<5} {key:<8}  acc {d["acc"]:.3f} vs {d["req"]["maker"]:.3f} -> '
          f'{"PASS" if b else "fail"}   |   net7 {d["gross"]-7:+.2f} '
          f'[{d["ci"][0]-7:+.2f}, {d["ci"][1]-7:+.2f}] -> {"PASS" if c else "fail"}')
'''
out.append(mkcode(G5))

G5B = '''# ── Cell G5b: DRIFT CONTROL — is criterion B beating skill, or beating drift? ──
# Defect found after the v1 run: criterion B compared accuracy to the FEE-breakeven
# number only. On high-volatility bars in this window the market drifts up hard, so a
# PERMANENT LONG scores 0.51-0.59 at 15 min and up to 0.68 at 1 h — i.e. B is passable
# with zero skill. run009d.offline.md section 8 solved this once already (blind
# benchmark + day-matched null); this cell ports the control the v1 probe was missing.
#
# Three references, all on the model's OWN trigger set:
#   blind      — always-long on the same bars (the drift the selection is exposed to)
#   signperm   — the model's own predicted signs SHUFFLED across the same bars, so the
#                sign multiset and therefore the drift exposure are identical. This
#                isolates "does sign ASSIGNMENT carry information", which is exactly
#                what criterion B claims.
#   volmatch   — always-long on bars chosen by CAUSAL trailing realised vol at the same
#                rate: a model-free version of the same selection.

_r1 = np.zeros(n); _ok1 = np.zeros(n, dtype=bool)
_ok1[1:] = (dt_s[1:] - dt_s[:-1]) == WINDOW_SEC
_r1[1:] = np.where(_ok1[1:], (close[1:] / close[:-1] - 1.0) * 1e4, 0.0)
TVOL = pd.Series(_r1).rolling(VOL_WINDOW, min_periods=VOL_WINDOW // 4).std().values

def sign_perm_null(sgn, rb_trig, n_draw=2000, seed=0):
    """null: same bars, same multiset of signs, shuffled."""
    rng = np.random.default_rng(seed)
    out = np.empty(n_draw)
    for i in range(n_draw):
        out[i] = (rng.permutation(sgn) * rb_trig).mean()
    return out

print(f'{"h":>5} {"rate":>7} {"stage2":>8} | {"model":>8} {"blind":>8} {"volmatch":>9} '
      f'{"excess":>8} | {"null p97.5":>10} {"pctile":>7} | {"P(up) trig":>10}')
DRIFT = {}
for h in LONG_H:
    rb = RAW_BP[h]
    for rate in TRIG_RATES:
        valid = np.isfinite(P[h]['mag']) & np.isfinite(rb)
        trig = fold_top(P[h]['mag'], rate, valid)
        blind = rb[trig].mean()
        p_up = (rb[trig] > 0).mean()
        vm = fold_top(np.where(np.isfinite(TVOL), TVOL, -np.inf), rate, valid)
        volmatch = rb[vm].mean()
        for key in ('dir_all', 'dir_evt'):
            m = trig & np.isfinite(P[h][key])
            sgn = np.sign(P[h][key][m])
            g = (sgn * rb[m]).mean()
            null = sign_perm_null(sgn, rb[m])
            pct = (null < g).mean() * 100
            DRIFT[(h, rate, key)] = dict(model=g, blind=blind, volmatch=volmatch,
                                         excess=g - blind, p_up=p_up,
                                         null_hi=np.percentile(null, 97.5), pct=pct)
            print(f'{h:>5} {rate:>7.2%} {key:>8} | {g:>+8.2f} {blind:>+8.2f} {volmatch:>+9.2f} '
                  f'{g-blind:>+8.2f} | {np.percentile(null,97.5):>+10.2f} {pct:>6.1f}% | {p_up:>10.3f}')

print()
print('REVISED CRITERION B — accuracy must beat BOTH the fee requirement AND always-long:')
for (h, rate, key), d in DRIFT.items():
    if rate != TRIG_RATES[0]:
        continue
    e = ECON[(h, rate, key)]
    need = max(e['req']['maker'], d['p_up'])
    ok = e['acc'] >= need
    print(f'  h{h:<5} {key:<8}  acc {e["acc"]:.3f}  vs  max(fee {e["req"]["maker"]:.3f}, '
          f'drift {d["p_up"]:.3f}) = {need:.3f}  -> {"PASS" if ok else "fail"}'
          f'   [excess over blind {d["excess"]:+.2f} bp, sign-perm pctile {d["pct"]:.1f}%]')
print()
print('A cell that beats the fee requirement but not always-long is measuring drift')
print('in this window, not directional skill. The sign-permutation percentile is the')
print('cleanest read: it holds the bars AND the sign multiset fixed.')
'''
out.append(mkcode(G5B))

G6 = '''# ── Cell G6: which features matter for DIRECTION (the prune, re-judged) ───
# run.009b/009d ranked features by the h18 up-head AP drop — a MAGNITUDE metric at
# a 90 s horizon — and dropped 13 as "harmful" + 5 as "dead". PRUNE_MODE='none'
# put them back. If a dropped feature ranks high here, the prune was measured on
# the wrong objective for long-horizon direction.

DROPPED = set(DROP_HARMFUL) | set(DROP_DEAD)

def base_name(col):
    for L in LAGS:
        if col.endswith(f'_d{L}'):
            return col[:-len(f'_d{L}')]
    return col

for h in LONG_H:
    print(f'\\n═══ h{h} ({h*WINDOW_SEC/60:.0f} min) — dir_all gain importance ' + '═' * 28)
    imp = IMP[h]['dir_all']
    # aggregate lag variants back onto the parent feature
    agg = {}
    for c_, v in zip(GBM_COLS, imp):
        agg[base_name(c_)] = agg.get(base_name(c_), 0.0) + float(v)
    tot = sum(agg.values()) or 1.0
    rank = sorted(agg.items(), key=lambda kv: -kv[1])
    print(f'  {"#":>3} {"feature":<24} {"share":>7}  {"was pruned by run.009d?":<24}')
    for i, (f_, v) in enumerate(rank[:25], 1):
        tag = 'DROP_HARMFUL' if f_ in set(DROP_HARMFUL) else ('DROP_DEAD' if f_ in set(DROP_DEAD) else '')
        print(f'  {i:>3} {f_:<24} {v/tot*100:>6.2f}%  {tag:<24}')
    share_dropped = sum(v for f_, v in agg.items() if f_ in DROPPED) / tot
    n_top = sum(1 for f_, _ in rank[:25] if f_ in DROPPED)
    print(f'\\n  the 18 run.009d-dropped features carry {share_dropped*100:.1f}% of total gain'
          f'  ({n_top}/25 of the top 25)')
    print(f'  -> {"the prune looks WRONG for this objective" if share_dropped > 18/len(features) else "the prune looks defensible here"}')
'''
out.append(mkcode(G6))

G7 = '''# ── Cell G7: verdict + save ───────────────────────────────────────────────
print('=' * 78)
print('PROBE VERDICT')
print('=' * 78)
any_a = False
for h in LONG_H:
    for key in ('dir_all', 'dir_evt'):
        d = SIG[(h, key)]
        a = (d['t'] >= GATE_T) and sum(1 for v in d['per'] if v > 0) >= len(folds) - 1
        any_a = any_a or a
        e = ECON.get((h, TRIG_RATES[0], key))
        if e is None:
            print(f'  h{h:<5} {key:<8}  A(signal) {"PASS" if a else "fail":<4}   '
                  f'B/C not evaluated (no triggers)')
            continue
        b = e['acc'] >= e['req']['maker']
        c = (e['gross'] - 7.0 > 0) and (e['ci'][0] - 7.0 > 0)
        print(f'  h{h:<5} {key:<8}  A(signal) {"PASS" if a else "fail":<4}   '
              f'B(accuracy) {"PASS" if b else "fail":<4}   C(net) {"PASS" if c else "fail":<4}'
              f'   [IC t {d["t"]:+.2f}, acc {e["acc"]:.3f} vs {e["req"]["maker"]:.3f}]')

print()
if not any_a:
    print('  FALSIFIED: no long-horizon directional signal in the existing 76 features.')
    print('  -> Do NOT build the two-stage notebook on these inputs. The remaining')
    print('     options are new input classes (run.013 price panel) or closing the')
    print('     direction line. See horizon_economics_and_next_ideas.md section 6.7.')
else:
    print('  Criterion A passed somewhere: a long-horizon signal exists.')
    print('  Read B before celebrating — A-pass/B-fail is the run.009d-f pattern')
    print('  (signal real, too weak to trade) and is NOT a green light.')

np.savez_compressed(
    os.path.join(OUTPUT_DIR, 'gbm_probe_scores.npz'),
    **{f'p_{h}_{k}': P[h][k] for h in LONG_H for k in P[h]},
    **{f'imp_{h}_{k}': IMP[h][k] for h in LONG_H for k in IMP[h]},
    **{f'y_{h}': Y_LONG[h] for h in LONG_H},
    **{f'rawbp_{h}': RAW_BP[h] for h in LONG_H},
    dt_s=dt_s, close=close, gbm_cols=np.array(GBM_COLS),
    features=np.array(features), long_h=np.array(LONG_H),
    fold_test_lo=np.array([f_['test'][0] for f_ in folds]),
    fold_test_hi=np.array([f_['test'][1] for f_ in folds]),
)
print(f'\\nsaved -> {OUTPUT_DIR}/gbm_probe_scores.npz')

import shutil
os.makedirs(DRIVE_SAVE_DIR, exist_ok=True)
shutil.copy(os.path.join(OUTPUT_DIR, 'gbm_probe_scores.npz'), DRIVE_SAVE_DIR)
print(f'copied -> {DRIVE_SAVE_DIR}')
'''
out.append(mkcode(G7))

# Clear every executed artefact inherited from run.009f. Carrying a parent run's
# outputs into a child notebook is defect #4 in run009f.notes.md ("10 cells carried
# run.009d's executed outputs, incl. a stale EARLY_STOP=ap print").
for c in out:
    if c["cell_type"] == "code":
        c["outputs"] = []
        c["execution_count"] = None

nb["cells"] = out
nb.setdefault("metadata", {})
json.dump(nb, io.open(DST, "w", encoding="utf-8"), indent=1)
print(f"wrote {DST}  ({len(out)} cells, {os.path.getsize(DST):,} bytes)")
