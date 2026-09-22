"""Build runs/btc_v1_stage2_probe.ipynb (Colab, GPU xgboost) from the cell sources below.

    python build_notebook.py        -> writes ../btc_v1_stage2_probe.ipynb (outputs cleared)
"""
import os
import nbformat as nbf

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "btc_v1_stage2_probe.ipynb")

MD0 = r"""# v1-year two-stage probe — can a model trained only on big moves call their direction?

**Question (user's proposal, `next_signal_ideas.md` follow-up).** Use the 331-day v1 collector year at
15 s. Stage 1 flags upcoming big moves; stage 2 is trained **only on big-move bars** and predicts
**which way** the move goes. Is stage 2 good enough to trade?

**Why this probe and not a full run.** The year adds ~10–15× more big-move examples than run.014's
plan had and 11 months of regimes. It does not add the v3 microstructure block (OFI, add/cancel,
microprice), which carried 97 % of the 5 s signal (run.009b). So the one thing tested here is
stage 2. Stage 1 is **model-free** — a trailing 5-min realised |return| score, causal
7-day rolling quantile — because `breakout_probe.analysis.md` §2 showed it matches the LSTM
detector's magnitude lift.

**Design.**
- Label (primary): first touch of ±20 bp on the futures mid within **90 s (6 bars)**; `y = 1` if up first.
  Secondary: ±30 bp within 5 min (exploratory).
- Stage-2 arms: **`evt`** trains on every training bar where a barrier was touched ("only big moves");
  **`trig`** trains only on stage-1 triggers that were touched (matches the deployment distribution).
- Walk-forward by calendar month, expanding window, last 10 days of each training window = early-stopping
  val, purge `4·max(h)` bars at every boundary. xgboost on GPU (`device='cuda'`), 3-seed average.
- Test set for the verdict: stage-1 triggers (5 % rate) in each test month.

**Pre-registered kill criteria — primary cell `evt`, 90 s / 20 bp, stage-1 rate 5 %. PASS needs all four:**
- **K1** pooled test AUC ≥ **0.65** and per-month AUC ≥ 0.65 in ≥ 75 % of test months
  (0.65 ≈ the full-maker break-even on this kind of trigger set, `run014.plan.md` §9.3).
- **K2** pooled AUC above the p97.5 of a **label-permutation refit null** (shuffled training labels, refit).
- **K3** one-position trade accuracy > max(best constant side on the same trades, fee-required accuracy at 7 bp).
- **K4** net at 7 bp > 0 with a day-bootstrap 95 % CI lower bound > 0.

**K1 fails → direction is not learnable at a tradeable strength from the v1 features; close the two-stage idea.**
Everything outside the primary cell is exploratory.

**How to run.** 1) Locally: `python runs/harness_v1_stage2/extract_v1_15s.py` → `data/v1_year/v1_15s.parquet`.
2) Upload it to `MyDrive/v1_15s.parquet`. 3) Colab → Runtime → GPU → Run all. Outputs go to
`MyDrive/btc_v1_stage2_probe/`. GPU ≈ 10–20 min; CPU works too (auto-detected), several × slower.
"""

C_SETUP = r"""# Cell 1 — environment
import os, sys, json, time, subprocess
IN_COLAB = 'google.colab' in sys.modules
if IN_COLAB:
    subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', 'xgboost>=2.0'], check=False)
import numpy as np, pandas as pd, xgboost as xgb
from sklearn.metrics import roc_auc_score

def _has_gpu():
    try:
        return subprocess.run(['nvidia-smi'], capture_output=True).returncode == 0
    except FileNotFoundError:
        return False

DEVICE = 'cuda' if _has_gpu() else 'cpu'
print(f'xgboost {xgb.__version__} | device {DEVICE} | colab {IN_COLAB}')
if IN_COLAB:
    from google.colab import drive
    drive.mount('/content/drive')
"""

C_CONFIG = r"""# Cell 2 — config (pre-registered primary cell: ARM 'evt', H 6 bars / 20 bp, stage-1 rate 5 %)
SMOKE = os.environ.get('SMOKE') == '1'          # local pipeline test only
DATA_PATH = os.environ.get('V1_DATA', '/content/drive/MyDrive/v1_15s.parquet')
OUT_DIR = os.environ.get('OUT_DIR', '/content/drive/MyDrive/btc_v1_stage2_probe')

BAR = 15                                        # seconds
BARS_DAY = 86400 // BAR                         # 5760
HORIZONS = [(6, 20.0), (20, 30.0)]              # (bars, theta bp); first entry is primary
PRIMARY_H = 6
STAGE1_WIN = 20                                 # 5-min trailing mean |r|
STAGE1_LOOKBACK_DAYS = 7
STAGE1_RATES = [0.02, 0.05, 0.10]
PRIMARY_RATE = 0.05
ARMS = ['evt', 'trig']
PRIMARY_ARM = 'evt'
MIN_TRAIN_DAYS = 60                             # a month is a test month only with >= this history
VAL_DAYS = 10
PURGE_BARS = 4 * max(h for h, _ in HORIZONS)
SEEDS = [0] if SMOKE else [0, 1, 2]
N_PERM = 2 if SMOKE else 20                     # label-permutation refits per test month (K2)
N_ROUNDS = 200 if SMOKE else 3000
EARLY = 30 if SMOKE else 150
XGB_PARAMS = dict(max_depth=4, learning_rate=0.03, subsample=0.8, colsample_bytree=0.8,
                  min_child_weight=50, reg_lambda=5.0, tree_method='hist', device=DEVICE)
FEES = {'taker': 10.0, 'mixed': 7.0, 'maker': 4.0}   # round trip, bp
INCLUDE_DEAD = False      # funding / premium / liquidations die 2026-04-24 (v1_year_data_audit.md §2)
TIME_FEATS = False        # time-of-day / dow carried the run.011 artifact that died walk-forward
AUC_KILL = 0.65
MONTH_FRAC = 0.75
N_BOOT = 500 if SMOKE else 2000
RNG = np.random.default_rng(0)
os.makedirs(OUT_DIR, exist_ok=True)
print(f'SMOKE={SMOKE}  data={DATA_PATH}  out={OUT_DIR}  purge={PURGE_BARS} bars')
"""

C_LOAD = r"""# Cell 3 — load and place on a complete 15 s grid (gaps become NaN rows, so every rolling
# window and every shift is a true wall-clock window; nothing is interpolated)
t0 = time.time()
raw = pd.read_parquet(DATA_PATH)
ts_raw = raw['ts'].to_numpy(np.int64)
T0 = int(ts_raw[0])
slot = np.round((ts_raw - T0) / BAR).astype(np.int64)
off = np.abs((ts_raw - T0) - slot * BAR)
print(f'rows {len(raw):,} | off-grid by >2 s: {(off > 2).mean() * 100:.3f} % (snapped to nearest slot)')
keep = np.r_[slot[1:] != slot[:-1], True]           # collisions after snapping: keep last
raw, slot = raw[keep].reset_index(drop=True), slot[keep]
G = int(slot[-1]) + 1
g = pd.DataFrame(np.nan, index=np.arange(G), columns=[c for c in raw.columns if c != 'ts'], dtype=np.float32)
g.iloc[slot] = raw.drop(columns='ts').to_numpy(np.float32)
ts = T0 + np.arange(G, dtype=np.int64) * BAR
present = np.zeros(G, bool); present[slot] = True
day = ts // 86400
month = pd.to_datetime(ts, unit='s', utc=True).strftime('%y%m').to_numpy()
del raw
print(f'grid {G:,} slots | present {present.mean() * 100:.1f} % | '
      f'{pd.to_datetime(ts[0], unit="s")} -> {pd.to_datetime(ts[-1], unit="s")} | {time.time() - t0:.0f}s')
"""

C_FEAT = r"""# Cell 4 — causal features (every value at bar t uses bars <= t only) and first-touch labels
LV_F = ['0.0', '0.01', '0.05', '0.1', '0.2', '0.4']

def _r(x, n, frac=0.8):
    return x.rolling(n, min_periods=max(1, int(frac * n)))

def _z(x, n):
    m, s = _r(x, n, 0.5).mean(), _r(x, n, 0.5).std()
    return (x - m) / s

def build_features(g, ts):
    f = {}
    fb, fa = g['future_bid_close'], g['future_ask_close']
    mid = (fb + fa) / 2
    lm = np.log(mid)
    r1 = lm.diff() * 1e4
    ar = r1.abs()
    sig = _r(r1, 240, 0.75).std()
    for n in (20, 80, 240):
        f[f'rv_{n}'] = _r(ar, n).mean() / sig
    f['rv_ratio_20_240'] = _r(ar, 20).mean() / _r(ar, 240).mean()
    f['rv_ratio_240_5760'] = _r(ar, 240).mean() / _r(ar, 5760, 0.5).mean()
    f['sig_bp'] = sig
    for n in (1, 4, 20, 80, 240, 960, 5760):
        f[f'ret_{n}'] = (lm - lm.shift(n)) * 1e4 / (sig * np.sqrt(n))
    for n in (240, 960, 5760):
        lo, hi = _r(mid, n, 0.5).min(), _r(mid, n, 0.5).max()
        f[f'rpos_{n}'] = (mid - lo) / (hi - lo)
    hib, lob = g['future_ask_max'], g['future_bid_min']
    f['bar_range'] = (hib - lob) / mid * 1e4 / sig
    f['bar_pos'] = (mid - lob) / (hib - lob)
    bq, sq = g['future_buy_qty'], g['future_sell_qty']
    for n in (1, 4, 20, 80):
        b, s = _r(bq, n).sum(), _r(sq, n).sum()
        f[f'fimb_{n}'] = (b - s) / (b + s)
    bc, sc = g['future_buy_samples'], g['future_sell_samples']
    for n in (1, 20):
        b, s = _r(bc, n).sum(), _r(sc, n).sum()
        f[f'cimb_{n}'] = (b - s) / (b + s)
    sb, ss = g['spot_buy_qty'], g['spot_sell_qty']
    for n in (1, 20):
        b, s = _r(sb, n).sum(), _r(ss, n).sum()
        f[f'simb_{n}'] = (b - s) / (b + s)
    f['flow_div_20'] = f['fimb_20'] - f['simb_20']
    lv = np.log1p(bq + sq)
    f['vol_z'] = (_r(lv, 20).mean() - _r(lv, 5760, 0.5).mean()) / _r(lv, 5760, 0.5).std()
    f['sflow_z'] = (_r(bq - sq, 20).sum() / 20) / _r(bq + sq, 5760, 0.5).mean()
    vw = ((g['future_buy_vwap'] - g['future_sell_vwap']) / mid * 1e4).where((bq > 0) & (sq > 0))
    f['vwap_gap_4'] = vw.rolling(4, min_periods=1).mean()
    for l in LV_F:
        b, a = g[f'future_bid_liq_{l}_median'], g[f'future_ask_liq_{l}_median']
        f[f'dimb_{l}'] = (b - a) / (b + a)
    for l in ('0.0', '0.05'):
        b, a = g[f'future_bid_liq_{l}_close'], g[f'future_ask_liq_{l}_close']
        f[f'dimb_{l}_close'] = (b - a) / (b + a)
    f['dimb_0.05_d4'] = f['dimb_0.05'] - f['dimb_0.05'].shift(4)
    f['dimb_0.05_d20'] = f['dimb_0.05'] - f['dimb_0.05'].shift(20)
    f['dimb_0.1_avg20'] = _r(f['dimb_0.1'], 20).mean()
    dep = np.log(g['future_bid_liq_0.2_median'] + g['future_ask_liq_0.2_median'])
    f['depth_z'] = _z(dep, 5760)
    f['slope_asym'] = (np.log(g['future_bid_liq_0.4_median'] / g['future_bid_liq_0.05_median'])
                       - np.log(g['future_ask_liq_0.4_median'] / g['future_ask_liq_0.05_median']))
    for l in ('0.05', '0.2'):
        b, a = g[f'spot_bid_liq_{l}_median'], g[f'spot_ask_liq_{l}_median']
        f[f'sdimb_{l}'] = (b - a) / (b + a)
    spr = g['future_spread_median'] / mid * 1e4
    f['spread_bp'], f['spread_z'] = spr, _z(spr, 5760)
    f['spread_max_bp'] = g['future_spread_max'] / mid * 1e4
    qb, qa = g['future_bid_samples'], g['future_ask_samples']
    f['quote_imb'] = (qb - qa) / (qb + qa)
    smid = (g['spot_bid_close'] + g['spot_ask_close']) / 2
    basis = (mid - smid) / smid * 1e4
    f['basis_bp'], f['basis_z_960'] = basis, _z(basis, 960)
    f['basis_d20'], f['basis_d240'] = basis - basis.shift(20), basis - basis.shift(240)
    loi = np.log(g['opt_open_interest_sample'])
    for n in (20, 240, 960):
        f[f'oi_d{n}'] = (loi - loi.shift(n)) * 1e4
    f['oi_z'] = _z(loi, 5760)
    lsr = g['opt_long_short_ratio_sample']
    f['lsr'], f['lsr_d240'], f['lsr_z'] = lsr, lsr - lsr.shift(240), _z(lsr, 5760)
    if INCLUDE_DEAD:
        f['funding_bp'] = g['opt_funding_rate_sample'] * 1e4
        f['premium_bp'] = (g['opt_mark_price_sample'] / g['opt_index_price_sample'] - 1) * 1e4
        lq, sq_ = g['opt_long_force_exit_qty_sum'], g['opt_short_force_exit_qty_sum']
        for n in (20, 240):
            a_, b_ = _r(lq, n).sum(), _r(sq_, n).sum()
            f[f'liq_log_{n}'] = np.log1p(a_ + b_)
            f[f'liq_imb_{n}'] = (a_ - b_) / (a_ + b_)
    if TIME_FEATS:
        hr = (ts % 86400) / 3600.0
        dw = ((ts // 86400) + 4) % 7
        f['hour_sin'], f['hour_cos'] = np.sin(2 * np.pi * hr / 24), np.cos(2 * np.pi * hr / 24)
        f['dow_sin'], f['dow_cos'] = np.sin(2 * np.pi * dw / 7), np.cos(2 * np.pi * dw / 7)
    F = pd.DataFrame(f, index=g.index).replace([np.inf, -np.inf], np.nan).astype(np.float32)
    s1 = _r(ar, STAGE1_WIN).mean().to_numpy()       # stage-1 score: raw 5-min mean |r| in bp
    return F, lm.to_numpy(), s1

def first_touch(lm, h, theta_bp):
    # forward path t+1..t+h on the grid; any missing bar -> label invalid
    N = len(lm)
    fw = np.full((N, h), np.nan, np.float32)
    for k in range(1, h + 1):
        fw[:N - k, k - 1] = (lm[k:] - lm[:N - k]) * 1e4
    valid = np.isfinite(fw).all(1) & np.isfinite(lm)
    upx, dnx = fw >= theta_bp, fw <= -theta_bp
    iu = np.where(upx.any(1), upx.argmax(1), h)
    idn = np.where(dnx.any(1), dnx.argmax(1), h)
    touched = valid & ((iu < h) | (idn < h))
    return dict(valid=valid, touched=touched, up=touched & (iu < idn), ret=np.where(valid, fw[:, -1], np.nan))

def stage1_triggers(score, day, rate, lookback):
    # threshold for day d = quantile of the score over the previous `lookback` days only
    trig = np.zeros(len(score), bool)
    ud = np.unique(day)
    lo = np.searchsorted(day, ud, 'left'); hi = np.searchsorted(day, ud, 'right')
    span = {d: (a, b) for d, a, b in zip(ud, lo, hi)}
    for d in ud:
        past = [score[span[x][0]:span[x][1]] for x in range(d - lookback, d) if x in span]
        if not past:
            continue
        past = np.concatenate(past); past = past[np.isfinite(past)]
        if len(past) < 0.5 * lookback * BARS_DAY:
            continue                                # warm-up: not enough history
        a, b = span[d]
        s = score[a:b]
        trig[a:b] = np.isfinite(s) & (s >= np.quantile(past, 1 - rate))
    return trig

t0 = time.time()
F, LM, S1 = build_features(g, ts)
FEATS = list(F.columns)
X = F.to_numpy(np.float32)
LAB = {h: first_touch(LM, h, th) for h, th in HORIZONS}
TRIG = {r: stage1_triggers(S1, day, r, STAGE1_LOOKBACK_DAYS) for r in STAGE1_RATES}
print(f'{len(FEATS)} features | {time.time() - t0:.0f}s')
"""

C_SANITY = r"""# Cell 5 — sanity: stage 1 must concentrate big moves; base rates of the labels
rows = []
for h, th in HORIZONS:
    L = LAB[h]; v = L['valid']
    base_t, base_m = L['touched'][v].mean(), np.nanmean(np.abs(L['ret'][v]))
    for r in STAGE1_RATES:
        m = TRIG[r] & v
        rows.append(dict(h=h, theta=th, rate=r, realised=TRIG[r][v].mean(), n=int(m.sum()),
                         p_touch_trig=L['touched'][m].mean(), p_touch_all=base_t,
                         emove_trig=np.nanmean(np.abs(L['ret'][m])), emove_all=base_m,
                         lift=np.nanmean(np.abs(L['ret'][m])) / base_m,
                         p_up_given_touch_trig=L['up'][m & L['touched']].mean()))
SAN = pd.DataFrame(rows)
print(SAN.round(4).to_string(index=False))
for h, th in HORIZONS:
    L = LAB[h]
    print(f'h{h}/{th:.0f}bp: touched bars {int(L["touched"].sum()):,} of {int(L["valid"].sum()):,} valid '
          f'({L["touched"][L["valid"]].mean() * 100:.2f} %) — the "big move" training pool of arm evt')
"""

C_WF = r"""# Cell 6 — walk-forward stage 2 (GPU xgboost), monthly test blocks
umon = [m for m in dict.fromkeys(month)]
first_idx = {m: int(np.argmax(month == m)) for m in umon}
TEST_MONTHS = [m for m in umon if (ts[first_idx[m]] - ts[0]) >= MIN_TRAIN_DAYS * 86400]
if SMOKE:
    TEST_MONTHS = TEST_MONTHS[:2]
print('test months:', TEST_MONTHS)

def arm_mask(arm, h):
    L = LAB[h]
    return L['touched'] & (TRIG[PRIMARY_RATE] if arm == 'trig' else True)

def split(m):
    a = first_idx[m]; b = a + int((month[a:] == m).sum())
    val_hi = a - PURGE_BARS
    val_lo = val_hi - VAL_DAYS * BARS_DAY
    tr_hi = val_lo - PURGE_BARS
    return (0, tr_hi), (val_lo, val_hi), (a, b)

def fit_predict(Xtr, ytr, Xva, yva, Xte, seed):
    clf = xgb.XGBClassifier(n_estimators=N_ROUNDS, early_stopping_rounds=EARLY, eval_metric='auc',
                            random_state=seed, **XGB_PARAMS)
    clf.fit(Xtr, ytr, eval_set=[(Xva, yva)], verbose=False)
    return clf.predict_proba(Xte)[:, 1], clf.best_iteration, clf

P = {(arm, h): np.full(len(ts), np.nan, np.float32) for arm in ARMS for h, _ in HORIZONS}
FIT_LOG, IMP = [], {}
t0 = time.time()
for m in TEST_MONTHS:
    (tlo, thi), (vlo, vhi), (a, b) = split(m)
    for h, _ in HORIZONS:
        y_all = LAB[h]['up'].astype(np.int8)
        te = np.arange(a, b)[LAB[h]['valid'][a:b]]
        for arm in ARMS:
            am = arm_mask(arm, h)
            tr = np.arange(tlo, thi)[am[tlo:thi]]
            va = np.arange(vlo, vhi)[am[vlo:vhi]]
            if len(tr) < 500 or len(va) < 100 or len(np.unique(y_all[va])) < 2:
                print(f'  {m} h{h} {arm}: skipped (train {len(tr)}, val {len(va)})'); continue
            preds, its = [], []
            for sd in SEEDS:
                p, it, clf = fit_predict(X[tr], y_all[tr], X[va], y_all[va], X[te], sd)
                preds.append(p); its.append(it)
            P[(arm, h)][te] = np.mean(preds, 0)
            if arm == PRIMARY_ARM and h == PRIMARY_H:
                IMP[m] = dict(zip(FEATS, clf.feature_importances_.astype(float)))
            FIT_LOG.append(dict(month=m, h=h, arm=arm, n_train=len(tr), n_val=len(va),
                                p_up_train=float(y_all[tr].mean()), best_iter=its))
            print(f'  {m} h{h:<2} {arm:4}: train {len(tr):>7,}  val {len(va):>6,}  best_iter {its}  '
                  f'({time.time() - t0:.0f}s)')
print(pd.DataFrame(FIT_LOG)[['month', 'h', 'arm', 'n_train', 'best_iter']].to_string(index=False))
"""

C_AUC = r"""# Cell 7 — stage-2 AUC on test stage-1 triggers that touched a barrier (+ all touched bars, info)
def auc_or_nan(y, p):
    return roc_auc_score(y, p) if len(np.unique(y)) == 2 and len(y) >= 20 else np.nan

def eval_auc(arm, h, rate):
    L, p = LAB[h], P[(arm, h)]
    base = L['touched'] & np.isfinite(p)
    out = {}
    for name, mask in (('trig', base & TRIG[rate]), ('all_touched', base)):
        per = {mo: auc_or_nan(L['up'][mask & (month == mo)], p[mask & (month == mo)]) for mo in TEST_MONTHS}
        out[name] = dict(pooled=auc_or_nan(L['up'][mask], p[mask]), n=int(mask.sum()), per_month=per)
    return out

AUC = {(arm, h, r): eval_auc(arm, h, r) for arm in ARMS for h, _ in HORIZONS for r in STAGE1_RATES}
for (arm, h, r), v in AUC.items():
    pm = [x for x in v['trig']['per_month'].values() if np.isfinite(x)]
    flag = '  <- PRIMARY' if (arm, h, r) == (PRIMARY_ARM, PRIMARY_H, PRIMARY_RATE) else ''
    print(f'{arm:4} h{h:<2} rate {r:.2f}: trig AUC {v["trig"]["pooled"]:.4f} (n {v["trig"]["n"]:>6,}, '
          f'months >= {AUC_KILL}: {sum(x >= AUC_KILL for x in pm)}/{len(pm)}, range '
          f'{min(pm, default=np.nan):.3f}..{max(pm, default=np.nan):.3f}) | all-touched AUC '
          f'{v["all_touched"]["pooled"]:.4f}{flag}')
prim_auc = AUC[(PRIMARY_ARM, PRIMARY_H, PRIMARY_RATE)]
print('\nprimary per-month:', {k: round(v, 4) for k, v in prim_auc['trig']['per_month'].items()})
imp = pd.DataFrame(IMP).mean(1).sort_values(ascending=False)
print('\nprimary-arm mean gain importance, top 15:\n' + imp.head(15).round(4).to_string())
"""

C_NULL = r"""# Cell 8 — K2: label-permutation refit null for the primary cell (this is where the GPU pays)
h, arm = PRIMARY_H, PRIMARY_ARM
L = LAB[h]; y_all = L['up'].astype(np.int8); am = arm_mask(arm, h)
null_pooled = []
t0 = time.time()
test_mask = L['touched'] & TRIG[PRIMARY_RATE]
for i in range(N_PERM):
    ys, ps = [], []
    for m in TEST_MONTHS:
        (tlo, thi), (vlo, vhi), (a, b) = split(m)
        tr = np.arange(tlo, thi)[am[tlo:thi]]; va = np.arange(vlo, vhi)[am[vlo:vhi]]
        te = np.arange(a, b)[test_mask[a:b]]
        if len(tr) < 500 or len(va) < 100 or len(te) < 20:
            continue
        ytr = RNG.permutation(y_all[tr]); yva = RNG.permutation(y_all[va])
        p, _, _ = fit_predict(X[tr], ytr, X[va], yva, X[te], 1000 + i)
        ys.append(y_all[te]); ps.append(p)
    null_pooled.append(auc_or_nan(np.concatenate(ys), np.concatenate(ps)))
    print(f'  perm {i + 1}/{N_PERM}: pooled AUC {null_pooled[-1]:.4f}  ({time.time() - t0:.0f}s)')
null_pooled = np.array(null_pooled)
NULL = dict(mean=float(np.nanmean(null_pooled)), p975=float(np.nanpercentile(null_pooled, 97.5)),
            values=null_pooled.tolist())
print(f'null pooled AUC: mean {NULL["mean"]:.4f}  p97.5 {NULL["p975"]:.4f}  | model {prim_auc["trig"]["pooled"]:.4f}')
"""

C_ECON = r"""# Cell 9 — economics: one position at a time on test stage-1 triggers, hold h bars, side = sign(p - 0.5)
def boot_day(vals, days):
    ud = np.unique(days)
    if len(ud) < 3:
        return (np.nan, np.nan)
    sums = np.array([vals[days == u].sum() for u in ud]); cnt = np.array([(days == u).sum() for u in ud])
    bi = RNG.integers(0, len(ud), (N_BOOT, len(ud)))
    mm = sums[bi].sum(1) / cnt[bi].sum(1)
    return (float(np.percentile(mm, 2.5)), float(np.percentile(mm, 97.5)))

def econ(arm, h, rate):
    L, p = LAB[h], P[(arm, h)]
    cand = np.flatnonzero(TRIG[rate] & L['valid'] & np.isfinite(p))
    take, busy = [], -1
    for t in cand:                                  # one position at a time, hold exactly h bars
        if t > busy:
            take.append(t); busy = t + h
    take = np.array(take, np.int64)
    if len(take) < 20:
        return dict(n=int(len(take)))
    side = np.where(p[take] >= 0.5, 1, -1)
    ret = L['ret'][take].astype(float)
    g_ = side * ret
    em = float(np.mean(np.abs(ret)))
    pu = float((ret > 0).mean())
    perm = np.array([(RNG.permutation(side) * ret).mean() for _ in range(2000)])
    out = dict(n=int(len(take)), days=int(len(np.unique(day[take]))), acc=float((g_ > 0).mean()),
               gross=float(g_.mean()), median=float(np.median(g_)), emove=em,
               p_up=pu, best_const_acc=max(pu, 1 - pu), always_long_gross=float(ret.mean()),
               long_n=int((side == 1).sum()), short_n=int((side == -1).sum()),
               long_gross=float(g_[side == 1].mean()) if (side == 1).any() else np.nan,
               short_gross=float(g_[side == -1].mean()) if (side == -1).any() else np.nan,
               signperm_pctile=float((perm < g_.mean()).mean() * 100),
               per_month={mo: float(g_[month[take] == mo].mean()) for mo in TEST_MONTHS if (month[take] == mo).any()})
    for k, fee in FEES.items():
        out[f'req_acc_{k}'] = (1 + fee / em) / 2
        out[f'net_{k}'] = out['gross'] - fee
        out[f'net_{k}_ci'] = boot_day(g_ - fee, day[take])
    return out

ECON = {(arm, h, r): econ(arm, h, r) for arm in ARMS for h, _ in HORIZONS for r in STAGE1_RATES}
cols = ['n', 'acc', 'best_const_acc', 'req_acc_mixed', 'gross', 'emove', 'net_mixed', 'long_gross',
        'short_gross', 'signperm_pctile']
tab = pd.DataFrame({f'{a} h{h} r{r}': {c: v.get(c, np.nan) for c in cols} for (a, h, r), v in ECON.items()}).T
print(tab.round(3).to_string())
"""

C_VERDICT = r"""# Cell 10 — pre-registered verdict (primary cell only)
pa, pe = prim_auc['trig'], ECON[(PRIMARY_ARM, PRIMARY_H, PRIMARY_RATE)]
pm = [x for x in pa['per_month'].values() if np.isfinite(x)]
K = {
    'K1 pooled AUC >= 0.65 and >= 75 % of months >= 0.65':
        pa['pooled'] >= AUC_KILL and len(pm) > 0 and np.mean([x >= AUC_KILL for x in pm]) >= MONTH_FRAC,
    'K2 pooled AUC > label-permutation null p97.5': pa['pooled'] > NULL['p975'],
    'K3 acc > max(best constant side, fee-required acc @7bp)':
        pe.get('n', 0) >= 20 and pe['acc'] > max(pe['best_const_acc'], pe['req_acc_mixed']),
    'K4 net @7bp > 0, day-bootstrap CI lo > 0':
        pe.get('n', 0) >= 20 and pe['net_mixed'] > 0 and pe['net_mixed_ci'][0] > 0,
}
print(f'PRIMARY: arm {PRIMARY_ARM}, h {PRIMARY_H} bars, stage-1 rate {PRIMARY_RATE}')
print(f'  pooled trig AUC {pa["pooled"]:.4f} (n {pa["n"]:,}) | null p97.5 {NULL["p975"]:.4f}')
if pe.get('n', 0) >= 20:
    print(f'  trades {pe["n"]} | acc {pe["acc"]:.3f} vs best-constant {pe["best_const_acc"]:.3f} and '
          f'required@7bp {pe["req_acc_mixed"]:.3f} | gross {pe["gross"]:+.2f} bp | net@7 {pe["net_mixed"]:+.2f} '
          f'CI {np.round(pe["net_mixed_ci"], 2)}')
for k, v in K.items():
    print(f'  {"PASS" if v else "FAIL"}  {k}')
PASSED = all(K.values())
print('VERDICT:', 'PASS — escalate to a realistic execution study' if PASSED else
      ('FAIL (K1) — direction is not learnable at tradeable strength from v1 features; close the two-stage idea'
       if not list(K.values())[0] else 'FAIL — see which criterion'))
"""

C_SAVE = r"""# Cell 11 — save artifacts
def _j(o):
    if isinstance(o, dict):
        return {str(k): _j(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_j(v) for v in o]
    if isinstance(o, (np.floating, np.integer, np.bool_)):
        return o.item()
    return o

res = dict(config=dict(HORIZONS=HORIZONS, STAGE1_RATES=STAGE1_RATES, PRIMARY_RATE=PRIMARY_RATE,
                       PRIMARY_ARM=PRIMARY_ARM, PRIMARY_H=PRIMARY_H, SEEDS=SEEDS, N_PERM=N_PERM,
                       XGB_PARAMS=XGB_PARAMS, INCLUDE_DEAD=INCLUDE_DEAD, TIME_FEATS=TIME_FEATS, SMOKE=SMOKE),
           features=FEATS, test_months=TEST_MONTHS, sanity=SAN.to_dict('records'), fits=FIT_LOG,
           auc={f'{a}|{h}|{r}': v for (a, h, r), v in AUC.items()},
           econ={f'{a}|{h}|{r}': v for (a, h, r), v in ECON.items()},
           null=NULL, verdict={k: bool(v) for k, v in K.items()}, passed=bool(PASSED),
           importance=imp.to_dict())
with open(os.path.join(OUT_DIR, 'v1_stage2_results.json'), 'w') as fh:
    json.dump(_j(res), fh, indent=1, default=float)
keep = np.isfinite(P[(PRIMARY_ARM, PRIMARY_H)]) | TRIG[PRIMARY_RATE]
np.savez_compressed(os.path.join(OUT_DIR, 'v1_stage2_scores.npz'), ts=ts[keep],
                    **{f'p_{a}_{h}': P[(a, h)][keep] for a in ARMS for h, _ in HORIZONS},
                    **{f'up_{h}': LAB[h]['up'][keep] for h, _ in HORIZONS},
                    **{f'touched_{h}': LAB[h]['touched'][keep] for h, _ in HORIZONS},
                    **{f'ret_{h}': LAB[h]['ret'][keep] for h, _ in HORIZONS},
                    **{f'trig_{r}': TRIG[r][keep] for r in STAGE1_RATES})
print('saved to', OUT_DIR, os.listdir(OUT_DIR))
"""

CELLS = [("md", MD0), ("code", C_SETUP), ("code", C_CONFIG), ("code", C_LOAD), ("code", C_FEAT),
         ("code", C_SANITY), ("code", C_WF), ("code", C_AUC), ("code", C_NULL), ("code", C_ECON),
         ("code", C_VERDICT), ("code", C_SAVE)]


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
    print("wrote", build())
