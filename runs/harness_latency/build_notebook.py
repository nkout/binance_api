"""Build runs/btc_latency_decay_probe.ipynb (R1, next_signal_ideas.md Round 2) — Colab, GPU xgboost.

    python build_notebook.py [out.ipynb]    (default ../btc_latency_decay_probe.ipynb; refuses to
                                             overwrite an executed notebook)
"""
import os, sys
import nbformat as nbf

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "btc_latency_decay_probe.ipynb")

MD0 = r"""# R1 — latency decay of the v1 stage-2 confident tail, measured on 5 s bars

**Question.** In `v1_stage2_probe.analysis.md` §3 the stage-2 model's most confident 1 % of triggers made
**+11.1 bp** gross at zero entry delay, **+3.4 bp** at 15 s, **+0.1 bp** at 30 s. The 15 s grid cannot
say what happens in between. Entry is only possible some hundreds of milliseconds to seconds after the
bar closes, and `harness_fees/fee_reprice.py` (R3) showed the reachable round trip is **9.0 bp**
(VIP0 + BNB, taker both legs). So: **how much of the +11 bp is left 5 s after the signal?**

**Design.**
- **`v1x` (primary):** the 1d design trained on the **whole v1 year** (15 s, arm `evt`, 90 s / ±20 bp),
  then applied to the **60-day 5 s set** on bars **after the v1 year ends** (2026-08-16 → 09-20) — fully
  out of sample, and a fresh replication of the tail. Features are the same 60, rebuilt on the 5 s grid
  with every window kept at the same **wall-clock** length (a "1-bar" 15 s quantity becomes a 3-bar
  rolling quantity), so the model sees the distribution it was trained on, sampled every 5 s.
- **`w5`, `mix` (exploratory):** walk-forward weekly on the 5 s set itself, and v1 year + 5 s past combined.
- Stage 1 unchanged: trailing 5-min mean |15 s return|, 7-day causal quantile, 5 %.
- Confident tail: |p − 0.5| above an **expanding causal quantile** of earlier out-of-sample days (≥ 3 days).
- One position at a time, hold 90 s (18 bars) after entry, entry delay **0 / 5 / 10 / 15 / 30 s**.
- Also the **accrual curve**: mean signed move from the signal close to +k·5 s on the zero-delay trades.

**Pre-registered verdict — primary `v1x`, top 1 % and top 2 %.** PASS iff, in either cell, gross at a
**5 s** delay has a day-bootstrap 95 % CI lower bound **> 9.0 bp** with n ≥ 100. Otherwise FAIL.
Information only: a linear 1 s estimate `g0 − (g0 − g5)/5`. If 5 s fails but that estimate ≥ 9 bp,
the notebook flags **EVENT-STREAM CHECK** (sub-second data is the only way to settle it).
Everything else is exploratory. **Power caveat:** ~32 out-of-sample days; expect n ≈ 100–300 per cell.

**How to run.** Upload `data/w5_60d.parquet` to `MyDrive/w5_60d.parquet` (the v1 file `v1_15s.parquet`
is already there). Colab → Runtime → GPU → Run all. Outputs: `MyDrive/btc_latency_decay_probe/`.
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

C_CONFIG = r"""# Cell 2 — config
SMOKE = os.environ.get('SMOKE') == '1'
V1_DATA = os.environ.get('V1_DATA', '/content/drive/MyDrive/v1_15s.parquet')
W5_DATA = os.environ.get('W5_DATA', '/content/drive/MyDrive/w5_60d.parquet')
OUT_DIR = os.environ.get('OUT_DIR', '/content/drive/MyDrive/btc_latency_decay_probe')
OOS_START = int(os.environ.get('OOS_START', pd.Timestamp('2026-08-16', tz='UTC').timestamp()))

H_SEC, THETA = 90, 20.0                    # label: first touch of +-20 bp within 90 s (as in 1d)
STAGE1_WIN_15 = 20                         # 5 min, in 15 s units
STAGE1_LOOKBACK_DAYS = 7
PRIMARY_RATE = 0.05
TAILS = [0.01, 0.02, 0.05, 0.10]
PRIMARY_TAILS = [0.01, 0.02]
DELAYS_SEC = [0, 5, 10, 15, 30]
PATH_SEC = 180                             # accrual curve length
MIN_TAIL_DAYS = 3                          # expanding causal threshold needs this many earlier days
VAL_DAYS_V1, VAL_DAYS_W5 = 10, 7
MIN_TRAIN_DAYS_W5 = 21
FEE_RT = {'VIP0 taker-taker': 10.0, 'VIP0+BNB taker-taker': 9.0, 'VIP0+BNB taker-in/maker-out': 6.3}
PASS_FEE = 9.0
SEEDS = [0] if SMOKE else [0, 1, 2]
N_ROUNDS = 200 if SMOKE else 3000
EARLY = 30 if SMOKE else 150
XGB_PARAMS = dict(max_depth=4, learning_rate=0.03, subsample=0.8, colsample_bytree=0.8,
                  min_child_weight=50, reg_lambda=5.0, tree_method='hist', device=DEVICE)
INCLUDE_DEAD, TIME_FEATS = False, False
N_BOOT = 500 if SMOKE else 4000
RNG = np.random.default_rng(0)
os.makedirs(OUT_DIR, exist_ok=True)
print(f'SMOKE={SMOKE} | OOS from {pd.to_datetime(OOS_START, unit="s", utc=True)} | out {OUT_DIR}')
"""

C_LOAD = r"""# Cell 3 — load both datasets onto complete grids (gaps -> NaN rows; nothing interpolated)
def load_grid(path, bar):
    raw = pd.read_parquet(path)
    ts_raw = raw['ts'].to_numpy(np.int64)
    t0 = int(ts_raw[0])
    slot = np.round((ts_raw - t0) / bar).astype(np.int64)
    keep = np.r_[slot[1:] != slot[:-1], True]
    raw, slot = raw[keep].reset_index(drop=True), slot[keep]
    G = int(slot[-1]) + 1
    g = pd.DataFrame(np.nan, index=np.arange(G), columns=[c for c in raw.columns if c != 'ts'], dtype=np.float32)
    g.iloc[slot] = raw.drop(columns='ts').to_numpy(np.float32)
    ts = t0 + np.arange(G, dtype=np.int64) * bar
    print(f'{os.path.basename(path)}: {len(raw):,} rows -> {G:,} slots of {bar}s, present {len(raw) / G * 100:.1f} % | '
          f'{pd.to_datetime(ts[0], unit="s")} -> {pd.to_datetime(ts[-1], unit="s")}')
    return g, ts

t0 = time.time()
g1, ts1 = load_grid(V1_DATA, 15)
g5, ts5 = load_grid(W5_DATA, 5)
print(f'{time.time() - t0:.0f}s')
"""

C_FEAT = r"""# Cell 4 — features at any bar width. Windows are written in 15 s units (identical to the 1d
# notebook at BAR=15) and converted to bars by _w(); a 1-bar 15 s quantity becomes a 3-bar rolling
# quantity at 5 s, so the model sees 15 s-equivalent features sampled every 5 s.
LV_F = ['0.0', '0.01', '0.05', '0.1', '0.2', '0.4']

def build_features(g, ts, BAR):
    k = 15 // BAR
    def _w(n15): return max(1, int(n15 * k))
    def _r(x, n, frac=0.8): return x.rolling(n, min_periods=max(1, int(frac * n)))
    def _z(x, n):
        m, s = _r(x, n, 0.5).mean(), _r(x, n, 0.5).std()
        return (x - m) / s
    f = {}
    fb, fa = g['future_bid_close'], g['future_ask_close']
    mid = (fb + fa) / 2
    lm = np.log(mid)
    r1 = (lm - lm.shift(_w(1))) * 1e4            # 15 s return
    ar = r1.abs()
    sig = _r(r1, _w(240), 0.75).std()
    for n in (20, 80, 240):
        f[f'rv_{n}'] = _r(ar, _w(n)).mean() / sig
    f['rv_ratio_20_240'] = _r(ar, _w(20)).mean() / _r(ar, _w(240)).mean()
    f['rv_ratio_240_5760'] = _r(ar, _w(240)).mean() / _r(ar, _w(5760), 0.5).mean()
    f['sig_bp'] = sig
    for n in (1, 4, 20, 80, 240, 960, 5760):
        f[f'ret_{n}'] = (lm - lm.shift(_w(n))) * 1e4 / (sig * np.sqrt(n))
    for n in (240, 960, 5760):
        lo, hi = _r(mid, _w(n), 0.5).min(), _r(mid, _w(n), 0.5).max()
        f[f'rpos_{n}'] = (mid - lo) / (hi - lo)
    hib, lob = _r(g['future_ask_max'], _w(1)).max(), _r(g['future_bid_min'], _w(1)).min()
    f['bar_range'] = (hib - lob) / mid * 1e4 / sig
    f['bar_pos'] = (mid - lob) / (hib - lob)
    bq, sq = g['future_buy_qty'], g['future_sell_qty']
    for n in (1, 4, 20, 80):
        b, s = _r(bq, _w(n)).sum(), _r(sq, _w(n)).sum()
        f[f'fimb_{n}'] = (b - s) / (b + s)
    bc, sc = g['future_buy_samples'], g['future_sell_samples']
    for n in (1, 20):
        b, s = _r(bc, _w(n)).sum(), _r(sc, _w(n)).sum()
        f[f'cimb_{n}'] = (b - s) / (b + s)
    sb, ss = g['spot_buy_qty'], g['spot_sell_qty']
    for n in (1, 20):
        b, s = _r(sb, _w(n)).sum(), _r(ss, _w(n)).sum()
        f[f'simb_{n}'] = (b - s) / (b + s)
    f['flow_div_20'] = f['fimb_20'] - f['simb_20']
    lv = np.log1p(_r(bq + sq, _w(1)).sum().astype(np.float32))
    f['vol_z'] = (_r(lv, _w(20)).mean() - _r(lv, _w(5760), 0.5).mean()) / _r(lv, _w(5760), 0.5).std()
    f['sflow_z'] = (_r(bq - sq, _w(20)).sum() / 20) / (_r(bq + sq, _w(5760), 0.5).mean() * k)
    vw = ((g['future_buy_vwap'] - g['future_sell_vwap']) / mid * 1e4).where((bq > 0) & (sq > 0))
    f['vwap_gap_4'] = vw.rolling(_w(4), min_periods=1).mean()
    for l in LV_F:
        b, a = g[f'future_bid_liq_{l}_median'], g[f'future_ask_liq_{l}_median']
        f[f'dimb_{l}'] = (b - a) / (b + a)
    for l in ('0.0', '0.05'):
        b, a = g[f'future_bid_liq_{l}_close'], g[f'future_ask_liq_{l}_close']
        f[f'dimb_{l}_close'] = (b - a) / (b + a)
    f['dimb_0.05_d4'] = f['dimb_0.05'] - f['dimb_0.05'].shift(_w(4))
    f['dimb_0.05_d20'] = f['dimb_0.05'] - f['dimb_0.05'].shift(_w(20))
    f['dimb_0.1_avg20'] = _r(f['dimb_0.1'], _w(20)).mean()
    dep = np.log(g['future_bid_liq_0.2_median'] + g['future_ask_liq_0.2_median'])
    f['depth_z'] = _z(dep, _w(5760))
    f['slope_asym'] = (np.log(g['future_bid_liq_0.4_median'] / g['future_bid_liq_0.05_median'])
                       - np.log(g['future_ask_liq_0.4_median'] / g['future_ask_liq_0.05_median']))
    for l in ('0.05', '0.2'):
        b, a = g[f'spot_bid_liq_{l}_median'], g[f'spot_ask_liq_{l}_median']
        f[f'sdimb_{l}'] = (b - a) / (b + a)
    spr = g['future_spread_median'] / mid * 1e4
    f['spread_bp'], f['spread_z'] = spr, _z(spr, _w(5760))
    f['spread_max_bp'] = _r(g['future_spread_max'], _w(1)).max() / mid * 1e4
    qb, qa = _r(g['future_bid_samples'], _w(1)).sum(), _r(g['future_ask_samples'], _w(1)).sum()
    f['quote_imb'] = (qb - qa) / (qb + qa)
    smid = (g['spot_bid_close'] + g['spot_ask_close']) / 2
    basis = (mid - smid) / smid * 1e4
    f['basis_bp'], f['basis_z_960'] = basis, _z(basis, _w(960))
    f['basis_d20'], f['basis_d240'] = basis - basis.shift(_w(20)), basis - basis.shift(_w(240))
    loi = np.log(g['opt_open_interest_sample'])
    for n in (20, 240, 960):
        f[f'oi_d{n}'] = (loi - loi.shift(_w(n))) * 1e4
    f['oi_z'] = _z(loi, _w(5760))
    lsr = g['opt_long_short_ratio_sample']
    f['lsr'], f['lsr_d240'], f['lsr_z'] = lsr, lsr - lsr.shift(_w(240)), _z(lsr, _w(5760))
    if INCLUDE_DEAD:
        f['funding_bp'] = g['opt_funding_rate_sample'] * 1e4
        f['premium_bp'] = (g['opt_mark_price_sample'] / g['opt_index_price_sample'] - 1) * 1e4
        lq, sq_ = g['opt_long_force_exit_qty_sum'], g['opt_short_force_exit_qty_sum']
        for n in (20, 240):
            a_, b_ = _r(lq, _w(n)).sum(), _r(sq_, _w(n)).sum()
            f[f'liq_log_{n}'] = np.log1p(a_ + b_)
            f[f'liq_imb_{n}'] = (a_ - b_) / (a_ + b_)
    if TIME_FEATS:
        hr = (ts % 86400) / 3600.0
        dw = ((ts // 86400) + 4) % 7
        f['hour_sin'], f['hour_cos'] = np.sin(2 * np.pi * hr / 24), np.cos(2 * np.pi * hr / 24)
        f['dow_sin'], f['dow_cos'] = np.sin(2 * np.pi * dw / 7), np.cos(2 * np.pi * dw / 7)
    F = pd.DataFrame(f, index=g.index).replace([np.inf, -np.inf], np.nan).astype(np.float32)
    s1 = _r(ar, _w(STAGE1_WIN_15)).mean().to_numpy()
    return F, lm.to_numpy(), s1

def first_touch(lm, h, theta_bp):
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

def stage1_triggers(score, day, rate, lookback, bars_day):
    trig = np.zeros(len(score), bool)
    ud = np.unique(day)
    lo = np.searchsorted(day, ud, 'left'); hi = np.searchsorted(day, ud, 'right')
    span = {d: (a, b) for d, a, b in zip(ud, lo, hi)}
    for d in ud:
        past = [score[span[x][0]:span[x][1]] for x in range(d - lookback, d) if x in span]
        if not past:
            continue
        past = np.concatenate(past); past = past[np.isfinite(past)]
        if len(past) < 0.5 * lookback * bars_day:
            continue
        a, b = span[d]
        s = score[a:b]
        trig[a:b] = np.isfinite(s) & (s >= np.quantile(past, 1 - rate))
    return trig

def prepare(g, ts, BAR):
    F, lm, s1 = build_features(g, ts, BAR)
    day = ts // 86400
    return dict(F=F, X=F.to_numpy(np.float32), lm=lm, ts=ts, day=day, bar=BAR,
                lab=first_touch(lm, H_SEC // BAR, THETA),
                trig=stage1_triggers(s1, day, PRIMARY_RATE, STAGE1_LOOKBACK_DAYS, 86400 // BAR))

t0 = time.time()
D1, D5 = prepare(g1, ts1, 15), prepare(g5, ts5, 5)
FEATS = list(D1['F'].columns)
assert FEATS == list(D5['F'].columns)
for nm, D in (('v1 15s', D1), ('w5 5s', D5)):
    L = D['lab']; v = L['valid']
    print(f'{nm}: touched {L["touched"][v].mean() * 100:.2f} % | stage-1 realised {D["trig"][v].mean() * 100:.2f} % | '
          f'P(touch|trig) {L["touched"][v & D["trig"]].mean() * 100:.1f} %')
print(f'{len(FEATS)} features | {time.time() - t0:.0f}s')
"""

C_XFER = r"""# Cell 5 — transfer check: on the ~33 days both collectors ran, do the 5 s-grid features match
# the 15 s-grid features at the same timestamps? (validity of applying the v1 model to 5 s bars)
m5 = pd.DataFrame({'ts': D5['ts']}); m5['i5'] = np.arange(len(m5))
m1 = pd.DataFrame({'ts': D1['ts']}); m1['i1'] = np.arange(len(m1))
mm = pd.merge_asof(m1, m5, on='ts', direction='nearest', tolerance=2)
mm = mm.dropna(subset=['i5'])
i1, i5 = mm['i1'].to_numpy(int), mm['i5'].to_numpy(int)
XC = []
for j, c in enumerate(FEATS):
    a, b = D1['X'][i1, j], D5['X'][i5, j]
    ok = np.isfinite(a) & np.isfinite(b)
    XC.append(dict(feature=c, n=int(ok.sum()), corr=float(np.corrcoef(a[ok], b[ok])[0, 1]) if ok.sum() > 1000 else np.nan,
                   ratio_sd=float(np.nanstd(b[ok]) / np.nanstd(a[ok])) if ok.sum() > 1000 else np.nan))
XC = pd.DataFrame(XC).sort_values('corr')
print(f'overlap bars matched: {len(mm):,}')
print(f'feature corr v1 vs w5 at the same ts: median {XC["corr"].median():.3f}, '
      f'{(XC["corr"] < 0.8).sum()} of {len(XC)} below 0.8')
print(XC.head(12).round(3).to_string(index=False))
"""

C_TRAIN = r"""# Cell 6 — train and score. v1x = whole v1 year; w5 / mix = weekly walk-forward on the 5 s set.
def fit(Xtr, ytr, Xva, yva, seed):
    clf = xgb.XGBClassifier(n_estimators=N_ROUNDS, early_stopping_rounds=EARLY, eval_metric='auc',
                            random_state=seed, **XGB_PARAMS)
    clf.fit(Xtr, ytr, eval_set=[(Xva, yva)], verbose=False)
    return clf

def evt_rows(D, lo, hi):
    return np.arange(lo, hi)[D['lab']['touched'][lo:hi]]

def ens_predict(Xtr, ytr, Xva, yva, Xte):
    ps, its = [], []
    for sd in SEEDS:
        c = fit(Xtr, ytr, Xva, yva, sd); ps.append(c.predict_proba(Xte)[:, 1]); its.append(c.best_iteration)
    return np.mean(ps, 0), its

G5 = len(D5['ts'])
P = {a: np.full(G5, np.nan, np.float32) for a in ('v1x', 'w5', 'mix')}
LOG = []
t0 = time.time()
y1, y5 = D1['lab']['up'].astype(np.int8), D5['lab']['up'].astype(np.int8)
bday1, bday5 = 86400 // 15, 86400 // 5
purge1, purge5 = 4 * H_SEC // 15, 4 * H_SEC // 5

# v1x: train on the whole v1 year (val = its last 10 days), score every 5 s bar after the v1 year
n1 = len(D1['ts']); va_lo = n1 - VAL_DAYS_V1 * bday1
tr = evt_rows(D1, 0, va_lo - purge1); va = evt_rows(D1, va_lo, n1)
te = np.flatnonzero((D5['ts'] >= max(OOS_START, D1['ts'][-1] + H_SEC)) & D5['lab']['valid'])
P['v1x'][te], its = ens_predict(D1['X'][tr], y1[tr], D1['X'][va], y1[va], D5['X'][te])
LOG.append(dict(arm='v1x', block='all OOS', n_train=len(tr), n_val=len(va), n_test=len(te), best_iter=its))
print(f'v1x: train {len(tr):,} val {len(va):,} -> scored {len(te):,} 5 s bars | best_iter {its} | {time.time() - t0:.0f}s')

# weekly blocks on the 5 s set
w_start = D5['ts'][0] + MIN_TRAIN_DAYS_W5 * 86400
blocks = []
b = int(np.searchsorted(D5['ts'], w_start))
while b < G5:
    e = min(G5, b + 7 * bday5); blocks.append((b, e)); b = e
if SMOKE:
    blocks = blocks[-2:]
for (a, e) in blocks:
    va_hi = a - purge5; va_lo = va_hi - VAL_DAYS_W5 * bday5
    tr5 = evt_rows(D5, 0, va_lo - purge5); va5 = evt_rows(D5, va_lo, va_hi)
    te5 = np.arange(a, e)[D5['lab']['valid'][a:e]]
    if len(tr5) < 500 or len(va5) < 100:
        print(f'  block {a}: skipped (train {len(tr5)}, val {len(va5)})'); continue
    P['w5'][te5], its = ens_predict(D5['X'][tr5], y5[tr5], D5['X'][va5], y5[va5], D5['X'][te5])
    LOG.append(dict(arm='w5', block=str(pd.to_datetime(D5['ts'][a], unit='s').date()), n_train=len(tr5),
                    n_val=len(va5), n_test=len(te5), best_iter=its))
    if D5['ts'][a] >= OOS_START:                  # mix only where the v1 year cannot overlap the test week
        tr1m = evt_rows(D1, 0, n1)
        Xtr = np.vstack([D1['X'][tr1m], D5['X'][tr5]]); ytr = np.r_[y1[tr1m], y5[tr5]]
        P['mix'][te5], its_m = ens_predict(Xtr, ytr, D5['X'][va5], y5[va5], D5['X'][te5])
        LOG.append(dict(arm='mix', block=str(pd.to_datetime(D5['ts'][a], unit='s').date()), n_train=len(ytr),
                        n_val=len(va5), n_test=len(te5), best_iter=its_m))
    print(f'  block {pd.to_datetime(D5["ts"][a], unit="s").date()}: w5 train {len(tr5):,} | {time.time() - t0:.0f}s')
print(pd.DataFrame(LOG).to_string(index=False))
"""

C_AUC = r"""# Cell 7 — sanity: stage-2 AUC on out-of-sample 5 s stage-1 triggers that touched a barrier
OOS = D5['ts'] >= OOS_START
AUC = {}
for arm in P:
    for name, mask in (('OOS', OOS), ('all scored', np.ones(G5, bool))):
        m = D5['trig'] & D5['lab']['touched'] & np.isfinite(P[arm]) & mask
        y = D5['lab']['up'][m]
        AUC[f'{arm}|{name}'] = dict(n=int(m.sum()), auc=float(roc_auc_score(y, P[arm][m])) if len(np.unique(y)) == 2 else np.nan)
print(pd.DataFrame(AUC).T.to_string())
print('(1d reference on 15 s, 9 months: 0.584)')
"""

C_DECAY = r"""# Cell 8 — the decay: confident tail x entry delay, one position at a time, hold 90 s after entry
BAR5 = 5; HB = H_SEC // BAR5
lm5, day5 = D5['lm'], D5['day']

def boot(v, days):
    ud = np.unique(days)
    s = np.array([v[days == u].sum() for u in ud]); c = np.array([(days == u).sum() for u in ud])
    bi = RNG.integers(0, len(ud), (N_BOOT, len(ud)))
    mm = s[bi].sum(1) / c[bi].sum(1)
    return [float(np.percentile(mm, 2.5)), float(np.percentile(mm, 97.5))]

def tail_candidates(arm, q):
    # expanding causal threshold: quantile of |p-0.5| over candidate bars on EARLIER scored days
    p = P[arm]; cand = D5['trig'] & np.isfinite(p) & OOS
    conf = np.abs(p - 0.5)
    days = np.unique(day5[cand]); sel = np.zeros(G5, bool)
    for i, d in enumerate(days):
        if i < MIN_TAIL_DAYS:
            continue
        past = conf[cand & (day5 < d)]
        th = np.quantile(past, 1 - q)
        cur = cand & (day5 == d)
        sel[cur] = conf[cur] >= th
    return np.flatnonzero(sel)

def trades(arm, q, dsec):
    d = dsec // BAR5
    take, busy = [], -1
    for t in tail_candidates(arm, q):
        if t > busy and t + d + HB < G5 and np.isfinite(lm5[t:t + d + HB + 1]).all():
            take.append(t); busy = t + d + HB
    take = np.array(take, np.int64)
    side = np.where(P[arm][take] >= 0.5, 1, -1) if len(take) else np.array([], int)
    ret = (lm5[take + d + HB] - lm5[take + d]) * 1e4 if len(take) else np.array([])
    return take, side, ret

ROWS = []
for arm in P:
    for q in TAILS:
        for dsec in DELAYS_SEC:
            take, side, ret = trades(arm, q, dsec)
            if len(take) < 20:
                ROWS.append(dict(arm=arm, tail=q, delay=dsec, n=len(take))); continue
            gtr = side * ret; ci = boot(gtr, day5[take])
            row = dict(arm=arm, tail=q, delay=dsec, n=len(take), days=int(len(np.unique(day5[take]))),
                       acc=float((gtr > 0).mean()), gross=float(gtr.mean()), gross_ci=ci,
                       long=float(gtr[side == 1].mean()) if (side == 1).any() else np.nan,
                       short=float(gtr[side == -1].mean()) if (side == -1).any() else np.nan,
                       blind=float(ret.mean()))
            for k_, f_ in FEE_RT.items():
                row[f'net {k_}'] = row['gross'] - f_
            ROWS.append(row)
DEC = pd.DataFrame(ROWS)
show = DEC[['arm', 'tail', 'delay', 'n', 'days', 'acc', 'gross', 'gross_ci', 'long', 'short', 'blind']].copy()
show['gross_ci'] = show['gross_ci'].apply(lambda c: f'[{c[0]:+.2f},{c[1]:+.2f}]' if isinstance(c, list) else '')
print(show.round(3).to_string(index=False))
"""

C_PATH = r"""# Cell 9 — accrual curve: where does the edge build up after the signal? (zero-delay trades)
KP = PATH_SEC // BAR5
PATHS = {}
for arm in P:
    for q in PRIMARY_TAILS:
        take, side, _ = trades(arm, q, 0)
        take = take[take + KP < G5]; side = np.where(P[arm][take] >= 0.5, 1, -1)
        if len(take) < 20:
            continue
        mat = np.stack([(lm5[take + k] - lm5[take]) * 1e4 for k in range(KP + 1)], 1) * side[:, None]
        PATHS[f'{arm}|{q}'] = np.nanmean(mat, 0)
secs = [0, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180]
tab = pd.DataFrame({k: {f'+{s}s': v[s // BAR5] for s in secs} for k, v in PATHS.items()}).T
print('mean signed move since the signal close, bp:')
print(tab.round(2).to_string())
"""

C_VERDICT = r"""# Cell 10 — pre-registered verdict (primary arm v1x, tails top 1 % / top 2 %)
V = {}
for q in PRIMARY_TAILS:
    r0 = DEC[(DEC.arm == 'v1x') & (DEC.tail == q) & (DEC.delay == 0)]
    r5 = DEC[(DEC.arm == 'v1x') & (DEC.tail == q) & (DEC.delay == 5)]
    if r0.empty or r5.empty or r5['n'].iloc[0] < 20 or 'gross' not in r5 or pd.isna(r5['gross'].iloc[0]):
        V[q] = dict(pass_=False, note='too few trades'); continue
    g0, g5, n5, ci5 = r0['gross'].iloc[0], r5['gross'].iloc[0], int(r5['n'].iloc[0]), r5['gross_ci'].iloc[0]
    est1 = g0 - (g0 - g5) / 5
    V[q] = dict(g0=float(g0), g5=float(g5), n5=n5, ci5=ci5, est_1s=float(est1),
                pass_=bool(n5 >= 100 and ci5[0] > PASS_FEE), flag_event=bool(ci5[0] <= PASS_FEE and est1 >= PASS_FEE))
for q, v in V.items():
    if 'g0' in v:
        print(f'v1x top {q:.0%}: gross 0 s {v["g0"]:+.2f} | 5 s {v["g5"]:+.2f} CI [{v["ci5"][0]:+.2f},{v["ci5"][1]:+.2f}] '
              f'n {v["n5"]} | linear 1 s estimate {v["est_1s"]:+.2f} | {"PASS" if v["pass_"] else "FAIL"}'
              f'{"  -> EVENT-STREAM CHECK" if v["flag_event"] else ""}')
    else:
        print(f'v1x top {q:.0%}: {v["note"]}')
PASSED = any(v['pass_'] for v in V.values())
FLAG = any(v.get('flag_event') for v in V.values())
print('VERDICT:', 'PASS — the tail survives a 5 s delay at the reachable fee; build an execution study' if PASSED
      else ('FAIL at 5 s, but the 1 s estimate clears 9 bp — only sub-second event data can settle it' if FLAG
            else 'FAIL — the confident tail does not survive realistic entry latency at reachable fees'))
"""

C_SAVE = r"""# Cell 11 — save
def _j(o):
    if isinstance(o, dict): return {str(k): _j(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)): return [_j(v) for v in o]
    if isinstance(o, np.ndarray): return o.tolist()
    if isinstance(o, (np.floating, np.integer, np.bool_)): return o.item()
    return o
res = dict(config=dict(OOS_START=OOS_START, H_SEC=H_SEC, THETA=THETA, TAILS=TAILS, DELAYS_SEC=DELAYS_SEC,
                       FEE_RT=FEE_RT, PASS_FEE=PASS_FEE, SEEDS=SEEDS, SMOKE=SMOKE),
           transfer=XC.to_dict('records'), fits=LOG, auc=AUC, decay=DEC.to_dict('records'),
           paths={k: v.tolist() for k, v in PATHS.items()}, verdict=V, passed=PASSED, event_flag=FLAG)
json.dump(_j(res), open(os.path.join(OUT_DIR, 'latency_decay_results.json'), 'w'), indent=1, default=float)
keep = np.isfinite(P['v1x']) | np.isfinite(P['w5'])
np.savez_compressed(os.path.join(OUT_DIR, 'latency_decay_scores.npz'), ts=D5['ts'][keep],
                    **{f'p_{a}': P[a][keep] for a in P}, trig=D5['trig'][keep],
                    up=D5['lab']['up'][keep], touched=D5['lab']['touched'][keep], lm=D5['lm'][keep])
print('saved to', OUT_DIR, os.listdir(OUT_DIR))
"""

CELLS = [("md", MD0), ("code", C_SETUP), ("code", C_CONFIG), ("code", C_LOAD), ("code", C_FEAT),
         ("code", C_XFER), ("code", C_TRAIN), ("code", C_AUC), ("code", C_DECAY), ("code", C_PATH),
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
    print("wrote", build(sys.argv[1] if len(sys.argv) > 1 else OUT))
