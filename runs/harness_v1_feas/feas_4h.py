"""Stage 2 — 4-hour horizon feasibility check on the v1 year.

The question (horizon_economics_and_next_ideas.md §7, v1_year_data_audit.md §4):
is there ANY directional signal at a 4h horizon in slow features -- OI, long/short
ratio, basis, funding, liquidations, book shape -- over 331 days?

Pre-registered read:
  A  daily-IC t >= 3.0 and IC > 0 in >= 3/4 folds
  B  accuracy on the selected set >= max(fee requirement, P(up) on the same set)
  C  net at 7bp > 0 with a day-clustered CI excluding 0

Two arms, because funding and liquidations die on 2026-04-24:
  ARM A  331 days, no funding/liquidation features
  ARM B  216 days, everything
"""
import os, sys, numpy as np, pandas as pd
from scipy.stats import spearmanr

DATA = "/home/nkout/projects/binance_api/data/v1_year/v1_5min.pkl"
BARS_H, H_FWD = 12, 48          # 12 bars = 1h ; 48 bars = 4h horizon
N_FOLDS, TEST_FRAC = 4, 0.60
GATE_T = 3.0
FEES = [('maker', 4.0), ('mixed', 7.0), ('taker', 10.0)]
LEVELS = ['0.0', '0.01', '0.05', '0.1', '0.2', '0.4']


def zs(s, win):
    return (s - s.rolling(win, min_periods=win // 4).mean()) / \
           (s.rolling(win, min_periods=win // 4).std() + 1e-12)


def build(df):
    d = pd.DataFrame(index=df.index)
    mid = (df['future_bid_close'] + df['future_ask_close']) / 2
    smid = (df['spot_bid_close'] + df['spot_ask_close']) / 2
    lm = np.log(mid)
    d['_mid'] = mid
    d['_lm'] = lm

    r1 = lm.diff()
    rv1h, rv4h, rv24h = (r1.rolling(w, min_periods=w // 2).std()
                         for w in (BARS_H, 4 * BARS_H, 24 * BARS_H))
    d['vol_ratio_1h_24h'] = rv1h / (rv24h + 1e-12)
    d['vol_ratio_4h_24h'] = rv4h / (rv24h + 1e-12)
    d['rv24h_z'] = zs(rv24h, 24 * BARS_H * 7)

    for h, nm in [(BARS_H, '1h'), (4 * BARS_H, '4h'), (24 * BARS_H, '24h')]:
        d[f'ret_norm_{nm}'] = lm.diff(h) / (rv24h * np.sqrt(h) + 1e-12)
    hi = mid.rolling(24 * BARS_H, min_periods=BARS_H).max()
    lo = mid.rolling(24 * BARS_H, min_periods=BARS_H).min()
    d['range_pos_24h'] = (mid - lo) / (hi - lo + 1e-12)

    basis = (mid - smid) / smid * 1e4
    d['basis_bp'] = basis.clip(-100, 100)
    d['basis_z'] = zs(basis, 24 * BARS_H).clip(-8, 8)

    oi = df['opt_open_interest_sample']
    d['oi_z'] = zs(oi, 24 * BARS_H).clip(-8, 8)
    for h, nm in [(BARS_H, '1h'), (4 * BARS_H, '4h'), (24 * BARS_H, '24h')]:
        d[f'oi_chg_{nm}'] = (oi.pct_change(h) * 100).clip(-20, 20)
    # OI rising into a rally = new longs ; OI rising into a selloff = new shorts
    d['oi_ret_interact'] = d['oi_chg_4h'] * np.sign(d['ret_norm_4h'])

    lsr = df['opt_long_short_ratio_sample']
    d['lsr_z'] = zs(lsr, 24 * BARS_H).clip(-8, 8)
    d['lsr_chg_4h'] = lsr.pct_change(4 * BARS_H).clip(-1, 1) * 100

    bid_near = sum(df[f'future_bid_liq_{l}_median'] for l in LEVELS[:2])
    ask_near = sum(df[f'future_ask_liq_{l}_median'] for l in LEVELS[:2])
    bid_far = sum(df[f'future_bid_liq_{l}_median'] for l in LEVELS[-2:])
    ask_far = sum(df[f'future_ask_liq_{l}_median'] for l in LEVELS[-2:])
    d['depth_imb_near'] = (bid_near - ask_near) / (bid_near + ask_near + 1e-12)
    d['depth_imb_far'] = (bid_far - ask_far) / (bid_far + ask_far + 1e-12)
    d['depth_slope'] = np.log((bid_near + ask_near + 1e-9) / (bid_far + ask_far + 1e-9))
    d['depth_total_z'] = zs(np.log(bid_near + ask_near + bid_far + ask_far + 1e-9), 24 * BARS_H).clip(-8, 8)
    d['spread_z'] = zs(df['future_spread_median'], 24 * BARS_H).clip(-8, 8)

    fb, fs = df['future_buy_qty'], df['future_sell_qty']
    d['vol_imb_1h'] = ((fb - fs).rolling(BARS_H).sum() /
                       ((fb + fs).rolling(BARS_H).sum() + 1e-12))
    d['vol_imb_4h'] = ((fb - fs).rolling(4 * BARS_H).sum() /
                       ((fb + fs).rolling(4 * BARS_H).sum() + 1e-12))
    d['vol_z'] = zs(np.log(fb + fs + 1e-9), 24 * BARS_H).clip(-8, 8)
    d['spot_fut_vol_ratio'] = np.log((df['spot_buy_qty'] + df['spot_sell_qty'] + 1e-9) /
                                     (fb + fs + 1e-9))

    hod = df['dt'].dt.hour + df['dt'].dt.minute / 60
    dow = df['dt'].dt.dayofweek
    d['hour_sin'], d['hour_cos'] = np.sin(2*np.pi*hod/24), np.cos(2*np.pi*hod/24)
    d['dow_sin'], d['dow_cos'] = np.sin(2*np.pi*dow/7), np.cos(2*np.pi*dow/7)

    # --- arm-B only: funding + liquidations ---
    fr = df['opt_funding_rate_sample']
    d['funding_bp'] = (fr * 1e4).clip(-20, 20)
    d['funding_z'] = zs(fr, 24 * BARS_H * 3).clip(-8, 8)
    ll = df['opt_long_force_exit_qty_sum'].rolling(BARS_H, min_periods=1).sum()
    ls = df['opt_short_force_exit_qty_sum'].rolling(BARS_H, min_periods=1).sum()
    d['liq_long_z'] = zs(np.log1p(ll), 24 * BARS_H).clip(-8, 8)
    d['liq_short_z'] = zs(np.log1p(ls), 24 * BARS_H).clip(-8, 8)
    d['liq_imb'] = (ll - ls) / (ll + ls + 1e-9)

    # --- targets ---
    fwd = lm.shift(-H_FWD) - lm
    vol4 = lm.diff(H_FWD).rolling(24 * BARS_H * 7, min_periods=24 * BARS_H).std()
    d['y_norm'] = (fwd / (vol4 + 1e-12)).clip(-5, 5)
    d['y_bp'] = fwd * 1e4
    # a target is valid only if the bar 48 ahead is exactly 4h ahead (no collector gap)
    ts = df.index.values
    ok = np.zeros(len(df), bool)
    ok[:len(df) - H_FWD] = (ts[H_FWD:] - ts[:len(df) - H_FWD]) == H_FWD * 300
    d['_valid'] = ok & np.isfinite(d['y_norm']) & np.isfinite(d['y_bp'])
    d['_day'] = (df.index.values // 86400)
    return d


ARM_B_ONLY = ['funding_bp', 'funding_z', 'liq_long_z', 'liq_short_z', 'liq_imb']


def daily_ic_t(pred, y, day, min_n=40):
    ics = []
    for u in np.unique(day):
        m = day == u
        if m.sum() < min_n:
            continue
        r = spearmanr(pred[m], y[m]).statistic
        if np.isfinite(r):
            ics.append(r)
    ics = np.asarray(ics)
    if len(ics) < 10:
        return np.nan, np.nan, len(ics)
    return ics.mean() / (ics.std(ddof=1) / np.sqrt(len(ics))), ics.mean(), len(ics)


def day_ci(v, day, nb=3000, seed=0):
    rng = np.random.default_rng(seed)
    ud = np.unique(day); idx = {d: np.where(day == d)[0] for d in ud}
    out = np.array([v[np.concatenate([idx[d] for d in rng.choice(ud, len(ud), True)])].mean()
                    for _ in range(nb)])
    return tuple(np.percentile(out, [2.5, 97.5]))


def run_arm(d, feats, tag):
    print("\n" + "=" * 92)
    print(f"ARM {tag}: {len(feats)} features")
    print("=" * 92)
    sub = d[d['_valid'] & np.isfinite(d[feats]).all(axis=1)].copy()
    if len(sub) < 5000:
        print(f"  only {len(sub):,} usable rows -- skipping"); return
    days = sub['_day'].values
    span = (sub.index.max() - sub.index.min()) / 86400
    print(f"  rows {len(sub):,}  days {len(np.unique(days))}  span {span:.0f}d  "
          f"({pd.Timestamp(sub.index.min(), unit='s'):%Y-%m-%d} -> "
          f"{pd.Timestamp(sub.index.max(), unit='s'):%Y-%m-%d})")

    y, ybp = sub['y_norm'].values, sub['y_bp'].values
    print(f"\n  --- univariate daily-IC t (vs 4h vol-normalised return) ---")
    rows = []
    for f in feats:
        t, m, nd = daily_ic_t(sub[f].values, y, days)
        rows.append((f, t, m, nd))
    rows.sort(key=lambda r: -abs(r[1]) if np.isfinite(r[1]) else 0)
    print(f"  {'feature':<22} {'daily-IC t':>11} {'mean IC':>9} {'days':>5}")
    for f, t, m, nd in rows[:14]:
        star = ' *' if abs(t) >= GATE_T else ''
        print(f"  {f:<22} {t:>+11.2f} {m:>+9.4f} {nd:>5}{star}")
    n_sig = sum(1 for _, t, _, _ in rows if np.isfinite(t) and abs(t) >= GATE_T)
    print(f"  -> {n_sig}/{len(feats)} features with |daily-IC t| >= {GATE_T}")

    # walk-forward GBM, purged
    try:
        import xgboost as xgb
    except ImportError:
        print("\n  xgboost not importable -- univariate read only"); return
    n = len(sub)
    start = int(n * (1 - TEST_FRAC)); chunk = (n - start) // N_FOLDS
    pred = np.full(n, np.nan)
    print(f"\n  --- walk-forward GBM ({N_FOLDS} folds, purge {H_FWD} bars = 4h) ---")
    for k in range(N_FOLDS):
        lo = start + k * chunk
        hi = n if k == N_FOLDS - 1 else lo + chunk
        tr_hi = lo - H_FWD
        va_lo = int(tr_hi * 0.88)
        Xtr, ytr = sub[feats].values[:va_lo - H_FWD], y[:va_lo - H_FWD]
        Xva, yva = sub[feats].values[va_lo:tr_hi], y[va_lo:tr_hi]
        Xte = sub[feats].values[lo:hi]
        m = xgb.XGBRegressor(n_estimators=2000, max_depth=4, learning_rate=0.03,
                             subsample=0.8, colsample_bytree=0.6, min_child_weight=100,
                             reg_lambda=3.0, tree_method='hist', device='cpu',
                             early_stopping_rounds=50, eval_metric='rmse', random_state=0)
        m.fit(Xtr, ytr, eval_set=[(Xva, yva)], verbose=False)
        pred[lo:hi] = m.predict(Xte)
        print(f"    fold {k}  train {len(Xtr):>7,}  val {len(Xva):>6,}  test {hi-lo:>6,}  "
              f"best_iter {m.best_iteration}")

    te = np.isfinite(pred)
    ic = spearmanr(pred[te], y[te]).statistic
    t, mic, nd = daily_ic_t(pred[te], y[te], days[te])
    per = []
    for k in range(N_FOLDS):
        lo = start + k * chunk; hi = n if k == N_FOLDS - 1 else lo + chunk
        per.append(spearmanr(pred[lo:hi], y[lo:hi]).statistic)
    print(f"\n  pooled IC {ic:+.4f}   daily-IC t {t:+.2f} ({nd} days)   "
          f"per-fold " + " ".join(f"{v:+.4f}" for v in per) +
          f"   folds>0 {sum(1 for v in per if v > 0)}/{N_FOLDS}")
    A = (np.isfinite(t) and t >= GATE_T) and sum(1 for v in per if v > 0) >= N_FOLDS - 1
    print(f"  CRITERION A: {'PASS' if A else 'fail'}")

    # economics, with the drift control
    print(f"\n  --- economics (blind = always-long on the same bars) ---")
    print(f"  {'select':<16} {'n':>7} {'E|mv|':>7} {'acc':>7} {'gross':>8} {'blind':>8} "
          f"{'P(up)':>7} {'excess':>8} {'req(mk)':>8}  {'95% CI excess':>22}")
    vol = sub['vol_ratio_1h_24h'].values
    for nm, sel in [('all bars', te),
                    ('pred top 10%', te & (pred >= np.nanquantile(pred[te], 0.90))),
                    ('pred top 1%', te & (pred >= np.nanquantile(pred[te], 0.99))),
                    ('|pred| top 10%', te & (np.abs(pred) >= np.nanquantile(np.abs(pred[te]), 0.90)))]:
        if sel.sum() < 50:
            continue
        rb = ybp[sel]; sg = np.sign(pred[sel])
        g = (sg * rb).mean(); blind = rb.mean(); em = np.abs(rb).mean()
        acc = (g / em + 1) / 2; pup = (rb > 0).mean()
        req = (1 + 4.0 / em) / 2
        lo_, hi_ = day_ci(sg * rb - blind, days[sel])
        print(f"  {nm:<16} {sel.sum():>7,} {em:>7.1f} {acc:>7.3f} {g:>+8.2f} {blind:>+8.2f} "
              f"{pup:>7.3f} {g-blind:>+8.2f} {req:>8.3f}  [{lo_:>+8.2f},{hi_:>+8.2f}]")
    print(f"\n  (criterion B needs acc >= max(req(mk), P(up)); C needs net@7bp > 0 with CI > 0)")


def main():
    df = pd.read_pickle(DATA)
    df.index = df.index.astype(np.int64)
    print(f"loaded {len(df):,} 5-min bars")
    d = build(df)
    feats_all = [c for c in d.columns if not c.startswith('_') and not c.startswith('y_')]
    feats_a = [f for f in feats_all if f not in ARM_B_ONLY]
    run_arm(d, feats_a, "A (331d, no funding/liq)")
    dB = d[np.isfinite(d[ARM_B_ONLY]).all(axis=1)]
    run_arm(dB, feats_all, "B (216d, all features)")


if __name__ == "__main__":
    main()
