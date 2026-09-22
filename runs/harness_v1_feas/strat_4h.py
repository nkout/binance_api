"""§7 of v1_4h_feasibility.analysis.md — implement the four ideas and test whether
they close the 3-5x gap.

  1 better entry inside the episode   2 exit on signal decay   3 maker fees   4 sizing

Honest accounting throughout:
  * THREE-way time split. Feature IC signs come from SIGN region, every rule/threshold
    is chosen on VAL, and TEST is evaluated ONCE with the frozen config.
  * event-driven, ONE position at a time, real path-dependent exits.
  * P&L per TRADE, never per overlapping signal (the 9.4x trap of section 3).
"""
import sys, warnings, numpy as np, pandas as pd
warnings.filterwarnings('ignore')
sys.path.insert(0, '/home/nkout/projects/binance_api/runs/harness_v1_feas')
from feas_4h import build, ARM_B_ONLY, daily_ic_t

DATA = "/home/nkout/projects/binance_api/data/v1_year/v1_5min.pkl"
BAR = 300
H4 = 48                      # 4 h in bars
SIGN_END, VAL_END = 0.40, 0.60
FEES = {'maker': 4.0, 'mixed': 7.0, 'taker': 10.0}
MIN_TRADES = 40


# ───────────────────────────────────────────────── engine
def backtest(score, mid, ts, thr, entry='first', exit_mode='time', max_hold=H4,
             tp=None, sl=None, decay=0.5, wait_k=1, size=False, fee=0.0):
    """One position at a time. Returns a list of (entry_idx, exit_idx, bp_net, w)."""
    n = len(score)
    out = []
    i, pos, ei, epx, edir, ew, run = 1, 0, 0, 0.0, 0, 1.0, 0
    while i < n:
        if pos == 0:
            a = abs(score[i])
            hot = a >= thr
            run = run + 1 if hot else 0
            ok = hot
            if ok and entry == 'confirm':
                ok = a < abs(score[i-1])                      # extension has stopped
            elif ok and entry == 'wait_k':
                ok = run >= wait_k
            elif ok and entry == 'tick':
                d = 1 if score[i] > 0 else -1
                ok = d * (mid[i] - mid[i-1]) > 0              # price already turning
            if ok:
                pos = 1 if score[i] > 0 else -1
                ei, epx, edir = i, mid[i], (1 if score[i] > 0 else -1)
                ew = min(a / thr, 3.0) if size else 1.0
                run = 0
        else:
            held = i - ei
            r = edir * (mid[i] / epx - 1.0) * 1e4
            gap = (ts[i] - ts[ei]) != held * BAR              # collector gap -> bail out
            hit = False
            if gap or held >= max_hold:
                hit = True
            elif tp is not None and r >= tp:
                hit = True
            elif sl is not None and r <= -sl:
                hit = True
            elif exit_mode == 'decay' and abs(score[i]) < decay * thr:
                hit = True
            elif exit_mode == 'flip' and np.sign(score[i]) == -edir and abs(score[i]) >= decay * thr:
                hit = True
            if hit:
                out.append((ei, i, r - fee, ew))
                pos = 0
        i += 1
    return out


def stats(tr, days, fee_name='mixed', nb=2000, seed=0):
    if len(tr) < 3:
        return None
    r = np.array([t[2] for t in tr]); w = np.array([t[3] for t in tr])
    di = np.array([days[t[0]] for t in tr])
    wm = float((r * w).sum() / w.sum())
    rng = np.random.default_rng(seed); ud = np.unique(di)
    idx = {d: np.where(di == d)[0] for d in ud}
    bs = np.empty(nb)
    for k in range(nb):
        s = np.concatenate([idx[d] for d in rng.choice(ud, len(ud), True)])
        bs[k] = (r[s] * w[s]).sum() / w[s].sum()
    lo, hi = np.percentile(bs, [2.5, 97.5])
    return dict(n=len(tr), mean=wm, win=float((r > 0).mean()), lo=lo, hi=hi,
                days=len(ud), total=float((r * w).sum()),
                hold=float(np.mean([t[1] - t[0] for t in tr])))


def main():
    df = pd.read_pickle(DATA); df.index = df.index.astype(np.int64)
    d = build(df)
    feats = [c for c in d.columns if not c.startswith('_') and not c.startswith('y_')
             and c not in ARM_B_ONLY]
    sub = d[np.isfinite(d[feats]).all(axis=1) & np.isfinite(d['_mid'])].copy()
    n = len(sub)
    a, b = int(n * SIGN_END), int(n * VAL_END)
    mid = sub['_mid'].values.astype(float)
    ts = sub.index.values.astype(np.int64)
    days = sub['_day'].values
    y = sub['y_norm'].values
    print(f"rows {n:,}   SIGN [0:{a:,}]  VAL [{a:,}:{b:,}]  TEST [{b:,}:{n:,}]")
    for nm, s_, e_ in [('SIGN', 0, a), ('VAL', a, b), ('TEST', b, n)]:
        print(f"  {nm:<5} {pd.Timestamp(ts[s_],unit='s'):%Y-%m-%d} -> "
              f"{pd.Timestamp(ts[e_-1],unit='s'):%Y-%m-%d}  ({(ts[e_-1]-ts[s_])/86400:.0f}d)")

    # combo score: IC signs from the SIGN region only
    val_y = np.isfinite(y)
    rows = []
    for f in feats:
        t, m, _ = daily_ic_t(sub[f].values[:a][val_y[:a]], y[:a][val_y[:a]], days[:a][val_y[:a]])
        if np.isfinite(t): rows.append((f, t, m))
    rows.sort(key=lambda r: -abs(r[1]))
    top = rows[:3]
    print("\ncombo (signs frozen from SIGN region): " +
          ", ".join(f"{f}({m:+.2f})" for f, t, m in top))
    sc = np.zeros(n)
    for f, t, m in top:
        v = pd.Series(sub[f].values)
        z = ((v - v.rolling(2016, min_periods=288).mean()) /
             (v.rolling(2016, min_periods=288).std() + 1e-12)).values
        sc += np.nan_to_num(np.sign(m) * np.clip(z, -5, 5))

    THR = {q: float(np.nanquantile(np.abs(sc[a:b]), q)) for q in (0.80, 0.90, 0.95, 0.99)}
    print("thresholds from VAL |score| quantiles: " +
          "  ".join(f"q{int(q*100)}={v:.2f}" for q, v in THR.items()))

    # ── grid on VAL ────────────────────────────────────────────────────────
    grid = []
    for q, thr in THR.items():
        for entry in ('first', 'confirm', 'wait_k', 'tick'):
            for exit_mode, mh, tp, sl in (('time', H4, None, None),
                                          ('time', 96, None, None),
                                          ('decay', H4, None, None),
                                          ('decay', 96, None, None),
                                          ('flip', 96, None, None),
                                          ('time', H4, 40.0, 40.0),
                                          ('decay', 96, 60.0, 40.0)):
                for size in (False, True):
                    grid.append(dict(q=q, thr=thr, entry=entry, exit_mode=exit_mode,
                                     max_hold=mh, tp=tp, sl=sl, size=size,
                                     wait_k=3 if entry == 'wait_k' else 1))
    print(f"\nVAL grid: {len(grid)} configs (selection happens HERE, not on TEST)")
    res = []
    for g in grid:
        tr = backtest(sc[a:b], mid[a:b], ts[a:b], g['thr'], entry=g['entry'],
                      exit_mode=g['exit_mode'], max_hold=g['max_hold'], tp=g['tp'],
                      sl=g['sl'], wait_k=g['wait_k'], size=g['size'],
                      fee=FEES['mixed'])
        st = stats(tr, days[a:b], nb=1) if len(tr) >= MIN_TRADES else None
        if st: res.append((g, st))
    res.sort(key=lambda r: -r[1]['mean'])
    print(f"  {len(res)} configs cleared >= {MIN_TRADES} trades. Top 8 on VAL "
          f"(net bp/trade after {FEES['mixed']:.0f}bp):")
    print(f"  {'q':>5} {'entry':<8} {'exit':<6} {'hold':>5} {'tp/sl':>9} {'sz':>3} "
          f"{'n':>5} {'net bp':>8} {'win%':>6}")
    for g, st in res[:8]:
        tps = f"{g['tp']:.0f}/{g['sl']:.0f}" if g['tp'] else "-"
        print(f"  {g['q']:>5.2f} {g['entry']:<8} {g['exit_mode']:<6} {g['max_hold']:>5} "
              f"{tps:>9} {str(g['size'])[0]:>3} {st['n']:>5} {st['mean']:>+8.2f} "
              f"{st['win']*100:>5.1f}%")

    if not res:
        print("no config cleared the trade minimum on VAL"); return
    best = res[0][0]
    print(f"\nFROZEN config: q={best['q']} entry={best['entry']} exit={best['exit_mode']} "
          f"hold={best['max_hold']} tp/sl={best['tp']}/{best['sl']} size={best['size']}")

    # ── single evaluation on TEST ──────────────────────────────────────────
    print("\n" + "=" * 86)
    print("TEST (evaluated once, config frozen on VAL)")
    print("=" * 86)
    print(f"  {'variant':<34} {'n':>5} {'days':>5} {'hold':>5} {'gross':>8} "
          + " ".join(f"{'net '+k:>9}" for k in FEES) + "   95% CI (gross)")

    def show(tag, g):
        tr = backtest(sc[b:], mid[b:], ts[b:], g['thr'], entry=g['entry'],
                      exit_mode=g['exit_mode'], max_hold=g['max_hold'], tp=g['tp'],
                      sl=g['sl'], wait_k=g.get('wait_k', 1), size=g['size'], fee=0.0)
        st = stats(tr, days[b:])
        if not st:
            print(f"  {tag:<34} too few trades"); return None
        print(f"  {tag:<34} {st['n']:>5} {st['days']:>5} {st['hold']:>5.0f} "
              f"{st['mean']:>+8.2f} " +
              " ".join(f"{st['mean']-f:>+9.2f}" for f in FEES.values()) +
              f"   [{st['lo']:+.2f}, {st['hi']:+.2f}]")
        return st, tr

    base = dict(q=0.95, thr=THR[0.95], entry='first', exit_mode='time',
                max_hold=H4, tp=None, sl=None, size=False, wait_k=1)
    r_base = show("baseline (first entry, 4h hold)", base)
    r_best = show("FROZEN best-on-VAL", best)

    if r_best:
        st, tr = r_best
        r = np.array([t[2] for t in tr]); di = np.array([days[b:][t[0]] for t in tr])
        dirs = np.array([1 if sc[b:][t[0]] > 0 else -1 for t in tr])
        print(f"\n  leg split: long n={int((dirs>0).sum())} "
              f"{r[dirs>0].mean():+.2f} bp | short n={int((dirs<0).sum())} "
              f"{r[dirs<0].mean():+.2f} bp")
        rng = np.random.default_rng(0)
        nul = np.array([(rng.permutation(dirs) * np.abs(r) * np.sign(r * dirs)).mean()
                        for _ in range(2000)])
        mon = pd.Series([pd.Timestamp(ts[b:][t[0]], unit='s').to_period('M') for t in tr])
        g_m = pd.Series(r).groupby(mon).agg(['size', 'mean'])
        print("  by month (gross bp/trade): " +
              "  ".join(f"{p} n={int(row['size'])} {row['mean']:+.1f}"
                        for p, row in g_m.iterrows()))
        print(f"  months positive: {int((g_m['mean']>0).sum())}/{len(g_m)}")
        for k, f in FEES.items():
            ok = (st['mean'] - f > 0) and (st['lo'] - f > 0)
            print(f"  VERDICT @{k} ({f:.0f}bp): net {st['mean']-f:+.2f} "
                  f"CI [{st['lo']-f:+.2f}, {st['hi']-f:+.2f}] -> {'PASS' if ok else 'fail'}")




# ═══════════════════════════════════════════════════════════════════════════
def walkforward(K=4):
    """Repeated (sign -> val -> test) blocks. Signs AND config are re-chosen from
    each fold's own past; test blocks are disjoint and pooled. Uses all 12 months,
    which is the only way to get the trade count the CI needs."""
    df = pd.read_pickle(DATA); df.index = df.index.astype(np.int64)
    d = build(df)
    feats = [c for c in d.columns if not c.startswith('_') and not c.startswith('y_')
             and c not in ARM_B_ONLY]
    sub = d[np.isfinite(d[feats]).all(axis=1) & np.isfinite(d['_mid'])].copy()
    n = len(sub)
    mid = sub['_mid'].values.astype(float); ts = sub.index.values.astype(np.int64)
    days = sub['_day'].values; y = sub['y_norm'].values
    fin = np.isfinite(y)

    t0 = int(n * 0.35)                       # first test block starts here
    blk = (n - t0) // K
    print("\n" + "=" * 86)
    print(f"WALK-FORWARD ({K} folds; signs + config re-chosen from each fold's own past)")
    print("=" * 86)
    all_tr, all_days, all_dir, all_ts, base_tr = [], [], [], [], []
    for k in range(K):
        te_lo = t0 + k * blk
        te_hi = n if k == K - 1 else te_lo + blk
        va_lo = int(te_lo * 0.78); sg_hi = va_lo
        rows = []
        for f in feats:
            m_ = fin[:sg_hi]
            t, m, _ = daily_ic_t(sub[f].values[:sg_hi][m_], y[:sg_hi][m_], days[:sg_hi][m_])
            if np.isfinite(t): rows.append((f, t, m))
        rows.sort(key=lambda r: -abs(r[1])); top = rows[:3]
        sc = np.zeros(n)
        for f, t, m in top:
            v = pd.Series(sub[f].values)
            z = ((v - v.rolling(2016, min_periods=288).mean()) /
                 (v.rolling(2016, min_periods=288).std() + 1e-12)).values
            sc += np.nan_to_num(np.sign(m) * np.clip(z, -5, 5))
        thr95 = float(np.nanquantile(np.abs(sc[va_lo:te_lo]), 0.95))
        thrs = {q: float(np.nanquantile(np.abs(sc[va_lo:te_lo]), q))
                for q in (0.90, 0.95, 0.99)}
        best, bestv = None, -1e9
        for q, thr in thrs.items():
            for entry in ('first', 'confirm', 'wait_k', 'tick'):
                for em, mh in (('time', H4), ('time', 96), ('decay', H4), ('decay', 96)):
                    for size in (False, True):
                        tr = backtest(sc[va_lo:te_lo], mid[va_lo:te_lo], ts[va_lo:te_lo],
                                      thr, entry=entry, exit_mode=em, max_hold=mh,
                                      wait_k=3 if entry == 'wait_k' else 1, size=size,
                                      fee=FEES['mixed'])
                        if len(tr) < 25: continue
                        r = np.array([t_[2] for t_ in tr]); w = np.array([t_[3] for t_ in tr])
                        v_ = (r * w).sum() / w.sum()
                        if v_ > bestv:
                            bestv, best = v_, dict(q=q, thr=thr, entry=entry, exit_mode=em,
                                                   max_hold=mh, size=size,
                                                   wait_k=3 if entry == 'wait_k' else 1)
        if best is None:
            print(f"  fold {k}: no config cleared the val trade minimum"); continue
        tr = backtest(sc[te_lo:te_hi], mid[te_lo:te_hi], ts[te_lo:te_hi], best['thr'],
                      entry=best['entry'], exit_mode=best['exit_mode'],
                      max_hold=best['max_hold'], wait_k=best['wait_k'],
                      size=best['size'], fee=0.0)
        bt = backtest(sc[te_lo:te_hi], mid[te_lo:te_hi], ts[te_lo:te_hi], thr95,
                      entry='first', exit_mode='time', max_hold=H4, fee=0.0)
        r = np.array([t_[2] for t_ in tr]) if tr else np.array([])
        print(f"  fold {k}  test {pd.Timestamp(ts[te_lo],unit='s'):%Y-%m-%d}->"
              f"{pd.Timestamp(ts[te_hi-1],unit='s'):%Y-%m-%d}  "
              f"cfg={best['entry']}/{best['exit_mode']}/{best['max_hold']}/q{best['q']:.2f}"
              f"{'/sz' if best['size'] else ''}  val {bestv:+.1f}  "
              f"-> test n={len(tr)} gross {r.mean() if len(r) else float('nan'):+.2f}"
              f"  (baseline n={len(bt)} "
              f"{np.mean([t_[2] for t_ in bt]) if bt else float('nan'):+.2f})")
        for t_ in tr:
            all_tr.append(t_[2]); all_days.append(days[te_lo:te_hi][t_[0]])
            all_dir.append(1 if sc[te_lo:te_hi][t_[0]] > 0 else -1)
            all_ts.append(ts[te_lo:te_hi][t_[0]])
        base_tr += [t_[2] for t_ in bt]

    r = np.array(all_tr); di = np.array(all_days); dr = np.array(all_dir)
    rng = np.random.default_rng(0); ud = np.unique(di)
    idx = {x: np.where(di == x)[0] for x in ud}
    bs = np.array([r[np.concatenate([idx[x] for x in rng.choice(ud, len(ud), True)])].mean()
                   for _ in range(4000)])
    lo, hi = np.percentile(bs, [2.5, 97.5])
    bb = np.array(base_tr)
    print(f"\n  POOLED  n={len(r)} trades over {len(ud)} days")
    print(f"    baseline (first/4h)  gross {bb.mean():+.2f} bp  (n={len(bb)})")
    print(f"    selected             gross {r.mean():+.2f} bp  "
          f"win {100*(r>0).mean():.1f}%  95% CI [{lo:+.2f}, {hi:+.2f}]")
    print(f"    leg split: long n={int((dr>0).sum())} {r[dr>0].mean():+.2f} | "
          f"short n={int((dr<0).sum())} {r[dr<0].mean():+.2f}")
    mon = pd.Series([pd.Timestamp(t_, unit='s').to_period('M') for t_ in all_ts])
    gm = pd.Series(r).groupby(mon).agg(['size', 'mean'])
    print("    by month: " + "  ".join(f"{p} n={int(x['size'])} {x['mean']:+.0f}"
                                       for p, x in gm.iterrows()))
    print(f"    months positive: {int((gm['mean']>0).sum())}/{len(gm)}")
    for kk, f in FEES.items():
        ok = (r.mean() - f > 0) and (lo - f > 0)
        print(f"    VERDICT @{kk} ({f:.0f}bp): net {r.mean()-f:+.2f} "
              f"CI [{lo-f:+.2f}, {hi-f:+.2f}] -> {'PASS' if ok else 'fail'}")


if __name__ == "__main__":
    main()
    walkforward()
