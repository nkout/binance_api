"""Idea 1a (runs/next_signal_ideas.md): breakout / continuation on volatility-detector triggers.

Question: when the run.009f volatility detector fires, does a move that crosses +-k bp CONTINUE?
Under a martingale a stop-entry has zero expected value, so any positive gross here is
conditional momentum, and the detector adds value only if it beats random entry on the same days.

Mechanics (one position at a time, chronological, per fold):
  trigger at bar t (score >= causal threshold, flat)  ->  OCO stop orders at P0*(1 +- k)
  first bar close crossing a level within W bars      ->  taker entry at THAT close (gap included)
  exit: hold H bars after entry (taker), or trailing stop of k bp from the best close
  no cross within W                                    ->  cancel, no cost, flat again at t+W
  a trigger is used only if t+W+H stays inside one contiguous 5 s segment of one fold.

Threshold is causal: quantile of the SAME score on the fold's own validation window, which
immediately precedes its test window (run.009f layout). No test information sets it.

PRE-REGISTERED primary cell: det18 score, rate 0.1 %, k = 10 bp, W = 180 (15 min),
H = 720 (60 min), exit = hold, fee 10 bp round trip (both legs are stop/market = taker).
Pass requires ALL of:
  P1 net > 0 with day-bootstrap 95 % CI lower bound > 0
  P2 mean gross beats the day-matched random-entry null p97.5
  P3 continuation P(side * ret > 0) >= 0.55
  P4 both legs (long / short) gross > 0, and >= 3/4 folds gross > 0
Everything else in the grid is exploratory.

Usage:  python breakout_probe.py            (writes breakout_probe_results.json next to itself)
"""
import json
import os
import sys
import time

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
NPZ = os.path.join(HERE, "..", "run009f_scores.npz")
FEE_RT = {"taker": 10.0, "mixed": 7.0, "maker": 4.0}
N_NULL = 1000
N_BOOT = 4000
RNG = np.random.default_rng(0)

PRIMARY = dict(score="det18", rate=0.001, k=10.0, W=180, H=720, exit_rule="hold")


# ---------------------------------------------------------------- data
def load():
    z = np.load(NPZ, allow_pickle=True)
    cols = [str(c) for c in z["cls_cols"]]
    iu, idn = cols.index("lup_18"), cols.index("ldn_18")
    d = dict(
        close=z["close"].astype(float),
        dt=z["dt_s"].astype(np.int64),
        fold=z["fold_of"].astype(int),
        det18=(z["prob"][:, iu] + z["prob"][:, idn]).astype(float),
        val_det18=(z["val_prob"][:, iu] + z["val_prob"][:, idn]).astype(float),
        val_fold=z["val_fold_of"].astype(int),
    )
    W = int(z["window_sec"])
    n = len(d["close"])
    # contiguous segments: break on any cadence gap or fold change
    brk = np.ones(n, bool)
    brk[1:] = (np.diff(d["dt"]) != W) | (d["fold"][1:] != d["fold"][:-1])
    seg = np.cumsum(brk) - 1
    seg_end = np.zeros(n, np.int64)  # last index of each bar's segment
    starts = np.flatnonzero(brk)
    ends = np.r_[starts[1:] - 1, n - 1]
    seg_end[:] = ends[seg]
    d["seg_end"] = seg_end
    d["day"] = d["dt"] // 86400
    # model-free control score: trailing 5-min realised |r| sum (causal, within segment)
    r1 = np.zeros(n)
    r1[1:] = np.abs(np.log(d["close"][1:] / d["close"][:-1])) * 1e4
    r1[brk] = 0.0
    cs = np.cumsum(r1)
    L = 60
    rv = cs - np.r_[np.zeros(L), cs[:-L]]
    # bars with < L history inside their segment are not eligible for the RV score
    pos_in_seg = np.arange(n) - starts[seg]
    rv[pos_in_seg < L] = np.nan
    d["rv5m"] = rv
    return d


def thresholds(d, score, rate):
    """Per-fold causal threshold."""
    thr = {}
    for f in np.unique(d["fold"]):
        if score == "det18":
            v = d["val_det18"][d["val_fold"] == f]
            thr[f] = float(np.quantile(v, 1 - rate))
        else:
            # rv5m has no val closes: use a trailing 3-day quantile inside the test stream,
            # computed per day from strictly earlier days of the same fold (causal)
            thr[f] = None
    return thr


def trigger_mask(d, score, rate):
    s = d[score]
    trig = np.zeros(len(s), bool)
    if score == "det18":
        thr = thresholds(d, score, rate)
        for f, t in thr.items():
            m = d["fold"] == f
            trig[m] = s[m] >= t
        return trig
    # causal rolling-day threshold for the model-free RV control
    for f in np.unique(d["fold"]):
        idx = np.flatnonzero(d["fold"] == f)
        days = d["day"][idx]
        for dd in np.unique(days):
            past = idx[(days < dd) & (days >= dd - 3)]
            past = past[np.isfinite(s[past])]
            if len(past) < 5000:
                continue  # warm-up: first day(s) of each fold not traded
            t = np.quantile(s[past], 1 - rate)
            cur = idx[days == dd]
            trig[cur] = np.nan_to_num(s[cur], nan=-np.inf) >= t
    return trig


# ---------------------------------------------------------------- one trade
def one_trade(close, t, k, W, H, exit_rule):
    """Returns (filled, side, gross_bp, entry_idx, exit_idx). Caller guarantees t+W+H in segment."""
    p0 = close[t]
    up, dn = p0 * (1 + k * 1e-4), p0 * (1 - k * 1e-4)
    win = close[t + 1 : t + 1 + W]
    hu = np.flatnonzero(win >= up)
    hd = np.flatnonzero(win <= dn)
    iu = hu[0] if len(hu) else W
    idd = hd[0] if len(hd) else W
    if iu == W and idd == W:
        return False, 0, 0.0, t, t + W
    side = 1 if iu < idd else -1  # same-bar tie impossible: a close is either >= up or <= dn
    e = t + 1 + min(iu, idd)
    pe = close[e]
    if exit_rule == "hold":
        x = e + H
    else:  # trailing stop k bp from the best close since entry, capped at H
        path = close[e + 1 : e + 1 + H]
        if side == 1:
            best = np.maximum.accumulate(np.r_[pe, path])[1:]
            hit = np.flatnonzero(path <= best * (1 - k * 1e-4))
        else:
            best = np.minimum.accumulate(np.r_[pe, path])[1:]
            hit = np.flatnonzero(path >= best * (1 + k * 1e-4))
        x = e + 1 + hit[0] if len(hit) else e + H
    gross = side * (close[x] / pe - 1.0) * 1e4
    return True, side, gross, e, x


def run_rule(d, trig_idx, k, W, H, exit_rule):
    """Sequential one-position book over candidate trigger bars (sorted)."""
    close, seg_end = d["close"], d["seg_end"]
    busy_until = -1
    out = []
    for t in trig_idx:
        if t <= busy_until:
            continue
        if t + W + H > seg_end[t]:
            continue
        filled, side, g, e, x = one_trade(close, t, k, W, H, exit_rule)
        busy_until = x
        out.append((t, filled, side, g, e, x))
    return out


# ---------------------------------------------------------------- stats
def boot_ci_day(vals, days):
    ud = np.unique(days)
    if len(ud) < 3:
        return (np.nan, np.nan)
    sums = np.array([vals[days == u].sum() for u in ud])
    cnts = np.array([(days == u).sum() for u in ud])
    b = RNG.integers(0, len(ud), (N_BOOT, len(ud)))
    m = sums[b].sum(1) / np.maximum(cnts[b].sum(1), 1)
    return (float(np.percentile(m, 2.5)), float(np.percentile(m, 97.5)))


def summarise(d, res):
    att = len(res)
    fills = [r for r in res if r[1]]
    if not fills:
        return dict(attempts=att, n=0)
    g = np.array([r[3] for r in fills])
    side = np.array([r[2] for r in fills])
    t = np.array([r[0] for r in fills])
    days = d["day"][t]
    folds = d["fold"][t]
    net = g - FEE_RT["taker"]
    lo, hi = boot_ci_day(net, days)
    per_fold = {int(f): float(g[folds == f].mean()) for f in np.unique(folds)}
    sk = float(((g - g.mean()) ** 3).mean() / (g.std() ** 3 + 1e-12))
    return dict(
        attempts=att, n=int(len(g)), fill_rate=len(g) / att, days=int(len(np.unique(days))),
        gross=float(g.mean()), median=float(np.median(g)), cont=float((g > 0).mean()),
        net_taker=float(net.mean()), net_taker_ci=[lo, hi],
        net_mixed=float(g.mean() - FEE_RT["mixed"]), net_maker=float(g.mean() - FEE_RT["maker"]),
        long_n=int((side == 1).sum()), long_gross=float(g[side == 1].mean()) if (side == 1).any() else np.nan,
        short_n=int((side == -1).sum()), short_gross=float(g[side == -1].mean()) if (side == -1).any() else np.nan,
        per_fold=per_fold, folds_pos=int(sum(v > 0 for v in per_fold.values())), skew=sk,
        worst=float(g.min()), best=float(g.max()),
    )


def day_matched_null(d, res, k, W, H, exit_rule, n_null=N_NULL):
    """Same number of order placements per day, at uniformly random eligible bars, one position
    at a time. Returns the null distribution of mean gross per filled trade."""
    n = len(d["close"])
    elig = np.flatnonzero(np.arange(n) + W + H <= d["seg_end"])
    eday = d["day"][elig]
    att_days = d["day"][np.array([r[0] for r in res])]
    ud, cnt = np.unique(att_days, return_counts=True)
    pools = {u: elig[eday == u] for u in ud}
    means = np.empty(n_null)
    for i in range(n_null):
        tot, m = 0.0, 0
        for u, c in zip(ud, cnt):
            pool = pools[u]
            # draw sequentially so the one-position constraint holds; oversample then walk
            cand = np.sort(RNG.choice(pool, size=min(len(pool), c * 6), replace=False))
            busy, placed = -1, 0
            for t in cand:
                if placed == c:
                    break
                if t <= busy:
                    continue
                filled, side, g, e, x = one_trade(d["close"], t, k, W, H, exit_rule)
                busy, placed = x, placed + 1
                if filled:
                    tot += g
                    m += 1
        means[i] = tot / max(m, 1)
    return means


# ---------------------------------------------------------------- main
def cell(d, score, rate, k, W, H, exit_rule, null=False, trig_cache={}):
    key = (score, rate)
    if key not in trig_cache:
        trig_cache[key] = np.flatnonzero(trigger_mask(d, score, rate))
    res = run_rule(d, trig_cache[key], k, W, H, exit_rule)
    s = summarise(d, res)
    s.update(score=score, rate=rate, k=k, W=W, H=H, exit=exit_rule,
             trig_bars=int(len(trig_cache[key])))
    if null and s.get("n", 0) > 0:
        nm = day_matched_null(d, res, k, W, H, exit_rule)
        s["null_mean"] = float(nm.mean())
        s["null_p975"] = float(np.percentile(nm, 97.5))
        s["null_pctile"] = float((nm < s["gross"]).mean() * 100)
    return s


def verdict(p):
    checks = {
        "P1 net>0, CI lo>0": p["net_taker"] > 0 and p["net_taker_ci"][0] > 0,
        "P2 beats null p97.5": p["gross"] > p["null_p975"],
        "P3 continuation>=0.55": p["cont"] >= 0.55,
        "P4 both legs>0, >=3/4 folds": p["long_gross"] > 0 and p["short_gross"] > 0 and p["folds_pos"] >= 3,
    }
    return checks, all(checks.values())


def main():
    t0 = time.time()
    d = load()
    print(f"loaded n={len(d['close']):,}  segments={len(np.unique(d['seg_end']))}  "
          f"days={len(np.unique(d['day']))}")

    # sanity: realised trigger rate and |move| lift on triggers vs all bars (should echo §7.2)
    close, se = d["close"], d["seg_end"]
    n = len(close)
    ok = np.arange(n) + 720 <= se
    fwd = np.full(n, np.nan)
    fwd[ok] = np.abs(close[np.flatnonzero(ok) + 720] / close[ok] - 1) * 1e4
    sanity = {}
    for sc in ("det18", "rv5m"):
        tm = trigger_mask(d, sc, 0.001)
        m = tm & ok
        sanity[sc] = dict(realised_rate=float(tm.mean()),
                          emove_1h_trig=float(np.nanmean(fwd[m])),
                          emove_1h_all=float(np.nanmean(fwd[ok])),
                          lift=float(np.nanmean(fwd[m]) / np.nanmean(fwd[ok])))
        print(f"sanity {sc}: rate {tm.mean():.4%}  E|move|1h trig {sanity[sc]['emove_1h_trig']:.1f} "
              f"vs all {sanity[sc]['emove_1h_all']:.1f}  lift {sanity[sc]['lift']:.2f}x")

    prim = cell(d, **PRIMARY, null=True)
    checks, passed = verdict(prim)
    print("\nPRIMARY", json.dumps({k: prim[k] for k in prim if k != "per_fold"}, default=float))
    print("per-fold gross", prim["per_fold"])
    for c, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {c}")
    print("PRIMARY VERDICT:", "PASS" if passed else "FAIL")

    grid = []
    for score in ("det18", "rv5m"):
        for rate in (0.001, 0.01):
            for k in (5.0, 10.0, 20.0):
                for W in (60, 180):
                    for H in (180, 720):
                        for ex in ("hold", "trail"):
                            s = cell(d, score, rate, k, W, H, ex)
                            grid.append(s)
    # nulls for the det18 hold cells only (cost)
    for s in grid:
        if s["score"] == "det18" and s["exit"] == "hold" and s.get("n", 0) >= 30 and s["rate"] == 0.001:
            nm = day_matched_null(d, run_rule(d, np.flatnonzero(trigger_mask(d, "det18", s["rate"])),
                                              s["k"], s["W"], s["H"], "hold"), s["k"], s["W"], s["H"], "hold", n_null=300)
            s["null_p975"] = float(np.percentile(nm, 97.5))
            s["null_pctile"] = float((nm < s["gross"]).mean() * 100)

    print(f"\n{'score':6} {'rate':>6} {'k':>4} {'W':>4} {'H':>4} {'exit':5} {'n':>5} {'fill':>5} "
          f"{'gross':>7} {'med':>6} {'cont':>5} {'netT':>7} {'CIlo':>7} {'L':>7} {'S':>7} {'f+':>3} {'nullpct':>7}")
    for s in grid:
        if s.get("n", 0) == 0:
            continue
        print(f"{s['score']:6} {s['rate']:6.3f} {s['k']:4.0f} {s['W']:4d} {s['H']:4d} {s['exit']:5} "
              f"{s['n']:5d} {s['fill_rate']:5.2f} {s['gross']:7.2f} {s['median']:6.2f} {s['cont']:5.2f} "
              f"{s['net_taker']:7.2f} {s['net_taker_ci'][0]:7.2f} {s['long_gross']:7.2f} "
              f"{s['short_gross']:7.2f} {s['folds_pos']:3d} {s.get('null_pctile', float('nan')):7.1f}")

    best = max((s for s in grid if s.get("n", 0) >= 30), key=lambda s: s["net_taker"])
    print(f"\nbest-of-{len(grid)} exploratory cell by net: {best['score']} rate {best['rate']} k {best['k']} "
          f"W {best['W']} H {best['H']} {best['exit']}  net {best['net_taker']:.2f} "
          f"CI {best['net_taker_ci']}  n {best['n']}")

    out = dict(primary=prim, checks=checks, passed=passed, sanity=sanity, grid=grid,
               runtime_s=time.time() - t0)
    with open(os.path.join(HERE, "breakout_probe_results.json"), "w") as fh:
        json.dump(out, fh, indent=1, default=float)
    print(f"\nwrote breakout_probe_results.json  ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    sys.exit(main())
