"""N1 long BTC / short a mid-cap alt basket, beta-neutral — real run (pre-registered in next_signal_ideas.md, Round 6 / N1).

    python run_n1.py      -> prints the report (tee to n1_output.txt) and writes n1_results.json

Registered: ranks 101-200 (BTC excluded), inverse-RVOL30 weights capped at 2 %, H = 7, days < 2026-08-31.
Days from 2026-08-31 are the forward block (descriptive). Tests: test_n1.py.
"""
import json, os, sys, time
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import altshort as A

ROOT = os.path.join(HERE, "..", "..", "data", "xsec")
T_HI = int(pd.Timestamp("2026-08-31", tz="UTC").timestamp())
T_END = "2026-10-01"


def run(root=ROOT, btc="BTCUSDT", band=(101, 200), t_hi=T_HI, t_end=T_END, info=True, quiet=False):
    t0 = time.time(); say = (lambda *a: None) if quiet else (lambda *a: print(*a, flush=True))
    p = X.load_panels(root, t_end=t_end)
    dp0, dp1 = X.daily_panels(p, 0), X.daily_panels(p, 1)
    fi = X.factor_inputs(p, dp0)
    j = p["syms"].index(btc)
    elig = fi["elig"].copy(); elig[:, j] = False
    days, rv, TR0, TR1, F = dp0["days"], fi["fac"]["RVOL30"], dp0["TR"], dp1["TR"], dp0["F"]
    say(f"panels {len(p['syms'])} symbols, {len(days)} days | {time.time() - t0:.0f}s")
    import lowvol as LV
    U = LV.universe_band(elig, fi["qv30"], *band)
    first_u = days[np.flatnonzero(U[:, 0] >= 0)[0]]
    res = dict(band=list(band), first_universe_day=str(pd.to_datetime(first_u, unit="s").date()))
    prim = A.arm(U, rv, TR0, TR1, F, j, days, t_hi, A.H_PRIMARY)
    idx = prim["idx"]
    res["window"] = [str(pd.to_datetime(days[idx[0]], unit="s").date()), str(pd.to_datetime(days[idx[-1]], unit="s").date()), len(idx)]
    v, crit = A.verdict(prim["stats"], prim["stats"]["lag_net_bp"])
    res["primary"] = dict(stats=prim["stats"], decomp=prim["decomp"], verdict=v, criteria={k: bool(c) for k, c in crit.items()},
                          worst=A.worst_coins(prim["full"], TR0, idx, j, p["syms"], days))
    # forward block: same frozen positions, days >= t_hi (never the final panel row)
    fidx = np.arange(np.searchsorted(days, t_hi), len(days) - 1)
    if len(fidx):
        fn, _ = LV.net_series(prim["full"], TR0, fidx)
        res["forward"] = dict(first=str(pd.to_datetime(days[fidx[0]], unit="s").date()), days=len(fidx),
                              net_bp=float(fn.mean() * 1e4), median_bp=float(np.median(fn) * 1e4), sum_pct=float(fn.sum() * 100),
                              worst_day_pct=float(fn.min() * 100))
    say(f"primary done | {time.time() - t0:.0f}s")
    if info:
        arms = {"H=1 (same construction)": A.arm(U, rv, TR0, TR1, F, j, days, t_hi, A.H_INFO, idx=idx),
                "equal-weight short, ranks 101-200": A.arm(U, rv, TR0, TR1, F, j, days, t_hi, A.H_PRIMARY, ew=True, idx=idx)}
        for lo, hi in ((41, 100), (1, 40)):
            Ub = LV.universe_band(elig, fi["qv30"], lo, hi)
            k = hi - lo + 1
            arms[f"ranks {lo}-{hi}, inv-vol cap {2 / k:.1%}"] = A.arm(Ub, rv, TR0, TR1, F, j, days, t_hi, A.H_PRIMARY, cap=2 / k, idx=idx)
        res["info"] = {k: dict(stats=a["stats"], decomp=a["decomp"]) for k, a in arms.items()}
        say(f"info arms done | {time.time() - t0:.0f}s")
    return res


def line(name, s):
    return (f"  {name:38s} net {s['net_bp']:+6.2f} bp/d (gross {s['gross_bp']:+6.2f}, cost {s['cost_bp']:.2f}) | NW t {s['nw_t']:5.2f} | "
            f"median {s['median_bp']:+6.2f} | trimmed {s['trimmed_bp']:+6.2f} | ann {s['ann_pct']:+6.1f} % | Sharpe {s['sharpe']:5.2f} | "
            f"std {s['std_pct']:.2f} % | worst {s['worst_day_pct']:+6.2f} % | worst-1% {s['worst1pct_mean_pct']:+6.2f} % | maxDD {s['max_dd_pct']:6.1f} | "
            f"beta {s['beta_to_btc']:+.2f} | hedge {s['mean_hedge']:.2f} | short {s['mean_short']:.2f} | turnover {s['turnover']:.2f} | lag {s['lag_net_bp']:+.2f}")


def report(res):
    pr = res["primary"]; s = pr["stats"]
    print(f"\n===== N1 PRIMARY: ranks {res['band'][0]}-{res['band'][1]}, inv-vol cap {A.CAP:.0%}, H = {A.H_PRIMARY} =====")
    print(f"first day with the band: {res['first_universe_day']} | window {res['window'][0]} -> {res['window'][1]} ({res['window'][2]} days, "
          f"{s['live_days']} live)")
    print(line("primary", s))
    print(f"  {A.BLOCK_D}-day blocks (bp/day): {np.round(s['blocks_bp'], 2).tolist()} -> {s['blocks_pos']}/{s['blocks_n']} positive")
    print("  P&L decomposition (bp/day): " + " | ".join(f"{k} {v:+.2f}" for k, v in pr["decomp"].items()))
    for k, c in pr["criteria"].items():
        print(f"    [{'x' if c else ' '}] {k}")
    print(f"  VERDICT N1: {pr['verdict']}")
    w = pr["worst"]
    print(f"\n  largest single-coin daily loss: {w['largest']['coin']} on {w['largest']['day']}: {w['largest']['pct']:+.2f} % of notional "
          f"(coin total return {w['largest']['coin_ret_pct']:+.1f} %)")
    print("  10 worst days:")
    for r in w["worst_days"]:
        print(f"    {r['day']}  book {r['book_pct']:+6.2f} % | worst coin {r['coin']:14s} {r['coin_pct']:+6.2f} % (ret {r['coin_ret_pct']:+7.1f} %) | BTC leg {r['btc_pct']:+6.2f} %")
    if "info" in res:
        print("\n===== INFORMATION ARMS (same window; not part of the verdict) =====")
        for k, a in res["info"].items():
            print(line(k, a["stats"]))
            print("      decomposition: " + " | ".join(f"{kk} {vv:+.2f}" for kk, vv in a["decomp"].items())
                  + f" | blocks {np.round(a['stats']['blocks_bp'], 1).tolist()}")
    if "forward" in res:
        f = res["forward"]
        print(f"\n===== FORWARD BLOCK from {f['first']} ({f['days']} days, unseen; descriptive) =====")
        print(f"  net {f['net_bp']:+.2f} bp/d | median {f['median_bp']:+.2f} | total {f['sum_pct']:+.2f} % | worst day {f['worst_day_pct']:+.2f} %")


def main():
    t0 = time.time()
    res = run()
    json.dump(res, open(os.path.join(HERE, "n1_results.json"), "w"), indent=1, default=float)
    report(res)
    print(f"\n({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
