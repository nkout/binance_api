"""R4b — run the pre-registered low-volatility factor test (next_signal_ideas.md, R4b + implementation details).

    python run_r4b.py [--root ../../data/xsec] [--out r4b_results.json]
"""
import argparse, json, os, sys, time
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import xsec as X
import lowvol as LV

HERE = os.path.dirname(os.path.abspath(__file__))


def run(root, btc="BTCUSDT", band=(41, 100), perms=300, quiet=False, info_top=True):
    t0 = time.time(); say = (lambda *a: None) if quiet else (lambda *a: print(*a, flush=True))
    p = X.load_panels(root)
    dp0, dp1 = X.daily_panels(p, 0), X.daily_panels(p, 1)
    fi = X.factor_inputs(p, dp0)
    j = p["syms"].index(btc)
    elig = fi["elig"].copy(); elig[:, j] = False                     # BTC is the hedge, never a candidate
    days = dp0["days"]; rv = fi["fac"]["RVOL30"]
    say(f"panels {len(p['syms'])} symbols, {len(days)} days | {time.time() - t0:.0f}s")
    U = LV.universe_band(elig, fi["qv30"], *band)
    res = LV.evaluate(U, rv, rv, dp0["TR"], dp1["TR"], j, days, perms=perms)
    res["band"] = list(band); say(f"primary universe {band} done | {time.time() - t0:.0f}s")
    out = dict(primary=res)
    if info_top:
        U1 = LV.universe_band(elig, fi["qv30"], 1, 40)
        out["info_top40"] = LV.evaluate(U1, rv, rv, dp0["TR"], dp1["TR"], j, days, perms=100, ls_arm=False)
        out["info_top40"]["band"] = [1, 40]; say(f"top-40 info done | {time.time() - t0:.0f}s")
    return out


def _line(name, s):
    return (f"    {name:8s} net {s['net_bp']:+6.1f} bp/d (gross {s['gross_bp']:+6.1f}, cost {s['cost_bp']:.1f}) | ann {s['ann_pct']:+6.1f} % | Sharpe {s['sharpe']:5.2f} | NW t {s['nw_t']:5.2f} | "
            f"median {s['median_bp']:+6.1f} | trimmed mean {s['mean_trim_bp']:+6.1f} | std {s['daily_std_pct']:.2f} % | skew {s['skew']:+5.2f} | worst {s['worst_day_pct']:+6.1f} % | "
            f"maxDD {s['max_dd_pct']:7.1f} pp | beta {s['beta_to_btc']:+.2f} | hedge {s['mean_hedge']:.2f} | turnover {s['turnover']:.2f}/d | blocks+ {s['blocks_pos']}/{s['blocks_n']} | lag {s['lag_net_bp']:+.1f}")


def report(res):
    for key in ("primary", "info_top40"):
        if key not in res:
            continue
        r = res[key]
        print(f"\n===== {key}: ranks {r['band'][0]}-{r['band'][1]}, quintile {r['quintile']}, {r['first_day']} -> {r['last_day']} ({r['days']} d) =====")
        for H, c in r["cells"].items():
            print(f"  H={H}")
            for nm in ("primary", "control", "info_ls"):
                if nm in c:
                    print(_line(nm, c[nm]))
            print(f"    blocks bp/day (primary): {np.round(c['primary']['blocks_bp_per_day'], 1).tolist()} | primary - control {c['diff_bp']:+.1f} bp/d (NW t {c['diff_nw_t']:.2f}) | perm pct {c['perm_pct']:.1f} (null p97.5 {c['perm_q975_bp']:+.1f} bp)")
            for k, v in c["criteria"].items():
                print(f"      {'OK  ' if v['ok'] else 'FAIL'} {k}: {v['detail']}")
            print(f"    cell: {'PASS' if c['pass'] else 'FAIL'}")
        print(f"  {key} VERDICT: {'PASS' if r['passed'] else 'FAIL'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join(HERE, "..", "..", "data", "xsec"))
    ap.add_argument("--out", default=os.path.join(HERE, "r4b_results.json"))
    a = ap.parse_args()
    res = run(a.root)
    json.dump(res, open(a.out, "w"), indent=1, default=float)
    report(res)


if __name__ == "__main__":
    main()
