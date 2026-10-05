"""R6 — run the pre-registered daily trend-following / vol-targeting test (next_signal_ideas.md, Round 4, R6).

    python run_r6.py [--root ../../data/xsec] [--out r6_results.json]
"""
import argparse, json, os, sys, time
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import xsec as X
import trend as T

HERE = os.path.dirname(os.path.abspath(__file__))


def run(root, btc="BTCUSDT", nshift=2000, nshift_xs=500, quiet=False):
    t0 = time.time(); say = (lambda *a: None) if quiet else (lambda *a: print(*a, flush=True))
    p = X.load_panels(root)
    dp = X.daily_panels(p, 0)
    days, P, TR = dp["days"], dp["P"], dp["TR"]
    say(f"panels: {len(p['syms'])} symbols, {len(days)} days | {time.time() - t0:.0f}s")
    j = p["syms"].index(btc)
    rb, stb = T.evaluate_btc(P[:, j], TR[:, j], days, nshift=nshift)
    say(f"BTC arms done | {time.time() - t0:.0f}s")
    fi = X.factor_inputs(p, dp)
    U = X.universe(fi["elig"], fi["qv30"])
    rx, stx = T.evaluate_xs(U, P, TR, days, nshift=nshift_xs)
    say(f"XS arms done | {time.time() - t0:.0f}s")
    return dict(btc=rb, xs=rx, meta=dict(rf_pct=T.RF * 100, cost_bp=X.COST_BP, target_vol=T.TARGET_VOL, cap=T.CAP,
                                         band=T.BAND, lookbacks=list(T.LOOKBACKS)))


def _row(name, v, lag=None):
    nb, ny = T.years_beating_cash(v)
    s = (f"  {name:8s} net {v['ann_pct']:+7.2f} %/yr (CAGR {v['cagr_pct']:+7.2f}) | Sharpe {v['sharpe']:5.2f} | vol {v['vol_ann_pct']:5.1f} % | "
         f"maxDD {v['maxdd_pct']:7.1f} % | skew {v['skew']:+5.2f} | worst day {v['worst_day_pct']:+6.1f} % | exposure {v['mean_abs_exposure']:.2f} "
         f"| turnover {v['turnover_per_year']:5.1f}/yr | cost {v['cost_ann_pct']:.2f} %/yr | beats cash {nb}/{ny}")
    if lag is not None:
        s += f" | lag-1d {lag['ann_pct']:+.2f} %/yr"
    return s


def _years(v):
    return "      " + "  ".join(f"{y} {x['ret'] * 100:+.1f}%" for y, x in v["years"].items())


def report(res):
    for key, title in (("btc", "BTC (1A / 1B)"), ("xs", "per-coin top-40 (1C)")):
        r = res[key]
        print(f"\n===== {title}: {r['start']} -> {r['end']} ({r['days']} d) =====")
        for k, v in r["arms"].items():
            print(_row(k, v, r["lag1"].get(k))); print(_years(v))
        print(f"  shift null: actual percentile {r['null']['pct']:.1f} (null median Sharpe {r['null']['q50']:.2f}, p97.5 {r['null']['q975']:.2f}, n {r['null']['n']})")
        for ck in [c for c in r if c.startswith("sharpe_diff")]:
            print(f"  {ck}: {r[ck]}")
        if key == "btc":
            print(f"  A leg split (ann %): {r['leg_split_ann_pct']}")
        for k, c in r["criteria"].items():
            print(f"    {'OK  ' if c['ok'] else 'FAIL'} {k}: {c['detail']}")
        print("  VERDICT:", "PASS" if r["passed"] else "FAIL")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join(HERE, "..", "..", "data", "xsec"))
    ap.add_argument("--out", default=os.path.join(HERE, "r6_results.json"))
    a = ap.parse_args()
    res = run(a.root)
    json.dump(res, open(a.out, "w"), indent=1, default=float)
    report(res)


if __name__ == "__main__":
    main()
