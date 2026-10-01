"""R4 — run the pre-registered cross-sectional test (next_signal_ideas.md, R4). Local CPU, ~5-10 min.

    python run_r4.py [--root ../../data/xsec] [--out r4_results.json] [--perms 1000]
"""
import argparse, json, os, sys, time
import numpy as np, pandas as pd
from scipy.stats import norm
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import xsec as X

HERE = os.path.dirname(os.path.abspath(__file__))


def oos_years(n):
    """Split n OOS days into 4 consecutive 365-day blocks (remainder joins the last block)."""
    b = [min(n, 365 * i) for i in range(5)]; b[-1] = n
    return [(b[i], b[i + 1]) for i in range(4) if b[i + 1] > b[i]]


def run(root, perms=1000, seed=0, quiet=False):
    t0 = time.time()
    say = (lambda *a: None) if quiet else (lambda *a: print(*a, flush=True))
    p = X.load_panels(root)
    say(f"panels: {len(p['syms'])} symbols x {len(p['grid']):,} hours | {time.time() - t0:.0f}s")
    dp0, dp1 = X.daily_panels(p, 0), X.daily_panels(p, 1)
    fi = X.factor_inputs(p, dp0)
    U = X.universe(fi["elig"], fi["qv30"])
    days = dp0["days"]; D = len(days)
    act = np.flatnonzero(U[:, 0] >= 0)
    if len(act) == 0:
        raise SystemExit("no day with a full universe")
    start = act[0]; is_end = start + X.IS_DAYS
    oos = np.arange(is_end, D - 1)                                     # last day has no next-day return
    say(f"study {pd.to_datetime(days[start], unit='s').date()} -> {pd.to_datetime(days[-2], unit='s').date()} | "
        f"IS to {pd.to_datetime(days[is_end], unit='s').date()} | OOS {len(oos)} d | "
        f"universe days {len(act)} / {D - start} | {time.time() - t0:.0f}s")
    n_in = len({int(i) for i in U[start:].ravel() if i >= 0})
    btc = p["syms"].index("BTCUSDT") if "BTCUSDT" in p["syms"] else None
    TR0, TR1 = dp0["TR"], dp1["TR"]
    ISd = np.arange(start, is_end)

    # null: random 8 / 8 from the same universe (== permuting factor values across coins within a day)
    rng = np.random.default_rng(seed); k = X.TOP_N // X.QUANT
    NULL = {H: [] for H in X.HOLDS}
    for _ in range(perms):
        o = np.argsort(rng.random(U.shape), 1)
        Ur = np.take_along_axis(U, o, 1)
        L, S = Ur[:, -k:].copy(), Ur[:, :k].copy(); L[U[:, 0] < 0] = -1; S[U[:, 0] < 0] = -1
        for H in X.HOLDS:
            NULL[H].append(X.cohort_gross(L, S, TR0, H)[oos].mean())
    say(f"null done ({perms} draws) | {time.time() - t0:.0f}s")

    CELLS, FAC = [], {}
    for f in X.FACTORS:
        F = fi["fac"][f]
        ic = X.rank_ic(U, F, TR0)
        sign = 1.0 if np.nanmean(ic[ISd]) >= 0 else -1.0
        FAC[f] = dict(sign=sign, ic_is=float(np.nanmean(ic[ISd])), ic_is_t=float(X.nw_t(ic[ISd])),
                      ic_oos=float(np.nanmean(ic[oos])), ic_oos_t=float(X.nw_t(ic[oos])))
        L, S = X.legs(U, F, sign)
        for H in X.HOLDS:
            b0, b1 = X.book_returns(L, S, TR0, H), X.book_returns(L, S, TR1, H)
            cost = X.COST_BP / 1e4 * b0["turnover"]
            net = b0["gross"] - cost; net_lag = b1["gross"] - X.COST_BP / 1e4 * b1["turnover"]
            g, n_ = b0["gross"][oos], net[oos]; to = b0["turnover"][oos]
            t = X.nw_t(n_); pv = float(1 - norm.cdf(t)) if np.isfinite(t) else 1.0
            yrs = [float(n_[a:b].mean() * 1e4) for a, b in oos_years(len(oos))]
            beta = float(np.polyfit(TR0[oos, btc], n_, 1)[0]) if btc is not None else np.nan
            nullv = np.array(NULL[H])
            CELLS.append(dict(
                factor=f, H=H, sign=sign, n_days=int(len(oos)),
                gross_bp=float(g.mean() * 1e4), net_bp=float(n_.mean() * 1e4), net_lag_bp=float(net_lag[oos].mean() * 1e4),
                funding_bp=float(-(np.einsum('dn,dn->d', b0['W'], dp0['F'])[oos]).mean() * 1e4),
                turnover=float(to.mean()), c_star_bp=float(g.mean() / to.mean() * 1e4) if to.mean() > 0 else np.nan,
                sharpe=float(n_.mean() / n_.std() * np.sqrt(365)) if n_.std() > 0 else np.nan,
                nw_t=float(t), p_one=pv, years_bp=yrs, years_pos=int(sum(y > 0 for y in yrs)),
                maxdd_pct=float(X.max_drawdown(n_) * 100), btc_beta=beta,
                long_bp=float(b0["long"][oos].mean() * 1e4), short_bp=float(b0["short"][oos].mean() * 1e4),
                is_net_bp=float((b0["gross"] - cost)[ISd].mean() * 1e4),
                perm_pct=float((nullv < g.mean()).mean() * 100)))
        say(f"{f}: sign {sign:+.0f} | IC IS {FAC[f]['ic_is']:+.4f} (t {FAC[f]['ic_is_t']:+.1f}) "
            f"OOS {FAC[f]['ic_oos']:+.4f} (t {FAC[f]['ic_oos_t']:+.1f}) | {time.time() - t0:.0f}s")
    adj = X.holm([c["p_one"] for c in CELLS])
    for c, a in zip(CELLS, adj):
        c["p_holm"] = float(a)
        c["pass"] = bool(c["net_bp"] > 0 and a < 0.05 and c["c_star_bp"] >= 1.5 * X.COST_BP
                         and c["years_pos"] >= 3 and c["perm_pct"] >= 97.5 and c["net_lag_bp"] > 0)
    meta = dict(start=str(pd.to_datetime(days[start], unit="s").date()),
                is_end=str(pd.to_datetime(days[is_end], unit="s").date()),
                end=str(pd.to_datetime(days[-2], unit="s").date()), oos_days=int(len(oos)),
                symbols_loaded=len(p["syms"]), symbols_ever_in_universe=n_in, perms=perms,
                null_mean_bp={H: float(np.mean(NULL[H]) * 1e4) for H in X.HOLDS},
                null_p975_bp={H: float(np.percentile(NULL[H], 97.5) * 1e4) for H in X.HOLDS})
    return dict(meta=meta, factors=FAC, cells=CELLS, passed=any(c["pass"] for c in CELLS))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join(HERE, "..", "..", "data", "xsec"))
    ap.add_argument("--out", default=os.path.join(HERE, "r4_results.json"))
    ap.add_argument("--perms", type=int, default=1000)
    a = ap.parse_args()
    res = run(a.root, a.perms)
    json.dump(res, open(a.out, "w"), indent=1, default=float)
    m = res["meta"]
    print(f"\nstudy {m['start']} -> {m['end']} | IS to {m['is_end']} | OOS {m['oos_days']} d | "
          f"{m['symbols_ever_in_universe']} coins ever in the top-40 (of {m['symbols_loaded']})")
    df = pd.DataFrame(res["cells"])
    df["years"] = df["years_bp"].apply(lambda y: " ".join(f"{v:+.1f}" for v in y))
    print(df[["factor", "H", "sign", "gross_bp", "net_bp", "net_lag_bp", "funding_bp", "turnover", "c_star_bp",
              "sharpe", "nw_t", "p_holm", "years", "perm_pct", "maxdd_pct", "btc_beta", "long_bp", "short_bp",
              "is_net_bp", "pass"]].round(3).to_string(index=False))
    print("\nVERDICT:", "PASS — " + ", ".join(f"{c['factor']} H{c['H']}" for c in res["cells"] if c["pass"])
          if res["passed"] else "FAIL — no factor x holding cell passes all pre-registered criteria")


if __name__ == "__main__":
    main()
