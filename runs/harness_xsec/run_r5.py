"""R5 — run the pre-registered delta-neutral funding-carry test (next_signal_ideas.md, R5). Local CPU.

    python run_r5.py [--root ../../data/xsec] [--out r5_results.json]
"""
import argparse, json, os, sys, time
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import xsec as X
import carry as C

HERE = os.path.dirname(os.path.abspath(__file__))
RF_PCT = 4.5


def run(root, quiet=False, bench_syms=("BTCUSDT", "ETHUSDT")):
    t0 = time.time(); say = (lambda *a: None) if quiet else (lambda *a: print(*a, flush=True))
    p = X.load_panels(root)
    dp0, dp1 = X.daily_panels(p, 0), X.daily_panels(p, 1)
    fi = X.factor_inputs(p, dp0)
    U = X.universe(fi["elig"], fi["qv30"])
    cs, qs = C.load_spot(root, p["syms"], p["grid"])
    dps0, sok = C.spot_panels(p, cs, qs, 0)
    dps1, _ = C.spot_panels(p, cs, qs, 1)
    days = dp0["days"]; D, N = dp0["P"].shape
    inU = np.zeros((D, N), bool)
    for d in np.flatnonzero(U[:, 0] >= 0):
        inU[d, U[d]] = True
    elig = inU & sok & np.isfinite(dp0["P"])
    start = int(np.flatnonzero(U[:, 0] >= 0)[0]); idx = np.arange(start, D - 1)
    say(f"panels + spot: {N} perps, {int(np.isfinite(cs).any(0).sum())} with spot | study "
        f"{pd.to_datetime(days[start], unit='s').date()} -> {pd.to_datetime(days[-2], unit='s').date()} | "
        f"mean eligible/day {elig[idx].sum(1).mean():.1f} of {inU[idx].sum(1).mean():.0f} | {time.time() - t0:.0f}s")
    fz = fi["fac"]["FUND7"]

    def legs_ret(dp, dps):
        Rp = np.where(np.isfinite(dp["P"]), dp["TR"] + dp["F"], 0.0)          # perp price-only return
        return Rp, dps["TR"], dp["F"]

    out = {}
    for name, mirror in (("CARRY+", False), ("CARRY-", True)):
        r0 = C.simulate(elig, fz, *legs_ret(dp0, dps0), mirror=mirror)
        r1 = C.simulate(elig, fz, *legs_ret(dp1, dps1), mirror=mirror)
        eps = C.episodes(r0["pos"][idx])
        npos = r0["pos"][idx].sum(1)
        held = npos.sum()
        unit_daily = r0["gross"][idx].sum() / C.N_SLOT / held if held else np.nan
        highs = {}
        for j in {e[0] for e in eps}:
            k = pd.read_parquet(os.path.join(root, "klines", f"{p['syms'][j]}.parquet"), columns=["ts", "high"])
            h = np.full(len(p["grid"]), np.nan); k = k[(k.ts >= p["grid"][0]) & (k.ts <= p["grid"][-1])]
            h[((k.ts.to_numpy() - p["grid"][0]) // 3600).astype(int)] = k.high.to_numpy(); highs[j] = h
        eps_abs = [(j, a + start, b + start) for j, a, b in eps]
        stress = C.margin_stress(eps_abs, days, p["grid"], highs, dp0["P"]) if not mirror else None
        out[name] = dict(
            **C.summarize(r0["net"], days, idx),
            lag_ann_pct=float(r1["net"][idx].mean() * 365 * 100),
            gross_ann_pct=float(r0["gross"][idx].mean() * 365 * 100),
            funding_ann_pct=float(r0["funding"][idx].mean() * 365 * 100),
            basis_ann_pct=float((r0["gross"] - r0["funding"])[idx].mean() * 365 * 100),
            cost_ann_pct=float(r0["cost"][idx].mean() * 365 * 100),
            mean_positions=float(npos.mean()), days_with_position_pct=float((npos > 0).mean() * 100),
            episodes=len(eps), mean_hold_days=float(np.mean([b - a + 1 for _, a, b in eps])) if eps else 0.0,
            coins_used=len({e[0] for e in eps}), margin_stress_episodes=stress,
            per_position_ann_pct=float(unit_daily * 365 * 100) if held else np.nan,
            top_coins=pd.Series([p["syms"][e[0]] for e in eps]).value_counts().head(10).to_dict() if eps else {})
        out[name]["_net"] = r0["net"]
        say(f"{name}: ann {out[name]['ann_pct']:+.2f} % | {time.time() - t0:.0f}s")

    # benchmark: BTC + ETH permanently hedged, 50 / 50
    jb, je = (p["syms"].index(s) for s in bench_syms)
    n_b = 0.5 / (1 + 1 / C.LEV)
    Rp, Rs, F = legs_ret(dp0, dps0)
    ok = np.isfinite(dp0["P"][:, [jb, je]]).all(1) & np.isfinite(dps0["P"][:, [jb, je]]).all(1)
    bench = np.where(ok, n_b * (Rs[:, jb] - Rp[:, jb] + F[:, jb] + Rs[:, je] - Rp[:, je] + F[:, je]), 0.0)
    bench[start] -= 2 * n_b * (C.SPOT_BP + C.PERP_BP) / 1e4
    out["BTCETH"] = dict(**C.summarize(bench, days, idx),
                         funding_ann_pct=float(np.where(ok, n_b * (F[:, jb] + F[:, je]), 0)[idx].mean() * 365 * 100))
    cp = out["CARRY+"]
    exc = cp["_net"][idx] - bench[idx]
    cp["excess_vs_btceth_ann_pct"] = float(exc.mean() * 365 * 100); cp["excess_nw_t"] = float(X.nw_t(exc))
    years = cp["years_pct"]
    cp["pass_detail"] = dict(ann_ge_rf=cp["ann_pct"] >= RF_PCT, every_year_pos=all(v > 0 for v in years.values()),
                             worst_month=cp["worst_month_pct"] > -5, nw_t=cp["nw_t"] >= 2, lag_pos=cp["lag_ann_pct"] > 0)
    cp["pass"] = all(cp["pass_detail"].values())
    for v in out.values():
        v.pop("_net", None)
    meta = dict(start=str(pd.to_datetime(days[start], unit="s").date()), end=str(pd.to_datetime(days[-2], unit="s").date()),
                days=int(len(idx)), rf_pct=RF_PCT, params=dict(enter=C.ENTER, exit=C.EXIT, max_pos=C.MAX_POS, lev=C.LEV,
                spot_bp=C.SPOT_BP, perp_bp=C.PERP_BP, min_spot_qv=C.MIN_SPOT_QV))
    return dict(meta=meta, **out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join(HERE, "..", "..", "data", "xsec"))
    ap.add_argument("--out", default=os.path.join(HERE, "r5_results.json"))
    a = ap.parse_args()
    res = run(a.root)
    json.dump(res, open(a.out, "w"), indent=1, default=float)
    m = res["meta"]; print(f"\nstudy {m['start']} -> {m['end']} ({m['days']} d) | risk-free {m['rf_pct']} %")
    for k in ("CARRY+", "BTCETH", "CARRY-"):
        v = res[k]
        print(f"\n{k}: ann net {v['ann_pct']:+.2f} % | Sharpe {v['sharpe']:.2f} | NW t {v['nw_t']:.2f} | worst month "
              f"{v['worst_month_pct']:+.2f} % | max DD {v['maxdd_pct']:+.2f} % | funding {v['funding_ann_pct']:+.2f} %/yr")
        print("   years: " + "  ".join(f"{y} {x:+.2f}%" for y, x in v["years_pct"].items()))
        if k != "BTCETH":
            print(f"   lag 1 h {v['lag_ann_pct']:+.2f} % | gross {v['gross_ann_pct']:+.2f} = funding {v['funding_ann_pct']:+.2f} "
                  f"+ basis {v['basis_ann_pct']:+.2f} | costs {v['cost_ann_pct']:.2f} | mean positions {v['mean_positions']:.2f} "
                  f"| invested {v['days_with_position_pct']:.0f} % of days | {v['episodes']} episodes, mean hold "
                  f"{v['mean_hold_days']:.1f} d, {v['coins_used']} coins | per-position {v['per_position_ann_pct']:+.1f} %/yr"
                  + (f" | margin-stress episodes {v['margin_stress_episodes']}" if v.get("margin_stress_episodes") is not None else ""))
            print(f"   top coins: {v['top_coins']}")
    cp = res["CARRY+"]
    print(f"\nCARRY+ excess over BTCETH: {cp['excess_vs_btceth_ann_pct']:+.2f} %/yr (NW t {cp['excess_nw_t']:.2f})")
    print("criteria:", cp["pass_detail"])
    print("VERDICT:", "PASS" if cp["pass"] else "FAIL")


if __name__ == "__main__":
    main()
