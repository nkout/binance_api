"""N5 drift after a new token's Binance perp listing — real run (pre-registered in next_signal_ideas.md, Round 7 / N5).

    python run_n5.py      -> prints the report (tee to n5_output.txt) and writes n5_results.json

Registered cell: new tokens, entry at the first 00:00 >= L + 48 h, >= $5 M perp volume in the 24 h before entry, short the coin / long BTC
at equal notional for H = 14 days, 18 bp. In sample = exit <= 2026-08-31 00:00; exits after that are the forward block (descriptive).
Tests: test_listing.py.
"""
import json, os, sys, time
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import listing as N

ROOT = os.path.join(HERE, "..", "..", "data", "xsec")
T_HI = int(pd.Timestamp("2026-08-31", tz="UTC").timestamp())
T_END = "2026-10-01"


def _date(t):
    return str(pd.to_datetime(int(t), unit="s").date())


def cell(pp, tab, btc, H, t_hi, ctrl, cls="new", gap_h=N.ENTRY_GAP_H, min_qv=N.MIN_QV, lag_h=0, control=True):
    """Events of one construction split into in sample (exit <= t_hi) and forward, the control attached to the in-sample events."""
    ev = N.build_events(pp, tab, btc, H, cls=cls, gap_h=gap_h, min_qv=min_qv, lag_h=lag_h)
    if not len(ev):
        return ev, ev
    ins, fwd = ev[ev.exit <= t_hi].reset_index(drop=True), ev[ev.exit > t_hi].reset_index(drop=True)
    if control and len(ins):
        ins = N.add_control(pp, ins, btc, *ctrl)
    return ins, fwd


def summary(ins):
    s = N.stats_with_ci(ins["net"].to_numpy(), ins["entry"].to_numpy())
    if "minus_ctrl" in ins:
        s["minus_ctrl"] = N.stats_with_ci(ins["minus_ctrl"].to_numpy(), ins["entry"].to_numpy())
        s["ctrl_net"] = N.describe(ins["ctrl_net"].to_numpy())
        s["ctrl_missing"] = int(ins["ctrl_net"].isna().sum())
    s["blind"] = N.describe(ins["blind"].to_numpy())
    s["decomp_pct"] = {k: float(ins[k].mean() * 100) for k in ("coin_ret", "coin_fund", "btc_ret", "btc_fund")}
    s["died"] = int(ins["died"].sum())
    return s


def run(root=ROOT, btc="BTCUSDT", t_hi=T_HI, t_end=T_END, t_start="2020-01-01", info=True, quiet=False, meta=None):
    t0 = time.time(); say = (lambda *a: None) if quiet else (lambda *a: print(*a, flush=True))
    p = X.load_panels(root, t_start=t_start, t_end=t_end)
    meta = meta or N.load_meta()
    tab = N.listing_table(p, meta)
    pp = N.prep(p)
    j = p["syms"].index(btc)
    crypto = np.zeros(len(p["syms"]), bool)
    crypto[tab[~tab.cls.isin(["tradfi", "index"])].j.to_numpy()] = True
    ctrl = N.control_sets(p, crypto)
    days = X._days(p["grid"])
    say(f"panels {len(p['syms'])} symbols, {len(days)} days; classes {tab.cls.value_counts().to_dict()} | {time.time() - t0:.0f}s")
    res = dict(classes={k: int(v) for k, v in tab.cls.value_counts().items()},
               relisted=sorted(tab[tab.cls == "relisted"].sym.tolist()))

    ins, fwd = cell(pp, tab, j, N.H_PRIMARY, t_hi, ctrl)
    lag, _ = cell(pp, tab, j, N.H_PRIMARY, t_hi, ctrl, lag_h=1, control=False)
    prim = summary(ins)
    years = N.by_year(ins)
    for y, v in N.by_year(ins, "minus_ctrl").items():
        years[y]["minus_ctrl"] = v["mean"]
    for y, v in N.by_year(ins, "blind").items():
        years[y]["blind"] = v["mean"]
    net, taken, nopen = N.portfolio(pp, ins, j, days)
    idx = np.arange(int(np.searchsorted(days, ins.entry.min())), int(np.searchsorted(days, t_hi)))
    port = N.portfolio_stats(net, days, idx)
    port.update(skipped=int((~taken).sum()), mean_open=float(nopen[idx].mean()), max_open=int(nopen[idx].max()),
                first_day=_date(days[idx[0]]), last_day=_date(days[idx[-1]]), n_days=len(idx))
    lag_mean = float(lag["net"].mean() * 100)
    v, crit = N.verdict(prim, prim["minus_ctrl"], years, lag_mean, port)
    q = np.quantile(ins["qv24"], [1 / 3, 2 / 3])
    terc = np.digitize(ins["qv24"], q)
    res["primary"] = dict(stats=prim, years=years, lag_mean=lag_mean, lag_n=int(len(lag)), portfolio=port, verdict=v,
                          criteria={k: bool(c) for k, c in crit.items()},
                          terciles={lab: N.describe(ins["net"].to_numpy()[terc == k]) for k, lab in
                                    enumerate((f"low (< ${q[0] / 1e6:.0f} M)", f"mid (${q[0] / 1e6:.0f}-{q[1] / 1e6:.0f} M)",
                                               f"high (>= ${q[1] / 1e6:.0f} M)"))},
                          subtype={k: N.describe(g["net"].to_numpy()) for k, g in ins.groupby("sub")},
                          worst=[dict(sym=r.sym, entry=_date(r.entry), net_pct=float(r.net * 100), coin_ret_pct=float(r.coin_ret * 100),
                                      coin_fund_pct=float(r.coin_fund * 100), btc_pct=float(r.btc * 100), died=bool(r.died))
                                 for r in ins.nsmallest(10, "net").itertuples()],
                          window=[_date(ins.entry.min()), _date(ins.entry.max())])
    res["forward"] = dict(n=int(len(fwd)), **({"stats": N.describe(fwd["net"].to_numpy()),
                                                "events": [dict(sym=r.sym, entry=_date(r.entry), net_pct=float(r.net * 100)) for r in fwd.itertuples()]}
                                               if len(fwd) else {}))
    say(f"primary done | {time.time() - t0:.0f}s")

    if info:
        arms = {}
        for name, kw in ((f"H = {N.H_INFO[0]}", dict(H=N.H_INFO[0])), (f"H = {N.H_INFO[1]}", dict(H=N.H_INFO[1])),
                         ("seasoned listings", dict(H=N.H_PRIMARY, cls="seasoned")),
                         (f"liquidity >= ${N.QV_INFO[0] / 1e6:.0f} M", dict(H=N.H_PRIMARY, min_qv=N.QV_INFO[0])),
                         (f"liquidity >= ${N.QV_INFO[1] / 1e6:.0f} M", dict(H=N.H_PRIMARY, min_qv=N.QV_INFO[1])),
                         (f"entry >= L + {N.GAP_INFO_H} h", dict(H=N.H_PRIMARY, gap_h=N.GAP_INFO_H))):
            a, _ = cell(pp, tab, j, kw.pop("H"), t_hi, ctrl, **kw)
            arms[name] = dict(stats=summary(a), years=N.by_year(a)) if len(a) else dict(stats=dict(n=0))
        res["info"] = arms
        # sensitivity: the one registered re-listing exclusion put back
        rel = tab[tab.cls == "relisted"].assign(cls="new")
        e, _ = cell(pp, rel, j, N.H_PRIMARY, t_hi, ctrl, control=False)
        if len(e):
            allnet = np.r_[ins["net"].to_numpy(), e["net"].to_numpy()]
            res["relisted_back"] = dict(n_added=int(len(e)), mean_pct=float(allnet.mean() * 100),
                                        added=[dict(sym=r.sym, net_pct=float(r.net * 100)) for r in e.itertuples()])
        say(f"info arms done | {time.time() - t0:.0f}s")
    return res


def fmt(s):
    if not s.get("n"):
        return "n 0"
    out = (f"n {s['n']:4d} | mean {s['mean']:+6.2f} %" + (f" CI [{s['ci'][0]:+6.2f}, {s['ci'][1]:+6.2f}]" if "ci" in s else "")
           + f" | median {s['median']:+6.2f} | trimmed {s['trimmed']:+6.2f} | win {s['pos_share']:.0%} | worst {s['worst']:+7.1f} | "
           f"worst-1% {s['worst1']:+7.1f} | best {s['best']:+6.1f}")
    return out


def report(res):
    pr = res["primary"]; s = pr["stats"]; po = pr["portfolio"]
    print(f"\nclasses: {res['classes']} | relisted (excluded): {res['relisted']}")
    print(f"\n===== N5 PRIMARY: new tokens, entry >= L + {N.ENTRY_GAP_H} h, >= ${N.MIN_QV / 1e6:.0f} M, short coin / long BTC, H = {N.H_PRIMARY} d =====")
    print(f"entries {pr['window'][0]} -> {pr['window'][1]}")
    print(f"  event net (%)         {fmt(s)}")
    print(f"  event - control (%)   {fmt(s['minus_ctrl'])}   [control missing on {s['ctrl_missing']} events]")
    print(f"  control alone (%)     {fmt(s['ctrl_net'])}")
    print(f"  blind short (%)       {fmt(s['blind'])}")
    print(f"  1 h lag row: net mean {pr['lag_mean']:+.2f} % (n {pr['lag_n']})")
    print("  decomposition (mean %): " + " | ".join(f"{k} {v:+.2f}" for k, v in s["decomp_pct"].items()) + f" | cost {-N.COST_PAIR * 100:.2f}"
          + f" | events closed at a delisting: {s['died']}")
    print("  per entry year:")
    for y, v in pr["years"].items():
        print(f"    {y}: n {v['n']:3d} | net mean {v['mean']:+6.2f} | median {v['median']:+6.2f} | event - control "
              f"{v.get('minus_ctrl', float('nan')):+6.2f} | blind {v.get('blind', float('nan')):+6.2f}")
    print(f"  portfolio ({po['first_day']} -> {po['last_day']}, {po['n_days']} days; 2 % per leg, <= {N.MAX_OPEN} open; mean open {po['mean_open']:.1f}, "
          f"max {po['max_open']}, skipped {po['skipped']}):")
    print(f"    annualised {po['ann_pct']:+.2f} % | NW t {po['nw_t']:.2f} | Sharpe {po['sharpe']:.2f} | worst month {po['worst_month_pct']:+.2f} % | "
          f"worst day {po['worst_day_pct']:+.2f} % | max DD {po['maxdd_pct']:.1f} %")
    print("    calendar years (%): " + " | ".join(f"{y} {v:+.1f}" for y, v in po["years_pct"].items()))
    for k, c in pr["criteria"].items():
        print(f"    [{'x' if c else ' '}] {k}")
    print(f"  VERDICT N5: {pr['verdict']}")
    print("\n  by entry-day volume tercile (information):")
    for k, v in pr["terciles"].items():
        print(f"    {k:26s} {fmt(v)}")
    print("  by token type (information):")
    for k, v in sorted(pr["subtype"].items(), key=lambda kv: -kv[1].get("n", 0)):
        print(f"    {k:26s} {fmt(v)}")
    print("  10 worst events:")
    for r in pr["worst"]:
        print(f"    {r['entry']} {r['sym']:16s} net {r['net_pct']:+7.1f} % | coin {r['coin_ret_pct']:+7.1f} % | coin funding to short "
              f"{r['coin_fund_pct']:+6.1f} % | BTC leg {r['btc_pct']:+6.1f} %" + (" | delisted" if r["died"] else ""))
    if "info" in res:
        print("\n===== INFORMATION ARMS (not part of the verdict) =====")
        for k, a in res["info"].items():
            st = a["stats"]
            print(f"  {k:22s} {fmt(st)}")
            if st.get("n"):
                print(f"  {'':22s} event - control {fmt(st['minus_ctrl'])}")
                print(f"  {'':22s} years: " + " | ".join(f"{y} {v['mean']:+.1f} (n {v['n']})" for y, v in a["years"].items()))
    if "relisted_back" in res:
        r = res["relisted_back"]
        print(f"\n  sensitivity: with the {r['n_added']} re-listing exclusion(s) put back {r['added']}: primary mean {r['mean_pct']:+.2f} %")
    f = res["forward"]
    print(f"\n===== FORWARD BLOCK (exit after the in-sample end; unseen; descriptive) =====")
    if f["n"]:
        print(f"  {fmt(f['stats'])}")
        for r in f["events"]:
            print(f"    {r['entry']} {r['sym']:16s} {r['net_pct']:+7.1f} %")
    else:
        print("  no events")


def main():
    t0 = time.time()
    res = run()
    json.dump(res, open(os.path.join(HERE, "n5_results.json"), "w"), indent=1, default=float)
    report(res)
    print(f"\n({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
