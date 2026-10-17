"""R9-H — CARRY- on historical borrow rates (next_signal_ideas.md, Round 9). Local CPU + the signed Binance history endpoint.

    python run_r9h.py assets     # R5 panels -> r9h_assets.json (base assets that meet the CARRY- entry condition in the window)
    python run_r9h.py fetch      # needs BINANCE_KEY / BINANCE_SECRET (read-only key); raw cache in data/borrow_api/ (resumable)
    python run_r9h.py score      # rows (a)-(f), checks, verdict -> r9h_results.json
"""
import gzip, json, os, sys, time
from concurrent.futures import ThreadPoolExecutor
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import xsec as X
import carry as C
import r9_borrow as R
import r9h as H

ROOT = os.path.join(HERE, "..", "..", "data", "xsec")
CACHE = os.path.join(HERE, "..", "..", "data", "borrow_api")
ASSETS = os.path.join(HERE, "r9h_assets.json")
SNAP_R9 = os.path.join(HERE, "r9_borrow_snapshot.json")
SNAP_XCHECK = os.path.join(HERE, "..", "..", "data", "borrow_history", "borrow_2026-10-05.json.gz")
WINDOW_END = "2026-08-30"
ANCHORS = ("BTC", "ETH", "BNB")
RF = R.RF


def panels():
    p = X.load_panels(ROOT)
    dp0, dp1 = X.daily_panels(p, 0), X.daily_panels(p, 1)
    fi = X.factor_inputs(p, dp0)
    U = X.universe(fi["elig"], fi["qv30"])
    cs, qs = C.load_spot(ROOT, p["syms"], p["grid"])
    dps0, sok = C.spot_panels(p, cs, qs, 0)
    dps1, _ = C.spot_panels(p, cs, qs, 1)
    days = dp0["days"]; D, N = dp0["P"].shape
    inU = np.zeros((D, N), bool)
    for d in np.flatnonzero(U[:, 0] >= 0):
        inU[d, U[d]] = True
    elig = inU & sok & np.isfinite(dp0["P"])
    start = int(np.flatnonzero(U[:, 0] >= 0)[0])
    end = int(np.flatnonzero(days == pd.Timestamp(WINDOW_END, tz="UTC").timestamp())[0])
    legs = lambda dp, dps: (np.where(np.isfinite(dp["P"]), dp["TR"] + dp["F"], 0.0), dps["TR"], dp["F"])
    return dict(p=p, days=days, elig=elig, fz=fi["fac"]["FUND7"], start=start, end=end, L0=legs(dp0, dps0), L1=legs(dp1, dps1),
                bases=[R.base_asset(s) for s in p["syms"]])


def cmd_assets():
    P = panels(); idx = np.arange(P["start"], P["end"] + 1)
    hit = (P["elig"][idx] & (-P["fz"][idx] >= C.ENTER)).any(0)
    syms = [P["p"]["syms"][j] for j in np.flatnonzero(hit)]
    bases = sorted({P["bases"][j] for j in np.flatnonzero(hit)} | set(ANCHORS))
    json.dump(dict(window=[str(pd.to_datetime(P["days"][P["start"]], unit="s").date()), WINDOW_END], symbols=syms, assets=bases), open(ASSETS, "w"), indent=1)
    print(f"{len(syms)} perps meet the CARRY- entry condition on >= 1 day; {len(bases)} base assets (incl. anchors {ANCHORS}) -> {ASSETS}")


def cmd_fetch(threads=3):
    key, sec = os.environ["BINANCE_KEY"], os.environ["BINANCE_SECRET"]
    assets = json.load(open(ASSETS))["assets"]
    wins = H.windows() + H.windows(*H.XCHECK, span_d=15)
    lim = H.Limiter()
    get = lambda prm: H.signed_get(H.PATH, prm, key, sec, limiter=lim)
    t0 = time.time(); done = {"with_records": 0, "never_on_margin": 0, "error": 0}

    def one(a):
        try:
            return a, H.fetch_asset(a, wins, CACHE, get)
        except Exception as e:                                   # logged; a rerun resumes from the cache
            return a, f"error {type(e).__name__}: {str(e)[:120]}"

    print(f"fetch {len(assets)} assets x {len(wins)} windows -> {CACHE}", flush=True)
    with ThreadPoolExecutor(threads) as ex:
        for i, (a, st) in enumerate(ex.map(one, assets), 1):
            err = isinstance(st, str)
            done["error" if err else "with_records" if st[0] else "never_on_margin"] += 1
            if err or i % 10 == 0 or i == len(assets):
                print(f"  {i}/{len(assets)} {a}: {st if err else f'{st[0]} windows ok, {st[1]} not on margin'} | {done} | {time.time() - t0:.0f}s", flush=True)


def summarize_row(res, days, idx, B, pos_ref=None):
    sm = C.summarize(res["net"], days, idx)
    pos = res["restricted"]["pos"][idx]
    nb = float(res["restricted"]["net"][idx].mean() * 365); ba = float(res["borrow"][idx].mean() * 365)
    out = dict(ann_pct=sm["ann_pct"], years_pct=sm["years_pct"], worst_month_pct=sm["worst_month_pct"], maxdd_pct=sm["maxdd_pct"], nw_t=sm["nw_t"], sharpe=sm["sharpe"],
               mean_positions=float(pos.sum(1).mean()), borrow_cost_ann_pct=ba * 100, net_before_borrow_ann_pct=nb * 100,
               breakeven_multiple=R.breakeven_multiple(nb, ba, RF) if ba > 0 else float("inf"))
    if pos_ref is not None:
        out["share_r5_position_days_not_borrowable"] = float((pos_ref & ~B[idx]).sum() / max(pos_ref.sum(), 1))
    return out


def tier(row, full_years):
    yrs = {y: row["years_pct"].get(y, 0.0) for y in full_years}
    others = dict(net=row["ann_pct"] >= RF * 100, worst_month=row["worst_month_pct"] > -5, nw_t=row["nw_t"] >= 2,
                  lag=row.get("lag_ann_pct", 0) > 0, breakeven=row["breakeven_multiple"] >= 1.5)
    every_year = all(v > 0 for v in yrs.values())
    neg = [v for v in yrs.values() if v <= 0]
    if len(full_years) < 3:
        t = "UNDERPOWERED"
    elif all(others.values()) and every_year:
        t = "PASS"
    elif all(others.values()) and len(neg) == 1 and neg[0] > -2:
        t = "ALIVE-forward"
    else:
        t = "DEAD"
    return t, dict(others, every_year=every_year)


def cmd_score():
    t0 = time.time()
    P = panels(); days = P["days"]; elig, fz = P["elig"], P["fz"]; bases = P["bases"]
    fetched = json.load(open(ASSETS))["assets"]
    cols = [b if b in fetched else None for b in bases]
    r, ex, am, bad_vip = H.borrow_matrices(cols, CACHE, days)
    unsupported = sorted(a for a in fetched if H.load_records(CACHE, a) is None)
    # window: R5 start .. 2026-08-30, from the first day on which BTC, ETH, BNB all have a rate
    anc = [bases.index(s) for s in ("BTC", "ETH", "BNB")]
    have = np.isfinite(r[:, anc]).all(1)
    w0 = max(P["start"], int(np.flatnonzero(have)[0]))
    idx = np.arange(w0, P["end"] + 1)
    full_years = [y for y in range(2020, 2027) if pd.Timestamp(f"{y}-01-01", tz="UTC").timestamp() >= days[w0] and pd.Timestamp(f"{y}-12-31", tz="UTC").timestamp() <= days[P["end"]]]

    # checks on the real data
    snap = json.load(gzip.open(SNAP_XCHECK))["assets"]
    d_x = np.array([pd.Timestamp("2026-10-05", tz="UTC").timestamp()], np.int64)
    rel = {}
    for a in fetched:
        rec = H.load_records(CACHE, a)
        if rec is None or a not in snap or snap[a]["daily"] <= 0:
            continue
        hv = H.day_rates(rec[0], rec[1], d_x)[0]
        if np.isfinite(hv):
            rel[a] = abs(hv / snap[a]["daily"] - 1)
    xmatch = float(np.mean([v <= 0.10 for v in rel.values()])) if rel else float("nan")
    checks = dict(vip_level_all_zero=not bad_vip, bad_vip=bad_vip, xcheck_assets=len(rel), xcheck_share_within_10pct=xmatch, xcheck_pass=bool(rel) and xmatch >= 0.90,
                  xcheck_worst=sorted(rel.items(), key=lambda kv: -kv[1])[:5])

    # rows
    today = json.load(open(SNAP_R9))["assets"]
    on_list = np.array([b in today for b in bases]); rate_today = np.array([today[b]["daily"] if b in today else 0.0 for b in bases])
    L0, L1 = P["L0"], P["L1"]
    base = C.simulate(elig, fz, *L0, mirror=True); pos_ref = base["pos"][idx]
    rows = {}
    a_ = H.reprice_tv(elig, fz, *L0, am, r)
    rows["a_primary"] = summarize_row(a_, days, idx, am, pos_ref)
    rows["a_primary"]["lag_ann_pct"] = float(H.reprice_tv(elig, fz, *L1, am, r)["net"][idx].mean() * 365 * 100)
    Bb = ex & on_list[None, :]
    rows["b_hist_rates_today_list"] = summarize_row(H.reprice_tv(elig, fz, *L0, Bb, r), days, idx, Bb, pos_ref)
    c_ = R.reprice(elig, fz, *L0, on_list, rate_today)
    rows["c_r9_s2_today_rates"] = summarize_row(dict(restricted=c_["restricted"], borrow=c_["borrow"], net=c_["net"]), days, idx, np.broadcast_to(on_list, ex.shape), pos_ref)
    rows["d_r5_gross"] = dict(ann_pct=float(base["net"][idx].mean() * 365 * 100), years_pct=C.summarize(base["net"], days, idx)["years_pct"], mean_positions=float(pos_ref.sum(1).mean()))
    rows["f_registered_rate_exists"] = summarize_row(H.reprice_tv(elig, fz, *L0, ex, r), days, idx, ex, pos_ref)
    for k in ("b_hist_rates_today_list", "f_registered_rate_exists"):
        Bk = Bb if k.startswith("b") else ex
        rows[k]["lag_ann_pct"] = float(H.reprice_tv(elig, fz, *L1, Bk, r)["net"][idx].mean() * 365 * 100)

    # primary diagnostics
    pos = a_["restricted"]["pos"]; unit = L0[0] - L0[1] - L0[2]
    prev = np.vstack([np.zeros((1, pos.shape[1]), bool), pos[:-1]])
    trades_j = ((pos & ~prev) | (~pos & prev))[idx].sum(0)
    contrib = C.N_SLOT * (np.where(pos, unit - np.nan_to_num(r), 0.0)[idx].sum(0) - trades_j * (C.SPOT_BP + C.PERP_BP) / 1e4)
    order = np.argsort(-contrib); tot = contrib.sum()
    held = pos[idx]; rr = np.nan_to_num(r[idx]); deep = fz[idx] <= -0.001
    diag = dict(top3=[(P["p"]["syms"][j], float(contrib[j] * 100)) for j in order[:3]], top3_share=float(contrib[order[:3]].sum() / tot) if tot > 0 else float("nan"),
                coins_held=int(held.any(0).sum()),
                paid_rate_ann_pct_deep_neg_funding=float(rr[held & deep].mean() * 365 * 100) if (held & deep).any() else float("nan"),
                paid_rate_ann_pct_other=float(rr[held & ~deep].mean() * 365 * 100) if (held & ~deep).any() else float("nan"),
                held_days_deep=int((held & deep).sum()), held_days_other=int((held & ~deep).sum()),
                mean_paid_rate_ann_pct=float(rr[held].mean() * 365 * 100) if held.any() else float("nan"),
                frozen_position_days_excluded=int((base["pos"][idx] & ex[idx] & ~am[idx]).sum()))
    tiers = {k: tier(v, full_years) for k, v in rows.items() if k[0] in "abf"}
    verdict = tiers["a_primary"][0] if checks["vip_level_all_zero"] and checks["xcheck_pass"] else "STOPPED (real-data check failed)"
    out = dict(window=[str(pd.to_datetime(days[w0], unit="s").date()), str(pd.to_datetime(days[P["end"]], unit="s").date())], days=len(idx), full_years=full_years,
               assets_fetched=len(fetched), assets_unsupported=len(unsupported), unsupported=unsupported, checks=checks, rows=rows, diagnostics=diag,
               tiers={k: dict(tier=t, criteria=c) for k, (t, c) in tiers.items()}, verdict=verdict)
    json.dump(out, open(os.path.join(HERE, "r9h_results.json"), "w"), indent=1, default=float)

    print(f"R9-H window {out['window'][0]} -> {out['window'][1]} ({len(idx)} d), full years {full_years} | assets {len(fetched)} ({len(unsupported)} never on margin) | {time.time() - t0:.0f}s")
    print(f"checks: vipLevel all 0 {checks['vip_level_all_zero']} | snapshot 2026-10-05 within 10 %: {xmatch * 100:.1f} % of {len(rel)} assets -> {'ok' if checks['xcheck_pass'] else 'FAIL'}"
          f" (worst {', '.join(f'{a} {v * 100:.0f} %' for a, v in checks['xcheck_worst'][:3])})")
    names = dict(a_primary="(a) primary, amended B", b_hist_rates_today_list="(b) hist. rates, today's list", c_r9_s2_today_rates="(c) R9 S2, today's rates",
                 d_r5_gross="(d) R5 gross of borrow", f_registered_rate_exists="(f) registered B: rate exists")
    for k, v in rows.items():
        line = f"  {names[k]:34s} {v['ann_pct']:+7.2f} %/yr  pos {v['mean_positions']:.2f}"
        if "nw_t" in v:
            line += (f"  NW t {v['nw_t']:.2f}  worst month {v['worst_month_pct']:+.2f} %  maxDD {v['maxdd_pct']:.2f} %  borrow {v['borrow_cost_ann_pct']:.2f} %/yr"
                     f"  break-even {v['breakeven_multiple']:.2f}x")
        if "lag_ann_pct" in v:
            line += f"  lag {v['lag_ann_pct']:+.2f}"
        if "share_r5_position_days_not_borrowable" in v:
            line += f"  R5 days not borrowable {v['share_r5_position_days_not_borrowable'] * 100:.1f} %"
        print(line)
        print("      years: " + "  ".join(f"{y} {x:+.2f}" for y, x in v["years_pct"].items()))
    print(f"diagnostics: top-3 {diag['top3']} share {diag['top3_share'] * 100:.1f} % | coins held {diag['coins_held']} | mean paid rate {diag['mean_paid_rate_ann_pct']:.1f} %/yr"
          f" (FUND7 <= -0.10 %: {diag['paid_rate_ann_pct_deep_neg_funding']:.1f} on {diag['held_days_deep']} d; other {diag['paid_rate_ann_pct_other']:.1f} on {diag['held_days_other']} d)"
          f" | R5 position-days dropped as frozen {diag['frozen_position_days_excluded']}")
    for k, v in out["tiers"].items():
        print(f"  tier {names[k]:34s} {v['tier']:14s} {v['criteria']}")
    print(f"VERDICT (primary): {verdict}")


if __name__ == "__main__":
    {"assets": cmd_assets, "fetch": cmd_fetch, "score": cmd_score}[sys.argv[1]]()
