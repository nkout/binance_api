"""R9 — CARRY- borrow-rate check (next_signal_ideas.md, R9 implementation details). Local CPU + public Binance endpoints.

    python r9_borrow.py            # fetch today's snapshot, score S1 (today's candidates) and S2 (R5 CARRY- re-priced), write r9_results.json

Borrow rates come from Binance's undocumented public web endpoint (VIP0 cross-margin daily rate and borrow limit, current only).
The raw snapshot is saved to r9_borrow_snapshot.json so a history can accumulate.
"""
import json, os, re, sys, time, urllib.request
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import xsec as X
import carry as C

RF = 0.045
ROUND_TRIP_BP = 2 * (C.SPOT_BP + C.PERP_BP)           # 24 bp
HOLD_D = 15.0                                         # R5 mean CARRY- hold
AMORT = ROUND_TRIP_BP / 1e4 * 365 / HOLD_D            # per year
BORROW_URL = "https://www.binance.com/bapi/margin/v1/public/margin/vip/spec/list-all"
FAPI = "https://fapi.binance.com/fapi/v1/"
CAND_FUND7 = -0.0003
PREFILTER = -0.0001
MIN_LIMIT_USD, MIN_ALIVE = 50_000, 3


def get(url, tries=5):
    for k in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.load(r)
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(2 ** k)


def base_asset(symbol):
    """BTCUSDT -> BTC; 1000SHIBUSDT -> SHIB; 1MBABYDOGEUSDT -> BABYDOGE (multiplier prefixes stripped, 1INCH kept)."""
    b = symbol[:-4] if symbol.endswith("USDT") else symbol
    return re.sub(r"^(1000000|100000|10000|1000|1M)(?=[A-Z])", "", b)


def fetch_borrow():
    d = get(BORROW_URL)["data"]
    out = {}
    for a in d:
        v0 = next((s for s in a["specs"] if s["vipLevel"] == "0"), None)
        if v0:
            out[a["assetName"]] = dict(daily=float(v0["dailyInterestRate"]), limit=float(v0["borrowLimit"]))
    return out


def fund7(rates_by_time, now_ms):
    """Sum of funding over the last 7 days / 21 (the R4 / R5 per-8h convention)."""
    cut = now_ms - 7 * 86400 * 1000
    return sum(r for t, r in rates_by_time if t >= cut) / 21.0


def fetch_candidates(now_ms):
    pi = get(FAPI + "premiumIndex")
    rows = [(x["symbol"], float(x["markPrice"]), float(x["lastFundingRate"])) for x in pi
            if x["symbol"].endswith("USDT") and "_" not in x["symbol"] and x.get("lastFundingRate") not in (None, "")]
    pre = [r for r in rows if r[2] <= PREFILTER]
    out = []
    for s, mark, last in pre:
        h = get(FAPI + f"fundingRate?symbol={s}&limit=1000")
        f7 = fund7([(int(e["fundingTime"]), float(e["fundingRate"])) for e in h], now_ms)
        out.append(dict(symbol=s, base=base_asset(s), mark=mark, last_funding=last, fund7=f7))
        time.sleep(0.15)
    return len(rows), out


def annualise_daily(d):
    return d * 365.0


def score_s1(cands, borrow):
    rows = []
    for c in cands:
        if c["fund7"] > CAND_FUND7:
            continue
        b = borrow.get(c["base"])
        income = -c["fund7"] * 3 * 365
        row = dict(c, income_ann=income, borrowable=b is not None)
        if b:
            row.update(borrow_ann=annualise_daily(b["daily"]), limit_usd=b["limit"] * c["mark"])
            row["net_ann"] = income - row["borrow_ann"] - AMORT
        rows.append(row)
    ok = [r for r in rows if r["borrowable"] and r["net_ann"] >= RF and r["limit_usd"] >= MIN_LIMIT_USD]
    return rows, dict(n_candidates=len(rows), n_borrowable=sum(r["borrowable"] for r in rows), n_qualifying=len(ok), alive=len(ok) >= MIN_ALIVE)


def reprice(elig, fz, Rp, Rs, F, borrowable, rate_daily, rf=RF):
    """CARRY- as R5 but only on currently borrowable coins, paying today's borrow rate on the spot-short notional."""
    base = C.simulate(elig, fz, Rp, Rs, F, mirror=True)
    r = C.simulate(elig & borrowable[None, :], fz, Rp, Rs, F, mirror=True)
    borrow = C.N_SLOT * (r["pos"] * rate_daily[None, :]).sum(1)
    return dict(base=base, restricted=r, borrow=borrow, net=r["net"] - borrow)


def breakeven_multiple(net_no_borrow_ann, borrow_ann, rf=RF):
    """Uniform multiple k of today's borrow rates with net = rf (0 if already below rf before any borrow cost)."""
    if borrow_ann <= 0:
        return float("inf")
    return max(0.0, (net_no_borrow_ann - rf) / borrow_ann)


def s2(root, borrow, quiet=False):
    p = X.load_panels(root); dp0 = X.daily_panels(p, 0); fi = X.factor_inputs(p, dp0)
    U = X.universe(fi["elig"], fi["qv30"])
    cs, qs = C.load_spot(root, p["syms"], p["grid"]); dps0, sok = C.spot_panels(p, cs, qs, 0)
    days = dp0["days"]; D, N = dp0["P"].shape
    inU = np.zeros((D, N), bool)
    for d in np.flatnonzero(U[:, 0] >= 0):
        inU[d, U[d]] = True
    elig = inU & sok & np.isfinite(dp0["P"])
    start = int(np.flatnonzero(U[:, 0] >= 0)[0]); idx = np.arange(start, D - 1)
    Rp = np.where(np.isfinite(dp0["P"]), dp0["TR"] + dp0["F"], 0.0); Rs, F = dps0["TR"], dp0["F"]
    bases = [base_asset(s) for s in p["syms"]]
    borrowable = np.array([b in borrow for b in bases])
    rate = np.array([borrow[b]["daily"] if b in borrow else 0.0 for b in bases])
    res = reprice(elig, fi["fac"]["FUND7"], Rp, Rs, F, borrowable, rate)
    pos0, pos1 = res["base"]["pos"][idx], res["restricted"]["pos"][idx]
    days_all = int(pos0.sum()); days_not = int((pos0 & ~borrowable[None, :]).sum())
    net_nb = res["restricted"]["net"]; sm = C.summarize(res["net"], days, idx); sm0 = C.summarize(res["base"]["net"], days, idx)
    smnb = C.summarize(net_nb, days, idx)
    borrow_ann = float(res["borrow"][idx].mean() * 365)
    k = breakeven_multiple(float(net_nb[idx].mean() * 365), borrow_ann)
    held_borrow = pos1.sum()
    mean_rate_ann = float((pos1 * rate[None, :]).sum() / held_borrow * 365) if held_borrow else float("nan")
    top = pd.Series([bases[j] for j in np.nonzero(pos0.any(0))[0]])
    r5top = ["BNB", "WAVES", "BCH", "ENA", "TRUMP", "CRV", "TRX", "APE", "API3", "AXS"]
    return dict(
        start=str(pd.to_datetime(days[start], unit="s").date()), end=str(pd.to_datetime(days[-2], unit="s").date()), days=len(idx),
        r5_original=dict(ann_pct=sm0["ann_pct"], years_pct=sm0["years_pct"], mean_positions=float(pos0.sum(1).mean())),
        borrowable_only_no_borrow_cost=dict(ann_pct=smnb["ann_pct"], years_pct=smnb["years_pct"], mean_positions=float(pos1.sum(1).mean())),
        repriced=dict(ann_pct=sm["ann_pct"], years_pct=sm["years_pct"], worst_month_pct=sm["worst_month_pct"], maxdd_pct=sm["maxdd_pct"],
                      sharpe=sm["sharpe"], mean_positions=float(pos1.sum(1).mean()), borrow_cost_ann_pct=borrow_ann * 100,
                      mean_borrow_rate_on_held_ann_pct=mean_rate_ann * 100, breakeven_multiple=k),
        coverage=dict(r5_position_days=days_all, not_borrowable_today=days_not, share_not_borrowable=days_not / days_all if days_all else float("nan"),
                      coins_in_r5_episodes=int(len(top)), coins_not_listed=int(sum(not borrowable[j] for j in np.nonzero(pos0.any(0))[0]))),
        r5_top_coins_today={b: (dict(borrow_ann=borrow[b]["daily"] * 365, limit_coin=borrow[b]["limit"]) if b in borrow else None) for b in r5top},
        alive=bool(sm["ann_pct"] >= RF * 100 and all(sm["years_pct"].get(y, 0) > 0 for y in range(2021, 2026))),
    )


def main():
    now_ms = int(time.time() * 1000)
    borrow = fetch_borrow()
    json.dump(dict(fetched_utc=pd.Timestamp.now('UTC').isoformat(), source=BORROW_URL, vip_level=0, assets=borrow), open(os.path.join(HERE, "r9_borrow_snapshot.json"), "w"))
    n_sym, cands = fetch_candidates(now_ms)
    rows, v1 = score_s1(cands, borrow)
    print(f"borrow list: {len(borrow)} assets | perps scanned {n_sym}, pre-filtered {len(cands)} (last funding <= -0.01 %), FUND7 <= -0.03 %: {len(rows)}")
    for r in sorted(rows, key=lambda r: -r["income_ann"]):
        print(f"  {r['symbol']:14s} FUND7 {r['fund7'] * 100:+.3f} %/8h  income {r['income_ann'] * 100:6.1f} %/yr  " +
              (f"borrow {r['borrow_ann'] * 100:6.1f} %/yr  net {r['net_ann'] * 100:+7.1f} %/yr  limit ${r['limit_usd']:,.0f}" if r["borrowable"] else "NOT on the cross-margin list"))
    print(f"S1: {v1['n_borrowable']} of {v1['n_candidates']} borrowable; {v1['n_qualifying']} qualify (net >= 4.5 %, limit >= $50k) -> {'ALIVE' if v1['alive'] else 'DEAD'}")
    root = os.path.join(HERE, "..", "..", "data", "xsec")
    v2 = s2(root, borrow)
    o, b, r = v2["r5_original"], v2["borrowable_only_no_borrow_cost"], v2["repriced"]
    print(f"\nS2 R5 CARRY- {v2['start']} -> {v2['end']} ({v2['days']} d)")
    print(f"  R5 original (gross of borrow)        {o['ann_pct']:+7.2f} %/yr, {o['mean_positions']:.2f} positions")
    print(f"  currently-borrowable coins only      {b['ann_pct']:+7.2f} %/yr, {b['mean_positions']:.2f} positions  (share of R5 position-days not borrowable today {v2['coverage']['share_not_borrowable'] * 100:.0f} %, "
          f"{v2['coverage']['coins_not_listed']} of {v2['coverage']['coins_in_r5_episodes']} coins unlisted)")
    print(f"  + today's borrow rates               {r['ann_pct']:+7.2f} %/yr, Sharpe {r['sharpe']:.2f}, worst month {r['worst_month_pct']:+.1f} %, maxDD {r['maxdd_pct']:.1f} %, borrow cost {r['borrow_cost_ann_pct']:.2f} %/yr "
          f"(mean rate on held {r['mean_borrow_rate_on_held_ann_pct']:.1f} %/yr), break-even multiple of today's rates {r['breakeven_multiple']:.2f}x")
    print("  years %: " + "  ".join(f"{y} {x:+.2f}" for y, x in r["years_pct"].items()))
    print("  R5 top CARRY- coins today: " + ", ".join(f"{k} {v['borrow_ann'] * 100:.0f} %/yr" if v else f"{k} n/a" for k, v in v2["r5_top_coins_today"].items()))
    print(f"S2 -> {'ALIVE' if v2['alive'] else 'DEAD'}")
    json.dump(dict(fetched_ms=now_ms, S1=dict(summary=v1, rows=rows), S2=v2), open(os.path.join(HERE, "r9_results.json"), "w"), indent=1, default=float)


if __name__ == "__main__":
    main()
