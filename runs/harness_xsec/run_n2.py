"""N2 XFUND cross-venue funding spread (Binance vs Bybit) — real run (pre-registered in next_signal_ideas.md, Round 6 / N2).

    python fetch_bybit.py   (once)
    python run_n2.py        -> prints the report (tee to n2_output.txt) and writes n2_results.json

Registered window: first day with >= 20 coins in the set -> 2026-08-30. Days from 2026-08-31 are the forward block.
Tests: test_xfund.py.
"""
import json, os, sys, time
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import xfund as XF

ROOT = os.path.join(HERE, "..", "..", "data", "xsec")
BYBIT = os.path.join(HERE, "..", "..", "data", "bybit")
T_HI = int(pd.Timestamp("2026-08-31", tz="UTC").timestamp())
T_END = "2026-10-01"


def main():
    t0 = time.time()
    pb = X.load_panels(ROOT, t_end=T_END)
    py = XF.load_bybit(BYBIT, pb)
    mp = json.load(open(os.path.join(BYBIT, "map.json")))
    print(f"binance {len(pb['syms'])} symbols | bybit files aligned {int(np.isfinite(py['close']).any(0).sum())} | "
          f"unmapped top-100-ever: {len(mp['unmapped'])} {mp['unmapped']} | {time.time() - t0:.0f}s")
    _, _, cut = XF.close_at_common_death(pb, py)
    print(f"delisting rule: {len(cut)} coins closed at the last common price "
          f"(e.g. {', '.join(list(cut)[:8])})")
    B = XF.build(pb, py)
    days = B["days"]
    idx = XF.window(B["elig"], days, T_HI)
    mem = B["member"][idx]
    print(f"window {pd.to_datetime(days[idx[0]], unit='s').date()} -> {pd.to_datetime(days[idx[-1]], unit='s').date()} ({len(idx)} days)")
    print(f"coverage of Binance top-100 coin-days in the window: Bybit perp exists {(mem & B['has_by'][None, :]).sum() / mem.sum():.1%} | "
          f"in the set (filters incl. 2 M Bybit volume) {B['elig'][idx].sum() / mem.sum():.1%} | mean set size {B['elig'][idx].sum(1).mean():.1f}")
    pos = XF.simulate(B["elig"], B["delta"])
    P0 = XF.pnl(pos, B["TRb"], B["TRy"], B["Fb"], B["Fy"])
    P1 = XF.pnl(pos, B["TRb1"], B["TRy1"], B["Fb"], B["Fy"])
    npos = (pos[idx] != 0).sum(1)
    s, full_years, crit = XF.criteria(P0["net"], P1["net"], days, idx, float(npos.mean()))
    eps = [e for e in XF.episodes(pos) if idx[0] <= e[2] <= idx[-1]]
    hold = np.array([b - a + 1 for _, _, a, b in eps])
    stress = XF.margin_stress(eps, days, pb["grid"], pb["close"], py["close"], B["Pb"], B["Py"])
    net = P0["net"][idx]
    print("\n===== N2 XFUND (registered) =====")
    print(f"  annualised net {s['ann_pct']:+.2f} % on capital | NW t {s['nw_t']:.2f} | Sharpe {s['sharpe']:.2f} | max DD {s['maxdd_pct']:.2f} pp | "
          f"worst month {s['worst_month_pct']:+.2f} % | 1 h lag net {P1['net'][idx].mean() * 365 * 100:+.2f} %/yr")
    print("  years %: " + " | ".join(f"{y}: {v:+.2f}{'' if y in full_years else ' (partial, < 6 months)'}" for y, v in s["years_pct"].items()))
    split = {k: float(P0[k][idx].mean() * 365 * 100) for k in ("funding", "basis", "cost", "gross", "net")}
    print("  P&L split %/yr: " + " | ".join(f"{k} {v:+.2f}" for k, v in split.items()))
    print(f"  positions: mean open {npos.mean():.2f} | days with >= 1 open {(npos > 0).mean():.0%} | days at 10 {(npos == 10).mean():.0%} | "
          f"episodes {len(eps)} | median hold {np.median(hold) if len(hold) else float('nan'):.0f} d (mean {hold.mean() if len(hold) else float('nan'):.1f}) | "
          f"trades/yr {P0['trades'][idx].sum() / len(idx) * 365:.0f}")
    q = np.sort(net); k = max(1, int(0.01 * len(q)))
    print(f"  daily net: mean {net.mean() * 1e4:+.2f} bp | median {np.median(net) * 1e4:+.2f} bp | worst {q[0] * 100:+.2f} % | worst 1 % mean {q[:k].mean() * 100:+.2f} % | "
          f"best {q[-1] * 100:+.2f} %")
    held = pos[idx] != 0
    thin = np.minimum(B["qv30_bin"][idx], B["qv30_by"][idx])[held]
    print(f"  capacity: thinner venue's 30-day mean daily volume per held coin-day: median ${np.median(thin) / 1e6:.1f} M "
          f"(10 % quantile ${np.percentile(thin, 10) / 1e6:.1f} M)" if held.any() else "  capacity: no positions")
    print(f"  margin stress: {stress} of {len(eps)} episodes had a leg > {XF.STRESS:.0%} against entry")
    for kk, v in crit.items():
        print(f"    [{'x' if v else ' '}] {kk}")
    verdict = "PASS" if all(crit.values()) else "FAIL"
    print(f"  VERDICT N2: {verdict}")

    ad = np.abs(B["delta"][idx])[B["elig"][idx]]
    print(f"\n  info: |delta7| over set coin-days: median {np.median(ad) * 100:.4f} % | share >= 0.03 % {(ad >= XF.ENTER).mean():.2%} | "
          f">= 0.01 % {(ad >= XF.EXIT).mean():.1%}")
    yr = pd.to_datetime(days[idx], unit="s").year
    print("  info: mean open positions by year: " + " | ".join(f"{y}: {npos[yr == y].mean():.2f}" for y in np.unique(yr)))
    top = pd.Series([pb["syms"][j] for j, _, _, _ in eps]).value_counts().head(10)
    print("  info: most traded coins (episodes): " + ", ".join(f"{a} {b}" for a, b in top.items()))

    fidx = np.arange(np.searchsorted(days, T_HI), len(days) - 1)
    fwd = {}
    if len(fidx):
        fn = P0["net"][fidx]
        fwd = dict(days=len(fidx), net_pct=float(fn.sum() * 100), ann_pct=float(fn.mean() * 365 * 100),
                   mean_pos=float((pos[fidx] != 0).sum(1).mean()))
        print(f"\n===== FORWARD BLOCK from {pd.to_datetime(days[fidx[0]], unit='s').date()} ({len(fidx)} days, unseen) =====")
        print(f"  net {fwd['net_pct']:+.3f} % ({fwd['ann_pct']:+.2f} %/yr) | mean open positions {fwd['mean_pos']:.2f}")
    res = dict(window=[str(pd.to_datetime(days[idx[0]], unit="s").date()), str(pd.to_datetime(days[idx[-1]], unit="s").date()), len(idx)],
               summary=s, full_years=full_years, split_pct_yr=split, criteria={k: bool(v) for k, v in crit.items()}, verdict=verdict,
               mean_pos=float(npos.mean()), episodes=len(eps), median_hold=float(np.median(hold)) if len(hold) else None,
               margin_stress=stress, forward=fwd, unmapped=mp["unmapped"])
    json.dump(res, open(os.path.join(HERE, "n2_results.json"), "w"), indent=1, default=float)
    print(f"\n({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
