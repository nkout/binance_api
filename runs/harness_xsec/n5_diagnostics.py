"""N5 post-hoc diagnostics (information only; the registered verdict is DEAD and nothing here can promote it).

Question: the median event earns +14.6 % and the mean loses to a few pumps. Would a bounded-loss version keep the drift?
  stop s   exit both legs at the first 1 h close where the coin is >= (1 + s) x its entry price (hourly closes: gaps inside the hour
           and slippage at the stop are NOT modelled, so this is optimistic)
  tail     share of events whose coin rose > 100 % / > 300 % over the hold
Any variant is a new pre-registration scored only on events after 2026-09-30 (the N5 kill rule).

    python n5_diagnostics.py   (tee n5_diagnostics.txt)
"""
import os, sys
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import listing as N
import run_n5 as R

STOPS = (0.25, 0.50, 1.00)


def stop_instant(pp, j, t0, t1, s):
    """First bar-close instant in (t0, t1] at which P >= (1 + s) P(t0); t1 when never."""
    g0 = pp["grid"][0]
    i0, i1 = int((t0 - g0) // 3600), int((t1 - g0) // 3600)          # bars closing at t0 + 1 h .. t1
    p0 = N.price(pp, j, t0)
    path = pp["cf"][i0:i1, j]
    hit = np.flatnonzero(path >= (1 + s) * p0)
    return int(g0 + (i0 + hit[0] + 1) * 3600) if len(hit) else int(t1)


def main():
    p = X.load_panels(R.ROOT, t_end=R.T_END)
    tab = N.listing_table(p, N.load_meta())
    pp = N.prep(p)
    btc = p["syms"].index("BTCUSDT")
    ev = N.build_events(pp, tab, btc, N.H_PRIMARY)
    ev = ev[ev.exit <= R.T_HI].reset_index(drop=True)
    yr = pd.to_datetime(ev.entry, unit="s").dt.year.to_numpy()
    print(f"N5 primary events {len(ev)} | coin > +100 %: {(ev.coin_ret > 1).sum()} | > +300 %: {(ev.coin_ret > 3).sum()} | "
          f"coin < -30 %: {(ev.coin_ret < -0.3).sum()}")
    print(f"registered (no stop): {R.fmt(N.stats_with_ci(ev.net.to_numpy(), ev.entry.to_numpy()))}")
    without = ev.nsmallest(5, "net")
    print(f"  without the 5 worst events: mean {ev.drop(without.index).net.mean() * 100:+.2f} % ({', '.join(without.sym)})")
    for s in STOPS:
        rows, hit = [], 0
        for e in ev.itertuples():
            t = stop_instant(pp, e.j, e.t0, e.t1, s)
            hit += t < e.t1
            x = N.event_pnl(pp, e.j, btc, e.t0, t)
            rows.append(x["short"] + x["btc"] - N.COST_PAIR)
        net = np.array(rows)
        st = N.stats_with_ci(net, ev.entry.to_numpy())
        by = pd.Series(net).groupby(yr).mean() * 100
        print(f"stop +{s:.0%}: {R.fmt(st)} | stopped {hit}")
        print("   years: " + " | ".join(f"{y} {v:+.1f}" for y, v in by.items()))


if __name__ == "__main__":
    main()


# ------------------------------------------------------------------ realistic stop: trigger on the 1 h high, fill at max(stop, bar open) + slippage
SLIP = 0.005


def intrahour_stop(pp, hi_op, j, btc, t0, t1, s):
    """Short coin / long BTC with a buy-stop at (1 + s) P(t0) triggered on the bar high; fill at max(stop, bar open) x (1 + SLIP).
    Funding up to the trigger bar's close. Returns net (incl. 18 bp) and whether the stop fired."""
    g0 = pp["grid"][0]
    i0, i1 = int((t0 - g0) // 3600), int((t1 - g0) // 3600)
    p0 = N.price(pp, j, t0)
    hi, op = hi_op[j]
    k = np.flatnonzero(hi[i0:i1] >= (1 + s) * p0)
    if not len(k):
        x = N.event_pnl(pp, j, btc, t0, t1)
        return x["short"] + x["btc"] - N.COST_PAIR, False
    i = i0 + int(k[0]); t = int(g0 + (i + 1) * 3600)
    if t > pp["death"][j]:
        x = N.event_pnl(pp, j, btc, t0, t1)
        return x["short"] + x["btc"] - N.COST_PAIR, False
    fill = max((1 + s) * p0, op[i] if np.isfinite(op[i]) else (1 + s) * p0) * (1 + SLIP)
    short = -(fill / p0 - 1) + N.fund_sum(pp, j, t0, t, True) / p0
    rb, fb, _ = N.leg(pp, btc, t0, t)
    return short + rb - fb - N.COST_PAIR, True


def realistic(pp, p, ev, btc):
    import pandas as pd
    hi_op = {}
    for j in ev.j.unique():
        k = pd.read_parquet(os.path.join(R.ROOT, "klines", f"{p['syms'][j]}.parquet"), columns=["ts", "open", "high"])
        k = k[(k.ts >= p["grid"][0]) & (k.ts <= p["grid"][-1])]
        ii = ((k.ts.to_numpy() - p["grid"][0]) // 3600).astype(np.int64)
        h = np.full(len(p["grid"]), np.nan); o = h.copy(); h[ii] = k.high.to_numpy(); o[ii] = k.open.to_numpy()
        hi_op[j] = (h, o)
    return hi_op


def main2():
    p = X.load_panels(R.ROOT, t_end=R.T_END)
    tab = N.listing_table(p, N.load_meta())
    pp = N.prep(p)
    btc = p["syms"].index("BTCUSDT")
    crypto = np.zeros(len(p["syms"]), bool); crypto[tab[~tab.cls.isin(["tradfi", "index"])].j.to_numpy()] = True
    ev = N.build_events(pp, tab, btc, N.H_PRIMARY)
    ev = ev[ev.exit <= R.T_HI].reset_index(drop=True)
    ev = N.add_control(pp, ev, btc, *N.control_sets(p, crypto))
    hi_op = realistic(pp, p, ev, btc)
    yr = pd.to_datetime(ev.entry, unit="s").dt.year.to_numpy()
    print(f"\n== intrahour stop (trigger on the 1 h high, fill at max(stop, bar open) + {SLIP:.1%}); control = registered seasoned control, no stop ==")
    for s in STOPS:
        out = [intrahour_stop(pp, hi_op, e.j, btc, e.t0, e.t1, s) for e in ev.itertuples()]
        net = np.array([o[0] for o in out]); fired = sum(o[1] for o in out)
        st = N.stats_with_ci(net, ev.entry.to_numpy())
        dc = net - ev.ctrl_net.to_numpy()
        sc = N.stats_with_ci(dc, ev.entry.to_numpy())
        by = pd.Series(net).groupby(yr).mean() * 100
        byc = pd.Series(dc).groupby(yr).mean() * 100
        print(f"stop +{s:.0%}: {R.fmt(st)} | fired {fired}")
        print(f"   minus control: {R.fmt(sc)}")
        print("   years net: " + " | ".join(f"{y} {v:+.1f}" for y, v in by.items()) + "   | minus control: "
              + " | ".join(f"{y} {v:+.1f}" for y, v in byc.dropna().items()))


if __name__ == "__main__" and len(sys.argv) > 1 and sys.argv[1] == "realistic":
    main2()
