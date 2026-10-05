"""V1 forward scoring of the frozen DVOL rule (next_signal_ideas.md, 'V1 forward scoring'). Local CPU, seconds.

    python forward_score.py          # refreshes nothing: run fetch_klines.py / fetch_dvol.py first
Writes forward_ledger.csv and forward_status.txt next to this file.
"""
import json, os, sys
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import vrp as V
DATA = os.path.join(HERE, "..", "..", "data")
CUT = pd.Timestamp("2026-08-23")
FRICTION = 2.0


def main():
    kl = pd.read_pickle(os.path.join(DATA, "btcusdt_5m_klines.pkl")); kl = kl[kl.index >= int(pd.Timestamp("2020-01-01", tz="UTC").timestamp())]
    dv = pd.read_parquet(os.path.join(DATA, "dvol_daily.parquet"))
    var = V.daily_variance(kl.index.to_numpy(), kl.close.to_numpy())
    f = V.features(var); fc = V.har_forecast(f); iv = V.align_iv(dv.ts.to_numpy(), dv.close.to_numpy(), f.index)
    last_full = var.dropna().index[-1]                                           # last complete UTC day
    lines = [f"klines to {pd.to_datetime(kl.index.max(), unit='s')} UTC | last complete day {last_full.date()} | DVOL to {pd.to_datetime(dv.ts.iloc[-1], unit='s').date()}"]

    # regression check: the frozen pipeline reproduces V1 on entry days <= CUT
    ref = json.load(open(os.path.join(HERE, "vrp_results.json")))
    m = f.index <= CUT
    res = V.analyse(f.index[m], iv.to_numpy()[m], f.rv.to_numpy()[m], fc.to_numpy()[m], f.naive.to_numpy()[m], boot=200)
    ok = (res["n"] == ref["n"] and abs(res["P1_har"]["slope"] - ref["P1_har"]["slope"]) < 1e-9
          and abs(res["P2"]["veto_short"]["f2"]["mean"] - ref["P2"]["veto_short"]["f2"]["mean"]) < 1e-9)
    lines.append(f"regression check vs vrp_results.json (n {res['n']}, slope {res['P1_har']['slope']:+.4f}, veto-short f=2 {res['P2']['veto_short']['f2']['mean']:+.3f}): {'OK' if ok else 'MISMATCH'}")
    if not ok:
        print("\n".join(lines)); raise SystemExit("frozen pipeline no longer reproduces V1; do not score")

    d = pd.DataFrame(dict(iv=iv, har_f=fc, naive=f.naive, rv=f.rv))
    d = d[d.index > CUT].copy()
    d = d[np.isfinite(d.iv) & np.isfinite(d.har_f)]
    d["pos"] = np.where(d.har_f < d.iv, -1.0, 0.0)
    d["status"] = np.where(np.isfinite(d.rv), "complete", "open")
    elapsed = (last_full - d.index).days + 1                                     # complete days since entry
    d["days_elapsed"] = np.clip(elapsed, 0, V.HORIZON)
    togo = {}
    for t in d.index[d.status == "open"]:
        w = var[t:last_full]
        togo[t] = 100 * np.sqrt(V.ANN * w.mean()) if len(w) and w.notna().all() else np.nan
    d["rv_to_date"] = pd.Series(togo)
    d["pnl_veto_f2"] = np.where(d.status == "complete", d.pos * (d.rv - d.iv) - FRICTION * np.abs(d.pos), np.nan)
    d["pnl_always_f2"] = np.where(d.status == "complete", -(d.rv - d.iv) - FRICTION, np.nan)
    d.index.name = "entry_day"
    d.round(3).to_csv(os.path.join(HERE, "forward_ledger.csv"))

    c = d[d.status == "complete"]
    lines.append(f"forward entry days {len(d)} ({d.index[0].date()} -> {d.index[-1].date()}): complete {len(c)}, open {len(d) - len(c)}")
    lines.append(f"signal: veto-short active on {int((d.pos != 0).sum())} of {len(d)} days (F < IV); latest {d.index[-1].date()}: IV {d.iv.iloc[-1]:.1f}, HAR F {d.har_f.iloc[-1]:.1f}, "
                 f"position {'SHORT vol' if d.pos.iloc[-1] < 0 else 'flat'}")
    if len(c):
        lines.append(f"completed (n {len(c)}): mean IV {c.iv.mean():.1f}, mean RV {c.rv.mean():.1f}, VRP {c.iv.mean() - c.rv.mean():+.1f} | veto-short mean f=2 {c.pnl_veto_f2.mean():+.2f} "
                     f"({int((c.pos != 0).sum())} active) | always-short mean f=2 {c.pnl_always_f2.mean():+.2f}")
        if len(c) >= 20:
            x = np.log(c.har_f / c.iv); y = np.log(c.rv / c.iv)
            b, se, t = V.ols_nw(y.to_numpy(), np.column_stack([np.ones(len(c)), x.to_numpy()]), 30)
            lines.append(f"forward P1 slope {b[1]:+.3f} (NW t {t[1]:.2f}, n {len(c)} overlapping days: indicative only)")
        else:
            lines.append(f"forward P1 slope not computed (n {len(c)} < 20)")
    o = d[d.status == "open"]
    if len(o):
        lines.append("open windows (realised so far vs IV): " + ", ".join(f"{t.date()} IV {r.iv:.0f} RV-to-date {r.rv_to_date:.0f} ({int(r.days_elapsed)}d)" for t, r in o.iterrows() if np.isfinite(r.rv_to_date))[:900])
    lines.append(f"verdict: none until >= 180 completed forward entry days (have {len(c)})")
    txt = "\n".join(lines); print(txt); open(os.path.join(HERE, "forward_status.txt"), "w").write(txt + "\n")


if __name__ == "__main__":
    main()
