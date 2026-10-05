"""V1 — run the pre-registered DVOL vs forward realised-vol test (next_signal_ideas.md, Round 5). Local CPU, seconds.

    python run_vrp.py [--out vrp_results.json]
"""
import argparse, json, os, sys
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import vrp as V
DATA = os.path.join(HERE, "..", "..", "data")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--out", default=os.path.join(HERE, "vrp_results.json")); a = ap.parse_args()
    kl = pd.read_pickle(os.path.join(DATA, "btcusdt_5m_klines.pkl"))
    kl = kl[kl.index >= int(pd.Timestamp("2020-01-01", tz="UTC").timestamp())]
    dv = pd.read_parquet(os.path.join(DATA, "dvol_daily.parquet"))
    print(f"klines {len(kl):,} 5-min bars to {pd.to_datetime(kl.index[-1], unit='s')} | DVOL {len(dv)} days to {pd.to_datetime(dv.ts.iloc[-1], unit='s').date()}")
    res = V.run(kl.index.to_numpy(), kl.close.to_numpy(), dv.ts.to_numpy(), dv.close.to_numpy())
    json.dump(res, open(a.out, "w"), indent=1, default=float)
    p0 = res["P0"]
    print(f"\nsample {res['first']} -> {res['last']}, n = {res['n']} entry days")
    print(f"P0  mean IV {p0['mean_iv']:.1f}  mean RV {p0['mean_rv']:.1f}  VRP {p0['vrp_mean']:+.2f} vol pts (NW t {p0['vrp_nw_t']:.2f}) | median {p0['vrp_median']:+.2f} | "
          f"IV > RV on {p0['frac_positive'] * 100:.0f} % of days | non-overlapping n {p0['nonoverlap_n']}: {p0['nonoverlap_mean']:+.2f} (t {p0['nonoverlap_t']:.2f}) | worst 1 % {p0['worst_1pct']:+.1f}")
    for k in ("P1_har", "P1_naive"):
        q = res[k]; print(f"{k}: slope {q['slope']:+.3f} (se {q['slope_se']:.3f}, NW t {q['slope_nw_t']:.2f}), intercept {q['intercept']:+.3f}, corr {q['corr']:+.3f}, sd log(F/IV) {q['x_sd']:.3f}")
    print("\nP2 (vol points per entry day; flat days count 0)")
    for nm, r in res["P2"].items():
        print(f"  {nm:17s} " + " | ".join(f"f={f[1:]}: {r[f]['mean']:+.2f} (t {r[f]['nw_t']:.1f})" for f in ("f0", "f1", "f2", "f3"))
              + f" | active {r['active_pct']:.0f} % | per active (f=2) mean {r['mean_active_f2']:+.2f} median {r['median_active_f2']:+.2f} | worst 1 % {r['worst_1pct_f2']:+.1f}, worst {r['worst']:+.1f}"
              + f" | CI(f=2) {np.round(r['ci_f2'], 2).tolist()} | lag-1d {r['lag1_f2']:+.2f}")
        print("      years (f=2): " + "  ".join(f"{y} {v:+.1f}" for y, v in r["years_f2"].items()))
    d = res["veto_minus_always_f2"]; print(f"\nveto-short minus always-short (f=2): {d['mean']:+.2f}, CI {np.round(d['ci'], 2).tolist()}")
    for k, c in res["criteria"].items():
        print(f"  {'PASS' if c['ok'] else 'FAIL'} {k}: {c['detail']}")


if __name__ == "__main__":
    main()
