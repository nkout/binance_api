"""N4-X minute-scale execution check — real run (pre-registered in next_signal_ideas.md, "N4 follow-up").

    python run_n4x.py       -> downloads daily 1 m klines for the sampled coin-days to data/xsec/min1/, prints the report
                               (tee to n4x_output.txt), writes n4x_results.json and n4x_sample.csv
Tests: test_n4x.py.
"""
import json, os, sys, time
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import settle as S
import n4x as N
from run_n4 import ROOT, T_HI, T_END

MIN_ROOT = os.path.join(ROOT, "min1")
LO = int(pd.Timestamp("2025-01-01", tz="UTC").timestamp())


def ci_row(x, cl, label):
    ok = np.isfinite(x); b = S.cluster_boot(x[ok], cl[ok]); d = S.describe(x[ok])
    print(f"  {label:34s} mean {b['mean']:7.2f} [{b['lo']:7.2f}, {b['hi']:7.2f}] | median {d['median']:7.2f} | "
          f"trimmed {d['trimmed']:7.2f} | n {d['n']}")
    return b, d


def main():
    t0 = time.time()
    p = X.load_panels(ROOT, t_end=T_END)
    e, *_ = S.build_events(p)
    smp = N.sample_events(e, LO, T_HI)
    print(f"pool {((e.s >= LO) & (e.s < T_HI)).sum()} registered events 2025-01-01 .. 2026-08-30 -> sample {len(smp)} "
          f"({smp.sym.nunique()} coins, {smp.s.nunique()} settlement times) | {time.time() - t0:.0f}s")
    pairs = N.needed_days(smp)
    st = N.fetch_days(pairs, MIN_ROOT)
    print(f"1 m coin-days needed {len(pairs)}: {st} | {time.time() - t0:.0f}s")
    days_by = pd.Series([d for _, d in pairs]).groupby([s for s, _ in pairs]).apply(list)
    minutes = {s: N.load_minutes(MIN_ROOT, s, ds) for s, ds in days_by.items()}
    t, paths = N.trade_table(smp, minutes)
    s_arr = t.s.to_numpy()

    hs = S.close_at(p, smp.j.to_numpy(), smp.s.to_numpy() + 3600); h0 = S.close_at(p, smp.j.to_numpy(), smp.s.to_numpy())
    rd1 = np.abs(t.exit / hs - 1); rd0 = np.abs(t.p_s / h0 - 1)
    print(f"reconciliation with the 1 h archive: P(s+1h) mismatches > 1e-9: {(rd1 > 1e-9).sum()}, P(s): {(rd0 > 1e-9).sum()} "
          f"(of {np.isfinite(rd1).sum()} / {np.isfinite(rd0).sum()} comparable)")

    print(f"\n=== REGISTERED: entry O(s + {N.ENTRY_K} min), exit P(s + 1h), cost {N.COST_ALL_BP} bp all-in ===")
    v = N.verdict(t)
    b = v["boot"]; d = v["desc"]
    print(f"  net {b['mean']:.2f} bp  95% CI [{b['lo']:.2f}, {b['hi']:.2f}]  p {b['p']:.4f} | median {d['median']:.2f} | trimmed {d['trimmed']:.2f} "
          f"| worst {d['worst']:.0f} | worst-1% mean {d['worst1pct_mean']:.0f} | n {v['n_used']} (missing {v['n_missing']})")
    for k, c in v["criteria"].items():
        print(f"    [{'x' if c else ' '}] {k}")
    print(f"  VERDICT N4-X: {'PASS' if v['pass_'] else 'FAIL'}")

    print("\n=== Information (gross bp unless noted) ===")
    ci_row(t.post_1h.to_numpy(), s_arr, "sample, N4 1 h POST gross")
    for k in (1, 2, 5, 15):
        ci_row(t[f"g{k}"].to_numpy(), s_arr, f"entry s + {k} min gross")
    ci_row(t[f"g{N.ENTRY_K}"].to_numpy() - 9.0, s_arr, "entry s + 2 min, fee only (9 bp) net")
    print("\n  mean signed path from P(s) (bp, paying direction), clustered CI:")
    for k in (1, 2, 3, 5, 10, 15, 30, 45, 60, 75, 90, 120):
        ci_row(paths[:, k], s_arr, f"path({k:3d} min)")
    q = t.q.to_numpy()
    print("\n  by funding sign (registered net):")
    for lab, m in (("rate<0 (short)", q > 0), ("rate>0 (long)", q < 0)):
        ci_row(t.net.to_numpy()[m], s_arr[m], lab)
    yr = pd.to_datetime(t.s, unit="s").dt.year.to_numpy()
    for y in np.unique(yr):
        ci_row(t.net.to_numpy()[yr == y], s_arr[yr == y], f"year {y}")

    smp.assign(**{c: t[c] for c in ("g1", "g2", "g5", "g15", "net", "exit", "p_s")}).to_csv(os.path.join(HERE, "n4x_sample.csv"), index=False)
    res = dict(sample=len(smp), fetch=st, verdict="PASS" if v["pass_"] else "FAIL", boot=b, desc=d,
               criteria={k: bool(c) for k, c in v["criteria"].items()}, n_missing=v["n_missing"],
               path_mean={int(k): float(np.nanmean(paths[:, k])) for k in range(N.PATH_K + 1)})
    json.dump(res, open(os.path.join(HERE, "n4x_results.json"), "w"), indent=1, default=float)
    print(f"\n({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
