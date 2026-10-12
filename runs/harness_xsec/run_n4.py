"""N4 funding-settlement event study — real run (pre-registered in next_signal_ideas.md, Round 6 / N4).

    python run_n4.py            -> prints the report (tee to n4_output.txt) and writes n4_results.json

Registered window: settlements 2021-05-22 .. 2026-08-30. Settlements from 2026-08-31 on are the forward block
(descriptive only). Tests: test_settle.py.
"""
import json, os, sys, time
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import settle as S

ROOT = os.path.join(HERE, "..", "..", "data", "xsec")
T_LO = int(pd.Timestamp("2021-05-22", tz="UTC").timestamp())
T_HI = int(pd.Timestamp("2026-08-31", tz="UTC").timestamp())          # registered: s < 2026-08-31 00:00
T_END = "2026-10-01"


def fmt_desc(d):
    return (f"n {d['n']:6d} | mean {d['mean']:7.2f} | median {d['median']:7.2f} | trimmed {d['trimmed']:7.2f} | "
            f"worst {d['worst']:8.1f} | q01 {d['q01']:7.1f} | worst-1% mean {d['worst1pct_mean']:7.1f}")


def report_arms(e, title, seed=0):
    print(f"\n=== {title} ===")
    arms = S.verdict(e, seed=seed)
    for a in arms:
        bn, bd = a["boot_net"], a["boot_diff"]
        print(f"\n-- {a['arm'].upper()}  ->  {'PASS' if a['pass'] else 'FAIL'}")
        print(f"  net (bp)          {bn['mean']:7.2f}  95% CI [{bn['lo']:6.2f}, {bn['hi']:6.2f}]  p {bn['p']:.4f}  "
              f"(n {bn['n']}, {bn['clusters']} settlement clusters)")
        print(f"  event - placebo   {bd['mean']:7.2f}  95% CI [{bd['lo']:6.2f}, {bd['hi']:6.2f}]  p {bd['p']:.4f}")
        print(f"  Holm p (arm)      {a['holm_p']:.4f}")
        print(f"  net      {fmt_desc(a['desc_net'])}")
        print(f"  placebo  {fmt_desc(a['desc_placebo'])}   (placebo net of 9 bp)")
        print("  years    " + " | ".join(f"{y}: {v['mean']:6.2f} (n {v['n']})" for y, v in a["years"].items())
              + f"   -> {a['pos_years']}/{a['n_years']} positive")
        print("  sign legs " + " | ".join(f"{k}: {v:6.2f}" for k, v in a["sign_legs"].items()))
        for k, v in a["criteria"].items():
            print(f"    [{'x' if v else ' '}] {k}")
    return arms


def jsonable(a):
    return {k: (jsonable(v) if isinstance(v, dict) else (bool(v) if isinstance(v, (bool, np.bool_)) else v))
            for k, v in a.items()}


def main():
    t0 = time.time()
    p = X.load_panels(ROOT, t_end=T_END)
    print(f"loaded {len(p['syms'])} symbols, {len(p['grid'])} hours, {time.time() - t0:.0f}s")
    offs = np.concatenate([ft - (ft + 1800) // 3600 * 3600 for ft, _ in p["fund"].values() if len(ft)])
    print(f"funding calc_time minus rounded hour: min {offs.min()} s, max {offs.max()} s, "
          f"share |off| > 60 s: {(np.abs(offs) > 60).mean():.5f}")
    e_all, dropped, U, days = S.build_events(p, top=S.TOP_EV)
    print(f"extreme events in universe: {len(e_all) + dropped} ({dropped} dropped for a missing bar) | {time.time() - t0:.0f}s")
    first_u = days[np.flatnonzero(U[:, 0] >= 0)[0]]
    print(f"first day with a top-{S.TOP_EV} universe: {pd.to_datetime(first_u, unit='s').date()}")
    reg = e_all[(e_all.s >= T_LO) & (e_all.s < T_HI)].reset_index(drop=True)
    fwd = e_all[e_all.s >= T_HI].reset_index(drop=True)

    arms = report_arms(reg, f"REGISTERED  {pd.to_datetime(T_LO, unit='s').date()} .. 2026-08-30  "
                            f"(|r_prev| >= {S.EXTREME:.2%}, cost {S.COST_RT_BP} bp RT)")

    print("\n=== Blind / drift: unsigned mean raw return (bp) over the same bars ===")
    print(f"  PRE bar {reg.raw_pre.mean():6.2f} | POST bar {reg.raw_post.mean():6.2f} | "
          f"placebo PRE bar {reg.raw_plc_pre.mean():6.2f} | placebo POST bar {reg.raw_plc_post.mean():6.2f}")

    print("\n=== Splits (gross / net / placebo / event - placebo, bp) ===")
    reg = reg.assign(rate_sign=np.where(reg.q < 0, "rate>0", "rate<0"),
                     interval=reg.interval_h.map(lambda h: f"{h}h" if h in (1, 4, 8) else "other"))
    for arm in ("pre", "post"):
        for by in ("rate_sign", "interval", "year"):
            print(f"\n{arm.upper()} by {by}\n" + S.split_table(reg, arm, by).round(2).to_string())

    print("\n=== Lag row ===\n  PRE: its lag row is the placebo (see above). POST is PRE entered one hour later.")

    ei, *_ = S.build_events(p, top=S.TOP_EV, sign_by="settled")
    ei = ei[(ei.s >= T_LO) & (ei.s < T_HI)].reset_index(drop=True)
    print("\n=== INFORMATION: signed by the settled rate r_s (not causal, not tradeable) ===")
    for arm in ("pre", "post"):
        print(f"  {arm.upper()}  net {fmt_desc(S.describe(ei[f'{arm}_net']))}   placebo gross {ei[f'plc_{arm}'].mean():6.2f}")

    print("\n=== FORWARD BLOCK (settlements from 2026-08-31, seen by no study; descriptive) ===")
    for arm in ("pre", "post"):
        if len(fwd):
            d = S.describe(fwd[f"{arm}_net"])
            print(f"  {arm.upper()}  net {fmt_desc(d)}   event - placebo {(fwd[arm] - fwd[f'plc_{arm}']).mean():6.2f}")
    print(f"  forward events {len(fwd)}, last settlement {pd.to_datetime(e_all.s.max(), unit='s')}")

    res = dict(window=[T_LO, T_HI], n_registered=len(reg), n_forward=len(fwd), dropped=dropped,
               arms=[jsonable(a) for a in arms],
               verdict="PASS" if any(a["pass"] for a in arms) else "FAIL (both arms) -> N4 dead",
               forward={arm: S.describe(fwd[f"{arm}_net"]) for arm in ("pre", "post")} if len(fwd) else {})
    json.dump(res, open(os.path.join(HERE, "n4_results.json"), "w"), indent=1, default=float)
    print(f"\nVERDICT: {res['verdict']}   ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
