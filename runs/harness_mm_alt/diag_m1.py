"""M1 diagnostics (information only, not part of the verdict): mark-out decomposition by cohort, horizon, day and spread tercile.

    python diag_m1.py [--derived ../../data/mm_alt]
"""
import argparse, os, sys
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import mmalt as M
from run_m1 import load_rows, DEFAULT_DERIVED


def frame(rows):
    out = []
    for r in rows:
        d = dict(symbol=r["symbol"], day=r["day"], cohort=r["cohort"], n_tr=r["n_tr"], coverage=r["coverage"], qs=r["qs_median_bp"],
                 capture=r["mk"]["capture_bp"], e=r["hs"]["e_bp"], ofi_r2=r["ofi"]["r2"],
                 ident=bool(r["hs"]["identity"] and r["mk"]["identity"]))
        for t in map(str, M.TAUS):
            d[f"adv{t}"] = r["mk"]["adverse_bp"][t]; d[f"net{t}"] = r["mk"]["net_bp"][t]; d[f"naf{t}"] = r["mk"]["net_after_fee_bp"][t]
        out.append(d)
    return pd.DataFrame(out)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--derived", default=DEFAULT_DERIVED); a = ap.parse_args()
    df = frame(load_rows(a.derived))
    print(f"rows {len(df)} | identities hold on {df.ident.mean():.0%} | coverage median {df.coverage.median():.3f}, "
          f"< 0.9 on {(df.coverage < 0.9).sum()} rows")
    taus = [str(t) for t in M.TAUS]
    for c, g in df.groupby("cohort"):
        print(f"\n== {c}: {len(g)} symbol-days, {g.symbol.nunique()} symbols, days {sorted(g.day.unique())}")
        print(f"  median quoted spread {g.qs.median():.2f} bp (mean of medians {g.qs.mean():.2f}); half-spread capture mean {g.capture.mean():+.2f}, "
              f"effective half-spread {g.e.mean():+.2f}; OFI R2 {g.ofi_r2.mean():.3f}")
        print("  tau   adverse   net(before fee)   net after fee    share of symbol-days > 0 (after fee / before)")
        for t in taus:
            print(f"  {t:>3}s  {g[f'adv{t}'].mean():+7.2f}   {g[f'net{t}'].mean():+7.2f} (med {g[f'net{t}'].median():+6.2f})   "
                  f"{g[f'naf{t}'].mean():+7.2f} (med {g[f'naf{t}'].median():+6.2f})   {(g[f'naf{t}'] > 0).mean():.0%} / {(g[f'net{t}'] > 0).mean():.0%}")
        print(f"  break-even maker fee at 5 s (net before fee): {g['net5'].mean():+.2f} bp per side (VIP0 maker {M.MAKER_BP} bp, rebate {M.REBATE_BP})")
        print("  per day (net after fee 5 s, mean):  " + " | ".join(f"{d} {v:+.2f}" for d, v in g.groupby("day")["naf5"].mean().items()))
        q = g.qs.quantile([1 / 3, 2 / 3]).to_numpy()
        tb = np.digitize(g.qs, q)
        print("  by quoted-spread tercile:")
        for k, lab in enumerate(("narrow", "mid", "wide")):
            h = g[tb == k]
            if len(h):
                print(f"    {lab:6s} qs {h.qs.min():5.1f}-{h.qs.max():6.1f} bp | capture {h.capture.mean():+6.2f} | adverse(5s) {h.adv5.mean():+6.2f} | "
                      f"net(5s) {h.net5.mean():+6.2f} | after fee {h.naf5.mean():+6.2f} | > 0 after fee {(h.naf5 > 0).mean():.0%}")
        top = g.nlargest(5, "naf5")[["symbol", "day", "qs", "capture", "adv5", "naf5", "n_tr"]]
        print("  best 5 symbol-days (net after fee 5 s):")
        print("    " + top.round(2).to_string(index=False).replace("\n", "\n    "))


if __name__ == "__main__":
    main()
