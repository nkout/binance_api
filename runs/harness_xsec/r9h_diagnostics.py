"""R9-H post-verdict diagnostics (information only; they change no verdict). Run after run_r9h.py score.

1. Snapshot cross-check: the registered check compared the 2026-10-05 public snapshot with the day mean; compare it with the rate in effect
   at the snapshot's fetch time instead (does the failure come from intraday rate moves or from a mapping / unit error?).
2. Concentration of row (a): the top coins' holdings, funding vs basis, rate while held, and the net without them.
"""
import gzip, json, os, sys
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import run_r9h as RUN, r9h as H, carry as C


def xcheck():
    snap = json.load(gzip.open(RUN.SNAP_XCHECK))
    t_snap = int(np.datetime64(snap["fetched_utc"][:26], "us").astype("datetime64[ms]").astype(np.int64))
    d0 = H.ms("2026-10-05"); dm, pt = [], []
    for a in json.load(open(RUN.ASSETS))["assets"]:
        rec = H.load_records(RUN.CACHE, a)
        if rec is None or a not in snap["assets"] or snap["assets"][a]["daily"] <= 0:
            continue
        t, v, _ = rec; s = snap["assets"][a]["daily"]
        k = np.searchsorted(t, t_snap, "right") - 1; m = (t >= d0) & (t < d0 + H.DAY_MS)
        if k >= 0 and m.any():
            dm.append(abs(v[m].mean() / s - 1)); pt.append(abs(v[k] / s - 1))
    dm, pt = np.array(dm), np.array(pt)
    print(f"1. snapshot {snap['fetched_utc']}: {len(pt)} assets | within 10 %: day mean {np.mean(dm <= .1) * 100:.1f} %, rate in effect at the fetch time "
          f"{np.mean(pt <= .1) * 100:.1f} % | exact (< 0.1 %) {np.mean(pt < .001) * 100:.1f} % | median deviation: day mean {np.median(dm) * 100:.1f} %, at time {np.median(pt) * 100:.2f} %")


def concentration(top=("ALPACAUSDT", "DEXEUSDT", "FTTUSDT")):
    P = RUN.panels(); days, elig, fz, bases, syms = P["days"], P["elig"], P["fz"], P["bases"], P["p"]["syms"]
    fetched = json.load(open(RUN.ASSETS))["assets"]
    r, ex, am, _ = H.borrow_matrices([b if b in fetched else None for b in bases], RUN.CACHE, days)
    idx = np.arange(P["start"], P["end"] + 1); L0 = P["L0"]
    res = H.reprice_tv(elig, fz, *L0, am, r); pos = res["restricted"]["pos"]
    unit = L0[0] - L0[1] - L0[2]; yr = pd.to_datetime(days, unit="s", utc=True).year.values
    daily_j = C.N_SLOT * np.where(pos, unit - np.nan_to_num(r), 0.0)
    print("2. concentration of row (a) (pp = percentage points of capital, cumulative):")
    for s in top:
        j = syms.index(s); h = np.flatnonzero(pos[idx, j]) + idx[0]
        by = pd.Series(daily_j[idx, j]).groupby(yr[idx]).sum() * 100
        t, v, _ = H.load_records(RUN.CACHE, bases[j])
        lo = int(days[h[0]]) * 1000; hi = int(days[h[-1]]) * 1000 + H.DAY_MS; mm = (t >= lo) & (t < hi)
        print(f"   {s}: held {len(h)} d, {pd.to_datetime(days[h[0]], unit='s').date()} -> {pd.to_datetime(days[h[-1]], unit='s').date()} | by year "
              + " ".join(f"{y}:{x:+.1f}" for y, x in by.items() if abs(x) > 0.05)
              + f" | funding {C.N_SLOT * (-L0[2][h, j]).sum() * 100:+.1f} pp, basis {C.N_SLOT * (L0[0][h, j] - L0[1][h, j]).sum() * 100:+.1f} pp,"
              f" borrow {C.N_SLOT * np.nan_to_num(r[h, j]).sum() * 100:.1f} pp | rate while held mean {np.nanmean(r[h, j]) * 365 * 100:.0f} %/yr,"
              f" max {np.nanmax(r[h, j]) * 365 * 100:.0f} %/yr, {len(np.unique(v[mm]))} distinct values in {mm.sum()} records")
    for drop in ([top[0]], list(top)):
        e2 = elig.copy()
        for s in drop:
            e2[:, syms.index(s)] = False
        sm = C.summarize(H.reprice_tv(e2, fz, *L0, am, r)["net"], days, idx)
        print(f"   without {', '.join(drop)}: {sm['ann_pct']:+.2f} %/yr, NW t {sm['nw_t']:.2f}, worst month {sm['worst_month_pct']:+.2f} %, years "
              + " ".join(f"{y}:{x:+.2f}" for y, x in sm["years_pct"].items()))


if __name__ == "__main__":
    xcheck(); concentration()
