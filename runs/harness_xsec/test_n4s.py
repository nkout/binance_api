"""Tests for the N4-S harness: readers, extraction, fill-side logic, timing rules, identity, leak."""
import io, os, sys, zipfile
import numpy as np
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import n4s as N
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


def zcsv(rows, header=True):
    buf = io.StringIO()
    if header:
        buf.write(",".join(N.COLS) + "\n")
    for r in rows:
        buf.write(",".join(str(x) for x in r) + "\n")
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr("X-aggTrades-2025-01-01.csv", buf.getvalue())
    return out.getvalue()


rows = [(1, 10.0, 1, 1, 1, 2000, "true"), (2, 10.5, 1, 2, 2, 1000, "false"), (3, 11.0, 2, 3, 3, 3000, "false")]
for hdr in (True, False):
    t, p, ibm = N.read_agg(zcsv(rows, hdr))
    check(f"read_agg ({'header' if hdr else 'no header'}): columns, types, sorted by time",
          list(t) == [1000, 2000, 3000] and list(p) == [10.5, 10.0, 11.0] and list(ibm) == [False, True, False] and t.dtype == np.int64)

s = 1_735_718_400                                       # a settlement (seconds)
S = s * 1000
# synthetic tape around s: alternating sides every 100 ms from s - 2 s to s + 200 s, and around the exit
tt = np.r_[np.arange(S - 2000, S + 200_000, 100), np.arange(S + 3_290_000, S + 3_700_000, 100)].astype(np.int64)
ibm = (np.arange(len(tt)) % 2 == 0)                      # even index: seller-initiated (bid), odd: buyer-initiated (ask)
mid = 100.0 * (1 - 0.001 * (tt >= S + 300) - 0.002 * (tt >= S + 5_000))     # price drops 10 bp at +0.3 s and 20 bp more at +5 s
pp = np.where(ibm, mid - 0.01, mid + 0.01)               # bid = mid - 0.01, ask = mid + 0.01
tr = (tt, pp, ibm)

i = np.searchsorted(tt, S + 500)
check("fill: 'sell' takes the first seller-initiated trade at or after the target (the bid)",
      N.fill(tr, S + 500, "sell") == pp[i if ibm[i] else i + 1] and N.fill(tr, S + 500, "sell") < mid[i] + 1e-12)
check("fill: 'buy' takes the first buyer-initiated trade (the ask)", N.fill(tr, S + 500, "buy") > N.fill(tr, S + 500, "sell"))
j = np.flatnonzero(tt == S + 1000)[0]
check("fill: a trade exactly at the target counts (at or after)", N.fill(tr, S + 1000, "sell" if ibm[j] else "buy") == pp[j])
tg = np.r_[tt[tt < S + 10_000], tt[tt >= S + 80_000]]; mg = np.isin(tt, tg)
check("fill: no taker-side trade within 60 s -> NaN", np.isnan(N.fill((tg, pp[mg], ibm[mg]), S + 10_000, "sell")))

# event result: short (d = -1) into a falling price
r = N.event_result(s, -1, 100.0, -0.002, tr)
check("identity: gross = d x (exit / entry - 1) x 1e4 for every delta",
      all(np.isclose(r[f"gross_{D}"], -1 * (r["exit"] / r[f"entry_{D}"] - 1) * 1e4) for D in N.DELTAS))
check("timing: the short entered before the 10 bp drop at +0.3 s earns ~+30 bp; entered after it ~+20 bp; after +5 s ~0 (minus the spread)",
      25 < r["gross_0.1"] < 32 and 15 < r["gross_0.5"] < 22 and -3 < r["gross_10"] < 1, f"{r['gross_0.1']:.1f} {r['gross_0.5']:.1f} {r['gross_10']:.1f}")
check("path: signed move from P(s-) at +0.1 s ~0, at +1 s ~+10 bp, at +10 s ~+30 bp",
      abs(r["path_0.1"]) < 2 and 8 < r["path_1"] < 12 and 28 < r["path_10"] < 32, f"{r['path_0.1']:.1f} {r['path_1']:.1f} {r['path_10']:.1f}")
check("pre arm: entry before s, funding = -d x r_s x 1e4", np.isfinite(r["pre_gross"]) and np.isclose(r["pre_funding"], -20.0))
x2 = tt[tt < S + 3_600_000][-1]; e2 = tt[tt >= S + 120_000][0]
check("N4-X replication: first trade (any side) >= s + 120 s, last trade < s + 3600 s",
      np.isclose(r["n4x_g2"], -1 * (pp[tt == x2][0] / pp[tt == e2][0] - 1) * 1e4))

# extraction keeps every trade a computation needs
ex = N.extract(tt, pp, ibm, [s])[s]
r2 = N.event_result(s, -1, 100.0, -0.002, ex)
check("extract: results from the extracted windows equal those from the full tape", all(np.isclose(r[k], r2[k], equal_nan=True) for k in r))

# leak: changing trades after a target never changes an earlier fill
pp2 = pp.copy(); late = tt >= S + 2_000; pp2[late] *= 1.05
r3 = N.event_result(s, -1, 100.0, -0.002, (tt, pp2, ibm))
check("leak: trades after s + 2 s do not move entries / paths at delta <= 1 s",
      all(np.isclose(r[f"entry_{D}"], r3[f"entry_{D}"]) and np.isclose(r[f"path_{D}"], r3[f"path_{D}"]) for D in (0.1, 0.25, 0.5, 1)))
print("\nFAILS:", fails)
sys.exit(fails)
