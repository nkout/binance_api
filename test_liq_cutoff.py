"""Tests for liq_cutoff.py and the readers that use it (btc_signal_analysis.prepare, feas_4h.build).

Synthetic: the mask boundaries, NaN times, a pinned LIQ_LIVE_FROM, only liquidation columns change, the restart warning.
Real data when present: the v1 year places LIQ_DEAD_FROM at the stream's death; the local out5 files are all masked.
"""
import contextlib, glob, io, os, sys
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, "runs", "harness_v1_feas"))
import liq_cutoff as L
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


def masked(df, ts):
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        L.mask_dead_liquidations(df, ts)
    return df, err.getvalue()


D = L.LIQ_DEAD_FROM
check("dead_mask: live before LIQ_DEAD_FROM, dead from it on (LIQ_LIVE_FROM None), NaN time dead",
      list(L.dead_mask([D - 15, D, D + 86400 * 365, np.nan])) == [False, True, True, True])
L.LIQ_LIVE_FROM = L.FIX_DAY + 3600
check("dead_mask: a pinned LIQ_LIVE_FROM makes rows from it on live again",
      list(L.dead_mask([D - 15, L.FIX_DAY, L.FIX_DAY + 3600, L.FIX_DAY + 7200])) == [False, True, False, False])
L.LIQ_LIVE_FROM = None

ts = np.array([D - 30, D - 15, D, D + 15, L.FIX_DAY + 60])
df = pd.DataFrame({"spot_timestamp": ts, "opt_long_force_exit_qty_sum": [0.5, 0.0, 0.0, 0.0, 2.0],
                   "opt_short_force_exit_cnt_sum": [1, 0, 0, 0, 3], "opt_open_interest_sample": [1.0, 2.0, 3.0, 4.0, 5.0],
                   "future_buy_qty": [7.0, 8.0, 9.0, 10.0, 11.0]})
before = df.copy()
out, err = masked(df, df.spot_timestamp)
liq = ["opt_long_force_exit_qty_sum", "opt_short_force_exit_cnt_sum"]
check("mask: liquidation columns (float and int) NaN on dead rows, unchanged on live rows",
      out[liq].iloc[2:].isna().all().all() and out[liq].iloc[:2].astype(float).equals(before[liq].iloc[:2].astype(float)))
check("mask: other columns untouched", out.drop(columns=liq).equals(before.drop(columns=liq)))
check("mask: warns when rows after the fix hold liquidations while LIQ_LIVE_FROM is None", "LIQ_LIVE_FROM" in err, err.strip()[:80])
_, err2 = masked(pd.DataFrame({"opt_long_force_exit_qty_sum": [3.0]}), [D + 4 * 86400])
check("mask: no warning for the dead-era stragglers (2026-04-27)", err2 == "")
L.LIQ_LIVE_FROM = int(L.FIX_DAY + 30)
out3, err3 = masked(before.copy(), before.spot_timestamp)
check("mask: with LIQ_LIVE_FROM pinned, post-restart liquidations are kept and nothing warns",
      out3[liq].iloc[4].tolist() == [2.0, 3.0] and out3[liq].iloc[2:4].isna().all().all() and err3 == "")
L.LIQ_LIVE_FROM = None
check("mask: a frame without liquidation columns is returned unchanged",
      L.mask_dead_liquidations(pd.DataFrame({"a": [1]}), [D]).equals(pd.DataFrame({"a": [1]})))

# feas_4h.build: liquidation features NaN on dead bars, finite on live bars
import feas_4h as F
n = 6000
idx = pd.Index((D - 3000 * 300 + np.arange(n) * 300).astype(np.int64))
rng = np.random.default_rng(0)
bars = pd.DataFrame({c: 100 + rng.normal(0, 0.01, n).cumsum() for c in ("future_bid_close", "future_ask_close", "spot_bid_close", "spot_ask_close")}, index=idx)
for c in ("future_buy_qty", "future_sell_qty", "spot_buy_qty", "spot_sell_qty", "opt_open_interest_sample", "opt_long_short_ratio_sample",
          "future_spread_median") + tuple(f"future_{s}_liq_{l}_median" for l in F.LEVELS for s in ("bid", "ask")):
    bars[c] = rng.uniform(1, 2, n)
bars["opt_funding_rate_sample"] = 1e-4
bars["dt"] = pd.to_datetime(idx, unit="s", utc=True)
bars["opt_long_force_exit_qty_sum"] = rng.poisson(0.3, n).astype(float)
bars["opt_short_force_exit_qty_sum"] = rng.poisson(0.3, n).astype(float)
d = F.build(bars)
live = idx < D
LF = ["liq_long_z", "liq_short_z", "liq_imb"]
# the 1 h rolling sum (min_periods=1) still sees live bars during the first hour after the cutoff, as after any gap
check("feas_4h.build: liquidation features NaN on every dead bar from 1 h after the cutoff, finite on live bars after warm-up",
      d.loc[idx >= D + 11 * 300, LF].isna().all().all() and np.isfinite(d.loc[live, LF].iloc[300:]).all().all())
check("feas_4h.build: input frame not modified", bars["opt_long_force_exit_qty_sum"].notna().all())

# real data
P15 = os.path.join(HERE, "data", "v1_year", "v1_15s.parquet")
if os.path.exists(P15):
    p = pd.read_parquet(P15, columns=["ts", "opt_mark_price_sample", "opt_long_force_exit_qty_sum", "opt_short_force_exit_qty_sum"])
    hit = (p.opt_long_force_exit_qty_sum.fillna(0) + p.opt_short_force_exit_qty_sum.fillna(0)) > 0
    last_mark = p.ts[p.opt_mark_price_sample.notna()].max()
    last_live = p.ts[hit & (p.ts < D)].max()
    check("v1 year: LIQ_DEAD_FROM is the first bar after the last markPrice sample", D == last_mark + 15, f"last mark {last_mark}")
    check("v1 year: the last liquidation before it is within 5 minutes", 0 < D - last_live <= 300, f"{D - last_live} s")
    q, _ = masked(p.copy(), p.ts)
    liq_v1 = ["opt_long_force_exit_qty_sum", "opt_short_force_exit_qty_sum"]
    check("v1 year: after the mask no liquidation value at or after LIQ_DEAD_FROM; before it unchanged",
          q.loc[q.ts >= D, liq_v1].isna().all().all()
          and np.array_equal(q.loc[q.ts < D, liq_v1].to_numpy(float), p.loc[p.ts < D, liq_v1].to_numpy(float), equal_nan=True))
else:
    print("skip v1 year checks: no", P15)

files = sorted(glob.glob(os.path.join(HERE, "logs", "collector_v4.1_logs", "5sec", "out5.*.csv.gz")))
if files:
    import btc_signal_analysis as B
    with contextlib.redirect_stdout(io.StringIO()):
        raw = B.load_files(files[:3])
        prep = B.prepare(raw.copy(), 0.0)
    check("btc_signal_analysis: liq_flag NaN on every row of the dead-era out5 files", prep["liq_flag"].isna().all(), f"{len(prep)} rows")
    L.LIQ_LIVE_FROM = int(pd.to_numeric(raw.spot_timestamp).min())       # pretend these rows were collected live
    raw2 = raw.copy(); raw2["opt_long_force_exit_qty_sum"] = 0.0
    raw2.loc[raw2.index[::50], "opt_long_force_exit_qty_sum"] = 1.0
    with contextlib.redirect_stdout(io.StringIO()):
        prep2 = B.prepare(raw2, 0.0)
    L.LIQ_LIVE_FROM = None
    check("btc_signal_analysis: on live rows liq_flag is 0/1 with no NaN",
          prep2["liq_flag"].notna().all() and set(prep2["liq_flag"].unique()) == {0.0, 1.0})
else:
    print("skip out5 checks: no local collector files")

print("\nFAILS:", fails)
sys.exit(fails)
