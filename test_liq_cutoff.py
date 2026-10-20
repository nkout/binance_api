"""Tests for liq_cutoff.py and the readers that use it (btc_signal_analysis.prepare, feas_4h.build).

Synthetic: the time rule (v1-v5 rows), the live column (v6 rows), both mixed, only liquidation columns change, the
warning for liquidations without the live column.
Real data when present: the v1 year places LIQ_DEAD_FROM at the stream's death; the local out5 files are all masked;
btc_signal_analysis loads the live column from a v6-style file; aggregate_bars.py merges it as the minimum.
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
F1 = L.FIX_DAY
check("dead_mask, no live column: live before LIQ_DEAD_FROM, dead from it on, NaN time dead",
      list(L.dead_mask([D - 15, D, D + 86400 * 365, np.nan])) == [False, True, True, True])
check("dead_mask, live column: 1 is data, 0 is not, NaN (no column in that file) falls back to the time rule",
      list(L.dead_mask([D - 15, F1 + 5, F1 + 10, F1 + 15, F1 + 20], [np.nan, 1, 0, np.nan, 1.0]))
      == [False, False, True, True, False])

liq = ["opt_long_force_exit_qty_sum", "opt_short_force_exit_cnt_sum"]
ts = np.array([D - 30, D - 15, D, D + 15, F1 + 60])
df = pd.DataFrame({"spot_timestamp": ts, liq[0]: [0.5, 0.0, 0.0, 0.0, 2.0], liq[1]: [1, 0, 0, 0, 3],
                   "opt_open_interest_sample": [1.0, 2.0, 3.0, 4.0, 5.0], "future_buy_qty": [7.0, 8.0, 9.0, 10.0, 11.0]})
before = df.copy()
out, err = masked(df, df.spot_timestamp)
check("mask, v1-v5 rows: liquidation columns (float and int) NaN from LIQ_DEAD_FROM on, unchanged before",
      out[liq].iloc[2:].isna().all().all() and out[liq].iloc[:2].astype(float).equals(before[liq].iloc[:2].astype(float)))
check("mask: other columns untouched", out.drop(columns=liq).equals(before.drop(columns=liq)))
check("mask: warns when rows without the live column hold liquidations after the fix (a fixed v1-v5 collector)",
      L.LIVE_COL in err and "v6" in err, err.strip()[:80])
_, err2 = masked(pd.DataFrame({liq[0]: [3.0]}), [D + 4 * 86400])
check("mask: no warning for the dead-era stragglers (2026-04-27)", err2 == "")

v6 = pd.DataFrame({"spot_timestamp": F1 + 5 * np.arange(4), liq[0]: [0.0, 1.5, 2.0, 0.0], liq[1]: [0, 2, 1, 0],
                   L.LIVE_COL: [1, 1, 0, 1], "future_buy_qty": [1.0, 2.0, 3.0, 4.0]})
v6_before = v6.copy()
out6, err6 = masked(v6, v6.spot_timestamp)
check("mask, v6 rows: kept where the live column is 1 (zeros too), NaN where it is 0, no warning",
      out6[liq[0]].tolist()[:2] == [0.0, 1.5] and out6[liq[0]].iloc[3] == 0.0 and out6[liq].iloc[2].isna().all()
      and err6 == "")
check("mask, v6 rows: the live column and the other columns untouched",
      out6.drop(columns=liq).equals(v6_before.drop(columns=liq)))
old = pd.DataFrame({"spot_timestamp": [D - 15, D + 15], liq[0]: [4.0, 0.0], liq[1]: [1, 0], "future_buy_qty": [5.0, 6.0]})
mix = pd.concat([old, v6_before], ignore_index=True)       # v5 file + v6 file: the v5 rows get NaN in the live column
outm, errm = masked(mix, mix.spot_timestamp)
check("mask, v5 and v6 files concatenated: v5 rows by time, v6 rows by the live column",
      outm[liq[0]].iloc[0] == 4.0 and np.isnan(outm[liq[0]].iloc[1]) and outm[liq[0]].iloc[3] == 1.5
      and np.isnan(outm[liq[0]].iloc[4]) and errm == "")
check("mask: a frame without liquidation columns is returned unchanged",
      L.mask_dead_liquidations(pd.DataFrame({"a": [1]}), [D]).equals(pd.DataFrame({"a": [1]})))
check("the live column is not itself a liquidation column", L.LIVE_COL not in L.liq_columns(v6_before))

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
    # a v6-style file: a dead-era out5 file plus the trailing live column, with liquidations and not-live bars planted
    import subprocess, tempfile
    with tempfile.TemporaryDirectory() as tmp:
        src = pd.read_csv(files[0], sep=";")
        src["opt_long_force_exit_qty_sum"] = 0.0
        src.loc[src.index[::50], "opt_long_force_exit_qty_sum"] = 1.0
        src[L.LIVE_COL] = 1
        src.loc[src.index[7::40], L.LIVE_COL] = 0
        p6 = os.path.join(tmp, "out5.w5.vsixx.00000.261020.csv")
        src.to_csv(p6, sep=";", index=False)
        with contextlib.redirect_stdout(io.StringIO()):
            raw6 = B.load_files([p6])
            prep6 = B.prepare(raw6.copy(), 0.0)
        check("btc_signal_analysis: load_files reads the live column from a v6 file", L.LIVE_COL in raw6.columns)
        # prepare() keeps only features + dt, so look the live column up by bar time
        lv = raw6.assign(dt=pd.to_datetime(raw6["spot_datetime"])).drop_duplicates("dt").set_index("dt")[L.LIVE_COL]
        live = (prep6["dt"].map(lv) == 1).to_numpy()
        check("btc_signal_analysis, v6 file: liq_flag 0/1 on live bars (the column overrides the date), NaN on the others",
              set(prep6.loc[live, "liq_flag"].unique()) == {0.0, 1.0} and prep6.loc[~live, "liq_flag"].isna().all()
              and (~live).sum() > 0)

        agg = os.path.join(tmp, "agg15.csv")
        r = subprocess.run([sys.executable, os.path.join(HERE, "aggregate_bars.py"), "--target-sec", "15", "-o", agg, p6],
                           capture_output=True, text=True)
        s2 = src.sort_values("spot_timestamp").drop_duplicates("spot_timestamp")
        exp = s2.groupby(s2.spot_timestamp // 15 * 15)[L.LIVE_COL].min()
        a15 = pd.read_csv(agg, sep=";") if r.returncode == 0 else None
        check("aggregate_bars: a v6 file merges 5s -> 15s, same columns, live column = minimum of its source bars",
              a15 is not None and list(a15.columns) == list(src.columns) and a15[L.LIVE_COL].tolist() == exp.tolist(),
              (r.stderr or r.stdout).strip()[-200:])
        r2 = subprocess.run([sys.executable, os.path.join(HERE, "aggregate_bars.py"), "--target-sec", "15", "-o", agg,
                             files[0], p6], capture_output=True, text=True)
        check("aggregate_bars: refuses to merge v5 and v6 files together (column mismatch)",
              r2.returncode != 0 and "column mismatch" in r2.stderr, r2.stderr.strip()[-120:])
else:
    print("skip out5 checks: no local collector files")

print("\nFAILS:", fails)
sys.exit(fails)
