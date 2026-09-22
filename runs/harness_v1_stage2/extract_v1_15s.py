"""Stream the v1 monthly tars into ONE compact 15 s table for the stage-2 direction probe.

Reads ~50 of the 784 columns, dedupes on spot_timestamp across collector-instance handoffs,
maps the dead-REST sentinel (-1) to NaN, casts to float32 and writes a parquet that is small
enough to put on Google Drive for the Colab notebook `btc_v1_stage2_probe.ipynb`.

Local, CPU, ~10-15 min, peak RAM ~2 GB.
Usage: python extract_v1_15s.py [--months 2512,2601]   (default: all months)
       python extract_v1_15s.py --tar ../../data/60days_data.tar --member .w5. --out ../../data/w5_60d.parquet
"""
import argparse, glob, io, os, sys, tarfile, time
import numpy as np, pandas as pd

DATA = "/home/nkout/projects/binance_api/data/v1_year"
OUT = os.path.join(DATA, "v1_15s.parquet")

LV_F = ["0.0", "0.01", "0.05", "0.1", "0.2", "0.4"]   # cumulative depth within p % of best
LV_S = ["0.05", "0.2"]
PRICE = [f"future_{s}_{a}" for s in ("bid", "ask") for a in ("open", "close", "min", "max")] + \
        ["spot_bid_close", "spot_ask_close"]
FLOW = ["future_buy_qty", "future_sell_qty", "future_buy_samples", "future_sell_samples",
        "spot_buy_qty", "spot_sell_qty", "future_buy_vwap", "future_sell_vwap"]
BOOK = ["future_spread_median", "future_spread_max", "future_bid_samples", "future_ask_samples"] + \
       [f"future_{s}_liq_{l}_median" for l in LV_F for s in ("bid", "ask")] + \
       [f"future_{s}_liq_{l}_close" for l in ("0.0", "0.05") for s in ("bid", "ask")] + \
       [f"spot_{s}_liq_{l}_median" for l in LV_S for s in ("bid", "ask")]
OPT = ["opt_open_interest_sample", "opt_long_short_ratio_sample", "opt_funding_rate_sample",
       "opt_long_force_exit_qty_sum", "opt_short_force_exit_qty_sum", "opt_mark_price_sample",
       "opt_index_price_sample"]
USE = ["spot_timestamp"] + PRICE + FLOW + BOOK + OPT


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", default="")
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--tar", default="", help="single tar instead of the v1 monthly tars (e.g. 60days_data.tar)")
    ap.add_argument("--member", default="", help="substring a member name must contain (e.g. .w5.)")
    a = ap.parse_args()
    tars = [a.tar] if a.tar else sorted(glob.glob(os.path.join(DATA, "month.*.tar")))
    if a.months and not a.tar:
        keep = set(a.months.split(","))
        tars = [t for t in tars if os.path.basename(t).split(".")[1] in keep]
    assert tars, f"no monthly tars in {DATA}"
    useset = set(USE)
    parts, nfile, nskip, t0 = [], 0, 0, time.time()
    for t in tars:
        mon = os.path.basename(t).split(".")[1] if not a.tar else os.path.basename(t)
        with tarfile.open(t) as tf:
            names = sorted(n for n in tf.getnames() if n.endswith(".csv.gz") and a.member in n)
            for nm in names:
                try:
                    df = pd.read_csv(io.BytesIO(tf.extractfile(nm).read()), sep=";",
                                     compression="gzip", usecols=lambda c: c in useset,
                                     low_memory=False)
                except Exception as e:
                    print(f"  skip {nm}: {e}", file=sys.stderr); nskip += 1; continue
                miss = [c for c in USE if c not in df.columns]
                if miss:
                    print(f"  skip {nm}: missing {miss[:3]}", file=sys.stderr); nskip += 1; continue
                df = df[USE].apply(pd.to_numeric, errors="coerce")
                parts.append(df)
                nfile += 1
        print(f"{mon}: {len(names):>4} files  (total {nfile:,}, skipped {nskip}, "
              f"{time.time() - t0:.0f}s)", flush=True)

    raw = pd.concat(parts, ignore_index=True); del parts
    raw = raw.dropna(subset=["spot_timestamp"])
    raw["spot_timestamp"] = raw["spot_timestamp"].astype(np.int64)
    n0 = len(raw)
    raw = raw.sort_values("spot_timestamp").drop_duplicates("spot_timestamp", keep="last")
    print(f"rows {n0:,} -> {len(raw):,} after timestamp dedupe")

    # dead-REST era writes -1 forever (v1_year_data_audit.md §2) -> NaN, never a feature value
    for c in OPT:
        raw.loc[raw[c] == -1, c] = np.nan
    raw.loc[raw["opt_open_interest_sample"] <= 0, "opt_open_interest_sample"] = np.nan
    for c in ["future_bid_close", "future_ask_close"]:
        raw.loc[raw[c] <= 0, c] = np.nan
    raw = raw.dropna(subset=["future_bid_close", "future_ask_close"])

    out = raw.rename(columns={"spot_timestamp": "ts"}).reset_index(drop=True)
    for c in out.columns:
        if c != "ts":
            out[c] = out[c].astype(np.float32)
    d = np.diff(out["ts"].to_numpy())
    span = (out["ts"].iloc[-1] - out["ts"].iloc[0]) / 86400
    print(f"span {span:.1f} d  | 15 s steps {np.mean(d == 15) * 100:.2f} %  | gaps > 60 s: "
          f"{int((d > 60).sum())}")
    for c in OPT:
        print(f"  {c:<32} non-null {out[c].notna().mean() * 100:5.1f} %")
    out.to_parquet(a.out, index=False, compression="zstd")
    print(f"wrote {a.out}  {os.path.getsize(a.out) / 1e6:.0f} MB  {out.shape}  ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
