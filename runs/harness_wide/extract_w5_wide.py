"""Stream the 5 s collector tar (all 825 columns) into ONE wide parquet for the W1 probe.

Only mechanical, name-based transforms (pre-registered in next_signal_ideas.md, W1) -- no engineered
features:
  price levels (bid/ask OHLC, vwap, mark, index, est. settle price) -> bp vs the bar's futures mid
  $ amounts (spreads, micro-price dev, mid std, bar price diff)      -> bp of mid
  $^2 sums (mid rv / m2)                                             -> log1p(bp^2)
  ETH mid close -> 5 s log return bp; ETH open / median -> bp vs ETH close
  open interest -> per-bar log change bp (a level is a regime proxy)
  side / sign flags -> as is;  funding rate -> bp
  anything else: non-negative -> log1p, signed -> asinh
Dropped: timestamps, constants, duplicate _sum/_count helpers, time-to-funding (a time proxy),
dead liquidation columns. The decisions are written to wide_spec.json next to this file.

Pass 1 derives the spec from a sample of files; pass 2 streams every file (one row group per file,
rows deduped on spot_timestamp and kept strictly increasing).

Usage: python extract_w5_wide.py [--tar ../../data/60days_data.tar] [--out ../../data/w5_wide.parquet]
                                 [--max-files N]           (N > 0: first N files only, for tests)
Values are stored at fp16-level precision (see trunc16). Local, CPU, ~10-15 min, peak RAM < 1 GB.
"""
import argparse, io, json, os, re, sys, tarfile, time
import numpy as np, pandas as pd
import pyarrow as pa, pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
TAR = os.path.join(HERE, "..", "..", "data", "60days_data.tar")
OUT = os.path.join(HERE, "..", "..", "data", "w5_wide.parquet")
SPEC = os.path.join(HERE, "wide_spec.json")

DROP_RE = [r"_datetime$", r"^future_timestamp$", r"^schema_version$", r"^opt_remaining_time",
           r"^opt_.*_sample_(count|sum)$", r"^opt_eth_mid_(count|sum)$", r"force_exit",
           r"^(spot|future)_(mid|bid|ask|spread|micro_dev)_sum$", r"^(spot|future)_(bid|ask|diff)_liq_[\d.]+_sum$"]
PRICE_RE = [r"^(spot|future)_(bid|ask)_(open|close|min|median|max)$", r"_vwap$",
            r"^opt_(mark_price|index_price|est_funding_rate)_sample$"]
USD_RE = [r"_spread_(open|close|min|median|max)$", r"_micro_dev_(open|close|min|median|max)$",
          r"_mid_std$", r"_price_diff$", r"^opt_spread_sample$"]
USD2_RE = [r"_mid_rv$", r"_mid_m2$"]
FLAG_RE = [r"_side$", r"_move_sign$"]


def _any(res, c):
    return any(re.search(r, c) for r in res)


def classify(cols, sample):
    """Column -> action. `sample` is a DataFrame of raw rows used only for constants and sign."""
    spec = {}
    for c in cols:
        if c == "spot_timestamp":
            a = "ts"
        elif _any(DROP_RE, c):
            a = "drop"
        elif sample[c].nunique(dropna=True) <= 1:
            a = "drop_const"
        elif _any(PRICE_RE, c):
            a = "price_bp"
        elif _any(USD_RE, c):
            a = "usd_bp"
        elif _any(USD2_RE, c):
            a = "usd2_logbp"
        elif _any(FLAG_RE, c):
            a = "raw"
        elif c == "opt_eth_mid_close":
            a = "eth_ret"
        elif c in ("opt_eth_mid_open", "opt_eth_mid_median"):
            a = "eth_rel"
        elif c == "opt_open_interest_sample":
            a = "logchg"
        elif c == "opt_funding_rate_sample":
            a = "x1e4"
        elif c == "opt_long_short_ratio_sample":
            a = "raw"
        elif sample[c].min() >= 0:
            a = "log1p"
        else:
            a = "asinh"
        spec[c] = a
    return spec


def trunc16(v):
    """float32 with the low 13 mantissa bits zeroed: fp16-level precision (the notebook feeds the GPU in
    fp16 anyway) but full float32 range, and it compresses ~2x better. Relative error < 2**-10."""
    u = np.ascontiguousarray(v, dtype=np.float32).view(np.uint32) & np.uint32(0xFFFFE000)
    return u.view(np.float32)


def transform(df, spec, prev):
    """Raw file DataFrame -> (ts int64, dict name -> float32 array). `prev` carries the last ETH close
    and OI across files so per-bar changes are continuous."""
    bid, ask = df["future_bid_close"].to_numpy(np.float64), df["future_ask_close"].to_numpy(np.float64)
    mid = (bid + ask) / 2
    ts = df["spot_timestamp"].to_numpy(np.int64)
    eth_c = df["opt_eth_mid_close"].to_numpy(np.float64); eth_c = np.where(eth_c > 0, eth_c, np.nan)
    out = {}
    for c, a in spec.items():
        if a in ("drop", "drop_const", "ts"):
            continue
        x = df[c].to_numpy(np.float64)
        if a == "price_bp":
            v = np.where(x > 0, (x / mid - 1) * 1e4, np.nan)
        elif a == "usd_bp":
            v = x / mid * 1e4
        elif a == "usd2_logbp":
            v = np.log1p(np.maximum(x, 0) * (1e4 / mid) ** 2)
        elif a == "eth_ret":
            lc = np.log(eth_c); lp = np.r_[prev.get("eth", np.nan), lc[:-1]]
            v = (lc - lp) * 1e4
        elif a == "eth_rel":
            v = np.where(x > 0, (x / eth_c - 1) * 1e4, np.nan)
        elif a == "logchg":
            lx = np.log(np.where(x > 0, x, np.nan)); lp = np.r_[prev.get("oi", np.nan), lx[:-1]]
            v = (lx - lp) * 1e4
        elif a == "x1e4":
            v = np.where(x == -1, np.nan, x * 1e4)
        elif a == "log1p":
            v = np.log1p(np.maximum(x, 0))
        elif a == "asinh":
            v = np.arcsinh(x)
        else:
            v = x
        out[f"w_{c}"] = trunc16(v)
    if len(ts):
        prev["eth"] = float(np.log(eth_c[-1])) if np.isfinite(eth_c[-1]) else np.nan
        oi = df["opt_open_interest_sample"].to_numpy(np.float64)[-1]
        prev["oi"] = float(np.log(oi)) if oi > 0 else np.nan
    return ts, out


def read_member(tf, nm):
    df = pd.read_csv(io.BytesIO(tf.extractfile(nm).read()), sep=";", compression="gzip", low_memory=False)
    num = [c for c in df.columns if not c.endswith("_datetime")]
    df[num] = df[num].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=["spot_timestamp", "future_bid_close", "future_ask_close"])
    df = df[(df["future_bid_close"] > 0) & (df["future_ask_close"] > 0)]
    return df.sort_values("spot_timestamp").drop_duplicates("spot_timestamp", keep="last").reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tar", default=TAR); ap.add_argument("--out", default=OUT)
    ap.add_argument("--spec", default=SPEC); ap.add_argument("--max-files", type=int, default=0)
    a = ap.parse_args()
    t0 = time.time()
    with tarfile.open(a.tar) as tf:
        names = sorted(n for n in tf.getnames() if n.endswith(".csv.gz"))
        if a.max_files > 0:
            names = names[:a.max_files]
        # pass 1: spec from an even sample of files
        pick = names[:: max(1, len(names) // 24)][:24]
        sample = pd.concat([read_member(tf, n) for n in pick], ignore_index=True)
        cols = list(sample.columns)
        spec = classify(cols, sample)
        counts = pd.Series(spec).value_counts().to_dict()
        print(f"{len(names)} files | {len(cols)} raw columns -> spec {counts} | {time.time() - t0:.0f}s", flush=True)
        json.dump(dict(columns=spec, counts=counts, sample_files=pick), open(a.spec, "w"), indent=1)
        # pass 2: stream
        writer, last_ts, prev, nrow = None, -1, {}, 0
        for i, nm in enumerate(names):
            df = read_member(tf, nm)
            miss = [c for c in cols if c not in df.columns]
            if miss:
                print(f"  skip {nm}: missing {miss[:3]}", file=sys.stderr); continue
            df = df[df["spot_timestamp"] > last_ts].reset_index(drop=True)
            if df.empty:
                continue
            ts, out = transform(df, spec, prev)
            tab = pa.table({"ts": pa.array(ts), **{k: pa.array(v) for k, v in out.items()}})
            if writer is None:
                writer = pq.ParquetWriter(a.out, tab.schema, compression="zstd")
            writer.write_table(tab)
            last_ts = int(ts[-1]); nrow += len(ts)
            if i % 50 == 0:
                print(f"  {i + 1}/{len(names)} files, {nrow:,} rows, {time.time() - t0:.0f}s", flush=True)
        writer.close()
    print(f"wrote {a.out}: {nrow:,} rows x {len(out) + 1} cols, {os.path.getsize(a.out) / 1e6:.0f} MB, "
          f"{time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
