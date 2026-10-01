"""Download every USDT-M perpetual ever listed (incl. delisted) from the Binance public archive:
monthly 1 h klines and monthly funding-rate files, 2020-01 .. END_MONTH. One parquet per symbol.

    python fetch_archive.py [--symbols BTCUSDT,ETHUSDT] [--workers 16]
    python fetch_archive.py --spot --symbols-file ever_in_universe.json      (spot 1 h klines, for R5)

Survivorship-free by construction: the symbol list is the archive's own directory listing, which keeps
delisted contracts (LUNA, FTT, SRM, ...). Resumable: symbols already written are skipped.
Output: data/xsec/klines/<SYM>.parquet  (ts, open, high, low, close, quote_volume, taker_buy_quote, trades)
        data/xsec/funding/<SYM>.parquet (ts, rate)        ts = open time / funding time, seconds UTC
        data/xsec/symbols.json          (every archive symbol + which were kept)
        data/xsec/spot/<SYM>.parquet    (--spot: same kline columns; absent when the coin has no spot pair)
Spot archive timestamps switch from milliseconds to microseconds in 2025; both are handled.
"""
import argparse, io, json, os, re, sys, time, zipfile
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "..", "data", "xsec")
S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
DL = "https://data.binance.vision/"
START_MONTH, END_MONTH = "2020-01", "2026-08"
SYM_RE = re.compile(r"^[A-Z0-9]+USDT$")
KCOLS = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "count",
         "taker_buy_volume", "taker_buy_quote_volume", "ignore"]


def get(url, tries=4):
    for k in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            time.sleep(2 ** k)
        except Exception:
            time.sleep(2 ** k)
    raise RuntimeError(f"failed: {url}")


def list_prefix(prefix, files=False):
    """All sub-prefixes (or file keys) under an S3 prefix, following pagination."""
    out, marker = [], ""
    while True:
        x = get(f"{S3}?delimiter=/&prefix={prefix}&marker={marker}").decode()
        tag = "Key" if files else "Prefix"
        items = [m for m in re.findall(rf"<{tag}>([^<]*)</{tag}>", x) if m != prefix]
        out += items
        if "<IsTruncated>true" not in x or not items:
            return out
        marker = items[-1]


def read_zip_csv(blob, names):
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        raw = z.read(z.namelist()[0]).decode()
    first = raw.split("\n", 1)[0]
    header = 0 if not first[:1].isdigit() else None          # newer files carry a header row
    df = pd.read_csv(io.StringIO(raw), header=header)
    if header is None:
        df.columns = names[:df.shape[1]]
    return df


def month_ok(key):
    m = re.search(r"(\d{4}-\d{2})\.zip$", key)
    return m is not None and START_MONTH <= m.group(1) <= END_MONTH


def to_seconds(x):
    x = pd.to_numeric(x).astype(np.int64)
    return np.where(x > 10 ** 14, x // 10 ** 6, x // 1000).astype(np.int64)     # us (spot 2025+) or ms


def klines_frame(market, sym):
    keys = [k for k in list_prefix(f"data/{market}/monthly/klines/{sym}/1h/", files=True)
            if k.endswith(".zip") and month_ok(k)]
    parts = []
    for k in sorted(keys):
        b = get(DL + k)
        if b is None:
            continue
        d = read_zip_csv(b, KCOLS)
        d = d.rename(columns={"taker_buy_quote_volume": "taker_buy_quote", "count": "trades"})
        parts.append(d[["open_time", "open", "high", "low", "close", "quote_volume", "taker_buy_quote", "trades"]])
    if not parts:
        return None
    k = pd.concat(parts, ignore_index=True)
    k["ts"] = to_seconds(k["open_time"])
    k = k.drop(columns="open_time").drop_duplicates("ts").sort_values("ts")
    for c in ("open", "high", "low", "close", "quote_volume", "taker_buy_quote"):
        k[c] = pd.to_numeric(k[c], errors="coerce").astype(np.float64)
    k["trades"] = pd.to_numeric(k["trades"], errors="coerce").fillna(0).astype(np.int64)
    return k[["ts", "open", "high", "low", "close", "quote_volume", "taker_buy_quote", "trades"]]


def fetch_spot(sym):
    sp = os.path.join(OUT, "spot", f"{sym}.parquet")
    if os.path.exists(sp):
        return sym, "cached", 0
    k = klines_frame("spot", sym)
    if k is None:
        return sym, "no spot", 0
    k.to_parquet(sp, index=False)
    return sym, "ok", len(k)


def fetch_symbol(sym):
    kp, fp = os.path.join(OUT, "klines", f"{sym}.parquet"), os.path.join(OUT, "funding", f"{sym}.parquet")
    if os.path.exists(kp) and os.path.exists(fp):
        return sym, "cached", 0
    keys = [k for k in list_prefix(f"data/futures/um/monthly/klines/{sym}/1h/", files=True)
            if k.endswith(".zip") and month_ok(k)]
    parts = []
    for k in sorted(keys):
        b = get(DL + k)
        if b is None:
            continue
        d = read_zip_csv(b, KCOLS)
        d = d.rename(columns={"taker_buy_quote_volume": "taker_buy_quote", "count": "trades"})
        parts.append(d[["open_time", "open", "high", "low", "close", "quote_volume", "taker_buy_quote", "trades"]])
    if not parts:
        return sym, "no klines", 0
    k = pd.concat(parts, ignore_index=True)
    k["ts"] = (pd.to_numeric(k["open_time"]) // 1000).astype(np.int64)
    k = k.drop(columns="open_time").drop_duplicates("ts").sort_values("ts")
    for c in ("open", "high", "low", "close", "quote_volume", "taker_buy_quote"):
        k[c] = pd.to_numeric(k[c], errors="coerce").astype(np.float64)
    k["trades"] = pd.to_numeric(k["trades"], errors="coerce").fillna(0).astype(np.int64)
    k[["ts", "open", "high", "low", "close", "quote_volume", "taker_buy_quote", "trades"]].to_parquet(kp, index=False)
    fparts = []
    for fk in sorted(f for f in list_prefix(f"data/futures/um/monthly/fundingRate/{sym}/", files=True)
                     if f.endswith(".zip") and month_ok(f)):
        b = get(DL + fk)
        if b is not None:
            fparts.append(read_zip_csv(b, ["calc_time", "funding_interval_hours", "last_funding_rate"]))
    if fparts:
        f = pd.concat(fparts, ignore_index=True)
        f = pd.DataFrame({"ts": (pd.to_numeric(f["calc_time"]) // 1000).astype(np.int64),
                          "rate": pd.to_numeric(f["last_funding_rate"], errors="coerce")})
        f = f.drop_duplicates("ts").sort_values("ts")
    else:
        f = pd.DataFrame({"ts": np.zeros(0, np.int64), "rate": np.zeros(0)})
    f.to_parquet(fp, index=False)
    return sym, "ok", len(k)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default=""); ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--spot", action="store_true"); ap.add_argument("--symbols-file", default="")
    a = ap.parse_args()
    if a.spot:
        os.makedirs(os.path.join(OUT, "spot"), exist_ok=True)
        syms = json.load(open(a.symbols_file)) if a.symbols_file else a.symbols.split(",")
        t0, n_ok = time.time(), 0
        with ThreadPoolExecutor(a.workers) as ex:
            for fu in as_completed([ex.submit(fetch_spot, s) for s in syms]):
                s, st, n = fu.result(); n_ok += st in ("ok", "cached")
        print(f"spot: {n_ok}/{len(syms)} symbols have spot klines | {time.time() - t0:.0f}s")
        return
    os.makedirs(os.path.join(OUT, "klines"), exist_ok=True); os.makedirs(os.path.join(OUT, "funding"), exist_ok=True)
    t0 = time.time()
    if a.symbols:
        syms = a.symbols.split(",")
    else:
        allp = [p.rstrip("/").split("/")[-1] for p in list_prefix("data/futures/um/monthly/klines/")]
        syms = sorted(s for s in allp if SYM_RE.match(s))
        json.dump(dict(archive=sorted(allp), kept=syms), open(os.path.join(OUT, "symbols.json"), "w"), indent=0)
        print(f"archive symbols {len(allp)} -> USDT perps {len(syms)} | {time.time() - t0:.0f}s", flush=True)
    done = 0
    with ThreadPoolExecutor(a.workers) as ex:
        for fu in as_completed([ex.submit(fetch_symbol, s) for s in syms]):
            try:
                s, st, n = fu.result()
            except Exception as e:
                print(f"  ERROR {e}", file=sys.stderr); continue
            done += 1
            if done % 50 == 0 or st not in ("ok", "cached"):
                print(f"  {done}/{len(syms)} {s}: {st} {n} | {time.time() - t0:.0f}s", flush=True)
    print(f"done {done}/{len(syms)} in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
