"""Bybit USDT linear perpetuals (trading AND closed) — instruments, 1 h klines and funding history from the public v5 API,
for every Bybit perp that maps to a Binance perp ever in the point-in-time top 100 (N2, next_signal_ideas.md Round 6).

    python fetch_bybit.py [--workers 8] [--end 2026-10-01]

Survivorship: `instruments-info?status=Closed` lists delisted perps, and their klines / funding stay queryable when the
request window ends before the delisting, so delisted coins are included. Symbol renames are the remaining gap.
Mapping Binance -> Bybit: exact symbol, else a unique match after stripping 10^k multipliers (1000SHIB <-> SHIB1000).
Resumable: symbols already written are skipped.
Output: data/bybit/instruments.json, data/bybit/map.json ({binance_sym: bybit_sym}),
        data/bybit/klines/<BINANCE_SYM>.parquet (ts, close, turnover)     ts = bar open, seconds UTC
        data/bybit/funding/<BINANCE_SYM>.parquet (ts, rate)
"""
import argparse, json, os, re, sys, time
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "..", "data", "bybit")
XSEC = os.path.join(HERE, "..", "..", "data", "xsec")
API = "https://api.bybit.com/v5/market"
START_MS = int(pd.Timestamp("2020-01-01", tz="UTC").timestamp() * 1000)


def get(path, tries=6):
    for k in range(tries):
        try:
            with urllib.request.urlopen(API + path, timeout=30) as r:
                d = json.load(r)
            if d.get("retCode") == 0:
                return d["result"]
            if d.get("retCode") in (10006, 10016, 10002):          # rate limit / server busy / time window
                time.sleep(1 + 2 ** k); continue
            raise RuntimeError(f"{path}: {d.get('retCode')} {d.get('retMsg')}")
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(2 ** k)
    raise RuntimeError(f"failed: {path}")


def instruments(getter=get):
    out = []
    for status in ("Trading", "Closed"):
        cur = ""
        while True:
            r = getter(f"/instruments-info?category=linear&status={status}&limit=1000&cursor={cur}")
            out += r["list"]; cur = r.get("nextPageCursor", "")
            if not cur:
                break
    return [x for x in out if x.get("contractType") == "LinearPerpetual" and x.get("quoteCoin") == "USDT"]


_MULT = re.compile(r"^(10{2,})(?=[A-Z])|(?<=[A-Z])(10{2,})$")


def norm(sym):
    return _MULT.sub("", sym[:-4]) if sym.endswith("USDT") else sym


def map_symbols(binance, bybit):
    """{binance: bybit}: exact name, else a unique normalised-base match."""
    bset = set(bybit); by_norm = {}
    for s in bybit:
        by_norm.setdefault(norm(s), []).append(s)
    out = {}
    for s in binance:
        if s in bset:
            out[s] = s
        else:
            c = by_norm.get(norm(s), [])
            if len(c) == 1:
                out[s] = c[0]
    return out


def paged(path_fmt, end_ms, start_ms, key_ts, getter=get):
    """Walk backwards from end_ms with an end cursor until the API returns nothing or start_ms is passed."""
    rows, cur = [], end_ms
    while True:
        lst = getter(path_fmt.format(end=cur))["list"]
        if not lst:
            break
        rows += lst
        oldest = min(int(key_ts(x)) for x in lst)
        if oldest <= start_ms or oldest - 1 >= cur:
            break
        cur = oldest - 1
    return rows


def fetch_one(bin_sym, by_sym, end_ms, start_ms, getter=get, out=OUT):
    kp, fp = os.path.join(out, "klines", f"{bin_sym}.parquet"), os.path.join(out, "funding", f"{bin_sym}.parquet")
    if os.path.exists(kp) and os.path.exists(fp):
        return bin_sym, "cached", 0
    f = paged(f"/funding/history?category=linear&symbol={by_sym}&limit=200&endTime={{end}}", end_ms, start_ms,
              lambda x: x["fundingRateTimestamp"], getter)
    fr = pd.DataFrame({"ts": [int(x["fundingRateTimestamp"]) // 1000 for x in f], "rate": [float(x["fundingRate"]) for x in f]})
    fr = fr[(fr.ts * 1000 >= start_ms) & (fr.ts * 1000 < end_ms)].drop_duplicates("ts").sort_values("ts")
    k_end = min(end_ms, (int(fr.ts.max()) + 8 * 3600) * 1000) if len(fr) else end_ms     # closed perps: window before delisting
    k = paged(f"/kline?category=linear&symbol={by_sym}&interval=60&limit=1000&start={start_ms}&end={{end}}", k_end - 1, start_ms,
              lambda x: x[0], getter)
    kl = pd.DataFrame({"ts": [int(x[0]) // 1000 for x in k], "close": [float(x[4]) for x in k], "turnover": [float(x[6]) for x in k]})
    kl = kl[(kl.ts * 1000 >= start_ms) & (kl.ts * 1000 < end_ms)].drop_duplicates("ts").sort_values("ts")
    os.makedirs(os.path.dirname(kp), exist_ok=True); os.makedirs(os.path.dirname(fp), exist_ok=True)
    kl.to_parquet(kp, index=False); fr.to_parquet(fp, index=False)
    return bin_sym, "ok", len(kl)


def binance_top100_ever(t_end):
    """Every Binance perp that was ever in the point-in-time top 100 (R4 eligibility, 30-day volume)."""
    sys.path.insert(0, HERE)
    import xsec as X
    p = X.load_panels(XSEC, t_end=t_end)
    dp = X.daily_panels(p); fi = X.factor_inputs(p, dp)
    U = X.universe(fi["elig"], fi["qv30"], top=100)
    return sorted({p["syms"][j] for j in np.unique(U[U >= 0])})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8); ap.add_argument("--end", default="2026-10-01")
    a = ap.parse_args()
    t0 = time.time(); end_ms = int(pd.Timestamp(a.end, tz="UTC").timestamp() * 1000)
    os.makedirs(OUT, exist_ok=True)
    ins = instruments()
    json.dump(ins, open(os.path.join(OUT, "instruments.json"), "w"))
    wanted = binance_top100_ever(a.end)
    m = map_symbols(wanted, [x["symbol"] for x in ins])
    status = {x["symbol"]: x["status"] for x in ins}
    json.dump(dict(map=m, unmapped=sorted(set(wanted) - set(m)), wanted=len(wanted)), open(os.path.join(OUT, "map.json"), "w"), indent=0)
    print(f"bybit USDT perps {len(ins)} ({sum(v == 'Closed' for v in status.values())} closed) | binance top-100 ever {len(wanted)} "
          f"-> mapped {len(m)} ({sum(k != v for k, v in m.items())} by normalised name), unmapped {len(wanted) - len(m)} | {time.time() - t0:.0f}s", flush=True)
    done = 0
    with ThreadPoolExecutor(a.workers) as ex:
        futs = [ex.submit(fetch_one, b, y, end_ms, START_MS) for b, y in sorted(m.items())]
        for fu in as_completed(futs):
            try:
                s, st, n = fu.result()
            except Exception as e:
                print(f"  ERROR {e}", file=sys.stderr, flush=True); continue
            done += 1
            if done % 25 == 0:
                print(f"  {done}/{len(m)} {s}: {st} {n} | {time.time() - t0:.0f}s", flush=True)
    print(f"done {done}/{len(m)} in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
