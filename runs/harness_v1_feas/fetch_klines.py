"""Download BTCUSDT USDT-M perp 5-minute klines back to listing (2019-09-08).

Same instrument and cadence as the v1 collector, so §8.3's rule transfers unchanged.
Weight-aware: limit=1500 costs weight 10 on fapi (2400/min budget), so we pace at
~3 req/s. Resumes from the cache if interrupted.
"""
import urllib.request, json, time, os, sys
import numpy as np, pandas as pd

OUT = "/home/nkout/projects/binance_api/data/btcusdt_5m_klines.pkl"
URL = ("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m"
       "&limit=1500&startTime={}")
START = int(pd.Timestamp("2019-09-08", tz="UTC").timestamp() * 1000)
STEP_MS = 1500 * 5 * 60 * 1000


def get(url, tries=5):
    for k in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.load(r)
        except Exception as e:
            if k == tries - 1:
                raise
            time.sleep(2 ** k)


def main():
    rows = []
    if os.path.exists(OUT):
        old = pd.read_pickle(OUT)
        rows = old.reset_index().values.tolist()
        t = int(old.index.max() * 1000) + 300_000
        print(f"resuming from cache: {len(old):,} bars, next {pd.Timestamp(t, unit='ms')}")
    else:
        t = START
    now = int(time.time() * 1000)
    n0, t0 = len(rows), time.time()
    while t < now:
        d = get(URL.format(t))
        if not d:
            t += STEP_MS; continue
        for c in d:
            # openTime, o,h,l,c, volume, closeTime, quoteVol, trades, takerBuyBase, ...
            rows.append([c[0] // 1000, float(c[1]), float(c[2]), float(c[3]),
                         float(c[4]), float(c[5]), int(c[8]), float(c[9])])
        t = d[-1][0] + 300_000
        if len(rows) % 30000 < 1500:
            print(f"  {pd.Timestamp(t, unit='ms'):%Y-%m-%d}  {len(rows):,} bars  "
                  f"{time.time()-t0:.0f}s", flush=True)
        time.sleep(0.35)

    df = pd.DataFrame(rows, columns=['ts', 'open', 'high', 'low', 'close',
                                     'volume', 'trades', 'taker_buy_base'])
    df = df.drop_duplicates('ts').sort_values('ts').set_index('ts')
    df.to_pickle(OUT)
    span = (df.index.max() - df.index.min()) / 86400
    exp = span * 288
    print(f"\nwrote {OUT}")
    print(f"  {len(df):,} bars  {pd.Timestamp(df.index.min(),unit='s'):%Y-%m-%d} -> "
          f"{pd.Timestamp(df.index.max(),unit='s'):%Y-%m-%d}  ({span:.0f} days)")
    print(f"  coverage {len(df)/exp*100:.2f}% of {exp:,.0f} possible 5-min bars")
    print(f"  added {len(df)-n0:,} this run in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
