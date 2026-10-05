"""Fetch Deribit DVOL (BTC 30-day implied vol index) daily OHLC from the public API -> data/dvol_daily.parquet."""
import json, os, sys, time, urllib.request
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "..", "data", "dvol_daily.parquet")
URL = "https://www.deribit.com/api/v2/public/get_volatility_index_data?currency=BTC&start_timestamp={a}&end_timestamp={b}&resolution=1D"


def fetch(start="2021-03-01", end=None):
    a = int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)
    end = int((pd.Timestamp(end, tz="UTC") if end else pd.Timestamp.now(tz="UTC")).timestamp() * 1000)
    rows = []
    while a < end:
        b = min(a + 300 * 86400 * 1000, end)
        for attempt in range(5):
            try:
                r = json.load(urllib.request.urlopen(URL.format(a=a, b=b), timeout=30))["result"]; break
            except Exception as e:
                time.sleep(2 ** attempt)
        else:
            raise SystemExit("fetch failed")
        rows += r["data"]
        a = (r["data"][-1][0] + 86400 * 1000) if r["data"] else b + 1
    df = pd.DataFrame(rows, columns=["ts_ms", "open", "high", "low", "close"]).drop_duplicates("ts_ms").sort_values("ts_ms")
    df["ts"] = df.ts_ms // 1000
    return df[["ts", "open", "high", "low", "close"]].reset_index(drop=True)


if __name__ == "__main__":
    df = fetch()
    os.makedirs(os.path.dirname(OUT), exist_ok=True); df.to_parquet(OUT, index=False)
    d = pd.to_datetime(df.ts, unit="s")
    print(len(df), "days", d.iloc[0].date(), "->", d.iloc[-1].date(), "| gaps:", int((d.diff().dropna() != pd.Timedelta(days=1)).sum()),
          "| close min/mean/max", df.close.min(), round(df.close.mean(), 1), df.close.max())
