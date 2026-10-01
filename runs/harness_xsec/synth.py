"""Synthetic perp (+ optional spot) market in the data/xsec on-disk format, for the R4 / R5 tests."""
import os
import numpy as np, pandas as pd


def make_market(root, n=60, days=2300, kappa=0.0, seed=0, start="2020-03-01", spot=False, funding=None, basis_sd=0.0):
    """Hourly random walks with an optional planted daily reversal: drift of day d+1 = -kappa * return of day d."""
    rng = np.random.default_rng(seed)
    t0 = int(pd.Timestamp(start, tz="UTC").timestamp())
    sig_h = rng.uniform(0.004, 0.009, n)
    lvl = rng.lognormal(16, 1.0, n)
    listed = np.zeros(n, int); listed[50:] = rng.integers(200, 500, n - 50)
    delist = np.full(n, days * 24); delist[5] = 600 * 24 + 13              # coin 5 dies at 13:00 on day 600
    R = sig_h[None, None, :] * rng.standard_normal((days, 24, n))
    prev = np.zeros(n)
    for d in range(days):                                                  # planted reversal, day by day
        R[d] += -kappa * prev / 24
        prev = R[d].sum(0)
    path = np.cumsum(R.reshape(days * 24, n), 0)
    hours = np.arange(days * 24)
    os.makedirs(os.path.join(root, "klines"), exist_ok=True); os.makedirs(os.path.join(root, "funding"), exist_ok=True)
    names = [f"C{i:02d}USDT" for i in range(n)]
    for i, s in enumerate(names):
        m = (hours >= listed[i] * 24) & (hours < delist[i]); c = 100 * np.exp(path[m, i])
        k = pd.DataFrame({"ts": t0 + hours[m] * 3600, "open": c, "high": c, "low": c, "close": c,
                          "quote_volume": lvl[i] * rng.lognormal(0, 0.3, m.sum()), "taker_buy_quote": 0.0, "trades": 100})
        k.to_parquet(os.path.join(root, "klines", f"{s}.parquet"), index=False)
        ft = np.arange(t0 + listed[i] * 86400, t0 + min(days * 86400, delist[i] * 3600), 8 * 3600)
        if funding is not None and i in funding:
            rate = np.full(len(ft), funding[i])
        else:
            rate = np.full(len(ft), 0.001) if i == 7 else 0.0001 + 0.00005 * rng.standard_normal(len(ft))
        pd.DataFrame({"ts": ft, "rate": rate}).to_parquet(os.path.join(root, "funding", f"{s}.parquet"), index=False)
        if spot and i != 9:                                                 # coin 9 has no spot pair
            os.makedirs(os.path.join(root, "spot"), exist_ok=True)
            cs = c * np.exp(basis_sd * rng.standard_normal(len(c)))
            sk = k.assign(close=cs, open=cs, high=cs, low=cs, quote_volume=1e7)
            sk.to_parquet(os.path.join(root, "spot", f"{s}.parquet"), index=False)
    # a stablecoin perp that must be excluded at load
    k = pd.DataFrame({"ts": t0 + hours * 3600, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
                      "quote_volume": 1e12, "taker_buy_quote": 0.0, "trades": 1})
    k.to_parquet(os.path.join(root, "klines", "USDCUSDT.parquet"), index=False)
    pd.DataFrame({"ts": [], "rate": []}).to_parquet(os.path.join(root, "funding", "USDCUSDT.parquet"), index=False)
    return t0, names, listed


