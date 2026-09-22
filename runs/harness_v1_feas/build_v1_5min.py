"""Stage 1 — stream the v1 monthly tars into a single 5-minute bar table.

Reads only the ~27 columns needed for slow-horizon features (out of 784), dedupes
on timestamp across collector-instance handoffs, then aggregates 15s -> 5min.
Caches to data/v1_year/v1_5min.pkl so stage 2 is instant.

Runs on a laptop: ~1,925 files, peak RAM ~500 MB.
"""
import tarfile, io, os, glob, re, sys, time
import numpy as np, pandas as pd

DATA = "/home/nkout/projects/binance_api/data/v1_year"
OUT  = os.path.join(DATA, "v1_5min.pkl")
BAR  = 300  # 5 min

LEVELS = ['0.0', '0.01', '0.05', '0.1', '0.2', '0.4']
PRICE = ['future_bid_close', 'future_ask_close', 'spot_bid_close', 'spot_ask_close']
VOL   = ['future_buy_qty', 'future_sell_qty', 'spot_buy_qty', 'spot_sell_qty']
DEPTH = [f'future_{s}_liq_{l}_median' for l in LEVELS for s in ('bid', 'ask')]
OPT   = ['opt_open_interest_sample', 'opt_long_short_ratio_sample',
         'opt_funding_rate_sample', 'opt_long_force_exit_qty_sum',
         'opt_short_force_exit_qty_sum']
USE   = ['spot_timestamp'] + PRICE + VOL + ['future_spread_median'] + DEPTH + OPT

# how each raw column collapses into a 5-min bar
LAST  = PRICE + ['opt_open_interest_sample', 'opt_long_short_ratio_sample',
                 'opt_funding_rate_sample']
SUM   = VOL + ['opt_long_force_exit_qty_sum', 'opt_short_force_exit_qty_sum']
MEAN  = DEPTH + ['future_spread_median']


def main():
    tars = sorted(glob.glob(os.path.join(DATA, "month.*.tar")))
    assert tars, f"no monthly tars in {DATA}"
    parts, nfile, t0 = [], 0, time.time()
    for t in tars:
        mon = os.path.basename(t).split('.')[1]
        with tarfile.open(t) as tf:
            names = sorted(n for n in tf.getnames() if n.endswith('.csv.gz'))
            for nm in names:
                try:
                    df = pd.read_csv(io.BytesIO(tf.extractfile(nm).read()), sep=';',
                                     compression='gzip', usecols=lambda c: c in set(USE),
                                     low_memory=False)
                except Exception as e:
                    print(f"  skip {nm}: {e}", file=sys.stderr); continue
                missing = [c for c in USE if c not in df.columns]
                if missing:
                    print(f"  skip {nm}: missing {missing[:3]}", file=sys.stderr); continue
                for c in USE:
                    df[c] = pd.to_numeric(df[c], errors='coerce')
                parts.append(df[USE].astype(np.float64))
                nfile += 1
        print(f"{mon}: {len(names):>4} files  (running total {nfile:,}, "
              f"{time.time()-t0:.0f}s)", flush=True)

    raw = pd.concat(parts, ignore_index=True); del parts
    print(f"\nraw 15s rows: {len(raw):,}")

    # sentinel -1 from the dead-REST era -> NaN, so it never enters a feature
    for c in OPT:
        if c != 'opt_open_interest_sample':
            raw.loc[raw[c] == -1, c] = np.nan
    raw.loc[raw['opt_open_interest_sample'] <= 0, 'opt_open_interest_sample'] = np.nan

    raw = raw.dropna(subset=['spot_timestamp'])
    raw['spot_timestamp'] = raw['spot_timestamp'].astype(np.int64)
    before = len(raw)
    raw = raw.sort_values('spot_timestamp').drop_duplicates('spot_timestamp', keep='last')
    print(f"deduped on timestamp: {before:,} -> {len(raw):,} "
          f"({before-len(raw):,} overlapping rows from instance handoffs)")

    raw['bar'] = (raw['spot_timestamp'] // BAR) * BAR
    agg = {c: 'last' for c in LAST}
    agg.update({c: 'sum' for c in SUM})
    agg.update({c: 'mean' for c in MEAN})
    agg['spot_timestamp'] = 'count'
    g = raw.groupby('bar').agg(agg).rename(columns={'spot_timestamp': 'n_ticks'})

    # a 5-min bar should hold 20 ticks at 15s; keep only bars that are mostly complete
    full = (g['n_ticks'] >= 15).sum()
    print(f"5-min bars: {len(g):,}  ({full:,} with >=15/20 ticks, "
          f"{full/len(g)*100:.1f}%)")
    g = g[g['n_ticks'] >= 15].copy()

    g['dt'] = pd.to_datetime(g.index, unit='s', utc=True)
    span = (g.index.max() - g.index.min()) / 86400
    print(f"span: {g['dt'].iloc[0]} -> {g['dt'].iloc[-1]}  ({span:.1f} days)")
    print(f"coverage: {len(g):,} bars / {span*288:.0f} possible = {len(g)/(span*288)*100:.1f}%")
    for c in OPT:
        print(f"  {c:<34} non-null {g[c].notna().mean()*100:5.1f}%")

    g.to_pickle(OUT)
    print(f"\nwrote {OUT}  ({os.path.getsize(OUT)/1e6:.1f} MB, {len(g):,} rows x {g.shape[1]} cols)")


if __name__ == "__main__":
    main()
