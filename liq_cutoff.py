"""Which collector rows hold real liquidation data (the opt_*force_exit* columns).

Every collector takes liquidations from <symbol>@forceOrder. When Binance retired the legacy /ws/ path for /market
streams, that stream went silent and the collectors kept writing 0: "not collected", which no column tells apart from
"no liquidation in this bar" (runs/new_features.md §0). The routed /market/ws/ path (cfdbb1e, 2026-10-17) brings it
back, but only for a collector restarted on that code.

Rows with a timestamp in [LIQ_DEAD_FROM, LIQ_LIVE_FROM) are not data; mask_dead_liquidations() sets their liquidation
columns to NaN.
  LIQ_DEAD_FROM  the first 15 s bar of the v1 year with no markPrice sample (same /market stream group); the last
                 liquidation before it is in the same minute. The only later ones, 42 rows on 2026-04-27 at about a
                 sixth of the normal rate, are treated as dead too.
  LIQ_LIVE_FROM  the restart of the collector on the fixed code. None until that time is known: every row from
                 LIQ_DEAD_FROM on stays masked, and mask_dead_liquidations() warns (stderr) when rows after the fix hold
                 liquidations (the collector has been restarted -> pin LIQ_LIVE_FROM).

    python liq_cutoff.py out5.w5.*.csv.gz    # per file: span, rows with liquidations, the first one (UTC)
"""
import re, sys
import numpy as np, pandas as pd

LIQ_DEAD_FROM = 1776963810      # 2026-04-23 17:03:30 UTC
LIQ_LIVE_FROM = None            # epoch seconds (UTC) of the collector restart on the fixed code, once known
FIX_DAY = 1792195200            # 2026-10-17 00:00 UTC: no restart on the fixed code can be earlier
LIQ_RE = re.compile(r"force_exit")


def liq_columns(df):
    return [c for c in df.columns if LIQ_RE.search(c)]


def dead_mask(ts):
    """True where the liquidation columns are not data. ts: epoch seconds (UTC); an unknown (NaN) time counts as dead."""
    t = np.asarray(ts, dtype=float)
    live_from = np.inf if LIQ_LIVE_FROM is None else LIQ_LIVE_FROM
    return ~(t < LIQ_DEAD_FROM) & ~(t >= live_from)


def mask_dead_liquidations(df, ts):
    """Set the liquidation columns of df to NaN on dead rows, in place; returns df. ts: epoch seconds per row."""
    cols = liq_columns(df)
    if not cols:
        return df
    df[cols] = df[cols].astype(float)                     # count columns may load as int
    dead = dead_mask(ts)
    t = np.asarray(ts, dtype=float)
    seen = dead & (t >= FIX_DAY) & (df[cols].to_numpy() > 0).any(axis=1)
    if seen.any():
        first = pd.to_datetime(t[seen].min(), unit="s", utc=True)
        print(f"WARNING: {int(seen.sum())} rows after the forceOrder fix hold liquidations (first {first}) but are masked: "
              f"the collector was restarted on the fixed code -- set liq_cutoff.LIQ_LIVE_FROM to the restart time",
              file=sys.stderr)
    df.loc[dead, cols] = np.nan
    return df


def scan(paths):
    """Per collector file: span, rows with liquidations, the first one -- to find the restart and pin LIQ_LIVE_FROM."""
    utc = lambda s: pd.to_datetime(s, unit="s", utc=True).strftime("%Y-%m-%d %H:%M:%S")
    for p in sorted(paths):
        df = pd.read_csv(p, sep=";", usecols=lambda c: c == "spot_timestamp" or bool(LIQ_RE.search(c)))
        df = df.apply(pd.to_numeric, errors="coerce").dropna(subset=["spot_timestamp"])
        hit = df[liq_columns(df)].gt(0).any(axis=1)
        first = utc(df.spot_timestamp[hit].min()) if hit.any() else "-"
        print(f"{p}  {utc(df.spot_timestamp.min())} -> {utc(df.spot_timestamp.max())}  rows {len(df):>6}  "
              f"with liquidations {int(hit.sum()):>5}  first {first}")


if __name__ == "__main__":
    scan(sys.argv[1:])
