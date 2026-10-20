"""Which collector rows hold real liquidation data (the opt_*force_exit* columns).

Every collector takes liquidations from <symbol>@forceOrder. When Binance retired the legacy /ws/ path for /market
streams, that stream went silent and the collectors kept writing 0: "not collected", which no column tells apart from
"no liquidation in this bar" (runs/new_features.md §0).

binance_live_orderbook_v6.py reads the routed path and writes LIVE_COL in every bar: 1 = the liquidation feed was
verified live for the whole bar, 0 = not verified. mask_dead_liquidations() sets the liquidation columns to NaN on
the rows that are not data:
  rows with LIVE_COL (v6)       unless it is 1;
  rows without it (v1-v5)       from LIQ_DEAD_FROM on: the first 15 s bar of the v1 year with no markPrice sample
                                (same /market stream group); the last liquidation before it is in the same minute.
                                The only later ones, 42 rows on 2026-04-27 at about a sixth of the normal rate, are
                                treated as dead too.
A row has no LIVE_COL when its file lacks the column, or holds NaN there after v5 and v6 files are concatenated.

    python liq_cutoff.py out5.w5.*.csv.gz    # per file: span, live rows (v6), rows with liquidations, the first one (UTC)
"""
import re, sys
import numpy as np, pandas as pd

LIQ_DEAD_FROM = 1776963810      # 2026-04-23 17:03:30 UTC
LIVE_COL = "opt_force_order_live"
FIX_DAY = 1792195200            # 2026-10-17 00:00 UTC: no collector on the fixed route can be earlier
LIQ_RE = re.compile(r"force_exit")


def liq_columns(df):
    return [c for c in df.columns if LIQ_RE.search(c)]


def dead_mask(ts, live=None):
    """True where the liquidation columns are not data. ts: epoch seconds (UTC); an unknown (NaN) time counts as dead.
    live: LIVE_COL per row (NaN where a row has none), or None when no row has it."""
    t = np.asarray(ts, dtype=float)
    dead = ~(t < LIQ_DEAD_FROM)
    if live is None:
        return dead
    f = np.asarray(live, dtype=float)
    return np.where(np.isnan(f), dead, f != 1)


def mask_dead_liquidations(df, ts):
    """Set the liquidation columns of df to NaN on dead rows, in place; returns df. ts: epoch seconds per row."""
    cols = liq_columns(df)
    if not cols:
        return df
    df[cols] = df[cols].astype(float)                     # count columns may load as int
    live = pd.to_numeric(df[LIVE_COL], errors="coerce").to_numpy(float) if LIVE_COL in df.columns else None
    dead = dead_mask(ts, live)
    t = np.asarray(ts, dtype=float)
    no_flag = np.ones(len(df), bool) if live is None else np.isnan(live)
    seen = no_flag & (t >= FIX_DAY) & (df[cols].to_numpy() > 0).any(axis=1)
    if seen.any():
        first = pd.to_datetime(t[seen].min(), unit="s", utc=True)
        print(f"WARNING: {int(seen.sum())} rows without {LIVE_COL} hold liquidations after the forceOrder fix "
              f"(first {first}) but are masked: a v1-v5 collector cannot tell a dead feed from a quiet one -- "
              f"collect with binance_live_orderbook_v6.py", file=sys.stderr)
    df.loc[dead, cols] = np.nan
    return df


def scan(paths):
    """Per collector file: span, rows marked live (v6), rows with liquidations, the first one."""
    utc = lambda s: pd.to_datetime(s, unit="s", utc=True).strftime("%Y-%m-%d %H:%M:%S")
    for p in sorted(paths):
        df = pd.read_csv(p, sep=";", usecols=lambda c: c in ("spot_timestamp", LIVE_COL) or bool(LIQ_RE.search(c)))
        df = df.apply(pd.to_numeric, errors="coerce").dropna(subset=["spot_timestamp"])
        hit = df[liq_columns(df)].gt(0).any(axis=1)
        first = utc(df.spot_timestamp[hit].min()) if hit.any() else "-"
        live = f"live {int((df[LIVE_COL] == 1).sum()):>6}" if LIVE_COL in df.columns else "live      -"
        print(f"{p}  {utc(df.spot_timestamp.min())} -> {utc(df.spot_timestamp.max())}  rows {len(df):>6}  {live}  "
              f"with liquidations {int(hit.sum()):>5}  first {first}")


if __name__ == "__main__":
    scan(sys.argv[1:])
