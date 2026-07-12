"""
Aggregate schema-5 collector bars (binance_live_orderbook_v4.py output, out5.*
files) into wider bars, e.g. 5s -> 15s:

    python aggregate_bars.py --target-sec 15 -o out5.w15.agg.csv out5.w5.*.csv
    python aggregate_bars.py --target-sec 60 --require-full -o day.csv out5.w15.*.csv.gz

The target must be a multiple of the source interval (inferred from the
timestamps). Output keeps the exact input column set and order, including the
aggregation-support columns recomputed for the merged bars, so output files
can be aggregated again (5s -> 15s -> 60s).

Exact under aggregation: every sum/count/min/max/open/close, vwap, price_diff,
mid_std (Chan's parallel-variance formula over mid_m2), mid_rv (boundary terms
from close/open mids), mid_flips (boundary signs), max_buy/sell_run_qty (runs
stitched across bar boundaries via lead/tail runs), largest/first/last trade
side, and all optional sums/maxes.

Approximate by design (would need raw samples): every *_median / *_p90 column
(merged as samples-weighted median of sub-bar medians), the early/late trade
split (proportional allocation at the merged half-count boundary, so counts
may be fractional), and the optional-stream median columns.

Windows with missing source bars (collector outages) aggregate whatever is
present, mirroring what the collector itself writes during a partial-window
outage; --require-full drops them instead.
"""

import argparse
import datetime
import re
import sys

import numpy as np
import pandas as pd

SEPARATOR = ';'
SCHEMA_VERSION = 5

OPT_SUMS = ['long_force_exit_qty_sum', 'short_force_exit_qty_sum',
            'long_force_exit_cnt_sum', 'short_force_exit_cnt_sum',
            'long_force_exit_notional_sum', 'short_force_exit_notional_sum']
OPT_MAXES = ['force_exit_notional_max']
OPT_SAMPLES = ['est_funding_rate_sample', 'funding_rate_sample', 'index_price_sample',
               'long_short_ratio_sample', 'mark_price_sample', 'open_interest_sample',
               'remaining_time_sample', 'spread_sample']
OPT_OCM = ['eth_mid']


def weighted_median(values, weights):
    """Median of `values` weighted by `weights`; None if no positive weight."""
    v = np.asarray(values, dtype=float)
    w = np.asarray(weights, dtype=float)
    mask = w > 0
    if not mask.any():
        return None
    v, w = v[mask], w[mask]
    order = np.argsort(v)
    v, w = v[order], w[order]
    cum = np.cumsum(w)
    return float(v[np.searchsorted(cum, 0.5 * cum[-1])])


def stats_series_bases(cols, market):
    """Series merged as samples/open/close/min/median/max blocks, found via
    their *_samples column (bid, ask, spread, micro_dev, shallow liq levels).
    buy/sell_samples are trade counters, not series."""
    bases = []
    for c in cols:
        mt = re.match(rf'^{market}_(.+)_samples$', c)
        if mt and mt.group(1) not in ('buy', 'sell'):
            bases.append(mt.group(1))
    return bases


def deep_median_bases(cols, market, series_bases):
    """Median-only deep liquidity levels: *_median columns with no matching
    *_samples column and not part of another special block."""
    bases = []
    for c in cols:
        mt = re.match(rf'^{market}_(.+)_median$', c)
        if not mt:
            continue
        b = mt.group(1)
        if b in series_bases or b in ('buy_size', 'sell_size',
                                      'bid_wall_qty', 'bid_wall_dist',
                                      'ask_wall_qty', 'ask_wall_dist'):
            continue
        bases.append(b)
    return bases


def merge_mid(out, g, m):
    """mid_std / mid_rv / mid_flips and their aggregation-support columns."""
    n = g[f'{m}_bid_samples'].to_numpy(dtype=float)
    mid_sum = g[f'{m}_mid_sum'].to_numpy(dtype=float)
    m2s = g[f'{m}_mid_m2'].to_numpy(dtype=float)

    # Chan's parallel variance: exact merged std without cancellation
    N, mean, m2 = 0.0, 0.0, 0.0
    for ni, si, m2i in zip(n, mid_sum, m2s):
        if N == 0:
            N, mean, m2 = ni, si / ni, m2i
        else:
            mi = si / ni
            delta = mi - mean
            m2 = m2 + m2i + delta * delta * N * ni / (N + ni)
            mean = (mean * N + si) / (N + ni)
            N += ni
    out[f'{m}_mid_std'] = float(np.sqrt(m2 / N)) if N > 0 else 0.0
    out[f'{m}_mid_m2'] = m2

    open_mids = (g[f'{m}_bid_open'].to_numpy() + g[f'{m}_ask_open'].to_numpy()) / 2.0
    close_mids = (g[f'{m}_bid_close'].to_numpy() + g[f'{m}_ask_close'].to_numpy()) / 2.0
    rv = float(g[f'{m}_mid_rv'].sum())
    if len(g) > 1:
        rv += float(((open_mids[1:] - close_mids[:-1]) ** 2).sum())
    out[f'{m}_mid_rv'] = rv

    # Flips: within-bar flips are summed; junction flips resolved from the
    # boundary move sign and each bar's first/last non-zero move sign.
    first_signs = g[f'{m}_mid_first_move_sign'].to_numpy(dtype=int)
    last_signs = g[f'{m}_mid_last_move_sign'].to_numpy(dtype=int)
    flips = int(g[f'{m}_mid_flips'].sum())
    carry = 0          # sign of the last non-zero move seen so far
    merged_first = 0
    for i in range(len(g)):
        if i > 0:
            b = int(np.sign(open_mids[i] - close_mids[i - 1]))
            if b != 0:
                if carry != 0 and b != carry:
                    flips += 1
                if merged_first == 0:
                    merged_first = b
                carry = b
        if first_signs[i] != 0:
            if carry != 0 and first_signs[i] != carry:
                flips += 1
            if merged_first == 0:
                merged_first = first_signs[i]
        if last_signs[i] != 0:
            carry = last_signs[i]
    out[f'{m}_mid_flips'] = flips
    out[f'{m}_mid_first_move_sign'] = merged_first
    out[f'{m}_mid_last_move_sign'] = carry


def merge_trades(out, g, m):
    """Sub-bar trade sequencing block plus run/burst columns."""
    bs = g[f'{m}_buy_samples'].to_numpy(dtype=float)
    ss = g[f'{m}_sell_samples'].to_numpy(dtype=float)
    first_side = g[f'{m}_first_trade_side'].to_numpy(dtype=int)
    last_side = g[f'{m}_last_trade_side'].to_numpy(dtype=int)
    lead = g[f'{m}_lead_run_qty'].to_numpy(dtype=float)
    tail = g[f'{m}_tail_run_qty'].to_numpy(dtype=float)
    bq = g[f'{m}_buy_qty'].to_numpy(dtype=float)
    sq = g[f'{m}_sell_qty'].to_numpy(dtype=float)
    trading = np.nonzero(bs + ss > 0)[0]

    out[f'{m}_max_batch_buy_qty'] = float(g[f'{m}_max_batch_buy_qty'].max())
    out[f'{m}_max_batch_sell_qty'] = float(g[f'{m}_max_batch_sell_qty'].max())

    if len(trading) == 0:
        for c in ('first_trade_side', 'last_trade_side', 'largest_trade_side',
                  'buy_count_early', 'buy_count_late', 'sell_count_early', 'sell_count_late'):
            out[f'{m}_{c}'] = 0
        for c in ('largest_trade_qty', 'buy_qty_early', 'buy_qty_late',
                  'sell_qty_early', 'sell_qty_late',
                  'buy_size_median', 'buy_size_p90', 'sell_size_median', 'sell_size_p90',
                  'max_buy_run_qty', 'max_sell_run_qty', 'lead_run_qty', 'tail_run_qty'):
            out[f'{m}_{c}'] = 0.0
        return

    fi, li = trading[0], trading[-1]
    out[f'{m}_first_trade_side'] = int(first_side[fi])
    out[f'{m}_last_trade_side'] = int(last_side[li])
    largest_i = int(g[f'{m}_largest_trade_qty'].to_numpy().argmax())
    out[f'{m}_largest_trade_qty'] = float(g[f'{m}_largest_trade_qty'].iat[largest_i])
    out[f'{m}_largest_trade_side'] = int(g[f'{m}_largest_trade_side'].iat[largest_i])

    # single-run bar: every trade in it is on one side
    def single(i):
        return first_side[i] == last_side[i] and (ss[i] == 0 if first_side[i] == 1 else bs[i] == 0)

    def side_qty(i):
        return bq[i] if first_side[i] == 1 else sq[i]

    # max same-side runs, stitching runs that cross bar boundaries
    maxes = {1: 0.0, -1: 0.0}
    cur_side, cur_qty = 0, 0.0
    for i in trading:
        if cur_side != 0 and first_side[i] == cur_side:
            maxes[cur_side] = max(maxes[cur_side], cur_qty + lead[i])
        maxes[1] = max(maxes[1], float(g[f'{m}_max_buy_run_qty'].iat[i]))
        maxes[-1] = max(maxes[-1], float(g[f'{m}_max_sell_run_qty'].iat[i]))
        if single(i):
            if first_side[i] == cur_side:
                cur_qty += side_qty(i)
            else:
                cur_side, cur_qty = int(first_side[i]), side_qty(i)
            maxes[cur_side] = max(maxes[cur_side], cur_qty)
        else:
            cur_side, cur_qty = int(last_side[i]), float(tail[i])
    out[f'{m}_max_buy_run_qty'] = maxes[1]
    out[f'{m}_max_sell_run_qty'] = maxes[-1]

    # merged lead/tail runs (keep the output aggregatable again)
    def chain(indices, side_arr, run_arr):
        run_side = int(side_arr[indices[0]])
        qty = float(run_arr[indices[0]])
        if single(indices[0]):
            for j in indices[1:]:
                if int(side_arr[j]) != run_side:
                    break
                qty += float(run_arr[j])
                if not single(j):
                    break
        return qty

    out[f'{m}_lead_run_qty'] = chain(trading, first_side, lead)
    out[f'{m}_tail_run_qty'] = chain(trading[::-1], last_side, tail)

    # early/late split at the merged half-count boundary; sub-bar halves are
    # the finest known buckets, so the boundary bucket splits proportionally
    buckets = []
    for i in trading:
        for half in ('early', 'late'):
            hbc = float(g[f'{m}_buy_count_{half}'].iat[i])
            hsc = float(g[f'{m}_sell_count_{half}'].iat[i])
            hbq = float(g[f'{m}_buy_qty_{half}'].iat[i])
            hsq = float(g[f'{m}_sell_qty_{half}'].iat[i])
            if hbc + hsc > 0:
                buckets.append((hbc + hsc, hbc, hsc, hbq, hsq))
    n_total = sum(b[0] for b in buckets)
    half_n = n_total // 2
    early = [0.0, 0.0, 0.0, 0.0]
    cum = 0.0
    for cnt, hbc, hsc, hbq, hsq in buckets:
        take = min(cnt, half_n - cum)
        if take <= 0:
            break
        f = take / cnt
        for k, val in enumerate((hbc, hsc, hbq, hsq)):
            early[k] += f * val
        cum += take
    tot = [float(bs.sum()), float(ss.sum()), float(bq.sum()), float(sq.sum())]
    for k, name in enumerate(('buy_count', 'sell_count', 'buy_qty', 'sell_qty')):
        out[f'{m}_{name}_early'] = early[k]
        out[f'{m}_{name}_late'] = tot[k] - early[k]

    for side, w in (('buy', bs), ('sell', ss)):
        for stat in ('median', 'p90'):
            wm = weighted_median(g[f'{m}_{side}_size_{stat}'].to_numpy(), w)
            out[f'{m}_{side}_size_{stat}'] = wm if wm is not None else 0.0


def merge_market(out, g, m, cols, out_ts):
    out[f'{m}_datetime'] = datetime.datetime.fromtimestamp(out_ts).strftime('%Y-%m-%d %H:%M:%S')
    out[f'{m}_timestamp'] = out_ts

    open_mid = (g[f'{m}_bid_open'].iat[0] + g[f'{m}_ask_open'].iat[0]) / 2.0
    close_mid = (g[f'{m}_bid_close'].iat[-1] + g[f'{m}_ask_close'].iat[-1]) / 2.0
    out[f'{m}_price_diff'] = close_mid - open_mid

    for c in ('buy_qty', 'sell_qty'):
        out[f'{m}_{c}'] = float(g[f'{m}_{c}'].sum())
    for side in ('buy', 'sell'):
        q = g[f'{m}_{side}_qty'].to_numpy(dtype=float)
        v = g[f'{m}_{side}_vwap'].to_numpy(dtype=float)
        tq = q.sum()
        out[f'{m}_{side}_vwap'] = float((q * v).sum() / tq) if tq > 1e-9 else 0.0

    series = stats_series_bases(cols, m)
    for s in series:
        smp = g[f'{m}_{s}_samples'].to_numpy(dtype=float)
        out[f'{m}_{s}_open'] = g[f'{m}_{s}_open'].iat[0]
        out[f'{m}_{s}_close'] = g[f'{m}_{s}_close'].iat[-1]
        out[f'{m}_{s}_min'] = float(g[f'{m}_{s}_min'].min())
        out[f'{m}_{s}_max'] = float(g[f'{m}_{s}_max'].max())
        out[f'{m}_{s}_median'] = weighted_median(g[f'{m}_{s}_median'].to_numpy(), smp)

    bid_samples = g[f'{m}_bid_samples'].to_numpy(dtype=float)
    for b in deep_median_bases(cols, m, series):
        out[f'{m}_{b}_median'] = weighted_median(g[f'{m}_{b}_median'].to_numpy(), bid_samples)

    for side in ('bid', 'ask'):
        cnts = g[f'{m}_{side}_wall_count'].to_numpy(dtype=float)
        if cnts.sum() > 0:
            out[f'{m}_{side}_wall_qty_median'] = weighted_median(
                g[f'{m}_{side}_wall_qty_median'].to_numpy(), cnts)
            out[f'{m}_{side}_wall_qty_max'] = float(g[f'{m}_{side}_wall_qty_max'].max())
            out[f'{m}_{side}_wall_dist_median'] = weighted_median(
                g[f'{m}_{side}_wall_dist_median'].to_numpy(), cnts)
        else:
            out[f'{m}_{side}_wall_qty_median'] = 0.0
            out[f'{m}_{side}_wall_qty_max'] = 0.0
            out[f'{m}_{side}_wall_dist_median'] = -1.0

    merge_mid(out, g, m)
    merge_trades(out, g, m)


def merge_optional(out, g):
    for name in OPT_MAXES:
        out[f'opt_{name}'] = float(g[f'opt_{name}'].max())
    for name in OPT_SAMPLES:
        cnts = g[f'opt_{name}_count'].to_numpy(dtype=float)
        wm = weighted_median(g[f'opt_{name}'].to_numpy(), cnts)
        out[f'opt_{name}'] = wm if wm is not None else -1
    for name in OPT_OCM:
        cnts = g[f'opt_{name}_count'].to_numpy(dtype=float)
        idx = np.nonzero(cnts > 0)[0]
        if len(idx):
            out[f'opt_{name}_open'] = g[f'opt_{name}_open'].iat[idx[0]]
            out[f'opt_{name}_close'] = g[f'opt_{name}_close'].iat[idx[-1]]
            out[f'opt_{name}_median'] = weighted_median(g[f'opt_{name}_median'].to_numpy(), cnts)
        else:
            out[f'opt_{name}_open'] = -1
            out[f'opt_{name}_close'] = -1
            out[f'opt_{name}_median'] = -1


def merge_group(g, cols, out_ts):
    out = {'schema_version': SCHEMA_VERSION}
    for m in ('spot', 'future'):
        merge_market(out, g, m, cols, out_ts)
    merge_optional(out, g)
    # everything else is additive: *_samples, *_sum, *_sumsq, *_count cover
    # trade counters, flow sums/counts, wall and liq sums, optional counts
    for c in cols:
        if c in out:
            continue
        if c.endswith(('_samples', '_sum', '_sumsq', '_count')):
            out[c] = g[c].sum()
        else:
            raise RuntimeError(f'no merge rule for column {c}')
    return [out[c] for c in cols]


def main():
    parser = argparse.ArgumentParser(
        description='Aggregate schema-5 collector bars into wider bars')
    parser.add_argument('inputs', nargs='+', help='out5.* csv or csv.gz files')
    parser.add_argument('--target-sec', type=int, required=True,
                        help='output bar interval; must be a multiple of the source interval')
    parser.add_argument('-o', '--output', required=True, help='output csv path')
    parser.add_argument('--require-full', action='store_true',
                        help='drop output windows that are missing source bars')
    args = parser.parse_args()

    frames = [pd.read_csv(p, sep=SEPARATOR) for p in sorted(args.inputs)]
    cols = list(frames[0].columns)
    for p, f in zip(sorted(args.inputs), frames[1:]):
        if list(f.columns) != cols:
            sys.exit(f'column mismatch in {p}')
    if 'spot_mid_m2' not in cols:
        sys.exit('inputs lack aggregation columns -- schema-5 (out5.*) files '
                 'from binance_live_orderbook_v4.py are required')

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values('spot_timestamp').drop_duplicates('spot_timestamp').reset_index(drop=True)

    ts = df['spot_timestamp'].to_numpy(dtype=np.int64)
    diffs = np.diff(ts)
    if len(diffs) == 0:
        sys.exit('need at least two rows to infer the source interval')
    src = int(pd.Series(diffs).mode().iat[0])
    if args.target_sec % src != 0 or args.target_sec <= src:
        sys.exit(f'target {args.target_sec}s is not a multiple (>1x) of source interval {src}s')
    factor = args.target_sec // src

    rows = []
    partial = 0
    for key, g in df.groupby(ts // args.target_sec * args.target_sec, sort=True):
        if len(g) < factor:
            partial += 1
            if args.require_full:
                continue
        rows.append(merge_group(g, cols, int(key)))

    out_df = pd.DataFrame(rows, columns=cols)
    out_df.to_csv(args.output, sep=SEPARATOR, index=False)
    print(f'{len(df)} rows @ {src}s -> {len(out_df)} rows @ {args.target_sec}s '
          f'({partial} partial windows {"dropped" if args.require_full else "kept"}) '
          f'-> {args.output}')


if __name__ == '__main__':
    main()
