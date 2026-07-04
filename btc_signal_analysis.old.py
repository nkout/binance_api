"""
BTC Orderbook Signal Analysis
Loads all CSV files in a directory and runs full signal quality analysis.

Usage:
    python btc_signal_analysis.py                              # all data, threshold $5, horizon 1 bar
    python btc_signal_analysis.py --days 14                    # only last 14 days of data
    python btc_signal_analysis.py --threshold 10               # custom threshold
    python btc_signal_analysis.py --horizon 8                  # predict 8 bars (2 min) ahead
    python btc_signal_analysis.py --dir /path/to/csvs          # custom folder
"""

import os
import sys
import argparse
import glob
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.colors import LinearSegmentedColormap
from datetime import datetime, timedelta

try:
    from sklearn.feature_selection import mutual_info_classif
    from sklearn.linear_model import LogisticRegression
    from sklearn.dummy import DummyClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import classification_report, confusion_matrix
    SKLEARN_OK = True
except ImportError:
    SKLEARN_OK = False
    print("Warning: scikit-learn not found. Install with: pip install scikit-learn")
    print("Continuing with basic analysis only.\n")

try:
    from xgboost import XGBClassifier
    XGBOOST_OK = True
except ImportError:
    XGBOOST_OK = False
    print("Warning: xgboost not found. Install with: pip install xgboost")
    print("XGBoost comparison will be skipped.\n")


# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────

SEPARATOR = ';'

# Features to analyse
# Only features with confirmed signal in lag correlation analysis are kept.
# Dropped (no signal at 15s horizon): basis, spread, funding, est_funding,
#   futures_premium, ls_ratio.
FEATURE_DEFS = {
    # ── Confirmed signal (lag 0 corr > 0.02) ──────────────────────────────
    'book_imbalance':       'Bid vs ask size at best level (strongest signal)',
    'flow_imbalance':       'Buy vs sell volume ratio (confirmed signal)',

    # ── Borderline — kept, let MI decide ──────────────────────────────────
    'momentum':             'Price change open→close within this bar',

    # ── Deep book imbalance ────────────────────────────────────────────────
    'book_imbal_deep':      'Bid vs ask imbalance at ~0.05% depth',

    # ── Rolling versions — capture sustained pressure, not single-bar spikes
    'flow_imbal_roll4':     'Flow imbalance smoothed over last 4 bars (60s)',
    'flow_imbal_roll8':     'Flow imbalance smoothed over last 8 bars (2min)',
    'book_imbal_roll4':     'Book imbalance smoothed over last 4 bars (60s)',
    'book_imbal_roll8':     'Book imbalance smoothed over last 8 bars (2min)',

    # ── Additional microstructure features ────────────────────────────────
    'vwap_spread':          'Buy VWAP minus sell VWAP (aggression asymmetry)',
    'liq_flag':             'Liquidation occurred this bar (binary)',

    # ── Intra-bar price structure ──────────────────────────────────────────
    'stochastic':           'Where price closed within bar range (0=bottom 1=top)',
    'spread_expansion':     'Spread widening during bar (max minus open)',
    'sample_imbalance':     'Trade count imbalance: buy trades vs sell trades',

    # ── Cross-market signals ───────────────────────────────────────────────
    'flow_agreement':       'Spot and futures flow in same direction (positive=agree)',

    # ── Open interest momentum ─────────────────────────────────────────────
    'oi_change':            'Change in open interest vs previous bar',
    # mark_index_basis dropped: opt_mark_price_sample returns -1 (not collected)

    # ── Trade aggression ───────────────────────────────────────────────────
    'size_imbalance':       'Avg buy trade size minus avg sell trade size',

    # ── Book shape ─────────────────────────────────────────────────────────
    'liq_conc_bid':         'Bid liquidity concentration at touch vs total depth',
    'liq_conc_ask':         'Ask liquidity concentration at touch vs total depth',

    # ── Time-of-day (cyclic encoding) ──────────────────────────────────────
    'hour_sin':             'Hour of day — sine component (cyclic)',
    'hour_cos':             'Hour of day — cosine component (cyclic)',
    'minute_sin':           'Minute of hour — sine component (cyclic)',
    'minute_cos':           'Minute of hour — cosine component (cyclic)',

    # ── Funding event proximity ────────────────────────────────────────────
    'near_funding':         'Within 15 min of 8h funding payment (binary)',
    'funding_pressure':     'Funding rate x near_funding interaction',

    # ── Normalised volatility ──────────────────────────────────────────────
    'vol_norm':             'Current vol relative to 24h rolling average',
}

LAGS_TO_CHECK = [0, 1, 2, 3, 4, 6, 8, 12]   # in windows (multiply by 15s)
WINDOW_SEC    = 15


# ─────────────────────────────────────────────
#  FILE LOADING
# ─────────────────────────────────────────────

def find_csv_files(directory: str) -> list[str]:
    """Find all CSV and gzipped CSV (.csv.gz) files in directory."""
    patterns = ['*.csv', '*.csv.gz']
    files = []
    for pattern in patterns:
        files.extend(glob.glob(os.path.join(directory, pattern)))
    return sorted(files)


def load_files(files: list[str]) -> pd.DataFrame:
    """Load and concatenate CSV files, handling header repetition."""
    frames = []
    for path in files:
        try:
            df = pd.read_csv(path, sep=SEPARATOR, low_memory=False)
            # Some files may repeat the header as a data row
            if 'spot_datetime' in df.columns:
                df = df[df['spot_datetime'] != 'spot_datetime']
            frames.append(df)
            print(f"  Loaded {os.path.basename(path):40s}  {len(df):>5} rows")
        except Exception as e:
            print(f"  SKIP {os.path.basename(path)}: {e}")

    if not frames:
        raise RuntimeError("No valid CSV files found.")

    combined = pd.concat(frames, ignore_index=True)
    return combined


# ─────────────────────────────────────────────
#  DATA PREPARATION
# ─────────────────────────────────────────────

def prepare(df: pd.DataFrame, threshold: float, horizon: int = 1) -> pd.DataFrame:
    """
    Parse, sort, build features and target.

    TARGET DEFINITION — no lookahead bias:
        Target for row N = direction of (close_price[N+1] - close_price[N])
        close_price[N] = (future_bid_close[N] + future_ask_close[N]) / 2
        This is the LAST traded mid inside window N, i.e. the closing price
        of that 15-second bar. It is fully known when window N ends.
        Only close prices are used — no other fields from row N+1.

    FEATURE RULE:
        Every feature must be computable from rows 0..N only.
        No field that aggregates or summarises any part of window N+1 is allowed.
        All fields used below are from the CURRENT row (row N).
    """

    # Parse datetimes
    df['dt'] = pd.to_datetime(df['spot_datetime'], errors='coerce')
    df = df.dropna(subset=['dt']).sort_values('dt').reset_index(drop=True)

    # Drop duplicate timestamps — can happen if CSV files have overlapping ranges
    n_before = len(df)
    df = df.drop_duplicates(subset='dt', keep='first').reset_index(drop=True)
    if len(df) < n_before:
        print(f"  Dropped {n_before - len(df)} duplicate timestamps")

    # Numeric conversion for all non-datetime columns
    skip = {'spot_datetime', 'future_datetime', 'dt'}
    for col in df.columns:
        if col not in skip:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Replace sentinel -1 with NaN for optional fields
    opt_cols = [c for c in df.columns if c.startswith('opt_')]
    for col in opt_cols:
        df[col] = df[col].replace(-1, np.nan)

    # ── Close price of each window ─────────────────────────────────────────
    # Use the closing bid/ask of the futures bar — the last snapshot price
    # inside that 15-second window. Falls back to median if close is missing.
    if 'future_bid_close' in df.columns and 'future_ask_close' in df.columns:
        df['close_price'] = (df['future_bid_close'] + df['future_ask_close']) / 2.0
    else:
        print("  Warning: future_bid_close/future_ask_close not found, falling back to median")
        df['close_price'] = (df['future_bid_median'] + df['future_ask_median']) / 2.0

    # ── Target: close price N+horizon vs close price N ──────────────────────
    # shift(-horizon) pulls ONLY the close price of the bar being predicted.
    # No other column from any future row is touched.
    next_close = df['close_price'].shift(-horizon)
    delta      = next_close - df['close_price']

    df['target']    = np.where(delta >  threshold,  1,
                      np.where(delta < -threshold, -1, 0))
    df['delta_usd'] = delta

    # ── Features — all from current row only ──────────────────────────────
    # Features selected based on lag correlation analysis:
    #   Kept  : book_imbalance (+0.086), flow_imbalance (+0.065) — confirmed signal
    #   Kept  : momentum (+0.008) — borderline, MI will decide
    #   Dropped: basis, spread, funding, est_funding, futures_premium, ls_ratio
    #            (all showed near-zero correlation at every lag)

    eps = 1e-9

    # ── Confirmed signal features ──────────────────────────────────────────

    # Order flow: aggressive buy vs sell pressure within this bar
    df['flow_imbalance'] = (
        (df['future_buy_qty'] - df['future_sell_qty']) /
        (df['future_buy_qty'] + df['future_sell_qty'] + eps)
    )

    # Book imbalance at touch: bid vs ask resting size at best level
    bid0 = df['future_bid_liq_0.0_median']
    ask0 = df['future_ask_liq_0.0_median']
    df['book_imbalance'] = (bid0 - ask0) / (bid0 + ask0 + eps)

    # Momentum: price change open→close within this bar (fully in row N)
    df['momentum'] = df['future_price_diff']

    # ── Deep book imbalance ────────────────────────────────────────────────
    # Uses ~0.05% depth level — captures resting orders beyond the touch.
    # Falls back to 0.04 or 0.06 if 0.05 is missing.
    deep_col = None
    for lvl in ['0.05', '0.04', '0.06']:
        b = f'future_bid_liq_{lvl}_median'
        a = f'future_ask_liq_{lvl}_median'
        if b in df.columns and a in df.columns:
            deep_col = (b, a)
            break
    if deep_col:
        bid_d = df[deep_col[0]]
        ask_d = df[deep_col[1]]
        df['book_imbal_deep'] = (bid_d - ask_d) / (bid_d + ask_d + eps)
    else:
        df['book_imbal_deep'] = np.nan

    # ── Rolling features — sustained pressure over multiple bars ───────────
    # A sustained imbalance is a stronger signal than a single-bar spike.
    # All rolling values use only past + current rows (no lookahead).
    df['flow_imbal_roll4'] = df['flow_imbalance'].rolling(4, min_periods=2).mean()
    df['flow_imbal_roll8'] = df['flow_imbalance'].rolling(8, min_periods=4).mean()
    df['book_imbal_roll4'] = df['book_imbalance'].rolling(4, min_periods=2).mean()
    df['book_imbal_roll8'] = df['book_imbalance'].rolling(8, min_periods=4).mean()

    # ── Additional microstructure features ────────────────────────────────

    # VWAP spread: did buyers pay more than sellers received?
    # Positive → buyers more aggressive; negative → sellers more aggressive
    if 'future_buy_vwap' in df.columns and 'future_sell_vwap' in df.columns:
        df['vwap_spread'] = df['future_buy_vwap'] - df['future_sell_vwap']
    else:
        df['vwap_spread'] = np.nan

    # Liquidation flag: binary — did any forced liquidation happen this bar?
    # Liquidations are impactful but rare; treat as binary not quantity.
    liq_long  = df.get('opt_long_force_exit_qty_sum',  pd.Series(0, index=df.index))
    liq_short = df.get('opt_short_force_exit_qty_sum', pd.Series(0, index=df.index))
    df['liq_flag'] = ((liq_long + liq_short) > 0).astype(float)

    # ── Intra-bar price structure ────────────────────────

    # Stochastic: where did price close within the bar high-low range?
    # 1.0 = closed at top (bullish), 0.0 = closed at bottom (bearish)
    if all(c in df.columns for c in ['future_bid_max', 'future_bid_min', 'future_bid_close']):
        bar_range = df['future_bid_max'] - df['future_bid_min']
        df['stochastic'] = np.where(
            bar_range > 0,
            (df['future_bid_close'] - df['future_bid_min']) / bar_range,
            0.5
        )
    else:
        df['stochastic'] = np.nan

    # Spread expansion: did bid-ask spread widen during the bar?
    # Widening = liquidity pulling back = uncertainty / imminent move
    if all(c in df.columns for c in ['future_spread_max', 'future_spread_open']):
        df['spread_expansion'] = df['future_spread_max'] - df['future_spread_open']
    else:
        df['spread_expansion'] = np.nan

    # Sample imbalance: trade count ratio, independent of volume size
    # High buy_samples vs sell_samples = many small buys = retail accumulation
    if all(c in df.columns for c in ['future_buy_samples', 'future_sell_samples']):
        bs = df['future_buy_samples']
        ss = df['future_sell_samples']
        df['sample_imbalance'] = (bs - ss) / (bs + ss + eps)
    else:
        df['sample_imbalance'] = np.nan

    # ── Cross-market signals ───────────────────────────

    # Flow agreement: spot and futures flow in same direction?
    # Positive = both agree (stronger signal), negative = diverge (arbitrage)
    if all(c in df.columns for c in ['spot_buy_qty', 'spot_sell_qty']):
        spot_flow = (
            (df['spot_buy_qty'] - df['spot_sell_qty']) /
            (df['spot_buy_qty'] + df['spot_sell_qty'] + eps)
        )
        df['flow_agreement'] = df['flow_imbalance'] * spot_flow
    else:
        df['flow_agreement'] = np.nan

    # mark_index_basis dropped: opt_mark_price_sample returns sentinel -1
    # (Binance mark price not being collected in current script version)

    # ── Open interest momentum ─────────────────────────

    # OI change: rising OI + rising price = new longs entering (conviction)
    # Falling OI + rising price = short covering (weaker signal)
    # diff() uses only current and previous row -- no lookahead
    if 'opt_open_interest_sample' in df.columns:
        df['oi_change'] = df['opt_open_interest_sample'].diff()
    else:
        df['oi_change'] = np.nan

    # ── Trade aggression ──────────────────────────────────────────────────

    # Average trade size per side: large avg buy size = institutional aggression
    # distinct from flow_imbalance (volume) and sample_imbalance (count)
    if all(c in df.columns for c in ['future_buy_samples', 'future_sell_samples',
                                      'future_buy_qty', 'future_sell_qty']):
        avg_buy  = df['future_buy_qty']  / (df['future_buy_samples']  + eps)
        avg_sell = df['future_sell_qty'] / (df['future_sell_samples'] + eps)
        df['size_imbalance'] = avg_buy - avg_sell
    else:
        df['size_imbalance'] = np.nan

    # ── Book shape ────────────────────────────────────────────────────────

    # Liquidity concentration: fraction of total book depth sitting at the touch.
    # High concentration = thin book behind it = price can move fast if hit.
    # Uses deepest available liq level as proxy for total depth.
    deep_bid_col = next((c for c in [
        'future_bid_liq_0.4_median', 'future_bid_liq_0.3_median',
        'future_bid_liq_0.2_median', 'future_bid_liq_0.1_median',
    ] if c in df.columns), None)
    deep_ask_col = next((c for c in [
        'future_ask_liq_0.4_median', 'future_ask_liq_0.3_median',
        'future_ask_liq_0.2_median', 'future_ask_liq_0.1_median',
    ] if c in df.columns), None)

    if deep_bid_col and 'future_bid_liq_0.0_median' in df.columns:
        df['liq_conc_bid'] = (
            df['future_bid_liq_0.0_median'] / (df[deep_bid_col] + eps)
        )
    else:
        df['liq_conc_bid'] = np.nan

    if deep_ask_col and 'future_ask_liq_0.0_median' in df.columns:
        df['liq_conc_ask'] = (
            df['future_ask_liq_0.0_median'] / (df[deep_ask_col] + eps)
        )
    else:
        df['liq_conc_ask'] = np.nan

    # ── Time-of-day (cyclic encoding) ─────────────────────────────────────

    # Sine/cosine so that 23:59 and 00:00 are treated as close in time.
    # Sessions: Asia 00-08 UTC, Europe 08-13 UTC, US 13-21 UTC.
    # All derived from the bar's own timestamp — no future information.
    hour   = df['dt'].dt.hour
    minute = df['dt'].dt.minute
    df['hour_sin']   = np.sin(2 * np.pi * hour   / 24)
    df['hour_cos']   = np.cos(2 * np.pi * hour   / 24)
    df['minute_sin'] = np.sin(2 * np.pi * minute / 60)
    df['minute_cos'] = np.cos(2 * np.pi * minute / 60)

    # ── Funding event proximity ────────────────────────────────────────────

    # Binance perpetual funding fires every 8 hours (00:00, 08:00, 16:00 UTC).
    # In the ~15 min before each payment, price systematically squeezes
    # the losing side. near_funding captures this window.
    # Uses bar timestamp only — no future data.
    seconds_in_cycle = (hour * 3600 + minute * 60 + df['dt'].dt.second) % (8 * 3600)
    remaining        = (8 * 3600) - seconds_in_cycle
    df['near_funding'] = (remaining < 900).astype(float)  # <15 min to funding

    # Interaction: near funding AND large funding rate = stronger squeeze pressure
    if 'opt_funding_rate_sample' in df.columns:
        funding = df['opt_funding_rate_sample'].replace(-1, np.nan).fillna(0)
        df['funding_pressure'] = funding * df['near_funding']
    else:
        df['funding_pressure'] = np.nan

    # ── Volatility regime — for regime analysis, NOT a model feature ────
    # Rolling absolute price change over last 16 bars (~4 min). Past rows only.
    df['volatility'] = df['close_price'].diff().abs().rolling(16, min_periods=4).mean()

    # ── Normalised volatility ──────────────────────────────────────────────

    # Current bar volatility relative to 24-hour rolling average.
    # > 1.0 = more volatile than recent baseline; < 1.0 = calmer than baseline.
    # Allows model to weight signals differently by volatility level.
    # 5760 bars * 15s = 86400s = 24 hours. min_periods=96 = 24 min warm-up.
    # volatility must be computed above before this line.
    df['vol_norm'] = (
        df['volatility'] /
        df['volatility'].rolling(5760, min_periods=96).mean()
    ).replace([np.inf, -np.inf], np.nan)

    # Regime thresholds use a TRAILING rolling quantile, not a global qcut.
    # pd.qcut() would compute terciles from the entire column — including
    # future rows — which leaks future information into early-row labels.
    # A rolling quantile only looks at the trailing ROLL_WINDOW bars, so the
    # regime label for row N depends only on rows <= N.
    ROLL_WINDOW = 2000  # ~8.3 hours of 15s bars
    vol = df['volatility']
    q_low  = vol.rolling(ROLL_WINDOW, min_periods=200).quantile(1/3)
    q_high = vol.rolling(ROLL_WINDOW, min_periods=200).quantile(2/3)

    df['vol_regime'] = np.select(
        [vol <= q_low, vol >= q_high],
        ['low', 'high'],
        default='mid'  # also covers warm-up rows where q_low/q_high are NaN
    )

    # ── Drop last `horizon` rows — they have no valid target ──────────
    df = df.iloc[:-horizon].copy()

    return df


# ─────────────────────────────────────────────
#  ANALYSIS FUNCTIONS
# ─────────────────────────────────────────────

def print_header(title: str):
    w = 72
    print()
    print('=' * w)
    print(f"  {title}")
    print('=' * w)


def section(title: str):
    print(f"\n── {title} {'─' * (68 - len(title))}")


def class_distribution(df: pd.DataFrame, threshold: float, horizon: int = 1):
    print_header("CLASS DISTRIBUTION")
    counts = df['target'].value_counts().sort_index()
    total  = len(df)
    labels = {-1: 'DOWN', 0: 'FLAT', 1: 'UP'}
    for v, n in counts.items():
        bar = '█' * int(40 * n / total)
        print(f"  {labels[v]:4s} ({v:+d})  {n:6d}  {n/total*100:5.1f}%  {bar}")
    print(f"\n  Total rows : {total}")
    print(f"  Date range : {df['dt'].min()} → {df['dt'].max()}")
    print(f"  Threshold  : ±${threshold:.2f}")
    print(f"  Horizon    : {horizon} bar(s) = {horizon * WINDOW_SEC}s ahead")

    delta = df['delta_usd'].describe()
    print(f"\n  Price delta stats (USD):")
    print(f"    mean={delta['mean']:+.2f}  std={delta['std']:.2f}  "
          f"min={delta['min']:+.2f}  max={delta['max']:+.2f}")


def mean_by_class(df: pd.DataFrame, features: list):
    print_header("MEAN FEATURE VALUE BY TARGET CLASS")
    labels = {-1: 'DOWN', 0: 'FLAT', 1: 'UP'}
    grouped = df.groupby('target')[features].mean()

    # Header
    print(f"  {'Feature':<22}", end='')
    for v in [-1, 0, 1]:
        print(f"  {labels[v]:>10}", end='')
    print(f"  {'Signal?':>10}")
    print(f"  {'-'*22}", end='')
    for _ in range(4):
        print(f"  {'-'*10}", end='')
    print()

    for feat in features:
        row = grouped[feat]
        down = row.get(-1, np.nan)
        flat = row.get(0,  np.nan)
        up   = row.get(1,  np.nan)
        # Signal: is ordering monotonic (down < flat < up or reverse)?
        if not any(np.isnan([down, flat, up])):
            monotonic = (down < flat < up) or (down > flat > up)
            signal = '✓ YES' if monotonic else '~ WEAK'
        else:
            signal = '  N/A'
        print(f"  {feat:<22}  {down:>10.5f}  {flat:>10.5f}  {up:>10.5f}  {signal:>10}")


def lag_correlations(df: pd.DataFrame, features: list):
    print_header("LAG CORRELATION WITH TARGET")
    print(f"  (correlation of past feature values with next window direction)")
    print()

    header = f"  {'Feature':<22}"
    for lag in LAGS_TO_CHECK:
        header += f"  {lag*WINDOW_SEC:>5}s"
    print(header)
    print(f"  {'-'*22}" + f"  {'-----'}" * len(LAGS_TO_CHECK))

    for feat in features:
        row = f"  {feat:<22}"
        for lag in LAGS_TO_CHECK:
            corr = df[feat].shift(lag).corr(df['target'])
            if np.isnan(corr):
                row += f"  {'N/A':>5}"
            else:
                marker = '*' if abs(corr) > 0.02 else ' '
                row += f"  {corr:>+4.3f}{marker}"
        print(row)

    print(f"\n  * |correlation| > 0.02")


def mutual_information(df: pd.DataFrame, features: list) -> pd.DataFrame:
    """Returns MI dataframe sorted by score."""
    print_header("MUTUAL INFORMATION (non-linear signal)")
    print("  Measures statistical dependence between feature and target.")
    print("  MI = 0 → no signal.  MI > 0.001 → some signal.\n")

    clean = df[features + ['target']].dropna()
    if len(clean) < 100:
        print("  Not enough clean rows for MI calculation.")
        return pd.DataFrame()

    X = clean[features].values
    y = (clean['target'] + 1).values  # shift to 0,1,2

    mi = mutual_info_classif(X, y, random_state=42, n_neighbors=5)
    mi_df = pd.DataFrame({'feature': features, 'mi': mi})
    mi_df = mi_df.sort_values('mi', ascending=False)

    max_mi = mi_df['mi'].max() if mi_df['mi'].max() > 0 else 1
    for _, r in mi_df.iterrows():
        bar = '█' * int(30 * r['mi'] / max_mi)
        useful = '✓' if r['mi'] > 0.001 else '✗'
        print(f"  {useful} {r['feature']:<22}  {r['mi']:.5f}  {bar}")

    return mi_df


def _split(df, features):
    clean = df[features + ['target']].dropna()
    X = clean[features].values
    y = clean['target'].values
    split = int(len(X) * 0.70)
    return X[:split], X[split:], y[:split], y[split:]


def _print_result(name, dummy_acc, model_acc, y_test, y_pred, features, importances):
    labels_map = {-1: 'DOWN', 0: 'FLAT', 1: 'UP'}
    unique_labels = sorted(set(y_test))
    lift = model_acc - dummy_acc
    verdict = '✓ Signal exists' if lift > 0.01 else ('~ Marginal' if lift > 0.0 else '✗ No signal')
    print(f"    {name:<26} : {model_acc*100:.2f}%  (lift {lift*100:+.2f}%)  {verdict}")
    print(f"\n  Classification report — {name}:")
    print(classification_report(
        y_test, y_pred,
        target_names=[labels_map[l] for l in unique_labels],
        labels=unique_labels, zero_division=0
    ))
    if importances is not None:
        imp_df = pd.DataFrame({'feature': features, 'importance': importances})
        imp_df = imp_df.sort_values('importance', ascending=False)
        max_imp = imp_df['importance'].max() if imp_df['importance'].max() > 0 else 1
        print(f"  Feature importance — {name}:")
        for _, r in imp_df.iterrows():
            bar = '█' * int(25 * r['importance'] / max_imp)
            print(f"    {r['feature']:<24}  {r['importance']:.4f}  {bar}")
        print()
    return lift


def models_baseline(df, features):
    print_header("MODEL COMPARISON")
    X_train, X_test, y_train, y_test = _split(df, features)
    print(f"  Train rows : {len(X_train)}  |  Test rows : {len(X_test)}")

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s  = scaler.transform(X_test)

    dummy = DummyClassifier(strategy='most_frequent')
    dummy.fit(X_train_s, y_train)
    dummy_acc = (dummy.predict(X_test_s) == y_test).mean()
    print(f"\n  Accuracy:")
    print(f"    {'Dummy (majority class)':<26} : {dummy_acc*100:.2f}%  (baseline)")

    # Logistic Regression
    lr = LogisticRegression(max_iter=2000, C=0.1, class_weight='balanced')
    lr.fit(X_train_s, y_train)
    lr_acc = (lr.predict(X_test_s) == y_test).mean()
    lr_imp = np.abs(lr.coef_).mean(axis=0) if lr.coef_.shape[0] > 1 else np.abs(lr.coef_[0])
    lr_lift = _print_result('Logistic Regression', dummy_acc, lr_acc,
                            y_test, lr.predict(X_test_s), features, lr_imp)

    # XGBoost
    if XGBOOST_OK:
        le     = {-1: 0, 0: 1, 1: 2}
        le_inv = {0: -1, 1: 0, 2: 1}
        y_tr_x = np.array([le[v] for v in y_train])
        y_te_x = np.array([le[v] for v in y_test])
        xgb = XGBClassifier(
            n_estimators=500, max_depth=4, learning_rate=0.01,
            subsample=0.8, colsample_bytree=0.8, min_child_weight=50,
            eval_metric='mlogloss', early_stopping_rounds=30,
            verbosity=0, random_state=42,
        )
        xgb.fit(X_train_s, y_tr_x, eval_set=[(X_test_s, y_te_x)], verbose=False)
        xgb_pred = np.array([le_inv[v] for v in xgb.predict(X_test_s)])
        xgb_acc  = (xgb_pred == y_test).mean()
        xgb_lift = _print_result('XGBoost', dummy_acc, xgb_acc,
                                 y_test, xgb_pred, features, xgb.feature_importances_)

        print_header("MODEL HEAD-TO-HEAD")
        print(f"  {'Model':<26}  {'Accuracy':>10}  {'Lift':>8}")
        print(f"  {'-'*26}  {'-'*10}  {'-'*8}")
        ranked = sorted([('Logistic Regression', lr_acc, lr_lift),
                         ('XGBoost',             xgb_acc, xgb_lift)],
                        key=lambda x: x[1], reverse=True)
        for i, (name, acc, lift) in enumerate(ranked):
            crown = '  👑' if i == 0 else '  '
            print(f"  {name:<26}  {acc*100:>9.2f}%  {lift*100:>+7.2f}%{crown}")
        gap = (xgb_acc - lr_acc) * 100
        print()
        if gap > 1.0:
            print(f"  XGBoost outperforms LR by {gap:.2f}% → non-linear signal.")
            print(f"  Neural network is worth trying next.")
        elif gap < -1.0:
            print(f"  LR outperforms XGBoost by {abs(gap):.2f}% → signal is linear.")
            print(f"  A neural network is unlikely to add much.")
        else:
            print(f"  Both models perform similarly (gap {abs(gap):.2f}%).")
            if max(lr_lift, xgb_lift) < 0.01:
                print(f"  Neither beats dummy → try --horizon 4, 8, or 16.")
    else:
        print("\n  XGBoost not installed. Install: pip install xgboost")


def rolling_accuracy(df, features, window_rows=0, max_windows=100):
    """
    Rolling out-of-sample evaluation: train on a trailing window, test on
    the next window, slide forward.

    window_rows : rows per train/test window. 0 = auto-size based on data
                   length so the total number of windows stays near
                   max_windows. Large datasets (months of 15s bars) would
                   otherwise produce tens of thousands of windows.
    max_windows : target number of windows when auto-sizing (also caps the
                   step size for manually-specified window_rows).
    """
    print_header("ROLLING ACCURACY (stability check)")

    clean = df[features + ['target', 'dt']].dropna().reset_index(drop=True)
    n = len(clean)

    if window_rows <= 0:
        # Auto-size: aim for ~max_windows total windows, min 200 rows/window
        window_rows = max(200, n // (max_windows + 2))

    if n < window_rows * 3:
        print("  Not enough data for rolling accuracy. Need at least 3x window size.")
        return None

    # Step size: ensure total iterations stay near max_windows even for
    # huge datasets, but keep at least 25% overlap for small datasets.
    min_step  = max(1, window_rows // 4)
    auto_step = max(1, (n - window_rows * 2) // max_windows)
    step      = max(min_step, auto_step)

    n_windows = (n - window_rows * 2) // step + 1
    print(f"  Window  : {window_rows} rows ({window_rows * WINDOW_SEC / 3600:.1f} hours)")
    print(f"  Step    : {step} rows ({step * WINDOW_SEC / 3600:.1f} hours)")
    print(f"  Windows : ~{n_windows} (from {n:,} rows)")
    print("  LR and XGBoost trained on each rolling window independently.\n")

    X      = clean[features].values
    y      = clean['target'].values
    dates  = clean['dt'].values
    le     = {-1: 0, 0: 1, 1: 2}
    le_inv = {0: -1, 1: 0, 2: 1}

    results = []
    for start in range(0, len(X) - window_rows * 2, step):
        train_end = start + window_rows
        test_end  = min(train_end + window_rows, len(X))
        if test_end <= train_end:
            break
        X_tr, y_tr = X[start:train_end], y[start:train_end]
        X_te, y_te = X[train_end:test_end], y[train_end:test_end]
        try:
            sc = StandardScaler()
            X_tr_s = sc.fit_transform(X_tr)
            X_te_s = sc.transform(X_te)
            dummy_acc = max(np.bincount(y_tr + 1)) / len(y_tr)
            row = {'date': pd.Timestamp(dates[train_end]), 'dummy': dummy_acc}

            lr = LogisticRegression(max_iter=500, C=0.1, class_weight='balanced')
            lr.fit(X_tr_s, y_tr)
            lr_acc = (lr.predict(X_te_s) == y_te).mean()
            row['lr_acc']  = lr_acc
            row['lr_lift'] = lr_acc - dummy_acc

            if XGBOOST_OK:
                y_tr_x = np.array([le[v] for v in y_tr])
                y_te_x = np.array([le[v] for v in y_te])
                xgb = XGBClassifier(
                    n_estimators=200, max_depth=4, learning_rate=0.05,
                    subsample=0.8, colsample_bytree=0.8, min_child_weight=30,
                    eval_metric='mlogloss', verbosity=0, random_state=42,
                )
                xgb.fit(X_tr_s, y_tr_x, verbose=False)
                xgb_pred = np.array([le_inv[v] for v in xgb.predict(X_te_s)])
                xgb_acc  = (xgb_pred == y_te).mean()
                row['xgb_acc']  = xgb_acc
                row['xgb_lift'] = xgb_acc - dummy_acc

            results.append(row)
        except Exception:
            continue

    if not results:
        return None

    res_df  = pd.DataFrame(results)
    has_xgb = 'xgb_acc' in res_df.columns and XGBOOST_OK

    hdr = f"  {'Date':20s}  {'Dummy':>6}  {'LR':>7}  {'LR lift':>8}"
    if has_xgb:
        hdr += f"  {'XGB':>7}  {'XGB lift':>9}  Best"
    print(hdr)
    print(f"  {'-'*72}")

    # If there are many windows, printing every row is unreadable —
    # show at most ~40 rows, evenly spaced across the full series.
    MAX_PRINTED = 40
    if len(res_df) > MAX_PRINTED:
        sample_idx = np.linspace(0, len(res_df) - 1, MAX_PRINTED).astype(int)
        print_rows = res_df.iloc[sample_idx]
        print(f"  ({len(res_df)} windows total — showing {MAX_PRINTED} evenly spaced)")
    else:
        print_rows = res_df

    for _, r in print_rows.iterrows():
        lm   = '✓' if r['lr_lift'] > 0.01 else ('~' if r['lr_lift'] > 0 else '✗')
        line = (f"  {str(r['date'])[:19]:20s}  {r['dummy']*100:>5.1f}%"
                f"  {r['lr_acc']*100:>6.1f}%  {r['lr_lift']*100:>+7.2f}% {lm}")
        if has_xgb:
            xm     = '✓' if r['xgb_lift'] > 0.01 else ('~' if r['xgb_lift'] > 0 else '✗')
            better = 'XGB' if r['xgb_acc'] > r['lr_acc'] else 'LR '
            line  += (f"  {r['xgb_acc']*100:>6.1f}%  {r['xgb_lift']*100:>+8.2f}% {xm}"
                      f"  {better}")
        print(line)

    print(f"\n  LR  — Mean lift: {res_df['lr_lift'].mean()*100:+.2f}%  "
          f"Std: {res_df['lr_lift'].std()*100:.2f}%  "
          f"Windows >1%: {(res_df['lr_lift']>0.01).mean()*100:.0f}%")
    if has_xgb:
        print(f"  XGB — Mean lift: {res_df['xgb_lift'].mean()*100:+.2f}%  "
              f"Std: {res_df['xgb_lift'].std()*100:.2f}%  "
              f"Windows >1%: {(res_df['xgb_lift']>0.01).mean()*100:.0f}%")
        xgb_wins = (res_df['xgb_acc'] > res_df['lr_acc']).mean() * 100
        print(f"  XGB beats LR in {xgb_wins:.0f}% of rolling windows")
        res_df['accuracy'] = res_df[['lr_acc', 'xgb_acc']].max(axis=1)
        res_df['lift']     = res_df[['lr_lift', 'xgb_lift']].max(axis=1)
    else:
        res_df['accuracy'] = res_df['lr_acc']
        res_df['lift']     = res_df['lr_lift']

    return res_df




# ────────────────────────────────────────
def regime_analysis(df, features):
    """Split data by volatility regime and run models in each tercile."""
    print_header("REGIME ANALYSIS (does signal cluster in certain conditions?)")

    if 'vol_regime' not in df.columns:
        print("  vol_regime column missing -- skipping.")
        return
    if not SKLEARN_OK:
        print("  scikit-learn not available -- skipping.")
        return

    le     = {-1: 0, 0: 1, 1: 2}
    le_inv = {0: -1, 1: 0, 2: 1}

    hdr = f"  {'Regime':<8}  {'Rows':>7}  {'Vol $':>7}  {'Dummy':>6}  {'LR lift':>8}"
    if XGBOOST_OK:
        hdr += f"  {'XGB lift':>9}  {'Better':>6}  Signal?"
    else:
        hdr += "  Signal?"
    print(hdr)
    print("  " + "-" * 80)

    regime_results = {}
    for regime in ['low', 'mid', 'high']:
        mask  = df['vol_regime'].astype(str) == regime
        chunk = df[mask][features + ['target']].dropna()

        if len(chunk) < 200:
            print(f"  {regime:<8}  {len(chunk):>7}  (too few rows)")
            continue

        X = chunk[features].values
        y = chunk['target'].values
        split = int(len(X) * 0.70)
        X_tr, X_te = X[:split], X[split:]
        y_tr, y_te = y[:split], y[split:]
        vol_mean  = df[mask]['volatility'].mean()
        dummy_acc = max(np.bincount(y_tr + 1)) / len(y_tr)

        sc = StandardScaler()
        X_tr_s = sc.fit_transform(X_tr)
        X_te_s = sc.transform(X_te)

        try:
            lr = LogisticRegression(max_iter=500, C=0.1, class_weight='balanced')
            lr.fit(X_tr_s, y_tr)
            lr_lift = (lr.predict(X_te_s) == y_te).mean() - dummy_acc
        except Exception:
            lr_lift = np.nan

        xgb_lift = np.nan
        if XGBOOST_OK:
            try:
                y_tr_x = np.array([le[v] for v in y_tr])
                xgb = XGBClassifier(
                    n_estimators=300, max_depth=4, learning_rate=0.02,
                    subsample=0.8, colsample_bytree=0.8, min_child_weight=30,
                    eval_metric='mlogloss', verbosity=0, random_state=42,
                )
                xgb.fit(X_tr_s, y_tr_x, verbose=False)
                xgb_pred = np.array([le_inv[v] for v in xgb.predict(X_te_s)])
                xgb_lift = (xgb_pred == y_te).mean() - dummy_acc
            except Exception:
                xgb_lift = np.nan

        regime_results[regime] = {
            'rows': len(chunk), 'vol': vol_mean,
            'dummy': dummy_acc, 'lr_lift': lr_lift, 'xgb_lift': xgb_lift,
        }

        lr_m = '\u2713' if lr_lift > 0.01 else ('~' if lr_lift > 0 else '\u2717')
        line = (f"  {regime:<8}  {len(chunk):>7}  {vol_mean:>6.2f}$"
                f"  {dummy_acc*100:>5.1f}%  {lr_lift*100:>+7.2f}% {lr_m}")
        if XGBOOST_OK and not np.isnan(xgb_lift):
            xm     = '\u2713' if xgb_lift > 0.01 else ('~' if xgb_lift > 0 else '\u2717')
            better = 'XGB' if xgb_lift > lr_lift else 'LR '
            best_l = max(lr_lift, xgb_lift)
            line  += f"  {xgb_lift*100:>+8.2f}% {xm}  {better}"
        else:
            best_l = lr_lift if not np.isnan(lr_lift) else -999
        verdict = '  \u2713 Tradeable' if best_l > 0.02 else ('  ~ Marginal' if best_l > 0.005 else '  \u2717 Noise')
        print(line + verdict)

    if not regime_results:
        return

    best_r = max(regime_results, key=lambda r: max(
        regime_results[r].get('xgb_lift', -999) if not np.isnan(regime_results[r].get('xgb_lift', np.nan)) else -999,
        regime_results[r].get('lr_lift', -999)  if not np.isnan(regime_results[r].get('lr_lift', np.nan))  else -999,
    ))
    best   = regime_results[best_r]
    xgl    = best.get('xgb_lift', np.nan)
    lrl    = best.get('lr_lift', np.nan)
    best_l = max(
        xgl if not np.isnan(xgl) else -999,
        lrl if not np.isnan(lrl) else -999
    )

    print()
    if best_l > 0.02:
        print(f"  Best regime     : {best_r.upper()} volatility")
        print(f"  Avg vol (USD)   : ${best['vol']:.2f} per bar (~{16 * WINDOW_SEC}s window)")
        print(f"  Best lift       : {best_l*100:+.2f}%")
        print()
        print(f"  Recommendation  : Only run your model when 4-min rolling")
        print(f"                    volatility is in the {best_r.upper()} tercile.")
        print(f"                    Stay flat in other regimes.")
    else:
        print(f"  No regime shows reliable lift (best: {best_l*100:+.2f}%).")
        print(f"  Try time-of-day segmentation or a longer horizon.")


# ─────────────────────────────────────────────
#  PLOTS
# ─────────────────────────────────────────────

def make_plots(df: pd.DataFrame, features: list, mi_df: pd.DataFrame,
               rolling_df, output_dir: str):
    print_header("GENERATING PLOTS")

    fig = plt.figure(figsize=(20, 26), facecolor='#0d1117')
    gs  = gridspec.GridSpec(4, 3, figure=fig, hspace=0.45, wspace=0.35,
                            left=0.07, right=0.97, top=0.95, bottom=0.04)

    DARK  = '#0d1117'
    PANEL = '#161b22'
    GRID  = '#21262d'
    TEXT  = '#e6edf3'
    UP    = '#3fb950'
    DOWN  = '#f85149'
    FLAT  = '#6e7681'
    ACC   = '#58a6ff'

    def style_ax(ax, title):
        ax.set_facecolor(PANEL)
        ax.tick_params(colors=TEXT, labelsize=8)
        ax.set_title(title, color=TEXT, fontsize=9, fontweight='bold', pad=6)
        for spine in ax.spines.values():
            spine.set_edgecolor(GRID)
        ax.xaxis.label.set_color(TEXT)
        ax.yaxis.label.set_color(TEXT)
        ax.grid(color=GRID, linewidth=0.5, alpha=0.7)

    fig.text(0.5, 0.974, 'BTC Signal Analysis', ha='center', va='top',
             color=TEXT, fontsize=16, fontweight='bold')
    fig.text(0.5, 0.962, f"{df['dt'].min().date()} → {df['dt'].max().date()}  |  "
             f"{len(df):,} windows  |  15s bars",
             ha='center', va='top', color=FLAT, fontsize=9)

    # ── 1. Class distribution ──────────────────
    ax = fig.add_subplot(gs[0, 0])
    counts = df['target'].value_counts().sort_index()
    colors = [DOWN, FLAT, UP]
    bars = ax.bar(['-1\nDOWN', '0\nFLAT', '+1\nUP'],
                  [counts.get(k, 0) for k in [-1, 0, 1]],
                  color=colors, edgecolor=DARK, linewidth=0.5)
    for bar, cnt in zip(bars, [counts.get(k, 0) for k in [-1, 0, 1]]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{cnt}', ha='center', va='bottom', color=TEXT, fontsize=8)
    style_ax(ax, 'Class Distribution')

    # ── 2. Price delta histogram ───────────────
    ax = fig.add_subplot(gs[0, 1])
    delta = df['delta_usd'].dropna()
    ax.hist(delta[df['target'] == -1], bins=60, color=DOWN,  alpha=0.7, label='DOWN', density=True)
    ax.hist(delta[df['target'] ==  0], bins=60, color=FLAT,  alpha=0.5, label='FLAT', density=True)
    ax.hist(delta[df['target'] ==  1], bins=60, color=UP,    alpha=0.7, label='UP',   density=True)
    ax.legend(fontsize=7, facecolor=PANEL, labelcolor=TEXT, edgecolor=GRID)
    ax.set_xlabel('Price delta (USD)')
    style_ax(ax, 'Price Delta by Class')

    # ── 3. Mutual information ──────────────────
    ax = fig.add_subplot(gs[0, 2])
    if not mi_df.empty:
        mi_sorted = mi_df.sort_values('mi')
        bars = ax.barh(mi_sorted['feature'], mi_sorted['mi'],
                       color=ACC, edgecolor=DARK, linewidth=0.5)
        ax.axvline(0.001, color=UP, linewidth=1, linestyle='--', alpha=0.7, label='threshold=0.001')
        ax.legend(fontsize=7, facecolor=PANEL, labelcolor=TEXT, edgecolor=GRID)
        ax.set_xlabel('Mutual Information')
    style_ax(ax, 'Mutual Information Score')

    # ── 4-6. Feature distributions (top 3 by MI) ──
    top_features = mi_df.head(3)['feature'].tolist() if not mi_df.empty else features[:3]
    for i, feat in enumerate(top_features):
        ax = fig.add_subplot(gs[1, i])
        for label, color in [(-1, DOWN), (0, FLAT), (1, UP)]:
            vals = df[df['target'] == label][feat].dropna()
            if len(vals) > 5:
                ax.hist(vals, bins=50, alpha=0.6, color=color,
                        label={-1:'DOWN', 0:'FLAT', 1:'UP'}[label], density=True)
        ax.legend(fontsize=7, facecolor=PANEL, labelcolor=TEXT, edgecolor=GRID)
        style_ax(ax, f'Distribution: {feat}')

    # ── 7. Lag correlations heatmap ───────────
    ax = fig.add_subplot(gs[2, :2])
    lag_data = []
    for feat in features:
        row = []
        for lag in LAGS_TO_CHECK:
            corr = df[feat].shift(lag).corr(df['target'])
            row.append(corr if not np.isnan(corr) else 0)
        lag_data.append(row)

    lag_array = np.array(lag_data)
    cmap = LinearSegmentedColormap.from_list('rg', [DOWN, DARK, UP])
    vmax = max(abs(lag_array).max(), 0.01)
    im = ax.imshow(lag_array, aspect='auto', cmap=cmap, vmin=-vmax, vmax=vmax)
    ax.set_xticks(range(len(LAGS_TO_CHECK)))
    ax.set_xticklabels([f"{l*WINDOW_SEC}s" for l in LAGS_TO_CHECK], fontsize=8, color=TEXT)
    ax.set_yticks(range(len(features)))
    ax.set_yticklabels(features, fontsize=8, color=TEXT)

    for i in range(len(features)):
        for j in range(len(LAGS_TO_CHECK)):
            val = lag_array[i, j]
            ax.text(j, i, f'{val:.3f}', ha='center', va='center',
                    fontsize=6.5, color='white' if abs(val) > vmax*0.5 else TEXT)

    plt.colorbar(im, ax=ax, fraction=0.02, pad=0.02)
    style_ax(ax, 'Lag Correlation Heatmap (feature shifted by N seconds vs target)')
    ax.set_facecolor(DARK)
    for spine in ax.spines.values():
        spine.set_edgecolor(GRID)

    # ── 8. Mean by class ──────────────────────
    ax = fig.add_subplot(gs[2, 2])
    grouped = df.groupby('target')[features].mean()
    x = np.arange(len(features))
    w = 0.25
    for i, (label, color) in enumerate([(-1, DOWN), (0, FLAT), (1, UP)]):
        if label in grouped.index:
            vals = grouped.loc[label].values
            # Normalise for display
            mx = np.abs(vals).max()
            if mx > 0:
                vals = vals / mx
            ax.bar(x + i*w, vals, width=w, color=color, alpha=0.8,
                   label={-1:'DOWN', 0:'FLAT', 1:'UP'}[label], edgecolor=DARK)
    ax.set_xticks(x + w)
    ax.set_xticklabels(features, rotation=45, ha='right', fontsize=6.5, color=TEXT)
    ax.axhline(0, color=GRID, linewidth=0.8)
    ax.legend(fontsize=7, facecolor=PANEL, labelcolor=TEXT, edgecolor=GRID)
    style_ax(ax, 'Normalised Mean by Class')

    # ── 9. Rolling accuracy ───────────────────
    ax = fig.add_subplot(gs[3, :])
    if rolling_df is not None and len(rolling_df) > 0:
        ax.plot(rolling_df['date'], rolling_df['accuracy'] * 100,
                color=ACC, linewidth=1.5, label='LR Accuracy')
        ax.plot(rolling_df['date'], rolling_df['dummy'] * 100,
                color=FLAT, linewidth=1, linestyle='--', label='Dummy baseline')
        ax.fill_between(rolling_df['date'],
                        rolling_df['dummy'] * 100,
                        rolling_df['accuracy'] * 100,
                        where=rolling_df['accuracy'] >= rolling_df['dummy'],
                        alpha=0.2, color=UP, label='Positive lift')
        ax.fill_between(rolling_df['date'],
                        rolling_df['dummy'] * 100,
                        rolling_df['accuracy'] * 100,
                        where=rolling_df['accuracy'] < rolling_df['dummy'],
                        alpha=0.2, color=DOWN, label='Negative lift')
        ax.set_ylabel('Accuracy (%)')
        ax.legend(fontsize=8, facecolor=PANEL, labelcolor=TEXT,
                  edgecolor=GRID, loc='upper left')
        ax.xaxis.set_tick_params(rotation=30)
        style_ax(ax, 'Rolling Accuracy Over Time (is signal stable?)')
    else:
        ax.text(0.5, 0.5, 'Not enough data for rolling accuracy\n(need ≥3× window size)',
                ha='center', va='center', color=FLAT, fontsize=11,
                transform=ax.transAxes)
        style_ax(ax, 'Rolling Accuracy Over Time')

    out_path = os.path.join(output_dir, 'btc_signal_analysis.png')
    fig.savefig(out_path, dpi=150, bbox_inches='tight', facecolor=DARK)
    plt.close(fig)
    print(f"  Saved → {out_path}")
    return out_path


# ─────────────────────────────────────────────
#  BACKTEST
# ─────────────────────────────────────────────

def backtest_strategy(df: pd.DataFrame, features: list, horizon: int,
                      output_dir: str, min_confidence: float = 0.0):
    """
    Zero-fee backtest on the test split (last 30% of data).

    Rules (one unit, no compounding):
      Prediction UP   → buy at close_price[N], exit at close_price[N+1]
      Prediction DOWN → short at close_price[N], exit at close_price[N+1]
      Prediction FLAT → do nothing

    min_confidence : only trade when XGBoost's max class probability
                     exceeds this threshold (0.0 = trade all signals).
                     Use --confidence to set from CLI.

    The model is the same XGBoost trained on the first 70% of data.
    Predictions are generated only on the held-out 30% — no lookahead.

    Benchmark: buy-and-hold over the same test period.

    NOTE: fees, slippage, and funding costs are intentionally excluded.
    This measures raw signal quality, not tradeable profitability.
    """
    print_header("BACKTEST (zero fees, test set only)")

    if not XGBOOST_OK:
        print("  XGBoost not installed — skipping backtest.")
        return

    clean = df[features + ['target', 'close_price', 'dt']].dropna().reset_index(drop=True)
    n     = len(clean)
    split = int(n * 0.70)

    X = clean[features].values
    y = clean['target'].values

    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    scaler    = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s  = scaler.transform(X_test)

    le     = {-1: 0, 0: 1, 1: 2}
    le_inv = {0: -1, 1: 0, 2: 1}

    y_train_enc = np.array([le[v] for v in y_train])
    y_test_enc  = np.array([le[v] for v in y_test])
    xgb = XGBClassifier(
        n_estimators=500, max_depth=4, learning_rate=0.01,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=50,
        eval_metric='mlogloss', early_stopping_rounds=30,
        verbosity=0, random_state=42,
    )
    xgb.fit(X_train_s, y_train_enc,
            eval_set=[(X_test_s, y_test_enc)], verbose=False)

    # Get both hard predictions and probabilities
    preds_raw   = np.array([le_inv[v] for v in xgb.predict(X_test_s)])
    proba       = xgb.predict_proba(X_test_s)          # shape (n, 3): [DOWN, FLAT, UP]
    confidence  = proba.max(axis=1)                    # highest class probability per bar

    # Apply confidence filter: suppress signal when model is uncertain
    preds = np.where(confidence >= min_confidence, preds_raw, 0)

    # ── Build test dataframe ──────────────────────────────────────────────
    test_df = clean.iloc[split:].copy().reset_index(drop=True)
    test_df['pred']       = preds
    test_df['pred_raw']   = preds_raw
    test_df['confidence'] = confidence

    # Price move from bar N close to bar N+1 close — test set only, no leakage
    test_df['next_close'] = test_df['close_price'].shift(-1)
    test_df['price_move'] = test_df['next_close'] - test_df['close_price']
    test_df = test_df.iloc[:-1].copy().reset_index(drop=True)

    # Trim arrays to match after last-row drop
    preds_raw  = preds_raw[:-1]
    confidence = confidence[:-1]
    preds      = preds[:-1]

    # Update pred columns to trimmed arrays
    test_df['pred']       = preds
    test_df['pred_raw']   = preds_raw
    test_df['confidence'] = confidence

    def calc_stats(df_t, pred_col='pred'):
        """Compute P&L stats for a given prediction column."""
        pnl = np.where(
            df_t[pred_col] ==  1,  df_t['price_move'],
            np.where(
            df_t[pred_col] == -1, -df_t['price_move'],
            0.0)
        )
        cum     = np.cumsum(pnl)
        trades  = df_t[df_t[pred_col] != 0]
        t_pnl   = pnl[df_t[pred_col] != 0]
        wr      = (t_pnl > 0).mean() if len(t_pnl) > 0 else 0
        sharpe  = (t_pnl.mean() / t_pnl.std()) if (len(t_pnl) > 1 and t_pnl.std() > 0) else 0
        cummax  = np.maximum.accumulate(cum)
        max_dd  = (cum - cummax).min()
        avg_win  = t_pnl[t_pnl > 0].mean() if (t_pnl > 0).any() else 0
        avg_loss = t_pnl[t_pnl < 0].mean() if (t_pnl < 0).any() else 0
        return {
            'pnl': pnl, 'cum': cum, 'n_trades': len(trades),
            'wr': wr, 'sharpe': sharpe, 'max_dd': max_dd,
            'avg_win': avg_win, 'avg_loss': avg_loss,
            'total': cum[-1] if len(cum) > 0 else 0,
        }

    s = calc_stats(test_df)
    bnh_cum   = test_df['price_move'].cumsum().values
    bnh_total = bnh_cum[-1]

    test_df['strategy_cum'] = s['cum']
    test_df['drawdown']     = test_df['strategy_cum'] - test_df['strategy_cum'].cummax()
    test_df['bnh_cum']      = bnh_cum

    # ── Print main results ────────────────────────────────────────────────
    start_price = test_df['close_price'].iloc[0]
    end_price   = test_df['close_price'].iloc[-1]
    n_long  = (test_df['pred'] ==  1).sum()
    n_short = (test_df['pred'] == -1).sum()
    n_flat  = (test_df['pred'] ==  0).sum()

    conf_str = f" (confidence ≥ {min_confidence:.0%})" if min_confidence > 0 else ""
    print(f"  Test period  : {test_df['dt'].iloc[0]} → {test_df['dt'].iloc[-1]}")
    print(f"  Test rows    : {len(test_df):,}  ({len(test_df)*WINDOW_SEC/3600:.0f} hours)")
    print(f"  BTC price    : ${start_price:,.0f} → ${end_price:,.0f}  "
          f"(Δ ${end_price-start_price:+,.0f})")
    print(f"  Filter{conf_str}")
    print()
    print(f"  Trade breakdown:")
    print(f"    Total signals : {s['n_trades']:,}  ({s['n_trades']/len(test_df)*100:.1f}% of bars)")
    print(f"    Long          : {n_long:,}")
    print(f"    Short         : {n_short:,}")
    print(f"    Flat (skip)   : {n_flat:,}")
    print()
    print(f"  Performance (per 1 BTC unit, zero fees):")
    print(f"    Strategy P&L  : ${s['total']:>+12,.2f}")
    print(f"    Buy & Hold    : ${bnh_total:>+12,.2f}")
    print(f"    Edge vs B&H   : ${s['total'] - bnh_total:>+12,.2f}")
    print()
    print(f"  Trade quality:")
    print(f"    Win rate      : {s['wr']*100:.2f}%  (among {s['n_trades']:,} trades)")
    print(f"    Avg win/trade : ${s['avg_win']:.2f}")
    print(f"    Avg loss/trade: ${s['avg_loss']:.2f}")
    print(f"    Sharpe ratio  : {s['sharpe']:.4f}  (per-trade, not annualised)")
    print(f"    Max drawdown  : ${s['max_dd']:,.2f}")

    # ── Confidence threshold sweep ────────────────────────────────────────
    # Maker fee break-even at typical BTC price
    avg_price   = (start_price + end_price) / 2
    maker_fee   = avg_price * 0.0004   # 0.02% per side × 2
    taker_fee   = avg_price * 0.0010   # 0.05% per side × 2

    print()
    print(f"  Confidence threshold sweep (fee break-even: maker=${maker_fee:.0f}  "
          f"taker=${taker_fee:.0f} per trade):")
    print(f"  {'Threshold':>10}  {'Trades':>7}  {'% bars':>7}  "
          f"{'Win rate':>9}  {'Avg P&L':>8}  {'Total P&L':>11}  "
          f"{'After maker':>12}  {'After taker':>12}  Fee OK?")
    print(f"  {'-'*10}  {'-'*7}  {'-'*7}  {'-'*9}  {'-'*8}  {'-'*11}  "
          f"{'-'*12}  {'-'*12}  {'-'*6}")

    thresholds = [0.33, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]
    sweep_results = []
    for thresh in thresholds:
        filtered_preds = np.where(confidence >= thresh, preds_raw, 0)
        tmp = test_df.copy()
        tmp['pred'] = filtered_preds
        st = calc_stats(tmp)
        if st['n_trades'] == 0:
            avg_pnl = 0.0
        else:
            avg_pnl = st['total'] / st['n_trades']
        after_maker = st['total'] - st['n_trades'] * maker_fee
        after_taker = st['total'] - st['n_trades'] * taker_fee
        fee_ok = '✓' if after_maker > 0 else ('~' if after_taker > -abs(st['total'])*0.1 else '✗')
        pct = st['n_trades'] / len(test_df) * 100
        sweep_results.append({
            'threshold': thresh, 'n_trades': st['n_trades'],
            'wr': st['wr'], 'avg_pnl': avg_pnl,
            'total': st['total'], 'after_maker': after_maker,
            'after_taker': after_taker,
        })
        print(f"  {thresh:>10.0%}  {st['n_trades']:>7,}  {pct:>6.1f}%  "
              f"  {st['wr']*100:>7.2f}%  {avg_pnl:>+8.2f}  "
              f"{st['total']:>+11,.0f}  {after_maker:>+12,.0f}  "
              f"{after_taker:>+12,.0f}  {fee_ok}")

    # Best threshold by after-maker P&L
    best = max(sweep_results, key=lambda x: x['after_maker'])
    print()
    print(f"  Best threshold (after maker fees): {best['threshold']:.0%}  "
          f"→ {best['n_trades']:,} trades  "
          f"win rate {best['wr']*100:.2f}%  "
          f"after-maker P&L ${best['after_maker']:+,.0f}")

    # ── Calibration: confidence vs actual success rate ────────────────────
    # Focus on UP and DOWN signals only (exclude FLAT predictions).
    # For each confidence bucket, measure what fraction of signals were correct.
    # "Correct" = price actually moved in the predicted direction.
    print()
    print(f"  Calibration (UP/DOWN signals only — does confidence predict accuracy?):")
    print(f"  {'Conf range':>12}  {'Samples':>8}  {'Actual win%':>11}  "
          f"{'Expected':>9}  {'Calibrated?':>12}")
    print(f"  {'-'*12}  {'-'*8}  {'-'*11}  {'-'*9}  {'-'*12}")

    # Only look at bars where model predicted UP or DOWN (not FLAT)
    signal_mask = test_df['pred_raw'] != 0
    sig_df = test_df[signal_mask].copy()

    # Was the prediction correct? Price moved in the predicted direction.
    sig_df['correct'] = (
        ((sig_df['pred_raw'] ==  1) & (sig_df['price_move'] > 0)) |
        ((sig_df['pred_raw'] == -1) & (sig_df['price_move'] < 0))
    ).astype(int)

    # Bucket by confidence in steps of 5%
    bins      = np.arange(0.33, 1.01, 0.05)
    calib_rows = []
    for i in range(len(bins) - 1):
        lo, hi = bins[i], bins[i+1]
        bucket = sig_df[(sig_df['confidence'] >= lo) & (sig_df['confidence'] < hi)]
        if len(bucket) < 10:
            continue
        actual_wr  = bucket['correct'].mean()
        mid_conf   = (lo + hi) / 2
        n          = len(bucket)
        # Is actual win rate close to stated confidence? (within ±5%)
        calibrated = '✓ good' if abs(actual_wr - mid_conf) < 0.05 else \
                     ('~ ok'  if abs(actual_wr - mid_conf) < 0.10 else '✗ off')
        calib_rows.append({
            'lo': lo, 'hi': hi, 'mid': mid_conf,
            'n': n, 'actual_wr': actual_wr, 'calibrated': calibrated
        })
        print(f"  {lo*100:>5.0f}–{hi*100:<4.0f}%  {n:>8,}  "
              f"{actual_wr*100:>10.1f}%  {mid_conf*100:>8.0f}%  {calibrated:>12}")

    if calib_rows:
        # Summary: is confidence monotonically increasing with win rate?
        wrs = [r['actual_wr'] for r in calib_rows]
        monotonic = all(wrs[i] <= wrs[i+1] for i in range(len(wrs)-1))
        print()
        if monotonic:
            print(f"  ✓ Win rate increases with confidence — model is well-ordered")
        else:
            print(f"  ~ Win rate does not strictly increase with confidence")
            print(f"    This suggests confidence scores are not perfectly calibrated")
        print(f"    (Perfect calibration: 60% confidence → 60% actual win rate)")

    # ── Verdict ───────────────────────────────────────────────────────────
    print()
    if s['total'] > 0 and s['total'] > bnh_total:
        verdict = "✓ Strategy profitable and beats buy-and-hold (before fees)"
    elif s['total'] > 0:
        verdict = "~ Strategy profitable but underperforms buy-and-hold"
    else:
        verdict = "✗ Strategy loses money before fees"

    if best['after_maker'] > 0:
        verdict += (f"\n  ✓ After maker fees: profitable at ≥{best['threshold']:.0%} "
                    f"confidence ({best['n_trades']:,} trades)")
    else:
        verdict += f"\n  ✗ No confidence threshold produces profit after maker fees"

    binom_p = None
    try:
        from scipy.stats import binomtest
        n_wins = int(round(s['n_trades'] * s['wr']))
        binom_p = binomtest(n_wins, s['n_trades'], 0.5, alternative='greater').pvalue
        sig = f"p={binom_p:.4f} {'✓ significant' if binom_p < 0.05 else '✗ not significant'}"
        verdict += f"\n  Win rate {s['wr']*100:.2f}% — binomial test: {sig}"
    except ImportError:
        pass

    print(f"  Verdict: {verdict}")

    # ── Plots ─────────────────────────────────────────────────────────────
    DARK  = '#0d1117'
    PANEL = '#161b22'
    GRID  = '#21262d'
    TEXT  = '#e6edf3'
    UP    = '#3fb950'
    DOWN  = '#f85149'
    ACC   = '#58a6ff'
    GOLD  = '#f0883e'

    fig = plt.figure(figsize=(18, 18), facecolor=DARK)
    gs  = plt.GridSpec(4, 2, figure=fig, hspace=0.42, wspace=0.30,
                       left=0.07, right=0.97, top=0.94, bottom=0.06)

    def style(ax, title):
        ax.set_facecolor(PANEL)
        ax.tick_params(colors=TEXT, labelsize=8)
        ax.set_title(title, color=TEXT, fontsize=9, fontweight='bold', pad=6)
        for sp in ax.spines.values():
            sp.set_edgecolor(GRID)
        ax.grid(color=GRID, linewidth=0.5, alpha=0.7)
        ax.yaxis.label.set_color(TEXT)
        ax.xaxis.label.set_color(TEXT)

    # ── 1. Cumulative P&L ─────────────────────────────────────────────────
    ax = fig.add_subplot(gs[0, :])
    ax.plot(test_df['dt'], test_df['strategy_cum'], color=ACC,
            linewidth=1.5, label=f'Strategy{conf_str} (zero fees)')
    ax.plot(test_df['dt'], test_df['bnh_cum'], color=GOLD,
            linewidth=1.2, linestyle='--', label='Buy & Hold')
    ax.axhline(0, color=GRID, linewidth=0.8)
    ax.fill_between(test_df['dt'], test_df['strategy_cum'], 0,
                    where=test_df['strategy_cum'] >= 0, alpha=0.12, color=UP)
    ax.fill_between(test_df['dt'], test_df['strategy_cum'], 0,
                    where=test_df['strategy_cum'] < 0,  alpha=0.12, color=DOWN)
    ax.set_ylabel('Cumulative P&L (USD, 1 BTC)', color=TEXT)
    ax.legend(fontsize=9, facecolor=PANEL, labelcolor=TEXT, edgecolor=GRID)
    ax.tick_params(colors=TEXT)
    for sp in ax.spines.values():
        sp.set_edgecolor(GRID)
    ax.grid(color=GRID, linewidth=0.5, alpha=0.7)
    ax.set_facecolor(PANEL)
    ax.set_title(
        f'Cumulative P&L | Strategy ${s["total"]:+,.0f} vs B&H ${bnh_total:+,.0f} '
        f'| Win {s["wr"]*100:.2f}% | {s["n_trades"]:,} trades',
        color=TEXT, fontsize=10, fontweight='bold'
    )
    ax.xaxis.set_tick_params(rotation=20)

    # ── 2. Drawdown ───────────────────────────────────────────────────────
    ax2 = fig.add_subplot(gs[1, 0])
    ax2.fill_between(test_df['dt'], test_df['drawdown'], 0,
                     color=DOWN, alpha=0.7)
    ax2.set_ylabel('Drawdown (USD)')
    ax2.xaxis.set_tick_params(rotation=20)
    style(ax2, 'Strategy Drawdown')

    # ── 3. Confidence distribution ────────────────────────────────────────
    ax3 = fig.add_subplot(gs[1, 1])
    ax3.hist(confidence[preds_raw != 0], bins=50,
             color=ACC, alpha=0.7, label='Signalled bars', density=True)
    ax3.hist(confidence[preds_raw == 0], bins=50,
             color=GRID, alpha=0.5, label='Flat bars', density=True)
    if min_confidence > 0:
        ax3.axvline(min_confidence, color=GOLD, linewidth=1.5,
                    linestyle='--', label=f'Current filter ({min_confidence:.0%})')
    ax3.legend(fontsize=8, facecolor=PANEL, labelcolor=TEXT, edgecolor=GRID)
    ax3.set_xlabel('Model confidence (max class probability)')
    style(ax3, 'Confidence Distribution')

    # ── 4. Win rate vs threshold ──────────────────────────────────────────
    ax4 = fig.add_subplot(gs[2, 0])
    thresh_vals = [r['threshold'] for r in sweep_results]
    wr_vals     = [r['wr'] * 100 for r in sweep_results]
    trade_vals  = [r['n_trades'] for r in sweep_results]
    ax4.plot(thresh_vals, wr_vals, color=ACC, linewidth=2, marker='o',
             markersize=5, label='Win rate %')
    ax4.axhline(50, color=GRID, linewidth=0.8, linestyle='--')
    ax4.axhline(52, color=UP, linewidth=0.8, linestyle=':',
                label='~52% maker fee break-even')
    if min_confidence > 0:
        ax4.axvline(min_confidence, color=GOLD, linewidth=1.5,
                    linestyle='--', label=f'Current ({min_confidence:.0%})')
    ax4.set_xlabel('Confidence threshold')
    ax4.set_ylabel('Win rate (%)')
    ax4.legend(fontsize=8, facecolor=PANEL, labelcolor=TEXT, edgecolor=GRID)
    style(ax4, 'Win Rate vs Confidence Threshold')

    ax4b = ax4.twinx()
    ax4b.bar(thresh_vals, trade_vals, width=0.03, alpha=0.3,
             color=GOLD, label='# trades')
    ax4b.set_ylabel('Number of trades', color=GOLD)
    ax4b.tick_params(colors=GOLD, labelsize=7)
    ax4b.set_facecolor(PANEL)

    # ── 5. After-fee P&L vs threshold ────────────────────────────────────
    ax5 = fig.add_subplot(gs[2, 1])
    maker_vals = [r['after_maker'] for r in sweep_results]
    taker_vals = [r['after_taker'] for r in sweep_results]
    ax5.plot(thresh_vals, maker_vals, color=UP,   linewidth=2, marker='o',
             markersize=5, label='After maker fees (0.04%)')
    ax5.plot(thresh_vals, taker_vals, color=DOWN, linewidth=2, marker='s',
             markersize=5, label='After taker fees (0.10%)')
    ax5.axhline(0, color=TEXT, linewidth=0.8)
    if min_confidence > 0:
        ax5.axvline(min_confidence, color=GOLD, linewidth=1.5,
                    linestyle='--', label=f'Current ({min_confidence:.0%})')
    ax5.set_xlabel('Confidence threshold')
    ax5.set_ylabel('Total P&L after fees (USD)')
    ax5.legend(fontsize=8, facecolor=PANEL, labelcolor=TEXT, edgecolor=GRID)
    style(ax5, 'After-Fee P&L vs Confidence Threshold')

    # ── 6. Calibration curve ──────────────────────────────────────────────
    ax6 = fig.add_subplot(gs[3, 0])
    if calib_rows:
        mids    = [r['mid']       for r in calib_rows]
        actuals = [r['actual_wr'] for r in calib_rows]
        sizes   = [min(r['n'] / 50, 200) for r in calib_rows]  # bubble size

        ax6.plot([0.33, 1.0], [0.33, 1.0], color=GRID, linewidth=1,
                 linestyle='--', label='Perfect calibration')
        ax6.scatter(mids, actuals, s=sizes, color=ACC, alpha=0.8,
                    zorder=3, label='Actual win rate (bubble=sample size)')
        ax6.plot(mids, actuals, color=ACC, linewidth=1.2, alpha=0.5)
        ax6.axhline(0.5, color=GRID, linewidth=0.6, linestyle=':')
        ax6.set_xlabel('Model confidence (stated)')
        ax6.set_ylabel('Actual win rate')
        ax6.set_xlim(0.30, 1.0)
        ax6.set_ylim(0.30, 1.0)
        ax6.legend(fontsize=8, facecolor=PANEL, labelcolor=TEXT, edgecolor=GRID)
    else:
        ax6.text(0.5, 0.5, 'Not enough samples per bucket',
                 ha='center', va='center', color=GRID, transform=ax6.transAxes)
    style(ax6, 'Calibration Curve (UP/DOWN only) — stated vs actual win rate')

    # ── 7. Win rate by confidence bucket (bar chart) ──────────────────────
    ax7 = fig.add_subplot(gs[3, 1])
    if calib_rows:
        x_pos  = np.arange(len(calib_rows))
        labels = [f"{r['lo']*100:.0f}–{r['hi']*100:.0f}%" for r in calib_rows]
        colors = [UP if r['actual_wr'] >= 0.52 else
                  (GOLD if r['actual_wr'] >= 0.50 else DOWN)
                  for r in calib_rows]
        bars = ax7.bar(x_pos, [r['actual_wr']*100 for r in calib_rows],
                       color=colors, edgecolor=DARK, linewidth=0.5, alpha=0.85)
        ax7.axhline(50, color=GRID,  linewidth=0.8, linestyle='--', label='50% (random)')
        ax7.axhline(52, color=UP,    linewidth=0.8, linestyle=':',  label='~52% maker fee b/e')
        ax7.set_xticks(x_pos)
        ax7.set_xticklabels(labels, rotation=45, ha='right', fontsize=7, color=TEXT)
        ax7.set_ylabel('Actual win rate (%)')
        # Sample count on top of each bar
        for bar, r in zip(bars, calib_rows):
            ax7.text(bar.get_x() + bar.get_width()/2,
                     bar.get_height() + 0.3,
                     f"n={r['n']}", ha='center', va='bottom',
                     color=TEXT, fontsize=6)
        ax7.legend(fontsize=8, facecolor=PANEL, labelcolor=TEXT, edgecolor=GRID)
    else:
        ax7.text(0.5, 0.5, 'Not enough samples per bucket',
                 ha='center', va='center', color=GRID, transform=ax7.transAxes)
    style(ax7, 'Actual Win Rate by Confidence Bucket (UP/DOWN signals)')

    fig.suptitle('BTC Strategy Backtest — Confidence Filter Analysis',
                 color=TEXT, fontsize=13, fontweight='bold', y=0.97)

    out_path = os.path.join(output_dir, 'btc_backtest.png')
    fig.savefig(out_path, dpi=150, bbox_inches='tight', facecolor=DARK)
    plt.close(fig)
    print(f"\n  Chart saved → {out_path}")
    return test_df


# ─────────────────────────────────────────────
#  SUMMARY
# ─────────────────────────────────────────────

def print_summary(df, mi_df, rolling_df, features):
    print_header("SUMMARY & RECOMMENDATIONS")

    # Overall verdict — regime-aware: pooled mean is misleading if low-vol
    # drags down the average. Check both pooled and regime-filtered signal.
    if rolling_df is not None and len(rolling_df) > 0:
        mean_lift = rolling_df['lift'].mean()
        pct_pos   = (rolling_df['lift'] > 0.01).mean()

        # Regime-aware verdict: if regime analysis found tradeable regimes,
        # don't penalise the overall verdict for low-vol noise windows.
        if 'vol_regime' in df.columns:
            mid_high = df['vol_regime'].astype(str).isin(['mid', 'high'])
            frac_tradeable = mid_high.mean()
        else:
            frac_tradeable = 1.0

        if pct_pos > 0.4:
            verdict = "✓ SIGNAL EXISTS — worth building a model"
        elif pct_pos > 0.2 or mean_lift > 0.005:
            verdict = "~ REGIME-DEPENDENT SIGNAL — trade mid/high volatility only"
        else:
            verdict = "✗ NO RELIABLE SIGNAL — try different horizon or features"

        print(f"  Overall verdict : {verdict}")
        print(f"  Pooled lift     : {mean_lift*100:+.2f}% mean across all windows")
        print(f"  Stable windows  : {pct_pos*100:.0f}% of windows show >1% lift")
        print(f"  Tradeable bars  : {frac_tradeable*100:.0f}% of data in mid/high vol regime")
        if XGBOOST_OK and 'xgb_lift' in rolling_df.columns:
            xgb_mean = rolling_df['xgb_lift'].mean()
            lr_mean  = rolling_df['lr_lift'].mean()
            gap = (xgb_mean - lr_mean) * 100
            if gap > 1.0:
                print(f"  XGBoost edge    : XGB beats LR by {gap:.2f}% → non-linear signal present")
                print(f"                    Neural network is worth trying next.")
            elif gap < -1.0:
                print(f"  LR edge         : LR beats XGB by {abs(gap):.2f}% → signal is linear")
                print(f"                    Neural network unlikely to add value.")
            else:
                print(f"  LR ≈ XGBoost    : gap {abs(gap):.2f}% — signal is mostly linear")

    # Best features
    if not mi_df.empty:
        top = mi_df[mi_df['mi'] > 0.001]['feature'].tolist()
        if top:
            print(f"\n  Best features (MI > 0.001):")
            for f in top[:5]:
                print(f"    • {f}")
        else:
            print(f"\n  No features exceeded MI threshold of 0.001")
            print(f"  Consider: longer prediction horizon, more data, or additional features")

    # Data volume
    days_covered = (df['dt'].max() - df['dt'].min()).total_seconds() / 86400
    print(f"\n  Data coverage   : {days_covered:.1f} days, {len(df):,} rows")
    if days_covered < 14:
        print(f"  ⚠ Less than 14 days of data — results may not be stable")
    if len(df) < 2000:
        print(f"  ⚠ Less than 2,000 rows — collect more data before training a model")

    print()


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='BTC orderbook signal analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--days',      type=int,   default=0,    help='Look back N days from latest data timestamp. 0 = use all data (default: 0)')
    parser.add_argument('--threshold', type=float, default=5.0,  help='USD threshold for UP/DOWN (default: 5.0)')
    parser.add_argument('--dir',       type=str,   default='.',  help='Directory with CSV files (default: current dir)')
    parser.add_argument('--out',       type=str,   default='.',  help='Output directory for plots (default: current dir)')
    parser.add_argument('--horizon',   type=int,   default=1,    help='Bars ahead to predict (default: 1 = 15s, 4 = 60s, 8 = 2min)')
    parser.add_argument('--window-rows', type=int, default=0,    help='Rows per rolling train/test window. 0 = auto-size (default: 0)')
    parser.add_argument('--max-windows', type=int, default=100,  help='Target number of rolling windows for large datasets (default: 100)')
    parser.add_argument('--confidence',  type=float, default=0.0, help='Min XGBoost confidence to trade (0.0=all signals, 0.6=60%% confident only)')
    args = parser.parse_args()

    print(f"\n{'='*72}")
    print(f"  BTC SIGNAL ANALYSIS")
    days_str = f"last {args.days} days" if args.days > 0 else "all available data"
    print(f"  Using {days_str} | Threshold ±${args.threshold} | Horizon {args.horizon} bar(s) ({args.horizon * WINDOW_SEC}s) | Dir: {args.dir}")
    print(f"{'='*72}")

    # Load
    section("Loading files")
    files = find_csv_files(args.dir)
    if not files:
        print(f"  No CSV files found in '{args.dir}'.")
        sys.exit(1)
    print(f"  Found {len(files)} file(s):\n")
    df = load_files(files)
    print(f"\n  Total rows before cleaning: {len(df)}")

    # Filter by the data's own timestamp (not file mtime — files may be old
    # but still contain recent data, or vice versa).
    # --days 0 means "use all data, no date filtering".
    if args.days > 0:
        dt_col = pd.to_datetime(df['spot_datetime'], errors='coerce')
        cutoff = dt_col.max() - timedelta(days=args.days)
        before = len(df)
        df = df[dt_col >= cutoff].reset_index(drop=True)
        print(f"  Filtered to last {args.days} day(s): {before} → {len(df)} rows"
              f"  (cutoff: {cutoff})")
    else:
        print(f"  --days 0: using all available data ({len(df)} rows)")

    # Prepare
    section("Preparing data")
    df = prepare(df, args.threshold, args.horizon)
    features = list(FEATURE_DEFS.keys())

    available = [f for f in features if f in df.columns and df[f].notna().sum() > 100]
    dropped   = [f for f in features if f not in available]
    if dropped:
        print(f"  Dropped features (insufficient data): {dropped}")
    features = available
    print(f"  Clean rows: {len(df)}")
    print(f"  Features  : {len(features)}")

    if len(df) < 50:
        print("  ERROR: Not enough data rows. Check file format, threshold, or --days filter.")
        sys.exit(1)

    # Analyses
    class_distribution(df, args.threshold, args.horizon)
    mean_by_class(df, features)
    lag_correlations(df, features)

    mi_df    = pd.DataFrame()
    rolling_df = None

    if SKLEARN_OK:
        mi_df      = mutual_information(df, features)
        models_baseline(df, features)
        rolling_df = rolling_accuracy(df, features, args.window_rows, args.max_windows)
        regime_analysis(df, features)
        backtest_strategy(df, features, args.horizon, args.out, args.confidence)
    else:
        print("\n  Skipping ML steps (scikit-learn not installed).")

    # Plots
    plot_path = make_plots(df, features, mi_df, rolling_df, args.out)

    # Summary
    print_summary(df, mi_df, rolling_df, features)


if __name__ == '__main__':
    main()

