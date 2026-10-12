"""N1 long BTC / short a diversified alt basket, beta-neutral — library (pre-registered in next_signal_ideas.md,
Round 6 / N1). Daily conventions as xsec.py / lowvol.py:
  weights at day d use data up to 00:00 of d and are held d -> d + 1, earning W[d] . TR[d] (TR includes funding)
  short book   inverse-RVOL30 weights over a volume-rank band, total 1, cap CAP per coin (excess redistributed);
               formed only when >= MIN_SHARE of the band has an RVOL30 value, otherwise empty that day
  hedge        long BTC at beta[d] = -(OLS slope of the staggered short book's daily return on BTC's, d-60 .. d-1)
  staggering   H cohorts (lowvol.rolling_mean_rows); cost 4.5 bp per side on all turnover incl. the hedge
  P&L          on the short notional (= 1)
Verdict (one registered cell, H = 7): PASS / ALIVE (forward test) / DEAD.
"""
import numpy as np, pandas as pd
import xsec as X
import lowvol as LV

CAP, MIN_SHARE = 0.02, 0.90
H_PRIMARY, H_INFO = 7, 1
BLOCK_D, N_BLOCKS = 182, 5
WORST_LIMIT = -0.20
ANN = 365


def short_weights(U, V, cap=CAP, ew=False, min_share=MIN_SHARE):
    """(D, N) short-book weights (positive numbers; the book is short them). Empty row when < min_share of the band
    has a vol value. ew: equal weight over the names with a value (cap still applies)."""
    k = U.shape[1]
    Ic = np.where(U < 0, 0, U)
    v = np.take_along_axis(V, Ic, 1)
    good = np.isfinite(v) & (v > 0) & (U >= 0)
    ok = good.sum(1) >= int(np.ceil(min_share * k - 1e-9))
    Us = np.where(ok[:, None], U, -1)
    Vw = np.where(np.isfinite(V) & (V > 0), 1.0, np.nan) if ew else V
    return LV.inv_vol_weights(Us, Vw, 1.0, cap)


def book(Ws, TR, btc, H):
    """Full positions (D, N): -Ws staggered over H, plus the BTC hedge. Returns full, beta, live."""
    full, beta, live, _ = LV.hedged_book(np.zeros_like(Ws), Ws, TR, btc, H)
    return full, beta, live


def window(live, days, t_hi, D):
    """Contiguous day indices from the first live day to the last day < t_hi (never the final panel row)."""
    first = int(np.flatnonzero(live)[0])
    last = min(int(np.searchsorted(days, t_hi, side="left")), D - 1)
    return np.arange(first, last)


def decompose(full, TR, F, btc, idx):
    """Daily P&L split: alt price, alt funding, BTC price, BTC funding, cost. Sums to net."""
    W, T, Fu = full[idx], TR[idx], F[idx]
    R = T + Fu                                                      # TR = price return - funding paid by a long
    alt = np.ones(W.shape[1], bool); alt[btc] = False
    out = dict(alt_price=(W[:, alt] * R[:, alt]).sum(1), alt_funding=-(W[:, alt] * Fu[:, alt]).sum(1),
               btc_price=W[:, btc] * R[:, btc], btc_funding=-W[:, btc] * Fu[:, btc], cost=-LV.day_costs(W))
    return out


def stats(net, gross, full, TR, btc, idx):
    n = len(net); x = np.sort(net); c = int(np.floor(0.01 * n)); k1 = max(1, c)
    bt = TR[idx, btc]
    blocks = [float(net[i * BLOCK_D:(i + 1) * BLOCK_D].mean()) for i in range(min(N_BLOCKS, n // BLOCK_D))]
    W = full[idx]
    return dict(n=n, net_bp=float(net.mean() * 1e4), gross_bp=float(gross.mean() * 1e4), cost_bp=float((gross - net).mean() * 1e4),
                nw_t=float(X.nw_t(net)), median_bp=float(np.median(net) * 1e4), trimmed_bp=float(x[c:n - c].mean() * 1e4),
                ann_pct=float(net.mean() * ANN * 100), sharpe=float(net.mean() / net.std(ddof=1) * np.sqrt(ANN)),
                std_pct=float(net.std(ddof=1) * 100), worst_day_pct=float(x[0] * 100), q01_pct=float(np.percentile(net, 1) * 100),
                worst1pct_mean_pct=float(x[:k1].mean() * 100), best_day_pct=float(x[-1] * 100),
                max_dd_pct=float(X.max_drawdown(net) * 100), beta_to_btc=float(np.cov(net, bt)[0, 1] / bt.var(ddof=1)),
                mean_hedge=float(W[:, btc].mean()), mean_short=float(-np.where(W < 0, W, 0).sum(1).mean()),
                turnover=float(np.abs(np.diff(np.vstack([np.zeros((1, W.shape[1])), W]), axis=0)).sum(1).mean()),
                blocks_bp=[b * 1e4 for b in blocks], blocks_pos=int(sum(b > 0 for b in blocks)), blocks_n=len(blocks))


def verdict(st, lag_net_bp):
    """Registered three-way verdict for the H = 7 cell."""
    crit = {"net mean > 0 and NW t >= 2": st["net_bp"] > 0 and st["nw_t"] >= 2,
            "median and mean > 0": st["median_bp"] > 0 and st["net_bp"] > 0,
            "trimmed mean > 0": st["trimmed_bp"] > 0,
            f">= 4 of {N_BLOCKS} {BLOCK_D}-day blocks positive": st["blocks_n"] == N_BLOCKS and st["blocks_pos"] >= 4,
            "1 h lag net > 0": lag_net_bp > 0,
            "worst day > -20 %": st["worst_day_pct"] > WORST_LIMIT * 100}
    others = [v for k, v in crit.items() if not k.startswith("net mean")]
    if all(crit.values()):
        out = "PASS"
    elif all(others) and st["net_bp"] > 0 and 1 <= st["nw_t"] < 2:
        out = "ALIVE"
    else:
        out = "DEAD"
    return out, crit


def arm(U, V, TR0, TR1, F, btc, days, t_hi, H, cap=CAP, ew=False, idx=None):
    """One construction evaluated over idx (default: its own live window). Returns stats, lag, series, positions."""
    Ws = short_weights(U, V, cap=cap, ew=ew)
    full, beta, live = book(Ws, TR0, btc, H)
    if idx is None:
        idx = window(live, days, t_hi, len(days))
    net, gross = LV.net_series(full, TR0, idx)
    net1, _ = LV.net_series(full, TR1, idx)
    st = stats(net, gross, full, TR0, btc, idx)
    st["lag_net_bp"] = float(net1.mean() * 1e4)
    st["live_days"] = int(live[idx].sum())
    return dict(stats=st, net=net, gross=gross, full=full, idx=idx, live=live, beta=beta,
                decomp={k: float(v.mean() * 1e4) for k, v in decompose(full, TR0, F, btc, idx).items()})


def worst_coins(full, TR, idx, btc, syms, days, top=10):
    """Largest single-coin daily loss contribution and the `top` worst book days with the coin responsible."""
    contrib = full[idx] * TR[idx]
    contrib[:, btc] = np.where(np.isfinite(contrib[:, btc]), contrib[:, btc], 0)
    alt = contrib.copy(); alt[:, btc] = np.inf
    i, j = np.unravel_index(np.argmin(alt), alt.shape)
    day_net = contrib.sum(1)
    rows = []
    for d in np.argsort(day_net)[:top]:
        jj = int(np.argmin(alt[d]))
        rows.append(dict(day=str(pd.to_datetime(days[idx[d]], unit="s").date()), book_pct=float(day_net[d] * 100),
                         coin=syms[jj], coin_pct=float(alt[d, jj] * 100), coin_ret_pct=float(TR[idx[d], jj] * 100),
                         btc_pct=float(contrib[d, btc] * 100)))
    return dict(largest=dict(day=str(pd.to_datetime(days[idx[i]], unit="s").date()), coin=syms[j], pct=float(alt[i, j] * 100),
                             coin_ret_pct=float(TR[idx[i], j] * 100)), worst_days=rows)
