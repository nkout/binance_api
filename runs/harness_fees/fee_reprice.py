"""R3 (next_signal_ideas.md Round 2): re-price every measured signal at the fees actually reachable.

Tier-free output first: each signal's BREAK-EVEN round trip (gross per trade, with its day-bootstrap
CI). Then the comparison against named tiers. Only VIP0 and the BNB discount are well sourced;
higher VIP rates vary between secondary sources and require tens of millions USD of 30-day volume,
so they are shown as illustrative only.

Inputs: run009f_scores.npz (5 s netted book), v1_stage2_scores.npz + data/v1_year/v1_15s.parquet
(1d confident tail). CPU, ~1 min.   PYTHONPATH must provide pyarrow.
"""
import json, os
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
RUNS = os.path.join(HERE, "..")
rng = np.random.default_rng(0)

# per-side fee in bp: (maker, taker)
TIERS = {
    "VIP0": (2.0, 5.0),
    "VIP0+BNB": (1.8, 4.5),
    "VIP4 (illustrative)": (1.0, 3.0),
    "VIP9 (illustrative)": (0.0, 1.7),
}
ROUTES = {"taker-taker": ("t", "t"), "taker-in/maker-out": ("t", "m"), "maker-maker": ("m", "m")}


def rt(tier, route):
    mk, tk = TIERS[tier]
    return sum(tk if leg == "t" else mk for leg in ROUTES[route])


def boot(v, days, n=4000):
    ud = np.unique(days)
    s = np.array([v[days == u].sum() for u in ud]); c = np.array([(days == u).sum() for u in ud])
    bi = rng.integers(0, len(ud), (n, len(ud)))
    m = s[bi].sum(1) / c[bi].sum(1)
    return float(np.percentile(m, 2.5)), float(np.percentile(m, 97.5))


# ------------------------------------------------ A. 5 s netted book (horizon_economics §6)
z = np.load(os.path.join(RUNS, "run009f_scores.npz"), allow_pickle=True)
close, dt, fold = z["close"].astype(float), z["dt_s"], z["fold_of"]
W, n = int(z["window_sec"]), len(z["close"])
step_ok = np.zeros(n, bool); step_ok[:-1] = (np.diff(dt) == W) & (fold[:-1] == fold[1:])
r1 = np.zeros(n); r1[:-1] = (close[1:] / close[:-1] - 1) * 1e4; r1[~step_ok] = 0


def cstar(pos):
    pos = np.where(step_ok, pos, 0.0)
    return (pos * r1).sum() / np.abs(np.diff(np.r_[0.0, pos])).sum()


s = z["pred"][:, 0].astype(float)
A = {"model c* (one-way, bp)": cstar(s / s.std()), "oracle next-bar c*": cstar(np.sign(r1))}

# ------------------------------------------------ B. 1d confident tail (v1_stage2_probe §3)
zz = np.load(os.path.join(RUNS, "v1_stage2_scores.npz"))
raw = pd.read_parquet(os.path.join(RUNS, "..", "data", "v1_year", "v1_15s.parquet"),
                      columns=["ts", "future_bid_close", "future_ask_close"])
T0 = int(raw.ts.iloc[0]); slot = np.round((raw.ts.to_numpy() - T0) / 15).astype(np.int64)
G = slot[-1] + 1; lm = np.full(G, np.nan)
lm[slot] = np.log((raw.future_bid_close + raw.future_ask_close).to_numpy() / 2)
ts = T0 + np.arange(G) * 15
ix = np.round((zz["ts"] - T0) / 15).astype(np.int64)
p = np.full(G, np.nan); p[ix] = zz["p_evt_6"]
trig = np.zeros(G, bool); trig[ix] = zz["trig_0.05"]
month = pd.to_datetime(ts, unit="s", utc=True).strftime("%y%m").to_numpy(); day = ts // 86400
H = 6
months = list(dict.fromkeys(month[np.isfinite(p)]))
cand = trig & np.isfinite(p); conf = np.abs(p - 0.5)


def tail_trades(q, d):
    take = []
    for i, m in enumerate(months[1:], 1):
        th = np.quantile(conf[cand & (month == months[i - 1])], q)
        busy = -1
        for t in np.flatnonzero(cand & (month == m) & (conf >= th)):
            if t > busy and t + d + H < G:
                take.append(t); busy = t + d + H
    take = np.array(take)
    seg_ok = np.array([np.isfinite(lm[t:t + d + H + 1]).all() for t in take])
    take = take[seg_ok]
    side = np.where(p[take] >= 0.5, 1, -1)
    g = side * (lm[take + d + H] - lm[take + d]) * 1e4
    return g, day[take]


B = []
for q in (0.99, 0.98, 0.95):
    for d in (0, 1):
        g, dd = tail_trades(q, d)
        lo, hi = boot(g, dd)
        row = dict(signal=f"1d top {100 - 100 * q:.0f}% delay {d * 15}s", n=len(g), gross=float(g.mean()),
                   gross_ci=[lo, hi], breakeven_rt=float(g.mean()))
        for tier in TIERS:
            f = rt(tier, "taker-taker")
            row[f"net {tier} taker-taker"] = float(g.mean() - f)
            row[f"CI lo > fee {tier}"] = bool(lo > f)
        B.append(row)

# ------------------------------------------------ C. previously measured gross per trade (quoted)
C = [
    ("5 s up15 @0.01 % best gross cell (run009d.offline §2)", 8.48, "[+1.40, +11.04], collapses to +1.31 without fold 3"),
    ("90 s up18 @0.1 % maker, filled (run009d.analysis)", 0.0, "≈ 0 after adverse selection"),
    ("4 h mean reversion, 7 yr (v1_4h_feasibility §9)", -1.39, "[−7.72, +4.89]"),
    ("breakout primary (breakout_probe §3)", 3.70, "net CI at 10 bp [−26.7, +15.0]"),
    ("1d primary, all triggers (v1_stage2_probe §2)", 0.64, "12,863 trades"),
]

# ------------------------------------------------ report
print("Fee tiers (per side, bp):", {k: v for k, v in TIERS.items()})
print("Round trips (bp):")
for tier in TIERS:
    print(f"  {tier:22} " + "  ".join(f"{r}: {rt(tier, r):.2f}" for r in ROUTES))

print("\nA. 5 s netted book (one-way break-even vs one-way fee)")
for k, v in A.items():
    print(f"  {k:26} {v:.3f} bp")
for tier, (mk, tk) in TIERS.items():
    print(f"  {tier:22} maker one-way {mk:.2f} -> model short by {mk / A['model c* (one-way, bp)']:.1f}x"
          if mk > 0 else f"  {tier:22} maker one-way 0.00 -> model c* clears (but see note)")

print("\nB. 1d confident tail, taker-taker (entry must be immediate; hold-to-horizon exit is a market order)")
for r in B:
    print(f"  {r['signal']:26} n {r['n']:4d} gross {r['gross']:+6.2f} CI [{r['gross_ci'][0]:+.2f},{r['gross_ci'][1]:+.2f}]  "
          + "  ".join(f"{t.split()[0]} net {r[f'net {t} taker-taker']:+.2f}{'*' if r[f'CI lo > fee {t}'] else ''}" for t in TIERS))
print("  (* = gross CI lower bound clears that tier's round trip)")

print("\nC. Other measured signals, gross per trade (bp) vs cheapest reachable taker RT "
      f"{rt('VIP0+BNB', 'taker-taker'):.1f} / maker RT {rt('VIP0+BNB', 'maker-maker'):.1f}")
for name, g, note in C:
    print(f"  {name:55} {g:+6.2f}   {note}")

json.dump(dict(tiers=TIERS, A=A, B=B, C=C), open(os.path.join(HERE, "fee_reprice_results.json"), "w"),
          indent=1, default=float)
