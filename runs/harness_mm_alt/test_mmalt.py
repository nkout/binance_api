"""Tests for the M1 mid-cap alt market-making harness — real library functions on synthetic data.

  1. readers: in-memory zip round-trip for bookTicker / aggTrades (mid, rel_spread_bp, d, t_s), header
     and no-header archive layouts
  2. formulas on hand-built quotes + trades: quoted spread; Huang-Stoll e / r / impact with
     hand-computed expected values; the identity e == r + impact exactly; the mark-out identity
     capture + adverse == net exactly; net_after_fee = net - (MAKER_BP - REBATE_BP)
  3. OFI: Cont-Kukanov-Stoikov L1 on a scripted book with a known answer; planted OFI->mid R2 > 0
     and a shuffled-mid control R2 ~ 0
  4. look-ahead / leak: scrambling every book + trade row after T leaves the metrics restricted to
     trade_time + tau <= T bit-identical, and the future does change (the test can see it)
  5. coverage: a partial-day file returns coverage_fraction < 1
  6. planted maker-edge world (wide spread, uninformed flow) -> pool() PASSES every criterion
  7. >= 10 null worlds (iid mid, no spread edge, fee >= half-spread) -> at most 1 of 10 PASSES
  8. end-to-end: synthetic derived rows for 4 days through run_m1 -> JSON written + VERDICT emitted;
     zero data -> graceful "no data"
  9. fetcher: fetch_symbol_day with a mocked get resolving an in-memory zip; streaming + resume
"""
import io, json, os, sys, tempfile, zipfile
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import mmalt as M
import fetch_mm as F
import run_m1 as R

fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))
    fails += (not cond)


# ------------------------------------------------------------------ builders
def mk_bt(rows):
    """rows: (t_ms, bid, bid_qty, ask, ask_qty)."""
    d = pd.DataFrame({
        "update_id": np.arange(len(rows), dtype=np.int64),
        "best_bid_price": [r[1] for r in rows], "best_bid_qty": [r[2] for r in rows],
        "best_ask_price": [r[3] for r in rows], "best_ask_qty": [r[4] for r in rows],
        "transaction_time": [r[0] for r in rows], "event_time": [r[0] for r in rows],
    })
    d["mid"] = (d.best_bid_price + d.best_ask_price) / 2.0
    d["spread"] = d.best_ask_price - d.best_bid_price
    d["rel_spread_bp"] = d.spread / d.mid * 1e4
    return d


def mk_tr(rows):
    """rows: (t_ms, price, qty, isBuyerMaker 0/1)."""
    d = pd.DataFrame({
        "aggTradeId": np.arange(len(rows), dtype=np.int64),
        "price": [r[1] for r in rows], "qty": [r[2] for r in rows],
        "firstTradeId": np.arange(len(rows), dtype=np.int64),
        "lastTradeId": np.arange(len(rows), dtype=np.int64),
        "timestamp": [r[0] for r in rows], "isBuyerMaker": [r[3] for r in rows],
    })
    d["d"] = np.where(d.isBuyerMaker == 0, 1.0, -1.0)
    d["t_s"] = d.timestamp // 1000
    return d


def csv_zip(df, name, header=True):
    buf = io.StringIO(); df.to_csv(buf, index=False, header=header)
    z = io.BytesIO()
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, buf.getvalue())
    return z.getvalue()


# ------------------------------------------------------------------ 1 readers
BOOK_RAW = [(0, 100.0, 10.0, 102.0, 10.0, 0, 0),
            (1000, 100.0, 10.0, 100.5, 10.0, 1000, 1000)]
TR_RAW = [(0, 101.0, 3.0, 0, 0, 500, 0),
          (1, 101.0, 4.0, 1, 1, 1500, 1)]
bcols = ["update_id", "best_bid_price", "best_bid_qty", "best_ask_price", "best_ask_qty",
         "transaction_time", "event_time"]
tcols = ["aggTradeId", "price", "qty", "firstTradeId", "lastTradeId", "timestamp", "isBuyerMaker"]

bt_csv = pd.DataFrame(BOOK_RAW, columns=bcols)
tr_csv = pd.DataFrame(TR_RAW, columns=tcols)
bt_zh = csv_zip(bt_csv, "b.csv", header=True)
bt_zn = csv_zip(bt_csv, "b.csv", header=False)
tr_zh = csv_zip(tr_csv, "t.csv", header=True)
tr_zn = csv_zip(tr_csv, "t.csv", header=False)

b1 = M.read_bookticker_zip(bt_zh); b2 = M.read_bookticker_zip(bt_zn)
check("reader: bookTicker with and without a header row parse identically",
      b1.equals(b2) and len(b1) == 2 and list(b1.columns[:7]) == bcols)
check("reader: bookTicker adds mid / spread / rel_spread_bp with hand values",
      np.allclose(b1["mid"], [101.0, 100.25]) and np.allclose(b1["spread"], [2.0, 0.5])
      and np.allclose(b1["rel_spread_bp"], [2.0 / 101.0 * 1e4, 0.5 / 100.25 * 1e4]))
t1 = M.read_aggtrades_zip(tr_zh); t2 = M.read_aggtrades_zip(tr_zn)
check("reader: aggTrades sets d = +1 iff isBuyerMaker == 0 and t_s = timestamp // 1000",
      t1.equals(t2) and t1["d"].tolist() == [1.0, -1.0] and t1["t_s"].tolist() == [0, 1])
check("reader: aggTrades preserves price / qty and both header layouts agree",
      t1["price"].tolist() == [101.0, 101.0] and t1["qty"].tolist() == [3.0, 4.0])
# regression (real archive layout, 2023+): snake_case header + 'true'/'false' flags
tr_snake = pd.DataFrame(
    [[0, 101.0, 3.0, 0, 0, 500, "true"], [1, 101.0, 4.0, 1, 1, 1500, "false"]],
    columns=["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
             "transact_time", "is_buyer_maker"])
ts = M.read_aggtrades_zip(csv_zip(tr_snake, "t.csv", header=True))
check("reader: aggTrades real snake_case header + 'true'/'false' flags normalise correctly",
      ts["aggTradeId"].tolist() == [0, 1] and ts["qty"].tolist() == [3.0, 4.0]
      and ts["timestamp"].tolist() == [500, 1500] and ts["d"].tolist() == [-1.0, 1.0])
# path-based read
tmp = tempfile.mkdtemp(prefix="mmalt_rd_")
zp = os.path.join(tmp, "b.zip")
open(zp, "wb").write(bt_zh)
check("reader: read_bookticker_zip accepts a filesystem path as well as bytes",
      M.read_bookticker_zip(zp).equals(b1))

# ------------------------------------------------------------------ 2 formulas (hand-built)
# Book: 4 updates.  Trades: one buyer-aggressor fill at the ask of the strictly-prior book.
rows = [(0, 100.0, 10, 102.0, 10),
        (1000, 100.0, 10, 100.5, 10),
        (6000, 101.0, 10, 103.0, 10),
        (31000, 99.0, 10, 101.0, 10)]
bt = mk_bt(rows)
tr = mk_tr([(500, 102.0, 10, 0)])          # t=500 ms, p=102 = ask of row 0, buyer aggressor
# strictly-prior book for the trade = row 0: mid 101, bid 100, ask 102
# m_fwd(1 s) = last mid <= 1500 ms = row 1 mid 100.25
# e  = +1 * (102 - 101) / 101 * 1e4
# r  = +1 * (102 - 100.25) / 101 * 1e4
# im = +1 * (100.25 - 101) / 101 * 1e4
E_EXP, R_EXP, I_EXP = 1.0 / 101 * 1e4, 1.75 / 101 * 1e4, -0.75 / 101 * 1e4

qs = M.quoted_spread_stats(bt)
check("quoted spread: median / mean / deciles of rel_spread_bp match hand values",
      np.allclose([qs["median_bp"], qs["mean_bp"]],
                  [np.median([2 / 101 * 1e4, 0.5 / 100.25 * 1e4, 2 / 102 * 1e4, 2 / 100 * 1e4]),
                   np.mean([2 / 101 * 1e4, 0.5 / 100.25 * 1e4, 2 / 102 * 1e4, 2 / 100 * 1e4])])
      and 0 < qs["d10_bp"] < qs["median_bp"] < qs["d90_bp"])

h = M.huang_stoll(bt, tr, 1)
check("huang_stoll: e / r / impact equal the hand-computed bp values",
      np.allclose([h["e"][0], h["r"][0], h["impact"][0]], [E_EXP, R_EXP, I_EXP]),
      f"{h['e'][0]:.4f} {h['r'][0]:.4f} {h['impact'][0]:.4f}")
check("huang_stoll: identity e == r + impact holds exactly (bit-tight to 1e-9 bp) and is flagged",
      h["identity"] and np.allclose(h["e"], h["r"] + h["impact"], rtol=0, atol=1e-9))
check("huang_stoll: impact is the independently computed d(m_tau-m)/m*1e4, not e-r",
      np.isclose(h["impact"][0], (100.25 - 101.0) / 101.0 * 1e4))
check("huang_stoll: qty-VW means equal the single-trade values here",
      np.allclose([h["e_vw"], h["r_vw"], h["impact_vw"]], [E_EXP, R_EXP, I_EXP]))

mk = M.maker_markout(bt, tr, (1, 5))
# s = -d = -1 (maker sold); f = ask of the strictly-prior book = 102; m0 = 101
# capture = -1 * (101 - 102) / 101 * 1e4 = +1/101*1e4
# adverse(1s) = -1 * (100.25 - 101) / 101 * 1e4 = +0.75/101*1e4
# net(1s) = s*(m_tau - f)/m0*1e4 = -1 * (100.25 - 102)/101*1e4 = +1.75/101*1e4
check("maker_markout: capture / adverse / net equal the hand-computed bp values",
      np.allclose([mk["capture"][0], mk["adverse"]["1"][0], mk["net"]["1"][0]],
                  [1.0 / 101 * 1e4, 0.75 / 101 * 1e4, 1.75 / 101 * 1e4]))
check("maker_markout: identity capture + adverse == net holds exactly and is flagged",
      mk["identity"] and np.allclose(mk["capture"] + mk["adverse"]["1"], mk["net"]["1"], rtol=0, atol=1e-9))
check("maker_markout: net_after_fee = net - (MAKER_BP - REBATE_BP)",
      np.isclose(mk["net_after_fee_vw"]["1"], mk["net_vw"]["1"] - (M.MAKER_BP - M.REBATE_BP))
      and np.isclose(mk["net_after_fee_vw"]["1"], 1.75 / 101 * 1e4 - 1.9))
check("maker_markout: a maker fill at the touch has capture == the Huang-Stoll effective spread",
      np.isclose(mk["capture"][0], h["e"][0]))

# second trade: seller aggressor -> maker buys at the strictly-prior bid
tr2 = mk_tr([(500, 102.0, 10, 0), (1500, 100.0, 5, 1)])
mk2 = M.maker_markout(bt, tr2, (1,))
# trade 2 at t=1500: strictly-prior book = row 1 (bid 100, ask 100.5, mid 100.25); d=-1 -> s=+1
# f = bid = 100; capture = +1*(100.25-100)/100.25*1e4 = 0.25/100.25*1e4
check("maker_markout: seller-aggressor trade fills at the bid and captures the bid-side half-spread",
      np.isclose(mk2["capture"][1], 0.25 / 100.25 * 1e4),
      f"{mk2['capture'][1]:.4f}")
h2 = M.huang_stoll(bt, tr2, 1)
check("huang_stoll: seller-aggressor e = d(p-m)/m*1e4 = -1*(100-100.25)/100.25*1e4",
      np.isclose(h2["e"][1], -1.0 * (100.0 - 100.25) / 100.25 * 1e4), f"{h2['e'][1]:.4f}")

# ------------------------------------------------------------------ 3 OFI
ofi_book = mk_bt([(0, 100.0, 10, 102.0, 10),
                  (1000, 100.0, 12, 102.0, 10),      # bid qty up: e = 12 - 10 - 10 + 10 = +2
                  (2000, 100.5, 5, 102.0, 10),       # bid up:      e = 5 - 0 - 10 + 10 = +5
                  (3000, 100.5, 5, 101.0, 20),       # ask down:    e = 5 - 5 - 20 + 0 = -20
                  (4000, 100.2, 5, 101.0, 20)])      # bid down:    e = 0 - 5 - 20 + 20 = -5
e = M.ofi_l1(ofi_book)
check("ofi_l1: Cont-Kukanov-Stoikov L1 on a scripted book gives the known answer",
      np.allclose(e.to_numpy(), [0.0, 2.0, 5.0, -20.0, -5.0]), str(e.tolist()))
check("ofi_l1: length equals the book, first update contributes 0",
      len(e) == len(ofi_book) and e.iloc[0] == 0.0)

# planted OFI -> mid: a quote-level walk with constant quantities drives both OFI and dmid
def ofi_world(seed, n=400, beta_bp=0.0, noise_bp=0.0, step_ms=1000):
    """One book update per second. Bid/ask level walk; OFI = +-20 by construction."""
    rng = np.random.default_rng(seed)
    t0 = int(pd.Timestamp("2023-07-19", tz="UTC").timestamp()) * 1000
    lvl = 100.0
    rows = [(t0, lvl - 0.1, 10.0, lvl + 0.1, 10.0)]
    for i in range(1, n):
        shock = rng.standard_normal()
        sgn = 1.0 if shock > 0 else -1.0
        lvl = lvl * (1.0 + beta_bp * 1e-4 * sgn / 2.0 + noise_bp * 1e-4 * rng.standard_normal())
        rows.append((t0 + i * step_ms, lvl - 0.1, 10.0, lvl + 0.1, 10.0))
    return mk_bt(rows)

wb = ofi_world(1, n=600, beta_bp=8.0, noise_bp=0.0)
res_on = M.ofi_predictive(wb, step_s=1.0)
check("ofi_predictive: planted OFI->mid relation gives R2 > 0.9 (clean plant)",
      np.isfinite(res_on["r2"]) and res_on["r2"] > 0.9, f"r2={res_on['r2']:.3f} beta={res_on['beta']:.3f}")
# shuffled-mid control: same book, mid permuted across updates -> the relation is destroyed
wb_s = wb.copy()
wb_s["mid"] = wb_s["mid"].to_numpy()[np.random.default_rng(2).permutation(len(wb_s))]
res_sh = M.ofi_predictive(wb_s, step_s=1.0)
check("ofi_predictive: shuffled-mid control gives R2 ~ 0 (< 0.05)",
      np.isfinite(res_sh["r2"]) and res_sh["r2"] < 0.05, f"r2={res_sh['r2']:.4f}")
check("ofi_predictive: the plant is not just a level effect (control R2 << plant R2)",
      res_sh["r2"] < 0.2 * max(res_on["r2"], 1e-9))

# ------------------------------------------------------------------ 4 leak / look-ahead
def leak_world(seed=3, n_book=400, n_tr=200, t_end_s=400_000):
    rng = np.random.default_rng(seed)
    t0 = int(pd.Timestamp("2023-07-19", tz="UTC").timestamp()) * 1000
    ts = np.sort(rng.choice(np.arange(0, t_end_s * 1000, 200), n_book, replace=False))
    lvl = 100 * np.exp(np.cumsum(rng.standard_normal(n_book)) * 1e-4)
    half = 0.05
    bb, aa = lvl - half, lvl + half
    bt_ = mk_bt([(int(t), float(b), 10.0, float(a), 10.0) for t, b, a in zip(ts, bb, aa)])
    tts = np.sort(rng.choice(np.arange(1000, t_end_s * 1000 - 1000, 100), n_tr, replace=False))
    d = np.where(rng.random(n_tr) > 0.5, 1.0, -1.0)
    px = np.where(d > 0, aa[np.searchsorted(ts, tts) - 1], bb[np.searchsorted(ts, tts) - 1])
    tr_ = mk_tr([(int(t), float(p), 1.0, 0 if dd > 0 else 1) for t, p, dd in zip(tts, px, d)])
    return bt_, tr_

btL, trL = leak_world()
T = 200_000 * 1000                                     # ms cut
# scramble everything after T: new prices/qtys and a shuffled order
rng = np.random.default_rng(99)
bt2 = btL.copy()
aft = bt2["transaction_time"].to_numpy() > T
idx = np.flatnonzero(aft)
bt2.loc[idx, "best_bid_price"] = 50 + 40 * rng.random(len(idx))
bt2.loc[idx, "best_ask_price"] = bt2.loc[idx, "best_bid_price"] + 0.5 + rng.random(len(idx))
bt2.loc[idx, "best_bid_qty"] = 1 + 9 * rng.random(len(idx))
bt2.loc[idx, "best_ask_qty"] = 1 + 9 * rng.random(len(idx))
# transaction_time of the scrambled rows is untouched, so causal lookups cannot reach them
bt2 = bt2.sample(frac=1.0, random_state=7).sort_values(["transaction_time", "update_id"], kind="stable").reset_index(drop=True)
bt2["mid"] = (bt2.best_bid_price + bt2.best_ask_price) / 2.0
bt2["spread"] = bt2.best_ask_price - bt2.best_bid_price
bt2["rel_spread_bp"] = bt2.spread / bt2.mid * 1e4
tr2_ = trL.copy()
af = tr2_["timestamp"].to_numpy() > T
j = np.flatnonzero(af)
tr2_.loc[j, "price"] = 1 + 500 * rng.random(len(j))
tr2_.loc[j, "qty"] = 1 + 10 * rng.random(len(j))
tr2_ = tr2_.sample(frac=1.0, random_state=11).sort_values(["timestamp", "aggTradeId"], kind="stable").reset_index(drop=True)

tau = 5
hA = M.huang_stoll(btL, trL, tau)
hB = M.huang_stoll(bt2, tr2_, tau)
causal = trL["timestamp"].to_numpy() + tau * 1000 <= T
fut = ~causal
check("leak: e / r / impact over trades with t+tau <= T are bit-identical after scrambling the future",
      bool(np.array_equal(hA["e"][causal], hB["e"][causal], equal_nan=True)
           and np.array_equal(hA["r"][causal], hB["r"][causal], equal_nan=True)
           and np.array_equal(hA["impact"][causal], hB["impact"][causal], equal_nan=True)),
      f"{int(causal.sum())} causal trades")
mA = M.maker_markout(btL, trL, (tau,))
mB = M.maker_markout(bt2, tr2_, (tau,))
check("leak: maker capture / adverse / net over t+tau <= T are bit-identical",
      bool(np.array_equal(mA["capture"][causal], mB["capture"][causal], equal_nan=True)
           and np.array_equal(mA["adverse"][str(tau)][causal], mB["adverse"][str(tau)][causal], equal_nan=True)
           and np.array_equal(mA["net"][str(tau)][causal], mB["net"][str(tau)][causal], equal_nan=True)))
preA = M.quoted_spread_stats(btL[btL["transaction_time"] <= T])
preB = M.quoted_spread_stats(bt2[bt2["transaction_time"] <= T])
check("leak: quoted-spread stats over book rows <= T unchanged", preA == preB)
check("leak: the test can see a difference after T (r and e on future trades move)",
      (not np.allclose(hA["r"][fut], hB["r"][fut], equal_nan=True))
      and (not np.allclose(hA["e"][fut], hB["e"][fut], equal_nan=True)))
check("leak: strictly-prior mid forbids an exact-timestamp match (a trade on a quote does not see it)",
      bool(np.all(np.isnan(M._prior_mid(mk_bt([(0, 100.0, 1, 101.0, 1)]), np.array([0]))))))

# ------------------------------------------------------------------ 5 coverage
bt_full = mk_bt([(int(t), 100.0, 1, 100.2, 1) for t in np.linspace(0, 86_400_000, 1000)])
check("coverage: a full-day span of transaction_time gives coverage_fraction = 1",
      np.isclose(M.coverage_fraction(bt_full), 1.0), f"{M.coverage_fraction(bt_full):.6f}")
bt_part = mk_bt([(i * 1000, 100.0, 1, 100.2, 1) for i in range(36000)])     # 10 h of a 24 h day
cov = M.coverage_fraction(bt_part)
check("coverage: a partial-day file (10 h span) returns coverage_fraction < 1 and ~ 10/24",
      0.0 < cov < 1.0 and abs(cov - 36000 / 86400) < 0.02, f"{cov:.4f}")

# ------------------------------------------------------------------ 6/7 worlds -> pool()
DAY0 = int(pd.Timestamp("2023-07-19", tz="UTC").timestamp()) * 1000


def world_symbol_day(seed, day, cohort, half_bp, n_tr=1500, iid=False, tick_ms=200):
    """One synthetic symbol-day: constant half-spread, trades at the touch, random aggressor sign.

    half_bp  the half-spread in bp (capture is exactly half_bp for every fill);
    iid      iid mid (null) vs a slow random walk (planted / realistic).
    """
    rng = np.random.default_rng(seed)
    n_book = n_tr + 2
    ts = DAY0 + np.arange(n_book) * tick_ms
    if iid:
        lvl = 100 * np.exp(rng.standard_normal(n_book) * 2e-3)
    else:
        lvl = 100 * np.exp(np.cumsum(rng.standard_normal(n_book)) * 2e-5)
    half = half_bp * 1e-4 * lvl
    bt_ = mk_bt([(int(t), float(l - h), 10.0, float(l + h), 10.0) for t, l, h in zip(ts, lvl, half)])
    tts = ts[:-1] + tick_ms // 2                                  # strictly between book updates
    d = np.where(rng.random(len(tts)) > 0.5, 1.0, -1.0)
    px = np.where(d > 0, (lvl + half)[:-1], (lvl - half)[:-1])     # at the touch of the prior book
    tr_ = mk_tr([(int(t), float(p), 1.0, 0 if dd > 0 else 1) for t, p, dd in zip(tts, px, d)])
    return M.analyse_day(bt_, tr_, M.TAUS, symbol=f"S{seed}", day=day, cohort=cohort)


def build_rows(kind, seed0=0):
    rows = []
    for k, day in enumerate(M.SAMPLE_DAYS):
        for c in range(3):                                        # 3 band symbols + 1 comparator
            if kind == "planted":
                rows.append(world_symbol_day(seed0 + 100 * k + c, day, "band", half_bp=10.0, iid=False))
            else:
                rows.append(world_symbol_day(seed0 + 100 * k + c, day, "band", half_bp=1.5, iid=True))
        rows.append(world_symbol_day(seed0 + 100 * k + 50, day, "comparator",
                                     half_bp=(0.5 if kind == "planted" else 1.5), iid=True))
    return rows


planted = build_rows("planted")
pp = M.pool(planted)
check("planted world: pool() PASSES every pre-registered criterion",
      pp["pass_"] and all(c["ok"] for c in pp["criteria"].values()),
      "; ".join(f"{k}={v['ok']}" for k, v in pp["criteria"].items()))
check("planted world: band net_after_fee(5s) is comfortably positive and beats the comparator",
      pp["metrics"]["band_net_after_fee_5s_mean"] > 3.0
      and pp["metrics"]["band_net_after_fee_5s_mean"] > pp["metrics"]["comparator_net_after_fee_5s_mean"],
      f"band {pp['metrics']['band_net_after_fee_5s_mean']:+.2f} vs "
      f"comp {pp['metrics']['comparator_net_after_fee_5s_mean']:+.2f}")
check("planted world: the day-clustered bootstrap CI95 lower bound is > 0 and inside the sample",
      pp["metrics"]["band_net_after_fee_5s_ci95"][0] > 0
      and pp["metrics"]["band_days_positive"] == 4,
      str(np.round(pp["metrics"]["band_net_after_fee_5s_ci95"], 3).tolist()))
check("planted world: median quoted spread > 2 x maker fee",
      pp["criteria"]["median_spread_gt_2x_fee"]["ok"], pp["criteria"]["median_spread_gt_2x_fee"]["detail"])

npass, ndet = 0, []
for sd in range(10):
    rows = build_rows("null", seed0=1000 + 10 * sd)
    q = M.pool(rows)
    npass += int(q["pass_"])
    ndet.append((round(q["metrics"]["band_net_after_fee_5s_mean"], 3),
                 q["criteria"]["median_spread_gt_2x_fee"]["ok"]))
check("null: 10 iid-mid worlds with fee >= half-spread pass at most 1", npass <= 1,
      f"passes={npass} (band net5 means {ndet})")

# ------------------------------------------------------------------ 8 end to end
synth_dir = tempfile.mkdtemp(prefix="mm_alt_derived_")
n_written = 0
for r in planted:
    with open(os.path.join(synth_dir, f"{r['symbol']}-{r['day']}.json"), "w") as fh:
        json.dump(r, fh, indent=1, default=float)
    n_written += 1
rows_disk = R.load_rows(synth_dir)
check("end-to-end: every synthetic derived row round-trips through load_rows",
      len(rows_disk) == n_written and rows_disk[0]["cohort"] in ("band", "comparator"))
res = R.run(rows_disk, quiet=True)
out_json = os.path.join(synth_dir, "m1_results.json")
json.dump(res, open(out_json, "w"), indent=1, default=float)
buf = io.StringIO(); _stdout, sys.stdout = sys.stdout, buf
try:
    R.report(res)
finally:
    sys.stdout = _stdout
txt = buf.getvalue()
check("end-to-end: run_m1 writes the results JSON with a verdict string",
      os.path.exists(out_json) and json.load(open(out_json))["verdict"] in ("PASS", "FAIL")
      and "VERDICT:" in txt and res["verdict"] == "PASS", f"verdict={res['verdict']}")
check("end-to-end: the report prints one [x]/[ ] line per criterion",
      txt.count("] ") >= 5 and txt.count("net5_mean_pos_and_ci_pos") == 1)
empty = R.run(R.load_rows(tempfile.mkdtemp(prefix="mm_alt_empty_")), quiet=True)
check("end-to-end: zero real data reports 'no data' and a FAIL verdict",
      empty["verdict"] == "FAIL" and empty.get("note") == "no data"
      and all(not c["ok"] for c in empty["criteria"].values()))
buf2 = io.StringIO(); sys.stdout = buf2
try:
    R.report(empty)
finally:
    sys.stdout = _stdout
check("end-to-end: the no-data report still prints the VERDICT line", "VERDICT: FAIL" in buf2.getvalue())

# ------------------------------------------------------------------ 9 fetcher (mocked get)
def fake_zip_book(n=1440, t0_ms=DAY0, step_ms=60_000, half=0.2):
    rows = []
    for i in range(n):
        rows.append((t0_ms + i * step_ms, 100.0 - half, 5.0, 100.0 + half, 5.0,
                     t0_ms + i * step_ms, t0_ms + i * step_ms))
    return csv_zip(pd.DataFrame(rows, columns=bcols), "b.csv", header=True)


def fake_zip_trades(n=1439, t0_ms=DAY0, step_ms=60_000):
    rows = []
    for i in range(n):
        at_ask = i % 2 == 0                                    # isBuyerMaker 0 -> buyer hits the ask
        rows.append((i, 100.2 if at_ask else 99.8, 1.0, i, i,
                     t0_ms + i * step_ms + 500, 0 if at_ask else 1))
    return csv_zip(pd.DataFrame(rows, columns=tcols), "t.csv", header=True)


calls = []
BUCKET = {"bookTicker": fake_zip_book(), "aggTrades": fake_zip_trades()}


def fake_get(url, tries=4):
    calls.append(url)
    for k, v in BUCKET.items():
        if f"-{k}-" in url:
            return v
    return None


F.get = fake_get
blob = F.fetch_symbol_day("TESTUSDT", "2023-07-19", "bookTicker")
check("fetch: fetch_symbol_day calls get() with the documented archive URL and returns the bytes",
      isinstance(blob, (bytes, bytearray))
      and calls[-1] == ("https://data.binance.vision/data/futures/um/daily/bookTicker/TESTUSDT/"
                        "TESTUSDT-bookTicker-2023-07-19.zip"), calls[-1])
F.get = lambda url, tries=4: None
check("fetch: fetch_symbol_day returns None when the archive has no object (404)",
      F.fetch_symbol_day("NOPEUSDT", "2023-07-19", "bookTicker") is None)
F.get = fake_get

out_dir = tempfile.mkdtemp(prefix="mm_alt_fetch_")
st, row = F.process_symbol_day("TESTUSDT", "2023-07-19", out_dir, cohort="band", keep=False)
dpath = os.path.join(out_dir, "TESTUSDT-2023-07-19.json")
check("fetch: process_symbol_day streams one symbol-day into a derived JSON row",
      st == "ok" and os.path.exists(dpath) and row is not None and row["symbol"] == "TESTUSDT",
      f"status={st}")
disk_row = json.load(open(dpath))
check("fetch: the derived row carries the mmalt schema (mk.net_after_fee_bp, coverage, cohort)",
      "net_after_fee_bp" in disk_row["mk"] and "coverage" in disk_row and disk_row["cohort"] == "band")
check("fetch: streaming discards the zips (no _zips dir when keep=False)",
      not os.path.isdir(os.path.join(out_dir, "_zips")))
st2, _ = F.process_symbol_day("TESTUSDT", "2023-07-19", out_dir, cohort="band", keep=False)
check("fetch: resumable — a symbol-day whose derived row exists is skipped",
      st2 == "derived", f"status={st2}")
F.get = lambda url, tries=4: None
st3, _ = F.process_symbol_day("GHOSTUSDT", "2023-07-19", out_dir, cohort="band", keep=False)
check("fetch: a missing archive object writes a .missing sentinel and is skipped next time",
      st3 == "missing" and os.path.exists(os.path.join(out_dir, "GHOSTUSDT-2023-07-19.missing")))
st4, _ = F.process_symbol_day("GHOSTUSDT", "2023-07-19", out_dir, cohort="band", keep=False)
check("fetch: the .missing sentinel short-circuits the next run (no second download attempt)",
      st4 == "missing", f"status={st4}")
F.get = fake_get
kept_dir = tempfile.mkdtemp(prefix="mm_alt_keep_")
F.process_symbol_day("TESTUSDT", "2023-07-19", kept_dir, cohort="band", keep=True,
                     tmpdir=os.path.join(kept_dir, "_zips"))
check("fetch: --keep retains both zips next to the derived row",
      len(os.listdir(os.path.join(kept_dir, "_zips"))) == 2)

# fetch_mm.analyse_symbol_day parses both mocked objects into one row
row2 = F.analyse_symbol_day("TESTUSDT", "2023-07-19", BUCKET["bookTicker"], BUCKET["aggTrades"], "band")
check("fetch: analyse_symbol_day yields a finite net_after_fee(5s) and a full coverage span",
      np.isfinite(row2["mk"]["net_after_fee_bp"]["5"]) and row2["coverage"] > 0.5,
      f"net5={row2['mk']['net_after_fee_bp']['5']:+.3f} cov={row2['coverage']:.3f}")

# ------------------------------------------------------------------ pool verdict logic on hand rows
def fake_row(day, cohort, net5, spread):
    return dict(symbol="X", day=day, cohort=cohort, n_bt=1, n_tr=1, coverage=1.0,
                qs_median_bp=spread, qs_mean_bp=spread, qs_d10_bp=spread, qs_d90_bp=spread,
                hs=dict(e_bp=0.0, r_bp={}, impact_bp={}, identity=True),
                mk=dict(capture_bp=0.0, adverse_bp={}, net_bp={"5": net5 + 1.9},
                        net_after_fee_bp={"5": net5}, identity=True),
                ofi=dict(r2=0.0, beta=0.0, n=1))


good = [fake_row(d, "band", 8.0, 20.0) for d in M.SAMPLE_DAYS] + \
       [fake_row(d, "comparator", -1.0, 2.0) for d in M.SAMPLE_DAYS]
gp = M.pool(good)
check("pool: hand-made rows with a positive band edge pass all five criteria",
      gp["pass_"] and all(c["ok"] for c in gp["criteria"].values()))
bad_sign = [fake_row(d, "band", (8.0 if i < 2 else -8.0), 20.0) for i, d in enumerate(M.SAMPLE_DAYS)] + \
           [fake_row(d, "comparator", -1.0, 2.0) for d in M.SAMPLE_DAYS]
bs = M.pool(bad_sign)
check("pool: only 2 of 4 positive days fails the days_pos_ge_3 criterion",
      not bs["criteria"]["days_pos_ge_3"]["ok"] and not bs["pass_"],
      bs["criteria"]["days_pos_ge_3"]["detail"])
narrow = [fake_row(d, "band", 8.0, 3.0) for d in M.SAMPLE_DAYS] + \
         [fake_row(d, "comparator", -1.0, 2.0) for d in M.SAMPLE_DAYS]
np_ = M.pool(narrow)
check("pool: a median quoted spread of 3 bp fails median_spread_gt_2x_fee (needs > 4 bp)",
      not np_["criteria"]["median_spread_gt_2x_fee"]["ok"],
      np_["criteria"]["median_spread_gt_2x_fee"]["detail"])
worst = [fake_row(d, "band", 1.0, 20.0) for d in M.SAMPLE_DAYS] + \
        [fake_row(d, "comparator", 5.0, 2.0) for d in M.SAMPLE_DAYS]
wp = M.pool(worst)
check("pool: band below the comparator fails band_gt_comparator",
      not wp["criteria"]["band_gt_comparator"]["ok"], wp["criteria"]["band_gt_comparator"]["detail"])
boot = M.day_clustered_bootstrap([8.0, 8.0, 8.0, 8.0], M.SAMPLE_DAYS, n_boot=500, seed=1)
check("day_clustered_bootstrap: four identical positive day-means give a CI entirely above 0",
      boot[0] > 0 and boot[1] > 0, str(np.round(boot, 3).tolist()))
boot2 = M.day_clustered_bootstrap([10.0, -10.0, 10.0, -10.0], M.SAMPLE_DAYS, n_boot=500, seed=1)
check("day_clustered_bootstrap: mixed-sign day-means give a CI spanning 0 (resamples whole days)",
      boot2[0] < 0 < boot2[1], str(np.round(boot2, 3).tolist()))

# universe injection
u = M.select_universe("2023-07-19", universe={"band": ["AUSDT", "BUSDT"], "comparator": ["BTCUSDT"]})
check("select_universe: an injected synthetic universe is returned untouched",
      u == (["AUSDT", "BUSDT"], ["BTCUSDT"]))


# as-of lookups: the binary-search version equals the original pandas.merge_asof implementation
def _asof_ref(bt, ts, cols, exact):
    key = pd.DataFrame({"t": np.asarray(ts, np.int64)})
    ref = bt[["transaction_time"] + cols].rename(columns={"transaction_time": "t"})
    j = pd.merge_asof(key.sort_values("t", kind="stable"), ref.sort_values("t", kind="stable"),
                      on="t", direction="backward", allow_exact_matches=exact)
    return [j[c].to_numpy(float) for c in cols]


rng_a = np.random.default_rng(7)
tt = np.sort(rng_a.integers(1000, 60_000, 5000))                           # many duplicate milliseconds
bid = 100 + np.cumsum(rng_a.normal(0, 0.01, len(tt))); ask = bid + rng_a.choice([0.01, 0.02], len(tt))
bt_a = pd.DataFrame({"transaction_time": tt, "best_bid_price": bid, "best_ask_price": ask, "mid": (bid + ask) / 2})
ts_a = np.sort(np.r_[rng_a.integers(0, 61_000, 3000), tt[::50]])           # includes exact matches and times before the book
same = (np.array_equal(M._prior_mid(bt_a, ts_a), _asof_ref(bt_a, ts_a, ["mid"], False)[0], equal_nan=True)
        and np.array_equal(M._mid_at(bt_a, ts_a), _asof_ref(bt_a, ts_a, ["mid"], True)[0], equal_nan=True)
        and np.array_equal(M._mid_at(bt_a, ts_a + 5000), _asof_ref(bt_a, ts_a + 5000, ["mid"], True)[0], equal_nan=True))
pb, (rb, ra) = M._prior_book(bt_a, ts_a), _asof_ref(bt_a, ts_a, ["best_bid_price", "best_ask_price"], False)
check("as-of lookups: binary search == merge_asof (strict / inclusive / forward, duplicate ms, exact matches, before-book NaN)",
      same and np.array_equal(pb["bid"], rb, equal_nan=True) and np.array_equal(pb["ask"], ra, equal_nan=True))

print(f"\n{fails} failures")
sys.exit(fails > 0)
