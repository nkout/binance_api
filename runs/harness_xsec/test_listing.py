"""Tests for the N5 listing-drift harness — real library functions on synthetic data.

  1. classification: pre-2020, TradFi, index, new (no spot / spot <= 3 months earlier), seasoned, a re-listed multiplier base
  2. entry instant and ISO-week clustering
  3. P&L identity by hand: price step, value-weighted funding (settlement at entry excluded, at exit included), BTC leg, 18 bp
  4. delisting: zero-volume flat bars after the last trade -> both legs closed at the last traded instant
  5. look-ahead: prices / volumes after entry leave selection and entry unchanged; prices after exit leave the event P&L unchanged
  6. portfolio: constant-notional daily marks sum to the hand value; the open cap skips a later event
  7. bootstrap resamples whole weeks; verdict logic on hand-made inputs
  8. worlds on the on-disk format: planted new-token drift passes; 10 null worlds pass at most once; an alts-vs-BTC drift with no
     listing effect fails the control criterion; end-to-end report and forward block
"""
import os, sys, tempfile, io, contextlib
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import xsec as X
import listing as N
import run_n5 as R
fails = 0
DAY, HOUR = 86400, 3600


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


def ts(s):
    return int(pd.Timestamp(s, tz="UTC").timestamp())


# ------------------------------------------------------------------ 1 classification
meta = dict(spot={"NEWA": None, "NEWB": "2021-03", "OLDC": "2021-02", "SEAS": "2019-05", "TRAD": None, "IDX": None,
                  "1000XYZUSDT": None, "XYZUSDT": None},
            ei={"TRAD": {"underlyingSubType": ["TradFi"], "contractType": "TRADIFI_PERPETUAL", "underlyingType": "EQUITY"},
                "IDX": {"underlyingType": "INDEX", "underlyingSubType": ["Index"]},
                "NEWA": {"underlyingType": "COIN", "underlyingSubType": ["Meme", "Crypto"]}})
L = ts("2021-06-10 05:00")
check("classify: no spot pair -> new; spot 3 months before -> new; 4 months before -> seasoned; pre-2020; TradFi; index",
      N.classify("NEWA", L, meta) == "new" and N.classify("NEWB", L, meta) == "new" and N.classify("OLDC", L, meta) == "seasoned"
      and N.classify("SEAS", ts("2020-01-20"), meta) == "pre2020" and N.classify("TRAD", L, meta) == "tradfi"
      and N.classify("IDX", L, meta) == "index")
check("subtype: first non-Crypto tag; delisted when not in exchangeInfo", N.subtype("NEWA", meta) == "Meme" and N.subtype("NEWB", meta) == "delisted")
g = np.arange(ts("2021-01-01"), ts("2021-03-01"), HOUR)
c = np.full((len(g), 2), np.nan); c[100:, 0] = 1.0; c[900:, 1] = 2.0
tab = N.listing_table(dict(grid=g, syms=["1000XYZUSDT", "XYZUSDT"], close=c), meta)
check("listing table: L = first bar with a close; the later symbol with the same base is 'relisted'",
      tab.L.tolist() == [g[100], g[900]] and tab.cls.tolist() == ["new", "relisted"])

# ------------------------------------------------------------------ 2 entry and weeks
check("entry: first 00:00 >= L + 48 h (05:00 -> two days later rounded up; an exact 00:00 stays)",
      N.entry_instant(ts("2021-01-03 05:00"), 48) == ts("2021-01-06") and N.entry_instant(ts("2021-01-03"), 48) == ts("2021-01-05")
      and N.entry_instant(ts("2021-01-03 05:00"), 24) == ts("2021-01-05"))
check("iso_week: 2021-01-03 (Sunday) is in 2020-W53, 2021-01-04 in 2021-W01", N.iso_week([ts("2021-01-03"), ts("2021-01-04")]).tolist() == [202053, 202101])


# ------------------------------------------------------------------ hand world
def hand_world():
    """BTC 100 -> 110 from 01-11; NEW listed 01-03 05:00, 10 -> 8 from 01-08; DIE listed 01-02, 20 -> 15, last trade closes 01-09 12:00,
    then zero-volume bars printed at 1.0 (a wrong price that must never be used)."""
    g = np.arange(ts("2021-01-01"), ts("2021-02-10"), HOUR); T = len(g)
    close_t = g + HOUR                                                   # close instant of each bar
    btc = np.where(close_t >= ts("2021-01-11"), 110.0, 100.0)
    new = np.where(close_t >= ts("2021-01-08"), 8.0, 10.0); new[g < ts("2021-01-03 05:00")] = np.nan
    die = np.where(close_t >= ts("2021-01-08"), 15.0, 20.0); die[g < ts("2021-01-02")] = np.nan
    qv = np.full((T, 3), 1e6, np.float32)
    dead = close_t > ts("2021-01-09 12:00")
    die[dead] = 1.0; qv[dead, 2] = 0.0
    fund = {"BTCUSDT": (np.array([ts("2021-01-12")]), np.array([0.001])),
            "NEWUSDT": (np.array([ts("2021-01-06"), ts("2021-01-07"), ts("2021-01-09"), ts("2021-01-20"), ts("2021-01-21")]),
                        np.array([0.5, -0.01, 0.02, 0.001, 0.5])),
            "DIEUSDT": (np.zeros(0, np.int64), np.zeros(0))}
    return dict(grid=g, syms=["BTCUSDT", "NEWUSDT", "DIEUSDT"], close=np.c_[btc, new, die], qv=qv, fund=fund)


hmeta = dict(spot={"BTCUSDT": "2017-08", "NEWUSDT": None, "DIEUSDT": None}, ei={})
p = hand_world(); pp = N.prep(p)
htab = N.listing_table(p, hmeta)
ev = N.build_events(pp, htab, 0, 14)
e = ev.set_index("sym")
# ------------------------------------------------------------------ 3 P&L identity
short = -(8 / 10 - 1) + (-0.01 * 10 / 10 + 0.02 * 8 / 10 + 0.001 * 8 / 10)
btc = (110 / 100 - 1) - 0.001 * 110 / 100
check("P&L by hand: short leg = 0.2068 (price + value-weighted funding; entry settlement excluded, exit settlement included)",
      np.isclose(e.loc["NEWUSDT", "short"], short) and np.isclose(short, 0.2068), f"{e.loc['NEWUSDT', 'short']:.6f}")
check("P&L by hand: BTC leg = 0.0989, net = short + BTC - 18 bp, blind = short - 9 bp",
      np.isclose(e.loc["NEWUSDT", "btc"], btc) and np.isclose(e.loc["NEWUSDT", "net"], short + btc - 0.0018)
      and np.isclose(e.loc["NEWUSDT", "blind"], short - 0.0009), f"net {e.loc['NEWUSDT', 'net']:.6f}")
check("entry / exit / liquidity of the hand event", e.loc["NEWUSDT", "entry"] == ts("2021-01-06") and e.loc["NEWUSDT", "exit"] == ts("2021-01-20")
      and np.isclose(e.loc["NEWUSDT", "qv24"], 24e6))
check("liquidity filter: $25 M floor drops the $24 M event", len(N.build_events(pp, htab, 0, 14, min_qv=25e6)) == 0)

# ------------------------------------------------------------------ 4 delisting
check("delisting: closed at the last traded price (15, not the printed 1.0) at its close instant; BTC closed at the same instant",
      bool(e.loc["DIEUSDT", "died"]) and e.loc["DIEUSDT", "end"] == ts("2021-01-09 12:00") and np.isclose(e.loc["DIEUSDT", "short"], 0.25)
      and np.isclose(e.loc["DIEUSDT", "btc"], 0.0), f"short {e.loc['DIEUSDT', 'short']:.4f}, end {pd.to_datetime(e.loc['DIEUSDT', 'end'], unit='s')}")
p2 = hand_world(); p2["close"] = p2["close"][: -24 * 30]; p2["qv"] = p2["qv"][: -24 * 30]; p2["grid"] = p2["grid"][: -24 * 30]
p2["close"][np.arange(len(p2["grid"])) > int((ts("2021-01-09 11:00") - p2["grid"][0]) // HOUR), 2] = np.nan
pp2 = N.prep(p2)
check("delisting: a file that simply stops is closed after its last bar", pp2["death"][2] == ts("2021-01-09 12:00"))

# ------------------------------------------------------------------ 5 look-ahead
p3 = hand_world()
after_entry = p3["grid"] + HOUR > ts("2021-01-06")
p3["close"][after_entry, 1] *= 3.0; p3["qv"][after_entry, 1] *= 0.01
ev3 = N.build_events(N.prep(p3), N.listing_table(p3, hmeta), 0, 14).set_index("sym")
check("look-ahead: prices and volume after entry change -> same selection, entry and qv24",
      "NEWUSDT" in ev3.index and ev3.loc["NEWUSDT", "entry"] == ts("2021-01-06") and np.isclose(ev3.loc["NEWUSDT", "qv24"], 24e6))
p4 = hand_world()
after_exit = p4["grid"] + HOUR > ts("2021-01-20")
p4["close"][after_exit, :2] *= 0.3
ev4 = N.build_events(N.prep(p4), N.listing_table(p4, hmeta), 0, 14).set_index("sym")
check("look-ahead: prices after exit change -> event P&L unchanged", np.isclose(ev4.loc["NEWUSDT", "net"], e.loc["NEWUSDT", "net"]))

# ------------------------------------------------------------------ 6 portfolio
days = X._days(p["grid"])
one = ev[ev.sym == "NEWUSDT"].reset_index(drop=True)
net, taken, nopen = N.portfolio(pp, one, 0, days)
hand = 0.02 * (0.2 + (-0.01 + 0.02 + 0.001) + 0.1 - 0.001) - 2 * 0.02 * 0.0009
check("portfolio: constant-notional daily marks sum to the hand value (2 % per leg, 9 bp x 2 % at entry and exit)",
      np.isclose(net.sum(), hand) and nopen.max() == 1 and taken.all(), f"{net.sum():.6f} vs {hand:.6f}")
check("portfolio: held days = entry day .. the day before exit (14 days)", int((nopen > 0).sum()) == 14
      and days[np.flatnonzero(nopen)[0]] == ts("2021-01-06"))
both = ev.sort_values("entry").reset_index(drop=True)
_, tk, _ = N.portfolio(pp, both, 0, days, max_open=1)
check("portfolio: with max_open = 1 the later overlapping event is skipped", tk.tolist() == [True, False])

# ------------------------------------------------------------------ 7 bootstrap and verdict
x = np.r_[np.full(50, 0.1), np.full(50, -0.1)]; wk = np.r_[np.zeros(50), np.ones(50)]
lo, hi = N.cluster_ci(x, wk)
lo2, hi2 = N.cluster_ci(x, np.arange(100))
check("cluster_ci: two clusters of identical values -> wide CI (resamples weeks, not events)", lo < -0.05 and hi > 0.05 and hi2 - lo2 < hi - lo,
      f"[{lo:.3f}, {hi:.3f}] vs iid [{lo2:.3f}, {hi2:.3f}]")
good = dict(n=400, mean=3.0, ci=[1.0, 5.0], median=1.0, trimmed=2.5)
gctl = dict(n=400, mean=2.0, ci=[0.5, 3.5])
yrs = {2021: dict(n=30, mean=1.0), 2022: dict(n=25, mean=-0.5), 2023: dict(n=60, mean=2.0), 2020: dict(n=10, mean=-9.0)}
port = dict(ann_pct=6.0, nw_t=2.5, worst_month_pct=-3.0, worst_day_pct=-2.0)
v, crit = N.verdict(good, gctl, yrs, 0.5, port)
check("verdict: all criteria met -> PASS (one negative year with >= 20 events allowed; a 10-event year ignored)", v == "PASS", str(crit))
yrs2 = dict(yrs); yrs2[2024] = dict(n=40, mean=-0.1)
check("verdict: two negative qualifying years -> DEAD", N.verdict(good, gctl, yrs2, 0.5, port)[0] == "DEAD")
check("verdict: control CI touching 0 -> DEAD", N.verdict(good, dict(gctl, ci=[-0.1, 3.0]), yrs, 0.5, port)[0] == "DEAD")
check("verdict: worst day -5 % -> DEAD; n 299 -> DEAD", N.verdict(good, gctl, yrs, 0.5, dict(port, worst_day_pct=-5.0))[0] == "DEAD"
      and N.verdict(dict(good, n=299), gctl, yrs, 0.5, port)[0] == "DEAD")


# ------------------------------------------------------------------ 8 worlds on disk
T_START, T_END, T_HI_S = "2019-01-01", "2023-06-01", ts("2023-03-15")


def make_world(root, seed, drift_new=0.0, drift_alt=0.0, n_new=400, n_old=40, n_seasoned=40):
    """BTC + n_old coins listed in 2019 (the seasoned control pool) + listings from 2020-03 on: n_new new tokens (10 % below the
    volume floor) and n_seasoned seasoned listings, plus one TradFi listing. drift_new: log drift per day of a new token over its first
    40 days; drift_alt: log drift per day of every non-BTC coin. New-token files stop 60 days after listing."""
    rng = np.random.default_rng(seed)
    t0 = ts(T_START); D = (ts(T_END) - t0) // DAY; Hh = D * 24
    os.makedirs(os.path.join(root, "klines"), exist_ok=True); os.makedirs(os.path.join(root, "funding"), exist_ok=True)
    spot, ei = {}, {}

    def write(sym, start_h, n_h, sig, qv_h, drift_h, rate_mu, rate_sd):
        r = sig * rng.standard_normal(n_h) + drift_h[:n_h] - drift_alt / 24
        c = 10 * np.exp(np.cumsum(r))
        hrs = start_h + np.arange(n_h)
        pd.DataFrame({"ts": t0 + hrs * HOUR, "open": c, "high": c, "low": c, "close": c,
                      "quote_volume": qv_h * rng.lognormal(0, 0.3, n_h), "taker_buy_quote": 0.0, "trades": 100}).to_parquet(
            os.path.join(root, "klines", f"{sym}.parquet"), index=False)
        ft = t0 + np.arange(-(-start_h // 8) * 8, start_h + n_h, 8) * HOUR
        pd.DataFrame({"ts": ft, "rate": rate_mu + rate_sd * rng.standard_normal(len(ft))}).to_parquet(
            os.path.join(root, "funding", f"{sym}.parquet"), index=False)

    zero = np.zeros(Hh)
    rb = 0.005 * rng.standard_normal(Hh); cb = 10000 * np.exp(np.cumsum(rb))
    pd.DataFrame({"ts": t0 + np.arange(Hh) * HOUR, "open": cb, "high": cb, "low": cb, "close": cb, "quote_volume": 1e9,
                  "taker_buy_quote": 0.0, "trades": 1000}).to_parquet(os.path.join(root, "klines", "BTCUSDT.parquet"), index=False)
    pd.DataFrame({"ts": t0 + np.arange(0, Hh, 8) * HOUR, "rate": 0.0001}).to_parquet(os.path.join(root, "funding", "BTCUSDT.parquet"), index=False)
    spot["BTCUSDT"] = "2017-08"
    for i in range(n_old):
        s = f"OLD{i:02d}USDT"; spot[s] = "2018-01"
        write(s, i, Hh - i, 0.007, 2e6, zero, 0.0001, 0.00005)
    lst = np.sort(rng.integers(430 * 24, (D - 70) * 24, n_new + n_seasoned + 1))
    kinds = rng.permutation(np.r_[np.zeros(n_new, int), np.ones(n_seasoned, int), 2])
    dn = np.full(60 * 24, 0.0); dn[:40 * 24] = -drift_new / 24
    for k, (h, kind) in enumerate(zip(lst, kinds)):
        s = f"N{k:03d}USDT"
        low = rng.random() < 0.10
        if kind == 0:
            spot[s] = None
            write(s, int(h), 60 * 24, 0.010, 2e4 if low else 5e5, dn, 0.0, 0.0001)
        elif kind == 1:
            spot[s] = "2018-06"
            write(s, int(h), 60 * 24, 0.008, 5e5, zero, 0.0001, 0.00005)
        else:
            spot[s] = None; ei[s] = {"underlyingSubType": ["TradFi"], "contractType": "TRADIFI_PERPETUAL"}
            write(s, int(h), 60 * 24, 0.004, 5e5, dn * 0, 0.0, 0.00005)
    return dict(spot=spot, ei=ei)


def world_run(seed, **kw):
    with tempfile.TemporaryDirectory() as root:
        m = make_world(root, seed, **kw)
        return R.run(root=root, t_start=T_START, t_end=T_END, t_hi=T_HI_S, info=False, quiet=True, meta=m)


res = world_run(1, drift_new=0.012)
pr = res["primary"]
check("planted world: every registered criterion passes", pr["verdict"] == "PASS",
      f"{ {k: v for k, v in pr['criteria'].items() if not v} } mean {pr['stats']['mean']:+.2f} CI {np.round(pr['stats']['ci'], 2).tolist()} "
      f"ctrl {pr['stats']['minus_ctrl']['mean']:+.2f} ann {pr['portfolio']['ann_pct']:+.1f} t {pr['portfolio']['nw_t']:.1f}")
check("planted world: the TradFi listing is excluded and the control exists for every event",
      res["classes"].get("tradfi") == 1 and pr["stats"]["ctrl_missing"] == 0, f"{res['classes']}")
check("planted world: forward block holds the late events", res["forward"]["n"] > 0, f"n {res['forward']['n']}")
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    R.report(res)
out = buf.getvalue()
check("end-to-end: the report prints the verdict and one line per criterion", "VERDICT N5: PASS" in out and out.count("    [x]") == len(pr["criteria"]))

passes, means = 0, []
for sd in range(10, 20):
    r0 = world_run(sd)
    passes += r0["primary"]["verdict"] == "PASS"; means.append(round(r0["primary"]["stats"]["mean"], 2))
check("null: 10 worlds without a listing effect pass at most once", passes <= 1, f"passes {passes}, means {means}")

ra = world_run(3, drift_alt=0.006)
pa = ra["primary"]
check("alts-vs-BTC drift without a listing effect: event mean > 0 but the control criterion fails",
      pa["stats"]["mean"] > 0 and not pa["criteria"]["3 event - seasoned control > 0, CI lower > 0"],
      f"event {pa['stats']['mean']:+.2f}, event - control {pa['stats']['minus_ctrl']['mean']:+.2f} CI {np.round(pa['stats']['minus_ctrl']['ci'], 2).tolist()}")

print(f"\n{fails} failures")
sys.exit(1 if fails else 0)
