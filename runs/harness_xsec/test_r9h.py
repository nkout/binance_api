"""Tests for the R9-H harness (network mocked): signing, windows, fetch cache, day-rate rule, freeze rule, equivalence, leak, identity."""
import io, json, os, sys, tempfile, urllib.error
import numpy as np
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import r9h as H
import r9_borrow as R
import carry as C
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


DAY = H.DAY_MS
# --- signing: Binance's published example
q = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559"
check("sign: Binance's documented HMAC-SHA256 example", H.sign(q, "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j") ==
      "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71")

# --- windows cover every day exactly once
W = H.windows()
days_cov = np.concatenate([np.arange(a // DAY, (b + 1) // DAY) for a, b in W])
want = np.arange(H.ms(H.FETCH_START) // DAY, H.ms(H.FETCH_END) // DAY + 1)
check("windows: 74 windows, each <= 30 days, contiguous, every day 2020-10-01 .. 2026-09-30 exactly once",
      len(W) == 74 and all(b - a + 1 <= 30 * DAY for a, b in W) and all(W[i + 1][0] == W[i][1] + 1 for i in range(len(W) - 1))
      and len(days_cov) == len(set(days_cov)) and np.array_equal(np.sort(days_cov), want), f"{len(W)} windows")

# --- signed_get with a mocked opener
class Resp(io.BytesIO):
    def __enter__(self): return self
    def __exit__(self, *a): return False

seen = []
def opener_seq(seq):
    it = iter(seq)
    def op(req, timeout=30):
        seen.append(req)
        x = next(it)
        if isinstance(x, Exception):
            raise x
        return Resp(json.dumps(x).encode())
    return op

def http_err(code, body):
    return urllib.error.HTTPError("u", code, "m", {}, io.BytesIO(json.dumps(body).encode()))

H.time.sleep = lambda s: None                                           # no real waiting in tests
st, p = H.signed_get(H.PATH, dict(asset="BTC"), "KEYABC", "SECRETXYZ", opener=opener_seq([http_err(429, {}), [{"x": 1}]]))
req = seen[-1]; qs = req.full_url.split("?", 1)[1]; body, sig = qs.rsplit("&signature=", 1)
check("signed_get: 429 retried, then ok; signature is HMAC of the exact query; key in header; secret never in the URL",
      st == "ok" and p == [{"x": 1}] and len(seen) == 2 and sig == H.sign(body, "SECRETXYZ") and req.get_header("X-mbx-apikey") == "KEYABC"
      and "SECRETXYZ" not in req.full_url and "KEYABC" not in req.full_url)
st, p = H.signed_get(H.PATH, dict(asset="CARV"), "K", "S", opener=opener_seq([http_err(400, {"code": -11027, "msg": "asset CARV is not supported"})]))
check("signed_get: 400 -11027 -> 'unsupported'", st == "unsupported")
try:
    H.signed_get(H.PATH, dict(asset="X"), "K", "S", opener=opener_seq([http_err(400, {"code": -1102, "msg": "bad param"})])); ok = False
except RuntimeError:
    ok = True
check("signed_get: any other 400 raises", ok)
st, p = H.signed_get(H.PATH, dict(asset="BTC"), "K", "S", opener=opener_seq([http_err(400, {"code": -1021, "msg": "recvWindow"}), [{"y": 2}]]))
check("signed_get: 400 -1021 (stalled request) is retried with a fresh timestamp", st == "ok" and p == [{"y": 2}])

# --- fetch_asset: cache, resume, per-window -11027
with tempfile.TemporaryDirectory() as cache:
    calls = []
    def get(params):
        calls.append(params)
        return "ok", [{"asset": params["asset"], "timestamp": params["startTime"], "dailyInterestRate": "0.0001", "vipLevel": 0}]
    w3 = W[:3]
    s1 = H.fetch_asset("AAA", w3, cache, get); n1 = len(calls)
    s2 = H.fetch_asset("AAA", w3, cache, get); n2 = len(calls)
    t, v, vip = H.load_records(cache, "AAA")
    check("fetch_asset: one request per window, none on a second call (resumable); records loaded sorted",
          s1 == s2 == (3, 0) and n1 == 3 and n2 == 3 and np.array_equal(t, [a for a, _ in w3]) and np.allclose(v, 1e-4) and (vip == 0).all())
    def get_late(params):                                                 # not on margin in the first window, listed afterwards
        calls.append(params)
        if params["startTime"] == w3[0][0]:
            return "unsupported", {"code": -11027}
        return "ok", [{"timestamp": params["startTime"], "dailyInterestRate": "0.0002", "vipLevel": 0}]
    s3 = H.fetch_asset("LATE", w3, cache, get_late)
    t3, v3, _ = H.load_records(cache, "LATE")
    check("fetch_asset: a -11027 window is cached as 'no records' and the fetch continues to later windows",
          s3 == (2, 1) and np.array_equal(t3, [a for a, _ in w3[1:]]) and H.fetch_asset("LATE", w3, cache, get_late) == (2, 1))
    s4 = H.fetch_asset("NEVER", w3, cache, lambda prm: (calls.append(prm) or ("unsupported", {"code": -11027})))
    check("fetch_asset: every window -11027 -> load_records None (never on cross margin)", s4 == (0, 3) and H.load_records(cache, "NEVER") is None)

# --- day-rate rule
d0 = H.ms("2021-01-01"); days_s = (d0 + np.arange(10) * DAY) // 1000
t = np.array([d0, d0 + 3600_000, d0 + DAY, d0 + 5 * DAY], np.int64)
v = np.array([1.0, 3.0, 5.0, 7.0])
r = H.day_rates(t, v, days_s)
check("day rate: mean of the day's records; carry <= 3 days; then NaN",
      r[0] == 2.0 and r[1] == 5.0 and r[2] == 5.0 and r[3] == 5.0 and r[4] == 5.0 and r[5] == 7.0 and r[8] == 7.0 and np.isnan(r[9]), str(r))
check("day rate: day 4 is exactly 3 days after the last record (carried); day 9 is 4 days after (NaN)", r[4] == 5.0 and np.isnan(r[9]))

# --- freeze rule
dd = H.ms("2023-01-01") + np.arange(200) * DAY                              # daily records 2023-01-01 .. 2023-07-19
vals = np.where(np.arange(200) < 120, 1e-4 * (1 + np.arange(200) % 2), 3e-4)  # alternating 1e-4 / 2e-4, then constant 3e-4 from day 120
days2 = dd // 1000
fz_ = H.frozen(dd, vals, days2)
first_ok = (H.ms(H.REGIME) + H.FROZEN_D * DAY - dd[0]) // DAY
check("freeze: never before regime + 30 d, even for a constant series",
      not H.frozen(dd, np.full(200, 1e-4), days2)[:first_ok].any() and H.frozen(dd, np.full(200, 1e-4), days2)[first_ok:].all())
check("freeze: a changing series is not frozen; flagged once 30 days of one value have passed (strictly before d)",
      not fz_[first_ok:120].any() and not fz_[120:150].any() and fz_[150] and fz_[150:].all(), f"{np.flatnonzero(fz_)[:3]}")
vals2 = vals.copy(); vals2[160] = 9e-4
fz2 = H.frozen(dd, vals2, days2)
check("freeze: a change on day d does not unfreeze d itself (window excludes d); it unfreezes d + 1",
      fz2[160] and not fz2[161], f"{fz2[158:163]}")

# --- borrow_matrices: unsupported asset never borrowable; vip check
with tempfile.TemporaryDirectory() as cache:
    os.makedirs(os.path.join(cache, "AAA")); os.makedirs(os.path.join(cache, "BAD")); os.makedirs(os.path.join(cache, "UNS"))
    json.dump(dict(payload=[{"timestamp": int(x), "dailyInterestRate": str(y), "vipLevel": 0} for x, y in zip(dd, vals)]), open(os.path.join(cache, "AAA", "1.json"), "w"))
    json.dump(dict(payload=[{"timestamp": int(dd[0]), "dailyInterestRate": "0.1", "vipLevel": 1}]), open(os.path.join(cache, "BAD", "1.json"), "w"))
    json.dump(dict(status="unsupported", payload=[], reply={"code": -11027}), open(os.path.join(cache, "UNS", "1.json"), "w"))
    rM, ex, am, bad = H.borrow_matrices(["AAA", "UNS", None, "BAD", "AAA"], cache, days2)
    check("borrow_matrices: unsupported / None columns never borrowable; amended = exists & ~frozen; shared base filled for both columns; vip != 0 reported",
          not ex[:, 1].any() and not ex[:, 2].any() and np.array_equal(am[:, 0], ex[:, 0] & ~fz_) and np.array_equal(rM[:, 0], rM[:, 4], equal_nan=True) and bad == ["BAD"])

# --- re-pricing: equivalence with R9 S2, identity, leak
rng = np.random.default_rng(0); D, N = 400, 6
F = rng.normal(0.0004, 0.0003, (D, N)); Rp = rng.normal(0, 0.02, (D, N)); Rs = Rp + rng.normal(0, 0.002, (D, N))
fzz = np.where(rng.random((D, N)) < 0.5, -0.0006, 0.0); elig = np.ones((D, N), bool)
bor = np.array([True, True, True, False, True, False]); rate = np.array([0.001, 0.002, 0.0005, 0.0, 0.01, 0.0])
old = R.reprice(elig, fzz, Rp, Rs, F, bor, rate)
new = H.reprice_tv(elig, fzz, Rp, Rs, F, np.broadcast_to(bor, (D, N)), np.broadcast_to(np.where(bor, rate, np.nan), (D, N)))
check("equivalence: constant rates + today's list reproduce R9 S2 (r9_borrow.reprice) exactly", np.array_equal(new["net"], old["net"]) and np.array_equal(new["restricted"]["pos"], old["restricted"]["pos"]))
Btv = rng.random((D, N)) < 0.8; rtv = np.where(Btv, rng.uniform(1e-5, 3e-3, (D, N)), np.nan)
res = H.reprice_tv(elig, fzz, Rp, Rs, F, Btv, rtv)
pos = res["restricted"]["pos"]
check("identity: net = restricted net - N_SLOT x sum(pos x r[d, j]); never held where not borrowable",
      np.allclose(res["net"], res["restricted"]["net"] - C.N_SLOT * np.where(pos, rtv, 0).sum(1)) and not (pos & ~Btv).any())
T = 250
Bsc = Btv.copy(); Bsc[T + 1:] = rng.random((D - T - 1, N)) < 0.3
rsc = rtv.copy(); rsc[T + 1:] = rng.uniform(0, 0.05, (D - T - 1, N)); rsc[~Bsc] = np.nan
res2 = H.reprice_tv(elig, fzz, Rp, Rs, F, Bsc, rsc)
check("leak: scrambling borrowability and rates after day T leaves positions and net at <= T unchanged",
      np.array_equal(res2["restricted"]["pos"][:T + 1], pos[:T + 1]) and np.allclose(res2["net"][:T + 1], res["net"][:T + 1]))
tsc = dd.copy(); vsc = vals.copy(); cut = 140
vsc[cut:] = rng.uniform(0, 1e-2, len(vsc) - cut)                         # records from day 140 on are scrambled
check("leak: scrambling records on/after day k leaves day rates and freeze flags at < k unchanged",
      np.array_equal(H.day_rates(tsc, vsc, days2)[:cut], H.day_rates(dd, vals, days2)[:cut], equal_nan=True)
      and np.array_equal(H.frozen(tsc, vsc, days2)[:cut + 1], H.frozen(dd, vals, days2)[:cut + 1]))

print("\nFAILS:", fails)
sys.exit(fails)
