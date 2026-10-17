"""R9-H — CARRY- on the borrow rates actually charged (next_signal_ideas.md, Round 9, R9-H + implementation details).

Library: signed fetch of /sapi/v1/margin/interestRateHistory with a raw cache, the day-rate rule, point-in-time borrowability
(registered "rate exists" and the amended "rate exists and not frozen"), and CARRY- re-priced with time-varying rates.
The API key comes from BINANCE_KEY / BINANCE_SECRET and is never written anywhere.
"""
import hashlib, hmac, json, os, threading, time, urllib.error, urllib.parse, urllib.request
import numpy as np
import carry as C

API = "https://api.binance.com"
PATH = "/sapi/v1/margin/interestRateHistory"
DAY_MS = 86_400_000
SPAN_D = 30
FETCH_START = "2020-10-01"
FETCH_END = "2026-09-30"                        # last day covered by the registered windows
XCHECK = ("2026-10-01", "2026-10-15")           # extra window, snapshot cross-check only
CARRY_D = 3                                     # day without a record: last record of the previous 3 days
REGIME = "2023-03-01"                           # intraday rate-change records start
FROZEN_D = 30                                   # one value for 30 days -> frozen (applies from REGIME + 30 d)
NOT_SUPPORTED = -11027
RECV_WINDOW = -1021                             # timestamp outside recvWindow (retryable)
MAX_RPS = 5.0


def ms(day):
    return int(np.datetime64(day, "ms").astype(np.int64))


def sign(query, secret):
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


def windows(start=FETCH_START, end=FETCH_END, span_d=SPAN_D):
    """Contiguous [a, b] windows (ms, inclusive) of span_d days covering start 00:00 .. end 23:59:59.999."""
    a, stop = ms(start), ms(end) + DAY_MS - 1
    out = []
    while a <= stop:
        out.append((a, min(a + span_d * DAY_MS - 1, stop)))
        a += span_d * DAY_MS
    return out


class Limiter:
    def __init__(self, rps=MAX_RPS):
        self.gap, self.t, self.lock = 1.0 / rps, 0.0, threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            self.t = max(self.t + self.gap, now)
            delay = self.t - now
        if delay > 0:
            time.sleep(delay)


def signed_get(path, params, key, secret, limiter=None, tries=6, opener=urllib.request.urlopen):
    """Returns (status, payload). status 'ok' with the JSON body, or 'unsupported' for -11027; raises on other errors."""
    for k in range(tries):
        if limiter:
            limiter.wait()
        q = urllib.parse.urlencode(dict(params, recvWindow=10000, timestamp=int(time.time() * 1000)))
        req = urllib.request.Request(f"{API}{path}?{q}&signature={sign(q, secret)}", headers={"X-MBX-APIKEY": key})
        try:
            with opener(req, timeout=30) as r:
                return "ok", json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read()
            if e.code == 400:
                try:
                    code = json.loads(body).get("code")
                except ValueError:
                    code = None
                if code == NOT_SUPPORTED:
                    return "unsupported", json.loads(body)
                if code == RECV_WINDOW:                              # a stalled request; the next try gets a fresh timestamp
                    time.sleep(min(30, 2 ** (k + 1)))
                    continue
                raise RuntimeError(f"HTTP 400 {body[:200]!r}")
            if e.code in (418, 429) or e.code >= 500:
                time.sleep(min(120, 2 ** (k + 2)))
                continue
            raise RuntimeError(f"HTTP {e.code} {body[:200]!r}")
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(min(60, 2 ** (k + 1)))
    raise RuntimeError(f"failed after {tries} tries: {params}")


def cache_path(cache, asset, a):
    return os.path.join(cache, asset, f"{a}.json")


def fetch_asset(asset, wins, cache, get):
    """Fetch every window not cached yet. get(params) -> (status, payload). A -11027 reply means no margin listing in that window
    (Binance returns it for windows before an asset joined cross margin); it is cached as a window without records.
    Returns (windows with records, windows unsupported)."""
    os.makedirs(os.path.join(cache, asset), exist_ok=True)
    for a, b in wins:
        f = cache_path(cache, asset, a)
        if os.path.exists(f):
            continue
        st, payload = get(dict(asset=asset, vipLevel=0, startTime=a, endTime=b))
        rec = dict(fetched_ms=int(time.time() * 1000), asset=asset, startTime=a, endTime=b, status=st,
                   payload=payload if st == "ok" else [], reply=None if st == "ok" else payload)
        tmp = f + ".tmp"
        json.dump(rec, open(tmp, "w")); os.replace(tmp, f)
    recs = [json.load(open(cache_path(cache, asset, a))) for a, _ in wins]
    return sum(r.get("status", "ok") == "ok" for r in recs), sum(r.get("status") == "unsupported" for r in recs)


def load_records(cache, asset):
    """(timestamps ms, rates, vip levels) sorted by time, de-duplicated; None if no window was ever supported (never on cross margin)."""
    d = os.path.join(cache, asset)
    if not os.path.isdir(d):
        return None
    rows, supported = {}, False
    for f in os.listdir(d):
        if not f.endswith(".json"):
            continue
        rec = json.load(open(os.path.join(d, f)))
        if rec.get("status", "ok") != "ok":
            continue
        supported = True
        for r in rec["payload"]:
            rows[int(r["timestamp"])] = (float(r["dailyInterestRate"]), int(r["vipLevel"]))
    if not supported:
        return None
    if not rows:
        return np.zeros(0, np.int64), np.zeros(0), np.zeros(0, int)
    t = np.array(sorted(rows), np.int64)
    return t, np.array([rows[x][0] for x in t]), np.array([rows[x][1] for x in t])


def day_rates(t, v, days_s, carry_d=CARRY_D):
    """r[d] = mean of the records stamped in [d, d + 1 day) (UTC); with none, the last record of the previous carry_d days; else NaN."""
    D = len(days_s); r = np.full(D, np.nan)
    if len(t) == 0:
        return r
    d0 = np.asarray(days_s, np.int64) * 1000
    lo = np.searchsorted(t, d0, "left"); hi = np.searchsorted(t, d0 + DAY_MS, "left")
    cs = np.r_[0.0, np.cumsum(v)]
    has = hi > lo
    r[has] = (cs[hi[has]] - cs[lo[has]]) / (hi[has] - lo[has])
    for i in np.flatnonzero(~has):
        k = lo[i] - 1                                          # last record before day d
        if k >= 0 and t[k] >= d0[i] - carry_d * DAY_MS:
            r[i] = v[k]
    return r


def frozen(t, v, days_s, regime=REGIME, look_d=FROZEN_D):
    """True on day d >= regime + look_d when every record in [d - look_d, d) has the same value (and there is one)."""
    D = len(days_s); out = np.zeros(D, bool)
    if len(t) == 0:
        return out
    d0 = np.asarray(days_s, np.int64) * 1000
    chg = np.r_[False, v[1:] != v[:-1]]                       # record i differs from record i - 1
    cc = np.r_[0, np.cumsum(chg)]
    lo = np.searchsorted(t, d0 - look_d * DAY_MS, "left"); hi = np.searchsorted(t, d0, "left")
    n = hi - lo
    # changes strictly inside the window: records lo+1 .. hi-1 compared with their predecessor
    inner = np.where(n >= 2, cc[np.maximum(hi, 1)] - cc[np.minimum(lo + 1, len(chg))], 0)
    ok = d0 >= ms(regime) + look_d * DAY_MS
    out[:] = ok & (n >= 1) & (inner == 0)
    return out


def borrow_matrices(assets, cache, days_s):
    """For a list of base assets (one per column, None allowed): r (D, N), exists (D, N), amended (D, N), vip-level check."""
    D, N = len(days_s), len(assets)
    r = np.full((D, N), np.nan); ex = np.zeros((D, N), bool); am = np.zeros((D, N), bool)
    memo, bad_vip = {}, set()
    for j, a in enumerate(assets):
        if a is None:
            continue
        if a not in memo:
            rec = load_records(cache, a)
            if rec is None:
                memo[a] = None
            else:
                t, v, vip = rec
                if len(vip) and (vip != 0).any():
                    bad_vip.add(a)
                rr = day_rates(t, v, days_s)
                memo[a] = (rr, np.isfinite(rr), np.isfinite(rr) & ~frozen(t, v, days_s))
        if memo[a] is not None:
            r[:, j], ex[:, j], am[:, j] = memo[a]
    return r, ex, am, sorted(bad_vip)


def reprice_tv(elig, fz, Rp, Rs, F, B, rate):
    """CARRY- as R5 on elig & B (time-varying), paying rate[d, j] on the spot-short notional (N_SLOT) every held day."""
    r = C.simulate(elig & B, fz, Rp, Rs, F, mirror=True)
    borrow = C.N_SLOT * np.where(r["pos"], np.nan_to_num(rate), 0.0).sum(1)
    return dict(restricted=r, borrow=borrow, net=r["net"] - borrow)
