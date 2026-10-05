"""Daily Binance borrow-rate snapshot (idempotent per UTC day), run by cron. Builds a history the public endpoint does not offer.

Writes data/borrow_history/borrow_YYYY-MM-DD.json.gz holding:
  assets      VIP0 cross-margin daily interest rate + borrow limit (coin units) for every listed asset (undocumented public endpoint)
  perps       every USDT perp's mark price and last funding rate (public futures endpoint)
  candidates  perps whose last funding rate <= -0.01 %, with FUND7 (sum of 7 days of funding / 21), as in r9_borrow.py
The first successful run of each UTC day wins; later runs that day exit 0 without fetching. Failures exit 1 and write nothing.

    python snapshot_borrow.py [--force] [--out-dir DIR]
"""
import argparse, gzip, json, os, sys, time
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import r9_borrow as R

DEFAULT_DIR = os.path.join(HERE, "..", "..", "data", "borrow_history")
MIN_ASSETS = 100                                      # the list has ~480; far fewer means the endpoint changed or failed


def snapshot():
    now_ms = int(time.time() * 1000)
    borrow = R.fetch_borrow()
    if len(borrow) < MIN_ASSETS:
        raise RuntimeError(f"borrow list has only {len(borrow)} assets (< {MIN_ASSETS}); endpoint changed?")
    pi = R.get(R.FAPI + "premiumIndex")
    perps = {x["symbol"]: dict(mark=float(x["markPrice"]), last_funding=float(x["lastFundingRate"]))
             for x in pi if x["symbol"].endswith("USDT") and "_" not in x["symbol"] and x.get("lastFundingRate") not in (None, "")}
    n_sym, cands = R.fetch_candidates(now_ms)
    return dict(fetched_utc=pd.Timestamp.now("UTC").isoformat(), fetched_ms=now_ms, source=R.BORROW_URL, vip_level=0,
                assets=borrow, perps=perps, candidates=cands)


def write_atomic(path, obj):
    tmp = path + ".tmp"
    with gzip.open(tmp, "wt") as f:
        json.dump(obj, f, separators=(",", ":"))
    os.replace(tmp, path)


def main(argv=None):
    ap = argparse.ArgumentParser(); ap.add_argument("--force", action="store_true"); ap.add_argument("--out-dir", default=DEFAULT_DIR)
    a = ap.parse_args(argv)
    os.makedirs(a.out_dir, exist_ok=True)
    day = pd.Timestamp.now("UTC").strftime("%Y-%m-%d")
    path = os.path.join(a.out_dir, f"borrow_{day}.json.gz")
    if os.path.exists(path) and not a.force:
        print(f"{day}: snapshot exists, skipping"); return 0
    try:
        snap = snapshot()
    except Exception as e:
        print(f"{day}: snapshot FAILED: {type(e).__name__}: {e}", file=sys.stderr); return 1
    write_atomic(path, snap)
    print(f"{day}: wrote {os.path.basename(path)} ({os.path.getsize(path) / 1024:.0f} KB): {len(snap['assets'])} assets, {len(snap['perps'])} perps, {len(snap['candidates'])} candidates")
    return 0


if __name__ == "__main__":
    sys.exit(main())
