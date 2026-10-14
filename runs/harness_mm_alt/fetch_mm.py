"""M1 — resumable S3 fetcher for the free daily bookTicker / aggTrades archives.

URL pattern
    https://data.binance.vision/data/futures/um/daily/<dataset>/<SYM>/<SYM>-<dataset>-<YYYY-MM-DD>.zip
    dataset in {bookTicker, aggTrades}; directory listing via fetch_archive.list_prefix.

Streaming discipline (disk budget is tight)
    default flow is  download one file -> analyse -> delete;  only the derived per-symbol-day row is
    kept, at  <derived>/<SYM>-<date>.json.  Pass --keep to retain the zips (off by default).
    A single 1-symbol-day live fetch may be used only to verify the fetcher.

Resumable
    * a derived row that already exists is skipped;
    * a symbol-day with no archive file writes a <derived>/<SYM>-<date>.missing sentinel and is
      skipped on the next run.

    python fetch_mm.py --days 2023-07-19 --symbols BTCUSDT --limit 2     (the verification fetch)
    python fetch_mm.py --band --limit 200                                  (band universe, streamed)

fetch_symbol_day(sym, day, dataset) -> bytes | None is the mockable unit: tests monkeypatch
fetch_mm.get with an in-memory zip, so no network is ever touched by the test suite.
"""
import argparse, io, json, os, sys, tempfile, time, zipfile
from concurrent.futures import ThreadPoolExecutor
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "harness_xsec")))

import numpy as np, pandas as pd

from mmalt import (SAMPLE_DAYS, BAND, COMPARATOR_N, TAUS, analyse_day, select_universe,
                   read_bookticker_zip, read_aggtrades_zip)
try:
    from fetch_archive import get, list_prefix, to_seconds          # reused from the xsec harness
except Exception:                                                   # standalone fallback
    import urllib.request, urllib.error, re as _re

    def get(url, tries=4):
        for k in range(tries):
            try:
                with urllib.request.urlopen(url, timeout=60) as r:
                    return r.read()
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return None
                time.sleep(2 ** k)
            except Exception:
                time.sleep(2 ** k)
        raise RuntimeError(f"failed: {url}")

    def list_prefix(prefix, files=False):
        S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
        out, marker = [], ""
        while True:
            x = get(f"{S3}?delimiter=/&prefix={prefix}&marker={marker}").decode()
            tag = "Key" if files else "Prefix"
            items = [m for m in _re.findall(rf"<{tag}>([^<]*)</{tag}>", x) if m != prefix]
            out += items
            if "<IsTruncated>true" not in x or not items:
                return out
            marker = items[-1]

    def to_seconds(x):
        x = pd.to_numeric(x).astype(np.int64)
        return np.where(x > 10 ** 14, x // 10 ** 6, x // 1000).astype(np.int64)

DL = "https://data.binance.vision/"
DATASETS = ("bookTicker", "aggTrades")


def object_url(sym, day, dataset):
    return f"{DL}data/futures/um/daily/{dataset}/{sym}/{sym}-{dataset}-{day}.zip"


def fetch_symbol_day(sym, day, dataset) -> "bytes | None":
    """One archive object as raw zip bytes, or None when the archive has no such file (404)."""
    return get(object_url(sym, day, dataset))


def list_days(sym, dataset):
    """All YYYY-MM-DD dates the archive lists for (sym, dataset)."""
    keys = list_prefix(f"data/futures/um/daily/{dataset}/{sym}/", files=True)
    out = []
    for k in keys:
        if not k.endswith(".zip"):
            continue
        name = k.rsplit("/", 1)[-1]
        parts = name[:-4].split("-")
        if len(parts) >= 3:
            out.append("-".join(parts[-3:]))
    return sorted(out)


def _zip_bytes(df, name, header=True):
    """Pack a DataFrame as a one-member zip of CSV bytes (used by tests / --keep verification)."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=header)
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(name, buf.getvalue())
    return zbuf.getvalue()


def analyse_symbol_day(sym, day, book_blob, trade_blob, cohort="band"):
    """Parse both archive objects into one derived row (mmalt.analyse_day schema)."""
    bt = read_bookticker_zip(book_blob)
    tr = read_aggtrades_zip(trade_blob) if trade_blob is not None else pd.DataFrame(
        {"timestamp": np.zeros(0, np.int64), "price": np.zeros(0), "qty": np.zeros(0),
         "d": np.zeros(0), "t_s": np.zeros(0, np.int64)})
    return analyse_day(bt, tr, TAUS, symbol=sym, day=day, cohort=cohort)


def process_symbol_day(sym, day, out_dir, cohort="band", keep=False, tmpdir=None):
    """Stream one symbol-day: download both files -> derive the row -> write JSON -> delete zips.

    Returns (status, row) with status in {"ok", "missing", "missing-trades", "derived", "error"}.
    """
    os.makedirs(out_dir, exist_ok=True)
    stem = f"{sym}-{day}"
    derived = os.path.join(out_dir, f"{stem}.json")
    sentinel = os.path.join(out_dir, f"{stem}.missing")
    if os.path.exists(derived):
        return "derived", None
    if os.path.exists(sentinel):
        return "missing", None
    book = fetch_symbol_day(sym, day, "bookTicker")
    if book is None:
        with open(sentinel, "w") as fh:
            fh.write(f"no bookTicker object for {sym} {day}\n")
        return "missing", None
    trades = fetch_symbol_day(sym, day, "aggTrades")
    try:
        row = analyse_symbol_day(sym, day, book, trades, cohort=cohort)
    except Exception as e:
        if not keep:
            del book, trades
        raise RuntimeError(f"{stem}: {e}") from e
    with open(derived, "w") as fh:
        json.dump(row, fh, indent=1, default=float)
    if keep:
        tdir = tmpdir or os.path.join(out_dir, "_zips")
        os.makedirs(tdir, exist_ok=True)
        for nm, blob in (("bookTicker", book), ("aggTrades", trades)):
            if blob is not None:
                with open(os.path.join(tdir, f"{stem}-{nm}.zip"), "wb") as fh:
                    fh.write(blob)
    # stream discipline: drop the blobs before moving on (the caller deletes any on-disk temp)
    del book, trades
    return ("ok" if row["n_tr"] > 0 else "missing-trades"), row


def resolve_symbols(days, symbols=None, band=None, btc=False, root=None, universe=None):
    """(symbol, cohort) pairs to fetch: explicit list, or the point-in-time band / comparator per day."""
    from mmalt import DATA_ROOT
    root = root or DATA_ROOT
    if symbols:
        return {d: [(s, "band") for s in symbols] for d in days}
    out = {}
    for d in days:
        b, c = select_universe(d, root=root, universe=universe)
        sel = []
        if band:
            sel += [(s, "band") for s in b]
        if btc:
            sel += [(s, "comparator") for s in c]
        if not sel:
            sel = [(s, "band") for s in b] + [(s, "comparator") for s in c]
        out[d] = sel
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", default=",".join(SAMPLE_DAYS), help="comma-separated YYYY-MM-DD list")
    ap.add_argument("--symbols", default="", help="explicit comma-separated symbols (overrides --band/--btc)")
    ap.add_argument("--band", action="store_true", help="the point-in-time volume-rank band")
    ap.add_argument("--btc", action="store_true", help="BTCUSDT + top-10 majors comparator")
    ap.add_argument("--out", default=os.path.join(HERE, "..", "..", "data", "mm_alt"))
    ap.add_argument("--root", default=os.path.join(HERE, "..", "..", "data", "xsec"))
    ap.add_argument("--limit", type=int, default=0, help="safety cap on symbol-days (0 = no cap)")
    ap.add_argument("--workers", type=int, default=8, help="concurrent downloads")
    ap.add_argument("--keep", action="store_true", help="retain the downloaded zips")
    a = ap.parse_args()
    days = [d for d in a.days.split(",") if d]
    syms = [s for s in a.symbols.split(",") if s] if a.symbols else None
    want_band = a.band or not (a.band or a.btc or syms)     # default = band + comparator
    plan = resolve_symbols(days, syms, want_band, a.btc or not (a.band or a.btc or syms), a.root)
    jobs = [(d, s, ch) for d in days for (s, ch) in plan.get(d, [])]
    if a.limit:
        jobs = jobs[: a.limit]
    print(f"fetch: {len(jobs)} symbol-days over {len(days)} days -> {a.out}", flush=True)
    out_dir = a.out
    os.makedirs(out_dir, exist_ok=True)
    t0 = time.time(); n = {"ok": 0, "derived": 0, "missing": 0, "missing-trades": 0, "error": 0}
    tmpdir = tempfile.mkdtemp(prefix="mm_alt_tmp_")
    try:
        def _job(j):
            d, s, ch = j
            try:
                st, _ = process_symbol_day(s, d, out_dir, cohort=ch, keep=a.keep, tmpdir=tmpdir)
            except Exception as e:
                st = "error"; print(f"  ERROR {s} {d}: {e}", file=sys.stderr)
            return st
        with ThreadPoolExecutor(max_workers=max(1, a.workers)) as ex:
            for i, st in enumerate(ex.map(_job, jobs), 1):
                n[st] = n.get(st, 0) + 1
                if i % 20 == 0 or st in ("error", "missing"):
                    print(f"  {i}/{len(jobs)}: {st} | {time.time() - t0:.0f}s", flush=True)
    finally:
        if not a.keep:
            for f in os.listdir(tmpdir):
                try:
                    os.remove(os.path.join(tmpdir, f))
                except OSError:
                    pass
            try:
                os.rmdir(tmpdir)
            except OSError:
                pass
    print(f"done: {n} | {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
