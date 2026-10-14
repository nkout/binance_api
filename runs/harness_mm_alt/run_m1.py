"""M1 — run the pre-registered mid-cap alt market-making feasibility test (next_signal_ideas.md, R8/M1).

Loads the derived per-symbol-day rows written by fetch_mm.py (data/mm_alt/*.json), pools them
equal-weight, evaluates the pre-registered PASS criteria and prints a per-criterion report.

    python run_m1.py [--root ../../data/xsec] [--derived ../../data/mm_alt] [--out m1_results.json]
                     [--days 2023-07-19,...] [--limit 0]

Works with zero real data (graceful "no data" + FAIL verdict) and with synthetic derived rows.
"""
import argparse, glob, json, os, sys, time
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import mmalt as M

DEFAULT_DERIVED = os.path.join(HERE, "..", "..", "data", "mm_alt")


def load_rows(derived, days=None, limit=0):
    """Every derived <SYM>-<date>.json row under `derived`, optionally filtered by day / count."""
    files = sorted(glob.glob(os.path.join(derived, "*.json")))
    rows = []
    for f in files:
        try:
            r = json.load(open(f))
        except Exception:
            continue
        if not isinstance(r, dict) or "mk" not in r:
            continue
        if days and r.get("day") not in days:
            continue
        rows.append(r)
    if limit:
        rows = rows[:limit]
    return rows


def run(rows, quiet=False):
    """Pool the derived rows and evaluate the criteria -> results dict (JSON-safe)."""
    say = (lambda *a: None) if quiet else (lambda *a: print(*a, flush=True))
    t0 = time.time()
    if not rows:
        say("no derived rows found -> no data")
        return {
            "meta": dict(n_rows=0, source="none"), "metrics": {},
            "criteria": {k: dict(ok=False, detail="no data") for k in
                         ("net5_mean_pos_and_ci_pos", "median_spread_gt_2x_fee", "band_gt_comparator",
                          "same_sign_median_mean", "days_pos_ge_3")},
            "pass_": False, "pass": False, "verdict": "FAIL", "note": "no data",
        }
    p = M.pool(rows)
    days = p["days"]
    say(f"rows {p['n_rows']} (band {p['n_band']}, comparator {p['n_comparator']}) over days "
        f"{', '.join(days) if days else '-'} | {time.time() - t0:.1f}s")
    met = p["metrics"]
    say(f"band net_after_fee(5s) mean {met['band_net_after_fee_5s_mean']:+.3f} bp "
        f"(median {met['band_net_after_fee_5s_median']:+.3f}), day-clustered CI95 "
        f"[{met['band_net_after_fee_5s_ci95'][0]:+.3f}, {met['band_net_after_fee_5s_ci95'][1]:+.3f}]")
    say(f"band median rel spread {met['band_median_rel_spread_bp']:.3f} bp | "
        f"comparator net_after_fee(5s) {met['comparator_net_after_fee_5s_mean']:+.3f} bp | "
        f"positive days {met['band_days_positive']}/{met['band_days']} | "
        f"band OFI R2 {met['band_ofi_r2_mean']:.3f}")
    verdict = "PASS" if p["pass"] else "FAIL"
    out = {
        "meta": dict(n_rows=p["n_rows"], n_band=p["n_band"], n_comparator=p["n_comparator"],
                     days=days, t0=str(pd.Timestamp.now("UTC"))),
        "metrics": met, "criteria": p["criteria"],
        "pass_": p["pass"], "pass": p["pass"], "verdict": verdict,
    }
    return out


def report(res):
    for k, c in res["criteria"].items():
        print(f"  [{'x' if c['ok'] else ' '}] {k}: {c['detail']}")
    print(f"\nVERDICT: {res['verdict']}" + (" — no data" if res.get("note") == "no data" else
          (" — all pre-registered criteria met" if res["verdict"] == "PASS"
           else " — at least one pre-registered criterion failed")))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join(HERE, "..", "..", "data", "xsec"))
    ap.add_argument("--derived", default=DEFAULT_DERIVED)
    ap.add_argument("--out", default=os.path.join(HERE, "m1_results.json"))
    ap.add_argument("--days", default="", help="comma-separated YYYY-MM-DD filter (default: all)")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    days = [d for d in a.days.split(",") if d] or None
    rows = load_rows(a.derived, days, a.limit)
    res = run(rows)
    json.dump(res, open(a.out, "w"), indent=1, default=float)
    print(f"\nwrote {a.out}")
    report(res)


if __name__ == "__main__":
    main()
