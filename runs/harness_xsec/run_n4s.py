"""N4-S — the settlement move at sub-second resolution (next_signal_ideas.md, Round 10). Local runner; the Colab notebook
(runs/n4s_probe.ipynb, built by runs/harness_n4s/build_notebook.py) runs the same n4s.py functions remotely.

    python run_n4s.py fetch     # stream the aggTrades coin-days of the N4-X sample; keep only the event windows -> data/n4s/ (resumable)
    python run_n4s.py score     # replication check, primary cell, tables -> n4s_results.json, n4s_events.csv
"""
import json, os, sys, time
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import n4s as N

SAMPLE = os.path.join(HERE, "n4x_sample.csv")
OUT = os.path.join(HERE, "..", "..", "data", "n4s")
SAMPLE_COLS = ["sym", "s", "r_prev", "r_s", "interval_h", "q", "p_s", "g2"]


def prepare(e):
    """Sample columns -> the event table the library expects."""
    e = e[SAMPLE_COLS].copy()
    e["d"] = -e["q"]                                      # q = -sign(r_prev) (receiving side); the trade takes the paying side
    ts = pd.to_datetime(e["s"], unit="s", utc=True)
    e["day"], e["year"] = ts.dt.strftime("%Y-%m-%d"), ts.dt.year
    return e


def load_sample():
    return prepare(pd.read_csv(SAMPLE))


def cmd_fetch(workers=6):
    e = load_sample(); os.makedirs(OUT, exist_ok=True)
    groups = [(k, g["s"].tolist()) for k, g in e.groupby(["sym", "day"])]
    print(f"{len(groups)} coin-days -> {OUT}", flush=True)
    t0 = time.time(); mb = n = 0
    with ThreadPoolExecutor(workers) as ex:
        for (sym, day), (size, st) in zip([k for k, _ in groups], ex.map(lambda kg: N.fetch_coin_day(*kg[0], kg[1], OUT), groups)):
            n += 1; mb += size
            if n % 50 == 0 or n == len(groups) or st == "missing":
                print(f"  {n}/{len(groups)} {sym} {day}: {st} | {mb / 1e3:.2f} GB | {time.time() - t0:.0f}s", flush=True)


def cmd_score():
    e = load_sample()
    t, out = N.score_events(e, N.event_rows(e, OUT))
    t.to_csv(os.path.join(HERE, "n4s_events.csv"), index=False)
    json.dump(out, open(os.path.join(HERE, "n4s_results.json"), "w"), indent=1, default=float)
    print(N.report(out))


if __name__ == "__main__":
    {"fetch": cmd_fetch, "score": cmd_score}[sys.argv[1]]()
