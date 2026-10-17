"""Build runs/n4s_probe.ipynb (N4-S, next_signal_ideas.md Round 10) — Colab, CPU only (network bound).

    python build_notebook.py [out.ipynb]    (default ../n4s_probe.ipynb; refuses to overwrite an executed notebook)

The notebook is self-contained: it embeds harness_xsec/n4s.py verbatim, settle.cluster_boot / describe verbatim, run_n4s.prepare,
and the 2,000-event N4-X sample (gzip + base64). test_notebook.py checks each embedded piece against its source file.
"""
import base64, gzip, inspect, os, sys
import nbformat as nbf

HERE = os.path.dirname(os.path.abspath(__file__))
XS = os.path.join(HERE, "..", "harness_xsec")
sys.path.insert(0, XS)
import settle, run_n4s
OUT = os.path.join(HERE, "..", "n4s_probe.ipynb")
LIB_SRC = open(os.path.join(XS, "n4s.py")).read()
HELPER_SRC = f"B_BOOT = {settle.B_BOOT}\n\n\n" + inspect.getsource(settle.cluster_boot) + "\n\n" + inspect.getsource(settle.describe)
PREPARE_SRC = f"SAMPLE_COLS = {run_n4s.SAMPLE_COLS!r}\n\n\n" + inspect.getsource(run_n4s.prepare)


def sample_b64():
    import pandas as pd
    df = pd.read_csv(run_n4s.SAMPLE, dtype=str, keep_default_na=False)[run_n4s.SAMPLE_COLS]      # the original text, unchanged
    raw = df.to_csv(index=False).encode()
    return base64.b64encode(gzip.compress(raw, mtime=0)).decode()


MD0 = r"""# N4-S — the funding-settlement move at sub-second resolution

**Question.** N4 found a real move after extreme funding settlements: +25 bp per event net on 1 h bars, n 20,847. N4-X showed that about 26 of the hour's
43 bp happen in the first minute, so an entry at s + 2 min nets ~0. The direction (sign of the previous rate) and the time (the scheduled settlement) are
known in advance, so the open question is how fast the move happens **inside** that first minute. If a material part comes after half a second, an order timed
to the settlement can catch it.

**Design** (pre-registered in `next_signal_ideas.md`, Round 10, N4-S): the 2,000 N4-X events (2025-01 → 2026-08), re-priced on Binance aggTrades.
- Entry at s + Δ on the taker side (buy at the first buyer-initiated trade, sell at the first seller-initiated one), so the spread is inside the fills.
  **Primary Δ = 0.5 s**; information Δ = 0.1, 0.25, 1, 2, 5, 10, 30, 60, 120 s.
- Exit: first opposite-side taker trade at or after s + 3,595 s. Net = gross − 9 bp (VIP0 + BNB taker fees).
- Bootstrap clustered by settlement time. Equivalence check first: N4-X's s + 2 min trade rebuilt from aggTrades must match its saved values
  (≥ 99 % within 0.5 bp), otherwise the verdict is STOPPED.

**PASS (all, Δ = 0.5 s):** net mean > 0 with CI lower bound > 0; median > 0; mean > 0 in 2025 and in 2026; mean > 0 at Δ = 1 s; ≥ 1,800 events.

**How to run.** Colab → Runtime → Change runtime type → **CPU** is enough (no GPU needed) → Run all. It mounts Google Drive and writes to
`MyDrive/n4s_probe/`. Downloads are ~19 GB in total but streamed: only small extracts (~40 MB) are kept, in `MyDrive/n4s_probe/extracts/`.
- Expect roughly 30–90 minutes, mostly the fetch cell. Keep the tab open.
- **If the runtime disconnects, just Run all again**: finished coin-days are cached on Drive and skipped.
- Optional: copy the 170 extracts already made locally (`data/n4s/*.npz`) into `MyDrive/n4s_probe/extracts/` first to skip them.
- When it finishes, bring back `n4s_results.json`, `n4s_output.txt` and `n4s_events.csv` from `MyDrive/n4s_probe/` (or this executed notebook).
"""

C_SETUP = r"""# Cell 1 — environment
import os, sys, io, json, time, base64, gzip
from concurrent.futures import ThreadPoolExecutor
import numpy as np, pandas as pd, pyarrow
IN_COLAB = 'google.colab' in sys.modules
SMOKE = os.environ.get('SMOKE') == '1'
if IN_COLAB:
    from google.colab import drive
    drive.mount('/content/drive')
OUT_DIR = os.environ.get('N4S_OUT', '/content/drive/MyDrive/n4s_probe')
EXTRACTS = os.path.join(OUT_DIR, 'extracts')
os.makedirs(EXTRACTS, exist_ok=True)
WORKERS = int(os.environ.get('N4S_WORKERS', '8'))
SMOKE_DAYS = int(os.environ.get('SMOKE_DAYS', '3'))
print(f'colab {IN_COLAB} | smoke {SMOKE} | out {OUT_DIR} | workers {WORKERS} | cpus {os.cpu_count()} | pyarrow {pyarrow.__version__}')
"""

C_HELPERS = "# Cell 2 — statistics helpers, verbatim from harness_xsec/settle.py\nimport numpy as np\n\n" + HELPER_SRC

C_LIB = "# Cell 3 — the N4-S library, verbatim from harness_xsec/n4s.py\n" + LIB_SRC

C_SAMPLE = ("# Cell 4 — the 2,000 N4-X events (harness_xsec/n4x_sample.csv, registered columns), embedded\n" + PREPARE_SRC +
            "\n\nSAMPLE_B64 = '{B64}'\n"
            "e = prepare(pd.read_csv(io.BytesIO(gzip.decompress(base64.b64decode(SAMPLE_B64)))))\n"
            "print(f'{len(e)} events, {e.groupby([\"sym\", \"day\"]).ngroups} coin-days, years {e.year.value_counts().sort_index().to_dict()}')\n")

C_CHECK = r"""# Cell 5 — can this runtime reach the Binance archive?
t0 = time.time()
try:
    blob = get(URL.format(s='AGLDUSDT', d='2025-01-01'))
    print(f'ok: {len(blob) / 1e6:.1f} MB in {time.time() - t0:.1f} s')
except Exception as ex:
    raise SystemExit(f'cannot reach data.binance.vision from this runtime ({type(ex).__name__}: {ex}). '
                     'Try a new runtime or a different region; nothing has been written yet.')
"""

C_FETCH = r"""# Cell 6 — fetch: stream each coin-day, keep only the event windows (resumable: re-run after a disconnect)
groups = [(k, g['s'].tolist()) for k, g in e.groupby(['sym', 'day'])]
if SMOKE:
    groups = groups[:SMOKE_DAYS]
cached = sum(os.path.exists(extract_path(EXTRACTS, *k)) for k, _ in groups)
print(f'{len(groups)} coin-days, {cached} already on Drive, {len(groups) - cached} to fetch', flush=True)
errors, t0, mb, n = [], time.time(), 0.0, 0

def one(kg):
    (sym, day), ss = kg
    try:
        return (sym, day) + fetch_coin_day(sym, day, ss, EXTRACTS)
    except Exception as ex:
        return sym, day, 0.0, f'error {type(ex).__name__}: {str(ex)[:120]}'

with ThreadPoolExecutor(WORKERS) as ex:
    for sym, day, size, st in ex.map(one, groups):
        n += 1; mb += size
        if st.startswith('error'):
            errors.append((sym, day, st))
        if n % 50 == 0 or n == len(groups) or st.startswith('error') or st == 'missing':
            el = time.time() - t0
            print(f'  {n}/{len(groups)} {sym} {day}: {st} | {mb / 1e3:.2f} GB | {el / 60:.1f} min | ETA {el / n * (len(groups) - n) / 60:.0f} min', flush=True)
print(f'done: {len(errors)} errors' + (' -> re-run this cell (finished coin-days are cached)' if errors else ''))
assert not errors, errors[:5]
"""

C_SCORE = r"""# Cell 7 — score: replication check, the registered cell, information tables
ev = e
if SMOKE:
    keep = {k for k, _ in groups}
    ev = e[[(a, b) in keep for a, b in zip(e.sym, e.day)]].reset_index(drop=True)
t, out = score_events(ev, event_rows(ev, EXTRACTS))
TXT = report(out)
print(TXT)
"""

C_SAVE = r"""# Cell 8 — save (bring these three files back)
json.dump(out, open(os.path.join(OUT_DIR, 'n4s_results.json'), 'w'), indent=1, default=float)
t.to_csv(os.path.join(OUT_DIR, 'n4s_events.csv'), index=False)
open(os.path.join(OUT_DIR, 'n4s_output.txt'), 'w').write(TXT + '\n')
print('saved:', sorted(f for f in os.listdir(OUT_DIR) if f.startswith('n4s_')))
"""

CELLS = [("md", MD0), ("code", C_SETUP), ("code", C_HELPERS), ("code", C_LIB), ("code", None), ("code", C_CHECK), ("code", C_FETCH),
         ("code", C_SCORE), ("code", C_SAVE)]


def build(path=OUT):
    if os.path.exists(path):
        old = nbf.read(path, as_version=4)
        if any(c.get("outputs") for c in old.cells if c.cell_type == "code"):
            raise SystemExit(f"refusing to overwrite executed notebook {path}")
    sample_cell = C_SAMPLE.replace("{B64}", sample_b64())
    nb = nbf.v4.new_notebook()
    nb.cells = [nbf.v4.new_markdown_cell(s) if k == "md" else nbf.v4.new_code_cell(s if s is not None else sample_cell) for k, s in CELLS]
    nb.metadata = {"colab": {"provenance": []}, "kernelspec": {"display_name": "Python 3", "name": "python3"},
                   "language_info": {"name": "python"}}
    nbf.write(nb, path)
    return path


if __name__ == "__main__":
    print("wrote", build(sys.argv[1] if len(sys.argv) > 1 else OUT))
