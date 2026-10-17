"""Tests for the N4-S Colab notebook (runs/n4s_probe.ipynb).

  1. every embedded piece equals its source: n4s.py, settle.cluster_boot / describe, run_n4s.prepare, the sample
  2. every code cell compiles; outputs are empty; the builder refuses to overwrite an executed notebook
  3. SMOKE: the whole notebook executes locally with nbclient on 3 real coin-days (network), writes its three files, the N4-X
     replication holds on those events, and its per-event table equals the local runner's computation on the same extracts.
     No P&L is printed.
"""
import base64, gzip, io, json, os, sys, tempfile
import numpy as np, pandas as pd, nbformat
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, "..", "harness_xsec"))
import build_notebook as B
import n4s as N, run_n4s as R
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


with tempfile.TemporaryDirectory() as tmp:
    path = B.build(os.path.join(tmp, "nb.ipynb"))
    nb = nbformat.read(path, as_version=4)
    code = [c.source for c in nb.cells if c.cell_type == "code"]

    # 1. embedded pieces
    check("cell 3 is n4s.py verbatim", code[2].split("\n", 1)[1] == open(os.path.join(B.XS, "n4s.py")).read())
    check("cell 2 holds settle.cluster_boot / describe verbatim and B_BOOT",
          B.inspect.getsource(B.settle.cluster_boot) in code[1] and B.inspect.getsource(B.settle.describe) in code[1] and f"B_BOOT = {B.settle.B_BOOT}" in code[1])
    check("cell 4 holds run_n4s.prepare verbatim", B.inspect.getsource(R.prepare) in code[3])
    b64 = code[3].split("SAMPLE_B64 = '")[1].split("'")[0]
    emb = R.prepare(pd.read_csv(io.BytesIO(gzip.decompress(base64.b64decode(b64)))))
    loc = R.load_sample()
    check("embedded sample == n4x_sample.csv (registered columns, every value)", emb.shape == loc.shape and emb.equals(loc), f"{emb.shape}")

    # 2. compile, empty outputs, refuse to overwrite
    ok = True
    for i, src in enumerate(code):
        try:
            compile(src, f"cell{i + 1}", "exec")
        except SyntaxError as ex:
            ok = False; print("   ", i + 1, ex)
    check("every code cell compiles", ok)
    check("no outputs in the built notebook", not any(c.get("outputs") for c in nb.cells if c.cell_type == "code"))
    nb.cells[1].outputs = [nbformat.v4.new_output("stream", text="x")]; nbformat.write(nb, path)
    try:
        B.build(path); refused = False
    except SystemExit:
        refused = True
    check("builder refuses to overwrite an executed notebook", refused)

    # 3. SMOKE end-to-end
    from nbclient import NotebookClient
    out_dir = os.path.join(tmp, "out"); B.build(os.path.join(tmp, "smoke.ipynb")) if not os.path.exists(os.path.join(tmp, "smoke.ipynb")) else None
    nb2 = nbformat.read(os.path.join(tmp, "smoke.ipynb"), as_version=4)
    os.environ.update(SMOKE="1", N4S_OUT=out_dir, SMOKE_DAYS="3", N4S_WORKERS="3")
    try:
        NotebookClient(nb2, timeout=900, kernel_name="python3", resources={"metadata": {"path": tmp}}).execute()
        ran = True
    except Exception as ex:
        ran = False; print("   ", type(ex).__name__, str(ex)[-600:])
    check("SMOKE: the notebook executes end to end (3 coin-days)", ran)
    files = sorted(f for f in os.listdir(out_dir) if f.startswith("n4s_")) if os.path.isdir(out_dir) else []
    check("SMOKE: writes n4s_results.json, n4s_events.csv, n4s_output.txt", files == ["n4s_events.csv", "n4s_output.txt", "n4s_results.json"], str(files))
    if files:
        res = json.load(open(os.path.join(out_dir, "n4s_results.json")))
        rep = res["replication"]
        check("SMOKE: N4-X s + 2 min trade rebuilt from aggTrades within 0.5 bp on every smoke event",
              rep["ok"] and rep["share_within_0p5bp"] == 1.0, f"n {rep['n']}, max dev {rep['max_dev']}")
        nbt = pd.read_csv(os.path.join(out_dir, "n4s_events.csv"))
        keys = set(zip(nbt.sym, nbt.day))
        ev = R.load_sample(); ev = ev[[(a, b) in keys for a, b in zip(ev.sym, ev.day)]].reset_index(drop=True)
        lt, _ = N.score_events(ev, N.event_rows(ev, os.path.join(out_dir, "extracts")))
        cols = [c for c in lt.columns if c.startswith(("gross_", "path_", "entry_")) or c in ("exit", "n4x_g2", "pre_gross")]
        same = all(np.allclose(lt[c].to_numpy(float), nbt[c].to_numpy(float), equal_nan=True) for c in cols)
        check("SMOKE: the notebook's per-event table equals the local runner's on the same extracts", same and len(lt) == len(nbt), f"{len(nbt)} events")

print("\nFAILS:", fails)
sys.exit(fails)
