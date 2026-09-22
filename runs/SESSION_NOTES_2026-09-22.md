# Session notes — 2026-09-22 (evening)

Standalone state doc. Start here, then `next_signal_ideas.md` (the idea list with pre-registered kill
criteria and current status).

---

## Where things stand

**Every BTC direction signal found so far is real and too small or too fast to pay fees.** This
session added four measurements (1a, 1d, R3, and the 1d confident-tail follow-up), all consistent
with that. **One open thread remains: R1**, built and tested, waiting to be run on Colab.

| step | what | result | doc |
|---|---|---|---|
| read | all 25 analysis docs in `runs/` | ideas list written | `next_signal_ideas.md` |
| **1a** | breakout (OCO stops) on volatility-detector triggers, `run009f_scores.npz` | **FALSIFIED** — continuation 0.516, net −6.30 bp [−26.7, +15.0], 72.8th pct of day-matched null; 0 of 55 cells with n ≥ 100 net-positive | `breakout_probe.analysis.md` |
| **1d** | v1 year (331 d, 15 s), stage 1 model-free RV, stage 2 xgboost trained only on big-move bars, monthly walk-forward, Colab GPU | **FAILED** pre-registered K1/K3/K4 — AUC 0.584 (null p97.5 0.512, 8/9 months > 0.55), trades 50.6 % acc, +0.64 bp gross | `v1_stage2_probe.analysis.md` |
| 1d follow-up | confident tail, causal threshold, one position, entry delay | top 1 %: **+11.14 bp** gross @ 0 s delay (71.8 % acc, 7/7 months, both legs +), **+3.40** @ 15 s, **+0.11** @ 30 s | same, §3 |
| **R3** | re-price every measured signal at reachable fees | **fees are not the lever** — reachable tier VIP0 + BNB = 9.0 bp taker RT; 1d tail @ 0 s = +2.14 net, CI lo 0.1 bp short | `fee_reprice.analysis.md` |
| **R1** | latency decay of the 1d tail on the 60-day 5 s set, 0/5/10/15/30 s delays | **built + tested, NOT yet run** | notebook header |

## R1 — what to do next

1. Upload `data/w5_60d.parquet` (132 MB) to `MyDrive/w5_60d.parquet` (`v1_15s.parquet` is already there).
2. Colab → `runs/btc_latency_decay_probe.ipynb` → Runtime GPU → Run all. Outputs to
   `MyDrive/btc_latency_decay_probe/` (`latency_decay_results.json`, `latency_decay_scores.npz`).
3. Pre-registered pass: primary arm `v1x` (1d model trained on the whole v1 year, scored on 5 s bars
   2026-08-16 → 09-20, fully out of sample), top 1 % or top 2 % tail, **gross at 5 s delay with
   day-bootstrap CI lower bound > 9.0 bp, n ≥ 100**. If 5 s fails but the linear 1 s estimate ≥ 9 bp →
   "EVENT-STREAM CHECK" (only sub-second event data can settle it).
4. Power caveat: ~32 OOS days → n ≈ 100–300 per cell.

**Decision point (from `next_signal_ideas.md`):** if R1 fails, stop BTC direction work and move to
R4 (cross-sectional multi-asset) or R5 (funding carry) — different return sources.

## Key numbers to carry forward

- Reachable fees: VIP0 2.0 / 5.0 bp per side; with BNB 1.8 / 4.5 → RT **9.0** taker-taker, **6.3**
  taker-in/maker-out, **3.6** maker-maker. Higher tiers need tens of M USD / month volume (secondary
  sources disagree on the middle tiers; official table needs login).
- A trailing 5-min realised-vol score gives ≥ the LSTM detector's |move| lift at 1 h (2.33× vs 2.05×),
  so vol-conditioned ideas can run on the 7-yr klines without the LSTM.
- Breakouts after RV *spikes* tended to reverse in 1a (n ≈ 30) → idea R2 (fade on 7-yr klines).
- In 1d, training stage 2 on **all** big-move bars (`evt`) beat training on stage-1 triggers (`trig`):
  0.584 vs 0.542; `trig` heads often early-stop at 0–5 rounds.
- 1d AUC 0.58 buys only 50.6 % trade accuracy because AUC is on the 22.7 % of triggers that touched a
  barrier; trades are on all triggers.

## Artifacts created this session

| path | what |
|---|---|
| `runs/next_signal_ideas.md` | idea list, Round 1 + Round 2 (R1–R8), status, decision point |
| `runs/breakout_probe.analysis.md` · `harness_breakout/` | 1a probe + 8 synthetic tests + results json |
| `runs/btc_v1_stage2_probe.ipynb` | 1d notebook, **executed on Colab** (outputs kept) |
| `runs/v1_stage2_results.json` · `runs/v1_stage2_scores.npz` | 1d artifacts (downloaded from Drive) |
| `runs/v1_stage2_probe.analysis.md` | 1d write-up |
| `runs/harness_v1_stage2/` | `extract_v1_15s.py` (now also `--tar/--member`), `build_notebook.py` (refuses to overwrite an executed notebook), `test_notebook.py`, `confidence_check.py` |
| `data/v1_year/v1_15s.parquet` | v1 year, 1,844,323 rows × 50 cols, 231 MB (1 file skipped: first file, older schema) |
| `data/w5_60d.parquet` | 60-day 5 s set, 1,186,560 rows × 50 cols, 132 MB, 68.8 d |
| `runs/fee_reprice.analysis.md` · `harness_fees/` | R3 script + results json |
| `runs/btc_latency_decay_probe.ipynb` | R1 notebook, **unexecuted** |
| `runs/harness_latency/` | R1 `build_notebook.py`, `test_notebook.py` |

Nothing committed to git yet.

## Environment notes

- Local interpreter: `/home/nkout/projects/binance2/binance2/.venv/bin/python` (no xgboost / pyarrow).
  This session installed `xgboost 3.4.1`, `pyarrow`, `nbformat`, `nbclient`, `ipykernel` into the
  **session scratchpad** (`PYTHONPATH=<scratchpad>/pylib`) — ephemeral; reinstall with
  `pip install --target <dir> xgboost pyarrow nbformat nbclient ipykernel` if needed.
- A Jupyter kernel spec named `python3` was registered under `~/.local/share/jupyter/kernels/` pointing
  at the binance2 venv, for local nbclient execution.
- The xgboost "mismatched devices" warning on Colab is harmless (numpy input → CPU prediction).

## Process lessons (this session)

1. The **equivalence test** earned its keep: the bar-agnostic R1 feature code differed from the 1d code
   in one feature (`vol_z`, float32 vs float64, 2.6e-7). Fixed so `v1x` is provably the 1d model.
2. The **delay test** was what separated a real-looking tail (+11 bp, 7/7 months, both legs) from a
   tradeable one. Every future signal gets an entry-delay row by default.
3. A test that rebuilds a notebook can silently wipe executed outputs — builders now refuse to
   overwrite an executed notebook, and tests build into a temp path.
