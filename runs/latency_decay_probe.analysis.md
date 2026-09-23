# R1 latency decay + MLP arm — FAIL: the confident tail is gone 5 s after the signal, and an MLP finds exactly what xgboost finds

*2026-09-23. Notebook `runs/btc_latency_decay_probe.ipynb` (executed on Colab, cuda, 13/13 cells, ~2 min).
Artifacts `runs/latency_decay_results.json`, `runs/latency_decay_scores.npz`, `runs/holdout_c1_scores.npz`.
Pre-registration: `next_signal_ideas.md` Round 2 (R1) and Round 3 (MLP arm).*

**TLDR.**
- **R1 FAILS.** On out-of-sample 5 s bars (2026-08-16 → 09-20), the primary arm `v1x` top 1 % makes
  **+5.3 bp at zero delay and −0.4 bp with a 5 s delay**. Top 2 % goes +3.4 → −0.6. The linear 1 s
  estimates (+4.2 / +2.6 bp) are far below 9 bp, so there is **no event-stream flag**. The accrual curve
  shows why: the favourable move arrives **within the first 5 s** after the signal.
- **The MLP is not better (C1 FAIL).** On a 63-day v1 holdout with 4,738 touched triggers, AUC is
  **xgb 0.5956 vs MLP 0.5958**, difference +0.0001 with CI [−0.014, +0.018]. The confident tails also
  match: +13.8 vs +14.0 bp at zero delay, +4.8 vs +4.2 bp at 15 s. Model class is not the bottleneck.
- **By-product: the 1d tail replicates** on the later holdout on 15 s bars (top 1 %: +13.8 bp,
  78.8 % accuracy at 0 s → +4.8 bp at 15 s). The effect is real, and it decays exactly as in 1d.
- **Per the decision point, BTC direction work stops here.** Next is R4 (cross-sectional) or
  R5 (funding carry).

---

## 1. Run validity

| check | result |
|---|---|
| data | v1 1,844,323 rows (95.6 % of 15 s slots) · 5 s 1,186,560 rows (99.8 %) |
| stage 1 | v1 realised 5.86 %, P(touch \| trigger) 22.4 % · 5 s realised 5.51 %, 15.8 % (5 s window calmer) |
| `v1x` / `v1x_mlp` training | 65,778 big-move bars; **val only 169**: the last 10 v1 days were quiet (see §5) |
| xgb best iterations `v1x` | [180, 9, 4] per seed, noisy because of the tiny val set |
| MLP best epochs | `v1x_mlp` [7, 7, 10] · holdout [3, 9, 2] |
| holdout C1 | train 53,307 big-move bars → test 2026-05-25 → 08-15, 4,738 touched triggers on 63 days |

**Verdict-cell bug.** Cell 12 printed "too few trades" for every cell. The cause is `DEC.tail`, which
pandas resolves to the `DataFrame.tail` method, not the `tail` column. The bug came from the original
R1 builder and was masked in SMOKE runs, where there really were too few trades. It is fixed in
`harness_latency/build_notebook.py` (`DEC['tail']`), with a regression test on known rows (38/38 pass).
The verdict below is recomputed from `latency_decay_results.json`; the outcome is the same (FAIL).

## 2. R1 — pre-registered verdict (primary `v1x`, out-of-sample 5 s)

| tail | delay | n | days | acc | gross | 95 % CI | long / short |
|---|---|---|---|---|---|---|---|
| top 1 % | 0 s | 75 | 20 | 0.653 | **+5.32** | [−4.03, +10.66] | +3.6 / +7.5 |
| top 1 % | **5 s** | 73 | 20 | 0.575 | **−0.39** | [−10.42, +5.02] | −1.6 / +1.3 |
| top 1 % | 15 s | 70 | 20 | 0.486 | −2.34 | [−13.2, +4.1] | |
| top 2 % | 0 s | 123 | 25 | 0.577 | +3.40 | [−2.10, +7.32] | +3.1 / +3.7 |
| top 2 % | **5 s** | 120 | 25 | 0.525 | **−0.57** | [−5.90, +2.85] | −0.6 / −0.5 |
| top 2 % | 15 s | 116 | 25 | 0.474 | −2.20 | [−7.9, +1.5] | |

Pass line: gross at 5 s with CI lower bound > 9.0 bp and n ≥ 100. **FAIL** in both cells.
Linear 1 s estimates are +4.18 (top 1 %) and +2.61 (top 2 %), both < 9, so no event-stream check.

**Accrual curve** (zero-delay `v1x` top-1 % trades, mean signed move since signal close):
+4.9 bp @ 5 s · +7.3 @ 15 s · +8.7 @ 20 s (peak) · ~+6 @ 60–90 s. Most of what the model predicts has
already happened 5 s after the bar closes. A 5 s entry buys after the move and holds a coin flip.

Out-of-sample 5 s AUC on touched triggers (n 8,800): `v1x` 0.560 · `v1x_mlp` 0.534 · `w5` 0.515 ·
`mix` 0.524. The 5 s-only arms (`w5`, `mix`) have too little training data to matter.

## 3. C1 — xgboost vs MLP, identical splits (v1 holdout, 15 s)

| | xgb | MLP | avg |
|---|---|---|---|
| AUC (4,738 touched triggers) | **0.5956** | **0.5958** | 0.5997 |
| MLP − xgb | | +0.0001, CI [−0.0138, +0.0181] | |
| 2605 · 2606 · 2607 · 2608 | 0.645 · 0.584 · 0.638 · 0.536 | 0.664 · 0.587 · 0.638 · 0.622 | |
| top 1 %, 0 s | +13.82 [+9.95, +17.70], n 132, acc 0.788 | +14.03 [+10.26, +17.33] | +14.36 |
| top 1 %, 15 s | +4.79 [+1.14, +8.17] | +4.18 [+0.92, +7.34] | +4.32 |
| top 2 %, 0 s | +8.69 | +9.36 | +9.07 |
| top 2 %, 15 s | +1.84 | +2.75 | +1.44 |

Pre-registered: MLP better iff AUC ≥ 0.62 **and** diff ≥ 0.02 **and** CI lower bound > 0 →
**FAIL on all three.** Two very different learners given the same inputs arrive at the same ~0.60 AUC
and the same tail, which is what an information ceiling looks like. Averaging the two adds only +0.004 AUC.

## 4. The one exploratory curiosity — `v1x_mlp` tail on 5 s

On the 5 s out-of-sample set, the MLP's top 1 % stays positive at every delay: +12.1 · +7.3 · +8.9 ·
+8.4 · +9.4 bp at 0 / 5 / 10 / 15 / 30 s. The 30 s CI is [+3.1, +24.3], both legs are positive, and
the accrual keeps rising to +14 bp at 120 s. **Not a finding:**
- n = 25–29 trades on **10 days**, the best of 80 exploratory cells (4 arms × 4 tails × 5 delays);
- on the same 5 s triggers the MLP's AUC (0.534) is *lower* than xgb's (0.560);
- on the 63-day holdout, with 4–5× the trades, the MLP tail decays exactly like xgb's (+14.0 → +4.2 at 15 s).

Recorded so it is not rediscovered. The only honest check is a rerun on 5 s data collected after
2026-09-20 (`OOS_START` = that date). Pre-register top-2 % gross at 5 s, CI lower bound > 0, n ≥ 100.
Prior: low.

## 5. Caveats (none changes the verdict)

- **Feature transfer 15 s → 5 s grid is imperfect.** On the 33 overlap days, 17 of 60 features
  correlate < 0.8 with their 15 s counterparts, and these include the 1d top features: `dimb_0.0_close`
  0.36, `ret_1` 0.42, `bar_pos` 0.36, `fimb_1` 0.46. This probably explains why `v1x` zero-delay on 5 s
  (+5.3) is below the 15 s holdout (+13.8). But the 15 s holdout and the accrual curve tell the same
  story: the move is front-loaded into the first seconds.
- **`v1x` validation set was 169 bars.** The last 10 v1 days were quiet (08-08 / 08-09 near-flat), so
  early stopping was noisy (xgb seeds stopped at 9 and 4 rounds). The holdout C1 used a normal val window.
- **~32 out-of-sample days,** so the 5 s CIs are wide, as the power caveat anticipated.

## 6. What this settles

1. **R1 closes the last live BTC direction thread.** The confident big-move tail is real and replicates
   (1d: +11.1 bp; holdout: +13.8 bp at 0 s). But it is a first-seconds microstructure move. It is mostly
   done 5 s after the signal and gone at 15–30 s, and it never clears 9 bp with any realistic entry.
2. **Model class is not the lever.** A flat MLP and gradient-boosted trees on the same 60 features are
   indistinguishable (ΔAUC +0.0001). A deeper model on these inputs is closed. A deep net would only be
   worth building on *different raw inputs*, such as the full order-book ladder or the event stream,
   and those carry seconds-scale information, which is the horizon this result just closed.
3. **Decision point reached** (`next_signal_ideas.md`): stop BTC direction work; move to **R4
   cross-sectional multi-asset** or **R5 funding carry**, which are different return sources.

## Reproduce

```
cd runs/harness_latency
PYTHONPATH=<dir with xgboost, torch, pyarrow, nbformat, nbclient> python test_notebook.py   # 38/38 PASS, incl. SMOKE run
python build_notebook.py   # refuses to overwrite the executed notebook; build to another path
```
