# v1-year two-stage probe — FAILED as pre-registered; a real high-confidence direction tail exists but lives inside the first 15 s

*2026-09-22. Notebook `runs/btc_v1_stage2_probe.ipynb` (executed on Colab T4, cuda, 11/11 cells
clean, ~2 min training + permutation null). Artifacts `runs/v1_stage2_results.json`,
`runs/v1_stage2_scores.npz`. Follow-up controls: `runs/harness_v1_stage2/confidence_check.py`.*

**TLDR.** The pre-registered primary cell fails K1, K3 and K4. Stage 2, trained only on big-move bars,
has a **real** direction signal — pooled AUC **0.584** against a label-permutation null p97.5 of
**0.512**, above 0.55 in 8 of 9 months — but it is far below the 0.65 bar, and on all stage-1 triggers
it buys **50.6 %** accuracy and **+0.64 bp** gross against an **81 %** / 7 bp requirement.

The exploratory finding worth keeping: the model's **most confident 1 %** of triggers, traded causally
and one position at a time, hit **71.8 %** and **+11.1 bp gross** — balanced legs, 7/7 months positive,
103 distinct days. **But the whole edge is in the first 15-second bar after the signal:** delay entry
by one bar and it falls to **+3.4 bp**; by two bars, **+0.1 bp**. Even at zero delay it does not clear a
10 bp taker round trip (net **+1.14**, CI **[−1.09, +3.79]**). This is the project's known 30-second
microstructure signal, found again through a different route, with the same fee problem.

---

## 1. Run validity

| check | result |
|---|---|
| data | 1,844,323 rows → 1,928,446-slot 15 s grid, 95.6 % present, 2025-09-14 → 2026-08-15 |
| features | 60 (funding / premium / liquidations and time-of-day off by design) |
| test months | 9: 2512 → 2608, expanding train, 10-day val, 80-bar purge |
| labels | ±20 bp in 90 s touched on 3.76 % of bars → 65,947 big-move bars (arm `evt` pool) |
| stage 1 @ 5 % | realised 5.86 %, P(touch) 22.4 % vs 3.76 % base, E\|move\| 11.5 vs 5.0 bp (**2.30× lift**) |
| stage-1 direction leak | P(up \| touched, trigger) = **0.499** — stage 1 carries no direction, as intended |

The xgboost "mismatched devices" warning is harmless (prediction falls back to CPU for the numpy input).

## 2. Pre-registered verdict — primary `evt`, 90 s / 20 bp, rate 5 %

| | value | criterion | |
|---|---|---|---|
| pooled AUC on touched triggers | **0.5836** (n 17,207) | ≥ 0.65 and ≥ 75 % of months | **FAIL** (2/9 months) |
| label-permutation null | mean 0.496, p97.5 0.512 | AUC > p97.5 | PASS |
| one-position trades | 12,863 · acc **0.506** vs best-constant 0.502, required@7 bp **0.809** | acc > both | **FAIL** |
| net @ 7 bp | **−6.36** [−6.64, −6.09] | CI lo > 0 | **FAIL** |

Per month: 0.591 · 0.565 · 0.550 · 0.592 · 0.658 · 0.669 · 0.594 · 0.624 · **0.488** (2608).

**Arm comparison.** `evt` (train on all big-move bars) beats `trig` (train on triggers only) everywhere:
0.584 vs 0.542. The `trig` heads often early-stop at 0–5 rounds — the same collapse as the LSTM's
epoch 1 and the GBM probe. Training on *every* big move, as proposed, is the better design. The
5-min / 30 bp horizon is weaker in both arms (0.51–0.55).

**Why AUC 0.58 buys 50.6 %.** AUC is measured on the 22.7 % of triggers where a barrier was actually
touched (first-touch accuracy there: 55.2 %). The trades are taken on **all** triggers, and on the
other 77 % the 90 s move is small and unpredicted. Magnitude selection and direction do not combine.

**Top features** (gain): `rpos_240` (position in the 1 h range), `ret_20`, `basis_z_960`, `cimb_20`,
`sig_bp`, `basis_d240`, `dimb_0.0_close` (top-of-book imbalance at bar close) — short-horizon
price-shape, trade-count imbalance, basis and top-of-book. No regime or time proxies (they were
excluded), so this signal is not the run.011 artifact.

## 3. The confidence tail — exploratory, with the project's controls

Selecting on |p − 0.5| inside the stage-1 triggers produced a steep gradient (overlapping bars,
test-set quantiles): top 1 % hold accuracy 0.762, gross +11.8 bp. Re-tested with a **causal threshold**
(quantile from the previous test month), **one position at a time**, **entry delay** 0/1/2 bars and a
day-clustered CI:

| tail | delay | n | days | acc | gross | net @ 10 | 95 % CI | long / short | months > 0 |
|---|---|---|---|---|---|---|---|---|---|
| top 1 % | **0** | 387 | 103 | **0.718** | **+11.14** | +1.14 | [−1.09, +3.79] | +12.7 / +8.6 | **7/7** |
| top 1 % | 1 (15 s) | 368 | 103 | 0.535 | +3.40 | −6.60 | [−9.08, −3.64] | +3.8 / +2.7 | 6/7 |
| top 1 % | 2 (30 s) | 350 | 103 | 0.469 | +0.11 | −9.89 | [−12.3, −7.4] | +0.9 / −1.3 | 2/7 |
| top 2 % | 0 | 610 | 129 | 0.664 | +9.07 | −0.93 | [−2.93, +1.17] | +10.9 / +6.4 | 7/8 |
| top 5 % | 0 | 1,625 | 172 | 0.594 | +4.46 | −5.54 | [−6.67, −4.42] | +5.8 / +3.2 | 8/8 |
| top 10 % | 0 | 3,015 | 182 | 0.553 | +2.89 | −7.11 | [−7.84, −6.36] | +3.5 / +2.4 | 7/8 |

What passes: both legs positive, stable across months, spread over ~100 days (max single-day share
4 %), and well above the blind benchmark (+4.6 bp on the top-1 % set). This is the first time in the
project that a direction tail on big-move bars survives these controls across nine months.

What kills it:
1. **It decays within one bar.** +11.1 → +3.4 → +0.1 bp at 0 / 15 / 30 s delay. The model predicts the
   next few seconds after bar close, which is the 30 s microstructure signal the 5 s runs found
   (IC halves every ~30 s, `run007.analysis.md`).
2. **Even zero delay misses taker fees.** Net +1.14 bp, CI includes 0, on 387 trades over 11 months
   (~1.2 trades/day). At 7 bp (taker in, maker out) it would be about +4 bp — but a maker exit on a 90 s
   hold is not guaranteed, and delay 0 is itself the optimistic bound.
3. **The top-1 % cut is the best of four exploratory tails** and was noticed after the run.

## 4. What this settles

- **The two-stage idea, as proposed, is closed on the v1 features.** Direction on big moves is
  learnable (AUC 0.58, stationary), but not at tradeable strength. More data made the signal
  *measurable*; it did not make it large.
- **Training on all big-move bars (`evt`) is better than training on triggers (`trig`)** — keep that
  design if the idea is ever reused on richer features.
- **The only open thread is latency.** Whether any of the +11 bp survives depends on how much of it
  accrues in the first 0.1–2 s versus the full 15 s. The 15 s grid cannot answer that. The 60-day
  **5 s** set and the v5 **event stream** can: re-score the same model design there and measure gross
  at 0 / 1 / 2 / 5 / 10 / 15 s delay. Kill if gross at a realistic 1–2 s delay is below +10 bp.
  Prior: low, because the zero-delay upper bound already fails taker fees.

## Reproduce

```
cd runs
PYTHONPATH=<dir with pyarrow> python harness_v1_stage2/confidence_check.py   # §3, ~1 min, CPU
```
