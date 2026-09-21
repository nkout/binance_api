# run.009b feature-importance + block-control validation

*Validated 2026-09-20. Notebook: `runs/btc_lstm.run.009b.importance.ipynb`. Artifacts: `runs/run009b_importance_{per_feature,by_group,group_control,univariate}.csv`, `run009b_importance.png`.*

**TLDR:** The run is valid and the control did its job. **The `v3` (schema-3/4) block is confirmed special** — permuting 29 `v3` features drops h18 up-head AP by **+0.1360** with an across-repeat sd of just **0.0011** (t = 122), and **0 of 5 random 29-feature blocks** came close (random mean +0.0392, max +0.0946). So the earlier +0.1338 was **not** a block-size artifact. On IC, `v3` carries **97%** of h6 IC and **84%** of h18 IC. Two things were rejected: `v1_base` is **redundant** (−9.9σ *below* its random null), and `new_ctx`'s negative drop is **not significant** (t = −1.78 — the "anti-signal" flagged in the first pass was noise). The genuinely new result is the **negative tail**: **13 of 76 features have significantly negative permutation importance** — permuting them *improves* AP, the signature of harmful reliance. That list includes **`vol_norm`, the single strongest univariate feature (AUC 0.91 on *both* up and dn labels)**. Separately, **5 features are constant zero** in fold 3.

## Run integrity — PASS

| check | result |
|---|---|
| execution | all 21 cells, zero errors, `N_REPEATS=8`, `N_RANDOM_CONTROLS=5` |
| sweep | 76 features × 8 repeats in 14.1 min (T4) |
| baseline | `ic_h6 +0.1258`, `ic_h18 +0.0457`, `ap_up18 +0.1601`, `ap_dn18 +0.1913` — **identical to build 2** |
| eval set | fold 3 test, 20,000 of 164,671 windows, `lup_18` 0.98% / `ldn_18` 1.00% |
| pipeline | cells 1–9 byte-identical to `btc_lstm.run.009a.ipynb` (verified by the build test) |

The baseline matching build 2 exactly means the changes (`N_REPEATS` 2→8, pooled floor, control) are a **controlled comparison**, not a new window.

## The control — the open question is answered

| block | k | observed ΔAP | ±sd | t | random mean | random sd | random range | ≥obs | z vs null |
|---|---|---|---|---|---|---|---|---|---|
| **v3** | 29 | **+0.1360** | 0.0011 | **+122** | +0.0392 | 0.0361 | [0.0053, 0.0946] | **0/5** | **+2.7σ** |
| v1_base | 35 | +0.0455 | 0.0063 | +7.2 | +0.1291 | 0.0084 | [0.1156, 0.1386] | 5/5 | **−9.9σ** |
| new_ctx | 12 | −0.0214 | 0.0120 | −1.8 | +0.0131 | 0.0233 | [−0.0108, +0.0403] | 5/5 | −1.5σ |

- **`v3` is special.** No random same-size block matched it; it exceeds `random_max` by 44% and sits +2.7σ above the null mean. Its own across-repeat sd is 0.0011 — the drop is extremely stable.
- **`v1_base` is redundant.** Its drop sits *below* all five random blocks and −9.9σ from the null mean. Removing `v1_base` costs far less than removing an arbitrary 35-feature block.
- **`new_ctx` is not distinguishable from its null** (t = −1.78; the null's own sd is 0.0233 and its range straddles zero). The earlier "−0.0228 anti-signal" was noise.

Group IC drops reinforce it: `v3` ΔIC_h6 **+0.1222** (97% of the 0.1258 baseline) and ΔIC_h18 **+0.0383** (84% of 0.0457); `v1_base` +0.0100 / +0.0057; `new_ctx` −0.0033 / +0.0001.

### Control-design caveat

For `v3` the random pool is `v1_base + new_ctx` — the *redundant* blocks — so the null is a fair "uninformative block" baseline and the `v3` conclusion stands on its own. For `v1_base` the pool is `v3 + new_ctx`, i.e. mostly `v3`, so that row largely **re-demonstrates `v3`** rather than being independent evidence. Also, with only 5 draws the minimum attainable p-value is 1/6 ≈ 0.167 — the control is suggestive, and the conclusion rests on the control **combined with** `v3`'s 122σ stability. Raising `N_RANDOM_CONTROLS` to ~20 would tighten it.

## Per-feature — the pooled floor removed the inflation

Pooled floor = 2.0 × median across-repeat sd = 2.0 × 0.0016 = **0.0033**. **15/76** features clear it, versus 24/76 under the old per-feature-sd threshold — the inflation predicted in the first validation. Of those 15, **11** also have |t| > 2.37 (p < 0.05, 7 dof); `near_funding` (t=2.0), `range_pos_24h` (1.7), `sweep_buy_rel` (1.9) and `sell_accel` (1.8) do not.

| # | feature | block | ΔAP up18 | t |
|---|---|---|---|---|
| 0 | `lsr_z` | v3 | +0.0158 | +6.9 |
| 1 | `ma_gap_24h` | new_ctx | +0.0149 | +14.3 |
| 2 | `spread_expansion` | v1_base | +0.0093 | +8.9 |
| 3 | `mid_flips_norm` | v3 | +0.0092 | +4.4 |
| 4 | `sweep_sell_rel` | v3 | +0.0065 | +3.4 |
| 5 | `batch_buy_rel` | v3 | +0.0065 | +8.4 |
| 6 | `funding_pressure` | v1_base | +0.0064 | +2.6 |
| 7 | `near_funding` | v1_base | +0.0054 | +2.0 |
| 8 | `vol_ratio_1h_24h` | new_ctx | +0.0052 | +2.4 |
| 9 | `mid_rv_norm` | v3 | +0.0051 | +2.4 |
| 10 | `buy_tail_ratio` | v3 | +0.0050 | +3.0 |
| 11 | `range_pos_24h` | new_ctx | +0.0047 | +1.7 |
| 12 | `momentum` | v1_base | +0.0043 | +7.8 |
| 13 | `sweep_buy_rel` | v3 | +0.0035 | +1.9 |
| 14 | `sell_accel` | v1_base | +0.0034 | +1.8 |

Block composition: **7 v3, 5 v1_base, 3 new_ctx**. Note this cuts against the grouped result — `new_ctx` has the highest above-floor *hit rate* (3/12 = 25%) yet a ~zero grouped drop, while `v1_base` supplies 5 of the top 15 despite being the most redundant block. Per-feature and grouped importance answer different questions; neither is a prune list.

The most important single feature accounts for ~10% of baseline AP (0.0158 / 0.1601). **No single feature drives the model** — importance is diffuse, and the block-level result comes from many individually-weak, mutually-redundant `v3` features.

## New finding — 13 features are significantly *harmful*

Permuting these **improves** AP. A negative permutation drop is a stronger statement than a small positive one: it is not explained by redundancy alone, and is the classic signature of the model relying on a feature in a way that does not generalise — consistent with the run.009a finding that val IC peaks at epoch 1 and decays with training.

| feature | ΔAP up18 | t | block |
|---|---|---|---|
| `minute_sin` | −0.0158 | −3.7 | v1_base |
| **`vol_norm`** | **−0.0157** | **−4.2** | v1_base |
| `ma_gap_4h` | −0.0121 | −5.9 | new_ctx |
| `dow_sin` | −0.0077 | −4.3 | new_ctx |
| `basis_z_4h` | −0.0076 | −2.5 | new_ctx |
| `ma_gap_1h` | −0.0075 | −10.7 | new_ctx |
| `sell_tail_ratio` | −0.0075 | −5.2 | v3 |
| `wall_imbal` | −0.0074 | −3.2 | v3 |
| `largest_trade_rel` | −0.0052 | −2.6 | v1_base |
| `wall_qty_norm` | −0.0044 | −4.1 | v3 |
| `flow_net_widex_z` | −0.0036 | −14.1 | v3 |
| `buy_accel` | −0.0030 | −4.0 | v1_base |
| `minute_cos` | −0.0028 | −2.9 | v1_base |

Three things stand out:

1. **`vol_norm`** is the strongest univariate feature in the entire set (AUC **0.9108 up / 0.9142 dn**) and is **significantly harmful** on permutation. It predicts "a big move is coming" (in either direction) — which is what a first-touch label rewards — but the model's use of it degrades out-of-sample *ranking*. This is the clearest single piece of evidence for harmful reliance in the run.
2. **The `ma_gap_*` family is internally inconsistent**: 24h **+0.0149** (t=+14.3), 4h **−0.0121** (t=−5.9), 1h **−0.0075** (t=−10.7). Same family, opposite signs — instability, not a clean regime signal.
3. `flow_net_widex_z` at **t = −14.1** is the most extreme negative in the set.

Caveat: negative permutation importance can in principle arise from noise injection into a correlated representation, so a single negative value is weak evidence. What makes this list credible is the *count* (13) and the *magnitudes* of the t-statistics (up to 14σ) — that is not a noise pattern.

## Also found — 5 features are constant zero

`liq_flag`, `liq_cnt_log`, `liq_imbal`, `liq_notional_log`, `liq_notional_max_log` all show **ΔAP exactly 0.0000 with sd exactly 0.0000**, univariate **AUC exactly 0.5000** and raw IC exactly 0.0000. That is the signature of a constant column: **no liquidations occurred in the fold-3 eval window**, so these five carry no information there. They passed the coverage guard because the pipeline fills absent liquidations with 0 inside the schema-3+ era — a constant, not a missing value. They should be treated as dead in this window (and are untestable here regardless of their value in a liquidation-heavy regime).

## Caveats

- **Fold 3 only.** The Drive checkpoint holds only the last fold (run.009a cell 15 saves `results[-1]`). Everything here is one regime slice — the most volatile part of the window. Per-fold stability requires `EVAL_FOLD` 0–3 with retraining.
- **20,000-sample subsample** of 164,671 test windows.
- **Permutation importance is correlation-blind.** The below-floor list is descriptive only — the notebook now says so explicitly, and this validation endorses it. Prune with block ablation.
- **Univariate AUC measures volatility, not direction.** Every top-univariate feature scores high on *both* `lup_18` and `ldn_18` (`vol_norm` 0.9108/0.9142; `vol_ratio_1h_24h` 0.8767/0.8781; `mid_rv_norm` 0.8316/0.8050). First-touch labels require a big move either way. It is not directional skill.
- **`N_RANDOM_CONTROLS = 5`** caps the control's resolution at p ≈ 0.167.

## Verdict

| claim | verdict |
|---|---|
| the `v3` block carries most of the model's ranking signal | **confirmed** (0/5 random blocks; +2.7σ; 97% of h6 IC) |
| that result is a block-size artifact | **rejected** |
| `new_ctx` is harmful | **not supported** (t = −1.78) |
| `v1_base` is redundant | **supported**, but not independently of `v3` |
| no single feature dominates | **confirmed** (max ~10% of AP) |
| 13 features are significantly harmful | **new finding**, actionable |
| 5 liquidation features are dead in this window | **confirmed** (constant zero) |

**Bottom line:** the model is essentially a `v3` model. The schema-3/4 block is the signal; `v1_base` is largely redundant with it; `new_ctx` neither helps nor measurably hurts. The most useful new result is not what to *add* but what to *remove* — 13 features whose presence lowers out-of-sample ranking, including the strongest univariate feature in the set.

## Next step

A **block ablation run**: drop the 13 harmful features (or the whole `new_ctx` block) and retrain fold 3 with everything else frozen, then compare test AP/IC against the unchanged baseline (`ap_up18 0.1601`, `ic_h6 0.1258`). This is the prune method the notes recommend, and this run supplies the candidate list. Because the model overfits by epoch 1, a leaner set may also make longer training useful. If the ablation improves AP, it also re-opens the feature-expansion question with a cleaner starting point.

---

# Appendix — full reference tables

## A1. Per-feature permutation importance (all 76, sorted by ΔAP up18)

`t` = ΔAP / across-repeat sd (8 repeats → 7 dof; |t| > 2.37 ≈ p < 0.05). Pooled floor = 0.0033.

| # | feature | block | ΔAP up18 | ±sd | t | ΔAP dn18 | ΔIC h18 | ΔIC h6 |
|---|---|---|---|---|---|---|---|---|
| 0 | `lsr_z` | v3 | +0.0158 | 0.0023 | +6.9 | +0.0124 | +0.0017 | +0.0030 |
| 1 | `ma_gap_24h` | new_ctx | +0.0149 | 0.0010 | +14.3 | −0.0045 | +0.0002 | +0.0063 |
| 2 | `spread_expansion` | v1_base | +0.0093 | 0.0011 | +8.9 | +0.0281 | +0.0002 | +0.0004 |
| 3 | `mid_flips_norm` | v3 | +0.0092 | 0.0021 | +4.4 | +0.0195 | +0.0021 | +0.0018 |
| 4 | `sweep_sell_rel` | v3 | +0.0065 | 0.0019 | +3.4 | +0.0107 | −0.0001 | +0.0013 |
| 5 | `batch_buy_rel` | v3 | +0.0065 | 0.0008 | +8.4 | +0.0019 | +0.0004 | +0.0007 |
| 6 | `funding_pressure` | v1_base | +0.0064 | 0.0025 | +2.6 | +0.0048 | −0.0002 | +0.0005 |
| 7 | `near_funding` | v1_base | +0.0054 | 0.0026 | +2.0 | +0.0148 | +0.0013 | +0.0006 |
| 8 | `vol_ratio_1h_24h` | new_ctx | +0.0052 | 0.0022 | +2.4 | +0.0246 | −0.0022 | −0.0016 |
| 9 | `mid_rv_norm` | v3 | +0.0051 | 0.0021 | +2.4 | +0.0040 | −0.0003 | +0.0006 |
| 10 | `buy_tail_ratio` | v3 | +0.0050 | 0.0017 | +3.0 | +0.0060 | −0.0008 | −0.0014 |
| 11 | `range_pos_24h` | new_ctx | +0.0047 | 0.0028 | +1.7 | +0.0458 | +0.0037 | +0.0001 |
| 12 | `momentum` | v1_base | +0.0043 | 0.0006 | +7.8 | +0.0255 | +0.0026 | +0.0020 |
| 13 | `sweep_buy_rel` | v3 | +0.0035 | 0.0018 | +1.9 | +0.0110 | −0.0005 | +0.0001 |
| 14 | `sell_accel` | v1_base | +0.0034 | 0.0019 | +1.8 | +0.0012 | +0.0002 | +0.0007 |
| 15 | `depl_imbal` | v3 | +0.0027 | 0.0005 | +5.4 | −0.0030 | −0.0007 | −0.0017 |
| 16 | `oi_change` | v1_base | +0.0025 | 0.0007 | +3.6 | −0.0158 | −0.0003 | −0.0000 |
| 17 | `batch_sell_rel` | v3 | +0.0025 | 0.0011 | +2.1 | +0.0033 | −0.0015 | −0.0002 |
| 18 | `micro_dev_z` | v3 | +0.0024 | 0.0024 | +1.0 | −0.0013 | +0.0205 | +0.0467 |
| 19 | `flow_imbal_roll4` | v1_base | +0.0018 | 0.0018 | +1.0 | −0.0030 | +0.0006 | −0.0001 |
| 20 | `largest_trade_side` | v1_base | +0.0018 | 0.0010 | +1.8 | +0.0012 | +0.0002 | +0.0003 |
| 21 | `bid_wall_dist` | v3 | +0.0017 | 0.0036 | +0.5 | +0.0030 | +0.0008 | +0.0000 |
| 22 | `stochastic` | v1_base | +0.0016 | 0.0004 | +3.7 | +0.0023 | +0.0007 | −0.0014 |
| 23 | `trade_side_open` | v1_base | +0.0015 | 0.0005 | +3.1 | +0.0003 | +0.0012 | +0.0012 |
| 24 | `ofi_z` | v3 | +0.0015 | 0.0020 | +0.8 | +0.0009 | +0.0003 | +0.0025 |
| 25 | `flow_agreement` | v1_base | +0.0015 | 0.0018 | +0.8 | −0.0008 | −0.0004 | −0.0002 |
| 26 | `buy_count_accel` | v1_base | +0.0015 | 0.0007 | +2.2 | −0.0053 | +0.0004 | +0.0003 |
| 27 | `late_imbalance` | v1_base | +0.0012 | 0.0005 | +2.2 | +0.0010 | +0.0001 | +0.0015 |
| 28 | `ret_norm_1h` | new_ctx | +0.0012 | 0.0032 | +0.4 | −0.0056 | −0.0021 | +0.0007 |
| 29 | `pull_add_bid` | v3 | +0.0011 | 0.0016 | +0.7 | +0.0044 | +0.0055 | +0.0134 |
| 30 | `book_imbalance` | v1_base | +0.0011 | 0.0022 | +0.5 | +0.0030 | +0.0030 | +0.0039 |
| 31 | `liq_conc_ask` | v1_base | +0.0010 | 0.0007 | +1.5 | +0.0006 | +0.0002 | +0.0006 |
| 32 | `dow_cos` | new_ctx | +0.0008 | 0.0016 | +0.5 | +0.0038 | −0.0011 | −0.0004 |
| 33 | `flow_accel` | v1_base | +0.0007 | 0.0006 | +1.2 | −0.0037 | −0.0003 | +0.0000 |
| 34 | `flow_imbal_roll8` | v1_base | +0.0005 | 0.0010 | +0.5 | +0.0063 | −0.0015 | −0.0012 |
| 35 | `flow_imbalance` | v1_base | +0.0005 | 0.0020 | +0.2 | +0.0009 | +0.0001 | +0.0002 |
| 36 | `sample_imbalance` | v1_base | +0.0004 | 0.0017 | +0.2 | +0.0000 | +0.0007 | −0.0005 |
| 37 | `trade_side_momentum` | v1_base | +0.0003 | 0.0014 | +0.2 | −0.0009 | +0.0001 | +0.0001 |
| 38 | `liq_conc_bid` | v1_base | +0.0003 | 0.0009 | +0.3 | +0.0008 | +0.0001 | −0.0001 |
| 39 | `vwap_spread` | v1_base | +0.0002 | 0.0005 | +0.5 | +0.0003 | −0.0009 | −0.0001 |
| 40 | `liq_imbal` | v3 | +0.0000 | 0.0000 | — | +0.0000 | +0.0000 | +0.0000 |
| 41 | `liq_notional_max_log` | v3 | +0.0000 | 0.0000 | — | +0.0000 | +0.0000 | +0.0000 |
| 42 | `liq_cnt_log` | v3 | +0.0000 | 0.0000 | — | +0.0000 | +0.0000 | +0.0000 |
| 43 | `liq_flag` | v1_base | +0.0000 | 0.0000 | — | +0.0000 | +0.0000 | +0.0000 |
| 44 | `liq_notional_log` | v3 | +0.0000 | 0.0000 | — | +0.0000 | +0.0000 | +0.0000 |
| 45 | `hour_sin` | v1_base | −0.0006 | 0.0016 | −0.4 | +0.0081 | −0.0018 | −0.0014 |
| 46 | `msg_rate_norm` | v3 | −0.0007 | 0.0009 | −0.8 | +0.0007 | +0.0001 | +0.0001 |
| 47 | `flow_net_near_z` | v3 | −0.0007 | 0.0012 | −0.6 | +0.0014 | −0.0004 | +0.0035 |
| 48 | `size_imbalance` | v1_base | −0.0009 | 0.0014 | −0.7 | −0.0007 | −0.0006 | −0.0001 |
| 49 | `basis_mom_1h` | new_ctx | −0.0010 | 0.0024 | −0.4 | +0.0022 | −0.0016 | +0.0007 |
| 50 | `trade_side_close` | v1_base | −0.0011 | 0.0014 | −0.8 | −0.0005 | −0.0002 | −0.0001 |
| 51 | `depl_total_norm` | v3 | −0.0011 | 0.0030 | −0.4 | +0.0016 | −0.0010 | +0.0002 |
| 52 | `ret_norm_4h` | new_ctx | −0.0011 | 0.0014 | −0.8 | −0.0235 | +0.0006 | −0.0001 |
| 53 | `eth_ret_z` | v3 | −0.0016 | 0.0018 | −0.9 | +0.0083 | −0.0009 | +0.0053 |
| 54 | `eth_lead_gap` | v3 | −0.0016 | 0.0022 | −0.7 | −0.0034 | +0.0018 | +0.0037 |
| 55 | `book_imbal_roll4` | v1_base | −0.0023 | 0.0016 | −1.4 | −0.0014 | −0.0008 | +0.0006 |
| 56 | `pull_add_ask` | v3 | −0.0026 | 0.0013 | −2.0 | −0.0019 | +0.0031 | +0.0087 |
| 57 | `book_imbal_deep` | v1_base | −0.0026 | 0.0025 | −1.0 | −0.0005 | +0.0009 | +0.0019 |
| 58 | `minute_cos` | v1_base | −0.0028 | 0.0010 | −2.9 | +0.0108 | −0.0011 | −0.0011 |
| 59 | `cancel_imbal_near` | v3 | −0.0030 | 0.0020 | −1.5 | +0.0056 | +0.0005 | +0.0019 |
| 60 | `buy_accel` | v1_base | −0.0030 | 0.0008 | −4.0 | −0.0045 | −0.0005 | −0.0002 |
| 61 | `flow_net_widex_z` | v3 | −0.0036 | 0.0003 | −14.1 | −0.0013 | +0.0002 | +0.0002 |
| 62 | `hour_cos` | v1_base | −0.0039 | 0.0020 | −2.0 | −0.0026 | +0.0011 | −0.0006 |
| 63 | `wall_qty_norm` | v3 | −0.0044 | 0.0011 | −4.1 | −0.0023 | +0.0007 | −0.0001 |
| 64 | `book_imbal_roll8` | v1_base | −0.0046 | 0.0025 | −1.8 | −0.0006 | −0.0016 | −0.0016 |
| 65 | `ask_wall_dist` | v3 | −0.0047 | 0.0046 | −1.0 | +0.0034 | +0.0002 | +0.0009 |
| 66 | `largest_trade_rel` | v1_base | −0.0052 | 0.0020 | −2.6 | +0.0037 | −0.0005 | +0.0001 |
| 67 | `wall_imbal` | v3 | −0.0074 | 0.0023 | −3.2 | −0.0010 | −0.0001 | −0.0021 |
| 68 | `sell_tail_ratio` | v3 | −0.0075 | 0.0014 | −5.2 | +0.0066 | +0.0002 | +0.0005 |
| 69 | `ma_gap_1h` | new_ctx | −0.0075 | 0.0007 | −10.7 | +0.0188 | +0.0003 | −0.0009 |
| 70 | `basis_z_4h` | new_ctx | −0.0076 | 0.0031 | −2.5 | +0.0041 | +0.0005 | −0.0006 |
| 71 | `dow_sin` | new_ctx | −0.0077 | 0.0018 | −4.3 | +0.0014 | +0.0004 | +0.0003 |
| 72 | `ret_norm_24h` | new_ctx | −0.0102 | 0.0105 | −1.0 | −0.0069 | −0.0007 | −0.0014 |
| 73 | `ma_gap_4h` | new_ctx | −0.0121 | 0.0021 | −5.9 | −0.0045 | +0.0008 | +0.0003 |
| 74 | `vol_norm` | v1_base | −0.0157 | 0.0037 | −4.2 | −0.0215 | −0.0012 | +0.0011 |
| 75 | `minute_sin` | v1_base | −0.0158 | 0.0042 | −3.7 | +0.0132 | −0.0009 | −0.0027 |

Summary: median sd 0.0016 · pooled floor 0.0033 · **15/76 above floor** · **13/76 significantly negative** · **5/76 constant zero**.

## A2. Univariate read (model-free), all 76 sorted by |AUC − 0.5|

In-sample on the fold-3 eval set. Values near 0.5 on *both* sides = no directional information.

| # | feature | \|AUC−.5\| | AUC up | AUC dn | raw IC h18 |
|---|---|---|---|---|---|
| 0 | `vol_norm` | 0.4125 | 0.9108 | 0.9142 | −0.0117 |
| 1 | `vol_ratio_1h_24h` | 0.3774 | 0.8767 | 0.8781 | −0.0033 |
| 2 | `largest_trade_rel` | 0.3370 | 0.1589 | 0.1671 | −0.0003 |
| 3 | `mid_rv_norm` | 0.3183 | 0.8316 | 0.8050 | +0.0072 |
| 4 | `depl_total_norm` | 0.3155 | 0.8195 | 0.8115 | +0.0043 |
| 5 | `sweep_buy_rel` | 0.2999 | 0.1914 | 0.2089 | +0.0005 |
| 6 | `sweep_sell_rel` | 0.2992 | 0.2053 | 0.1963 | −0.0140 |
| 7 | `mid_flips_norm` | 0.2988 | 0.8036 | 0.7940 | +0.0065 |
| 8 | `buy_tail_ratio` | 0.2603 | 0.7967 | 0.7239 | +0.0232 |
| 9 | `sell_tail_ratio` | 0.2520 | 0.7404 | 0.7637 | −0.0099 |
| 10 | `batch_sell_rel` | 0.2445 | 0.2568 | 0.2542 | −0.0130 |
| 11 | `batch_buy_rel` | 0.2398 | 0.2439 | 0.2765 | +0.0016 |
| 12 | `hour_sin` | 0.2377 | 0.2600 | 0.2645 | −0.0117 |
| 13 | `dow_sin` | 0.1667 | 0.6595 | 0.6739 | −0.0049 |
| 14 | `lsr_z` | 0.1426 | 0.3401 | 0.3747 | −0.0113 |
| 15 | `liq_conc_bid` | 0.1072 | 0.4218 | 0.3639 | +0.0501 |
| 16 | `ask_wall_dist` | 0.1040 | 0.5975 | 0.6105 | −0.0043 |
| 17 | `spread_expansion` | 0.0971 | 0.5989 | 0.5952 | +0.0063 |
| 18 | `bid_wall_dist` | 0.0958 | 0.6050 | 0.5866 | −0.0132 |
| 19 | `liq_conc_ask` | 0.0913 | 0.3736 | 0.4438 | −0.0336 |
| 20 | `ret_norm_4h` | 0.0650 | 0.5791 | 0.5509 | +0.0060 |
| 21 | `basis_z_4h` | 0.0645 | 0.5519 | 0.5772 | −0.0232 |
| 22 | `vwap_spread` | 0.0614 | 0.4342 | 0.4430 | −0.0032 |
| 23 | `minute_sin` | 0.0517 | 0.4393 | 0.4573 | −0.0013 |
| 24 | `book_imbal_deep` | 0.0516 | 0.4353 | 0.4615 | +0.0344 |
| 25 | `flow_imbal_roll4` | 0.0511 | 0.5366 | 0.4344 | −0.0146 |
| 26 | `wall_qty_norm` | 0.0488 | 0.5850 | 0.4873 | +0.0084 |
| 27 | `wall_imbal` | 0.0484 | 0.4198 | 0.5167 | +0.0052 |
| 28 | `ret_norm_24h` | 0.0462 | 0.4821 | 0.4255 | +0.0031 |
| 29 | `momentum` | 0.0456 | 0.5353 | 0.4440 | +0.0100 |
| 30 | `msg_rate_norm` | 0.0450 | 0.5621 | 0.5279 | +0.0057 |
| 31 | `buy_accel` | 0.0446 | 0.4797 | 0.4310 | +0.0071 |
| 32 | `range_pos_24h` | 0.0430 | 0.5414 | 0.4555 | +0.0008 |
| 33 | `flow_imbal_roll8` | 0.0418 | 0.5234 | 0.4398 | −0.0118 |
| 34 | `basis_mom_1h` | 0.0413 | 0.5328 | 0.5498 | −0.0139 |
| 35 | `cancel_imbal_near` | 0.0391 | 0.4576 | 0.4643 | −0.0237 |
| 36 | `ma_gap_1h` | 0.0381 | 0.5077 | 0.4315 | −0.0138 |
| 37 | `flow_net_near_z` | 0.0378 | 0.5222 | 0.4466 | +0.0342 |
| 38 | `book_imbal_roll4` | 0.0344 | 0.5217 | 0.4528 | +0.0177 |
| 39 | `book_imbal_roll8` | 0.0334 | 0.5106 | 0.4438 | +0.0177 |
| 40 | `ret_norm_1h` | 0.0333 | 0.5323 | 0.4658 | −0.0183 |
| 41 | `ofi_z` | 0.0309 | 0.5274 | 0.4656 | +0.0287 |
| 42 | `eth_ret_z` | 0.0297 | 0.5141 | 0.4546 | +0.0253 |
| 43 | `ma_gap_24h` | 0.0289 | 0.5303 | 0.4725 | +0.0050 |
| 44 | `book_imbalance` | 0.0288 | 0.5198 | 0.4623 | +0.0518 |
| 45 | `ma_gap_4h` | 0.0278 | 0.5514 | 0.4957 | −0.0043 |
| 46 | `depl_imbal` | 0.0277 | 0.4764 | 0.5319 | −0.0171 |
| 47 | `trade_side_close` | 0.0275 | 0.5286 | 0.4736 | +0.0128 |
| 48 | `flow_accel` | 0.0270 | 0.4815 | 0.4644 | +0.0000 |
| 49 | `minute_cos` | 0.0255 | 0.5145 | 0.4635 | +0.0262 |
| 50 | `oi_change` | 0.0253 | 0.4803 | 0.5309 | −0.0008 |
| 51 | `flow_agreement` | 0.0245 | 0.4723 | 0.4787 | +0.0138 |
| 52 | `trade_side_momentum` | 0.0225 | 0.5182 | 0.4731 | +0.0077 |
| 53 | `pull_add_ask` | 0.0218 | 0.5187 | 0.4752 | +0.0458 |
| 54 | `hour_cos` | 0.0215 | 0.5073 | 0.5357 | −0.0109 |
| 55 | `stochastic` | 0.0205 | 0.5118 | 0.4707 | +0.0157 |
| 56 | `pull_add_bid` | 0.0191 | 0.4975 | 0.5357 | −0.0276 |
| 57 | `late_imbalance` | 0.0178 | 0.5066 | 0.4709 | +0.0153 |
| 58 | `micro_dev_z` | 0.0178 | 0.5227 | 0.4870 | +0.0665 |
| 59 | `sample_imbalance` | 0.0172 | 0.5076 | 0.4731 | +0.0225 |
| 60 | `buy_count_accel` | 0.0172 | 0.4675 | 0.5019 | +0.0043 |
| 61 | `flow_imbalance` | 0.0168 | 0.5180 | 0.4843 | +0.0189 |
| 62 | `sell_accel` | 0.0152 | 0.5204 | 0.5100 | +0.0058 |
| 63 | `largest_trade_side` | 0.0146 | 0.5085 | 0.4793 | +0.0149 |
| 64 | `dow_cos` | 0.0124 | 0.4828 | 0.4923 | −0.0193 |
| 65 | `flow_net_widex_z` | 0.0123 | 0.4837 | 0.5082 | −0.0052 |
| 66 | `size_imbalance` | 0.0115 | 0.5191 | 0.5039 | +0.0019 |
| 67 | `near_funding` | 0.0096 | 0.4879 | 0.4929 | −0.0074 |
| 68 | `funding_pressure` | 0.0096 | 0.4879 | 0.4930 | −0.0072 |
| 69 | `trade_side_open` | 0.0066 | 0.5048 | 0.5084 | +0.0023 |
| 70 | `eth_lead_gap` | 0.0058 | 0.5098 | 0.5018 | +0.0136 |
| 71 | `liq_flag` | 0.0000 | 0.5000 | 0.5000 | +0.0000 |
| 72 | `liq_notional_max_log` | 0.0000 | 0.5000 | 0.5000 | +0.0000 |
| 73 | `liq_cnt_log` | 0.0000 | 0.5000 | 0.5000 | +0.0000 |
| 74 | `liq_imbal` | 0.0000 | 0.5000 | 0.5000 | +0.0000 |
| 75 | `liq_notional_log` | 0.0000 | 0.5000 | 0.5000 | +0.0000 |

Rows 71–75 are the constant-zero liquidation features (AUC exactly 0.5). Note that rows 0–12 all score high on **both** sides — the volatility factor, not direction. Rows 2, 5, 6, 10, 11, 12, 14 are *below* 0.5 on both sides (high value = low move probability) — the same factor inverted.

## A3. Group drops

| group | n | ΔIC h6 | ΔIC h18 | ΔAP up18 | ΔAP dn18 |
|---|---|---|---|---|---|
| v1_base | 35 | +0.0100 | +0.0057 | +0.0455 | +0.0858 |
| new_ctx | 12 | −0.0033 | +0.0001 | −0.0214 | +0.0213 |
| v3 | 29 | +0.1222 | +0.0383 | +0.1360 | +0.1351 |

## A4. Group across-repeat sd (8 repeats)

| group | sd IC h6 | sd IC h18 | sd AP up18 | sd AP dn18 |
|---|---|---|---|---|
| v1_base | 0.0033 | 0.0036 | 0.0063 | 0.0122 |
| new_ctx | 0.0026 | 0.0030 | 0.0120 | 0.0140 |
| v3 | 0.0055 | 0.0086 | 0.0011 | 0.0065 |

`v3`'s ΔAP up18 sd (0.0011) is the smallest of the three despite `v3` having the largest drop — the opposite of a size effect.

## A5. Control — random matched-size blocks (5 draws each)

| group | k | observed | random mean | ±sd | random range | #random ≥ obs | z vs null |
|---|---|---|---|---|---|---|---|
| v1_base | 35 | +0.0455 | +0.1291 | 0.0084 | [+0.1156, +0.1386] | 5/5 | −9.9σ |
| new_ctx | 12 | −0.0214 | +0.0131 | 0.0233 | [−0.0108, +0.0403] | 5/5 | −1.5σ |
| v3 | 29 | +0.1360 | +0.0392 | 0.0361 | [+0.0053, +0.0946] | 0/5 | +2.7σ |

## A6. Baseline (fold-3 test, 20k subsample)

| metric | value |
|---|---|
| `ic_h6` | +0.1258 |
| `ic_h18` | +0.0457 |
| `ap_up18` | +0.1601 |
| `ap_dn18` | +0.1913 |
| label rates | `lup_18` 0.98% · `ldn_18` 1.00% |
| predict time | 1.7 s (3 seeds) |

## How to read this

- **ΔAP** = drop in h18 up-head average precision when the feature (or block) is permuted across samples. Positive = the model uses it. Negative = using it *hurts* out-of-sample ranking.
- **±sd / t** = spread across the 8 permutation repeats and the ratio. 8 repeats → 7 dof, so |t| > 2.37 ≈ p < 0.05. This is the honest per-feature uncertainty; the earlier run's 2 repeats gave a 1-dof sd.
- **Pooled floor** = 2 × median sd across all features (0.0033). Replaces the earlier per-feature-sd threshold, which flagged any feature whose two draws happened to be close.
- **Correlation-blind** — permutation importance under-rates a feature whose information is duplicated elsewhere. The below-floor list is descriptive, never a prune list. `vol_norm` is the proof: strongest univariate feature in the set, below the floor.
- **The control** is the null for a block drop: permuting k features costs k inputs' worth of information whatever they are, so a block is only special if it beats random same-size blocks.
- **Constant-zero rows** (ΔAP 0.0000, sd 0.0000, AUC 0.5000) are features with a single value in this window — no information, not "unimportant".

(End of file)
