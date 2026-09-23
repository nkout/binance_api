# Continuation after a move — a lower trigger does not help, and OI / funding / L-S do not pick the direction

*2026-09-23. Scripts `runs/harness_continuation/continuation_basic.py` (unconditioned, 5 s set) and
`continuation_conditioned.py` (OI / funding / L-S, both datasets). Outputs next to them:
`basic_w5.txt`, `cond_w5.txt|json`, `cond_v1.txt|json`. CPU, ~3 min per dataset. Motivation: the
question "if 20 bp is needed for profit, what does detecting at 10 bp buy — is it 50/50 to reach 20
or return to 0?", then idea 1 / 2 of the big-move brainstorm (OI-quadrant, crowding).*

**TLDR.** It is a coin flip. After price has moved X bp, reaching 2X before returning to the start
happens at exactly the rate a martingale predicts, and gross P&L is **0 ± 1 bp** at every X from 5 to
30 bp, on 68 days of 5 s bars and 330 days of 15 s bars. The apparent 56–60 % hit rate is entry
overshoot, not continuation. Conditioning on OI change during the leg, OI change over the prior hour,
funding crowding or long/short crowding changes nothing: **3 of 90 cells** have a CI above zero
(chance level), the best is +3.2 bp and flips to −5.5 bp on the other dataset. Against a 9.0 bp round
trip, nothing is close.

---

## 1. The question and the arithmetic

Waiting for the first +10 bp and then aiming at +20 bp is a symmetric bet: 10 bp to the target, 10 bp
back to the start. Under a martingale `P(target first) = d_stop / (d_stop + d_target)`, so expected
gross is **0 at any threshold**. With a 9 bp round trip, break-even on a ±10 bp bet needs
`p·10 − (1−p)·10 = 9` → **p = 0.95**. A wider target lowers the required p and the martingale p by
the same amount. The threshold only sets trade frequency and the size of wins and losses; it cannot
create edge. The only question is whether real data deviates from the martingale.

## 2. Design

- **Zigzag:** anchor `A` at a bar close; walk forward until the mid close is ≥ X bp from `A`; enter in
  that direction at that close (or `delay` bars later). Win = close reaches `A ± K·X` before returning
  to `A`; else loss. Cap 30 min (basic) / 60 min (conditioned); timeouts dropped. Re-anchor at the exit.
  One position at a time by construction; no crossing of data gaps.
- **Gross** is measured from the actual entry to the actual exit close, so overshoot on both sides is
  included. Fees are not subtracted in the tables; compare to **9.0 bp** (VIP0 + BNB taker RT,
  `fee_reprice.analysis.md`).
- **Conditions** (causal at the trigger bar): `ΔOI` from anchor to trigger; `ΔOI` over the prior 1 h;
  funding and L/S as trailing 3-day z-scores, signed by the move (`z·side < −1` = move goes **against**
  the crowded / funding-paying side). The OI "strongly up/down" terciles use full-sample quantiles —
  descriptive only.
- **CI:** 95 % day-bootstrap on gross. Long / short legs reported separately (drift check).

## 3. Unconditioned — 5 s set (68 d)

| trigger → target | n | P(target) | gross [95 % CI] | net @ 9 |
|---|---|---|---|---|
| 5 → 10 | 24,775 | 0.608 | +0.20 [+0.11, +0.29] | −8.80 |
| **10 → 20** | 7,818 | **0.565** | **+0.12** [−0.10, +0.33] | **−8.88** |
| 10 → 20, top-10 % trailing RV | 3,353 | 0.604 | +0.49 [+0.12, +0.95] | −8.51 |
| 10 → 20, 5 s delay | 7,722 | 0.565 | −0.10 [−0.28, +0.11] | −9.10 |
| 10 → 30 | 5,158 | 0.384 | −0.32 [−0.80, +0.15] | −9.32 |
| 15 → 30 | 3,637 | 0.553 | +0.37 [−0.12, +0.91] | −8.63 |
| 20 → 40 | 1,959 | 0.543 | +0.13 [−0.71, +1.07] | −8.87 |
| 20 → 60 | 1,231 | 0.328 | −2.84 [−5.08, −1.05] | −11.84 |

**Why 56 % wins and zero P&L.** Entry happens at the first close past +10 bp, typically ~+11.5 bp, so
the target is ~8.5 bp away and the stop ~11.5 bp: a martingale gives ≈ 0.57. The higher hit rate is
paid for with a smaller win. The one positive cell (high RV, +0.49) is carried by the long leg
(+1.13 vs −0.17) — drift, not continuation — and falls to +0.10 with a 5 s delay.

## 4. Conditioned — 90 cells

15 conditions × 3 thresholds (10→20, 20→40, 30→60) × 2 datasets. Selected rows (gross bp; full
output in `harness_continuation/cond_*.txt`):

| condition | v1 10→20 (n≈48k) | v1 30→60 (n≈6.6k) | 5 s 20→40 (n≈2.1k) | 5 s 30→60 (n≈930) |
|---|---|---|---|---|
| all | +0.05 [−0.06, +0.16] | +0.75 [−0.16, +1.63] | +0.78 [−0.09, +1.76] | −0.03 |
| OI rising in leg | +0.19 [+0.01, +0.36] | +0.69 | +0.87 | +0.39 |
| OI falling in leg | −0.08 | +0.79 | +0.64 | −0.48 |
| OI up prior 1 h | +0.11 | +0.56 | +1.27 [+0.16, +2.46] | +0.93 |
| OI down prior 1 h | −0.17 | +0.08 | −0.28 | −2.09 |
| funding: move against crowd | −0.15 | +1.39 | +0.99 | −2.55 |
| funding: move with crowd | −0.17 | +0.41 | +0.16 | −0.12 |
| L/S: move against crowd | +0.15 | +1.04 | +0.82 | +1.56 |
| L/S: move with crowd | −0.09 | −0.35 | +0.20 | −4.31 |
| OI rising + against funding crowd | +0.06 | **+3.23 [+0.29, +5.95]** (n 448) | +1.31 | −5.48 (n 72) |

- **OI quadrant (price + OI up = new positions → continue; OI down = covering → fade): not there.**
  OI-up minus OI-down is ≤ 0.3 bp on 48k trades and changes sign across thresholds.
- **Crowding: not there.** Neither funding nor L/S crowding separates continuation from reversal.
- **Multiple testing:** 3 of 90 cells have CI lo > 0, about what chance gives at 95 %. The best cell
  (+3.23, v1 30→60, OI rising + against funding crowd) is the maximum of 90 noisy cells, is −5.48 on
  the 5 s set (which overlaps v1 by ~1 month), and is a third of the fee even at face value. Recorded
  so it is not rediscovered as a result.
- The **"excess P" column** in the raw output (`P − martingale P` from the entry distance) is
  negative everywhere because stop-outs also overshoot past the anchor; gross is the clean metric.

## 5. What this settles

- **Lowering the trigger threshold is not a lever.** It changes frequency and payoff size, never the
  sign. This is the martingale result, and the data follow it at 5–30 bp on both datasets.
- **Given the first leg, the rest of the move is a coin flip** on the collector's positioning data
  (OI, funding, L/S). Together with the breakout probe (continuation 0.516,
  `breakout_probe.analysis.md`) and the 1d probe (direction AUC 0.584, untradeable,
  `v1_stage2_probe.analysis.md`), this is the third independent test that direction within big moves
  is not predictable from price, book or positioning features here.
- **Closes** brainstorm ideas 1 (OI / liquidation first-leg quadrant — liquidations were not tested
  because they are dead in the 5 s set; the 216-day v1 window remains untested for that one variable)
  and 2 (crowding) in their cheap form.
- **Still open, cheap:** break-vs-reject at price levels on the 7-year klines (brainstorm idea 3). After
  that, the planned move to R4 (cross-sectional multi-asset) or R5 (funding carry).

## Reproduce

```
cd runs/harness_continuation
PY=/home/nkout/projects/binance2/binance2/.venv/bin/python   # needs pyarrow on PYTHONPATH
$PY continuation_basic.py ../../data/w5_60d.parquet > basic_w5.txt
$PY continuation_conditioned.py ../../data/w5_60d.parquet 5 cond_w5.json > cond_w5.txt
$PY continuation_conditioned.py ../../data/v1_year/v1_15s.parquet 15 cond_v1.json > cond_v1.txt
```
