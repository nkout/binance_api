# R3 — fee re-pricing: fees are not the lever; latency is the only thing standing between the 1d tail and a pass

*2026-09-22. Script `runs/harness_fees/fee_reprice.py` (CPU, ~1 min), results
`runs/harness_fees/fee_reprice_results.json`. Plan: `next_signal_ideas.md` Round 2, R3.*

**TLDR.** The fee tier you can actually reach is set by the strategy's own volume. ~1–25 trades a day
is far below the tens of millions of USD per month the higher VIP tiers need, so the reachable tier is
**VIP0 + BNB: 9.0 bp taker round trip, 6.3 bp taker-in/maker-out, 3.6 bp maker-maker.** At that tier
**no signal measured in this project is significantly net-positive.** The closest is the 1d confident
tail at **zero** entry delay: +11.14 bp gross, CI [+8.90, +13.81], i.e. +2.14 bp net with a CI lower
bound 0.1 bp short of the fee. At a 15 s delay it is −5.60 bp net at VIP0 + BNB. Fees move nothing by
more than ~1 bp; **the decision rests on R1** (how much of the tail survives 0–5 s of latency).

## 1. Fee tiers used

| tier | maker / taker per side | taker-taker RT | taker-in / maker-out | maker-maker | source |
|---|---|---|---|---|---|
| VIP0 | 2.0 / 5.0 bp | 10.0 | 7.0 | 4.0 | notebooks `TAKER_FEE`/`MAKER_FEE`, public schedule |
| **VIP0 + BNB (−10 %)** | 1.8 / 4.5 | **9.0** | 6.3 | 3.6 | BNB futures discount, public |
| VIP4 (illustrative) | 1.0 / 3.0 | 6.0 | 4.0 | 2.0 | secondary sources, unverified |
| VIP9 (illustrative) | 0.0 / 1.7 | 3.4 | 1.7 | 0.0 | secondary sources, unverified |

The official fee table needs a login; secondary sources disagree on the middle tiers. Everything
below is also given as a **break-even round trip**, which needs no tier assumption.

## 2. Results

**A. 5 s netted book** (`horizon_economics…` §6): break-even **0.214 bp one-way** vs 1.8 bp VIP0 + BNB
maker → **8.4× short** (was 9.4× at VIP0). Only a 0 % maker tier clears it arithmetically, and
maker-maker at 5 s runs into the adverse selection measured in runs 008–011.

**B. 1d confident tail** (taker both legs — the entry must be immediate and a 90 s hold exits at market):

| cell | n | gross (= break-even RT) | 95 % CI | net VIP0 | net VIP0 + BNB | net VIP4* |
|---|---|---|---|---|---|---|
| top 1 %, 0 s | 387 | **+11.14** | [+8.90, +13.81] | +1.14 | **+2.14** (CI lo −0.10) | +5.14 ✓ |
| top 1 %, 15 s | 368 | +3.40 | [+0.82, +6.24] | −6.60 | −5.60 | −2.60 |
| top 2 %, 0 s | 610 | +9.07 | [+7.07, +11.28] | −0.93 | +0.07 | +3.07 ✓ |
| top 2 %, 15 s | 587 | +2.55 | [+0.77, +4.46] | −7.45 | −6.45 | −3.45 |
| top 5 %, 0 s | 1,625 | +4.46 | [+3.34, +5.66] | −5.54 | −4.54 | −1.54 |

✓ = gross CI lower bound clears that tier's round trip. *VIP4 is not reachable at this volume.

**C. Everything else measured** (gross per trade vs the 9.0 / 3.6 bp reachable floors): 5 s best cell
+8.48 (collapses to +1.31 without fold 3) · 90 s maker filled ≈ 0 · 4 h mean reversion −1.39 ·
breakout +3.70 · 1d all triggers +0.64. None changes sign at any reachable tier.

## 3. What it settles

- **Fees are not the lever.** The reachable discount is 1 bp on a round trip; the gaps are 3–10 bp.
- **The only cell within reach is the 1d tail at zero delay**, and zero delay is an upper bound. So R1 —
  the 0–5 s decay on the 5 s grid (`btc_latency_decay_probe.ipynb`) — decides the thread. Its pass
  line is set from this document: gross at a 5 s delay with CI lower bound > **9.0 bp**.
