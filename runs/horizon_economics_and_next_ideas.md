# Horizon economics — why nothing was profitable at any horizon tried

*Terminal document for the `btc_lstm` programme. Start at §8 (closing summary).*

*Written 2026-09-22 after reading the full `runs/` doc set: `no_profit_root_cause.md`,
`SESSION_NOTES_2026-09-21.md`, `ALL_RUNS_ANALYSIS.md`, `economics_and_metrics.md`,
`run009a/009b/009d/009e/009f` analyses, `run009d.offline.md`, `btc_lstm.run.012.md`,
`features.explanation.run.013.md`, plus the `btc_lstm.run.009f.ipynb` config and the
`run009f_scores.npz` schema. §1–§5 contain no new computation — every number there is either
quoted from those docs or derived from them by stated arithmetic. §6 and §7 are new
measurements, executed 2026-09-22 on CPU from `run009f_scores.npz`; reproduction code for
§6 is in the appendix. §7 ends in a built, tested notebook: `btc_lstm.probe.gbm.ipynb`.*

**TLDR:** the existing root cause ("the heads found volatility, not direction") is correct
but stops one step short. The binding constraint is not the model, the labels, the
estimator or the execution layer — it is that **the horizon was chosen where the fee is
2.5× the average move**. At 90 s, net-positive taker trading requires a directional
accuracy of **174 %**; the model has **~52–53 %**. Every other finding in the project —
the volatility artifact, adverse selection, `S ≤ 0` — is downstream of that one fact.

**The cost-curve measurement proposed in §4.1 has now been run (§6).** It settles the
question with a single number: the signal's **break-even one-way fee is `c* = 0.214 bp`**
(day-bootstrap CI `[+0.167, +0.256]`, `t = +7.8`). Binance VIP0 maker is **2.0 bp** — the
signal is **9.4× too weak**, and **23× too weak** for taker. Two results make this final:
turnover reduction does **not** help (the information decays faster than the turnover falls,
so `c*` *drops* under smoothing), and **even a perfect next-bar direction oracle only reaches
`c* = 1.15 bp` — below the maker fee.** At a 5 s cadence, perfect foresight does not pay
Binance's fees.

**§7's probe has now been run and is FALSIFIED (§7.8).** No long-horizon directional signal
exists in the 76 features: pooled IC `+0.005 / −0.009 / +0.001 / −0.007` at 15 min and 1 h,
daily-IC t never reaching 3, direction heads early-stopping at 0–14 rounds. Its criterion-B
"passes" were **drift** — on the trigger sets `P(up)` is 0.347 (15 min) and **0.873** (1 h),
and always-long beats the model in **7 of 8 cells**. Full write-up: `gbm_probe.analysis.md`.

**The 4 h branch is also closed (`v1_4h_feasibility.analysis.md` §9).** A genuine, stationary
rank signal there (`range_pos_24h` IC −0.34 in both train and test) yields **−1.39 bp gross
over 1,625 trades / 7 years** — win rate 54.9 %, median +11.47 bp, mean negative. **§8 is the
closing summary; read it first.**

---

## 1. The arithmetic that settles it

For a hold-to-horizon trade with symmetric payoff, expected net = `(2p − 1)·E|move| − F`,
where `p` is directional accuracy and `F` the round-trip fee. So the accuracy required to
break even is

```
p_required = (1 + F / E|move|) / 2
```

Anchor `E|move|` on the project's own measured number — **mean |move| @ h18 (90 s) =
4.03 bp** (`no_profit_root_cause.md` §1, reproduced from `run009f_scores.npz`) — and scale
by √t. The scaling is self-checking: it lands at ~125 bp for one day, i.e. ~1.6 % daily
vol, which is right for BTC.

| horizon | E\|move\| | p @ taker 10 bp | p @ mixed 7 bp | p @ full-maker 4 bp |
|---|---|---|---|---|
| 30 s | 2.3 bp | **2.15 → impossible** | impossible | impossible |
| 90 s *(the project's horizon)* | 4.0 bp | **1.74 → impossible** | 1.37 → impossible | 0.99 |
| 5 min | 7.4 bp | 1.18 → impossible | 0.97 | 0.77 |
| 15 min | 12.7 bp | 0.89 | 0.77 | 0.66 |
| 1 h | 25.5 bp | 0.70 | 0.64 | 0.58 |
| 4 h | 51 bp | 0.60 | 0.57 | 0.54 |
| 1 day | 125 bp | 0.54 | 0.53 | 0.52 |

**What the model actually has.** A Spearman IC of ρ buys directional accuracy
`≈ 0.5 + ρ/π`. At the project's pooled IC of +0.06 … +0.11 that is **51.9 % … 53.5 %**.
On the triggered bars it measured **43.2 % / 44.9 %** (`no_profit_root_cause.md` §2) —
i.e. somewhere between "nothing" and "noise around 52 %".

**A 52–53 % signal is monetizable only at daily horizons.** It was being traded at 90 s.

### Selection helps, but not enough

Trigger selection multiplies `E|move|` by ~3.3× (4.03 bp → 13.2 bp on up18 triggers). That
moves 90 s from:

| route | p_required on a 13.2 bp selected bar |
|---|---|
| taker 10 bp | 0.88 |
| realistic mixed 7 bp | 0.77 |
| full-maker 4 bp | 0.65 |

Which is why the maker route was the only door — and why it shut. Note also that
**full-maker 4 bp is not reachable while a stop-loss exists**: the sim correctly pays taker
on every SL and time-stop (`btc_lstm.run.009f.ipynb` cell 16:58–65), so the honest round
trip is 7–9 bp and the honest requirement is ~0.80, not 0.65.

### Restating the "4–7× gap"

`economics_and_metrics.md` frames the shortfall as concentration 17.9× vs 114–136×
required — "a gap of 4–7×". That understates it. The gap is not a factor on a
concentration statistic that more data or a better model could close; it is a required
accuracy above 100 %. **No achievable model closes it at this horizon.**

## 2. The three failures are one failure

1. **Fee/move ratio.** §1. At 90 s the fee *is* the return distribution.

2. **The label is a volatility detector by construction.** `THETA_BY_H` is a *fixed*
   15/20 bp (cell 4). A fixed bp barrier asks "did a big move happen", so `lup/ldn` is
   mechanically a |move| label with a sign attached. The heads learned the learnable part
   (magnitude: 3.3–5.3× |move| lift, 23.9× *opposite*-label lift) and ignored the part that
   isn't. Correctly diagnosed in `run009d.offline.md` §9 — but see §3 for what was done
   about it.

3. **The triggers maximise adverse selection.** This connection is not made anywhere in the
   doc set. The strategy posts a resting limit **on the top-0.1 % highest-volatility bars in
   the sample** — precisely the bars on which a passive quote gets run over.
   `hit_f 4.4 % ≪ hit_m 18.0 %` is therefore **not** a generic structural identity of maker
   entry; it is the specific consequence of letting a volatility detector choose the entry
   timing. A market maker uses that signal to *pull* quotes; this strategy used it to
   *post* them.

So (2) guarantees high-vol bar selection, (3) converts that into a ~5 bp execution tax, and
(1) means there was never enough move to pay for either.

## 3. Corrections / pushback on the existing write-ups

- **"`S` is significantly negative" is fragile, and the framing is mildly dangerous.**
  `run009f.analysis.md` reports `S = −3.79 [−7.22, −0.42]`; the independent re-derivation in
  `no_profit_root_cause.md` got `[−8.07, +0.47]`, which includes zero. The defensible claim
  is **`S ≈ 0`**, which is exactly what the volatility-artifact story predicts. Worth stating
  explicitly, because "significantly negative" eventually invites someone to invert the
  signal — and at −4 bp against a 10 bp floor, with fold-level signs that flip, there is
  nothing there.

- **"AP > DE > spread, monotone" is confounded.** 009f's own caveat #1 concedes it: the
  spread selector also produced the weakest model (IC +0.0602 vs 009d's +0.1093). "Worse
  objective" and "worse model" are not separable in that comparison. It does not change the
  verdict, but it should not be carried forward as a settled cross-run finding.

- **The vol-scaled barrier was recommended twice and never built.** `run009d.offline.md` §7
  item 3 and §10 item 2 both say: normalise the label by realised volatility. Runs 009e and
  009f then changed the *selector* instead — twice. The regression target `y_h` **is**
  vol-normalised (`prepare()`: `delta / rolling std`); the classification heads that drive
  every trigger, sim and gate number are **not**. If the direction line continues at all,
  this is the untested lever — not a fourth selection objective.

- **No run has ever optimised, or selected on, expected net P&L.** Huber on vol-norm
  returns, BCE on barriers, then AP / DE / spread for checkpointing. Cost never enters the
  objective, so the model has never been asked the question the project cares about.

- **The reported ICs are one-epoch ICs.** Model selection picks epoch 1 in 10–11 of 12
  seed-folds (`run009a.analysis.md`). A 240 k-parameter LSTM that overfits in under one
  epoch is, functionally, close to a linear map on these features. Consistent with the
  twice-cleared "capacity is not the constraint" finding — and an argument for dropping the
  LSTM for iteration speed (§4.4).

## 4. Ideas, ranked

### 4.1 Measure the cost curve, not another gate — ✅ **DONE 2026-09-22, see §6**

**Result: `c* = 0.214 bp` one-way vs a 2.0 bp maker fee — 9.4× short. The first branch
below is the one that fired; the direction line is closed.**

Everything needed is already in `run009f_scores.npz` (`pred`, `close`, `fwd_close`, `dt_s`,
`fold_of`). Instead of discrete triggers each paying a full round trip, simulate a **netted
position book**:

```
p_t   = clip(k · pred_t,  -1, +1)
PnL   = Σ p_t · r_t  −  c · Σ |Δp_t|
```

sweep `c` from 0 to 5 bp and report the **break-even `c`**.

Why this and not another notebook run: every sim to date charged a full 10 bp round trip per
overlapping 90 s signal, which is the most expensive possible way to express a persistent
score. The netted book is the correct way to ask what IC = 0.06 is worth **net**, and it
produces one number that decides the project's future:

- break-even `c ≈ 0.3 bp` → no achievable fee tier saves this; the direction line closes with
  certainty rather than exhaustion.
- break-even `c ≈ 2–3 bp` → fee tier + turnover control is a live path worth engineering.

### 4.2 Pre-register the economic feasibility check — *one line, prevents the next five dead runs*

Before building any run, compute `p_required = (1 + F/E|move|)/2` on the bars it intends to
trade, with `F` the **realistic** round trip for its exit structure (7–9 bp with a stop, not
4 bp). **If `p_required > 0.65`, do not build it.**

This would have killed 009e, 009f and most of the 008–011 execution grids before they were
written. The project's pre-registration discipline is already unusually good; this is the one
gate it lacks, and it is economic rather than statistical.

### 4.3 If the direction line continues: change the *label*, not the selector

Vol-scaled barriers `θ_t = max(k · σ_t, fee_floor)` with `σ_t` from the same causal rolling
window already used for `y_h`. This makes base rates stationary across regimes and removes
"big move" as the dominant learnable signal — attacking the diagnosed root cause
*structurally* instead of asking model selection to dodge it. The `max(·, fee_floor)` clamp
keeps the label economically meaningful (a vol-scaled 5 bp barrier in a quiet hour is
untradeable by construction). One config change, one retrain; the last genuinely untested
item on the current ladder.

### 4.4 Train on net P&L directly — *and drop the LSTM*

Replace Huber + BCE with a differentiable economic objective:

```
loss = −E[ p_t · r_t  −  c · |Δp_t| ],     p_t = tanh(head(x_t))
```

Cost-aware end-to-end training makes **abstention emergent** — the model outputs ~0 wherever
the edge does not cover the spread, which is precisely the selective behaviour the project
wanted from the start ("happy if most is 'no idea'"). Related and cheap: swap the LSTM for
LightGBM as the workhorse. Capacity was cleared twice (runs 002/003), the LSTM overfits
within one epoch, and a GBM gives ~10× iteration speed for free.

### 4.5 The horizon move — accept lower IC to buy a better fee ratio

`economics_and_metrics.md` §3 concludes "there is no horizon where both hold" and calls that
the wall. The claim should be narrowed: what was measured is that **5 s orderbook
microstructure features** have no IC past 2 minutes — which is near-tautological, since
microstructure information decays in seconds. It is *not* a statement about the market at
15 min – 1 h, where `p_required` falls to 0.64–0.77 and where an entirely different feature
class lives: funding, basis, open interest, liquidation cascades, spot↔futures and ETH
lead-lag aggregated over minutes, CVD.

68 days ≈ 6,500 fifteen-minute windows — enough for a GBM first look, no new collector
needed. Expect it to fail too (0.64 is still a lot), but it is the only direction where the
arithmetic is not already lost before the first line of code.

### 4.6 run.013 T3 — right priority, but with the target quantified

Agreed with `SESSION_NOTES_2026-09-21.md`: it is the only untested input class, and its
prerequisites are the right ones (shift the bin-end features per §6.4, re-index to a
mid-relative depth grid per §7.1, port the 009f diagnostics so it is not read through the
artifact-passing gate).

What to add: the required improvement is now a **number**. Adverse selection costs ~5 bp;
eliminating it entirely takes a 13.2 bp selected bar from `p_required = 0.80` to `0.65`,
against an achieved ~0.53. So even a *complete* solution to fill timing leaves a 12-point
accuracy gap. Frame T3 as "is fill timing predictable at all" — a pipeline + AUC result, as
the notes already propose — and explicitly **not** as a path to a gate pass, because
arithmetically it is not one on its own.

### 4.7 Invert the asset: the volatility forecast is the product

The single most robust, three-times-replicated, out-of-sample result in the whole project is
that the model predicts **|move|** with 3.3–5.3× lift and lifts `P(either barrier touched)`
from 2.30 % → 43.7 %. It has been treated throughout as an artifact to engineer away. It is
the one asset here with demonstrated out-of-sample value.

Constraints, stated honestly:

- Not tradeable as a straddle at retail on BTC perps.
- Classic spread-capture market making is dead on arrival: the BTCUSDT tick at ~65 k is
  **0.015 bp**, against a 4 bp maker round trip — the spread is ~250× too small to pay the
  fees. Only a maker-rebate / MM-program fee structure changes that, and retail cannot get
  one.
- Latency from Greece (~50–150 ms) rules out competing with professional HFT on speed.

What it *is* good for: a **toxicity / adverse-selection filter** on any passive order — the
exact inverse of how it was used (§2 item 3). Worth an hour of thought on what business model
consumes a 90-second volatility forecast, because the direction line has now been falsified
five separate ways.

## 5. Stop doing

All of these have been run properly and each was measured against a constraint it could not
touch:

- **Selection objectives** — three tried (AP / DE / spread), monotone failure.
- **Exit rules and TP/SL grids** — runs 007, 009a; exits redistribute path capture, they
  cannot add edge.
- **Entry latency** — run 008; edge is latency-robust, just below fees.
- **Maker wait × δ grids** — runs 008–010a; deeper δ strictly worse.
- **Feature prunes / importance studies** — 009b, 009d.
- **More bar-level data** — 009a settled it: 68.8 days, IC positive in 4/4 folds at every
  horizon, gate fails in *every* fold.
- **The maker/skip router** — 011, vacuous fail; its premise (0.610 AUC) did not reproduce
  (0.518).

## 6. RESULT — the cost curve, measured (2026-09-22)

*Executed locally on CPU from `runs/run009f_scores.npz`, ~30 s total, no GPU, no tar, no
retraining. Reproduction code in the appendix.*

### 6.1 Accounting and validation

```
p_t   = position held over bar t -> t+1, proportional to the model score (unit sd)
PnL   = sum p_t * r_t  -  c * sum |p_t - p_{t-1}|        c = ONE-WAY cost in bp
c*    = sum(p_t * r_t) / sum|p_t - p_{t-1}|              break-even one-way cost
```

`c*` is **scale-invariant** for any position map linear in the score (numerator and
denominator both scale with the sizing constant), so it is a property of the signal, not of
leverage. Positions are forced flat across every collector gap and every fold boundary.

| check | value |
|---|---|
| samples / span / test days | 666,028 · 40.8 d · 42 days |
| `dt` strictly increasing, folds non-decreasing | yes / yes |
| contiguous 5 s steps | 665,888 / 666,027 = **99.98 %** |
| contiguous segments | 140 (median 2,718 bars, max 34,380) |
| sd(1-bar return) | **1.470 bp** (fold 0/1/2/3: 1.21 / 1.85 / 1.33 / 1.41) |
| mean 1-bar return (drift) | +0.0032 bp |
| corr(`pred`, `score`) | 0.943 … 0.987 |

### 6.2 The headline — linear netted book, regression head

| horizon | gross (bp/day) | turnover (/day) | **c\* (bp, one-way)** | daily t | 95 % CI on c\* |
|---|---|---|---|---|---|
| **h6 (30 s)** | **+650.5** | 3,045 | **+0.2136** | **+7.82** | **[+0.167, +0.256]** |
| h9 | +550.3 | 2,825 | +0.1948 | +6.71 | [+0.143, +0.242] |
| h12 | +511.4 | 2,780 | +0.1840 | +6.03 | [+0.130, +0.233] |
| h15 | +480.5 | 2,752 | +0.1746 | +5.61 | [+0.119, +0.225] |
| h18 (90 s) | +469.9 | 2,764 | +0.1700 | +5.32 | [+0.113, +0.221] |
| h24 | +473.8 | 2,794 | +0.1696 | +5.17 | [+0.112, +0.221] |

**The signal is unambiguously real.** +650 bp/day gross at `t = +7.8`, CI on `c*` excluding
zero at every horizon. This is the first time the project has priced the ranking signal
without charging it a full round trip per overlapping 90 s trigger — and the answer is that
it is worth **0.21 bp per unit of turnover**.

**And that is ~10× too little.** Required `c*` by fee route:

| route | one-way | model short by |
|---|---|---|
| VIP0 maker 0.0200 % | 2.00 bp | **9.4×** |
| VIP9 maker 0.0100 % | 1.00 bp | 4.7× |
| VIP0 taker 0.0500 % | 5.00 bp | **23.4×** |

### 6.3 Turnover reduction does not rescue it — the decisive negative

The whole hypothesis behind §4.1 was that a netted book amortises fees over a persistent
score. It does not, because **the information decays faster than the turnover falls**:

| smoothing (h6) | gross bp/day | turnover/day | c\* (bp) | t |
|---|---|---|---|---|
| none | +650.5 | 3,045 | +0.2136 | +7.82 |
| EMA 6 (30 s) | +277.3 | 1,264 | +0.2194 | +5.57 |
| EMA 18 (90 s) | +135.1 | 739 | +0.1828 | +3.30 |
| EMA 60 (5 min) | +40.6 | 388 | **+0.1045** | +1.04 |
| EMA 180 (15 min) | −17.7 | 193 | **−0.0916** | −0.42 |
| EMA 720 (1 h) | −85.3 | 69 | **−1.2383** | −1.65 |

Turnover falls 44×; gross falls 7.6× then goes **negative**. `c*` is flat to EMA18 and then
collapses. Deadbands behave the same way (h6 EMA18 dead-0.9: `c* = −0.004`). **There is no
holding period at which this signal pays a 2 bp fee.**

### 6.4 The oracle ceiling — perfect foresight also fails at 5 s

Same accounting, but the position is the *true* sign of the future move:

| oracle | gross bp/day | turnover/day | **c\* (bp)** |
|---|---|---|---|
| perfect sign of next 5 s bar | 11,110 | 9,659 | **1.150** |
| perfect sign of 30 s move, held 30 s | 5,834 | 2,387 | 2.444 |
| perfect sign of 90 s move, held 90 s | 3,541 | 878 | 4.034 |
| perfect sign of 2 min move, held 2 min | 3,066 | 673 | 4.556 |
| perfect sign of 5 min move, held 5 min | 1,940 | 276 | 7.044 |
| perfect sign of 15 min move, held 15 min | 1,077 | 93 | 11.576 |
| perfect sign of 60 min move, held 60 min | 515 | 25 | 20.285 |
| perfect sign of 180 min move, held 180 min | 267 | 8 | 32.072 |

**A perfect next-bar direction predictor has `c* = 1.15 bp`, which is below the 2.0 bp maker
fee.** At a 5 s cadence, omniscience loses money on Binance. This is the cleanest possible
statement of the project's wall, and it is a property of the fee schedule and BTC's
volatility — not of any model.

The oracle clears each fee only past a minimum holding time:

| fee route | one-way | minimum oracle holding time |
|---|---|---|
| maker 2.0 bp | 2.00 | ~**30 s** (c\* 2.44) |
| mixed 3.5 bp | 3.50 | ~**90 s** (c\* 4.03) |
| taker 5.0 bp | 5.00 | ~**5 min** (c\* 7.04) |

**Model efficiency:** `c*_model / c*_oracle` at the bar level = 0.214 / 1.150 = **18.6 %** of
the achievable edge-per-turnover. That ratio is the useful design target: a model at a
longer horizon needs to capture roughly **10–20 % of oracle** to clear maker fees —
15 min oracle 11.58 × 0.186 ≈ 2.2 bp (just clears maker), 60 min oracle 20.3 × 0.186 ≈ 3.8 bp
(clears maker comfortably, still short of taker). **That is the quantitative bar for §4.5**,
and it is the only branch this measurement leaves open.

### 6.5 The classification heads carry negative direction — independent confirmation

Using `p ∝ P(lup_h) − P(ldn_h)` as the position (a pure directional read of the opportunity
heads, no triggers, no labels, no sim):

| h | 6 | 9 | 12 | 15 | 18 | 24 |
|---|---|---|---|---|---|---|
| c\* (bp) | −0.186 | −0.037 | −0.188 | −0.124 | −0.091 | −0.032 |
| t | −1.45 | −0.43 | −1.39 | −1.08 | −0.94 | −0.46 |

Negative at every horizon, significant at none. This reproduces the `S ≤ 0` finding through
a completely different accounting route, and supports the §3 reading: **`S ≈ 0`, not
"significantly negative"** — the heads carry no direction, rather than inverted direction.
(All six CIs on `c*` bracket zero.)

### 6.6 Stability and the one apparent exception

Per-fold `c*` (linear): **h6** `f0 +0.019 · f1 +0.334 · f2 +0.275 · f3 +0.179`;
**h18** `f0 −0.068 · f1 +0.329 · f2 +0.244 · f3 +0.088`. Fold 0 is ≈0 or negative on both —
so even the 0.21 bp is not uniform across the window.

A 72-cell sweep over horizon × smoothing × deadband produced one cell nominally above the
maker fee: **h6 EMA18 dead-0.9995, `c* = 3.07 [+0.141, +7.162]`, `t = +1.49`, 74 turnover
units/day.** It should not be believed: `t < 2`, the CI is enormous, it is the maximum of 72
cells with no multiple-testing correction, its neighbours disagree in sign (h18 EMA18
dead-0.9995 = −0.213), and it survives on ~0.05 % of bars. Recorded here so the next reader
does not re-discover it and mistake it for a result.

### 6.7 What §6 settles

- **The direction line is closed, quantitatively.** `c* = 0.214 bp` vs a 2.00 bp floor is not
  a gap that a better model, more data, a different label or a different execution layer
  closes. It is 9.4×.
- **Netting/amortisation is dead as a lever** (§6.3) — the one economic mechanism the project
  had never tested.
- **The fee schedule alone rules out the 5 s cadence** (§6.4), independent of any model.
- **`S ≈ 0` is the right reading, not `S < 0`** (§6.5) — nothing to invert.
- **The only surviving branch is §4.5** (15 min – 1 h horizon, different feature class), and
  it now has a concrete target: capture ≥ 10–20 % of the oracle's edge-per-turnover at that
  horizon. §4.3 (vol-scaled labels) and §4.4 (net-P&L objective) remain worth doing *only* in
  service of that branch, not on the 90 s ladder.

## 7. Long-horizon design — what (a) configurable horizon and (b) a direction model are worth

*Measured 2026-09-22 from `run009f_scores.npz`, prompted by two proposals: make the
horizon configurable, and split the model into a big-move detector plus a dedicated
direction model trained on big moves only. Both are right. The measurements say they
are worth far more **together** than separately.*

### 7.1 The existing model's direction does NOT transfer

Spearman IC of the existing `pred` against the raw forward return at each horizon:

| forward horizon | 30 s | 90 s | 5 min | 15 min | 30 min | 1 h | 3 h |
|---|---|---|---|---|---|---|---|
| IC | +0.054 | +0.046 | +0.034 | +0.025 | +0.013 | +0.006 | −0.012 |
| daily t | +6.7 | +4.1 | +1.6 | +0.2 | −0.4 | −0.9 | −1.5 |

Significance is gone by 5 minutes and the sign inverts by 3 h. **Stage 2 must be
trained fresh on a long-horizon target** — the existing regression head cannot be
reused, and neither can the assumption that its features are the right ones.

### 7.2 But the volatility detector DOES transfer — the enabling result

Top-0.1 % per fold by `P(lup_18) + P(ldn_18)` (the existing stage-1 detector, used
far outside its 90 s training horizon):

| horizon | 30 s | 90 s | 5 min | 15 min | 30 min | 1 h | 3 h | 6 h |
|---|---|---|---|---|---|---|---|---|
| \|move\| lift | 4.84× | 4.63× | 4.33× | **3.59×** | 2.72× | **2.27×** | 1.26× | 0.97× |
| E\|move\| on triggers | 10.7 bp | 18.6 bp | 31.7 bp | **44.5 bp** | 47.7 bp | **55.8 bp** | 54.9 bp | 61.3 bp |

Stage 1 keeps useful concentration out to ~1 h **with no retraining at all**. This is
the single most valuable measurement in this document after §6: it means the
expensive half of the two-stage design already exists and already works at the
horizons where the economics are survivable.

### 7.3 The consequence — required accuracy collapses

`p = (1 + F/E|move|)/2` on the stage-1 trigger set of §7.2:

| horizon | maker 4 bp | mixed 7 bp | stop 9 bp | taker 10 bp |
|---|---|---|---|---|
| 30 s | 0.687 | 0.827 | 0.920 | 0.967 |
| **90 s** *(where 13 runs were spent)* | 0.607 | 0.688 | 0.742 | **0.769** |
| 5 min | 0.563 | 0.610 | 0.642 | 0.658 |
| **15 min** | 0.545 | 0.579 | 0.601 | **0.612** |
| **1 h** | **0.536** | 0.563 | 0.581 | **0.590** |

Current directional accuracy is ~0.52–0.55 everywhere. **At 90 s the gap is ~24
points. At 1 h it is ~6 points (taker) or ~1–3 points (maker).** Same features, same
detector — the horizon does all the work.

> **CORRECTION (2026-09-22, after the probe).** This table benchmarks against a 0.50 coin.
> On volatility-selected bars at long horizons that is wrong: the drift baseline measured on
> the probe's own trigger sets was **0.347 at 15 min and 0.873 at 1 h**. The 1 h "maker
> requirement 0.536" is beaten by simply always being long. Any long-horizon criterion must
> read `required = max((1 + F/E|move|)/2, P(up) on the same trigger set)`. The required-fee
> arithmetic is unchanged; the *benchmark it is compared against* is not 0.50. See
> `gbm_probe.analysis.md` §3 and §7.

A third, independent benefit: adverse selection is a roughly fixed ~5 bp, so it falls
from **27 %** of the move at 90 s to **11 %** at 15 min and **9 %** at 1 h. The
mechanism that killed maker entry (`hit_f 4.4 % ≪ hit_m 18.0 %`) stops dominating.

### 7.4 What is NOT evidence — the per-horizon signed edge is a random walk

Using the existing `pred` sign on the §7.2 trigger sets:

| horizon | 90 s | 5 min | 15 min | 30 min | 1 h | 3 h |
|---|---|---|---|---|---|---|
| signed bp | +1.85 | **+5.01** | −5.25 | −7.22 | +3.41 | −4.91 |
| day-clustered 95 % CI | [−0.73, +5.06] | [−1.44, +11.68] | [−16.87, +8.22] | [−20.24, +9.66] | [−13.19, +20.52] | [−20.86, +12.58] |
| folds > 0 | 4/4 | 3/4 | 1/4 | 1/4 | 3/4 | 1/4 |

Every CI includes zero and the signs alternate. **The +5.01 bp at 5 min is not a
finding** — it is the same shape as fold 3, the run.010a 0.610 router AUC, and the
§6.6 deadband cell. Recorded here so it is not rediscovered as a result.

### 7.5 Design consequences for the two-stage run

1. **Stage-2 label = sign of the vol-normalised return at the horizon**, not a
   first-touch barrier. First-touch reintroduces magnitude into the direction target,
   which is precisely the coupling that made stage 1 and stage 2 the same detector.
2. **`THETA_BY_H` must scale with horizon and volatility.** A fixed 20 bp barrier
   fires on ~74 % of bars at 1 h. Use `θ = k·σ_h`. Configurable horizon makes the
   vol-scaled label of §4.3 mandatory rather than optional.
3. **The fold purge must widen from `MAX_H` (24) to `max(LONG_H)`.** A 1 h label
   started just before a fold boundary resolves 720 bars later — inside the next
   split. With run.009f's purge that is direct label leakage into test.
4. **Use GBM for stage 2, not an LSTM.** "Eventful" is 1.7 % at h18 → ~6.6 k
   sequences in fold 0; a model that already overfits at epoch 1 on 800 k samples
   will memorise that instantly. Longer horizons also *fix* the small-sample problem:
   with a vol-scaled θ the event rate is a choice, so 1 h at 5 % gives 20–40 k.
5. **Pre-register on required accuracy, not AUC** (§4.2, and `run014.plan.md` §9).

### 7.6 The feature prune was measured on the wrong objective

`DROP_HARMFUL` was derived from the **h18 up-head AP drop** — a magnitude metric at a
90 s horizon. What it dropped:

`basis_z_4h`, `ma_gap_1h`, `ma_gap_4h`, `minute_sin`, `minute_cos`, `dow_sin`,
`vol_norm`, `flow_net_widex_z`, `buy_accel`, `wall_imbal`, `wall_qty_norm`,
`sell_tail_ratio`, `largest_trade_rel`

Spot–futures basis, two trend gaps and the whole seasonality block — the slow,
directional, carry-flavoured features, discarded for being poor at 90-second
magnitude. That is the wrong objective and the wrong horizon for long-horizon
direction. Re-judge with a direction metric before trusting the prune.

**Caveat:** run.011 found regime/time proxies (`vol_ratio_1h_24h`, `lsr_z`,
`minute_cos`, `dow_sin`) carried a 0.613 AUC that died under walk-forward. These
features have a history of looking good and not being causal, so they must be
validated per fold, walk-forward. At 1 h they are at least the right *class*, which
they never were at 90 s.

### 7.7 The probe — `runs/btc_lstm.probe.gbm.ipynb`

Before building any two-stage notebook, one cheap question: **is there a
long-horizon directional signal in the existing features at all?** The probe trains
GBMs at 15 min and 1 h on the run.009f feature pipeline (cells 1–5 byte-identical),
walk-forward on the same folds with the widened purge, with three heads per fold:
`mag` (stage 1), `dir_all`, and `dir_evt` (direction trained only on eventful bars —
the explicit test of the "train on big moves only" hypothesis).

Pre-registered: **A** daily-IC t ≥ 3 and IC > 0 in ≥ 3/4 folds · **B** accuracy on
the stage-1 top-0.1 % ≥ the maker-route requirement (0.545 @ 15 min, 0.536 @ 1 h) ·
**C** net at 7 bp > 0 with a day-clustered CI excluding 0. Falsification: A fails at
both horizons → the existing bar features carry no usable long-horizon direction, so
do not build the two-stage notebook. **A-pass / B-fail is the run.009d–f pattern and
must not be read as success.** Build notes: `runs/gbm_probe.notes.md`.

### 7.8 Probe result — FALSIFIED (executed 2026-09-22)

Ran clean on Colab (14/14 cells, GPU). **Criterion A failed in all four arms:**

| | 15 min `dir_all` | 15 min `dir_evt` | 1 h `dir_all` | 1 h `dir_evt` |
|---|---|---|---|---|
| pooled IC | +0.0050 | −0.0088 | +0.0014 | −0.0073 |
| daily-IC t | +1.94 | −2.44 | +1.70 | +1.01 |
| folds > 0 | 4/4 | 1/4 | 2/4 | 2/4 |

For scale the LSTM's h6 IC is +0.0602. The direction heads early-stop at **0–14 boosting
rounds** (the `mag` head trains to 30–122) — the GBM reaches the same epoch-1 collapse the
LSTM does, from a different model family.

**Criterion B printed three PASSes and all three are drift.** The probe compared accuracy to
the fee-breakeven number only; it never inherited the blind benchmark from
`run009d.offline.md` §8. Computed afterwards from the saved npz, on the probe's own trigger
sets: `P(up)` = **0.347** (15 min) and **0.873** (1 h), always-long gross −10.10 and
**+20.62** bp, and the model **loses to always-long in 7 of 8 cells** — by 12.3 bp in the
worst. The 1 h requirement was 0.541; being long scored 0.873.

Harmless at 90 s (blind benchmark there is +0.04 bp), fatal at 1 h. **Lengthening the horizon
introduces a drift confound that did not exist at 90 s** — the single most transferable
lesson from this probe.

**What stands from §7.1–§7.3:** the required-accuracy relaxation is real and the volatility
detector really does transfer. What the probe tested was the remaining premise — that some
directional signal exists at those horizons to exploit the relaxation — and that premise is
**false for this feature set**. The GBM `mag` head also came in *worse* than the existing
LSTM detector (2.12× vs 3.59× at 15 min), so stage 1 needs no retraining either.

Full analysis, including why the sign-permutation null reads 100 % without rescuing anything:
`gbm_probe.analysis.md`. Control implemented for future runs in
`btc_lstm.probe.gbm2.ipynb` cell G5b; the executed v1 is kept unmodified as the record.

## 8. Closing summary — where the project ended

*Written 2026-09-22 after the last open branch closed. This section is the current
statement of the project; §1–§7 are the working record that produced it.*

### 8.1 The verdict in one line

> **The signal was always real and never tradeable.** Every horizon the project tried has a
> measured directional signal and a measured cost floor above it, and the gap is arithmetic,
> not a modelling failure.

### 8.2 Three branches, three measurements, all closed

| horizon | what exists | what it takes | gap |
|---|---|---|---|
| **5 s – 90 s** | IC +0.06…+0.11, daily-t +7.6; ~52–53 % directional | `c*` ≥ 2.0 bp (maker) | **`c* = 0.214 bp` — 9.4×.** A *perfect* next-bar oracle reaches only 1.15 bp, below the maker fee (§6.4) |
| **15 min – 1 h** | required accuracy relaxes to 0.54–0.61; the volatility detector still transfers (2.3–3.6× lift) | any directional signal at all | **IC +0.005 / −0.009 / +0.001 / −0.007**, daily-t never reaching 3; direction heads early-stop at 0–14 rounds (§7.8) |
| **4 h** | a genuine, stationary rank signal — `range_pos_24h` IC **−0.34** in both train and test, daily-t −18, 24/29 features keep sign OOS | mean P&L > 4 bp | **−1.39 bp gross over 1,625 trades / 7 years**, sign-perm percentile 33.2 (`v1_4h_feasibility.analysis.md` §9) |

Each row was closed by a different failure mode, and that is the substance of the finding:

1. **90 s — the fee is larger than the move.** At `E|move| = 4.03 bp` against a 10 bp taker
   round trip, breakeven needs 174 % accuracy. Selection lifts `E|move|` to 13.2 bp and the
   requirement to 88 % (taker) / 65 % (full-maker, which a stop makes unreachable — the honest
   figure is ~80 %). Nothing closes a gap that starts above 100 %.
2. **15 min – 1 h — the requirement relaxes but the signal vanishes.** Microstructure
   information decays in seconds; nothing in the 76 bar features replaces it at those horizons.
3. **4 h — the signal exists and the payoff is left-skewed.** Win rate 54.9 %, **median trade
   +11.47 bp, mean −1.39 bp**. Many modest winners, a thin tail of large losers. The two worst
   folds are 2021-10→2022-06 and 2023-01→2023-08, i.e. the drawdown regimes. This is the same
   "win capped, loss uncapped" asymmetry `economics_and_metrics.md` §7.5 found at 90 s,
   reappearing at a 4-hour horizon with an unrelated signal — so it is a property of the trade
   structure, not of any one horizon.

### 8.3 What is left, and why it does not change the verdict

**`run.013`'s price-level event panel** is the only untested input class. It should be run to
close the question, but not as a path to P&L: eliminating adverse selection *completely* moves
the 90 s requirement from 0.80 to 0.65 against an achieved 0.53 (§4.6), and §6.4 shows the 5 s
cadence fails even with perfect foresight. Frame it as "is fill timing predictable at all".

Beyond that, the configurations that remain arithmetically open are **daily horizons** (required
accuracy 0.54) and **near-zero fees** (≈0.002 % maker, which does not exist). Daily horizons
need years of data and a different signal class — and are now cheap to test, since the 7-year
kline set is downloaded.

### 8.4 What survives

- **The volatility forecaster is real and beat every falsification attempt.** Eventful-bar AUC
  **0.879**; |move| lift **4.63× at 90 s decaying to 2.27× at 1 h** with no retraining;
  `P(either barrier touched)` 2.30 % → 43.7 %. It is not a strategy, but it is a correct,
  durable model of something, and it is the right input for position sizing, stop placement and
  quote withdrawal if a base strategy ever exists.
- **The data.** 740,451 5-min klines (7 years, 100.00 % coverage); a year of v1 collector data
  with open interest, long/short ratio and the full depth ladder for **331 days** (plus funding
  and liquidations for **216**) — OI, L/S and liquidation history are not re-downloadable at any
  price; and the 60-day v5 set with all 76 features.
- **The method.** The diagnostic chain from 009a through §9 is more rigorous than most
  production research, and it is self-correcting: several published numbers in this repo were
  retracted by their own authors after re-derivation.

### 8.5 What actually caught errors

Ranked by what changed a conclusion, not by how much machinery it required:

1. **Out-of-sample sample length.** The 4 h strategy passed *every* structural control on one
   year — walk-forward, one-position accounting, balanced legs, 6/8 months, day-clustered CI —
   and was still an artifact. Only 8× the sample separated it from zero.
2. **Non-overlapping trade accounting.** Caught a **9.4×** inflation (+13.78 → +1.46 bp) that no
   other control detected. Per-signal averaging over overlapping windows is not a P&L.
3. **The blind / drift benchmark.** Caught three false criterion-B "passes" in the GBM probe,
   where `P(up)` on the trigger set was 0.873 and the fee requirement was 0.541.
4. **The sign-permutation null.** The one statistic that never lied: 90.3 on the favourable
   year (already below its 97.5 bar) and 33.2 over 7 years.

Day-clustered CIs, month-by-month stability and fold counts caught nothing the above missed.

**Two lessons about process**, both earned the hard way in this repo: a green test suite is not
evidence unless it executes the artifact (three defect classes shipped past green suites), and
**tuning cost 7 bp per trade** — per-fold config selection underperformed the untuned rule in 3
of 4 folds (`v1_4h_feasibility.analysis.md` §8.2).

### 8.6 If this were started again

Run the economic feasibility check **before** writing a notebook:

```
required accuracy = max( (1 + F/E|move|)/2 ,  P(up) on the same trigger set )
```

with `F` the realistic round trip for the intended exit structure (7–9 bp with a stop, not 4).
**If it exceeds ~0.65, do not build the run.** That single line, applied at run.007, would have
redirected the project four years of wall-clock earlier — and applied at each later branch, it
would have killed runs 009e, 009f, most of the 008–011 execution grids, and the criterion-B
half of the GBM probe before any of them were written.

The rest of the discipline here — pre-registration, one variable per run, adversarial review,
re-derivation from saved artifacts — was already good. What was missing was never statistical.
It was an economic gate.

## Appendix — how to check §1

- `E|move| @ 90 s = 4.03 bp` — measured, `no_profit_root_cause.md` §1 (from `run009f_scores.npz`).
- √t scaling: `E|move|(t) = 4.03 · √(t / 90 s)`. Cross-check: at t = 1 day this gives 125 bp
  ≈ 1.6 % daily vol, correct for BTC.
- `p_required = (1 + F/E|move|)/2` from `(2p−1)·E|move| = F`.
- Accuracy from IC: for jointly-normal scores/returns, `P(sign correct) = 0.5 + arcsin(ρ)/π
  ≈ 0.5 + ρ/π`. At ρ = 0.06 → 51.9 %; at ρ = 0.11 → 53.5 %.
- Fees: `TAKER_FEE = 0.0005`, `MAKER_FEE = 0.0002` per side (cell 4:46–47) → taker RT 10 bp,
  mixed 7 bp, full-maker 4 bp.
- Selection lift on `E|move|`: 13.22 bp on up18 triggers vs 4.03 bp on all bars = 3.28×
  (`no_profit_root_cause.md` §1).

## Appendix — how to reproduce §6

CPU only, ~30 s, `run009f_scores.npz` is the sole input (no tar, no GPU, no retraining).
Interpreter: `/home/nkout/projects/binance2/binance2/.venv/bin/python` (numpy 2.3.2).

```python
import numpy as np
z = np.load("runs/run009f_scores.npz", allow_pickle=True)
close, dt, fold = z["close"].astype(float), z["dt_s"], z["fold_of"]
pred, W, n = z["pred"].astype(float), int(z["window_sec"]), len(z["close"])

# one-bar step, valid only across a contiguous 5 s bar inside one fold
step_ok = np.zeros(n, bool)
step_ok[:-1] = (np.diff(dt) == W) & (fold[:-1] == fold[1:])
r1 = np.zeros(n)
r1[:-1] = (close[1:] / close[:-1] - 1.0) * 1e4      # bp return over bar t -> t+1
r1[~step_ok] = 0.0

def cstar(p):                                        # p = position held over t -> t+1
    p = np.where(step_ok, p, 0.0)                    # forced flat across gaps
    turn = np.abs(np.diff(np.concatenate([[0.0], p]))).sum()
    return (p * r1).sum() / turn                     # break-even ONE-WAY cost, bp

s = pred[:, 0]                                       # h6 regression head
print("model  c* =", cstar(s / s.std()))             # ~ +0.2136
print("oracle c* =", cstar(np.sign(r1)))             # ~ +1.1502
```

Notes on the accounting:

- `c*` is **scale-invariant** for a position map linear in the score, so sizing/leverage does
  not affect it. Deadbands and clipping do change it (they alter the shape, not the scale).
- The `|Δp|` sum includes the initial entry, and positions are forced flat on the last bar of
  every contiguous segment, so no P&L or turnover is booked across a collector gap or a fold
  boundary.
- Daily aggregation for the `t`-stat and the bootstrap uses `day = dt_s // 86400` (42 days),
  resampling whole days with replacement, 4,000 draws; the `c*` CI resamples the **ratio**
  (`Σgross_day / Σturn_day`), not the mean of daily ratios.
- Smoothing in §6.3 is a causal EMA restarted at every segment boundary.
- Oracle rows in §6.4 hold `sign(r_h)` for `h` bars non-overlapping, with the same gap rules;
  for `h > 24` the forward return is built from `close` directly rather than from `fwd_close`
  (which stores only 24 forward closes).
