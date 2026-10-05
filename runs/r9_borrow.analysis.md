# R9 `CARRY−` borrow-rate check — DEAD by the pre-registered letter on both parts, but the prior was wrong: borrowing does not absorb the gross return; the constraint is capacity

*2026-10-06. Harness `runs/harness_xsec/` (`r9_borrow.py`, `test_r9.py` 13/13 PASS; R4 20/20, R5 15/15 unchanged). Results `harness_xsec/r9_results.json`,
`r9_output.txt`; raw borrow snapshot `harness_xsec/r9_borrow_snapshot.json` (477 assets, VIP0). Pre-registration:
`next_signal_ideas.md`, R9 implementation details. Local CPU + public endpoints, ~2 min.*

**Data note.** No API key was configured, so the signed history endpoint was not used. Binance publishes per-asset VIP0 cross-margin **daily interest rates and borrow
limits** on an undocumented public web endpoint (`…/bapi/margin/v1/public/margin/vip/spec/list-all`). That is a **current snapshot only**; S2 uses today's rates as a
proxy for the past. The endpoint is not part of Binance's documented API and may change.

**TLDR.**
- **S1 (today's candidates): DEAD as registered — capacity.** 11 perps have FUND7 ≤ −0.03 % per 8 h, 9 are borrowable, funding collected 41–1,434 %/yr against borrow
  rates of 8–87 %/yr (8 of 9 net-positive after borrow and costs). **None qualifies because every VIP0 borrow limit is $1.5k–5.6k** (the bar was ≥ $50k and ≥ 3 coins).
- **S2 (R5 `CARRY−` re-priced): DEAD by one year.** Restricting to coins borrowable today and paying today's borrow rate gives **+18.9 %/yr** (Sharpe 3.8, worst month −0.8 %,
  max drawdown −1.5 %), but 2024 is **−0.51 %** and the pre-registered bar required every year 2021–2025 positive.
- **The prior is falsified.** The expectation was that the spot-borrow cost would absorb the +37 %/yr gross. It takes 7.9 points of the 26.7 %/yr available on borrowable coins,
  and the return only reaches the 4.5 % risk-free rate at **2.8× today's rates**.
- **What is left is a small-account, capacity-bound, unproven-history carry.** Not a strategy at scale, and the result rests on today's borrow rates.

## 1. S1 — today's candidates (FUND7 ≤ −0.03 % per 8 h, 874 perps scanned)

| perp | FUND7 per 8 h | funding collected | borrow (VIP0) | net after borrow + 5.84 % costs | borrow limit |
|---|---|---|---|---|---|
| ARK | −1.309 % | 1,434 % | 86.7 % | +1,341 % | $1,968 |
| ONE | −0.615 % | 673 % | 16.4 % | +651 % | $1,623 |
| GTC | −0.253 % | 277 % | 81.6 % | +190 % | $1,483 |
| SAND | −0.184 % | 202 % | 23.1 % | +173 % | $4,806 |
| 2Z | −0.130 % | 142 % | 60.3 % | +76 % | $2,727 |
| CARV | −0.125 % | 137 % | not listed | n/a | n/a |
| RLC | −0.121 % | 132 % | 86.0 % | +41 % | $4,560 |
| BWET | −0.110 % | 120 % | not listed | n/a | n/a |
| QNT | −0.085 % | 93 % | 18.0 % | +69 % | $2,527 |
| MANA | −0.057 % | 63 % | 8.2 % | +49 % | $5,584 |
| ORCA | −0.037 % | 41 % | 66.9 % | −32 % | $2,187 |

All annualised, simple. Net = funding − borrow − 24 bp round trip amortised over the R5 mean 15-day hold. Rates of this size (ARK −1.3 % per 8 h) are the signature of an
extreme, probably squeezed or distressed contract; the snapshot does not say how long they last. **S1 verdict: DEAD** (0 qualify; need ≥ 3). The reason is the limit, not the rate.

## 2. S2 — R5 `CARRY−` re-priced (2020-10-14 → 2026-08-30, 2,147 days)

| | net %/yr | positions |
|---|---|---|
| R5 original, gross of borrow | +37.33 | 3.07 |
| only coins on the cross-margin list today, no borrow cost | +26.74 | 2.66 |
| **+ today's VIP0 borrow rates** | **+18.88** | 2.66 |

- Borrow cost 7.86 %/yr of capital; mean rate on the coins actually held 39.4 %/yr. Break-even: net equals the 4.5 % risk-free rate at **2.83× today's rates**.
- 15 % of R5's position-days were on coins that are not borrowable today (43 of 193 coins; delisted or never listed); they are treated as untradable, and R5's return fell from 37.3 to 26.7 %
  on that restriction alone, so the excluded days were the more profitable ones.
- Calendar years (net %): 2020 (Q4) +1.06 · 2021 +1.37 · 2022 +23.81 · 2023 +14.30 · **2024 −0.51** · 2025 +22.78 · 2026 (8 mo) +48.25.
- Sharpe 3.79, worst month −0.8 %, max drawdown −1.5 %; median daily net 0.0 bp (a position on 80 % of days, usually 1–4 slots).
- Not concentrated: the best coin (TRB) contributes 1.7 points of ~20 gross; excluding it the net is 17.2 %/yr; the top three coins are 22.5 % of the total.
- **S2 verdict: DEAD** against "net ≥ 4.5 % and positive every year 2021–2025" because of 2024.

R5's biggest `CARRY−` names today: BNB 3 %/yr borrow, BCH 8, TRX 9, CRV 10, TRUMP 24, ENA 25, APE 30, AXS 31, API3 55; WAVES is not listed.

## 3. Reading it

1. **The +37 % gross was not mostly the price of a scarce short.** At today's rates, borrow cost is about a quarter of the tradable gross, and the strategy survives a rate level almost three
   times higher. The pre-registered expectation that borrow would absorb it was wrong. This matters for the R5 write-up and the programme summary.
2. **The formal DEAD is two different near-misses.** S1 fails on a $50k capacity bar that I set (every limit is $1.5k–5.6k at VIP0). S2 fails on one year at −0.51 %. Neither is evidence that
   the carry is absent; they are evidence that it is small and uneven. The criteria are kept as registered.
3. **Capacity is the real limit.** Per-coin VIP0 limits of ~$2–6k, with 3 positions on average, mean roughly $10–25k of capital can be deployed before limits bind. At that scale +19 %/yr is a few
   thousand dollars a year.
4. **Why today's rates probably flatter the history.** Borrow demand rises in exactly the squeezes where funding is most negative; the snapshot is one calm-ish day. The listing is a
   survivor list (delisted coins are excluded, 15 % of position-days), and ticker reuse can mis-map generations of a coin (the archive's `LUNA` and `FTT` contracts map to today's assets of the same
   name). The break-even multiple of 2.8× is the cushion against all of this, and it is not guaranteed.
5. **Operational risk the backtest does not charge for:** a spot-margin short needs collateral; if the coin pumps, the spot short loses on the margin account while the offsetting perp gain sits in the
   futures wallet; funding can flip quickly; borrow can be recalled or capped.

## 4. What this settles and what it does not

- **Settles:** the prior is falsified. `CARRY−` is not dead because of borrow. By the pre-registered letter it is DEAD on capacity and one weak year.
- **Does not settle:** the historical borrow rates (needs the signed `interestRateHistory` with a read-only key), and whether the 2025–26 strength persists.
- **Cheapest next step:** accumulate the snapshot daily from now (the harness already saves `r9_borrow_snapshot.json`) so a real rate history exists, and forward-score the live candidates.
  A one-off with a read-only key would give the past rates for the 193 coins immediately.

## Reproduce

```
cd runs/harness_xsec
PY=/home/nkout/projects/binance2/binance2/.venv/bin/python   # + pyarrow on PYTHONPATH
$PY test_r9.py      # 13/13
$PY r9_borrow.py    # fetches today's snapshot, scores S1 + S2 -> r9_results.json, r9_borrow_snapshot.json
```
