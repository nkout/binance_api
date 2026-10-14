# M1 mid-cap alt market-making feasibility — FAIL: adverse selection exceeds the half-spread before any fee, in every symbol-day, day and spread bucket

*2026-10-14. Harness `runs/harness_mm_alt/` (`mmalt.py`, `fetch_mm.py`, `run_m1.py`, `test_mmalt.py` 0 failures; diagnostics `diag_m1.py`). Results `m1_results.json`,
`m1_output.txt`, `m1_diagnostics.txt`. Derived rows `data/mm_alt/` (484 symbol-days). Pre-registration: `next_signal_ideas.md`, Round 8 / M1 (written 2026-10-12
before any code). Data: Binance's free daily `bookTicker` + `aggTrades` archives (L1 quotes exist only 2023-05-16 → 2024-03-30).*

**TLDR.**
- **FAIL: 4 of 5 registered criteria fail.**
  - Band (volume ranks 41–150, 440 symbol-days, 194 symbols): maker `net_after_fee(5 s)` **−4.30 bp** per fill, day-clustered CI [−4.91, −3.72].
  - **0 of 4 days positive**, and the band is *worse* than BTC + majors (−3.75 bp).
  - Only "median and mean share a sign" passes (both negative).
- **The premise was wrong: mid-cap spreads are tight.**
  - The median quoted spread is **2.2 bp**, not the 5–20 bp the Round 4 note assumed.
  - The pre-registered spread criterion (> 4 bp, twice the maker fee) fails on its own.
- **Adverse selection is larger than the spread, before fees.**
  - A passive fill at the touch captures +1.91 bp of half-spread, then the mid moves −4.31 bp against it within 5 s.
  - That is **−2.40 bp before any fee**, and 0 of 440 symbol-days are positive after the fee.
  - It holds at every horizon (1 / 5 / 30 / 60 s) and in every spread tercile. In the widest tercile (3–38 bp) capture is +3.83 bp and adverse −5.65 bp.
- **Implication.** Passive quoting on Binance perps loses even at a zero maker fee. Break-even needs a maker *rebate* of ~2.4 bp per side, about 5× the best
  liquidity-provider rebate (−0.5 bp). The cheaper-fee-tier lever in `PROGRAMME_SUMMARY.md` §5 cannot help a naive maker. The maker loss comes from
  being picked off, the same thing that made maker entry worse than taker in the BTC runs.

## 1. Run validity

| check | result |
|---|---|
| tests | `test_mmalt.py` 0 failures: formula units, the Huang–Stoll identity `e = r + impact` exactly, the mark-out identity `capture + adverse = net`, OFI on a synthetic book, look-ahead, a planted maker-edge world that passes, 10 iid-mid null worlds (0 pass), end-to-end, fetcher mocks; plus (2026-10-14) as-of lookups by binary search equal to the original `merge_asof` |
| identities on real data | hold on 100 % of 484 rows |
| coverage | median 1.000 of the UTC day; no row below 0.9 |
| universe | point-in-time R4 eligibility + trailing-30-day quote volume from the parquet archive; day-1 rows (fetched 2026-10-12) match the registered band exactly |
| loader change mid-fetch | the loader was made memory-lean after two OOM kills (in-place parsing, binary-search as-of). Two rows written by the old code (ZRXUSDT 2023-07-19, BTCUSDT 2024-01-17) re-derive **identically on all 35 fields** with the new code |

## 2. Registered criteria (band = volume ranks 41–150; comparator = BTC + top 10)

| criterion | value | result |
|---|---|---|
| band `net_after_fee(5 s)` mean > 0, CI lower > 0 | −4.296 bp, CI [−4.913, −3.721] | FAIL |
| band median quoted spread > 4 bp | 2.219 bp | FAIL |
| band > comparator | −4.296 vs −3.750 bp | FAIL |
| median and mean share a sign | −4.138 / −4.296 | pass |
| ≥ 3 of 4 days positive | 0 of 4 (−3.54, −4.23, −4.28, −5.14) | FAIL |

## 3. Decomposition (bp per fill, qty-weighted within a symbol-day, equal-weight across symbol-days)

| cohort | quoted spread (median) | capture (half-spread) | adverse 1 s | adverse 5 s | adverse 60 s | net 5 s before fee | net 5 s after fee | OFI R² |
|---|---|---|---|---|---|---|---|---|
| band (440) | 2.22 | +1.91 | −4.22 | −4.31 | −4.25 | **−2.40** | **−4.30** | 0.47 |
| comparator (44) | 0.46 | +0.48 | −2.31 | −2.33 | −2.14 | −1.85 | −3.75 | 0.50 |

**By quoted-spread tercile (band):**

| tercile | spread range | capture | adverse 5 s | net 5 s before fee |
|---|---|---|---|---|
| narrow | 0.4–1.7 bp | +0.69 | −3.35 | −2.66 |
| mid | 1.7–3.1 bp | +1.21 | −3.92 | −2.71 |
| wide | 3.1–38 bp | +3.83 | −5.65 | −1.82 |

Wider spreads come with proportionally larger adverse moves: the market prices the spread to the toxicity.

The best symbol-day (STMX 2023-07-19, 20.7 bp spread) captures +10.2 bp and loses −9.9 bp to the next 5 s: +0.3 before the fee, −1.6 after.

Almost all the adverse move happens within 1 s and then stays (−4.22 at 1 s, −4.25 at 60 s). It is information, not a transient that reverts. This matches R1 and N4:
on Binance, whatever moves the price is priced within a second.

## 4. Limitations (fixed in the registration) and which way they cut

- **Fill model: every trade at the touch fills the resting quote.**
  - Ignoring queue position *flatters* the maker: at the back of the queue you are filled only when the level is consumed, which is the most toxic case.
  - Counting each `aggTrade` of a multi-level sweep as a touch fill *penalises* the maker. Only the first level fills a touch quote, but every level is charged
    the sweep's mid move. The effective half-spread (+3.47 bp) above the quoted one (+1.91) shows sweeps are common in the qty weighting.
  - Neither bias plausibly closes a −2.4 bp gap that holds before fees in 440 of 440 symbol-days. The majors comparator, where sweeps are rare, shows the same sign.
- **Window.** 2023-05 → 2024-03 only; the free L1 archive stopped after that. Spreads and competition after 2024 are not observed.
- **L1 only.** No depth, no own-order simulation, no quote management. A real maker with a toxicity filter (the volatility detector, §4 of the summary) would quote
  less often. But the filter would have to remove more than the average adverse move from fills that are toxic on average. That is a latency and queue race,
  not a retail setup.

## 5. Verdict

**FAIL; the deferred idea is closed in this form**, as registered: mid-cap alt market-making is not viable at the reachable retail maker fee tier, and not at a
zero fee either. A live-collected forward test would be a new registration. Nothing here suggests it would change sign.
