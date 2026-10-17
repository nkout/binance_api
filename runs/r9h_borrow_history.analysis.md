# R9-H `CARRY−` on the borrow rates actually charged — STOPPED by the letter (a mis-specified cross-check), DEAD on every tier: two non-positive years, and the best year rests on two crisis events

*2026-10-17. Harness `runs/harness_xsec/` (`r9h.py`, `run_r9h.py`, `test_r9h.py` 19/19 PASS; R9 `test_r9.py` 13/13 unchanged; post-verdict `r9h_diagnostics.py`).
Results `harness_xsec/r9h_results.json`, `r9h_output.txt`, `r9h_diagnostics.txt`; asset list `r9h_assets.json`; raw rate cache `data/borrow_api/` (not in git).
Pre-registration: `next_signal_ideas.md`, Round 9, R9-H, plus its implementation details (fixed before any P&L). Read-only API key (`apiRestrictions`: reading only).
Local CPU; fetch ~45 min at ≤ 5 requests a second, scoring 38 s.*

**TLDR.**
- **Verdict by the letter: STOPPED.** The registered real-data check (the history's 2026-10-05 day rate within 10 % of that day's public snapshot for ≥ 90 % of assets) gave **78.4 %**
  of 153 assets.
  - The diagnostic shows this is not a data error: the rate in effect at the snapshot's fetch time (21:11 UTC) matches it **exactly for 153 of 153 assets**.
  - The check compared a point-in-time snapshot with a day mean, in a regime where rates move intraday.
- **Every tier is DEAD anyway**, on the "positive every full year" bar. 2021 is −0.04 % and 2024 −2.42 %; ALIVE-forward allowed exactly one negative year above −2 %. All other
  criteria pass: net, worst month, NW t, lag, break-even ≥ 1.5×.
- **Primary (point-in-time borrowability, historical rates): +18.59 %/yr**, NW t 2.96, worst month −1.24 %, max drawdown −4.96 %, break-even at **1.78×** the historical rates.
- **Historical borrow was much dearer than today's:** 18.1 %/yr of capital vs 7.9 % at today's rates. The mean paid rate is 80 %/yr, and **151 %/yr on days with
  FUND7 ≤ −0.10 % vs 50 %/yr on other held days**, so rates do spike in squeezes. The net is similar to R9's only because point-in-time borrowability adds back
  since-delisted coins, which were the more profitable ones.
- **Concentration.**
  - ALPACA's 7 days around its April 2025 delisting are +33 pp of capital: +29.7 pp perp-vs-spot basis, only +4.6 pp funding, at 920–1,325 %/yr borrow.
  - FTT in the FTX-collapse week (2022-11-09 → 11-15) adds +6.7 pp.
  - Without ALPACA: +13.0 %/yr. Without the top three coins: +10.5 %/yr (NW t 3.9). 2021 and 2024 stay non-positive either way.
- **Reading:** a real but uneven carry of roughly 10–13 %/yr outside two crisis events. It is flat or negative in 2 of 5 full years, capped at about $10–25k by VIP0 borrow limits,
  and its best episodes assume borrow was available exactly when it was least likely to be. **`CARRY−` is closed** under the pre-registered decision table.

## 1. Run validity

| check | result |
|---|---|
| tests | 19/19, network mocked. Signing against Binance's published HMAC example. 74 windows cover every day exactly once. `-11027` per window (cached as no records; the fetch continues) and `-1021` retried. Resumable cache. Day-rate rule (day mean, 3-day carry, then none). Freeze rule (30 days of one value, from 2023-03-31, window strictly before d). **Equivalence:** constant rates + today's list reproduce `r9_borrow.reprice` exactly. Exact P&L identity. Leak tests on positions / net and on day rates / freeze flags. |
| real-data equivalence | Row (c) reproduces R9 S2 exactly (+18.88 %/yr, same per-year figures). Row (d) reproduces R5 `CARRY−` (+37.33 %/yr). |
| fetch | 197 base assets (196 perps meet the `CARRY−` entry condition on ≥ 1 day, plus the BTC / ETH / BNB anchors) × 75 windows = **14,775 windows, 0 errors**. All 197 assets were on cross margin at some point. The history reaches the start of the window for the anchors, so the window is the full R5 window 2020-10-14 → 2026-08-30 (2,147 days, full years 2021–2025). |
| `vipLevel` | 0 on every record ✓ |
| snapshot cross-check (registered) | **FAIL**: 78.4 % of 153 assets within 10 % (needs ≥ 90 %). Diagnostic in §4. |
| two defects caught before any P&L | (1) The fetcher treated `-11027` as asset-level and stopped at the first such window. 20 assets listed on margin after 2020-10 (1INCH, ACH, AGLD, …) were wrongly marked never-borrowable. It was fixed, the markers were removed, and the assets were re-fetched. (2) Stalled requests (`-1021`, local clock within 0.1 s of the server, NTP synced) dropped 7 assets mid-fetch; made retryable, then resumed. Both are recorded in the implementation details. |
| deviation | The scoring run computed the rows and the checks in one pass, so the rows were seen although the cross-check failed. The registration said "stop and report". The rows are reported; the verdict is labelled STOPPED. |

## 2. Rows (net on capital, window 2020-10-14 → 2026-08-30)

| row | net %/yr | NW t | worst month | max DD | borrow %/yr | break-even | 1 h lag | positions | R5 days not borrowable |
|---|---|---|---|---|---|---|---|---|---|
| **(a) primary: rate exists and not frozen** | **+18.59** | 2.96 | −1.24 % | −4.96 % | 18.07 | 1.78× | +17.26 | 3.02 | 1.7 % |
| (b) historical rates, today's list | +12.01 | 3.69 | −1.61 % | −4.73 % | 14.74 | 1.51× | +10.91 | 2.66 | 14.7 % |
| (c) R9 S2: today's rates, today's list | +18.88 | 5.72 | −0.83 % | −1.48 % | 7.86 | 2.83× | — | 2.66 | 14.7 % |
| (d) R5 gross of borrow | +37.33 | | | | | | | 3.07 | |
| (f) registered B: rate exists | +18.82 | 2.98 | −1.82 % | −5.86 % | 18.51 | 1.77× | +17.52 | 3.07 | 0.0 % |

Calendar years (net %):

| row | 2020 (Q4) | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 (8 mo) |
|---|---|---|---|---|---|---|---|
| (a) | +0.56 | **−0.04** | +21.34 | +6.53 | **−2.42** | +38.39 | +45.00 |
| (b) | +0.30 | −0.93 | +19.93 | +0.65 | −2.93 | +8.18 | +45.44 |
| (c) | +1.06 | +1.37 | +23.81 | +14.30 | −0.51 | +22.78 | +48.25 |
| (f) | +0.56 | −0.04 | +21.34 | +6.16 | −3.52 | +39.15 | +47.02 |

**Tiers.** (a), (b) and (f) are all **DEAD**. Each passes net ≥ 4.5 %, worst month > −5 %, NW t ≥ 2, lag > 0 and break-even ≥ 1.5×, and fails "positive every full year".
ALIVE-forward needs exactly one negative year above −2 %; (a) has two non-positive years, one of them −2.42 %. The verdict is unchanged across the three borrowability rules,
so the amendment to B (frozen rates) did not decide anything. It excluded 110 R5 position-days.

## 3. Reading it

1. **The R9 prior stays falsified, with less cushion.** Borrow does not absorb the gross: net is +18.6 %/yr after paying the historical rates. But those rates cost 18.1 %/yr of
   capital, against 7.9 % at today's rates, so the break-even falls from 2.83× to 1.78×.
2. **Rates spike when the trade is best.** The mean paid rate is 151 %/yr on held days with FUND7 ≤ −0.10 % (1,919 days) and 50 %/yr on the other held days (4,567 days). R9's
   caveat ("borrow demand rises in exactly the squeezes where funding is most negative") is now measured.
3. **Point-in-time borrowability matters more than the rates.** Today's list (row b) drops coins that were borrowable then and delisted since, and those were among the most
   profitable episodes. That costs 6.6 %/yr.
4. **The best results are crisis events, and the backtest's weakest assumption sits there.**
   - ALPACA (2025-04-26 → 05-02, around its spot delisting) is +33 pp of capital, of which +29.7 pp is perp-vs-spot divergence. The published borrow rate was 920–1,325 %/yr.
   - FTT in the FTX-collapse week is +6.7 pp.
   - The backtest assumes a short could be borrowed at the published rate during those weeks. A rate existing does not show the pool had coins to lend.
   - Without ALPACA, 2025 is +5.4 % instead of +38.4 %.
5. **What remains outside the outliers is a modest, uneven carry:** about +10–13 %/yr, NW t ≈ 3.8, ~0 in 2021, negative in 2024. Capacity (VIP0 limits $1.5k–5.6k per
   coin, so ~$10–25k deployable) and the operational risks in R9 §3 are unchanged.

## 4. Diagnostics (after the verdict; they change no verdict)

| check | result |
|---|---|
| snapshot vs the rate in effect at its fetch time (21:11:39 UTC) | **153 of 153 assets exact** (< 0.1 %). Median deviation: 0.00 % at the fetch time vs 2.6 % for the day mean. The registered check failed because it compared a point-in-time snapshot with a day mean in the intraday-rate regime, not because of a mapping or unit error. |
| top coins (cumulative pp of capital) | ALPACA +33.0 (7 d; funding +4.6, basis +29.7, borrow −1.3; rate 920 %/yr mean, 1,325 max). DEXE +8.2 (34 d, 2026-07/08; funding +6.8; rate 66 %/yr). FTT +6.7 (7 d, 2022-11-09 → 11-15; funding +3.5, basis +3.3). Top-3 share 43.8 %; 191 coins held in total. |
| without ALPACA | +12.99 %/yr, NW t 3.75, worst month −1.79 %; 2021 −0.04, 2024 −2.42, 2025 +5.41 |
| without the top three | +10.46 %/yr, NW t 3.88; 2021 −0.04, 2024 −2.42 |

## 5. What this settles

- **`CARRY−` is closed** by the pre-registered decision table: DEAD in every borrowability variant, with the verdict labelled STOPPED because a registered check was
  mis-specified, as the diagnostic shows. No bar is changed after the numbers.
- **R9's open question is answered.** The past borrow rates were 2–3× today's on the coins this trade holds, and highest when funding is most negative. A small-account carry
  of ~10–13 %/yr survives them outside crisis events, but not every year.
- **Not settled, and not pursued:** whether borrow was available at the published rates during squeezes (needs order-level history, which does not exist publicly).
  A forward version would be a new registration scored only on data after 2026-10, at a capacity of a few thousand dollars per coin.

## Reproduce

```
cd runs/harness_xsec
PY=/home/nkout/projects/binance2/binance2/.venv/bin/python
$PY test_r9h.py                                   # 19/19
$PY run_r9h.py assets                             # R5 panels -> r9h_assets.json (run as a capped systemd-run unit; see memory note on oomd)
source ../../keys.sh && $PY run_r9h.py fetch      # read-only key; resumable cache in data/borrow_api/
$PY run_r9h.py score > r9h_output.txt             # -> r9h_results.json
$PY r9h_diagnostics.py > r9h_diagnostics.txt
```
