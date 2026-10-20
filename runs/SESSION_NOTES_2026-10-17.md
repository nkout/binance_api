# Session notes — 2026-10-17

Standalone state doc. Start here, then `next_signal_ideas.md` (Rounds 9–10: pre-registrations and status) and
`PROGRAMME_SUMMARY.md` (closing write-up through Round 8; it does not yet cover Rounds 9–10).

---

## Where things stand

**Still no profitable strategy.** Today closed one branch, fixed the liquidation data, and left one test ready to run.

| item | state | doc |
|---|---|---|
| **R9-H** `CARRY−` on historical borrow rates | **DEAD** on every tier, so `CARRY−` is closed. Historical borrow cost 18.1 %/yr of capital, against 7.9 % at today's rates. | `r9h_borrow_history.analysis.md` |
| **Liquidation stream** (`@forceOrder`) | Dead in every collector since 2026-04-23 17:03:30 UTC. Fixed in the new **v6** collector, which also marks every bar with `opt_force_order_live` (2026-10-20). v5 is back to its original code. **Not yet deployed.** | `new_features.md` §0 |
| **Readers** | Liquidation columns count as data only on v6 rows with `opt_force_order_live` = 1, plus rows from before the stream died. Everything else is NaN, and no restart date is needed. | `liq_cutoff.py` |
| **N4-S** settlement move at sub-second resolution | Pre-registered and built; tests pass 13/13 and 11/11. **Not run**, and no result has been seen. | `next_signal_ideas.md` Round 10 |
| V1 DVOL forward ledger | 52 forward days, 22 complete, rule flat on all. A verdict needs ≥ 180 complete days, around early 2027. | `harness_vrp/forward_status.txt` |

## Step 1 — update the collector (collection machine)

1. **Get the code.** In the collector's checkout, run `git pull`. The July log shows the machine running **v4**
   (`python binance_live_orderbook_v4.py --window-sec=5`).
2. **Run v6 with the same options as before:** `python binance_live_orderbook_v6.py --window-sec=5`. No API key and no
   environment variables; every endpoint is public. v6 writes the same schema-5 bars as v4/v5 plus one trailing column,
   `opt_force_order_live`, and a raw event stream (`--no-events` gives bars only).
   - Readers use liquidations only from v6 bars, so a v1–v5 collector on the fixed route would collect them for nothing.
3. **Stop the old process and start v6.** Python loads the code at start, so a process left running keeps writing zeros.
4. **Check it is live.**
   - **At start:** the log shows `Connected to force exit stream wss://fstream.binance.com/market/stream?streams=btcusdt@forceOrder/btcusdt@markPrice@1s`,
     then `liquidation feed live` within a few seconds.
   - **Afterwards,** the log line `liquidation feed silent for more than 5s` marks every gap; those bars get 0.
   - **After about an hour:**
     ```
     python liq_cutoff.py out5.w5.<new tag>.*.csv*
     ```
     `live` should be close to `rows`, and `with liquidations` > 0. Before the outage, BTC typically had a liquidation
     every minute or so; the longest quiet stretch in April 2026 was about 1 h.
5. Nothing to pin or commit afterwards. Aggregate v6 files separately from older ones: `aggregate_bars.py` refuses to mix
   the two column sets.

## Step 2 — run the N4-S notebook on Colab

1. Download `runs/n4s_probe.ipynb` from the repo. On colab.research.google.com use File → Upload notebook.
2. Runtime → Change runtime type → **CPU** (no GPU needed). Then Runtime → **Run all**, and allow the Google Drive mount.
3. *(Optional head start.)* Copy the 170 local extracts (`data/n4s/*.npz`, 4.7 MB) into `MyDrive/n4s_probe/extracts/` first;
   those coin-days are then skipped.
4. **Cell 5 checks that the runtime can reach `data.binance.vision`.** If it stops there, use Runtime → Disconnect and
   delete runtime, then Run all again. Nothing has been written at that point.
5. **The fetch cell takes most of the time.** Expect roughly 30–90 minutes; this hasn't been timed on Colab. It prints an
   ETA every 50 coin-days. If the runtime disconnects, **Run all again**: finished coin-days are cached on Drive.
6. **Bring back** `n4s_output.txt`, `n4s_results.json` and `n4s_events.csv` from `MyDrive/n4s_probe/`, or the executed
   notebook.

**What decides it** (pre-registered, primary entry at s + 0.5 s, net of 9 bp):

- **Replication gate first.** N4-X's s + 2 min trade must be rebuilt within 0.5 bp on ≥ 99 % of events. Otherwise the
  verdict is STOPPED, which points to a reader problem, not a result.
- **PASS needs all five:**
  1. Net mean > 0, with the clustered CI lower bound > 0.
  2. Median > 0.
  3. Mean > 0 in 2025 and in 2026.
  4. Mean > 0 at Δ = 1 s.
  5. At least 1,800 events.
- **PASS →** N4-L: a forward ledger of live aggTrades around settlements, then a small live pilot. Each is a new
  registration.
- **FAIL →** N4 is closed for good at retail latency. The Δ = 0.1 s and 0.25 s rows show whether co-location would matter.

## Later, optional

- **V1 forward ledger refresh** (any time):
  1. `runs/harness_v1_feas/fetch_klines.py`
  2. `runs/harness_vrp/fetch_dvol.py`
  3. `runs/harness_vrp/forward_score.py`
- **Liquidation data, once live:** liquidation features need months of collection before any test. Binance publishes no
  liquidation history, so this collector is the only source.
- **Not built:** Q1 (squeeze risk from the `metrics` archive; pre-registered in Round 9).
- **Not pre-registered:** an R6 `A_long` forward test.
- `PROGRAMME_SUMMARY.md`: add Rounds 9–10.
- *(Optional)* Rebuild `data/v1_year/v1_15s.parquet` with the liquidation mask (`extract_v1_15s.py`, about 10–15 min).
  Nothing reads those columns now.
