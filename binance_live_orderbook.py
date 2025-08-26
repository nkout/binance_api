import asyncio
import aiohttp
import websockets
import json
from decimal import Decimal
import multiprocessing as mp
import numpy as np


SYMBOL = "BTCUSDC"
SNAPSHOT_URL = f"https://api.binance.com/api/v3/depth?symbol={SYMBOL}&limit=5000"
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@depth@100ms"

order_book = {'bids': {}, 'asks': {}}
last_update_id = None


def aggregate_liquidity(update_queue, step_percent=0.005, max_percent=0.08):
    """Aggregate liquidity at steps above/below best bid/ask."""
    if not order_book['bids'] or not order_book['asks']:
        return

    best_bid = max(order_book['bids'])
    best_ask = min(order_book['asks'])

    out = {}
    out['bbid'] = float(best_bid)
    out['bask'] = float(best_ask)
    #out['liq'] = []

    p = 0 + 0.00001
    while p <= max_percent:
        threshold_bid = best_bid * (1 - Decimal(p / 100))
        threshold_ask = best_ask * (1 + Decimal(p / 100))
        liquidity_bid = sum(q for price, q in order_book['bids'].items() if price >= threshold_bid)
        liquidity_ask = sum(q for price, q in order_book['asks'].items() if price <= threshold_ask)
        #out['liq'].append([liquidity_bid, liquidity_ask])
        p += step_percent
    update_queue.put(out)

async def fetch_snapshot():
    """Fetch snapshot and normalize keys to 'b' and 'a'."""
    global order_book, last_update_id
    attempt = 1
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(SNAPSHOT_URL) as resp:
                    data = await resp.json()
                    # Normalize keys
                    bids = data.get('b') or data.get('bids')
                    asks = data.get('a') or data.get('asks')
                    if bids is None or asks is None:
                        raise KeyError("'b' or 'a' missing in snapshot data")
                    order_book['bids'] = {Decimal(p): Decimal(q) for p, q in bids}
                    order_book['asks'] = {Decimal(p): Decimal(q) for p, q in asks}
                    last_update_id = data['lastUpdateId']
                    print(f"Snapshot loaded: {len(order_book['bids'])} bids, {len(order_book['asks'])} asks")
                    return
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}, retrying in {2 ** attempt}s...")
            await asyncio.sleep(2 ** attempt)
            attempt += 1


async def handle_ws(update_queue):
    global order_book, last_update_id
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                print("WebSocket connected")
                async for message in ws:
                    data = json.loads(message)
                    if 'u' not in data or 'U' not in data:
                        continue

                    # Resync if missed updates
                    if last_update_id is None or data['U'] > last_update_id + 1:
                        print("Missed updates, resyncing snapshot...")
                        await fetch_snapshot()
                        continue

                    if data['U'] < last_update_id + 1:
                        print("ignore past update")
                        continue

                    last_update_id = data['u']

                    # Update bids
                    for price_str, qty_str in data.get('b', []):
                        price = Decimal(price_str)
                        qty = Decimal(qty_str)
                        if qty == 0:
                            order_book['bids'].pop(price, None)
                        else:
                            order_book['bids'][price] = qty

                    # Update asks
                    for price_str, qty_str in data.get('a', []):
                        price = Decimal(price_str)
                        qty = Decimal(qty_str)
                        if qty == 0:
                            order_book['asks'].pop(price, None)
                        else:
                            order_book['asks'][price] = qty

                    aggregate_liquidity(update_queue)
        except Exception as e:
            print(f"WebSocket disconnected, retrying... {e}")
            await asyncio.sleep(2)


async def main(update_queue):
    await fetch_snapshot()
    await handle_ws(update_queue)

def producer(queue):
    asyncio.run(main(queue))

def consumer(update_queue):
    """
    Process B: reads best bid/ask from queue and calculates liquidity
    """
    stats = {}
    stats["bbid"] = []
    while True:
        try:
            update = update_queue.get(timeout=5)
            stats["bbid"].append(update['bbid'])
            if len(stats["bbid"]) > 20:
                arr = np.array(stats["bbid"])
                min_val = np.min(arr)
                max_val = np.max(arr)
                mean_val = np.mean(arr)
                median_val = np.median(arr)
                q25 = np.percentile(arr, 25)
                q50 = np.percentile(arr, 50)  # same as median
                q75 = np.percentile(arr, 75)
                stats_list = [min_val, max_val, mean_val, median_val, q25, q50, q75]
                print("Stats:", stats_list)

                stats["bbid"] = []


            # Here you can add liquidity calculations
        except Exception:
            print("No update received in 5s")

if __name__ == "__main__":
    q = mp.Queue()
    p1 = mp.Process(target=producer, args=(q,))
    p2 = mp.Process(target=consumer, args=(q,))
    p1.start()
    p2.start()
    p1.join()
    p2.join()
