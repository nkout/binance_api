import asyncio
import aiohttp
import websockets
import json
from decimal import Decimal
import multiprocessing as mp
import numpy as np
import datetime
import csv

SYMBOL = "BTCUSDC"
SNAPSHOT_URL = f"https://api.binance.com/api/v3/depth?symbol={SYMBOL}&limit=5000"
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@depth@100ms"
outfile="out.txt"

order_book = {'bids': {}, 'asks': {}}
last_update_id = None
tolerance = 0.000001
step_percent=0.005
max_percent=0.010
window_msec = 15000


def aggregate_liquidity(update_queue, step_percent, max_percent):
    """Aggregate liquidity at steps above/below best bid/ask."""
    if not order_book['bids'] or not order_book['asks']:
        return

    best_bid = max(order_book['bids'])
    best_ask = min(order_book['asks'])

    out = {}
    out['best_prices'] = [float(best_bid), float(best_ask)]
    out['liq_levels'] = []

    p = 0.0
    while p <= max_percent + tolerance:
        threshold_bid = best_bid * (1 - Decimal((p + tolerance)/ 100))
        threshold_ask = best_ask * (1 + Decimal((p + tolerance)/ 100))
        liquidity_bid = sum(q for price, q in order_book['bids'].items() if price >= threshold_bid)
        liquidity_ask = sum(q for price, q in order_book['asks'].items() if price <= threshold_ask)
        out['liq_levels'].append([float(liquidity_bid), float(liquidity_ask)])
        p += step_percent
    update_queue.put(out)
    #print(out)

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

                    aggregate_liquidity(update_queue, step_percent, max_percent)
        except Exception as e:
            print(f"WebSocket disconnected, retrying... {e}")
            await asyncio.sleep(2)


async def subscribe_orderbook(update_queue):
    await fetch_snapshot()
    await handle_ws(update_queue)

def orderbook_subscriber(queue):
    asyncio.run(subscribe_orderbook(queue))

def init_stats(stats):
    stats["best_prices"] = [[],[],[]]
    stats['liq_levels'] = []
    p = 0.0
    while p <= max_percent + tolerance:
        stats['liq_levels'].append([[],[],[]])
        p += step_percent

def update_stats(stats, update):
    stats["best_prices"][0].append(update['best_prices'][0])
    stats["best_prices"][1].append(update['best_prices'][1])
    stats["best_prices"][2].append(update['best_prices'][1] - update['best_prices'][0])

    p = 0.0
    c = 0
    while p <= max_percent + tolerance:
        stats['liq_levels'][c][0].append(update['liq_levels'][c][0])
        stats['liq_levels'][c][1].append(update['liq_levels'][c][1])
        stats['liq_levels'][c][2].append(update['liq_levels'][c][1] - update['liq_levels'][c][0])
        p += step_percent
        c += 1

def reset_stats(stats):
    stats["best_prices"][0].clear()
    stats["best_prices"][1].clear()
    stats["best_prices"][2].clear()

    p = 0.0
    c = 0
    while p <= max_percent + tolerance:
        stats['liq_levels'][c][0].clear()
        stats['liq_levels'][c][1].clear()
        stats['liq_levels'][c][2].clear()
        p += step_percent
        c += 1

def calc_stats(line):
    arr = np.array(line)
    min_val = np.min(arr).item()
    max_val = np.max(arr).item()
    mean_val = np.mean(arr).item()
    q25 = np.percentile(arr, 25).item()
    q50 = np.percentile(arr, 50).item()
    q75 = np.percentile(arr, 75).item()
    return [len(line), line[0], line[-1], mean_val, min_val,  q25, q50, q75, max_val]

def stats_header():
    return ['samples', 'open', 'close', 'mean', 'min', '25perc', '50perc', '75perc', 'max']

def print_header():
    header = ['timestamp']
    header += ['bid_'+x for x in stats_header()]
    header += ['ask_' + x for x in stats_header()]
    header += ['spread_' + x for x in stats_header()]

    p = 0.0
    c = 0
    while p <= max_percent + tolerance:
        header += [f'bid_liq_{p:.4}_' + x for x in stats_header()]
        header += [f'ask_liq_{p:.4}_' + x for x in stats_header()]
        header += [f'diff_liq_{p:.4}_'  + x for x in stats_header()]
        p += step_percent
        c += 1

    return header

def print_stats(now, stats):
    line = [str(int(now.timestamp() * 1000))]
    line += calc_stats(stats["best_prices"][0])
    line += calc_stats(stats["best_prices"][1])
    line += calc_stats(stats["best_prices"][2])

    p = 0.0
    c = 0
    while p <= max_percent + tolerance:
        line += calc_stats(stats['liq_levels'][c][0])
        line += calc_stats(stats['liq_levels'][c][1])
        line += calc_stats(stats['liq_levels'][c][2])
        p += step_percent
        c += 1

    return line

def stats_calculator(update_queue):
    """
    Process B: reads best bid/ask from queue and calculates liquidity
    """
    stats = {}
    init_stats(stats)
    header_printed = False

    last_print = datetime.datetime.now()

    with open(outfile, "w") as f:
        writer = csv.writer(f, delimiter=';')
        while True:
            try:
                update = update_queue.get(timeout=5)
                update_stats(stats, update)
                now = datetime.datetime.now()

                if (now - last_print).total_seconds() * 1000 > window_msec:
                    if not header_printed:
                        print(print_header())
                        writer.writerow(print_header())
                        header_printed = True
                    line = print_stats(now, stats)
                    print(line)
                    writer.writerow(line)
                    reset_stats(stats)
                    last_print = now
                # Here you can add liquidity calculations
            except Exception:
                print("No update received in 5s")

if __name__ == "__main__":
    q = mp.Queue()
    p1 = mp.Process(target=orderbook_subscriber, args=(q,))
    p2 = mp.Process(target=stats_calculator, args=(q,))
    p1.start()
    p2.start()
    p1.join()
    p2.join()
