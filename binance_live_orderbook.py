import asyncio
import aiohttp
import websockets
import json
from decimal import Decimal
import multiprocessing as mp
import numpy as np
import datetime
import csv
import signal
import sys

SYMBOL = "BTCUSDC"
SNAPSHOT_URL = f"https://api.binance.com/api/v3/depth?symbol={SYMBOL}&limit=5000"
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@depth@100ms"
outfile="out.txt"

order_book = {'bids': {}, 'asks': {}}
last_update_id = None
tolerance = 0.000001
liq_steps = [x * 0.005 for x in range(5)]
window_sec = 15

processes = []

def aggregate_liquidity(update_queue, liq_steps):
    """Aggregate liquidity at steps above/below best bid/ask."""
    if not order_book['bids'] or not order_book['asks']:
        return

    best_bid = max(order_book['bids'])
    best_ask = min(order_book['asks'])

    out = {'type': 'prices'}
    out['best_prices'] = [float(best_bid), float(best_ask)]
    out['liq_levels'] = []

    for p in liq_steps:
        threshold_bid = best_bid * (1 - Decimal((p + tolerance)/ 100))
        threshold_ask = best_ask * (1 + Decimal((p + tolerance)/ 100))
        liquidity_bid = sum(q for price, q in order_book['bids'].items() if price >= threshold_bid)
        liquidity_ask = sum(q for price, q in order_book['asks'].items() if price <= threshold_ask)
        out['liq_levels'].append([float(liquidity_bid), float(liquidity_ask)])

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

                    if data['u'] < last_update_id + 1:
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

                    aggregate_liquidity(update_queue, liq_steps)
        except Exception as e:
            print(f"WebSocket disconnected, retrying... {e}")
            await asyncio.sleep(2)

def subscribe_orderbook_shutdown():
    sys.exit(0)

async def subscribe_orderbook(update_queue):
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, subscribe_orderbook_shutdown)

    await fetch_snapshot()
    await handle_ws(update_queue)

    while True:
        await asyncio.sleep(1)

def orderbook_subscriber(queue):
    asyncio.run(subscribe_orderbook(queue))

def init_stats(stats):
    stats["best_prices"] = [[],[],[]]
    stats['liq_levels'] = []
    for p in liq_steps:
        stats['liq_levels'].append([[],[],[]])

def update_stats(stats, update):
    if update['type'] == 'prices':
        stats["best_prices"][0].append(update['best_prices'][0])
        stats["best_prices"][1].append(update['best_prices'][1])
        stats["best_prices"][2].append(update['best_prices'][1] - update['best_prices'][0])

        for c, p in enumerate(liq_steps):
            stats['liq_levels'][c][0].append(update['liq_levels'][c][0])
            stats['liq_levels'][c][1].append(update['liq_levels'][c][1])
            stats['liq_levels'][c][2].append(update['liq_levels'][c][1] - update['liq_levels'][c][0])
    else:
        print('wrong update type')

def reset_stats(stats):
    stats["best_prices"][0].clear()
    stats["best_prices"][1].clear()
    stats["best_prices"][2].clear()

    for c, p in enumerate(liq_steps):
        stats['liq_levels'][c][0].clear()
        stats['liq_levels'][c][1].clear()
        stats['liq_levels'][c][2].clear()

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

def get_header():
    header = ['datetime', 'timestamp']
    header += ['bid_'+x for x in stats_header()]
    header += ['ask_' + x for x in stats_header()]
    header += ['spread_' + x for x in stats_header()]


    for c, p in enumerate(liq_steps):
        header += [f'bid_liq_{p:.4}_' + x for x in stats_header()]
        header += [f'ask_liq_{p:.4}_' + x for x in stats_header()]
        header += [f'diff_liq_{p:.4}_'  + x for x in stats_header()]

    return header

def get_stats(now, stats):
    if len(stats["best_prices"][0]) == 0:
        return None

    line = [now.strftime("%Y-%m-%d %H:%M:%S"), str(int(now.timestamp()))]
    line += calc_stats(stats["best_prices"][0])
    line += calc_stats(stats["best_prices"][1])
    line += calc_stats(stats["best_prices"][2])

    for c, p in enumerate(liq_steps):
        line += calc_stats(stats['liq_levels'][c][0])
        line += calc_stats(stats['liq_levels'][c][1])
        line += calc_stats(stats['liq_levels'][c][2])

    return line

def round_to_interval(dt, seconds):
    # Convert to seconds since epoch
    epoch = dt.timestamp()
    # Floor to nearest interval
    rounded = (epoch // seconds) * seconds
    return datetime.datetime.fromtimestamp(rounded, tz=dt.tzinfo)

def stats_calculator_shutdown(sig, frame=None):
    sys.exit(0)

def stats_calculator(update_queue):
    """
    Process B: reads best bid/ask from queue and calculates liquidity
    """
    signal.signal(signal.SIGINT, stats_calculator_shutdown)
    signal.signal(signal.SIGTERM, stats_calculator_shutdown)

    stats = {}
    init_stats(stats)
    header_printed = False
    exit_received = False

    last_print = round_to_interval(datetime.datetime.now(), window_sec)

    with open(outfile, "w") as f:
        writer = csv.writer(f, delimiter=';')
        while not exit_received:
            try:
                update = update_queue.get(timeout=1)
                now = datetime.datetime.now()

                if (now - last_print).total_seconds() > window_sec + tolerance:
                    if not header_printed:
                        print(get_header())
                        writer.writerow(get_header())
                        header_printed = True
                    line = get_stats(last_print, stats)
                    if line:
                        print(line)
                        writer.writerow(line)
                    reset_stats(stats)
                    last_print = round_to_interval(now, window_sec)

                update_stats(stats, update)
            except KeyboardInterrupt:
                exit_received = True
            except Exception as e:
                print(f"stats calculation exception: {e}")

def my_main_shutdown():
    print("Shutting down all processes...")
    for p in processes:
        if p.is_alive():
            p.terminate()
    sys.exit(0)

async def my_main():
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, my_main_shutdown)

    q = mp.Queue()
    p1 = mp.Process(target=orderbook_subscriber, args=(q,))
    p2 = mp.Process(target=stats_calculator, args=(q,))

    processes.append(p1)
    processes.append(p2)

    p1.start()
    p2.start()

    # just idle until stopped
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(my_main())
