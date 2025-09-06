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
from collections import deque
import time

SPOT_SYMBOL = "BTCUSDC"
outfile="out.csv"

tolerance = 0.000001
liq_steps = [0.0, 0.005, 0.01, 0.02, 0.03, 0.04]
window_sec = 15
TRADE_AGG_INTERVAL = 0.1  # seconds (100ms)

processes = []

def aggregate_liquidity(update_queue, liq_steps, order_book):
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

async def fetch_snapshot(spot_snapshot_url, order_book):
    """Fetch snapshot and normalize keys to 'b' and 'a'."""
    attempt = 1
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(spot_snapshot_url) as resp:
                    data = await resp.json()
                    # Normalize keys
                    bids = data.get('b') or data.get('bids')
                    asks = data.get('a') or data.get('asks')
                    if bids is None or asks is None:
                        raise KeyError("'b' or 'a' missing in snapshot data")
                    order_book['bids'] = {Decimal(p): Decimal(q) for p, q in bids}
                    order_book['asks'] = {Decimal(p): Decimal(q) for p, q in asks}
                    order_book['last_update'] = data['lastUpdateId']
                    print(f"Snapshot loaded: {len(order_book['bids'])} bids, {len(order_book['asks'])} asks")
                    return
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}, retrying in {2 ** attempt}s...")
            await asyncio.sleep(2 ** attempt)
            attempt += 1


async def handle_orderbook_ws(update_queue, spot_snapshot_url, spot_prices_ws_url, order_book):
    while True:
        try:
            async with websockets.connect(spot_prices_ws_url) as ws:
                print("Orderbook WebSocket connected")
                async for message in ws:
                    data = json.loads(message)
                    if 'u' not in data or 'U' not in data:
                        continue

                    # Resync if missed updates
                    if order_book['last_update'] is None or data['U'] > order_book['last_update'] + 1:
                        print("Missed updates, resyncing snapshot...")
                        await fetch_snapshot(spot_snapshot_url, order_book)
                        continue

                    if data['u'] < order_book['last_update'] + 1:
                        print("ignore past update")
                        continue

                    order_book['last_update'] = data['u']

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

                    aggregate_liquidity(update_queue, liq_steps, order_book)
        except Exception as e:
            print(f"WebSocket disconnected, retrying... {e}")
            await asyncio.sleep(2)

def subscriber_shutdown():
    sys.exit(0)

async def subscribe_orderbook(update_queue, spot_snapshot_url, spot_prices_ws_url):
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, subscriber_shutdown)

    order_book = {'bids': {}, 'asks': {}, 'last_update': None}

    await fetch_snapshot(spot_snapshot_url, order_book)
    await handle_orderbook_ws(update_queue, spot_snapshot_url, spot_prices_ws_url, order_book)

    while True:
        await asyncio.sleep(1)

def orderbook_subscriber(queue, spot_snapshot_url, spot_prices_ws_url):
    asyncio.run(subscribe_orderbook(queue, spot_snapshot_url, spot_prices_ws_url))

async def trade_ws(spot_trade_ws_url, trade_buffer):
    latest_id = 0
    while True:
        try:
            async with websockets.connect(spot_trade_ws_url) as ws:
                print("Connected to trade stream")
                async for message in ws:
                    data = json.loads(message)
                    price = Decimal(data['p'])
                    qty = Decimal(data['q'])
                    is_buyer_maker = data['m']  # True = sell, False = buy
                    if latest_id > 0 and data['t'] != latest_id + 1:
                        print("gap......")
                    latest_id  = data['t']
                    trade_buffer.append((price, qty, is_buyer_maker))
        except Exception as e:
            print(f"Trade WebSocket error: {e}, reconnecting in 2s...")
            await asyncio.sleep(2)

async def trade_aggregator(update_queue, trade_buffer):
    while True:
        await asyncio.sleep(TRADE_AGG_INTERVAL)
        if not trade_buffer:
            continue

        total_buy = Decimal(0)
        total_sell = Decimal(0)
        total_buy_amount = Decimal(0)
        total_sell_amount = Decimal(0)
        total_volume = Decimal(0)

        out = {'type': 'trade'}
        buy_samples = 0
        sell_samples = 0
        while trade_buffer:
            price, qty, is_buyer_maker = trade_buffer.popleft()
            total_volume += qty
            if is_buyer_maker:  # Sell
                total_sell += qty
                total_sell_amount += price * qty
                sell_samples += 1
            else:  # Buy
                total_buy += qty
                total_buy_amount += price * qty
                buy_samples += 1
        out['buy_qty'] = float(total_buy)
        out['sell_qty'] = float(total_sell)
        out['buy_amount'] = float(total_buy_amount)
        out['sell_amount'] = float(total_sell_amount)
        out['buy_samples'] = buy_samples
        out['sell_samples'] = sell_samples
        update_queue.put(out)

async def subscribe_trade(update_queue, spot_trade_ws_url):
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, subscriber_shutdown)

    trade_buffer = deque()
    await asyncio.gather(trade_ws(spot_trade_ws_url, trade_buffer), trade_aggregator(update_queue, trade_buffer))

    while True:
        await asyncio.sleep(1)

def trade_subscriber(queue, spot_trade_ws_url):
    asyncio.run(subscribe_trade(queue, spot_trade_ws_url))

def init_stats(stats):
    stats["spot_best_prices"] = [[],[],[]]
    stats['spot_liq_levels'] = []
    for p in liq_steps:
        stats['spot_liq_levels'].append([[],[],[]])
    stats['spot_trades'] = [[],[],[],[],[],[]]

def update_stats(stats, update):
    if update['type'] == 'prices':
        stats["spot_best_prices"][0].append(update['best_prices'][0])
        stats["spot_best_prices"][1].append(update['best_prices'][1])
        stats["spot_best_prices"][2].append(update['best_prices'][1] - update['best_prices'][0])

        for c, p in enumerate(liq_steps):
            stats['spot_liq_levels'][c][0].append(update['liq_levels'][c][0])
            stats['spot_liq_levels'][c][1].append(update['liq_levels'][c][1])
            stats['spot_liq_levels'][c][2].append(update['liq_levels'][c][1] - update['liq_levels'][c][0])
    elif update['type'] == 'trade':
        stats["spot_trades"][0].append(update['buy_qty'])
        stats["spot_trades"][1].append(update['sell_qty'])
        stats["spot_trades"][2].append(update['buy_amount'])
        stats["spot_trades"][3].append(update['sell_amount'])
        stats["spot_trades"][4].append(update['buy_samples'])
        stats["spot_trades"][5].append(update['sell_samples'])
    else:
        print('wrong update type')

def reset_stats(stats):
    stats["spot_best_prices"][0].clear()
    stats["spot_best_prices"][1].clear()
    stats["spot_best_prices"][2].clear()

    for c, p in enumerate(liq_steps):
        stats['spot_liq_levels'][c][0].clear()
        stats['spot_liq_levels'][c][1].clear()
        stats['spot_liq_levels'][c][2].clear()

    stats["spot_trades"][0].clear()
    stats["spot_trades"][1].clear()
    stats["spot_trades"][2].clear()
    stats["spot_trades"][3].clear()
    stats["spot_trades"][4].clear()
    stats["spot_trades"][5].clear()

def calc_stats(line):
    arr = np.array(line)
    min_val = np.min(arr).item()
    max_val = np.max(arr).item()
    #mean_val = np.mean(arr).item()
    #q25 = np.percentile(arr, 25).item()
    q50 = np.percentile(arr, 50).item()
    #q75 = np.percentile(arr, 75).item()
    #return [len(line), line[0], line[-1], mean_val, min_val,  q25, q50, q75, max_val]
    return [len(line), line[0], line[-1], min_val, q50, max_val]

def stats_header():
    #return ['samples', 'open', 'close', 'mean', 'min', '25perc', '50perc', '75perc', 'max']
    return ['samples', 'open', 'close', 'min', 'median', 'max']

def get_header():
    header = ['datetime', 'timestamp', 'spot_price_diff']
    header += ['spot_buy_samples', 'spot_sell_samples', 'spot_buy_qty', 'spot_sell_qty', 'spot_buy_vwap', 'spot_sell_vwap']
    header += ['spot_bid_'+x for x in stats_header()]
    header += ['spot_ask_' + x for x in stats_header()]
    header += ['spot_spread_' + x for x in stats_header()]

    for c, p in enumerate(liq_steps):
        header += [f'spot_bid_liq_{p:.4}_' + x for x in stats_header()]
        header += [f'spot_ask_liq_{p:.4}_' + x for x in stats_header()]
        header += [f'spot_diff_liq_{p:.4}_'  + x for x in stats_header()]

    return header

def get_stats(now, stats):
    if len(stats["spot_best_prices"][0]) == 0 or len(stats["spot_trades"][0]) == 0:
        return None

    line = [now.strftime("%Y-%m-%d %H:%M:%S"), str(int(now.timestamp()))]
    line += [(stats["spot_best_prices"][0][-1] +stats["spot_best_prices"][1][-1])/2.0 - (stats["spot_best_prices"][0][0] +stats["spot_best_prices"][1][0])/2.0]

    buy_qty = sum(stats["spot_trades"][0])
    sell_qty = sum(stats["spot_trades"][1])
    buy_vwap = sum(stats["spot_trades"][2]) / buy_qty if buy_qty > tolerance else 0.0
    sell_vwap = sum(stats["spot_trades"][3]) / sell_qty if sell_qty > tolerance else 0.0
    line += [sum(stats["spot_trades"][4]), sum(stats["spot_trades"][5]), buy_qty, sell_qty, buy_vwap, sell_vwap]

    line += calc_stats(stats["spot_best_prices"][0])
    line += calc_stats(stats["spot_best_prices"][1])
    line += calc_stats(stats["spot_best_prices"][2])

    for c, p in enumerate(liq_steps):
        line += calc_stats(stats['spot_liq_levels'][c][0])
        line += calc_stats(stats['spot_liq_levels'][c][1])
        line += calc_stats(stats['spot_liq_levels'][c][2])

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
                print(f"stats calculation exception {type(e).__name__}: {e}")

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

    spot_snapshot_url = f"https://api.binance.com/api/v3/depth?symbol={SPOT_SYMBOL.upper()}&limit=5000"
    spot_prices_ws_url = f"wss://stream.binance.com:9443/ws/{SPOT_SYMBOL.lower()}@depth@100ms"
    spot_trade_ws_url = f"wss://stream.binance.com:9443/ws/{SPOT_SYMBOL.lower()}@trade"

    q = mp.Queue()
    p1 = mp.Process(target=orderbook_subscriber, args=(q,spot_snapshot_url, spot_prices_ws_url))
    p2 = mp.Process(target=trade_subscriber, args=(q, spot_trade_ws_url))
    p3 = mp.Process(target=stats_calculator, args=(q,))

    processes.append(p1)
    processes.append(p2)
    processes.append(p3)

    p1.start()
    p2.start()
    p3.start()

    # just idle until stopped
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(my_main())
