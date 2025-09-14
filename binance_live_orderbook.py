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
FUTURE_SYMBOL = "BTCUSDT"
outfile="out.csv"

tolerance = 0.000001
liq_steps = [0.0, 0.005, 0.01, 0.02, 0.03, 0.04]
window_sec = 15
thread_interval = 0.2  # seconds (200ms)

processes = []

def aggregate_liquidity(update_queue, liq_steps, order_book, market_type):
    """Aggregate liquidity at steps above/below best bid/ask."""
    if not order_book['bids'] or not order_book['asks']:
        return

    best_bid = max(order_book['bids'])
    best_ask = min(order_book['asks'])

    out = {'type': 'prices', 'market_type': market_type}
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
                        raise KeyError(f"'b' or 'a' missing in snapshot data {data}")
                    order_book['bids'] = {Decimal(p): Decimal(q) for p, q in bids}
                    order_book['asks'] = {Decimal(p): Decimal(q) for p, q in asks}
                    order_book['last_update'] = data['lastUpdateId']
                    print(f"Snapshot loaded {spot_snapshot_url} : {len(order_book['bids'])} bids, {len(order_book['asks'])} asks")
                    return
        except Exception as e:
            print(f"Attempt {attempt} failed on {spot_snapshot_url}: {e}, retrying in {2 ** attempt}s...")
            await asyncio.sleep(2 ** attempt)
            attempt += 1


async def handle_orderbook_ws(update_queue, spot_snapshot_url, spot_prices_ws_url, order_book, market_type):
    first_sync = True

    while True:
        try:
            async with websockets.connect(spot_prices_ws_url) as ws:
                print(f"Orderbook WebSocket connected {spot_prices_ws_url}")
                async for message in ws:
                    data = json.loads(message)
                    if 'u' not in data or 'U' not in data:
                        continue

                    if market_type == "future" and 'pu' not in data:
                        continue

                    # Resync if missed updates
                    if order_book['last_update'] is None:
                        await fetch_snapshot(spot_snapshot_url, order_book)
                        first_sync = True
                        continue

                    if market_type == "spot" or first_sync:
                        if data['U'] > order_book['last_update'] + 1:
                            print(f"Missed updates, resyncing snapshot {spot_snapshot_url}...")
                            order_book['last_update'] = None
                            await fetch_snapshot(spot_snapshot_url, order_book)
                            first_sync = True
                            continue

                        if data['u'] < order_book['last_update'] + 1:
                            print(f"ignore past update of {spot_prices_ws_url}")
                            continue
                    else:
                        if data['pu'] != order_book['last_update']:
                            print(f"Missed future updates, resyncing snapshot {spot_snapshot_url}...")
                            order_book['last_update'] = None
                            await fetch_snapshot(spot_snapshot_url, order_book)
                            first_sync = True
                            continue

                    order_book['last_update'] = data['u']
                    first_sync = False

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

                    aggregate_liquidity(update_queue, liq_steps, order_book, market_type)
        except Exception as e:
            print(f"WebSocket disconnected {spot_prices_ws_url}, retrying... {e}")
            await asyncio.sleep(2)

def subscriber_shutdown():
    sys.exit(0)

async def subscribe_orderbook(update_queue, spot_snapshot_url, spot_prices_ws_url, market_type):
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, subscriber_shutdown)

    order_book = {'bids': {}, 'asks': {}, 'last_update': None}

    await fetch_snapshot(spot_snapshot_url, order_book)
    await handle_orderbook_ws(update_queue, spot_snapshot_url, spot_prices_ws_url, order_book, market_type)

    while True:
        await asyncio.sleep(1)

def orderbook_subscriber(queue, spot_snapshot_url, spot_prices_ws_url, market_type):
    asyncio.run(subscribe_orderbook(queue, spot_snapshot_url, spot_prices_ws_url, market_type))

async def trade_ws(spot_trade_ws_url, trade_buffer):
    latest_id = 0
    while True:
        try:
            async with websockets.connect(spot_trade_ws_url) as ws:
                print(f"Connected to trade stream {spot_trade_ws_url}")
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
            print(f"Trade WebSocket error {spot_trade_ws_url}: {e}, reconnecting in 2s...")
            await asyncio.sleep(2)

async def trade_aggregator(update_queue, trade_buffer, market_type):
    while True:
        await asyncio.sleep(thread_interval)
        if not trade_buffer:
            continue

        total_buy = Decimal(0)
        total_sell = Decimal(0)
        total_buy_amount = Decimal(0)
        total_sell_amount = Decimal(0)
        total_volume = Decimal(0)

        out = {'type': 'trade', 'market_type': market_type}
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

async def subscribe_trade(update_queue, spot_trade_ws_url, market_type):
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, subscriber_shutdown)

    trade_buffer = deque()
    await asyncio.gather(trade_ws(spot_trade_ws_url, trade_buffer), trade_aggregator(update_queue, trade_buffer, market_type))

    while True:
        await asyncio.sleep(1)

def trade_subscriber(queue, spot_trade_ws_url, market_type):
    asyncio.run(subscribe_trade(queue, spot_trade_ws_url, market_type))

async def fetch_long_short_ratio(symbol):
    url = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={symbol.upper()}&period=5m&limit=1"
    attempt = 1
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()

                if data:
                    ratio = float(data[0]["longShortRatio"])
                    #timestamp = int(data[0]["timestamp"])
                    return ratio
        except Exception as e:
            print(f"Attempt {attempt} failed on {url}: {e}, retrying in {2 ** attempt}s...")
            await asyncio.sleep(2 ** attempt)
            attempt += 1

async def poll_ratio(future_symbol, buffer):
    last_poll = round_to_interval(datetime.datetime.now(), window_sec)
    while True:
        await asyncio.sleep(thread_interval)
        now = datetime.datetime.now()
        rounded_now = round_to_interval(now, window_sec)

        if (now - last_poll).total_seconds() > (window_sec + tolerance) and (now - rounded_now).total_seconds() > (window_sec / 2.0 + tolerance):
            ratio = await fetch_long_short_ratio(future_symbol)
            if ratio is not None:
                last_poll = round_to_interval(datetime.datetime.now(), window_sec)
                buffer.append(("long_short_ratio", ratio))


async def fetch_open_interest(symbol):
    url = f'https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol.upper()}'
    attempt = 1
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()

                if data:
                    open_interest = float(data["openInterest"])
                    #timestamp = int(data[0]["timestamp"])
                    return open_interest
        except Exception as e:
            print(f"Attempt {attempt} failed on {url}: {e}, retrying in {2 ** attempt}s...")
            await asyncio.sleep(2 ** attempt)
            attempt += 1

async def poll_open_interest(future_symbol, buffer):
    last_poll = round_to_interval(datetime.datetime.now(), window_sec)
    while True:
        await asyncio.sleep(thread_interval)
        now = datetime.datetime.now()
        rounded_now = round_to_interval(now, window_sec)

        if (now - last_poll).total_seconds() > (window_sec + tolerance) and (now - rounded_now).total_seconds() > (window_sec / 2.0 + tolerance):
            open_interest = await fetch_open_interest(future_symbol)
            if open_interest is not None:
                last_poll = round_to_interval(datetime.datetime.now(), window_sec)
                buffer.append(("open_interest", open_interest))

async def subscribe_force_close(future_symbol, buffer):
    url = f'wss://fstream.binance.com/ws/{future_symbol.lower()}@forceOrder'

    while True:
        try:
            async with websockets.connect(url) as ws:
                print(f"Connected to force exit stream {url}")
                async for message in ws:
                    data = json.loads(message)

                    if 'e' not in data or 'o' not in data or data['e'] != 'forceOrder':
                        print(f"invalid force exit data {data}")
                        continue

                    o_data = data['o']

                    if 'S' not in o_data or 'q' not in o_data:
                        print(f"invalid force exit o_data {data}")
                        continue

                    long_liq = o_data['S'].lower() == "SELL".lower()
                    buffer.append(("force_exit", Decimal(o_data['q']), long_liq))
        except Exception as e:
            print(f"Force exit WebSocket error {url}: {e}, reconnecting in 2s...")
            await asyncio.sleep(2)

async def subscribe_spread(future_symbol, buffer):
    url = f"wss://fstream.binance.com/ws/{future_symbol.lower()}@markPrice"

    while True:
        try:
            async with websockets.connect(url) as ws:
                print(f"Connected to force exit stream {url}")
                async for message in ws:
                    data = json.loads(message)
                    event_time = data["E"]
                    mark_price = float(data["p"])
                    index_price = float(data["i"])
                    funding_rate = float(data["r"])
                    est_funding_rate = float(data["P"])
                    next_funding_time = data["T"]
                    spread = (mark_price - index_price) / index_price * 100
                    remaining_time = next_funding_time / 1000 - datetime.datetime.now().timestamp()
                    buffer.append(("spread", mark_price, index_price, funding_rate, est_funding_rate, spread, remaining_time))
        except Exception as e:
            print(f"Force exit WebSocket error {url}: {e}, reconnecting in 2s...")
            await asyncio.sleep(2)

def get_optional_header_samples():
    return sorted([
        'mark_price_sample',
        'index_price_sample',
        'funding_rate_sample',
        'est_funding_rate_sample',
        'spread_sample',
        'remaining_time_sample',
        'long_short_ratio_sample',
        'open_interest_sample',
    ])

def get_optional_header_sum():
    return sorted([
        'long_force_exit_qty_sum',
        'short_force_exit_qty_sum'
    ])

async def optional_aggregator(update_queue, buffer):
    while True:
        await asyncio.sleep(thread_interval)
        if not buffer:
            continue

        out = {'type': 'optional', 'market_type': "optional"}
        for x in get_optional_header_samples():
            out[x] = []
        for x in get_optional_header_sum():
            out[x] = 0.0

        while buffer:
            data = buffer.popleft()
            update_type = data[0]
            update_data = data[1:]
            if update_type == 'force_exit':
                qty, long_liq = update_data
                if long_liq:
                    out['long_force_exit_qty_sum'] += float(qty)
                else:
                    out['short_force_exit_qty_sum'] += float(qty)
            elif update_type == 'long_short_ratio':
                ratio, = update_data
                out['long_short_ratio_sample'].append(ratio)
            elif update_type == 'open_interest':
                open_interest, = update_data
                out['open_interest_sample'].append(open_interest)
            elif update_type == 'spread':
                mark_price, index_price, funding_rate, est_funding_rate, spread, remaining_time = update_data
                out['mark_price_sample'].append(mark_price)
                out['index_price_sample'].append(index_price)
                out['funding_rate_sample'].append(funding_rate)
                out['est_funding_rate_sample'].append(est_funding_rate)
                out['spread_sample'].append(spread)
                out['remaining_time_sample'].append(remaining_time)
        update_queue.put(out)


async def subscribe_optional(queue, future_symbol):
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, subscriber_shutdown)

    buffer = deque()
    await asyncio.gather(subscribe_force_close(future_symbol, buffer),
                         subscribe_spread(future_symbol, buffer),
                         poll_ratio(future_symbol, buffer),
                         poll_open_interest(future_symbol, buffer),
                         optional_aggregator(queue, buffer))

    while True:
        await asyncio.sleep(1)

def optional_stats_subscriber(queue, future_symbol):
    asyncio.run(subscribe_optional(queue, future_symbol))

def init_stats(stats, market_type):
    stats["best_prices"] = [[],[],[]]
    stats['liq_levels'] = []
    stats['market_type'] = market_type
    for p in liq_steps:
        stats['liq_levels'].append([[],[],[]])
    stats['trades'] = [[],[],[],[],[],[]]

def init_optional_stats(stats):
    for _ in (get_optional_header_samples() + get_optional_header_sum()):
        stats.append([])

def update_stats(stats, update):
    if update['market_type'] != stats['market_type']:
        return

    if update['type'] == 'prices':
        stats["best_prices"][0].append(update['best_prices'][0])
        stats["best_prices"][1].append(update['best_prices'][1])
        stats["best_prices"][2].append(update['best_prices'][1] - update['best_prices'][0])

        for c, p in enumerate(liq_steps):
            stats['liq_levels'][c][0].append(update['liq_levels'][c][0])
            stats['liq_levels'][c][1].append(update['liq_levels'][c][1])
            stats['liq_levels'][c][2].append(update['liq_levels'][c][1] - update['liq_levels'][c][0])
    elif update['type'] == 'trade':
        stats["trades"][0].append(update['buy_qty'])
        stats["trades"][1].append(update['sell_qty'])
        stats["trades"][2].append(update['buy_amount'])
        stats["trades"][3].append(update['sell_amount'])
        stats["trades"][4].append(update['buy_samples'])
        stats["trades"][5].append(update['sell_samples'])
    else:
        print('wrong update type')

def update_optional_stats(stats, update):
    if update['market_type'] != 'optional':
        return
    for i, p in enumerate(get_optional_header_sum() + get_optional_header_samples()):
        if p in (get_optional_header_sum()):
            stats[i].append(update[p])
        else:
            for u in update[p]:
                stats[i].append(u)

def reset_stats(stats):
    stats["best_prices"][0].clear()
    stats["best_prices"][1].clear()
    stats["best_prices"][2].clear()

    for c, p in enumerate(liq_steps):
        stats['liq_levels'][c][0].clear()
        stats['liq_levels'][c][1].clear()
        stats['liq_levels'][c][2].clear()

    stats["trades"][0].clear()
    stats["trades"][1].clear()
    stats["trades"][2].clear()
    stats["trades"][3].clear()
    stats["trades"][4].clear()
    stats["trades"][5].clear()

def reset_optional_stats(stats):
    for i, _ in enumerate(get_optional_header_sum() + get_optional_header_samples()):
        stats[i].clear()

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

def calc_median(line):
    arr = np.array(line)
    q50 = np.percentile(arr, 50).item()
    return q50

def stats_header():
    #return ['samples', 'open', 'close', 'mean', 'min', '25perc', '50perc', '75perc', 'max']
    return ['samples', 'open', 'close', 'min', 'median', 'max']

def get_header():
    header = ['datetime', 'timestamp', 'price_diff']
    header += ['buy_samples', 'sell_samples', 'buy_qty', 'sell_qty', 'buy_vwap', 'sell_vwap']
    header += ['bid_'+x for x in stats_header()]
    header += ['ask_' + x for x in stats_header()]
    header += ['spread_' + x for x in stats_header()]

    for c, p in enumerate(liq_steps):
        header += [f'bid_liq_{p:.4}_' + x for x in stats_header()]
        header += [f'ask_liq_{p:.4}_' + x for x in stats_header()]
        header += [f'diff_liq_{p:.4}_'  + x for x in stats_header()]

    return header

def get_stats(now, stats):
    if len(stats["best_prices"][0]) == 0 or len(stats["trades"][0]) == 0:
        return None

    line = [now.strftime("%Y-%m-%d %H:%M:%S"), str(int(now.timestamp()))]
    line += [(stats["best_prices"][0][-1] +stats["best_prices"][1][-1])/2.0 - (stats["best_prices"][0][0] +stats["best_prices"][1][0])/2.0]

    buy_qty = sum(stats["trades"][0])
    sell_qty = sum(stats["trades"][1])
    buy_vwap = sum(stats["trades"][2]) / buy_qty if buy_qty > tolerance else 0.0
    sell_vwap = sum(stats["trades"][3]) / sell_qty if sell_qty > tolerance else 0.0
    line += [sum(stats["trades"][4]), sum(stats["trades"][5]), buy_qty, sell_qty, buy_vwap, sell_vwap]

    line += calc_stats(stats["best_prices"][0])
    line += calc_stats(stats["best_prices"][1])
    line += calc_stats(stats["best_prices"][2])

    for c, p in enumerate(liq_steps):
        line += calc_stats(stats['liq_levels'][c][0])
        line += calc_stats(stats['liq_levels'][c][1])
        line += calc_stats(stats['liq_levels'][c][2])

    return line

def get_optional_stats(stats):
    line  = []

    for i, p in enumerate(get_optional_header_sum() + get_optional_header_samples()):
        if p in (get_optional_header_sum()):
            line.append(sum(stats[i]))
        else:
            if len(stats[i]) > 0:
                line.append(calc_median(stats[i]))
            else:
                line.append(-1)
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

    spot_stats = {}
    future_stats = {}
    optional_stats = []
    init_stats(spot_stats, "spot")
    init_stats(future_stats, "future")
    init_optional_stats(optional_stats)
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
                        spot_header = ["spot_" + x for x in get_header()]
                        future_header = ["future_" + x for x in get_header()]
                        optional_header = ["opt_" + x for x in get_optional_header_sum()] + ["opt_" + x for x in get_optional_header_samples()]
                        full_header = [val for pair in zip(spot_header, future_header) for val in pair] + optional_header
                        print(full_header)
                        writer.writerow(full_header)
                        header_printed = True
                    spot_line = get_stats(last_print, spot_stats)
                    future_line = get_stats(last_print, future_stats)
                    optional_line = get_optional_stats(optional_stats)
                    if spot_line and future_line and optional_line:
                        line = [val for pair in zip(spot_line, future_line) for val in pair] + optional_line
                        print(line)
                        writer.writerow(line)
                    reset_stats(spot_stats)
                    reset_stats(future_stats)
                    reset_optional_stats(optional_stats)
                    last_print = round_to_interval(now, window_sec)

                update_stats(spot_stats, update)
                update_stats(future_stats, update)
                update_optional_stats(optional_stats, update)
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

    future_snapshot_url =  f"https://fapi.binance.com/fapi/v1/depth?symbol={FUTURE_SYMBOL.upper()}&limit=1000"
    future_prices_ws_url = f"wss://fstream.binance.com/ws/{FUTURE_SYMBOL.lower()}@depth@100ms"
    future_trade_ws_url =  f"wss://fstream.binance.com/ws/{FUTURE_SYMBOL.lower()}@trade"

    q = mp.Queue()
    p1 = mp.Process(target=orderbook_subscriber, args=(q,spot_snapshot_url, spot_prices_ws_url, "spot"))
    p2 = mp.Process(target=trade_subscriber, args=(q, spot_trade_ws_url, "spot"))
    p3 = mp.Process(target=orderbook_subscriber, args=(q, future_snapshot_url, future_prices_ws_url, "future"))
    p4 = mp.Process(target=trade_subscriber, args=(q, future_trade_ws_url, "future"))
    p5 = mp.Process(target=optional_stats_subscriber, args=(q, FUTURE_SYMBOL))
    p6 = mp.Process(target=stats_calculator, args=(q,))

    processes.append(p1)
    processes.append(p2)
    processes.append(p3)
    processes.append(p4)
    processes.append(p5)
    processes.append(p6)

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()

    # just idle until stopped
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(my_main())
