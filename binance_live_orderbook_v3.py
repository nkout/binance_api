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
import random
import string

SPOT_SYMBOL = "BTCUSDC"
FUTURE_SYMBOL = "BTCUSDT"
SCHEMA_VERSION = 3
outfile_prefix="out3"
tolerance = 0.000001
# Shallow levels keep full bar stats; deep levels keep only the median
# (models only ever used the deep medians -- saves ~240 columns per market).
liq_steps_full = [0.0, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10]
liq_steps_deep = [0.12, 0.14, 0.16, 0.18, 0.20, 0.25, 0.30, 0.40]
liq_steps = liq_steps_full + liq_steps_deep
NEAR_TOUCH_PCT = 0.05  # band around mid (in % of mid) for add/cancel flow metrics
window_sec = 15
thread_interval = 0.2 # seconds (200ms)
max_lines_per_file = 24 * 3600 / (15 * 6) #6 files per day
processes = []


def aggregate_liquidity(update_queue, liq_steps, order_book, market_type, flow=None):
    """Aggregate liquidity at steps above/below best bid/ask."""
    if not order_book['bids'] or not order_book['asks']:
        return

    best_bid = max(order_book['bids'])
    best_ask = min(order_book['asks'])

    out = {'type': 'prices', 'market_type': market_type}
    out['best_prices'] = [float(best_bid), float(best_ask)]

    # Microprice deviation: queue-imbalance-weighted mid minus mid. Positive
    # when the bid queue is heavier than the ask queue (upward pressure).
    q_bid = float(order_book['bids'][best_bid])
    q_ask = float(order_book['asks'][best_ask])
    bid_f, ask_f = float(best_bid), float(best_ask)
    mid = (bid_f + ask_f) / 2.0
    denom = q_bid + q_ask
    micro = (q_bid * ask_f + q_ask * bid_f) / denom if denom > 0 else mid
    out['micro_dev'] = micro - mid

    out['flow'] = flow  # per-message order-flow event metrics (may be None)
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
            wait = min(2 ** attempt, 60)
            print(f"Attempt {attempt} failed on {spot_snapshot_url}: {e}, retrying in {wait}s...")
            await asyncio.sleep(wait)
            attempt += 1


def apply_depth_update(order_book, data):
    """
    Apply a depth-diff message to the book and return order-flow event
    metrics for it. NOTE: 'cancel' volumes include fills -- depth diffs
    cannot distinguish a cancellation from a trade removing liquidity.
    """
    bids, asks = order_book['bids'], order_book['asks']
    prev_bb = max(bids) if bids else None
    prev_ba = min(asks) if asks else None
    prev_qbb = float(bids[prev_bb]) if prev_bb is not None else 0.0
    prev_qba = float(asks[prev_ba]) if prev_ba is not None else 0.0

    near_lo = near_hi = None
    if prev_bb is not None and prev_ba is not None:
        prev_mid = (prev_bb + prev_ba) / 2
        band = prev_mid * Decimal(str(NEAR_TOUCH_PCT / 100.0))
        near_lo, near_hi = prev_mid - band, prev_mid + band

    add_bid = cancel_bid = add_ask = cancel_ask = 0.0

    for side, key in (('bids', 'b'), ('asks', 'a')):
        book = order_book[side]
        for price_str, qty_str in data.get(key, []):
            price = Decimal(price_str)
            qty = Decimal(qty_str)
            if near_lo is not None and near_lo <= price <= near_hi:
                delta = float(qty - book.get(price, Decimal(0)))
                if side == 'bids':
                    if delta > 0:
                        add_bid += delta
                    else:
                        cancel_bid -= delta
                else:
                    if delta > 0:
                        add_ask += delta
                    else:
                        cancel_ask -= delta
            if qty == 0:
                book.pop(price, None)
            else:
                book[price] = qty

    if prev_bb is None or prev_ba is None or not bids or not asks:
        return None

    new_bb, new_ba = max(bids), min(asks)
    new_qbb, new_qba = float(bids[new_bb]), float(asks[new_ba])

    # Cont-style order flow imbalance at the best level
    ofi = 0.0
    if new_bb >= prev_bb:
        ofi += new_qbb
    if new_bb <= prev_bb:
        ofi -= prev_qbb
    if new_ba <= prev_ba:
        ofi -= new_qba
    if new_ba >= prev_ba:
        ofi += prev_qba

    return {
        'ofi': ofi,
        'add_bid_near': add_bid, 'cancel_bid_near': cancel_bid,
        'add_ask_near': add_ask, 'cancel_ask_near': cancel_ask,
        'best_bid_moved': 1 if new_bb != prev_bb else 0,
        'best_ask_moved': 1 if new_ba != prev_ba else 0,
        'bid_depleted': 1 if prev_bb not in bids else 0,
        'ask_depleted': 1 if prev_ba not in asks else 0,
    }


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

                    flow = apply_depth_update(order_book, data)
                    aggregate_liquidity(update_queue, liq_steps, order_book, market_type, flow)
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
    # handle_orderbook_ws() loops forever internally (reconnect loop), so
    # this call never returns under normal operation -- no code after it runs.
    await handle_orderbook_ws(update_queue, spot_snapshot_url, spot_prices_ws_url, order_book, market_type)


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
                    latest_id = data['t']
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

        # Sub-bar sequencing: preserve individual trade sequence within this
        # 200ms batch so the 15s bar assembly can derive first/last trade
        # side, largest trade, and early/late split. is_buyer_maker=True
        # means the trade was a SELL (taker sold into a resting bid).
        trade_seq = []

        while trade_buffer:
            price, qty, is_buyer_maker = trade_buffer.popleft()
            total_volume += qty
            side = 'sell' if is_buyer_maker else 'buy'
            trade_seq.append((side, float(qty)))
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
        out['trade_seq'] = trade_seq  # ordered list of (side, qty) for this 200ms batch

        update_queue.put(out)


async def subscribe_trade(update_queue, spot_trade_ws_url, market_type):
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, subscriber_shutdown)

    trade_buffer = deque()
    # asyncio.gather() never returns: both trade_ws() and trade_aggregator()
    # loop forever internally -- no code after this call runs.
    await asyncio.gather(trade_ws(spot_trade_ws_url, trade_buffer), trade_aggregator(update_queue, trade_buffer, market_type))


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
            wait = min(2 ** attempt, 60)
            print(f"Attempt {attempt} failed on {url}: {e}, retrying in {wait}s...")
            await asyncio.sleep(wait)
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
            wait = min(2 ** attempt, 60)
            print(f"Attempt {attempt} failed on {url}: {e}, retrying in {wait}s...")
            await asyncio.sleep(wait)
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
                    qty = Decimal(o_data['q'])
                    avg_price = Decimal(o_data.get('ap') or o_data.get('p') or 0)
                    buffer.append(("force_exit", qty, long_liq, float(avg_price * qty)))
        except Exception as e:
            print(f"Force exit WebSocket error {url}: {e}, reconnecting in 2s...")
            await asyncio.sleep(2)


async def subscribe_spread(future_symbol, buffer):
    url = f"wss://fstream.binance.com/ws/{future_symbol.lower()}@markPrice"
    while True:
        try:
            async with websockets.connect(url) as ws:
                print(f"Connected to mark price stream {url}")
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
            print(f"Mark price WebSocket error {url}: {e}, reconnecting in 2s...")
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
        'short_force_exit_qty_sum',
        'long_force_exit_cnt_sum',
        'short_force_exit_cnt_sum',
        'long_force_exit_notional_sum',
        'short_force_exit_notional_sum',
    ])


def get_optional_header_max():
    return sorted([
        'force_exit_notional_max',
    ])


async def optional_aggregator(update_queue, buffer):
    while True:
        await asyncio.sleep(thread_interval)
        if not buffer:
            continue

        out = {'type': 'optional', 'market_type': "optional"}
        for x in get_optional_header_samples():
            out[x] = []
        for x in (get_optional_header_sum() + get_optional_header_max()):
            out[x] = 0.0

        while buffer:
            data = buffer.popleft()
            update_type = data[0]
            update_data = data[1:]

            if update_type == 'force_exit':
                qty, long_liq, notional = update_data
                if long_liq:
                    out['long_force_exit_qty_sum'] += float(qty)
                    out['long_force_exit_cnt_sum'] += 1
                    out['long_force_exit_notional_sum'] += notional
                else:
                    out['short_force_exit_qty_sum'] += float(qty)
                    out['short_force_exit_cnt_sum'] += 1
                    out['short_force_exit_notional_sum'] += notional
                out['force_exit_notional_max'] = max(out['force_exit_notional_max'], notional)
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
    # asyncio.gather() never returns: all five coroutines loop forever
    # internally -- no code after this call runs.
    await asyncio.gather(subscribe_force_close(future_symbol, buffer),
                          subscribe_spread(future_symbol, buffer),
                          poll_ratio(future_symbol, buffer),
                          poll_open_interest(future_symbol, buffer),
                          optional_aggregator(queue, buffer))


def optional_stats_subscriber(queue, future_symbol):
    asyncio.run(subscribe_optional(queue, future_symbol))


def flow_header():
    """Column names for per-bar order-flow event sums."""
    return [
        'ofi_sum',              # Cont order-flow imbalance at best level, summed over bar
        'add_bid_near_sum',     # qty added to bids within NEAR_TOUCH_PCT of mid
        'cancel_bid_near_sum',  # qty removed from bids near mid (cancels + fills)
        'add_ask_near_sum',
        'cancel_ask_near_sum',
        'depth_msg_count',      # depth diff messages in bar (book activity)
        'best_bid_move_count',  # times best bid price changed
        'best_ask_move_count',
        'bid_depletion_count',  # times the best bid level fully disappeared
        'ask_depletion_count',
    ]


def init_stats(stats, market_type):
    stats["best_prices"] = [[],[],[]]
    stats['liq_levels'] = []
    stats['market_type'] = market_type
    for p in liq_steps:
        stats['liq_levels'].append([[],[],[]])
    stats['trades'] = [[],[],[],[],[],[]]
    stats['trade_seq'] = []  # accumulates (side, qty) across the full 15s bar, in arrival order
    stats['micro_dev'] = []  # microprice - mid, one sample per depth message
    stats['flow'] = [0.0] * len(flow_header())


def init_optional_stats(stats):
    for _ in (get_optional_header_sum() + get_optional_header_max() + get_optional_header_samples()):
        stats.append([])


def update_stats(stats, update):
    if update['market_type'] != stats['market_type']:
        return
    if update['type'] == 'optional':
        # Optional-stream updates are handled by update_optional_stats(),
        # not here -- silently ignore instead of falling through to the
        # 'wrong update type' branch below.
        return

    if update['type'] == 'prices':
        stats["best_prices"][0].append(update['best_prices'][0])
        stats["best_prices"][1].append(update['best_prices'][1])
        stats["best_prices"][2].append(update['best_prices'][1] - update['best_prices'][0])

        stats['micro_dev'].append(update['micro_dev'])
        fl = update.get('flow')
        if fl is not None:
            stats['flow'][0] += fl['ofi']
            stats['flow'][1] += fl['add_bid_near']
            stats['flow'][2] += fl['cancel_bid_near']
            stats['flow'][3] += fl['add_ask_near']
            stats['flow'][4] += fl['cancel_ask_near']
            stats['flow'][5] += 1
            stats['flow'][6] += fl['best_bid_moved']
            stats['flow'][7] += fl['best_ask_moved']
            stats['flow'][8] += fl['bid_depleted']
            stats['flow'][9] += fl['ask_depleted']

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
        stats['trade_seq'].extend(update.get('trade_seq', []))
    else:
        print('wrong update type')


def update_optional_stats(stats, update):
    if update['market_type'] != 'optional':
        return

    sums = get_optional_header_sum()
    maxes = get_optional_header_max()
    for i, p in enumerate(sums + maxes + get_optional_header_samples()):
        if p in sums or p in maxes:
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
    stats['trade_seq'].clear()
    stats['micro_dev'].clear()
    stats['flow'] = [0.0] * len(flow_header())


def reset_optional_stats(stats):
    for i, _ in enumerate(get_optional_header_sum() + get_optional_header_max() + get_optional_header_samples()):
        stats[i].clear()


def calc_stats(line):
    arr = np.array(line)
    min_val = np.min(arr).item()
    max_val = np.max(arr).item()
    #mean_val = np.mean(arr).item()
    #q25 = np.percentile(arr, 25).item()
    q50 = np.percentile(arr, 50).item()
    #q75 = np.percentile(arr, 75).item()
    #return [len(line), line[0], line[-1], mean_val, min_val, q25, q50, q75, max_val]
    return [len(line), line[0], line[-1], min_val, q50, max_val]


def calc_median(line):
    arr = np.array(line)
    q50 = np.percentile(arr, 50).item()
    return q50


def mid_vol_header():
    return [
        'mid_std',    # std of the 200ms mid series within the bar
        'mid_rv',     # realized variance: sum of squared 200ms mid changes
        'mid_flips',  # direction changes of the (non-zero) mid moves
    ]


def calc_mid_vol(bid_series, ask_series):
    """Second-moment / choppiness stats of the intra-bar mid series."""
    mid = (np.array(bid_series) + np.array(ask_series)) / 2.0
    if len(mid) < 3:
        return [0.0, 0.0, 0]
    d = np.diff(mid)
    nz = d[d != 0]
    flips = int(np.sum(np.diff(np.sign(nz)) != 0)) if len(nz) > 1 else 0
    return [float(np.std(mid)), float(np.sum(d * d)), flips]


def sub_bar_header():
    """Column names for sub-bar trade sequencing features."""
    return [
        'first_trade_side',    # +1 = first trade in bar was a buy, -1 = sell, 0 = no trades
        'last_trade_side',     # +1 = last trade in bar was a buy, -1 = sell, 0 = no trades
        'largest_trade_qty',   # size of the single largest trade in the bar
        'largest_trade_side',  # +1 = largest trade was a buy, -1 = sell, 0 = no trades
        'buy_count_early',     # number of buy trades in first half of bar (by trade count)
        'buy_count_late',      # number of buy trades in second half of bar
        'sell_count_early',    # number of sell trades in first half of bar
        'sell_count_late',     # number of sell trades in second half of bar
        'buy_qty_early',       # buy volume in first half of bar
        'buy_qty_late',        # buy volume in second half of bar
        'sell_qty_early',      # sell volume in first half of bar
        'sell_qty_late',       # sell volume in second half of bar
        'buy_size_median',     # per-trade size distribution: informed flow shows
        'buy_size_p90',        # up as a fat right tail at equal total volume
        'sell_size_median',
        'sell_size_p90',
    ]


def calc_sub_bar_stats(trade_seq):
    """
    Compute sub-bar trade sequencing features from an ordered list of
    (side, qty) tuples covering the full 15s bar.

    Splits the bar by TRADE COUNT (not wall-clock time) into early/late
    halves, since trade arrival is already in chronological order within
    the bar (trade_buffer is FIFO, batches appended in arrival order).
    """
    n = len(trade_seq)
    if n == 0:
        return [0, 0, 0.0, 0] + [0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0] + [0.0, 0.0, 0.0, 0.0]

    first_side = 1 if trade_seq[0][0] == 'buy' else -1
    last_side = 1 if trade_seq[-1][0] == 'buy' else -1

    largest = max(trade_seq, key=lambda t: t[1])
    largest_qty = largest[1]
    largest_side = 1 if largest[0] == 'buy' else -1

    mid = n // 2
    early = trade_seq[:mid] if mid > 0 else trade_seq[:1]
    late = trade_seq[mid:] if mid > 0 else trade_seq[1:]

    buy_count_early = sum(1 for s, q in early if s == 'buy')
    buy_count_late = sum(1 for s, q in late if s == 'buy')
    sell_count_early = sum(1 for s, q in early if s == 'sell')
    sell_count_late = sum(1 for s, q in late if s == 'sell')

    buy_qty_early = sum(q for s, q in early if s == 'buy')
    buy_qty_late = sum(q for s, q in late if s == 'buy')
    sell_qty_early = sum(q for s, q in early if s == 'sell')
    sell_qty_late = sum(q for s, q in late if s == 'sell')

    buy_sizes = [q for s, q in trade_seq if s == 'buy']
    sell_sizes = [q for s, q in trade_seq if s == 'sell']
    buy_size_median = float(np.percentile(buy_sizes, 50)) if buy_sizes else 0.0
    buy_size_p90 = float(np.percentile(buy_sizes, 90)) if buy_sizes else 0.0
    sell_size_median = float(np.percentile(sell_sizes, 50)) if sell_sizes else 0.0
    sell_size_p90 = float(np.percentile(sell_sizes, 90)) if sell_sizes else 0.0

    return [
        first_side, last_side, largest_qty, largest_side,
        buy_count_early, buy_count_late, sell_count_early, sell_count_late,
        buy_qty_early, buy_qty_late, sell_qty_early, sell_qty_late,
        buy_size_median, buy_size_p90, sell_size_median, sell_size_p90,
    ]



def stats_header():
    #return ['samples', 'open', 'close', 'mean', 'min', '25perc', '50perc', '75perc', 'max']
    return ['samples', 'open', 'close', 'min', 'median', 'max']


def get_header():
    header = ['datetime', 'timestamp', 'price_diff']
    header += ['buy_samples', 'sell_samples', 'buy_qty', 'sell_qty', 'buy_vwap', 'sell_vwap']
    header += sub_bar_header()
    header += ['bid_'+x for x in stats_header()]
    header += ['ask_' + x for x in stats_header()]
    header += ['spread_' + x for x in stats_header()]
    header += mid_vol_header()
    header += ['micro_dev_' + x for x in stats_header()]
    header += flow_header()

    for p in liq_steps_full:
        header += [f'bid_liq_{p:.4}_' + x for x in stats_header()]
        header += [f'ask_liq_{p:.4}_' + x for x in stats_header()]
        header += [f'diff_liq_{p:.4}_' + x for x in stats_header()]
    for p in liq_steps_deep:
        header += [f'bid_liq_{p:.4}_median', f'ask_liq_{p:.4}_median', f'diff_liq_{p:.4}_median']

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
    line += calc_sub_bar_stats(stats.get('trade_seq', []))

    line += calc_stats(stats["best_prices"][0])
    line += calc_stats(stats["best_prices"][1])
    line += calc_stats(stats["best_prices"][2])
    line += calc_mid_vol(stats["best_prices"][0], stats["best_prices"][1])
    line += calc_stats(stats['micro_dev'])
    line += list(stats['flow'])

    n_full = len(liq_steps_full)
    for c in range(n_full):
        line += calc_stats(stats['liq_levels'][c][0])
        line += calc_stats(stats['liq_levels'][c][1])
        line += calc_stats(stats['liq_levels'][c][2])
    for c in range(n_full, len(liq_steps)):
        line += [calc_median(stats['liq_levels'][c][0]),
                 calc_median(stats['liq_levels'][c][1]),
                 calc_median(stats['liq_levels'][c][2])]

    return line


def get_optional_stats(stats):
    line = []
    sums = get_optional_header_sum()
    maxes = get_optional_header_max()
    for i, p in enumerate(sums + maxes + get_optional_header_samples()):
        if p in sums:
            line.append(sum(stats[i]))
        elif p in maxes:
            line.append(max(stats[i]) if stats[i] else 0.0)
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
    line_count = 0
    f = None
    file_index = -1
    random_prefix = ''.join(random.choices(string.ascii_lowercase, k=5))
    writer = None

    # with open(outfile, "w") as f:
    #     writer = csv.writer(f, delimiter=';')
    while not exit_received:
        try:
            if f is None:
                file_index += 1
                f = open(f"{outfile_prefix}.{random_prefix}.{file_index:05}.{datetime.datetime.now().strftime("%y%m%d")}.csv", "w")
                line_count = 0
                header_printed = False
                writer = csv.writer(f, delimiter=';')

            update = update_queue.get(timeout=1)
            now = datetime.datetime.now()

            if (now - last_print).total_seconds() > window_sec + tolerance:
                if not header_printed:
                    spot_header = ["spot_" + x for x in get_header()]
                    future_header = ["future_" + x for x in get_header()]
                    optional_header = ["opt_" + x for x in get_optional_header_sum()] \
                                    + ["opt_" + x for x in get_optional_header_max()] \
                                    + ["opt_" + x for x in get_optional_header_samples()]
                    full_header = [val for pair in zip(spot_header, future_header) for val in pair] + optional_header + ['schema_version']
                    #print(full_header)
                    writer.writerow(full_header)
                    f.flush()
                    line_count += 1
                    header_printed = True

                spot_line = get_stats(last_print, spot_stats)
                future_line = get_stats(last_print, future_stats)
                optional_line = get_optional_stats(optional_stats)

                if spot_line and future_line and optional_line:
                    line = [val for pair in zip(spot_line, future_line) for val in pair] + optional_line + [SCHEMA_VERSION]
                    #print(line)
                    writer.writerow(line)
                    f.flush()
                    line_count += 1

                if line_count > max_lines_per_file:
                    f.close()
                    f = None

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

    if f is not None:
        f.close()


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

    future_snapshot_url = f"https://fapi.binance.com/fapi/v1/depth?symbol={FUTURE_SYMBOL.upper()}&limit=1000"
    future_prices_ws_url = f"wss://fstream.binance.com/ws/{FUTURE_SYMBOL.lower()}@depth@100ms"
    future_trade_ws_url = f"wss://fstream.binance.com/ws/{FUTURE_SYMBOL.lower()}@trade"

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
