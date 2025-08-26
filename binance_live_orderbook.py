import asyncio
import aiohttp
import websockets
import json
from decimal import Decimal

SYMBOL = "BTCUSDC"
SNAPSHOT_URL = f"https://api.binance.com/api/v3/depth?symbol={SYMBOL}&limit=5000"
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@depth@100ms"

order_book = {'bids': {}, 'asks': {}}
last_update_id = None


def aggregate_liquidity(step_percent=0.01, max_percent=0.08):
    """Aggregate liquidity at steps above/below best bid/ask."""
    if not order_book['bids'] or not order_book['asks']:
        return

    best_bid = max(order_book['bids'])
    best_ask = min(order_book['asks'])

    print("")
    print(f"Best Bid Ask {best_bid} / {best_ask}")

    p = 0 + 0.00001
    while p <= max_percent:
        threshold_bid = best_bid * (1 - Decimal(p / 100))
        threshold_ask = best_ask * (1 + Decimal(p / 100))
        liquidity_bid = sum(q for price, q in order_book['bids'].items() if price >= threshold_bid)
        liquidity_ask = sum(q for price, q in order_book['asks'].items() if price <= threshold_ask)
        print(f"{p:.2f}% bid: {liquidity_bid} / {liquidity_ask}")
        p += step_percent

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


async def handle_ws():
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

                    aggregate_liquidity()
        except Exception as e:
            print(f"WebSocket disconnected, retrying... {e}")
            await asyncio.sleep(2)


async def main():
    await fetch_snapshot()
    await handle_ws()


if __name__ == "__main__":
    asyncio.run(main())
