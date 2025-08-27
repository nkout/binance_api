import asyncio
import json
import websockets
from decimal import Decimal
from collections import deque
import time

SYMBOL = "btcusdc"
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"
AGG_INTERVAL = 15.0  # seconds (100ms)

trade_buffer = deque()

async def trade_ws():
    global trade_buffer
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                print("Connected to BTC/USDC trade stream")
                async for message in ws:
                    data = json.loads(message)
                    price = Decimal(data['p'])
                    qty = Decimal(data['q'])
                    is_buyer_maker = data['m']  # True = sell, False = buy
                    trade_buffer.append((price, qty, is_buyer_maker))
        except Exception as e:
            print(f"WebSocket error: {e}, reconnecting in 2s...")
            await asyncio.sleep(2)

async def aggregator():
    global trade_buffer
    while True:
        await asyncio.sleep(AGG_INTERVAL)
        if not trade_buffer:
            continue

        total_buy = Decimal(0)
        total_sell = Decimal(0)
        total_buy_amount = Decimal(0)
        total_sell_amount = Decimal(0)
        total_volume = Decimal(0)

        while trade_buffer:
            price, qty, is_buyer_maker = trade_buffer.popleft()
            total_volume += qty
            if is_buyer_maker:  # Sell
                total_sell += qty
                total_sell_amount += price * qty
            else:  # Buy
                total_buy += qty
                total_buy_amount += price * qty

        # Compute VWAP
        buy_vwap = (total_buy_amount / total_buy) if total_buy > 0 else 0
        sell_vwap = (total_sell_amount / total_sell) if total_sell > 0 else 0
        total_trades = total_buy + total_sell
        buy_ratio = (total_buy / total_trades) if total_trades > 0 else 0
        sell_ratio = (total_sell / total_trades) if total_trades > 0 else 0

        print(f"[{time.strftime('%H:%M:%S')}] Volume: {total_volume:.6f} | "
              f"Buy: {total_buy:.6f} (VWAP {buy_vwap:.2f}) | "
              f"Sell: {total_sell:.6f} (VWAP {sell_vwap:.2f}) | "
              f"Spread: {(sell_vwap - buy_vwap):.2f} | "
              f"BuyRatio: {buy_ratio:.2%} SellRatio: {sell_ratio:.2%}")

async def main():
    await asyncio.gather(trade_ws(), aggregator())

if __name__ == "__main__":
    asyncio.run(main())
