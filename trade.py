import asyncio
import json
import websockets
from decimal import Decimal

SYMBOL = "btcusdc"  # Binance spot USDC market
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"

async def trade_ws():
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                print("Connected to BTC/USDC trade stream")
                async for message in ws:
                    data = json.loads(message)
                    # Example trade data
                    price = Decimal(data['p'])
                    qty = Decimal(data['q'])
                    is_buyer_maker = data['m']  # True if trade was a sell (market maker)
                    trade_id = data['t']
                    print(f"Trade {trade_id}: Price={price} Qty={qty} BuyerMaker={is_buyer_maker}")
        except Exception as e:
            print(f"WebSocket error: {e}, reconnecting in 2s...")
            await asyncio.sleep(2)

async def main():
    await trade_ws()

if __name__ == "__main__":
    asyncio.run(main())
