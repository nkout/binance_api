import asyncio
import websockets
import json
import datetime

symbol = "btcusdt"
url = f"wss://fstream.binance.com/ws/{symbol}@markPrice"

async def mark_price_ws():
    async with websockets.connect(url) as ws:
        print(f"Connected to {url}")
        async for msg in ws:
            data = json.loads(msg)

            event_time = data["E"]
            mark_price = float(data["p"])
            index_price = float(data["i"])
            funding_rate = float(data["r"])
            est_funding_rate = float(data["P"])
            next_funding_time = data["T"]

            # Calculate spread/basis in percentage
            basis = (mark_price - index_price) / index_price * 100

            print(
                f"\nEvent Time: {event_time}"
                f"\nMark Price: {mark_price:.2f}"
                f"\nIndex Price: {index_price:.2f}"
                f"\nSpread/Basis: {basis:.4f}%"
                f"\nCurrent Funding Rate: {funding_rate:.6f}"
                f"\nEstimated Funding Rate: {est_funding_rate:.6f}"
                f"\nNext Funding Time: {next_funding_time}"
                f"=> {next_funding_time / 1000 - datetime.datetime.now().timestamp()}"
            )

async def main():
    while True:
        try:
            await mark_price_ws()
        except Exception as e:
            print(f"Connection lost: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
