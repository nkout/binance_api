import json
import decimal
import websocket
from decimal import Decimal
from threading import Thread
import time

decimal.getcontext().prec = 12

SYMBOL = "BTCUSDT"
LIMIT = 100  # Order book depth
PASSIVE_PERCENTS = [Decimal("0.001") * i for i in range(1, 21)]  # 0.1%, 0.2%, ..., 2%

order_book = {"bids": {}, "asks": {}}
best_bid = Decimal("0")
best_ask = Decimal("0")

def aggregate_liquidity():
    result = {"bids": {}, "asks": {}}
    for pct in PASSIVE_PERCENTS:
        bid_limit = best_bid * (Decimal("1") - pct)
        ask_limit = best_ask * (Decimal("1") + pct)

        bid_qty = sum(qty for price, qty in order_book["bids"].items() if price >= bid_limit)
        ask_qty = sum(qty for price, qty in order_book["asks"].items() if price <= ask_limit)

        level = f"{(pct*100):.1f}%"
        result["bids"][level] = bid_qty
        result["asks"][level] = ask_qty
    return result

def print_liquidity():
    while True:
        if best_bid and best_ask:
            agg = aggregate_liquidity()
            print("\n" + "="*40)
            print(f"Best bid: {best_bid}, Best ask: {best_ask}")
            print("Aggregated Bids:")
            for lvl, qty in agg["bids"].items():
                print(f"  {lvl}: {qty}")
            print("Aggregated Asks:")
            for lvl, qty in agg["asks"].items():
                print(f"  {lvl}: {qty}")
        time.sleep(1)

def update_order_book(data):
    global best_bid, best_ask
    # Bids
    for price_str, qty_str in data.get("b", []):
        price = Decimal(price_str)
        qty = Decimal(qty_str)
        if qty == 0:
            order_book["bids"].pop(price, None)
        else:
            order_book["bids"][price] = qty
    # Asks
    for price_str, qty_str in data.get("a", []):
        price = Decimal(price_str)
        qty = Decimal(qty_str)
        if qty == 0:
            order_book["asks"].pop(price, None)
        else:
            order_book["asks"][price] = qty

    # Update best levels
    best_bid = max(order_book["bids"].keys(), default=Decimal("0"))
    best_ask = min(order_book["asks"].keys(), default=Decimal("0"))

def on_message(ws, message):
    data = json.loads(message)
    if "u" in data:
        update_order_book(data)

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed, reconnecting...")
    time.sleep(1)
    start_ws()

def on_open(ws):
    print("WebSocket connected")

def start_ws():
    ws = websocket.WebSocketApp(
        f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@depth@100ms",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws.run_forever()

# Start WebSocket in background
Thread(target=start_ws, daemon=True).start()
# Start printing liquidity
print_liquidity()
