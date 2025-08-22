import requests
import websocket
import json
import threading
import time

symbol = "BTCUSDT"
url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth"

buffer = []
snapshot = None
last_update_id = None
got_snapshot = False

# -----------------------------
# Apply updates
# -----------------------------
def apply_update(orderbook, data):
    for bid in data["b"]:
        price, qty = float(bid[0]), float(bid[1])
        if qty == 0:
            orderbook["bids"] = [b for b in orderbook["bids"] if float(b[0]) != price]
        else:
            orderbook["bids"] = [b for b in orderbook["bids"] if float(b[0]) != price]
            orderbook["bids"].append([str(price), str(qty)])

    for ask in data["a"]:
        price, qty = float(ask[0]), float(ask[1])
        if qty == 0:
            orderbook["asks"] = [a for a in orderbook["asks"] if float(a[0]) != price]
        else:
            orderbook["asks"] = [a for a in orderbook["asks"] if float(a[0]) != price]
            orderbook["asks"].append([str(price), str(qty)])

    # Sort
    orderbook["bids"] = sorted(orderbook["bids"], key=lambda x: float(x[0]), reverse=True)
    orderbook["asks"] = sorted(orderbook["asks"], key=lambda x: float(x[0]))

# -----------------------------
# Liquidity aggregation
# -----------------------------
def aggregate_liquidity(orderbook):
    if not orderbook["bids"] or not orderbook["asks"]:
        return None

    best_bid = float(orderbook["bids"][0][0])
    best_ask = float(orderbook["asks"][0][0])

    levels = [0, 0.0005, 0.001, 0.0015, 0.002, 0.0025, 0.003, 0.004, 0.005, 0.01, 0.02]  # 0%, 0.1%, 0.2%, 0.5%, 1%, 2%

    bid_liquidity = {}
    ask_liquidity = {}

    for lvl in levels:
        min_bid = best_bid * (1 - lvl)
        bid_liquidity[f"{lvl*100:.2f}%"] = sum(
            float(qty) for price, qty in orderbook["bids"] if float(price) >= min_bid
        )

        max_ask = best_ask * (1 + lvl)
        ask_liquidity[f"{lvl*100:.2f}%"] = sum(
            float(qty) for price, qty in orderbook["asks"] if float(price) <= max_ask
        )

    return best_bid, best_ask, bid_liquidity, ask_liquidity

# -----------------------------
# Process buffered messages
# -----------------------------
def process_buffer():
    global buffer, snapshot, last_update_id

    while buffer:
        data = buffer.pop(0)
        U, u = data["U"], data["u"]

        if u < last_update_id:
            continue
        if U <= last_update_id + 1 <= u:
            apply_update(snapshot, data)
            last_update_id = u

            agg = aggregate_liquidity(snapshot)
            if agg:
                best_bid, best_ask, bids, asks = agg
                print(f"Updated book LastUpdateId={last_update_id}")
                print(f"Best Bid: {best_bid:.2f} | Best Ask: {best_ask:.2f}")
                print("Bid Liquidity:", bids)
                print("Ask Liquidity:", asks)
                print()
        elif U > last_update_id + 1:
            print("Missed updates, resync required!")
            break

# -----------------------------
# WebSocket handlers
# -----------------------------
def on_message(ws, message):
    global buffer, got_snapshot
    data = json.loads(message)
    buffer.append(data)

    if got_snapshot:
        process_buffer()

def on_open(ws):
    print("WebSocket connection opened")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")

# -----------------------------
# Start WebSocket
# -----------------------------
def start_ws():
    ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open, on_close=on_close)
    ws.run_forever()

ws_thread = threading.Thread(target=start_ws, daemon=True)
ws_thread.start()

# -----------------------------
# Snapshot after WS connects
# -----------------------------
time.sleep(1)

resp = requests.get("https://api.binance.com/api/v3/depth", params={"symbol": symbol, "limit": 1000})
snapshot = resp.json()
last_update_id = snapshot["lastUpdateId"]
got_snapshot = True

print("📥 Snapshot received with lastUpdateId:", last_update_id)

process_buffer()

# Keep alive
while True:
    time.sleep(1)
