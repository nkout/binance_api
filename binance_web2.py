import requests
import websocket
import json
import threading
import time

symbol = "BTCUSDT"
url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth"

# Buffer for websocket messages
buffer = []
snapshot = None
last_update_id = None
got_snapshot = False

# 1. WebSocket handler
def on_message(ws, message):
    global buffer, snapshot, last_update_id, got_snapshot
    data = json.loads(message)
    buffer.append(data)

    # If we already have snapshot, start processing updates
    if got_snapshot:
        process_buffer()

def on_open(ws):
    print("WebSocket connection opened")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")

# 2. Function to apply updates
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

    orderbook["bids"] = sorted(orderbook["bids"], key=lambda x: float(x[0]), reverse=True)[:10]
    orderbook["asks"] = sorted(orderbook["asks"], key=lambda x: float(x[0]))[:10]

# 3. Process buffered messages
def process_buffer():
    global buffer, snapshot, last_update_id

    while buffer:
        data = buffer.pop(0)
        U, u = data["U"], data["u"]

        # First event must satisfy U <= lastUpdateId+1 <= u
        if u < last_update_id:
            continue
        if U <= last_update_id + 1 <= u:
            apply_update(snapshot, data)
            last_update_id = u
            print(f"✅ Updated book LastUpdateId={last_update_id}")
            print("Top bid:", snapshot["bids"][0], "Top ask:", snapshot["asks"][0])
        elif U > last_update_id + 1:
            print("⚠️ Missed updates, resync required!")
            break

# 4. Start websocket in background thread
def start_ws():
    ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open, on_close=on_close)
    ws.run_forever()

ws_thread = threading.Thread(target=start_ws, daemon=True)
ws_thread.start()

# 5. Wait a bit, then request snapshot
time.sleep(1)  # let websocket connect

resp = requests.get("https://api.binance.com/api/v3/depth", params={"symbol": symbol, "limit": 1000})
snapshot = resp.json()
last_update_id = snapshot["lastUpdateId"]
got_snapshot = True

print("📥 Snapshot received with lastUpdateId:", last_update_id)

# 6. Process any buffered messages
process_buffer()

# Keep running
while True:
    time.sleep(1)
