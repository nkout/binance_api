import websocket
import json

def on_message(ws, message):
    data = json.loads(message)
    print("Update:", data)

def on_open(ws):
    print("WebSocket connection opened")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")

symbol = "btcusdt"
url = f"wss://stream.binance.com:9443/ws/{symbol}@depth"

ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open, on_close=on_close)
ws.run_forever()