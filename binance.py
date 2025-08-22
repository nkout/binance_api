import requests

def get_orderbook(symbol="BTCUSDT", limit=100):
    url = f"https://api.binance.com/api/v3/depth"
    params = {"symbol": symbol, "limit": limit}
    response = requests.get(url, params=params)
    data = response.json()
    return data

orderbook = get_orderbook("BTCUSDT", 10)
print("Bids:", orderbook["bids"])
print("Asks:", orderbook["asks"])
