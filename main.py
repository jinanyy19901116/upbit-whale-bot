import requests
import time
import datetime
import logging

# ================== 配置 ==================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

MIN_USD = 50000
STRONG_USD = 120000

EXCLUDE = ["BTC", "ETH", "USDT"]

seen = set()
buy_flow = {}
last_price = {}

logging.basicConfig(level=logging.INFO)

# ================== 工具 ==================
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10
        )
    except:
        pass

def format_usd(x):
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    elif x >= 1_000:
        return f"${x/1_000:.0f}K"
    return f"${x:.0f}"

def get_binance_price(symbol):
    try:
        r = requests.get(
            f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT",
            timeout=3
        ).json()
        if "price" in r:
            return float(r["price"])
    except:
        pass
    return None

# ================== 市场 ==================
def get_top_markets():
    res = requests.get("https://api.upbit.com/v1/market/all").json()
    krw = [
        m["market"] for m in res
        if m["market"].startswith("KRW-")
        and not any(x in m["market"] for x in EXCLUDE)
    ]

    tickers = requests.get(
        "https://api.upbit.com/v1/ticker",
        params={"markets": ",".join(krw)}
    ).json()

    sorted_m = sorted(tickers, key=lambda x: x["acc_trade_price_24h"], reverse=True)

    return [m["market"] for m in sorted_m[:30]]

def get_trades(market):
    try:
        return requests.get(
            "https://api.upbit.com/v1/trades/ticks",
            params={"market": market, "count": 10},
            timeout=5
        ).json()
    except:
        return []

# ================== 主逻辑 ==================
def run():
    send("🚀 实盘信号系统启动")

    markets = get_top_markets()

    while True:
        for market in markets:
            trades = get_trades(market)

            for t in trades:
                try:
                    tid = t["sequential_id"]
                    if tid in seen:
                        continue
                    seen.add(tid)

                    price = t["trade_price"]
                    volume = t["trade_volume"]
                    side = t["ask_bid"]

                    symbol = market.replace("KRW-", "")

                    # ===== Binance价格 =====
                    binance_price = get_binance_price(symbol)
                    if not binance_price:
                        continue

                    usd = volume * binance_price

                    if usd < MIN_USD:
                        continue

                    # ===== 吸筹 =====
                    buy_flow[symbol] = buy_flow.get(symbol, 0)
                    if side == "BID":
                        buy_flow[symbol] += 1
                    else:
                        buy_flow[symbol] = 0

                    absorb = buy_flow[symbol] >= 2

                    # ===== 拉盘 =====
                    pump = False
                    if symbol in last_price:
                        change = (price - last_price[symbol]) / last_price[symbol]
                        if change > 0.006:
                            pump = True

                    last_price[symbol] = price

                    # ===== 信号分级 =====
                    level = ""
                    if usd > STRONG_USD and absorb and pump:
                        level = "🔴机会（可考虑进场）"
                    elif absorb and usd > STRONG_USD:
                        level = "🟡强势（主力吸筹）"
                    elif pump:
                        level = "🟢可做（资金推动）"
                    else:
                        continue

                    side_str = "🟢买单" if side == "BID" else "🔴卖单"

                    ts = t["timestamp"] / 1000
                    dt = datetime.datetime.utcfromtimestamp(ts) + datetime.timedelta(hours=8)

                    msg = (
                        f"{symbol}/USDT\n"
                        f"{level}\n"
                        f"{side_str}\n"
                        f"💰 {format_usd(usd)}\n"
                        f"📍 {binance_price:.4f}\n"
                        f"⏰ {dt.strftime('%H:%M:%S')}"
                    )

                    send(msg)

                except Exception as e:
                    logging.error(e)

            time.sleep(0.2)

if __name__ == "__main__":
    run()
