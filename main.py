import websocket
import json
import requests
import time
import datetime
import threading
import logging

# ================== 配置 ==================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

MIN_USD = 50000
STRONG_USD = 120000

EXCLUDE = ["BTC", "ETH", "USDT"]

# ================== 状态 ==================
seen = set()
buy_flow = {}
last_price = {}
binance_cache = {}

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

# ================== Binance缓存价格 ==================
def get_binance_price(symbol):
    now = time.time()

    if symbol in binance_cache:
        price, ts = binance_cache[symbol]
        if now - ts < 5:
            return price

    try:
        r = requests.get(
            f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT",
            timeout=3
        ).json()

        if "price" in r:
            price = float(r["price"])
            binance_cache[symbol] = (price, now)
            return price
    except:
        pass

    return None

# ================== 获取市场 ==================
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

# ================== 信号处理 ==================
def handle_trade(data):
    try:
        symbol = data["cd"].replace("KRW-", "")
        price = data["tp"]
        volume = data["tv"]
        side = data["ab"]  # BID / ASK

        tid = f"{symbol}-{data['tms']}"
        if tid in seen:
            return
        seen.add(tid)

        # ===== Binance价格 =====
        binance_price = get_binance_price(symbol)
        if not binance_price:
            return

        usd = volume * binance_price
        if usd < MIN_USD:
            return

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
            if (price - last_price[symbol]) / last_price[symbol] > 0.006:
                pump = True

        last_price[symbol] = price

        # ===== 信号 =====
        if usd > STRONG_USD and absorb and pump:
            level = "🔴机会（可进场）"
        elif absorb and usd > STRONG_USD:
            level = "🟡吸筹"
        elif pump:
            level = "🟢拉升"
        else:
            return

        side_str = "🟢买单" if side == "BID" else "🔴卖单"

        dt = datetime.datetime.utcfromtimestamp(data["tms"]/1000) + datetime.timedelta(hours=8)

        msg = (
            f"{symbol}/USDT\n"
            f"{level}\n"
            f"{side_str}\n"
            f"💰 {format_usd(usd)}\n"
            f"📍 {binance_price:.4f}\n"
            f"⏰ {dt.strftime('%H:%M:%S')}"
        )

        print(msg)
        send(msg)

    except Exception as e:
        logging.error(e)

# ================== WebSocket ==================
def on_message(ws, message):
    try:
        data = json.loads(message)
        handle_trade(data)
    except:
        pass

def on_error(ws, error):
    logging.error(f"WS错误: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.warning("WS断开，准备重连...")

def on_open(ws):
    logging.info("WebSocket已连接")

    markets = get_top_markets()

    sub = [
        {"ticket": "test"},
        {
            "type": "trade",
            "codes": markets,
            "isOnlyRealtime": True
        }
    ]

    ws.send(json.dumps(sub))

# ================== 启动 ==================
def run_ws():
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://api.upbit.com/websocket/v1",
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.on_open = on_open
            ws.run_forever(ping_interval=30)

        except Exception as e:
            logging.error(f"WS重启: {e}")
            time.sleep(5)

# ================== 主入口 ==================
if __name__ == "__main__":
    send("🚀 WebSocket 实盘系统启动")
    run_ws()
