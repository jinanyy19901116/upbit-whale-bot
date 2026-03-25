import requests
import time
import datetime
import logging

# ================== 配置 ==================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

MIN_USD = 200000
KRW_TO_USD = 0.00075

EXCLUDE_MARKETS = ["KRW-BTC", "KRW-ETH", "KRW-USDT"]

# ================== 日志 ==================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ================== 工具 ==================
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=10)
    except Exception as e:
        logging.error(f"Telegram失败: {e}")

def format_usd(x):
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    elif x >= 1_000:
        return f"${x/1_000:.0f}K"
    return f"${x:.0f}"

def format_price(x):
    if x >= 1:
        return f"{x:.2f}"
    elif x >= 0.01:
        return f"{x:.4f}"
    return f"{x:.6f}"

def price_to_usdt(price_krw):
    return price_krw * KRW_TO_USD

# ================== 市场数据 ==================
def get_top_markets():
    markets = requests.get("https://api.upbit.com/v1/market/all").json()

    krw = [
        m["market"] for m in markets
        if m["market"].startswith("KRW-")
        and m["market"] not in EXCLUDE_MARKETS
    ]

    tickers = requests.get("https://api.upbit.com/v1/ticker", params={"markets": ",".join(krw)}).json()

    sorted_markets = sorted(tickers, key=lambda x: x["acc_trade_price_24h"], reverse=True)

    return [m["market"] for m in sorted_markets[:30]]

def get_trades(market):
    try:
        return requests.get(
            "https://api.upbit.com/v1/trades/ticks",
            params={"market": market, "count": 10},
            timeout=5
        ).json()
    except:
        return []

# ================== Binance价格 ==================
def get_binance_price(symbol):
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT"
        return float(requests.get(url, timeout=3).json()["price"])
    except:
        return None

# ================== 主逻辑 ==================
def run():
    logging.info("程序启动")
    send_telegram("✅ 系统已启动（终极版）")

    markets = get_top_markets()
    seen = set()

    # 状态缓存
    buy_flow = {}
    last_price = {}

    while True:
        try:
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

                        usd = price * volume * KRW_TO_USD
                        if usd < MIN_USD:
                            continue

                        symbol = market.replace("KRW-", "")
                        price_usdt = price_to_usdt(price)

                        # ================== 买卖方向 ==================
                        side_str = "🟢买单" if side == "BID" else "🔴卖单"

                        # ================== 时间 ==================
                        ts = t["timestamp"] / 1000
                        dt = datetime.datetime.utcfromtimestamp(ts) + datetime.timedelta(hours=8)
                        time_str = dt.strftime("%H:%M:%S")

                        # ================== 吸筹检测 ==================
                        if symbol not in buy_flow:
                            buy_flow[symbol] = 0

                        if side == "BID":
                            buy_flow[symbol] += 1
                        else:
                            buy_flow[symbol] = 0

                        absorb_signal = "📦吸筹中" if buy_flow[symbol] >= 3 else ""

                        # ================== 拉盘检测 ==================
                        pump_signal = ""
                        if symbol in last_price:
                            change = (price - last_price[symbol]) / last_price[symbol]
                            if change > 0.01:  # 1%瞬涨
                                pump_signal = "🚀拉盘启动"

                        last_price[symbol] = price

                        # ================== Binance确认 ==================
                        binance_price = get_binance_price(symbol)
                        confirm = ""
                        premium = ""

                        if binance_price:
                            diff = (price_usdt - binance_price) / binance_price

                            if abs(diff) < 0.01:
                                confirm = "✅同步上涨"
                            else:
                                confirm = "⚠️韩盘独立"

                            premium = f"溢价:{diff*100:.2f}%"

                        # ================== 消息 ==================
                        msg = (
                            f"{symbol}/USDT\n"
                            f"{side_str}\n"
                            f"💰 {format_usd(usd)}\n"
                            f"📍 {format_price(price_usdt)}\n"
                            f"{absorb_signal} {pump_signal}\n"
                            f"{confirm} {premium}\n"
                            f"⏰ {time_str}"
                        )

                        send_telegram(msg)

                    except Exception as e:
                        logging.error(f"{market}解析失败: {e}")

                time.sleep(0.2)

        except Exception as e:
            logging.error(f"主循环错误: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run()
