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
    except:
        pass

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

# ================== 市场 ==================
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

def get_binance_price(symbol):
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT"
        return float(requests.get(url, timeout=3).json()["price"])
    except:
        return None

# ================== 主逻辑 ==================
def run():
    logging.info("启动（信号系统版）")
    send_telegram("🚀 信号系统已启动")

    markets = get_top_markets()
    seen = set()

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

                        # ===== 买卖方向 =====
                        side_str = "🟢买单" if side == "BID" else "🔴卖单"

                        # ===== 时间 =====
                        ts = t["timestamp"] / 1000
                        dt = datetime.datetime.utcfromtimestamp(ts) + datetime.timedelta(hours=8)
                        time_str = dt.strftime("%H:%M:%S")

                        # ===== 吸筹 =====
                        buy_flow[symbol] = buy_flow.get(symbol, 0)
                        if side == "BID":
                            buy_flow[symbol] += 1
                        else:
                            buy_flow[symbol] = 0

                        absorb = buy_flow[symbol] >= 3

                        # ===== 拉盘 =====
                        pump = False
                        if symbol in last_price:
                            change = (price - last_price[symbol]) / last_price[symbol]
                            if change > 0.01:
                                pump = True

                        last_price[symbol] = price

                        # ===== Binance =====
                        binance_price = get_binance_price(symbol)
                        confirm = False
                        premium = 0

                        if binance_price:
                            diff = (price_usdt - binance_price) / binance_price
                            premium = diff * 100
                            confirm = abs(diff) < 1

                        # ===== 评分系统 =====
                        score = 0

                        if usd > 300000:
                            score += 20
                        elif usd > 200000:
                            score += 10

                        if absorb:
                            score += 25

                        if pump:
                            score += 25

                        if confirm:
                            score += 20

                        if premium > 2:
                            score += 10

                        # ===== 信号等级 =====
                        level = ""
                        if score >= 90:
                            level = "🔴极强"
                        elif score >= 75:
                            level = "🟠强"
                        elif score >= 60:
                            level = "🟡关注"
                        else:
                            level = "⚪普通"

                        # ===== 买入提示 =====
                        buy_signal = ""
                        if absorb and pump and confirm and side == "BID":
                            buy_signal = "🟢买入信号"

                        # ===== 消息 =====
                        msg = (
                            f"{symbol}/USDT\n"
                            f"{level} {buy_signal}\n"
                            f"{side_str}\n"
                            f"💰 {format_usd(usd)}\n"
                            f"📍 {format_price(price_usdt)}\n"
                            f"📊评分: {score}\n"
                            f"溢价: {premium:.2f}%\n"
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
