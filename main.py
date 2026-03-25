import requests
import time
import datetime
import logging

# ================== 配置 ==================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

MIN_USD = 200000  # 大单阈值（20万美元）
KRW_TO_USD = 0.00075  # 汇率（可自行调整）

EXCLUDE_MARKETS = ["KRW-BTC", "KRW-ETH", "KRW-USDT"]

# ================== 日志 ==================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ================== 工具函数 ==================
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg
        }, timeout=10)
    except Exception as e:
        logging.error(f"Telegram发送失败: {e}")

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
    else:
        return f"{x:.6f}"

def price_to_usdt(price_krw):
    return price_krw * KRW_TO_USD

# ================== 获取Top30交易对 ==================
def get_top_markets():
    url = "https://api.upbit.com/v1/market/all"
    markets = requests.get(url).json()

    krw_markets = [
        m["market"] for m in markets
        if m["market"].startswith("KRW-")
        and m["market"] not in EXCLUDE_MARKETS
    ]

    ticker_url = "https://api.upbit.com/v1/ticker"
    tickers = requests.get(ticker_url, params={"markets": ",".join(krw_markets)}).json()

    sorted_markets = sorted(tickers, key=lambda x: x["acc_trade_price_24h"], reverse=True)

    return [m["market"] for m in sorted_markets[:30]]

# ================== 获取成交 ==================
def get_trades(market):
    url = "https://api.upbit.com/v1/trades/ticks"
    try:
        res = requests.get(url, params={"market": market, "count": 5}, timeout=5)
        return res.json()
    except Exception as e:
        logging.error(f"{market} 请求失败: {e}")
        return []

# ================== 主逻辑 ==================
def run():
    logging.info("程序启动")

    send_telegram("✅ 监控系统已启动")

    markets = get_top_markets()
    logging.info(f"监控交易对: {markets}")

    seen = set()

    while True:
        try:
            for market in markets:
                trades = get_trades(market)

                for t in trades:
                    try:
                        trade_id = t["sequential_id"]
                        if trade_id in seen:
                            continue
                        seen.add(trade_id)

                        price = t["trade_price"]
                        volume = t["trade_volume"]
                        side = t["ask_bid"]  # ASK卖 BID买

                        usd = price * volume * KRW_TO_USD

                        if usd < MIN_USD:
                            continue

                        # 买卖方向
                        if side == "BID":
                            side_str = "🟢买单"
                        else:
                            side_str = "🔴卖单"

                        # 时间转北京时间
                        ts = t["timestamp"] / 1000
                        dt = datetime.datetime.utcfromtimestamp(ts) + datetime.timedelta(hours=8)
                        time_str = dt.strftime("%H:%M:%S")

                        # 价格转USDT
                        price_usdt = price_to_usdt(price)

                        # 币种格式
                        symbol = market.replace("KRW-", "")

                        # ================== Telegram消息 ==================
                        msg = (
                            f"{symbol}/USDT\n"
                            f"{side_str}\n"
                            f"💰 {format_usd(usd)}\n"
                            f"📍 {format_price(price_usdt)}\n"
                            f"⏰ {time_str}"
                        )

                        send_telegram(msg)

                    except Exception as e:
                        logging.error(f"{market} 数据解析失败: {e}")

                time.sleep(0.3)  # 限速

        except Exception as e:
            logging.error(f"主循环异常: {e}")
            time.sleep(5)

# ================== 启动 ==================
if __name__ == "__main__":
    run()
