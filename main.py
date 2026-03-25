import websocket
import json
import threading
import time
import requests
import logging
from datetime import datetime, timezone, timedelta

# =========================
# 🔑 Telegram 配置（自己填）
# =========================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

# =========================
# ⚙️ 参数配置
# =========================
MIN_USD = 50000        # 最低提醒（降低了，保证有信号）
BIG_USD = 200000       # 大单
ACCUM_USD = 300000     # 吸筹识别

EXCHANGE_RATE = 1300   # KRW → USD（备用）

# =========================
# 🪙 屏蔽交易对
# =========================
BLACKLIST = ["BTC", "ETH", "USDT"]

# =========================
# 🌍 全局变量
# =========================
markets = []
known_markets = set()
accum_data = {}
binance_price = {}

# =========================
# 🧰 工具函数
# =========================
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg
        }, timeout=5)
    except:
        pass


def now_bj():
    return datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(
        timezone(timedelta(hours=8))
    ).strftime("%H:%M:%S")


def format_usd(x):
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    elif x >= 1000:
        return f"${x/1000:.1f}K"
    return f"${x:.0f}"


# =========================
# 🚀 获取Top30交易对
# =========================
def get_top_markets():
    try:
        url = "https://api.upbit.com/v1/ticker"
        all_markets = requests.get("https://api.upbit.com/v1/market/all").json()

        krw_markets = [
            m["market"] for m in all_markets
            if m["market"].startswith("KRW-")
            and not any(x in m for x in BLACKLIST)
        ]

        res = requests.get(url, params={"markets": ",".join(krw_markets)}).json()

        res.sort(key=lambda x: x["acc_trade_price_24h"], reverse=True)

        top = [r["market"] for r in res[:30]]

        logging.info(f"当前监控: {top}")
        return top

    except Exception as e:
        logging.error(f"获取市场失败: {e}")
        return []


# =========================
# 🆕 新币检测
# =========================
def check_new_listing():
    global known_markets

    try:
        res = requests.get("https://api.upbit.com/v1/market/all").json()
        current = set([m["market"] for m in res if m["market"].startswith("KRW-")])

        if not known_markets:
            known_markets = current
            return

        new = current - known_markets

        for m in new:
            if any(x in m for x in BLACKLIST):
                continue

            symbol = m.replace("KRW-", "")

            send_telegram(
                f"🆕 新币上线\n{symbol}/USDT\n⚠️ 注意首波波动"
            )

        known_markets = current

    except Exception as e:
        logging.error(f"新币检测失败: {e}")


# =========================
# 💰 币安价格（校准）
# =========================
def update_binance_price():
    global binance_price
    while True:
        try:
            url = "https://api.binance.com/api/v3/ticker/price"
            res = requests.get(url, timeout=5).json()

            for item in res:
                if item["symbol"].endswith("USDT"):
                    coin = item["symbol"].replace("USDT", "")
                    binance_price[coin] = float(item["price"])

        except Exception as e:
            logging.error(f"币安价格失败: {e}")

        time.sleep(10)


# =========================
# 🧠 信号处理
# =========================
def handle_trade(data):
    try:
        if "cd" not in data:
            return

        market = data["cd"]
        coin = market.replace("KRW-", "")

        if coin in BLACKLIST:
            return

        price_krw = data.get("tp", 0)
        volume = data.get("tv", 0)
        side = data.get("ab", "")

        # ===== 用币安价格 =====
        if coin in binance_price:
            price_usd = binance_price[coin]
        else:
            price_usd = price_krw / EXCHANGE_RATE

        usd = price_usd * volume

        if usd < MIN_USD:
            return

        # =========================
        # 🧠 吸筹识别
        # =========================
        now = time.time()
        accum = accum_data.get(coin, {"usd": 0, "time": now})

        if now - accum["time"] < 60:
            accum["usd"] += usd
        else:
            accum = {"usd": usd, "time": now}

        accum_data[coin] = accum

        # =========================
        # 🟢 买卖判断
        # =========================
        side_str = "🟢买入" if side == "BID" else "🔴卖出"

        # =========================
        # 🚀 信号评分
        # =========================
        score = 0

        if usd > BIG_USD:
            score += 2
        if accum["usd"] > ACCUM_USD:
            score += 2

        if score < 2:
            return

        # =========================
        # 📩 Telegram消息
        # =========================
        msg = (
            f"{coin}/USDT\n"
            f"{side_str}\n"
            f"💰 {format_usd(usd)}\n"
            f"📍 {price_usd:.6f}\n"
            f"⏰ {now_bj()}"
        )

        send_telegram(msg)

    except Exception as e:
        logging.error(f"处理错误: {e}")


# =========================
# 🌐 WebSocket
# =========================
def on_message(ws, message):
    try:
        if isinstance(message, bytes):
            message = message.decode("utf-8")

        data = json.loads(message)

        if isinstance(data, dict):
            handle_trade(data)

    except Exception as e:
        logging.error(f"WS解析错误: {e}")


def run_ws():
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://api.upbit.com/websocket/v1",
                on_message=on_message
            )

            def on_open(ws):
                subscribe = [
                    {"ticket": "test"},
                    {"type": "trade", "codes": markets}
                ]
                ws.send(json.dumps(subscribe))

            ws.on_open = on_open

            ws.run_forever(ping_interval=30)

        except Exception as e:
            logging.error(f"WS断开重连: {e}")

        time.sleep(3)


# =========================
# 🏁 主程序
# =========================
def main():
    global markets

    logging.basicConfig(level=logging.INFO)
    logging.info("🚀 程序启动")

    send_telegram("✅ 监控系统已启动")

    markets = get_top_markets()

    threading.Thread(target=update_binance_price, daemon=True).start()
    threading.Thread(target=run_ws, daemon=True).start()

    while True:
        check_new_listing()

        # 每5分钟更新交易对
        if int(time.time()) % 300 == 0:
            markets = get_top_markets()

        time.sleep(30)


if __name__ == "__main__":
    main()
