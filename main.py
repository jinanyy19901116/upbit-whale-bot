import websocket
import json
import threading
import time
import requests
import logging
from datetime import datetime, timezone, timedelta

# =========================
# 🔑 Telegram（自己填）
# =========================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

# =========================
# ⚙️ 参数
# =========================
MIN_USD = 20000
BIG_USD = 200000
ACCUM_USD = 300000

EXCHANGE_RATE = 1300

BLACKLIST = ["BTC", "ETH", "USDT"]

markets = []
known_markets = set()
accum_data = {}
binance_price = {}

# =========================
# 📩 Telegram
# =========================
def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg
        }, timeout=5)
    except:
        pass


# =========================
# 🕒 北京时间
# =========================
def now_bj():
    return datetime.utcnow().replace(tzinfo=timezone.utc)\
        .astimezone(timezone(timedelta(hours=8)))\
        .strftime("%H:%M:%S")


# =========================
# 💰 格式化
# =========================
def format_usd(x):
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    elif x >= 1000:
        return f"${x/1000:.1f}K"
    return f"${x:.0f}"


# =========================
# 🚀 获取Top30
# =========================
def get_top_markets():
    try:
        all_markets = requests.get("https://api.upbit.com/v1/market/all").json()

        krw_markets = [
            m["market"] for m in all_markets
            if m["market"].startswith("KRW-")
            and not any(x in m for x in BLACKLIST)
        ]

        res = requests.get(
            "https://api.upbit.com/v1/ticker",
            params={"markets": ",".join(krw_markets)}
        ).json()

        if not isinstance(res, list):
            return []

        res.sort(key=lambda x: x["acc_trade_price_24h"], reverse=True)

        return [r["market"] for r in res[:30]]

    except Exception as e:
        logging.error(f"市场获取失败: {e}")
        return []


# =========================
# 🆕 新币检测
# =========================
def check_new_listing():
    global known_markets

    try:
        res = requests.get("https://api.upbit.com/v1/market/all").json()

        current = set([
            m["market"] for m in res
            if m["market"].startswith("KRW-")
        ])

        if not known_markets:
            known_markets = current
            return

        new = current - known_markets

        for m in new:
            if any(x in m for x in BLACKLIST):
                continue

            coin = m.replace("KRW-", "")

            send_telegram(
                f"🆕 新币上线\n{coin}/USDT\n⚠️ 注意波动"
            )

        known_markets = current

    except Exception as e:
        logging.error(f"新币检测失败: {e}")


# =========================
# 💰 币安价格（修复版）
# =========================
def update_binance_price():
    global binance_price

    while True:
        try:
            url = "https://api.binance.com/api/v3/ticker/price"
            res = requests.get(url, timeout=5)

            if res.status_code != 200:
                logging.error(f"币安HTTP错误: {res.status_code}")
                time.sleep(5)
                continue

            data = res.json()

            if not isinstance(data, list):
                logging.error(f"币安返回异常: {data}")
                time.sleep(5)
                continue

            temp = {}

            for item in data:
                symbol = item.get("symbol")
                price = item.get("price")

                if symbol and price and symbol.endswith("USDT"):
                    coin = symbol.replace("USDT", "")
                    temp[coin] = float(price)

            binance_price = temp

        except Exception as e:
            logging.error(f"币安价格失败: {e}")

        time.sleep(10)


# =========================
# 🧠 交易处理
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

        if volume == 0:
            return

        # ===== 优先币安价格 =====
        if coin in binance_price:
            price_usd = binance_price[coin]
        else:
            price_usd = price_krw / EXCHANGE_RATE

        usd = price_usd * volume

        if usd < MIN_USD:
            return

        # ===== 吸筹识别 =====
        now = time.time()
        acc = accum_data.get(coin, {"usd": 0, "time": now})

        if now - acc["time"] < 60:
            acc["usd"] += usd
        else:
            acc = {"usd": usd, "time": now}

        accum_data[coin] = acc

        side_str = "🟢买入" if side == "BID" else "🔴卖出"

        # ===== 信号评分 =====
        score = 0
        if usd > BIG_USD:
            score += 2
        if acc["usd"] > ACCUM_USD:
            score += 2

        if score < 2:
            return

        # ===== 消息 =====
        msg = (
            f"{coin}/USDT\n"
            f"{side_str}\n"
            f"💰 {format_usd(usd)}\n"
            f"📍 {price_usd:.6f}\n"
            f"⏰ {now_bj()}"
        )

        send_telegram(msg)

    except Exception as e:
        logging.error(f"处理失败: {e}")


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
        logging.error(f"WS错误: {e}")


def run_ws():
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://api.upbit.com/websocket/v1",
                on_message=on_message
            )

            def on_open(ws):
                ws.send(json.dumps([
                    {"ticket": "test"},
                    {"type": "trade", "codes": markets}
                ]))

            ws.on_open = on_open

            ws.run_forever(ping_interval=30)

        except Exception as e:
            logging.error(f"WS重连: {e}")

        time.sleep(3)


# =========================
# 🏁 主程序
# =========================
def main():
    global markets

    logging.basicConfig(level=logging.INFO)
    logging.info("🚀 启动")

    send_telegram("✅ 系统启动成功")

    markets = get_top_markets()

    threading.Thread(target=update_binance_price, daemon=True).start()
    threading.Thread(target=run_ws, daemon=True).start()

    while True:
        check_new_listing()

        # 每5分钟刷新
        if int(time.time()) % 300 == 0:
            markets = get_top_markets()

        time.sleep(30)


if __name__ == "__main__":
    main()
