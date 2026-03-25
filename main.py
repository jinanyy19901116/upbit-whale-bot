import websocket
import json
import threading
import time
import requests
import logging
from datetime import datetime, timezone, timedelta

# =========================
# Telegram 配置
# =========================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

# =========================
# 参数配置
# =========================
MIN_USD = 20000
BIG_USD = 200000
ACCUM_USD = 300000
EXCHANGE_RATE = 1300

BLACKLIST = ["BTC", "ETH", "USDT"]

markets = []
known_markets = set()
accum_data = {}
price_usd = {}

# =========================
# 工具函数
# =========================
def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
    except:
        pass

def now_bj():
    return datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%H:%M:%S")

def format_usd(x):
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    elif x >= 1000:
        return f"${x/1000:.1f}K"
    return f"${x:.0f}"

# =========================
# 获取Top30交易对
# =========================
def get_top_markets():
    try:
        all_markets = requests.get("https://api.upbit.com/v1/market/all").json()
        krw_markets = [m["market"] for m in all_markets if m["market"].startswith("KRW-") and not any(x in m for x in BLACKLIST)]
        res = requests.get("https://api.upbit.com/v1/ticker", params={"markets": ",".join(krw_markets)}).json()
        if not isinstance(res, list):
            return []
        res.sort(key=lambda x: x["acc_trade_price_24h"], reverse=True)
        top = [r["market"] for r in res[:30]]
        logging.info(f"监控市场: {top}")
        return top
    except Exception as e:
        logging.error(f"获取市场失败: {e}")
        return []

# =========================
# 新币检测
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
            coin = m.replace("KRW-", "")
            send_telegram(f"🆕 新币上线\n{coin}/USDT\n⚠️ 注意首波波动")
        known_markets = current
    except Exception as e:
        logging.error(f"新币检测失败: {e}")

# =========================
# CoinGecko 价格更新（限速保护版）
# =========================
def update_price():
    global price_usd
    while True:
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {"vs_currency":"usd","order":"market_cap_desc","per_page":250,"page":1}
            res = requests.get(url, params=params, timeout=10)
            if res.status_code == 429:
                logging.warning("CoinGecko 限速，使用 KRW兜底")
                time.sleep(60)
                continue
            data = res.json()
            if not isinstance(data, list):
                logging.warning(f"价格返回异常: {data}")
                time.sleep(60)
                continue
            temp = {}
            for coin in data:
                symbol = coin.get("symbol", "").upper()
                p = coin.get("current_price")
                if symbol and p:
                    temp[symbol] = float(p)
            price_usd = temp
        except Exception as e:
            logging.error(f"价格更新失败: {e}")
        time.sleep(60)  # 每60秒更新一次，避免限速

# =========================
# Fallback价格
# =========================
def get_price_fallback(coin, price_krw):
    try:
        return price_krw / EXCHANGE_RATE
    except:
        return 0

# =========================
# 处理成交信号
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

        usd_price = price_usd.get(coin, get_price_fallback(coin, price_krw))
        usd_total = usd_price * volume
        if usd_total < MIN_USD:
            return

        now = time.time()
        acc = accum_data.get(coin, {"usd":0,"time":now})
        if now - acc["time"] < 60:
            acc["usd"] += usd_total
        else:
            acc = {"usd":usd_total,"time":now}
        accum_data[coin] = acc

        side_str = "🟢买入" if side=="BID" else "🔴卖出"
        score = 0
        if usd_total>BIG_USD:
            score+=2
        if acc["usd"]>ACCUM_USD:
            score+=2
        if score<2:
            return

        msg = (
            f"{coin}/USDT\n"
            f"{side_str}\n"
            f"💰 {format_usd(usd_total)}\n"
            f"📍 {usd_price:.6f}\n"
            f"⏰ {now_bj()}"
        )
        send_telegram(msg)
    except Exception as e:
        logging.error(f"处理失败: {e}")

# =========================
# WebSocket 高频
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
                    {"ticket":"test"},
                    {"type":"trade","codes":markets}
                ]))
            ws.on_open = on_open
            ws.run_forever(ping_interval=30)
        except Exception as e:
            logging.error(f"WS重连: {e}")
        time.sleep(3)

# =========================
# 主程序
# =========================
def main():
    global markets
    logging.basicConfig(level=logging.INFO)
    logging.info("🚀 系统启动")
    send_telegram("✅ 系统启动成功")
    markets = get_top_markets()

    threading.Thread(target=update_price, daemon=True).start()
    threading.Thread(target=run_ws, daemon=True).start()

    while True:
        check_new_listing()
        if int(time.time()) % 300 == 0:
            markets = get_top_markets()
        time.sleep(30)

if __name__=="__main__":
    main()
