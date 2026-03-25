import pyupbit
import requests
import time
import logging

# ------------------- Telegram 配置 -------------------
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

# ------------------- 大单阈值 & 监控币种 -------------------
BIG_TRADE_THRESHOLD = 27_000_000  # KRW, 约20万美元
MARKETS = ["KRW-SIGN", "KRW-CHZ", "KRW-MANA", "KRW-SAND", "KRW-ENJ"]

# ------------------- 日志设置 -------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ------------------- Telegram 推送 -------------------
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, data=data)
        if r.status_code != 200:
            logging.error(f"Telegram 推送失败: {r.text}")
    except Exception as e:
        logging.error(f"Telegram 异常: {e}")

send_telegram("✅ Telegram 测试消息：大单监控程序已启动！")

# ------------------- 轮询 REST API -------------------
last_seen = {market: None for market in MARKETS}

while True:
    try:
        for market in MARKETS:
            ticks = pyupbit.get_ticks(market, count=10)  # 最近10笔成交
            for tick in reversed(ticks):  # 从旧到新
                trade_time = tick['timestamp']
                price = tick['trade_price']
                volume = tick['trade_volume']
                amount = price * volume

                # 避免重复通知
                if last_seen[market] is not None and trade_time <= last_seen[market]:
                    continue

                if amount >= BIG_TRADE_THRESHOLD:
                    msg = f"💰 大单成交！\n交易对: {market}\n价格: {price:,} KRW\n数量: {volume}\n成交额: {amount:,} KRW"
                    logging.info(msg)
                    send_telegram(msg)

                last_seen[market] = trade_time

        time.sleep(5)  # 每5秒轮询一次

    except KeyboardInterrupt:
        logging.info("程序手动停止")
        break
    except Exception as e:
        logging.error(f"异常: {e}")
        time.sleep(5)
