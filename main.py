import requests
import time
import logging

# ------------------- Telegram 配置 -------------------
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

# ------------------- 参数 -------------------
BIG_TRADE_THRESHOLD = 27_000_000  # 20万美元
REQUEST_INTERVAL = 0.2   # 每个请求间隔（防限流）
LOOP_INTERVAL = 1        # 每轮循环间隔

MARKETS = [
    "KRW-SIGN",
    "KRW-CHZ",
    "KRW-MANA",
    "KRW-SAND",
    "KRW-ENJ",
    "KRW-ANKR",
    "KRW-LOOM",
]

# ------------------- 日志 -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("程序启动")

# ------------------- Telegram -------------------
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
    try:
        r = requests.post(url, data=data, timeout=10)
        if r.status_code == 200:
            logging.info("Telegram 推送成功")
        else:
            logging.error(f"Telegram失败: {r.text}")
    except Exception as e:
        logging.error(f"Telegram异常: {e}")

send_telegram("✅ 大单监控程序已启动（限频稳定版）")

# ------------------- 记录已处理数据 -------------------
last_timestamp = {m: 0 for m in MARKETS}

# ------------------- 主循环 -------------------
while True:
    try:
        for market in MARKETS:
            try:
                url = f"https://api.upbit.com/v1/trades/ticks?market={market}&count=10"
                r = requests.get(url, timeout=10)
                ticks = r.json()

                for tick in reversed(ticks):
                    ts = tick["timestamp"]

                    if ts <= last_timestamp[market]:
                        continue

                    price = tick["trade_price"]
                    volume = tick["trade_volume"]
                    amount = price * volume

                    # 过滤机器人（简单版）
                    if volume < 0.01:
                        continue

                    if amount >= BIG_TRADE_THRESHOLD:
                        msg = (
                            f"💰 大单成交\n"
                            f"{market}\n"
                            f"价格: {price:,} KRW\n"
                            f"数量: {volume}\n"
                            f"成交额: {amount:,.0f} KRW"
                        )
                        logging.info(msg)
                        send_telegram(msg)

                    last_timestamp[market] = ts

                # 👉 每个请求间隔（防限流）
                time.sleep(REQUEST_INTERVAL)

            except Exception as e:
                logging.error(f"{market} 请求失败: {e}")
                time.sleep(1)

        # 👉 每轮循环间隔
        time.sleep(LOOP_INTERVAL)

    except KeyboardInterrupt:
        logging.info("程序停止")
        break
    except Exception as e:
        logging.error(f"主循环异常: {e}")
        time.sleep(2)
