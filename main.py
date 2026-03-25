import requests
import time
import logging

# ------------------- 配置 -------------------
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

BIG_TRADE_THRESHOLD = 27_000_000
REQUEST_INTERVAL = 0.2
LOOP_INTERVAL = 1

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
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

logging.info("程序启动")

# ------------------- Telegram -------------------
def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=10)
    except Exception as e:
        logging.error(f"TG失败: {e}")

send_telegram("✅ 程序启动成功（稳定版）")

# ------------------- 状态 -------------------
last_timestamp = {m: 0 for m in MARKETS}

# ------------------- 主循环 -------------------
while True:
    try:
        for market in MARKETS:
            try:
                url = f"https://api.upbit.com/v1/trades/ticks?market={market}&count=10"
                r = requests.get(url, timeout=10)

                # ✅ 状态码检查
                if r.status_code != 200:
                    logging.error(f"{market} HTTP错误: {r.status_code}")
                    continue

                # ✅ JSON解析保护
                try:
                    ticks = r.json()
                except:
                    logging.error(f"{market} JSON解析失败")
                    continue

                # ✅ 类型检查（关键）
                if not isinstance(ticks, list):
                    logging.error(f"{market} 返回异常: {ticks}")
                    continue

                for tick in reversed(ticks):

                    # ✅ tick结构保护
                    if not isinstance(tick, dict):
                        continue

                    if "timestamp" not in tick:
                        continue

                    ts = tick["timestamp"]

                    if ts <= last_timestamp[market]:
                        continue

                    price = tick.get("trade_price", 0)
                    volume = tick.get("trade_volume", 0)

                    # 过滤异常数据
                    if price == 0 or volume == 0:
                        continue

                    amount = price * volume

                    # 简单过滤机器人
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

                time.sleep(REQUEST_INTERVAL)

            except Exception as e:
                logging.error(f"{market} 请求异常: {e}")
                time.sleep(1)

        time.sleep(LOOP_INTERVAL)

    except KeyboardInterrupt:
        logging.info("程序停止")
        break
    except Exception as e:
        logging.error(f"主循环异常: {e}")
        time.sleep(2)
