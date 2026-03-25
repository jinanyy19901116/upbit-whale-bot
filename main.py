import logging
import requests
import time
import pyupbit

# ------------------- Telegram 配置 -------------------
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

# ------------------- 大单阈值 & 监控币种 -------------------
BIG_TRADE_THRESHOLD = 27_000_000  # KRW，大约 20万美元

# 小币种列表
MARKETS = [
    "KRW-SIGN",
    "KRW-CHZ",
    "KRW-MANA",
    "KRW-SAND",
    "KRW-ENJ",
    "KRW-AAVE",
    "KRW-1INCH",
    "KRW-CRV",
    "KRW-ANKR",
    "KRW-LOOM",
]

# ------------------- 日志设置 -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("程序启动")

# ------------------- Telegram 推送 -------------------
def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, data=data)
        if r.status_code == 200:
            logging.info("Telegram 推送成功")
        else:
            logging.error(f"Telegram 推送失败: {r.text}")
    except Exception as e:
        logging.error(f"Telegram 异常: {e}")

# 启动测试推送
send_telegram("✅ Telegram 测试消息：大单监控程序已启动！")

# ------------------- 机器人交易过滤 -------------------
def is_human_trade(trade):
    volume = trade.get("trade_volume", 0)
    if volume < 0.01 or volume == int(volume):
        return False
    return True

# ------------------- 实时监听成交 -------------------
try:
    # 1. 使用 WebSocketManager 订阅成交数据
    wm = pyupbit.WebSocketManager("transaction", MARKETS)

    logging.info("WebSocketManager 启动完成")

    while True:
        data = wm.get()  # 每次返回一个成交事件
        if not data:
            time.sleep(0.1)
            continue

        # 交易类型字段可能是 "trade_price"/"trade_volume" 或者不同字段名
        price = data.get("trade_price") or data.get("price")
        volume = data.get("trade_volume") or data.get("volume")

        if price is None or volume is None:
            continue  # 无效记录就跳过

        # 过滤机器人
        if not is_human_trade({"trade_volume": volume}):
            continue

        amount = price * volume
        if amount >= BIG_TRADE_THRESHOLD:
            msg = (
                f"💰 大单成交！\n"
                f"交易对: {data.get('market') or data.get('code')}\n"
                f"价格: {price:,} KRW\n"
                f"数量: {volume}\n"
                f"成交额: {amount:,} KRW"
            )
            logging.info(msg)
            send_telegram(msg)

except KeyboardInterrupt:
    logging.info("程序手动停止")
    wm.terminate()
except Exception as e:
    logging.error(f"异常退出: {e}")
    try:
        wm.terminate()
    except:
        pass
