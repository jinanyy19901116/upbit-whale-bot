import logging
import requests
import pyupbit

# ------------------- Telegram 配置 -------------------
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI" 
TELEGRAM_CHAT_ID = "5671949305"          

# ------------------- 大单阈值 & 监控币种 -------------------
BIG_TRADE_THRESHOLD = 27_000_000  # KRW, 约20万美元

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
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logging.info("程序启动")

# ------------------- Telegram 推送 -------------------
def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, data=data)
        if response.status_code == 200:
            logging.info("Telegram 推送成功")
        else:
            logging.error(f"Telegram 推送失败: {response.text}")
    except Exception as e:
        logging.error(f"Telegram 推送异常: {e}")

# ------------------- 启动测试消息 -------------------
send_telegram("✅ Telegram 测试消息：大单监控程序已启动！")

# ------------------- 机器人过滤 -------------------
def is_human_trade(trade):
    volume = trade.get("trade_volume", 0)
    if volume < 0.01 or volume == int(volume):
        return False
    return True

# ------------------- WebSocket 回调 -------------------
def handle_trade(msg):
    if msg.get("type") != "trade":
        return
    if not is_human_trade(msg):
        return

    price = msg["trade_price"]
    volume = msg["trade_volume"]
    amount = price * volume

    if amount >= BIG_TRADE_THRESHOLD:
        message = (
            f"💰 大单成交！\n"
            f"交易对: {msg['code']}\n"
            f"价格: {price:,} KRW\n"
            f"数量: {volume}\n"
            f"成交额: {amount:,} KRW"
        )
        logging.info(message)
        send_telegram(message)

# ------------------- 启动 WebSocket -------------------
if __name__ == "__main__":
    try:
        ws = pyupbit.WebSocketClient(
            type="trade",
            markets=MARKETS,
            callback=handle_trade
        )
        ws.run()
    except KeyboardInterrupt:
        logging.info("程序手动停止")
    except Exception as e:
        logging.error(f"异常退出: {e}")
