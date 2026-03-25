import logging
import requests
from pyupbit import WebSocketClient

# ------------------- Telegram 配置 -------------------
# TODO: 在这里填写你的 Telegram Bot token 和 chat_id
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

# ------------------- 大单阈值 & 监控币种 -------------------
# 20万美元≈27,000,000 KRW，可根据汇率调整
BIG_TRADE_THRESHOLD = 27_000_000  

# 监控交易对（小币种，韩国人偏好）
MARKETS = [
    "KRW-SIGN",   # Signum
    "KRW-CHZ",    # Chiliz
    "KRW-MANA",   # Decentraland
    "KRW-SAND",   # Sandbox
    "KRW-ENJ",    # Enjin Coin
    "KRW-AAVE",   # Aave
    "KRW-1INCH",  # 1inch
    "KRW-CRV",    # Curve
    "KRW-ANKR",   # Ankr
    "KRW-LOOM",   # Loom Network
]

# ------------------- 日志设置 -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("程序启动")

# ------------------- Telegram 推送函数 -------------------
def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, data=data)
        if response.status_code == 200:
            logging.info("Telegram 推送成功")
        else:
            logging.error(f"Telegram 推送失败: {response.text}")
    except Exception as e:
        logging.error(f"Telegram 推送异常: {e}")

# ------------------- 启动 Telegram 测试消息 -------------------
send_telegram("✅ Telegram 测试消息：大单监控程序已启动！")

# ------------------- 简单机器人过滤函数 -------------------
def is_human_trade(trade):
    """
    简单过滤逻辑：
    - 成交量过小或过于规则可能是机器人交易
    """
    volume = trade.get("trade_volume", 0)
    if volume < 0.01 or volume == int(volume):
        return False
    return True

# ------------------- WebSocket 回调 -------------------
def on_trade(msg):
    """
    msg 示例：
    {
        "type": "trade",
        "code": "KRW-SIGN",
        "trade_price": 1000,
        "trade_volume": 1000,
        ...
    }
    """
    if msg.get("type") != "trade":
        return

    if not is_human_trade(msg):
        return  # 过滤机器人交易

    price = msg["trade_price"]
    volume = msg["trade_volume"]
    amount = price * volume  # 单笔成交金额

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

# ------------------- 启动 WebSocket（最新版 pyupbit） -------------------
if __name__ == "__main__":
    try:
        WebSocketClient.run_websocket_client(
            markets=MARKETS,
            type="trade",
            callback=on_trade
        )
    except KeyboardInterrupt:
        logging.info("程序手动停止")
    except Exception as e:
        logging.error(f"异常退出: {e}")
