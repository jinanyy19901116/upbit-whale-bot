if __name__ == "__main__":
    print("启动 Upbit 大单监控机器人...")
    print(f"监控币种: {len(MARKETS)} 个")
    validate_markets()
    
    # ←←← 加这一行测试 Telegram 是否通
    send_telegram("<b>测试消息</b>\n这是代码自动发的测试通知\n如果收到，说明 Telegram 配置 OK！")
    
    threading.Thread(target=start_websocket, daemon=True).start()
    ...
import json
import uuid
import threading
import time
import gzip
import requests
from datetime import datetime
import websocket  # pip install websocket-client requests

# ==================== 配置 ====================
UPBIT_REST = "https://api.upbit.com"             # 韩国官方端点（主流币数据最多）
# UPBIT_REST = "https://sg-api.upbit.com"        # 海外新加坡端点（备用）

WS_URL = "wss://api.upbit.com/websocket/v1"      # 对应韩国 WS
# WS_URL = "wss://sg-api.upbit.com/websocket/v1" # 备用

BIG_TRADE_THRESHOLD = 50_000_000                 # 大单阈值 KRW

# 你指定的全部币种
MARKETS = [
    "KRW-TRX", "KRW-XRP", "KRW-BTC", "KRW-ETH", "KRW-USDT", "KRW-SOL",
    "KRW-BARD", "KRW-SIGN", "KRW-ORDER", "KRW-ZETA", "KRW-DOGE", "KRW-IP",
    "KRW-KITE", "KRW-TAO", "KRW-NOM", "KRW-IMX", "KRW-0G", "KRW-ETHFI",
    "KRW-ADA", "KRW-SUI", "KRW-VIRTUAL", "KRW-SUN", "KRW-CPOOL", "KRW-ATH",
    "KRW-MANTRA", "KRW-ENSO", "KRW-TRUMP", "KRW-LINK", "KRW-AVAX", "KRW-TON",
    "KRW-SHIB", "KRW-BCH", "KRW-DOT", "KRW-LTC", "KRW-BNB", "KRW-MATIC",
    "KRW-ICP", "KRW-ATOM", "KRW-HBAR", "KRW-VET", "KRW-XLM", "KRW-FIL"
]

# ==================== Telegram 配置 ====================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = 5671949305                    # 已填入你提供的 ID（正整数 = 个人聊天）

def send_telegram(msg: str):
    """
    发送 Telegram 消息，支持 HTML 格式
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print("[Telegram] 消息已发送")
        else:
            print(f"[Telegram 失败] {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[Telegram 发送异常]: {e}")


# ==================== Upbit REST 辅助（启动时验证市场） ====================
def upbit_api(path: str, params: dict = None):
    url = f"{UPBIT_REST}{path}"
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"REST API 错误 {path}: {e}")
        return None

def validate_markets():
    print("验证市场有效性...")
    markets_info = upbit_api("/v1/market/all")
    if not markets_info:
        print("无法获取市场列表，继续运行但可能有无效币种")
        return
    
    valid = {m["market"] for m in markets_info}
    invalid = [m for m in MARKETS if m not in valid]
    if invalid:
        print(f"⚠️ 以下币种可能未上线或无效：{', '.join(invalid)}")
    else:
        print("所有币种有效")


# ==================== WebSocket 回调 ====================
def on_open(ws):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] WS 已连接 → 订阅 {len(MARKETS)} 个币种...")
    
    ticket = str(uuid.uuid4())
    subscribe_payload = [
        {"ticket": ticket},
        {"type": "trade",    "codes": MARKETS},
        {"type": "orderbook", "codes": [f"{m}.15" for m in MARKETS]},
        {"type": "ticker",   "codes": MARKETS}
    ]
    
    ws.send(json.dumps(subscribe_payload))
    print("订阅成功：trade / orderbook(15档) / ticker")


def on_message(ws, message):
    if isinstance(message, bytes):
        try:
            message = gzip.decompress(message).decode('utf-8')
        except:
            return

    try:
        data_list = json.loads(message)
        if not isinstance(data_list, list):
            data_list = [data_list]

        for data in data_list:
            t = data.get("type")
            if t == "trade":
                handle_trade(data)
            elif t == "orderbook":
                handle_orderbook(data)
            elif t == "ticker":
                handle_ticker(data)
    except Exception as e:
        print("消息处理错误:", e)


def handle_trade(data):
    code = data.get("code")
    if not code:
        return

    price = float(data.get("trade_price", 0))
    volume = float(data.get("trade_volume", 0))
    amount_krw = price * volume

    if amount_krw < BIG_TRADE_THRESHOLD:
        return

    ts_ms = data.get("trade_timestamp")
    ts = datetime.fromtimestamp(ts_ms / 1000) if ts_ms else datetime.now()

    direction = "买单（BID）" if data.get("ask_bid") == "BID" else "卖单（ASK）"

    msg = f"""
<b>大单触发！</b> {direction}
币种：{code}
📈 价格：{price:,.0f} KRW
📦 成交量：{volume:.6f} {code.split('-')[1]}
金额：{amount_krw:,.0f} KRW
🕒 时间：{ts.strftime('%H:%M:%S')}
🔗 https://upbit.com/exchange?code=CRIX.UPBIT.{code}
    """.strip()

    print(msg.replace("<b>", "").replace("</b>", ""))
    threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()


def handle_orderbook(data):
    code = data.get("code")
    units = data.get("orderbook_units", [])
    if units:
        top_bid = units[0].get("bid_price", 0)
        top_ask = units[0].get("ask_price", 0)
        print(f"[{code}] 订单簿 | 买一 {top_bid:,.0f} | 卖一 {top_ask:,.0f} ({len(units)}档)")


def handle_ticker(data):
    code = data.get("code")
    price = data.get("trade_price")
    vol24 = data.get("acc_trade_volume_24h", 0)
    print(f"[{code}] Ticker | 价 {price:,.0f} KRW | 24h量 {vol24:.2f}")


def on_error(ws, error):
    print("WS 错误:", error)


def on_close(ws, code, msg):
    print(f"WS 关闭 ({code}): {msg} → 5s 后重连")
    time.sleep(5)
    start_websocket()


# ==================== 启动 ====================
def start_websocket():
    ws_app = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws_app.run_forever(ping_interval=30, ping_timeout=10)


if __name__ == "__main__":
    print("Upbit 大单监控启动...")
    print(f"监控币种: {len(MARKETS)} 个")
    validate_markets()
    
    threading.Thread(target=start_websocket, daemon=True).start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("已停止")
