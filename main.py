import json
import uuid
import threading
import time
import gzip
import requests
from datetime import datetime
import websocket  # pip install websocket-client requests

# ==================== 配置区（所有变量必须在这里定义） ====================
UPBIT_REST = "https://api.upbit.com"             # 韩国官方端点（数据最全）
WS_URL = "wss://api.upbit.com/websocket/v1"

BIG_TRADE_THRESHOLD = 50_000_000

MARKETS = [
    "KRW-TRX", "KRW-XRP", "KRW-BTC", "KRW-ETH", "KRW-USDT", "KRW-SOL",
    "KRW-BARD", "KRW-SIGN", "KRW-ORDER", "KRW-ZETA", "KRW-DOGE", "KRW-IP",
    "KRW-KITE", "KRW-TAO", "KRW-NOM", "KRW-IMX", "KRW-0G", "KRW-ETHFI",
    "KRW-ADA", "KRW-SUI", "KRW-VIRTUAL", "KRW-SUN", "KRW-CPOOL", "KRW-ATH",
    "KRW-MANTRA", "KRW-ENSO", "KRW-TRUMP", "KRW-LINK", "KRW-AVAX", "KRW-TON",
    "KRW-SHIB", "KRW-BCH", "KRW-DOT", "KRW-LTC", "KRW-BNB", "KRW-MATIC",
    "KRW-ICP", "KRW-ATOM", "KRW-HBAR", "KRW-VET", "KRW-XLM", "KRW-FIL"
]

TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = 5671949305

# ==================== Telegram 发送函数（加强版调试） ====================
def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        response = requests.post(url, json=payload, timeout=15)
        print(f"[Telegram Debug] 状态码: {response.status_code}")
        if response.status_code == 200:
            print("[Telegram] ✅ 发送成功")
        else:
            result = response.json()
            print(f"[Telegram] ❌ 发送失败: {result.get('description', '未知错误')}")
    except Exception as e:
        print(f"[Telegram] ❌ 异常: {type(e).__name__} - {e}")

# ==================== 获取真实 chat_id 调试函数 ====================
def get_my_chat_id():
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        print("[DEBUG] getUpdates 返回内容:", data)
        if data.get("result"):
            chat_id = data["result"][0]["message"]["chat"]["id"]
            print(f"[DEBUG] 检测到的真实 chat_id 是: {chat_id}")
            print(f"[DEBUG] 你当前代码里写的 chat_id 是: {TELEGRAM_CHAT_ID}")
        else:
            print("[DEBUG] 目前没有新消息，请先用 Telegram 私聊你的 Bot 并发送 /start")
    except Exception as e:
        print(f"[DEBUG] 获取 chat_id 失败: {e}")

# ==================== Upbit REST 验证 ====================
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
    print("正在验证市场有效性...")
    markets_info = upbit_api("/v1/market/all")
    if not markets_info:
        print("无法获取市场列表，继续运行")
        return
    valid = {m["market"] for m in markets_info}
    invalid = [m for m in MARKETS if m not in valid]
    if invalid:
        print(f"⚠️ 以下币种可能不存在：{', '.join(invalid[:10])} ... 等")
    else:
        print("所有币种验证通过")

# ==================== WebSocket 部分（保持不变） ====================
def on_open(ws):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] WebSocket 已连接 → 订阅 {len(MARKETS)} 个币种...")
    ticket = str(uuid.uuid4())
    subscribe_payload = [
        {"ticket": ticket},
        {"type": "trade",    "codes": MARKETS},
        {"type": "orderbook", "codes": [f"{m}.15" for m in MARKETS]},
        {"type": "ticker",   "codes": MARKETS}
    ]
    ws.send(json.dumps(subscribe_payload))
    print("订阅已发送")

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
    if not code: return
    price = float(data.get("trade_price", 0))
    volume = float(data.get("trade_volume", 0))
    amount = price * volume
    if amount < BIG_TRADE_THRESHOLD: return
    ts = datetime.fromtimestamp(data.get("trade_timestamp", 0) / 1000)
    direction = "买单（BID）" if data.get("ask_bid") == "BID" else "卖单（ASK）"
    msg = f"""
<b>大单触发！</b> {direction}
币种：{code}
📈 价格：{price:,.0f} KRW
📦 成交量：{volume:.6f} {code.split('-')[1]}
金额：{amount:,.0f} KRW
🕒 时间：{ts.strftime('%H:%M:%S')}
🔗 https://upbit.com/exchange?code=CRIX.UPBIT.{code}
    """.strip()
    print(msg.replace("<b>", "").replace("</b>", ""))
    threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()

def handle_orderbook(data):
    code = data.get("code")
    units = data.get("orderbook_units", [])
    if units:
        print(f"[{code}] 订单簿 | 买一 {units[0].get('bid_price',0):,.0f} | 卖一 {units[0].get('ask_price',0):,.0f}")

def handle_ticker(data):
    code = data.get("code")
    price = data.get("trade_price")
    print(f"[{code}] Ticker | 当前价 {price:,.0f} KRW")

def on_error(ws, error):
    print("WebSocket 错误:", error)

def on_close(ws, *args):
    print("WebSocket 关闭 → 5秒后重连")
    time.sleep(5)
    start_websocket()

def start_websocket():
    ws_app = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws_app.run_forever(ping_interval=30, ping_timeout=10)

# ==================== 主程序 ====================
if __name__ == "__main__":
    print("Upbit 大单监控机器人启动中...")
    print(f"监控币种数量: {len(MARKETS)} 个")
    
    validate_markets()
    
    # Telegram 测试
    print("\n[Telegram 测试] 正在发送测试消息...")
    get_my_chat_id()                     # 先打印真实 chat_id
    test_msg = f"<b>【机器人启动测试】</b>\n时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n监控币种：{len(MARKETS)} 个\n请检查是否收到此消息"
    send_telegram(test_msg)
    
    print("─" * 60)
    
    # 启动 WebSocket
    threading.Thread(target=start_websocket, daemon=True).start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序已停止")
