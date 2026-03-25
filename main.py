import json
import time
import threading
import requests
import websocket
from collections import defaultdict
from datetime import datetime, timezone, timedelta

# ==================== 配置 ====================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = 5671949305

ARKHAM_API_KEY = "19d2a233-8eaa-49be-bb02-403a8e636f9b"

# ==================== 参数 ====================
BIG_MONEY = 1_000_000   # 100万美元
CHECK_INTERVAL = 30

EXCLUDE_COINS = ["BTC", "ETH", "XRP", "SOL", "ADA", "DOGE"]

# ==================== 时间 ====================
BEIJING_TZ = timezone(timedelta(hours=8))
def now(): return datetime.now(BEIJING_TZ)

# ==================== Telegram ====================
def tg(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except:
        pass

# ==================== Upbit交易对 ====================
def get_symbols():
    try:
        r = requests.get("https://api.upbit.com/v1/market/all").json()
        return [x["market"] for x in r if x["market"].startswith("KRW-")]
    except:
        return []

SYMBOLS = get_symbols()

# ==================== 缓存 ====================
price_cache = defaultdict(list)
whale_cache = {}   # Arkham资金缓存

# ==================== Arkham监控 ====================
def arkham_monitor():
    print("🧠 Arkham启动")

    while True:
        try:
            url = "https://api.arkhamintelligence.com/transfers"

            headers = {
                "Authorization": f"Bearer {ARKHAM_API_KEY}"
            }

            res = requests.get(url, headers=headers, timeout=10).json()

            for tx in res.get("transfers", []):
                usd = float(tx.get("usdValue", 0))
                symbol = tx.get("tokenSymbol", "")
                to = str(tx.get("toEntityName", "")).lower()
                from_ = str(tx.get("fromEntityName", "")).lower()

                # ==================== 过滤 ====================
                if symbol in EXCLUDE_COINS:
                    continue

                if "exchange" in from_:
                    continue   # 过滤交易所内部

                if usd < BIG_MONEY:
                    continue

                # ==================== 记录主力资金 ====================
                whale_cache[symbol] = time.time()

                tg(f"""
🧠 <b>链上主力资金进入</b>
币种：{symbol}
金额：${usd:,.0f}
⚡ 关注是否拉盘
""")

        except Exception as e:
            print("Arkham错误:", e)

        time.sleep(CHECK_INTERVAL)

# ==================== Upbit ====================
def on_open(ws):
    for i in range(0, len(SYMBOLS), 30):
        ws.send(json.dumps([
            {"ticket": "x"},
            {"type": "trade", "codes": SYMBOLS[i:i+30]}
        ]))
        time.sleep(0.2)

def on_message(ws, message):
    try:
        d = json.loads(message)

        symbol = d.get("code", "")
        price = d.get("trade_price", 0)
        vol = d.get("trade_volume", 0)
        amount = price * vol
        t = time.time()

        coin = symbol.replace("KRW-", "")

        # ==================== 过滤主流币 ====================
        if coin in EXCLUDE_COINS:
            return

        # ==================== 价格缓存 ====================
        price_cache[symbol].append((t, price))
        price_cache[symbol] = [(ts, p) for ts, p in price_cache[symbol] if t - ts < 30]

        prices = [p for ts, p in price_cache[symbol]]

        if len(prices) < 5:
            return

        # ==================== 判断上涨 ====================
        if max(prices) > min(prices):

            # ==================== 核心联动 ====================
            if coin in whale_cache and t - whale_cache[coin] < 300:

                tg(f"""
🔥 <b>妖币启动（链上+价格共振）</b>
{symbol}
📈 价格开始上涨
🧠 主力资金已进入
⚡ 高概率拉盘
""")

                whale_cache.pop(coin, None)

    except:
        pass

def start_ws():
    ws = websocket.WebSocketApp(
        "wss://api.upbit.com/websocket/v1",
        on_open=on_open,
        on_message=on_message
    )
    ws.run_forever()

# ==================== 主程序 ====================
if __name__ == "__main__":
    tg("🚀 启动（Arkham + 妖币监控版）")

    threading.Thread(target=arkham_monitor, daemon=True).start()
    threading.Thread(target=start_ws, daemon=True).start()

    while True:
        time.sleep(1)
