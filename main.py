import json
import time
import threading
import requests
import os
from datetime import datetime, timezone, timedelta
import websocket
from collections import defaultdict

# ==================== Telegram ====================
TELEGRAM_TOKEN =  "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = 5671949305

# ==================== 参数 ====================
BIG_TRADE = 200_000_000
SUPER_BIG = 600_000_000

WINDOW = 10
MIN_COUNT = 3

BREAK_WINDOW = 30   # 价格突破窗口
PUMP_WINDOW = 10
PUMP_COUNT = 4

# ==================== 时间 ====================
BEIJING_TZ = timezone(timedelta(hours=8))
def now(): return datetime.now(BEIJING_TZ)

# ==================== 获取交易对 ====================
def get_symbols():
    try:
        r = requests.get("https://api.upbit.com/v1/market/all").json()
        return [x["market"] for x in r if x["market"].startswith("KRW-")]
    except:
        return ["KRW-BTC","KRW-ETH"]

SYMBOLS = get_symbols()

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

# ==================== 缓存 ====================
trade_cache = defaultdict(list)
price_cache = defaultdict(list)

# ==================== WS ====================
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

        price = d.get("trade_price", 0)
        vol = d.get("trade_volume", 0)
        symbol = d.get("code", "")
        amount = price * vol
        side = "BUY" if d.get("ask_bid") == "BID" else "SELL"
        t = time.time()

        # ==================== 价格缓存 ====================
        price_cache[symbol].append((t, price))
        price_cache[symbol] = [(ts, p) for ts, p in price_cache[symbol] if t - ts <= BREAK_WINDOW]

        recent_prices = [p for ts, p in price_cache[symbol]]
        max_price = max(recent_prices) if recent_prices else price

        # ==================== 大单标签 ====================
        tag = ""
        if amount >= SUPER_BIG:
            tag = "⚠️ 巨鲸"
        elif amount >= BIG_TRADE:
            tag = "🐳 主力"

        # ==================== 大单 ====================
        if amount >= BIG_TRADE:
            tg(f"🚨 大单 {symbol}\n{tag} {side}\n₩{amount:,.0f}")

        # ==================== 缓存 ====================
        trade_cache[symbol].append({"t": t, "side": side, "amount": amount, "price": price})
        trade_cache[symbol] = [x for x in trade_cache[symbol] if t - x["t"] <= WINDOW]

        same = [x for x in trade_cache[symbol] if x["side"] == side and x["amount"] >= BIG_TRADE]

        # ==================== 连续大单 ====================
        if len(same) >= MIN_COUNT:
            total = sum(x["amount"] for x in same)
            tg(f"🔥 连续大单 {symbol}\n{side}\n{len(same)}次 ₩{total:,.0f}")
            trade_cache[symbol].clear()
            return

        # ==================== 🚀 价格突破 + 大单 ====================
        if price >= max_price and amount >= BIG_TRADE and side == "BUY":
            tg(f"""
🚀 <b>突破 + 大单确认</b>
{symbol}
价格突破 + 主力买入
₩{amount:,.0f}
""")
            trade_cache[symbol].clear()
            return

        # ==================== 🔥 妖币启动 ====================
        pump = [x for x in trade_cache[symbol] if t - x["t"] <= PUMP_WINDOW and x["side"] == "BUY"]

        if len(pump) >= PUMP_COUNT:
            total = sum(x["amount"] for x in pump)
            prices = [x["price"] for x in pump]

            if max(prices) > min(prices):  # 有上涨
                tg(f"""
🔥 <b>妖币启动</b>
{symbol}
连续拉升 {len(pump)}次
₩{total:,.0f}
⚡ 高概率主力拉盘
""")
                trade_cache[symbol].clear()

    except:
        pass

def on_close(ws, *args):
    time.sleep(5)
    start()

def start():
    ws = websocket.WebSocketApp(
        "wss://api.upbit.com/websocket/v1",
        on_open=on_open,
        on_message=on_message,
        on_close=on_close
    )
    ws.run_forever()

# ==================== 主程序 ====================
if __name__ == "__main__":
    tg("🚀 启动（突破+妖币识别版）")
    threading.Thread(target=start, daemon=True).start()

    while True:
        time.sleep(1)
