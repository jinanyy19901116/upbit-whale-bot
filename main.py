import json
import time
import threading
import requests
from datetime import datetime
import websocket  # pip install websocket-client

# ==================== 配置 ====================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = 5671949305

# 要监控的永续合约（USDT 本位）
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
    "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TONUSDT", "SUIUSDT",
    "TAOUSDT", "SIGNUSDT"   # ← 你关心的币种可以继续加在这里，例如 "KITEUSDT"
]

# 大单阈值（USDT）
BIG_TRADE_THRESHOLD = 500000   # 50万美元以上，可改成 100000（10万）

# 爆仓金额阈值（USDT）
LIQUIDATION_THRESHOLD = 100000  # 10万美元以上爆仓提醒

HEADERS = {"User-Agent": "Mozilla/5.0"}

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
        print("[Telegram] 已推送")
    except Exception as e:
        print(f"[Telegram 失败]: {e}")

# ==================== WebSocket 回调 ====================
def on_open(ws):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Binance Futures WebSocket 已连接")
    
    # 订阅全市场爆仓流（最重要）
    streams = [f"{s.lower()}@forceOrder" for s in SYMBOLS]          # 单个符号爆仓
    streams.append("!forceOrder@arr")                               # 全市场爆仓（推荐）
    streams.extend([f"{s.lower()}@aggTrade" for s in SYMBOLS])      # 大单监控
    
    payload = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    }
    ws.send(json.dumps(payload))
    print("已订阅：爆仓流 + 大单流")

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # 处理爆仓事件
        if data.get("e") == "forceOrder" or (isinstance(data, list) and data[0].get("e") == "forceOrder"):
            order = data if isinstance(data, dict) else data[0]
            qty = float(order.get("q", 0))
            price = float(order.get("p", 0))
            amount = qty * price
            symbol = order.get("s", "UNKNOWN")
            
            if amount >= LIQUIDATION_THRESHOLD:
                side = "多单爆仓" if order.get("S") == "SELL" else "空单爆仓"
                msg = f"""
<b>💥 Binance 爆仓警报！</b>
{symbol} {side}
金额：${amount:,.0f} USDT
价格：{price:,.4f}
时间：{datetime.now().strftime('%H:%M:%S')}
🔗 https://www.binance.com/en/futures/{symbol}
                """.strip()
                print(f"爆仓触发 → {symbol} ${amount:,.0f}")
                send_telegram(msg)
        
        # 处理大单（aggTrade）
        elif data.get("e") == "aggTrade":
            symbol = data.get("s")
            qty = float(data.get("q", 0))
            price = float(data.get("p", 0))
            amount = qty * price
            is_buyer_maker = data.get("m", False)  # True=卖方主动
            
            if amount >= BIG_TRADE_THRESHOLD:
                direction = "大额买入" if not is_buyer_maker else "大额卖出"
                msg = f"""
<b>🚨 Binance 大单警报！</b>
{symbol} {direction}
金额：${amount:,.0f} USDT
数量：{qty:,.2f}
价格：{price:,.4f}
时间：{datetime.now().strftime('%H:%M:%S')}
                """.strip()
                print(f"大单触发 → {symbol} ${amount:,.0f}")
                send_telegram(msg)
                
    except Exception as e:
        pass  # 忽略解析错误

def on_error(ws, error):
    print("WebSocket 错误:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket 关闭 → 5秒后重连")
    time.sleep(5)
    start_ws()

def start_ws():
    ws = websocket.WebSocketApp(
        "wss://fstream.binance.com/ws",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(ping_interval=30, ping_timeout=10)

# ==================== 主程序 ====================
if __name__ == "__main__":
    print("Binance Futures 合约监控机器人启动中...")
    print(f"监控合约: {len(SYMBOLS)} 个")
    print(f"大单阈值: ${BIG_TRADE_THRESHOLD:,} USDT")
    print(f"爆仓阈值: ${LIQUIDATION_THRESHOLD:,} USDT")
    
    start_msg = f"<b>🚀 Binance Futures 监控已启动</b>\n监控合约：{len(SYMBOLS)} 个\n大单阈值：${BIG_TRADE_THRESHOLD:,}\n爆仓阈值：${LIQUIDATION_THRESHOLD:,}"
    send_telegram(start_msg)
    
    threading.Thread(target=start_ws, daemon=True).start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序已停止")
