import json
import time
import threading
import requests
from datetime import datetime, timezone, timedelta
import websocket

# ==================== 配置 ====================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = 5671949305

# ==================== 监控合约列表（Binance 永续 + Upbit 热门币对应） ====================
SYMBOLS = [
    # 主流高流动性合约
    "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TONUSDT", "SUIUSDT",
    "TRXUSDT", "SHIBUSDT", "DOTUSDT", "LTCUSDT", "BCHUSDT",
    "BNBUSDT", "MATICUSDT", "HBARUSDT", "VETUSDT", "XLMUSDT",
    "DOGEUSDT","TAOUSDT", "SIGNUSDT", "KITEUSDT", "ZETAUSDT", "ATHUSDT",
    "CPOOLUSDT", "IPUSDT", "AKTUSDT", "SAHARAUSDT", "SUNUSDT"
]

# 阈值设置（USDT）
BIG_TRADE_THRESHOLD = 100000      # 大单阈值：10万美元（可调低测试）
LIQUIDATION_THRESHOLD = 100000    # 爆仓阈值：10万美元

# 北京时间
BEIJING_TZ = timezone(timedelta(hours=8))

def beijing_time():
    return datetime.now(BEIJING_TZ)

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
    print(f"[{beijing_time().strftime('%H:%M:%S')}] Binance Futures WebSocket 已连接（北京时间）")
    
    streams = [f"{s.lower()}@forceOrder" for s in SYMBOLS]
    streams.append("!forceOrder@arr")                    # 全市场爆仓
    streams.extend([f"{s.lower()}@aggTrade" for s in SYMBOLS])  # 大单
    
    payload = {"method": "SUBSCRIBE", "params": streams, "id": 1}
    ws.send(json.dumps(payload))
    print(f"已订阅 {len(SYMBOLS)} 个合约的爆仓 + 大单流")

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # === 爆仓处理 ===
        if data.get("e") == "forceOrder" or (isinstance(data, list) and data and data[0].get("e") == "forceOrder"):
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
北京时间：{beijing_time().strftime('%H:%M:%S')}
🔗 https://www.binance.com/en/futures/{symbol}
                """.strip()
                print(f"[{beijing_time().strftime('%H:%M:%S')}] 爆仓 → {symbol} ${amount:,.0f}")
                send_telegram(msg)
        
        # === 大单处理 ===
        elif data.get("e") == "aggTrade":
            symbol = data.get("s")
            qty = float(data.get("q", 0))
            price = float(data.get("p", 0))
            amount = qty * price
            is_buyer_maker = data.get("m", False)
            
            if amount >= BIG_TRADE_THRESHOLD:
                direction = "大额买入" if not is_buyer_maker else "大额卖出"
                msg = f"""
<b>🚨 Binance 大单警报！</b>
{symbol} {direction}
金额：${amount:,.0f} USDT
数量：{qty:,.2f}
价格：{price:,.4f}
北京时间：{beijing_time().strftime('%H:%M:%S')}
                """.strip()
                print(f"[{beijing_time().strftime('%H:%M:%S')}] 大单 → {symbol} ${amount:,.0f}")
                send_telegram(msg)
                
    except:
        pass

def on_error(ws, error):
    print("WebSocket 错误:", error)

def on_close(ws, *args):
    print(f"[{beijing_time().strftime('%H:%M:%S')}] WebSocket 关闭 → 5秒后重连")
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
    bt = beijing_time()
    print(f"Binance Futures 监控机器人启动成功！（北京时间 {bt.strftime('%Y-%m-%d %H:%M:%S')}）")
    print(f"监控合约数量: {len(SYMBOLS)} 个")
    print(f"大单阈值: ${BIG_TRADE_THRESHOLD:,} USDT")
    print(f"爆仓阈值: ${LIQUIDATION_THRESHOLD:,} USDT")
    
    start_msg = f"<b>🚀 Binance Futures 监控已启动</b>\n北京时间：{bt.strftime('%Y-%m-%d %H:%M:%S')}\n监控合约：{len(SYMBOLS)} 个\n大单阈值：${BIG_TRADE_THRESHOLD:,} USDT\n爆仓阈值：${LIQUIDATION_THRESHOLD:,} USDT"
    send_telegram(start_msg)
    
    threading.Thread(target=start_ws, daemon=True).start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序已停止")
