import websocket
import json
import time
import threading
import requests
import os
from datetime import datetime

# ====================== 從 Railway 環境變數讀取 ======================
COINS = os.getenv("COINS", "KRW-BTC,KRW-ETH,KRW-SOL,KRW-XRP").split(",")
VOLUME_THRESHOLD_KRW = int(os.getenv("VOLUME_THRESHOLD_KRW", "50000000"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WS_URL = "wss://sg-api.upbit.com/websocket/v1"
# ================================================================

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=5)
    except:
        pass

def on_message(ws, message):
    try:
        data = json.loads(message)
        if data.get("type") != "trade":
            return

        code = data["code"]
        price = float(data["trade_price"])
        vol_coin = float(data["trade_volume"])
        krw_vol = price * vol_coin
        side = "🔴 大買單" if data["ask_bid"] == "BID" else "🔵 大賣單"
        
        if krw_vol >= VOLUME_THRESHOLD_KRW and code in COINS:
            timestamp = datetime.fromtimestamp(data["trade_timestamp"] / 1000)
            msg = f"""🚨 <b>Upbit 大單警報</b>

{code}
{side}
💰 成交金額：₩{krw_vol:,.0f}
📈 價格：{price:,.0f} KRW
📦 成交量：{vol_coin:.6f} {code.split('-')[1]}
🕒 時間：{timestamp.strftime('%H:%M:%S')}

🔗 https://upbit.com/exchange?code=CRIX.UPBIT.{code}"""
            
            print(msg.replace("<b>", "").replace("</b>", ""))
            threading.Thread(target=send_telegram, args=(msg,)).start()

    except Exception as e:
        print("解析錯誤:", e)

def on_open(ws):
    subscribe = [
        {"ticket": str(time.time())},
        {"type": "trade", "codes": COINS, "is_only_realtime": True},
        {"format": "DEFAULT"}
    ]
    ws.send(json.dumps(subscribe))
    print(f"✅ 已訂閱 {len(COINS)} 個幣種！閾值 {VOLUME_THRESHOLD_KRW:,} KRW")

def on_error(ws, error):
    print("WebSocket 錯誤:", error)

def on_close(ws, close_status_code, close_msg):
    print("連線斷開，5秒後重連...")
    time.sleep(5)

def main():
    print("🚀 Upbit 大單機器人已啟動（Railway 部署模式）...")
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except:
            time.sleep(5)

if name == "__main__":
    if not all([TELEGRAM_TOKEN, TELEGRAM_CHAT_ID]):
        print("❌ 請在 Railway 設定 TELEGRAM_TOKEN 和 TELEGRAM_CHAT_ID")
    else:
        main()
