import os
import time
import requests
import sys
from datetime import datetime

# ================= 配置区 =================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PREMIUM_THRESHOLD = float(os.getenv("PREMIUM_THRESHOLD", "0.05"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
# ==========================================

def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"【待发通知】: {message}", flush=True)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"❌ Telegram发送失败: {e}", flush=True)

def get_usd_to_krw():
    try:
        res = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=10).json()
        return float(res['rates']['KRW'])
    except:
        return 1350.0

def get_upbit_prices():
    try:
        m_url = "https://api.upbit.com/v1/market/all"
        markets = requests.get(m_url, timeout=10).json()
        krw_markets = [m['market'] for m in markets if m['market'].startswith('KRW-')]
        t_url = f"https://api.upbit.com/v1/ticker?markets={','.join(krw_markets)}"
        tickers = requests.get(t_url, timeout=10).json()
        return {t['market'].split('-')[1]: float(t['trade_price']) for t in tickers}
    except Exception as e:
        print(f"Upbit获取失败: {e}", flush=True)
        return {}

def get_binance_prices():
    """使用备用域名防止IP封锁"""
    endpoints = [
        "https://api1.binance.com/api/v3/ticker/price",
        "https://api3.binance.com/api/v3/ticker/price",
        "https://api.binance.com/api/v3/ticker/price"
    ]
    for url in endpoints:
        try:
            res = requests.get(url, timeout=10)
            data = res.json()
            if isinstance(data, list):
                return {item['symbol'].replace('USDT', ''): float(item['price']) 
                        for item in data if item.get('symbol', '').endswith('USDT')}
        except:
            continue
    return {}

def monitor():
    print(f"🚀 泡菜溢价全币种监控启动...", flush=True)
    send_telegram_alert("🤖 <b>全币种监控已上线</b>")

    while True:
        try:
            rate = get_usd_to_krw()
            upbit_prices = get_upbit_prices()
            binance_prices = get_binance_prices()
            
            common_coins = set(upbit_prices.keys()) & set(binance_prices.keys())
            
            if not common_coins:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 警告：未获取到共有币种数据，检查网络...", flush=True)
            else:
                alerts = []
                for coin in common_coins:
                    u_price = upbit_prices[coin]
                    b_price = binance_prices[coin]
                    
                    # 核心防御：确保币安价格大于 0
                    if b_price > 0:
                        b_price_krw = b_price * rate
                        premium = (u_price - b_price_krw) / b_price_krw
                        
                        if abs(premium) >= PREMIUM_THRESHOLD:
                            emoji = "🚨" if premium > 0 else "📉"
                            alerts.append(
                                f"{emoji} <b>{coin}</b>\n"
                                f"溢价: <code>{premium*100:.2f}%</code>\n"
                                f"Upbit: {u_price:,.0f}\n"
                                f"Binance: ${b_price:,.4f}"
                            )

                if alerts:
                    # 汇总报警，每次发 5 个
                    for i in range(0, len(alerts), 5):
                        send_telegram_alert("<b>【溢价异常】</b>\n\n" + "\n\n".join(alerts[i:i+5]))
                        time.sleep(1)

                print(f"[{datetime.now().strftime('%H:%M:%S')}] 扫描完成，对比币种: {len(common_coins)}", flush=True)

        except Exception as e:
            print(f"❌ 运行异常: {e}", flush=True)
        
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    monitor()
