import os
import time
import requests
import sys
from datetime import datetime

# ================= 配置区 =================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 只有溢价超过这个比例才发报警 (3% 比较敏感，5% 比较稳健)
PREMIUM_THRESHOLD = float(os.getenv("PREMIUM_THRESHOLD", "0.03"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))

# --- 垃圾币黑名单 ---
# 这些币种因为新老币替换、合约不同、或长期关闭充提，会导致数据错误
BLACKLIST = ["BEAM", "WAVES", "SNT", "ELF", "HIFI", "PLA"] 
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
    print(f"🚀 自动过滤版监控启动...", flush=True)
    send_telegram_alert("🤖 <b>监控已升级：自动过滤垃圾币模式</b>\n已屏蔽 BEAM/WAVES 等异常币种。")

    while True:
        try:
            rate = get_usd_to_krw()
            up_data = get_upbit_prices()
            bin_data = get_binance_prices()
            
            common_coins = set(up_data.keys()) & set(bin_data.keys())
            
            valid_stats = []
            alerts = []

            for coin in common_coins:
                # 1. 检查黑名单
                if coin in BLACKLIST:
                    continue
                
                u_price = up_data[coin]
                b_price = bin_data[coin]
                
                if b_price > 0:
                    b_price_krw = b_price * rate
                    premium = (u_price - b_price_krw) / b_price_krw
                    
                    # 2. 极端异常值过滤：
                    # 如果溢价绝对值超过 40%，基本可以断定该币种在 Upbit 处于“单机”或“维护”状态。
                    if abs(premium) > 0.40:
                        continue
                    
                    valid_stats.append({'coin': coin, 'p': premium})
                    
                    # 3. 报警判断
                    if abs(premium) >= PREMIUM_THRESHOLD:
                        emoji = "🚨" if premium > 0 else "📉"
                        alerts.append(
                            f"{emoji} <b>{coin}</b>\n"
                            f"溢价: <code>{premium*100:.2f}%</code>\n"
                            f"Upbit: {u_price:,.0f} | Bin: ${b_price:,.4f}"
                        )

            if alerts:
                for i in range(0, len(alerts), 5):
                    send_telegram_alert("<b>【实时异动】</b>\n\n" + "\n\n".join(alerts[i:i+5]))
                    time.sleep(1)

            # 日志显示正常的前三名
            top3 = sorted(valid_stats, key=lambda x: x['p'], reverse=True)[:3]
            top3_str = " | ".join([f"{item['coin']}:{item['p']*100:.1f}%" for item in top3])
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 有效币数:{len(valid_stats)} | 最高:{top3_str}", flush=True)

        except Exception as e:
            print(f"❌ 运行异常: {e}", flush=True)
        
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    monitor()
