import os
import time
import requests
import sys
from datetime import datetime

# ================= 配置区 =================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# 只有溢价超过这个比例才发 Telegram 报警 (0.05 = 5%)
PREMIUM_THRESHOLD = float(os.getenv("PREMIUM_THRESHOLD", "0.05"))
# 检查频率（秒）
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
# ==========================================

def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"【控制台备份】: {message}", flush=True)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"❌ Telegram发送失败: {e}", flush=True)

def get_usd_to_krw():
    """获取实时汇率，失败则用 1350 保底"""
    try:
        res = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=10).json()
        return float(res['rates']['KRW'])
    except:
        return 1350.0

def get_upbit_prices():
    """获取 Upbit 所有 KRW 交易对"""
    try:
        m_url = "https://api.upbit.com/v1/market/all"
        markets = requests.get(m_url, timeout=10).json()
        krw_markets = [m['market'] for m in markets if m['market'].startswith('KRW-')]
        
        t_url = f"https://api.upbit.com/v1/ticker?markets={','.join(krw_markets)}"
        tickers = requests.get(t_url, timeout=10).json()
        return {t['market'].split('-')[1]: float(t['trade_price']) for t in tickers}
    except Exception as e:
        print(f"Upbit数据获取失败: {e}", flush=True)
        return {}

def get_binance_prices():
    """多域名轮询，绕过 Railway 的美国 IP 封锁"""
    endpoints = [
        "https://api1.binance.com/api/v3/ticker/price",
        "https://api3.binance.com/api/v3/ticker/price",
        "https://api2.binance.com/api/v3/ticker/price"
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
    print(f"📊 当前报警阈值: {PREMIUM_THRESHOLD*100}%", flush=True)
    send_telegram_alert("🤖 <b>全币种溢价监控(增强版)已上线</b>\n正在实时扫描 180+ 个币种...")

    while True:
        try:
            rate = get_usd_to_krw()
            upbit_prices = get_upbit_prices()
            binance_prices = get_binance_prices()
            
            common_coins = set(upbit_prices.keys()) & set(binance_prices.keys())
            
            if not common_coins:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ 未发现共有币种，检查 API 连接...", flush=True)
            else:
                current_stats = [] # 用于存储当前所有币种的溢价情况
                alerts = []       # 用于存储超过阈值的报警

                for coin in common_coins:
                    u_price = upbit_prices[coin]
                    b_price = binance_prices[coin]
                    
                    if b_price > 0:
                        b_price_krw = b_price * rate
                        premium = (u_price - b_price_krw) / b_price_krw
                        
                        # 记录当前溢价用于日志分析
                        current_stats.append({'coin': coin, 'p': premium})
                        
                        # 触发报警逻辑
                        if abs(premium) >= PREMIUM_THRESHOLD:
                            emoji = "🚨" if premium > 0 else "📉"
                            alerts.append(
                                f"{emoji} <b>{coin}</b>\n"
                                f"溢价: <code>{premium*100:.2f}%</code>\n"
                                f"Upbit: {u_price:,.0f} KRW\n"
                                f"Binance: ${b_price:,.4f}"
                            )

                # 1. 处理 Telegram 报警
                if alerts:
                    for i in range(0, len(alerts), 5):
                        send_telegram_alert("<b>【溢价异常报警】</b>\n\n" + "\n\n".join(alerts[i:i+5]))
                        time.sleep(1)

                # 2. 日志打印溢价前三名 (Top 3)
                top3 = sorted(current_stats, key=lambda x: x['p'], reverse=True)[:3]
                top3_str = " | ".join([f"{item['coin']}: {item['p']*100:.2f}%" for item in top3])
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 扫描 {len(common_coins)} 币 | Top3 溢价: {top3_str}", flush=True)

        except Exception as e:
            print(f"❌ 运行异常: {e}", flush=True)
        
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    monitor()
