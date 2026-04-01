import os
import time
import requests
import asyncio
from datetime import datetime

# ================= 配置区 =================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# 溢价报警阈值 (例如 0.05 代表 5%)
PREMIUM_THRESHOLD = float(os.getenv("PREMIUM_THRESHOLD", "0.05"))
# 检查间隔（秒）
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
# ==========================================

def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"DEBUG: {message}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"Telegram发送失败: {e}")

def get_upbit_prices():
    """获取 Upbit 所有韩元交易对的价格 (KRW-BTC, KRW-ETH等)"""
    try:
        # 1. 获取所有市场列表
        markets_url = "https://api.upbit.com/v1/market/all"
        markets = requests.get(markets_url).json()
        krw_markets = [m['market'] for m in markets if m['market'].startswith('KRW-')]
        
        # 2. 获取实时报价
        ticker_url = f"https://api.upbit.com/v1/ticker?markets={','.join(krw_markets)}"
        tickers = requests.get(ticker_url).json()
        
        # 返回字典: {'BTC': 100000000, 'ETH': 5000000}
        return {t['market'].split('-')[1]: float(t['trade_price']) for t in tickers}
    except Exception as e:
        print(f"获取 Upbit 数据失败: {e}")
        return {}

def get_binance_prices():
    """获取 Binance 所有 USDT 交易对价格"""
    try:
        url = "https://api.binance.com/api/v3/ticker/price"
        data = requests.get(url).json()
        # 返回字典: {'BTC': 65000.0, 'ETH': 3500.0}
        return {item['symbol'].replace('USDT', ''): float(item['price']) for item in data if item['symbol'].endswith('USDT')}
    except Exception as e:
        print(f"获取 Binance 数据失败: {e}")
        return {}

def get_usd_to_krw():
    """获取实时汇率 (简易版，也可固定或使用API)"""
    try:
        # 这里使用一个免费汇率接口
        res = requests.get("https://api.exchangerate-api.com/v4/latest/USD").json()
        return res['rates']['KRW']
    except:
        return 1350.0  # 保底汇率

def monitor():
    print(f"🚀 泡菜溢价全币种监控启动... 阈值: {PREMIUM_THRESHOLD*100}%")
    send_telegram_alert("🔔 <b>泡菜溢价监控已启动</b>\n正在扫描 Upbit 与 Binance 共有币种...")

    while True:
        try:
            usd_krw = get_usd_to_krw()
            upbit_data = get_upbit_prices()
            binance_data = get_binance_prices()
            
            alerts = []
            
            # 找两个交易所的交集币种
            common_symbols = set(upbit_data.keys()) & set(binance_data.keys())
            
            for symbol in common_symbols:
                u_price_krw = upbit_data[symbol]
                b_price_usd = binance_data[symbol]
                
                # 将 Binance 价格换算为韩元
                b_price_krw = b_price_usd * usd_krw
                
                # 计算溢价: (Upbit - Binance) / Binance
                premium = (u_price_krw - b_price_krw) / b_price_krw
                
                if abs(premium) >= PREMIUM_THRESHOLD:
                    emoji = "🚨" if premium > 0 else "📉"
                    type_str = "正溢价" if premium > 0 else "负溢价"
                    alerts.append(
                        f"{emoji} <b>{symbol}</b>\n"
                        f"溢价率: <code>{premium*100:.2f}%</code>\n"
                        f"Upbit: {u_price_krw:,.0f} KRW\n"
                        f"Binance: ${b_price_usd:,.2f}"
                    )

            # 如果有报警，合并发送防止被 Telegram 限制
            if alerts:
                # 每次最多发 5 个，防止消息过长
                for i in range(0, len(alerts), 5):
                    full_msg = "<b>【溢价报警】</b>\n\n" + "\n\n".join(alerts[i:i+5])
                    send_telegram_alert(full_msg)
                    time.sleep(1) # 稍微停顿

            print(f"[{datetime.now().strftime('%H:%M:%S')}] 已检查 {len(common_symbols)} 个币种")
            
        except Exception as e:
            print(f"监控循环出错: {e}")
            
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    monitor()
