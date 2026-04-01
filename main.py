import os
import time
import requests
import sys
from datetime import datetime

# ================= 配置区 =================
# 从 Railway 环境变量读取，若本地测试可填写默认值
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 溢价报警阈值 (例如 0.05 代表 5%)
PREMIUM_THRESHOLD = float(os.getenv("PREMIUM_THRESHOLD", "0.05"))
# 检查间隔（秒），建议不要低于 30 秒，防止被封 IP
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
# ==========================================

def send_telegram_alert(message):
    """发送消息到 Telegram"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"【待发通知】: {message}", flush=True)
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print("✅ Telegram 通知已发出", flush=True)
        else:
            print(f"❌ Telegram 发送失败: {response.text}", flush=True)
    except Exception as e:
        print(f"❌ 网络异常，无法连接 Telegram: {e}", flush=True)

def get_usd_to_krw():
    """获取实时美元对韩元汇率"""
    try:
        # 使用免费汇率 API
        res = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=10).json()
        rate = res['rates']['KRW']
        return rate
    except Exception as e:
        print(f"汇率获取失败: {e}，使用保底汇率 1350", flush=True)
        return 1350.0

def get_upbit_prices():
    """获取 Upbit 所有韩元交易对的价格"""
    try:
        # 1. 获取所有市场
        m_url = "https://api.upbit.com/v1/market/all"
        markets = requests.get(m_url, timeout=10).json()
        krw_markets = [m['market'] for m in markets if m['market'].startswith('KRW-')]
        
        # 2. 批量获取报价
        t_url = f"https://api.upbit.com/v1/ticker?markets={','.join(krw_markets)}"
        tickers = requests.get(t_url, timeout=10).json()
        
        return {t['market'].split('-')[1]: float(t['trade_price']) for t in tickers}
    except Exception as e:
        print(f"获取 Upbit 数据失败: {e}", flush=True)
        return {}

def get_binance_prices():
    """获取 Binance 所有 USDT 交易对价格 (增加健壮性校验)"""
    try:
        url = "https://api.binance.com/api/v3/ticker/price"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        # 确保返回的是列表格式
        if not isinstance(data, list):
            print(f"币安 API 异常: {data}", flush=True)
            return {}

        prices = {}
        for item in data:
            if isinstance(item, dict) and 'symbol' in item and 'price' in item:
                if item['symbol'].endswith('USDT'):
                    coin = item['symbol'].replace('USDT', '')
                    prices[coin] = float(item['price'])
        return prices
    except Exception as e:
        print(f"获取 Binance 数据失败: {e}", flush=True)
        return {}

def monitor():
    print(f"🚀 泡菜溢价全币种监控启动...", flush=True)
    print(f"📈 报警阈值: {PREMIUM_THRESHOLD*100}% | 检查频率: {CHECK_INTERVAL}s", flush=True)
    
    send_telegram_alert("🤖 <b>全币种溢价监控已上线</b>\n正在同步 Upbit 与 Binance 数据...")

    while True:
        try:
            # 1. 更新基础数据
            rate = get_usd_to_krw()
            upbit_prices = get_upbit_prices()
            binance_prices = get_binance_prices()
            
            # 2. 找出共有币种
            common_coins = set(upbit_prices.keys()) & set(binance_prices.keys())
            
            alerts = []
            for coin in common_coins:
                u_price = upbit_prices[coin]
                b_price_krw = binance_prices[coin] * rate
                
                # 计算溢价
                premium = (u_price - b_price_krw) / b_price_krw
                
                # 只有超过阈值的才加入报警列表
                if abs(premium) >= PREMIUM_THRESHOLD:
                    emoji = "🚨" if premium > 0 else "📉"
                    alerts.append(
                        f"{emoji} <b>{coin}</b>\n"
                        f"溢价率: <code>{premium*100:.2f}%</code>\n"
                        f"Upbit: {u_price:,.0f} KRW\n"
                        f"Binance: ${binance_prices[coin]:,.4f}"
                    )

            # 3. 汇总发送报警 (分批发送防止触及 Telegram 字符限制)
            if alerts:
                for i in range(0, len(alerts), 5):
                    chunk = alerts[i:i+5]
                    msg = "<b>【实时溢价异常】</b>\n\n" + "\n\n".join(chunk)
                    send_telegram_alert(msg)
                    time.sleep(1) # 短暂停顿避免消息过于密集

            print(f"[{datetime.now().strftime('%H:%M:%S')}] 扫描完成，对比币种: {len(common_coins)}", flush=True)

        except Exception as e:
            print(f"❌ 监控主循环异常: {e}", flush=True)
        
        # 休眠进入下一轮
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    monitor()
