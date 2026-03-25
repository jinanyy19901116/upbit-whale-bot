import time
from collections import defaultdict
import threading
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import requests

# ==================== 配置 ====================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = 5671949305

# ==================== 参数 ====================
BIG_MONEY = 200_000          # 单笔大单金额（美元）
CHECK_INTERVAL = 60          # 每分钟刷新
EXCLUDE_COINS = ["BTC","ETH","XRP","SOL","ADA","DOGE"]
KOREA_EXCHANGES = ["upbit","bithumb","coinone","korbit"]
CONSECUTIVE_COUNT = 3        # 连续大单次数触发信号
CONSECUTIVE_WINDOW = 300     # 5分钟窗口统计连续大单
PRICE_CHECK_MARGIN = 0.05    # 价格共振上下5%范围作为参考买入区间

# ==================== Telegram ====================
def tg(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"},
            timeout=10
        )
    except Exception as e:
        print("Telegram推送失败:", e)

# ==================== 缓存 ====================
flow_cache = defaultdict(list)
known_coins = set()
price_cache = defaultdict(list)

# ==================== Selenium 爬取 Arkham 数据 ====================
def crawl_arkham_selenium():
    url = "https://arkhamintelligence.com/transfers"  # Arkham交易列表
    transfers = []

    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(options=chrome_options)

        driver.get(url)
        time.sleep(5)

        rows = driver.find_elements(By.CSS_SELECTOR,"table tbody tr")
        for row in rows:
            cols = row.find_elements(By.TAG_NAME,"td")
            if len(cols)<4:
                continue
            symbol = cols[0].text.strip().upper()
            try:
                usd = float(cols[1].text.replace("$","").replace(",","").strip())
            except:
                usd = 0
            exchange = cols[2].text.strip().lower()
            t = cols[3].text.strip()
            transfers.append({"symbol":symbol,"usdValue":usd,"toEntityName":exchange,"time":t})
        driver.quit()
    except Exception as e:
        print("Selenium爬取失败:", e)
    return transfers

# ==================== Selenium 爬取交易所实时价格 ====================
def fetch_price(symbol):
    # 示例：使用 Upbit 公开接口获取 KRW 价格（或替换为其他交易所API）
    try:
        url = f"https://api.upbit.com/v1/ticker?markets=KRW-{symbol}"
        r = requests.get(url,timeout=5)
        if r.status_code==200:
            price = r.json()[0]["trade_price"]
            return price
    except:
        return None
    return None

# ==================== 主监控逻辑 ====================
def monitor():
    print("🇰🇷 Arkham Selenium 监控 + 新币提醒 + 连续大单 + 实时价格启动")
    while True:
        now_time = time.time()
        transfers = crawl_arkham_selenium()
        for tx in transfers:
            symbol = tx.get("symbol","")
            usd = float(tx.get("usdValue",0))
            exchange = tx.get("toEntityName","").lower()

            if symbol in EXCLUDE_COINS or usd<BIG_MONEY:
                continue
            if not any(ex in exchange for ex in KOREA_EXCHANGES):
                continue

            # ==================== 新币提醒 ====================
            if symbol not in known_coins:
                tg(f"🆕 <b>新币上线</b>\n币种：{symbol}\n首次大单 ≥${BIG_MONEY:,.0f}")
                known_coins.add(symbol)

            # ==================== 连续大单缓存 ====================
            flow_cache[symbol].append({"time":now_time,"amount":usd})
            flow_cache[symbol] = [x for x in flow_cache[symbol] if now_time - x["time"]<CONSECUTIVE_WINDOW]

            # ==================== 单笔大额提示 ====================
            # 获取实时价格
            price = fetch_price(symbol)
            price_info = f"\n当前价格：{price:,.2f} KRW" if price else ""
            # 参考买入区间
            if price:
                lower = price*(1-PRICE_CHECK_MARGIN)
                upper = price*(1+PRICE_CHECK_MARGIN)
                price_info += f"\n参考买入区间：{lower:,.2f} ~ {upper:,.2f} KRW"

            tg(f"🇰🇷 <b>大单资金流</b>\n币种：{symbol}\n金额：${usd:,.0f}\n交易所：{exchange}{price_info}")

            # ==================== 连续大单信号 ====================
            if len(flow_cache[symbol])>=CONSECUTIVE_COUNT:
                total = sum(x["amount"] for x in flow_cache[symbol])
                tg(f"🔥 <b>连续大单警告（妖币启动信号）</b>\n币种：{symbol}\n次数：{len(flow_cache[symbol])}\n总金额：${total:,.0f}")
                flow_cache[symbol].clear()

        time.sleep(CHECK_INTERVAL)

# ==================== 主程序 ====================
if __name__=="__main__":
    tg("🚀 启动（Arkham Selenium生产版 + 新币提醒 + 连续大单 + 实时价格参考）")
    threading.Thread(target=monitor,daemon=True).start()

    while True:
        time.sleep(1)
