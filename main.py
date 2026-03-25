import requests
import time
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

# ------------------- Telegram -------------------
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

# ------------------- 参数 -------------------
BIG_TRADE = 27_000_000
BURST = 80_000_000
ACCUMULATION = 100_000_000

TIME_WINDOW = 3
ACC_WINDOW = 10

REQUEST_INTERVAL = 0.15
LOOP_INTERVAL = 1

# ------------------- 日志 -------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

logging.info("程序启动")

# ------------------- 时间 -------------------
def bj_time(ts):
    utc = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    bj = utc.astimezone(timezone(timedelta(hours=8)))
    return bj.strftime("%H:%M:%S")

# ------------------- Telegram -------------------
def tg(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
    except:
        pass

tg("✅ 监控系统已启动（无买卖比版）")

# ------------------- 获取Top30 -------------------
def get_top30():
    try:
        all_m = requests.get("https://api.upbit.com/v1/market/all").json()
        krw = [m["market"] for m in all_m if m["market"].startswith("KRW-") and m["market"] not in ["KRW-BTC","KRW-ETH"]]

        result = []
        for m in krw:
            try:
                data = requests.get(f"https://api.upbit.com/v1/ticker?markets={m}").json()
                vol = data[0]["acc_trade_price_24h"]
                result.append((m, vol))
                time.sleep(0.05)
            except:
                continue

        result.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in result[:30]]
    except:
        return []

# ------------------- 初始化 -------------------
MARKETS = get_top30()
last_ts = {m: 0 for m in MARKETS}

burst_data = defaultdict(lambda: deque())
acc_data = defaultdict(lambda: deque())
price_cache = defaultdict(lambda: deque())

# ------------------- 主循环 -------------------
while True:
    try:
        for market in MARKETS:
            try:
                r = requests.get(f"https://api.upbit.com/v1/trades/ticks?market={market}&count=10")

                if r.status_code != 200:
                    continue

                ticks = r.json()
                if not isinstance(ticks, list):
                    continue

                for t in reversed(ticks):

                    if not isinstance(t, dict):
                        continue

                    ts = t.get("timestamp")
                    if not ts or ts <= last_ts[market]:
                        continue

                    price = t.get("trade_price", 0)
                    vol = t.get("trade_volume", 0)
                    side = t.get("ask_bid", "")

                    if price == 0 or vol == 0:
                        continue

                    amount = price * vol
                    now = time.time()
                    time_str = bj_time(ts)

                    # ------------------- 大单 -------------------
                    if amount >= BIG_TRADE:
                        msg = f"💰大单 {market} {time_str} {amount:,.0f}"
                        logging.info(msg)
                        tg(msg)

                    # ------------------- 拉盘 -------------------
                    if side == "BID":
                        burst_data[market].append((now, amount))

                        while burst_data[market] and now - burst_data[market][0][0] > TIME_WINDOW:
                            burst_data[market].popleft()

                        total = sum(x[1] for x in burst_data[market])

                        if total >= BURST:
                            msg = f"🚀拉盘 {market} {time_str} {total:,.0f}"
                            logging.warning(msg)
                            tg(msg)
                            burst_data[market].clear()

                    # ------------------- 吸筹 -------------------
                    if side == "BID":
                        acc_data[market].append((now, amount))

                        while acc_data[market] and now - acc_data[market][0][0] > ACC_WINDOW:
                            acc_data[market].popleft()

                        acc_total = sum(x[1] for x in acc_data[market])

                        if acc_total >= ACCUMULATION:
                            msg = f"🟢吸筹 {market} {time_str} {acc_total:,.0f}"
                            logging.info(msg)
                            tg(msg)
                            acc_data[market].clear()

                    # ------------------- 价格联动 -------------------
                    price_cache[market].append((now, price))

                    while price_cache[market] and now - price_cache[market][0][0] > 5:
                        price_cache[market].popleft()

                    if len(price_cache[market]) >= 2:
                        old_price = price_cache[market][0][1]
                        change = (price - old_price) / old_price

                        if change > 0.01:
                            tg(f"⚡上涨 {market} +{change*100:.2f}%")
                        elif change < -0.01:
                            tg(f"⚡下跌 {market} {change*100:.2f}%")

                    last_ts[market] = ts

                time.sleep(REQUEST_INTERVAL)

            except Exception as e:
                logging.error(f"{market}错误 {e}")

        time.sleep(LOOP_INTERVAL)

    except Exception as e:
        logging.error(f"主循环错误 {e}")
        time.sleep(2)
