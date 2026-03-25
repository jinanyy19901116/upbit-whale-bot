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

# ------------------- 汇率 -------------------
def get_krw_usd():
    try:
        r = requests.get(
            "https://api.exchangerate.host/latest?base=KRW&symbols=USD",
            timeout=5
        )
        return r.json()["rates"]["USD"]
    except:
        return 0.00075

KRW_TO_USD = get_krw_usd()
last_rate_update = time.time()

def to_usd(krw):
    return krw * KRW_TO_USD

def format_usd(x):
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    elif x >= 1_000:
        return f"${x/1_000:.1f}K"
    else:
        return f"${x:.0f}"

# ------------------- 时间 -------------------
def bj_time(ts):
    utc = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    bj = utc.astimezone(timezone(timedelta(hours=8)))
    return bj.strftime("%H:%M:%S")

# ------------------- Telegram -------------------
def tg(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
    except:
        pass

tg("✅ 监控系统启动（多行格式版）")

# ------------------- 获取Top30 -------------------
def get_top30():
    try:
        all_m = requests.get("https://api.upbit.com/v1/market/all", timeout=5).json()
        krw = [
            m["market"] for m in all_m
            if m["market"].startswith("KRW-")
            and m["market"] not in ["KRW-BTC", "KRW-ETH"]
        ]

        result = []
        for m in krw:
            try:
                data = requests.get(
                    f"https://api.upbit.com/v1/ticker?markets={m}",
                    timeout=5
                ).json()
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
logging.info(f"监控币种: {len(MARKETS)}")

last_ts = {m: 0 for m in MARKETS}

burst_data = defaultdict(lambda: deque())
acc_data = defaultdict(lambda: deque())
price_cache = defaultdict(lambda: deque())

signal_state = defaultdict(lambda: {"acc": False, "burst": False})

# ------------------- 主循环 -------------------
while True:
    try:
        # 汇率更新
        if time.time() - last_rate_update > 600:
            KRW_TO_USD = get_krw_usd()
            last_rate_update = time.time()
            logging.info(f"汇率更新: {KRW_TO_USD}")

        for market in MARKETS:
            try:
                r = requests.get(
                    f"https://api.upbit.com/v1/trades/ticks?market={market}&count=10",
                    timeout=5
                )

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
                    usd = to_usd(amount)

                    now = time.time()
                    time_str = bj_time(ts)

                    # ------------------- 大单（多行格式）-------------------
                    if amount >= BIG_TRADE:
                        side_str = "🟢买单" if side == "BID" else "🔴卖单"

                        msg = (
                            f"{market}\n"
                            f"{side_str}\n"
                            f"💰 {format_usd(usd)}\n"
                            f"📍 {price}\n"
                            f"⏰ {time_str}"
                        )

                        logging.info(msg)
                        tg(msg)

                    # ------------------- 拉盘 -------------------
                    if side == "BID":
                        burst_data[market].append((now, amount))

                        while burst_data[market] and now - burst_data[market][0][0] > TIME_WINDOW:
                            burst_data[market].popleft()

                        total = sum(x[1] for x in burst_data[market])

                        if total >= BURST:
                            signal_state[market]["burst"] = True
                            tg(f"🚀拉盘 {market} {format_usd(to_usd(total))}")
                            burst_data[market].clear()

                    # ------------------- 吸筹 -------------------
                    if side == "BID":
                        acc_data[market].append((now, amount))

                        while acc_data[market] and now - acc_data[market][0][0] > ACC_WINDOW:
                            acc_data[market].popleft()

                        acc_total = sum(x[1] for x in acc_data[market])

                        if acc_total >= ACCUMULATION:
                            signal_state[market]["acc"] = True
                            tg(f"🟢吸筹 {market} {format_usd(to_usd(acc_total))}")
                            acc_data[market].clear()

                    # ------------------- 价格联动 -------------------
                    price_cache[market].append((now, price))

                    while price_cache[market] and now - price_cache[market][0][0] > 5:
                        price_cache[market].popleft()

                    if len(price_cache[market]) >= 2:
                        old_price = price_cache[market][0][1]
                        change = (price - old_price) / old_price

                        # 买入信号
                        if change > 0.01:
                            tg(f"⚡上涨 {market} +{change*100:.2f}%")

                            if signal_state[market]["acc"] and signal_state[market]["burst"]:
                                tg(f"🟢【买入信号】{market} 🚀主力启动")
                                signal_state[market] = {"acc": False, "burst": False}

                        # 卖出信号
                        elif change < -0.01:
                            tg(f"⚡下跌 {market} {change*100:.2f}%")

                            if signal_state[market]["acc"] or signal_state[market]["burst"]:
                                tg(f"🔴【卖出信号】{market} ⚠️主力可能出货")
                                signal_state[market] = {"acc": False, "burst": False}

                    last_ts[market] = ts

                time.sleep(REQUEST_INTERVAL)

            except Exception as e:
                logging.error(f"{market}错误: {e}")

        time.sleep(LOOP_INTERVAL)

    except Exception as e:
        logging.error(f"主循环错误: {e}")
        time.sleep(2)
