import requests
import time
import logging
from collections import defaultdict, deque

# ------------------- Telegram -------------------
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

# ------------------- 参数 -------------------
BIG_TRADE_THRESHOLD = 27_000_000   # 单笔大单
BURST_THRESHOLD = 80_000_000       # 3秒累计（拉盘信号）
TIME_WINDOW = 3                    # 秒

REQUEST_INTERVAL = 0.15
LOOP_INTERVAL = 1

# ------------------- 日志 -------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

logging.info("程序启动")

# ------------------- Telegram -------------------
def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=10)
    except Exception as e:
        logging.error(f"TG错误: {e}")

send_telegram("✅ 拉盘监控系统已启动")

# ------------------- 获取市场 -------------------
def get_top30_markets():
    try:
        # 获取全部市场
        all_markets = requests.get(
            "https://api.upbit.com/v1/market/all", timeout=10
        ).json()

        krw_markets = [
            m["market"] for m in all_markets
            if m["market"].startswith("KRW-") and m["market"] not in ["KRW-BTC", "KRW-ETH"]
        ]

        result = []

        # 获取成交额
        for m in krw_markets:
            try:
                url = f"https://api.upbit.com/v1/ticker?markets={m}"
                r = requests.get(url, timeout=5)

                if r.status_code != 200:
                    continue

                data = r.json()
                if not isinstance(data, list):
                    continue

                volume = data[0].get("acc_trade_price_24h", 0)
                result.append((m, volume))

                time.sleep(0.05)

            except:
                continue

        # 排序取前30
        result.sort(key=lambda x: x[1], reverse=True)

        top30 = [x[0] for x in result[:30]]

        logging.info(f"TOP30币种加载完成: {top30}")
        return top30

    except Exception as e:
        logging.error(f"获取市场失败: {e}")
        return []

# ------------------- 初始化 -------------------
MARKETS = get_top30_markets()

last_timestamp = {m: 0 for m in MARKETS}
burst_data = defaultdict(lambda: deque())

# ------------------- 主循环 -------------------
while True:
    try:
        for market in MARKETS:
            try:
                url = f"https://api.upbit.com/v1/trades/ticks?market={market}&count=10"
                r = requests.get(url, timeout=10)

                if r.status_code != 200:
                    continue

                try:
                    ticks = r.json()
                except:
                    continue

                if not isinstance(ticks, list):
                    continue

                for tick in reversed(ticks):

                    if not isinstance(tick, dict):
                        continue

                    ts = tick.get("timestamp")
                    if not ts or ts <= last_timestamp[market]:
                        continue

                    price = tick.get("trade_price", 0)
                    volume = tick.get("trade_volume", 0)
                    ask_bid = tick.get("ask_bid", "")

                    if price == 0 or volume == 0:
                        continue

                    amount = price * volume

                    # 过滤机器人
                    if volume < 0.01:
                        continue

                    now = time.time()

                    # ------------------- 大单 -------------------
                    if amount >= BIG_TRADE_THRESHOLD:
                        msg = (
                            f"💰 大单\n"
                            f"{market}\n"
                            f"{price:,} KRW\n"
                            f"{amount:,.0f}"
                        )
                        logging.info(msg)
                        send_telegram(msg)

                    # ------------------- 拉盘检测 -------------------
                    if ask_bid == "BID":  # 主动买入
                        burst_data[market].append((now, amount))

                        # 清理旧数据
                        while burst_data[market] and now - burst_data[market][0][0] > TIME_WINDOW:
                            burst_data[market].popleft()

                        total = sum(x[1] for x in burst_data[market])

                        if total >= BURST_THRESHOLD:
                            msg = (
                                f"🚀 拉盘预警\n"
                                f"{market}\n"
                                f"{TIME_WINDOW}秒买入: {total:,.0f}"
                            )
                            logging.warning(msg)
                            send_telegram(msg)

                            burst_data[market].clear()

                    last_timestamp[market] = ts

                time.sleep(REQUEST_INTERVAL)

            except Exception as e:
                logging.error(f"{market}异常: {e}")
                time.sleep(0.5)

        # 每5分钟刷新一次币种（防止新币）
        if int(time.time()) % 300 == 0:
            MARKETS = get_top30_markets()

        time.sleep(LOOP_INTERVAL)

    except KeyboardInterrupt:
        logging.info("程序停止")
        break
    except Exception as e:
        logging.error(f"主循环异常: {e}")
        time.sleep(2)
