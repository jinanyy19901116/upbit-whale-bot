import logging
import time
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

import requests

# =========================
# Telegram 配置
# =========================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

# =========================
# 参数配置
# =========================
# 先把阈值调低，保证系统能稳定出信号
MIN_USD = 20_000        # 最低提醒
BIG_USD = 50_000       # 大单
ACCUM_USD = 100_000    # 60秒累计吸筹

# 汇率兜底（1 USD = 多少 KRW）
DEFAULT_USDKRW = 1350.0

# 精确屏蔽
BLACKLIST_MARKETS = {"KRW-BTC", "KRW-ETH", "KRW-USDT"}

# 轮询频率
POLL_INTERVAL = 5          # 每轮轮询间隔（秒）
TOP_MARKETS_REFRESH = 600  # Top30刷新间隔（秒）
NEW_LISTING_REFRESH = 60   # 新币检测间隔（秒）
FX_REFRESH = 600           # 汇率刷新间隔（秒）

# 请求超时
HTTP_TIMEOUT = 8

# 去重容量
SEEN_ID_MAXLEN = 20000

# =========================
# 全局状态
# =========================
session = requests.Session()

markets = []
known_markets = set()

# 1 USD = ? KRW
usdkrw_rate = DEFAULT_USDKRW
last_fx_update_ts = 0.0

# 吸筹累计
accum_data = {}

# 去重
seen_ids = set()
seen_queue = deque()

# =========================
# 日志
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =========================
# 工具函数
# =========================
def safe_get_json(url, params=None, timeout=HTTP_TIMEOUT):
    try:
        r = session.get(url, params=params, timeout=timeout)
        if r.status_code != 200:
            logging.warning(f"HTTP异常 {r.status_code}: {url}")
            return None
        return r.json()
    except Exception as e:
        logging.warning(f"请求失败: {url}, 错误: {e}")
        return None


def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram 未配置")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg
    }
    try:
        session.post(url, data=payload, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logging.warning(f"Telegram发送失败: {e}")


def now_bj() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(
        timezone(timedelta(hours=8))
    ).strftime("%H:%M:%S")


def format_usd(x: float) -> str:
    if x >= 1_000_000:
        return f"${x / 1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x / 1_000:.1f}K"
    return f"${x:.0f}"


def format_price(x: float) -> str:
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.8f}"


def krw_to_usd(price_krw: float) -> float:
    if usdkrw_rate <= 0:
        return price_krw / DEFAULT_USDKRW
    return price_krw / usdkrw_rate


def mark_seen(unique_id: str):
    if unique_id in seen_ids:
        return False

    seen_ids.add(unique_id)
    seen_queue.append(unique_id)

    while len(seen_queue) > SEEN_ID_MAXLEN:
        old = seen_queue.popleft()
        seen_ids.discard(old)

    return True


# =========================
# 汇率更新
# =========================
def update_fx_rate(force=False):
    global usdkrw_rate, last_fx_update_ts

    now = time.time()
    if not force and (now - last_fx_update_ts < FX_REFRESH):
        return

    # 尽量少请求，失败就沿用旧值
    data = safe_get_json(
        "https://api.exchangerate.host/latest",
        params={"base": "USD", "symbols": "KRW"}
    )

    if isinstance(data, dict):
        rates = data.get("rates", {})
        krw = rates.get("KRW")
        if isinstance(krw, (int, float)) and krw > 0:
            usdkrw_rate = float(krw)
            last_fx_update_ts = now
            logging.info(f"汇率更新成功: 1 USD = {usdkrw_rate:.2f} KRW")
            return

    logging.warning(f"汇率更新失败，继续使用当前值: {usdkrw_rate:.2f}")


# =========================
# Top30市场
# =========================
def get_top_markets():
    all_markets = safe_get_json("https://api.upbit.com/v1/market/all")
    if not isinstance(all_markets, list):
        return []

    krw_markets = [
        m["market"]
        for m in all_markets
        if isinstance(m, dict)
        and m.get("market", "").startswith("KRW-")
        and m.get("market") not in BLACKLIST_MARKETS
    ]

    if not krw_markets:
        return []

    ticker = safe_get_json(
        "https://api.upbit.com/v1/ticker",
        params={"markets": ",".join(krw_markets)}
    )
    if not isinstance(ticker, list):
        return []

    valid = []
    for item in ticker:
        if not isinstance(item, dict):
            continue
        market = item.get("market")
        vol24 = item.get("acc_trade_price_24h", 0)
        if market and isinstance(vol24, (int, float)):
            valid.append((market, vol24))

    valid.sort(key=lambda x: x[1], reverse=True)
    top = [m for m, _ in valid[:30]]
    logging.info(f"监控市场: {top}")
    return top


# =========================
# 新币检测
# =========================
def check_new_listing():
    global known_markets

    data = safe_get_json("https://api.upbit.com/v1/market/all")
    if not isinstance(data, list):
        return

    current = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        market = item.get("market", "")
        if market.startswith("KRW-") and market not in BLACKLIST_MARKETS:
            current.add(market)

    if not known_markets:
        known_markets = current
        return

    new_markets = current - known_markets
    for market in sorted(new_markets):
        coin = market.replace("KRW-", "")
        msg = (
            f"🆕 新币上线\n"
            f"{coin}/USDT\n"
            f"⚠️ 注意首波波动\n"
            f"⏰ {now_bj()}"
        )
        send_telegram(msg)

    known_markets = current


# =========================
# 信号处理
# =========================
def handle_trade(market: str, trade: dict):
    if not isinstance(trade, dict):
        return

    seq_id = trade.get("sequential_id")
    if seq_id is None:
        # 如果没有 sequential_id，就退化成时间+价格+量 去重
        seq_id = f"{market}-{trade.get('timestamp')}-{trade.get('trade_price')}-{trade.get('trade_volume')}"

    if not mark_seen(str(seq_id)):
        return

    coin = market.replace("KRW-", "")

    price_krw = trade.get("trade_price", 0)
    volume = trade.get("trade_volume", 0)
    side_raw = trade.get("ask_bid", "BID")
    timestamp_ms = trade.get("timestamp")

    if not isinstance(price_krw, (int, float)) or price_krw <= 0:
        return
    if not isinstance(volume, (int, float)) or volume <= 0:
        return

    price_usd = krw_to_usd(price_krw)
    usd_total = price_usd * volume

    # 调试日志，确认系统真的在流动
    logging.info(f"{coin} 成交 USD: {usd_total:.2f}")

    if usd_total < MIN_USD:
        return

    # 60秒内累计吸筹
    now_ts = time.time()
    acc = accum_data.get(coin, {"usd": 0.0, "time": now_ts})

    if now_ts - acc["time"] <= 60:
        acc["usd"] += usd_total
    else:
        acc = {"usd": usd_total, "time": now_ts}

    accum_data[coin] = acc

    score = 0
    if usd_total >= BIG_USD:
        score += 2
    if acc["usd"] >= ACCUM_USD:
        score += 2

    if score < 2:
        return

    side_str = "🟢买入" if side_raw == "BID" else "🔴卖出"

    # 用成交时间，不用本机当前时间
    if isinstance(timestamp_ms, (int, float)) and timestamp_ms > 0:
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).astimezone(
            timezone(timedelta(hours=8))
        )
        time_str = dt.strftime("%H:%M:%S")
    else:
        time_str = now_bj()

    msg = (
        f"{coin}/USDT\n"
        f"{side_str}\n"
        f"💰 {format_usd(usd_total)}\n"
        f"📍 {format_price(price_usd)}\n"
        f"⏰ {time_str}"
    )
    send_telegram(msg)


# =========================
# REST轮询
# =========================
def run_rest_polling():
    logging.info("启动 REST API 轮询")

    while True:
        for market in markets:
            try:
                data = safe_get_json(
                    "https://api.upbit.com/v1/trades/ticks",
                    params={"market": market, "count": 10}
                )

                if not isinstance(data, list):
                    continue

                # 从旧到新处理
                for trade in reversed(data):
                    handle_trade(market, trade)

            except Exception as e:
                logging.warning(f"{market} 轮询失败: {e}")

        time.sleep(POLL_INTERVAL)


# =========================
# 主程序
# =========================
def main():
    global markets

    logging.info("🚀 系统启动")

    send_telegram("✅ 系统启动成功")
    update_fx_rate(force=True)

    markets = get_top_markets()
    check_new_listing()

    last_market_refresh = time.time()
    last_listing_refresh = time.time()
    last_fx_refresh = time.time()

    # 启动轮询线程
    polling_thread = threading.Thread(target=run_rest_polling, daemon=True)
    polling_thread.start()

    while True:
        now = time.time()

        if now - last_market_refresh >= TOP_MARKETS_REFRESH:
            markets = get_top_markets()
            last_market_refresh = now

        if now - last_listing_refresh >= NEW_LISTING_REFRESH:
            check_new_listing()
            last_listing_refresh = now

        if now - last_fx_refresh >= FX_REFRESH:
            update_fx_rate()
            last_fx_refresh = now

        time.sleep(5)


if __name__ == "__main__":
    main()
