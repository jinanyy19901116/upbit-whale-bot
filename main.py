import requests
import time
import datetime
import logging

# ================== 配置 ==================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = "5671949305"

MIN_USD = 200000
NEW_COIN_USD = 100000

EXCLUDE = ["BTC", "ETH", "USDT"]

# ================== 汇率 ==================
krw_to_usdt_rate = 0.00075  # 初始备用

def update_fx_rate():
    global krw_to_usdt_rate
    try:
        btc_usdt = float(requests.get(
            "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
            timeout=3
        ).json()["price"])

        btc_krw = float(requests.get(
            "https://api.upbit.com/v1/ticker",
            params={"markets": "KRW-BTC"},
            timeout=3
        ).json()[0]["trade_price"])

        krw_to_usdt_rate = btc_usdt / btc_krw

    except Exception as e:
        logging.error(f"汇率更新失败: {e}")

def price_to_usdt(price_krw):
    return price_krw * krw_to_usdt_rate

# ================== 状态 ==================
known_markets = set()
new_coin_watchlist = {}
trade_history = {}
seen = set()
buy_flow = {}
last_price = {}

# ================== 日志 ==================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ================== 工具 ==================
def send_telegram(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10
        )
    except:
        pass

def format_usd(x):
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    elif x >= 1_000:
        return f"${x/1_000:.0f}K"
    return f"${x:.0f}"

def format_price(x):
    if x >= 1:
        return f"{x:.4f}"
    elif x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.8f}"

# ================== 机器人过滤 ==================
def is_bot_trade(symbol, usd, side):
    if symbol not in trade_history:
        trade_history[symbol] = []

    trade_history[symbol].append((usd, side, time.time()))
    trade_history[symbol] = trade_history[symbol][-10:]

    amounts = [round(x[0], -3) for x in trade_history[symbol]]

    return amounts.count(amounts[-1]) >= 3

def is_arbitrage(symbol):
    if symbol not in trade_history:
        return False

    sides = [x[1] for x in trade_history[symbol]]
    flips = sum(1 for i in range(len(sides)-1) if sides[i] != sides[i+1])

    return flips >= 4

def is_fake_pump(absorb, pump, confirm):
    return absorb and pump and not confirm

# ================== 新币 ==================
def check_new_listings():
    global known_markets, new_coin_watchlist

    try:
        res = requests.get("https://api.upbit.com/v1/market/all", timeout=5).json()
        current = set([m["market"] for m in res if m["market"].startswith("KRW-")])

        if not known_markets:
            known_markets = current
            return

        new = current - known_markets

        for m in new:
            symbol = m.replace("KRW-", "")
            if any(x in symbol for x in EXCLUDE):
                continue

            new_coin_watchlist[symbol] = time.time()

            send_telegram(f"🆕 新币上线\n{symbol}/USDT")

        known_markets = current

    except Exception as e:
        logging.error(f"新币检测失败: {e}")

# ================== 市场 ==================
def get_top_markets():
    markets = requests.get("https://api.upbit.com/v1/market/all").json()

    krw = [
        m["market"] for m in markets
        if m["market"].startswith("KRW-")
        and not any(x in m["market"] for x in EXCLUDE)
    ]

    tickers = requests.get(
        "https://api.upbit.com/v1/ticker",
        params={"markets": ",".join(krw)}
    ).json()

    sorted_m = sorted(tickers, key=lambda x: x["acc_trade_price_24h"], reverse=True)

    return [m["market"] for m in sorted_m[:30]]

def get_trades(market):
    try:
        return requests.get(
            "https://api.upbit.com/v1/trades/ticks",
            params={"market": market, "count": 10},
            timeout=5
        ).json()
    except:
        return []

def get_binance_price(symbol):
    try:
        return float(requests.get(
            f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT",
            timeout=3
        ).json()["price"])
    except:
        return None

# ================== 主逻辑 ==================
def run():
    logging.info("系统启动")
    send_telegram("🚀 系统启动成功")

    markets = get_top_markets()

    last_fx_update = 0

    while True:
        try:
            # ===== 汇率更新（30秒）=====
            if time.time() - last_fx_update > 30:
                update_fx_rate()
                last_fx_update = time.time()

            check_new_listings()

            for market in markets:
                trades = get_trades(market)

                for t in trades:
                    try:
                        tid = t["sequential_id"]
                        if tid in seen:
                            continue
                        seen.add(tid)

                        price = t["trade_price"]
                        volume = t["trade_volume"]
                        side = t["ask_bid"]

                        symbol = market.replace("KRW-", "")
                        usd = price * volume * krw_to_usdt_rate

                        threshold = MIN_USD
                        if symbol in new_coin_watchlist:
                            if time.time() - new_coin_watchlist[symbol] < 1800:
                                threshold = NEW_COIN_USD

                        if usd < threshold:
                            continue

                        if is_bot_trade(symbol, usd, side):
                            continue

                        if is_arbitrage(symbol):
                            continue

                        # ===== 核心：价格校准 =====
                        binance_price = get_binance_price(symbol)

                        if binance_price:
                            price_usdt = binance_price
                        else:
                            price_usdt = price_to_usdt(price)

                        side_str = "🟢买单" if side == "BID" else "🔴卖单"

                        ts = t["timestamp"] / 1000
                        dt = datetime.datetime.utcfromtimestamp(ts) + datetime.timedelta(hours=8)
                        time_str = dt.strftime("%H:%M:%S")

                        # ===== 吸筹 =====
                        buy_flow[symbol] = buy_flow.get(symbol, 0)
                        if side == "BID":
                            buy_flow[symbol] += 1
                        else:
                            buy_flow[symbol] = 0

                        absorb = buy_flow[symbol] >= 3

                        # ===== 拉盘 =====
                        pump = False
                        if symbol in last_price:
                            if (price - last_price[symbol]) / last_price[symbol] > 0.01:
                                pump = True

                        last_price[symbol] = price

                        confirm = binance_price is not None

                        if is_fake_pump(absorb, pump, confirm):
                            continue

                        score = 0
                        if usd > 300000: score += 20
                        elif usd > 200000: score += 10
                        if absorb: score += 25
                        if pump: score += 25
                        if confirm: score += 20

                        level = "🔴极强" if score >= 90 else "🟠强" if score >= 75 else "🟡关注"

                        buy_signal = "🟢买入信号" if absorb and pump and confirm and side == "BID" else ""

                        msg = (
                            f"{symbol}/USDT\n"
                            f"{level} {buy_signal}\n"
                            f"{side_str}\n"
                            f"💰 {format_usd(usd)}\n"
                            f"📍 {format_price(price_usdt)}\n"
                            f"⏰ {time_str}"
                        )

                        send_telegram(msg)

                    except Exception as e:
                        logging.error(f"{market}解析失败: {e}")

                time.sleep(0.2)

        except Exception as e:
            logging.error(f"主循环错误: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run()
