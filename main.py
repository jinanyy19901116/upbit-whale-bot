import time
import requests
from collections import defaultdict

# ==================== 配置 ====================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = 5671949305
ARKHAM_API_KEY = "19d2a233-8eaa-49be-bb02-403a8e636f9b"

# ==================== 参数 ====================
BIG_MONEY = 200_000  # 20万美元
CHECK_INTERVAL = 20

EXCLUDE_COINS = ["BTC", "ETH", "XRP", "SOL", "ADA", "DOGE"]

KOREA_EXCHANGES = ["upbit", "bithumb", "coinone", "korbit"]

# ==================== Telegram ====================
def tg(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except:
        pass

# ==================== 缓存 ====================
flow_cache = defaultdict(list)

# ==================== Arkham监控 ====================
def arkham_monitor():
    print("🇰🇷 韩国资金监控启动")

    while True:
        try:
            url = "https://api.arkhamintelligence.com/transfers"
            headers = {"Authorization": f"Bearer {ARKHAM_API_KEY}"}

            res = requests.get(url, headers=headers, timeout=10).json()

            for tx in res.get("transfers", []):
                usd = float(tx.get("usdValue", 0))
                symbol = tx.get("tokenSymbol", "")

                to_entity = str(tx.get("toEntityName", "")).lower()
                from_entity = str(tx.get("fromEntityName", "")).lower()

                # ==================== 过滤 ====================
                if symbol in EXCLUDE_COINS:
                    continue

                if usd < BIG_MONEY:
                    continue

                # ==================== 🇰🇷 韩国交易所识别 ====================
                is_korea = any(ex in to_entity for ex in KOREA_EXCHANGES)

                if not is_korea:
                    continue

                now = time.time()

                flow_cache[symbol].append({
                    "time": now,
                    "amount": usd
                })

                # 只保留最近5分钟
                flow_cache[symbol] = [
                    x for x in flow_cache[symbol] if now - x["time"] < 300
                ]

                # ==================== 单次大额 ====================
                tg(f"""
🇰🇷 <b>韩国资金买入</b>
币种：{symbol}
金额：${usd:,.0f}
交易所：{to_entity}
""")

                # ==================== 连续资金流入 ====================
                if len(flow_cache[symbol]) >= 3:
                    total = sum(x["amount"] for x in flow_cache[symbol])

                    tg(f"""
🔥 <b>韩国连续买入（重点）</b>
币种：{symbol}

次数：{len(flow_cache[symbol])}
总金额：${total:,.0f}

⚡ 高概率市场关注上升
""")

                    flow_cache[symbol].clear()

        except Exception as e:
            print("Arkham错误:", e)

        time.sleep(CHECK_INTERVAL)

# ==================== 主程序 ====================
if __name__ == "__main__":
    tg("🚀 启动（纯韩国资金监控版）")

    arkham_monitor()
