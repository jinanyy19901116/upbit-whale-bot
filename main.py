import time
import requests
from collections import defaultdict
import threading

# ==================== 配置 ====================
TELEGRAM_TOKEN = "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
TELEGRAM_CHAT_ID = 5671949305
ARKHAM_API_KEY = "19d2a233-8eaa-49be-bb02-403a8e636f9b"

# ==================== 参数 ====================
BIG_MONEY = 200_000  # 20万美元大单
CHECK_INTERVAL = 20   # Arkham接口轮询间隔（秒）
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
    except Exception as e:
        print("Telegram推送失败:", e)

# ==================== 缓存 ====================
flow_cache = defaultdict(list)

# ==================== Arkham生产监控 ====================
def arkham_monitor():
    print("🇰🇷 Arkham稳定生产版启动")

    while True:
        try:
            now = int(time.time())
            start = now - CHECK_INTERVAL  # 查询最近 CHECK_INTERVAL 秒
            end = now

            url = f"https://api.arkhamintelligence.com/transfers?startTime={start}&endTime={end}"
            headers = {"Authorization": f"Bearer {ARKHAM_API_KEY}"}

            try:
                r = requests.get(url, headers=headers, timeout=10)
            except Exception as e:
                print("Arkham请求异常:", e)
                time.sleep(10)
                continue

            if r.status_code != 200:
                print("Arkham接口异常:", r.status_code, r.text)
                time.sleep(10)
                continue

            try:
                res = r.json()
            except Exception:
                print("Arkham返回非JSON:", r.text)
                time.sleep(10)
                continue

            if not isinstance(res, dict):
                print("Arkham返回非字典:", res)
                time.sleep(10)
                continue

            transfers = res.get("transfers", [])
            if not isinstance(transfers, list):
                print("transfers格式异常:", transfers)
                time.sleep(10)
                continue

            now_time = time.time()

            for tx in transfers:
                if not isinstance(tx, dict):
                    continue

                usd = float(tx.get("usdValue", 0) or 0)
                symbol = str(tx.get("tokenSymbol", "") or "").upper()
                to_entity = str(tx.get("toEntityName", "") or "").lower()

                # 过滤条件
                if symbol in EXCLUDE_COINS:
                    continue
                if usd < BIG_MONEY:
                    continue
                if not any(ex in to_entity for ex in KOREA_EXCHANGES):
                    continue

                # 记录资金流
                flow_cache[symbol].append({
                    "time": now_time,
                    "amount": usd
                })

                # 保留最近5分钟
                flow_cache[symbol] = [
                    x for x in flow_cache[symbol] if now_time - x["time"] < 300
                ]

                # ✅ 单笔大额提示
                tg(f"""
🇰🇷 <b>韩国主力资金买入</b>
币种：{symbol}
金额：${usd:,.0f}
交易所：{to_entity}
""")

                # ✅ 连续资金流提示
                if len(flow_cache[symbol]) >= 3:
                    total = sum(x["amount"] for x in flow_cache[symbol])
                    tg(f"""
🔥 <b>连续大额买入警告</b>
币种：{symbol}
次数：{len(flow_cache[symbol])}
总金额：${total:,.0f}
⚡ 高概率市场关注
""")
                    flow_cache[symbol].clear()

        except Exception as e:
            print("Arkham主循环异常:", e)

        time.sleep(CHECK_INTERVAL)

# ==================== 主程序 ====================
if __name__ == "__main__":
    tg("🚀 启动（Arkham稳定生产版 + 时间戳修复）")
    threading.Thread(target=arkham_monitor, daemon=True).start()

    while True:
        time.sleep(1)
