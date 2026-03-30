import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv()

# =========================
# 基础配置
# =========================
BEIJING_TZ = timezone(timedelta(hours=8))

UPBIT_REST_BASE = "https://api.upbit.com"
UPBIT_WS_URL = "wss://api.upbit.com/websocket/v1"

TELEGRAM_BASE = "https://api.telegram.org"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# 只监控你截图里的币种（全部按 KRW 市场）
DEFAULT_MARKETS = [
    "KRW-SENT",
    "KRW-SOL",
    "KRW-CFG",
    "KRW-DOGE",
    "KRW-DOOD",
    "KRW-ANKR",
    "KRW-BARD",
    "KRW-TAO",
    "KRW-ELSA",
    "KRW-ADA",
    "KRW-IP",
    "KRW-KITE",
    "KRW-ONG",
    "KRW-VANA",
    "KRW-KAT",
    "KRW-WLD",
    "KRW-SUI",
    "KRW-LA",
    "KRW-STEEM",
    "KRW-VIRTUAL",
    "KRW-GAS",
    "KRW-MOODENG",
    "KRW-XLM",
    "KRW-SAHARA",
    "KRW-CHZ",
    "KRW-TRUMP",
    "KRW-ANIME",
    "KRW-SHIB",
    "KRW-SEI",
    "KRW-SIGN",
    "KRW-SONIC",
    "KRW-TRX",
    "KRW-SKR",
    "KRW-BCH",
    "KRW-PENGU",
    "KRW-KERNEL",
    "KRW-ORDER",
    "KRW-ENSO",
    "KRW-ATH",
    "KRW-KNC",
    "KRW-ZBT",
    "KRW-LINK",
    "KRW-AKT",
    "KRW-CPOOL",
]

# 可在 .env 里覆盖，格式示例：
# UPBIT_MARKETS=KRW-BTC,KRW-ETH,KRW-XRP
UPBIT_MARKETS_RAW = os.getenv("UPBIT_MARKETS", "").strip()

# 大单阈值：按 KRW 计
# 例如 30000000 = 3000万韩元
BIG_TRADE_KRW = float(os.getenv("BIG_TRADE_KRW", "30000000"))

# 同一币同一方向冷却时间，避免狂刷
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "20"))

# 心跳/状态日志
STATUS_LOG_SECONDS = int(os.getenv("STATUS_LOG_SECONDS", "60"))


def now_bj() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


def parse_markets(raw: str) -> List[str]:
    if not raw:
        return DEFAULT_MARKETS[:]

    out = []
    for x in raw.split(","):
        s = x.strip().upper()
        if not s:
            continue
        if "-" not in s:
            s = f"KRW-{s}"
        out.append(s)
    return out


def market_to_symbol(market: str) -> str:
    # KRW-BTC -> BTCKRW
    base = market.split("-", 1)[1]
    return f"{base}KRW"


def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default


class Bot:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

        self.configured_markets = parse_markets(UPBIT_MARKETS_RAW)
        self.live_markets = set()
        self.active_markets: List[str] = []

        self.last_alert_ts: Dict[str, float] = {}
        self.last_status_log_ts = 0.0
        self.trade_count = 0
        self.big_trade_count = 0

    async def send_telegram(self, text: str):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            raise ValueError("TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 未设置")

        url = f"{TELEGRAM_BASE}/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }

        async with self.session.post(url, json=payload) as r:
            body = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"Telegram send failed: {r.status} {body[:500]}")

    async def load_upbit_markets(self):
        url = f"{UPBIT_REST_BASE}/v1/market/all"
        async with self.session.get(url, params={"isDetails": "false"}) as r:
            body = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"Upbit markets failed: {r.status} {body[:500]}")
            data = await r.json()

        live = set()
        for item in data:
            market = item.get("market")
            if market:
                live.add(market.upper())

        self.live_markets = live

        active = []
        missing = []
        for market in self.configured_markets:
            if market in self.live_markets:
                active.append(market)
            else:
                missing.append(market)

        self.active_markets = active

        if missing:
            logging.warning("Upbit 当前不存在或不可交易的市场: %s", ", ".join(missing))

        if not self.active_markets:
            raise RuntimeError("没有可用的 Upbit KRW 市场，请检查币种列表")

    def should_alert(self, market: str, side: str) -> bool:
        key = f"{market}:{side}"
        now_ts = time.time()
        last_ts = self.last_alert_ts.get(key, 0.0)
        if now_ts - last_ts < ALERT_COOLDOWN_SECONDS:
            return False
        self.last_alert_ts[key] = now_ts
        return True

    async def alert_big_trade(
        self,
        market: str,
        side: str,
        price: float,
        volume: float,
        value_krw: float,
        trade_ts_ms: int,
        trade_id: str,
    ):
        if not self.should_alert(market, side):
            return

        direction_text = "买入大单" if side == "buy" else "卖出大单"
        emoji = "🟢" if side == "buy" else "🔴"

        dt_text = datetime.fromtimestamp(trade_ts_ms / 1000, tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            f"{emoji} Upbit {direction_text}",
            f"时间: {dt_text} 北京时间",
            f"市场: {market}",
            f"币种: {market_to_symbol(market)}",
            f"方向: {'BUY' if side == 'buy' else 'SELL'}",
            f"成交价: {price:,.8f} KRW",
            f"成交量: {volume:,.8f}",
            f"成交额: {value_krw:,.0f} KRW",
            f"大单阈值: {BIG_TRADE_KRW:,.0f} KRW",
            f"Trade ID: {trade_id}",
        ]

        await self.send_telegram("\n".join(lines))
        logging.info("SEND | %s | %s | %.0f KRW", market, side, value_krw)

    async def handle_upbit_trade(self, data: dict):
        # 兼容 DEFAULT / SIMPLE 格式字段
        market = (data.get("code") or data.get("cd") or "").upper()
        if not market or market not in self.active_markets:
            return

        side_raw = (data.get("ask_bid") or data.get("ab") or "").upper()
        side = "buy" if side_raw == "BID" else "sell" if side_raw == "ASK" else ""
        if not side:
            return

        price = safe_float(data.get("trade_price", data.get("tp")))
        volume = safe_float(data.get("trade_volume", data.get("tv")))
        trade_ts_ms = safe_int(data.get("trade_timestamp", data.get("ttms", time.time() * 1000)))
        trade_id = str(data.get("sequential_id", data.get("sid", trade_ts_ms)))

        if price <= 0 or volume <= 0:
            return

        self.trade_count += 1
        value_krw = price * volume

        if value_krw >= BIG_TRADE_KRW:
            self.big_trade_count += 1
            await self.alert_big_trade(
                market=market,
                side=side,
                price=price,
                volume=volume,
                value_krw=value_krw,
                trade_ts_ms=trade_ts_ms,
                trade_id=trade_id,
            )

        now_ts = time.time()
        if now_ts - self.last_status_log_ts >= STATUS_LOG_SECONDS:
            self.last_status_log_ts = now_ts
            logging.info(
                "状态 | active_markets=%s | total_trades=%s | big_trades=%s | threshold=%.0f KRW",
                len(self.active_markets),
                self.trade_count,
                self.big_trade_count,
                BIG_TRADE_KRW,
            )

    async def upbit_ws_loop(self):
        payload = [
            {"ticket": str(uuid.uuid4())},
            {"type": "trade", "codes": self.active_markets},
        ]

        while True:
            try:
                async with self.session.ws_connect(
                    UPBIT_WS_URL,
                    heartbeat=30,
                    autoclose=True,
                    autoping=True,
                    receive_timeout=90,
                ) as ws:
                    await ws.send_json(payload)
                    logging.info("Upbit WS connected | markets=%s", len(self.active_markets))

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            data = json.loads(msg.data.decode("utf-8"))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        else:
                            continue

                        try:
                            msg_type = data.get("type") or data.get("ty")
                            if msg_type == "trade":
                                await self.handle_upbit_trade(data)
                        except Exception:
                            logging.exception("handle_upbit_trade error")

            except Exception as e:
                logging.exception("Upbit WS error: %s", e)

            await asyncio.sleep(3)

    async def run(self):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            raise ValueError("TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 未设置")

        timeout = aiohttp.ClientTimeout(
            total=20,
            connect=10,
            sock_connect=10,
            sock_read=20,
        )
        headers = {
            "User-Agent": "upbit-krw-big-trade-bot/1.0",
            "Accept": "application/json",
        }

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            self.session = session

            await self.load_upbit_markets()

            logging.info("Starting Upbit KRW Big Trade Bot")
            logging.info("Configured markets: %s", self.configured_markets)
            logging.info("Active markets: %s", self.active_markets)
            logging.info("BIG_TRADE_KRW=%.0f", BIG_TRADE_KRW)

            await self.send_telegram(
                f"✅ Upbit 大单监控机器人启动成功\n"
                f"时间: {now_bj()} 北京时间\n"
                f"模式: 仅 Upbit / 仅 KRW 市场 / 仅买卖大单\n"
                f"大单阈值: {BIG_TRADE_KRW:,.0f} KRW\n"
                f"监控数量: {len(self.active_markets)}\n"
                f"监控列表: {', '.join(self.active_markets)}"
            )

            await self.upbit_ws_loop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    asyncio.run(Bot().run())
