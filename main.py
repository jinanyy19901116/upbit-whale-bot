# main.py
import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Tuple

import aiohttp
import websockets

# =========================
# ENV
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise RuntimeError("Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID in environment variables")

# =========================
# USER CONFIG
# =========================
RAW_SYMBOLS = [
    "signusdt",
    "kiteusdt",
    "hypeusdt",
    "sirenusdt",
    "phausdt",
    "powerusdt",
    "skyaiusdt",
    "bardusdt",
    "qusdt",
    "uaiusdt",
    "husdt",
    "icxusdt",
    "robousdt",
    "ognusdt",
    "xaiusdt",
    "ipusdt",
    "xagusdt",
    "gusdt",
    "ankrusdt",
    "animeusdt",
    "banusdt",
    "gunusdt",
    "zrousdt",
    "cusdt",
    "lightusdt",
    "cvcusdt",
    "avausdt",
]

# 阈值可在 Railway 环境变量里覆盖
SINGLE_TRADE_USDT = float(os.getenv("SINGLE_TRADE_USDT", "100000"))
SWEEP_WINDOW_SEC = float(os.getenv("SWEEP_WINDOW_SEC", "5"))
SWEEP_TOTAL_USDT = float(os.getenv("SWEEP_TOTAL_USDT", "300000"))
SWEEP_MIN_TRADES = int(os.getenv("SWEEP_MIN_TRADES", "3"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "20"))

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("whale-monitor")

# =========================
# NORMALIZATION HELPERS
# =========================
def normalize_user_symbols(raw: List[str]) -> List[str]:
    out = []
    for s in raw:
        s = s.strip().upper()
        if not s.endswith("USDT"):
            continue
        out.append(s)
    return out

USER_SYMBOLS = normalize_user_symbols(RAW_SYMBOLS)

def to_binance_symbol(sym: str) -> str:
    return sym.lower()

def to_bybit_symbol(sym: str) -> str:
    return sym.upper()

def to_okx_inst_id(sym: str) -> str:
    base = sym[:-4]
    return f"{base}-USDT-SWAP"

# =========================
# TELEGRAM
# =========================
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.session: aiohttp.ClientSession | None = None

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))

    async def close(self):
        if self.session:
            await self.session.close()

    async def send(self, text: str):
        if self.session is None:
            await self.start()
        try:
            async with self.session.post(
                self.url,
                data={
                    "chat_id": self.chat_id,
                    "text": text,
                    "disable_web_page_preview": "true",
                },
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error("Telegram send failed: %s %s", resp.status, body)
        except Exception as e:
            logger.exception("Telegram error: %s", e)

# =========================
# EVENT MODEL
# =========================
@dataclass
class TradeEvent:
    exchange: str
    symbol: str            # unified, e.g. SIGNUSDT
    side: str              # BUY / SELL
    price: float
    qty: float
    notional_usdt: float
    ts_ms: int

# =========================
# DETECTOR
# =========================
class SweepDetector:
    def __init__(self, notifier: TelegramNotifier):
        self.notifier = notifier
        self.buffers: Dict[Tuple[str, str, str], Deque[TradeEvent]] = defaultdict(deque)
        self.last_alert_at: Dict[Tuple[str, str, str, str], float] = {}

    async def on_trade(self, ev: TradeEvent):
        await self._maybe_alert_single(ev)
        await self._maybe_alert_sweep(ev)

    async def _maybe_alert_single(self, ev: TradeEvent):
        if ev.notional_usdt < SINGLE_TRADE_USDT:
            return

        key = (ev.exchange, ev.symbol, ev.side, "single")
        now = time.time()
        if now - self.last_alert_at.get(key, 0) < ALERT_COOLDOWN_SEC:
            return
        self.last_alert_at[key] = now

        emoji = "🟢" if ev.side == "BUY" else "🔴"
        msg = (
            f"{emoji} 大额成交\n"
            f"交易所: {ev.exchange}\n"
            f"合约: {ev.symbol}\n"
            f"方向: {ev.side}\n"
            f"金额: {ev.notional_usdt:,.0f} USDT\n"
            f"价格: {ev.price}\n"
            f"数量: {ev.qty}\n"
            f"时间: {ev.ts_ms}"
        )
        await self.notifier.send(msg)
        logger.info("single alert | %s | %s | %s | %.0f", ev.exchange, ev.symbol, ev.side, ev.notional_usdt)

    async def _maybe_alert_sweep(self, ev: TradeEvent):
        key = (ev.exchange, ev.symbol, ev.side)
        dq = self.buffers[key]
        dq.append(ev)

        cutoff = ev.ts_ms - int(SWEEP_WINDOW_SEC * 1000)
        while dq and dq[0].ts_ms < cutoff:
            dq.popleft()

        total = sum(x.notional_usdt for x in dq)
        count = len(dq)

        if total < SWEEP_TOTAL_USDT or count < SWEEP_MIN_TRADES:
            return

        alert_key = (ev.exchange, ev.symbol, ev.side, "sweep")
        now = time.time()
        if now - self.last_alert_at.get(alert_key, 0) < ALERT_COOLDOWN_SEC:
            return
        self.last_alert_at[alert_key] = now

        first_ts = dq[0].ts_ms
        last_ts = dq[-1].ts_ms
        seconds = max((last_ts - first_ts) / 1000, 0.001)

        emoji = "🚀" if ev.side == "BUY" else "💥"
        msg = (
            f"{emoji} 连续扫单\n"
            f"交易所: {ev.exchange}\n"
            f"合约: {ev.symbol}\n"
            f"方向: {ev.side}\n"
            f"窗口: {seconds:.2f}s\n"
            f"笔数: {count}\n"
            f"累计金额: {total:,.0f} USDT\n"
            f"最新价格: {ev.price}\n"
            f"单笔阈值: {SINGLE_TRADE_USDT:,.0f} / "
            f"扫单阈值: {SWEEP_TOTAL_USDT:,.0f}"
        )
        await self.notifier.send(msg)
        logger.info("sweep alert | %s | %s | %s | count=%d total=%.0f", ev.exchange, ev.symbol, ev.side, count, total)

# =========================
# BINANCE
# docs: <symbol>@aggTrade, lowercase symbols; combined streams supported.
# buyer maker(m=true) => taker SELL, else BUY
# =========================
class BinanceMonitor:
    def __init__(self, detector: SweepDetector, symbols: List[str]):
        self.detector = detector
        self.symbols = symbols

    async def run(self):
        streams = "/".join(f"{to_binance_symbol(s)}@aggTrade" for s in self.symbols)
        url = f"wss://fstream.binance.com/stream?streams={streams}"

        while True:
            try:
                logger.info("Binance connected")
                async with websockets.connect(url, ping_interval=150, ping_timeout=30, max_size=2**23) as ws:
                    async for raw in ws:
                        msg = json.loads(raw)
                        data = msg.get("data")
                        if not data or data.get("e") != "aggTrade":
                            continue

                        symbol = data["s"].upper()
                        price = float(data["p"])
                        qty = float(data["q"])
                        notional = price * qty
                        side = "SELL" if data.get("m", False) else "BUY"
                        ts_ms = int(data["T"])

                        ev = TradeEvent(
                            exchange="BINANCE",
                            symbol=symbol,
                            side=side,
                            price=price,
                            qty=qty,
                            notional_usdt=notional,
                            ts_ms=ts_ms,
                        )
                        await self.detector.on_trade(ev)
            except Exception as e:
                logger.exception("Binance error: %s", e)
                await asyncio.sleep(5)

# =========================
# BYBIT
# docs: publicTrade.{symbol}; S is taker side Buy/Sell
# =========================
class BybitMonitor:
    def __init__(self, detector: SweepDetector, symbols: List[str]):
        self.detector = detector
        self.symbols = symbols

    async def run(self):
        url = "wss://stream.bybit.com/v5/public/linear"
        topics = [f"publicTrade.{to_bybit_symbol(s)}" for s in self.symbols]

        while True:
            try:
                logger.info("Bybit connected")
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**23) as ws:
                    sub = {"op": "subscribe", "args": topics}
                    await ws.send(json.dumps(sub))

                    async for raw in ws:
                        msg = json.loads(raw)

                        # 订阅错误 / 非数据消息
                        if msg.get("op") == "subscribe":
                            continue
                        topic = msg.get("topic", "")
                        data = msg.get("data", [])
                        if not topic.startswith("publicTrade.") or not data:
                            continue

                        for item in data:
                            symbol = item["s"].upper()
                            price = float(item["p"])
                            qty = float(item["v"])
                            notional = price * qty
                            side = item["S"].upper()
                            ts_ms = int(item["T"])

                            ev = TradeEvent(
                                exchange="BYBIT",
                                symbol=symbol,
                                side=side,
                                price=price,
                                qty=qty,
                                notional_usdt=notional,
                                ts_ms=ts_ms,
                            )
                            await self.detector.on_trade(ev)
            except Exception as e:
                logger.exception("Bybit error: %s", e)
                await asyncio.sleep(5)

# =========================
# OKX
# docs: public WS channel "trades", instrument IDs like BTC-USDT-SWAP
# side field is taker side buy/sell
# 注意: sz 是合约张数，不同币种合约面值不同；这里用 px * sz 近似 USDT 名义金额
# 若你后续要更精确，需要再接 instruments 接口换算 ctVal
# =========================
class OkxMonitor:
    def __init__(self, detector: SweepDetector, symbols: List[str]):
        self.detector = detector
        self.symbols = symbols

    @staticmethod
    def inst_id_to_unified(inst_id: str) -> str:
        # SIGN-USDT-SWAP -> SIGNUSDT
        if inst_id.endswith("-USDT-SWAP"):
            base = inst_id.replace("-USDT-SWAP", "")
            return f"{base}USDT"
        return inst_id.replace("-", "")

    async def run(self):
        url = "wss://ws.okx.com:8443/ws/v5/public"
        args = [{"channel": "trades", "instId": to_okx_inst_id(s)} for s in self.symbols]

        while True:
            try:
                logger.info("OKX connected")
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**23) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))

                    async for raw in ws:
                        msg = json.loads(raw)

                        # 订阅事件
                        if msg.get("event") in {"subscribe", "unsubscribe"}:
                            continue
                        if "arg" not in msg or "data" not in msg:
                            continue
                        if msg["arg"].get("channel") != "trades":
                            continue

                        for item in msg["data"]:
                            inst_id = item["instId"]
                            symbol = self.inst_id_to_unified(inst_id)
                            price = float(item["px"])
                            qty = float(item["sz"])
                            notional = price * qty  # 近似值
                            side = item["side"].upper()
                            ts_ms = int(item["ts"])

                            ev = TradeEvent(
                                exchange="OKX",
                                symbol=symbol,
                                side=side,
                                price=price,
                                qty=qty,
                                notional_usdt=notional,
                                ts_ms=ts_ms,
                            )
                            await self.detector.on_trade(ev)
            except Exception as e:
                logger.exception("OKX error: %s", e)
                await asyncio.sleep(5)

# =========================
# MAIN
# =========================
async def main():
    notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    await notifier.start()

    detector = SweepDetector(notifier)

    monitors = [
        BinanceMonitor(detector, USER_SYMBOLS),
        BybitMonitor(detector, USER_SYMBOLS),
        OkxMonitor(detector, USER_SYMBOLS),
    ]

    logger.info("Start symbols: %s", ", ".join(USER_SYMBOLS))
    try:
        await asyncio.gather(*(m.run() for m in monitors))
    finally:
        await notifier.close()

if __name__ == "__main__":
    asyncio.run(main())
