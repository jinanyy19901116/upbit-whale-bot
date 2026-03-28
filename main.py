import asyncio
import json
import logging
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Deque, Dict, List, Optional, Tuple

import aiohttp
import websockets

# =========================
# ENV
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise RuntimeError("缺少 TELEGRAM_TOKEN 或 TELEGRAM_CHAT_ID 环境变量")

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

# 单笔大额成交阈值
SINGLE_TRADE_USDT = float(os.getenv("SINGLE_TRADE_USDT", "100000"))

# 同交易所+同合约+同方向 的提醒冷却
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "15"))

# 机器人刷单过滤
BOT_WINDOW_SEC = float(os.getenv("BOT_WINDOW_SEC", "3"))
BOT_MIN_COUNT = int(os.getenv("BOT_MIN_COUNT", "4"))
BOT_NOTIONAL_DIFF_RATIO = float(os.getenv("BOT_NOTIONAL_DIFF_RATIO", "0.005"))  # 0.5%
BOT_PRICE_DIFF_RATIO = float(os.getenv("BOT_PRICE_DIFF_RATIO", "0.0005"))      # 0.05%

# 双向流过滤：1分钟内买卖都很活跃时，不推送，避免做市/对冲噪音
DUAL_FLOW_WINDOW_SEC = int(os.getenv("DUAL_FLOW_WINDOW_SEC", "60"))
DUAL_FLOW_MIN_TOTAL = float(os.getenv("DUAL_FLOW_MIN_TOTAL", "500000"))
DUAL_FLOW_RATIO_MIN = float(os.getenv("DUAL_FLOW_RATIO_MIN", "0.7"))

# OKX instruments 定时刷新
OKX_CTVAL_REFRESH_SEC = int(os.getenv("OKX_CTVAL_REFRESH_SEC", "3600"))

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("whale-monitor")

# =========================
# HELPERS
# =========================
CN_TZ = timezone(timedelta(hours=8))

def format_beijing_time(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(CN_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def normalize_user_symbols(raw: List[str]) -> List[str]:
    out = []
    for s in raw:
        s = s.strip().upper()
        if s.endswith("USDT"):
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

def okx_inst_id_to_unified(inst_id: str) -> str:
    if inst_id.endswith("-USDT-SWAP"):
        base = inst_id.replace("-USDT-SWAP", "")
        return f"{base}USDT"
    return inst_id.replace("-", "")

def side_to_cn(side: str) -> str:
    return "买入" if side.upper() == "BUY" else "卖出"

# =========================
# TELEGRAM
# =========================
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.session: Optional[aiohttp.ClientSession] = None

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
                    logger.error("Telegram 发送失败: %s %s", resp.status, body)
        except Exception as e:
            logger.exception("Telegram 异常: %s", e)

# =========================
# EVENT MODEL
# =========================
@dataclass
class TradeEvent:
    exchange: str
    symbol: str
    side: str
    price: float
    qty: float
    notional_usdt: float
    ts_ms: int
    raw_qty: Optional[float] = None
    contract_multiplier: Optional[float] = None

# =========================
# OKX CONTRACT VALUE CACHE
# =========================
class OkxContractSpecCache:
    """
    拉取 OKX SWAP instruments，缓存 instId -> ctVal
    notional = px * sz * ctVal
    """
    def __init__(self):
        self.ctval_map: Dict[str, float] = {}
        self.last_refresh_ts: float = 0.0
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        await self.refresh()

    async def close(self):
        if self.session:
            await self.session.close()

    async def refresh(self):
        if self.session is None:
            await self.start()

        url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
        try:
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error("拉取 OKX instruments 失败: %s %s", resp.status, body)
                    return

                data = await resp.json()
                items = data.get("data", [])
                ctval_map: Dict[str, float] = {}

                for item in items:
                    inst_id = item.get("instId")
                    ct_val = item.get("ctVal")
                    if not inst_id or ct_val in (None, ""):
                        continue
                    try:
                        ctval_map[inst_id] = float(ct_val)
                    except Exception:
                        continue

                if ctval_map:
                    self.ctval_map = ctval_map
                    self.last_refresh_ts = asyncio.get_running_loop().time()
                    logger.info("OKX ctVal 已刷新，数量: %d", len(ctval_map))
                else:
                    logger.warning("OKX ctVal 刷新为空，保留旧缓存")
        except Exception as e:
            logger.exception("刷新 OKX ctVal 异常: %s", e)

    async def auto_refresh_loop(self):
        while True:
            try:
                await self.refresh()
            except Exception as e:
                logger.exception("OKX ctVal 定时刷新异常: %s", e)
            await asyncio.sleep(OKX_CTVAL_REFRESH_SEC)

    def get_ctval(self, inst_id: str) -> float:
        return self.ctval_map.get(inst_id, 1.0)

# =========================
# LARGE TRADE DETECTOR
# =========================
class LargeTradeDetector:
    def __init__(self, notifier: TelegramNotifier):
        self.notifier = notifier

        # 同方向冷却
        self.last_alert_at: Dict[Tuple[str, str, str], float] = {}

        # 机器人刷单缓冲
        self.recent_same_side_trades: Dict[Tuple[str, str, str], Deque[TradeEvent]] = defaultdict(deque)

        # 双向流检测缓冲
        self.recent_symbol_trades: Dict[Tuple[str, str], Deque[TradeEvent]] = defaultdict(deque)

    async def on_trade(self, ev: TradeEvent):
        if ev.notional_usdt < SINGLE_TRADE_USDT:
            return

        self._push_dual_flow_buffer(ev)

        if self._is_bot_like(ev):
            logger.info(
                "过滤疑似机器人刷单 | %s | %s | %s | %.0f",
                ev.exchange, ev.symbol, ev.side, ev.notional_usdt
            )
            return

        if self._is_dual_flow_noise(ev):
            logger.info(
                "过滤双向流噪音 | %s | %s | %s | %.0f",
                ev.exchange, ev.symbol, ev.side, ev.notional_usdt
            )
            return

        await self._alert_large_trade(ev)

    def _push_dual_flow_buffer(self, ev: TradeEvent):
        key = (ev.exchange, ev.symbol)
        dq = self.recent_symbol_trades[key]
        dq.append(ev)

        cutoff = ev.ts_ms - DUAL_FLOW_WINDOW_SEC * 1000
        while dq and dq[0].ts_ms < cutoff:
            dq.popleft()

    def _is_bot_like(self, ev: TradeEvent) -> bool:
        key = (ev.exchange, ev.symbol, ev.side)
        dq = self.recent_same_side_trades[key]
        dq.append(ev)

        cutoff = ev.ts_ms - int(BOT_WINDOW_SEC * 1000)
        while dq and dq[0].ts_ms < cutoff:
            dq.popleft()

        if len(dq) < BOT_MIN_COUNT:
            return False

        notionals = [x.notional_usdt for x in dq]
        prices = [x.price for x in dq]

        max_notional = max(notionals)
        min_notional = min(notionals)
        max_price = max(prices)
        min_price = min(prices)

        notional_ratio = (max_notional - min_notional) / max(min_notional, 1)
        price_ratio = (max_price - min_price) / max(min_price, 1)

        if notional_ratio <= BOT_NOTIONAL_DIFF_RATIO and price_ratio <= BOT_PRICE_DIFF_RATIO:
            return True

        return False

    def _is_dual_flow_noise(self, ev: TradeEvent) -> bool:
        """
        1分钟内买卖两边都很活跃，且金额接近，则视为双向流噪音
        """
        key = (ev.exchange, ev.symbol)
        dq = self.recent_symbol_trades[key]
        if not dq:
            return False

        buy_total = sum(x.notional_usdt for x in dq if x.side == "BUY")
        sell_total = sum(x.notional_usdt for x in dq if x.side == "SELL")

        if buy_total < DUAL_FLOW_MIN_TOTAL or sell_total < DUAL_FLOW_MIN_TOTAL:
            return False

        smaller = min(buy_total, sell_total)
        bigger = max(buy_total, sell_total)
        ratio = smaller / max(bigger, 1)

        return ratio >= DUAL_FLOW_RATIO_MIN

    async def _alert_large_trade(self, ev: TradeEvent):
        key = (ev.exchange, ev.symbol, ev.side)
        now_ts = ev.ts_ms / 1000

        last_ts = self.last_alert_at.get(key, 0)
        if now_ts - last_ts < ALERT_COOLDOWN_SEC:
            return

        self.last_alert_at[key] = now_ts

        side_cn = side_to_cn(ev.side)
        emoji = "🟢" if ev.side == "BUY" else "🔴"

        extra = ""
        if ev.exchange == "OKX" and ev.contract_multiplier is not None:
            extra = f"\nOKX面值：{ev.contract_multiplier:g}"

        msg = (
            f"{emoji} 大额成交单\n"
            f"交易所：{ev.exchange}\n"
            f"合约：{ev.symbol}\n"
            f"方向：{side_cn}\n"
            f"金额：{ev.notional_usdt:,.0f} USDT\n"
            f"价格：{ev.price}\n"
            f"数量：{ev.qty}\n"
            f"时间：{format_beijing_time(ev.ts_ms)}（北京时间）"
            f"{extra}"
        )

        await self.notifier.send(msg)
        logger.info(
            "推送大额成交 | %s | %s | %s | %.0f",
            ev.exchange, ev.symbol, side_cn, ev.notional_usdt
        )

# =========================
# BINANCE
# =========================
class BinanceMonitor:
    def __init__(self, detector: LargeTradeDetector, symbols: List[str]):
        self.detector = detector
        self.symbols = symbols

    async def run(self):
        streams = "/".join(f"{to_binance_symbol(s)}@aggTrade" for s in self.symbols)
        url = f"wss://fstream.binance.com/stream?streams={streams}"

        while True:
            try:
                logger.info("Binance 已连接")
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
                            raw_qty=qty,
                            notional_usdt=notional,
                            ts_ms=ts_ms,
                        )
                        await self.detector.on_trade(ev)
            except Exception as e:
                logger.exception("Binance 异常: %s", e)
                await asyncio.sleep(5)

# =========================
# BYBIT
# =========================
class BybitMonitor:
    def __init__(self, detector: LargeTradeDetector, symbols: List[str]):
        self.detector = detector
        self.symbols = symbols

    async def run(self):
        url = "wss://stream.bybit.com/v5/public/linear"
        topics = [f"publicTrade.{to_bybit_symbol(s)}" for s in self.symbols]

        while True:
            try:
                logger.info("Bybit 已连接")
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**23) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": topics}))

                    async for raw in ws:
                        msg = json.loads(raw)

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
                                raw_qty=qty,
                                notional_usdt=notional,
                                ts_ms=ts_ms,
                            )
                            await self.detector.on_trade(ev)
            except Exception as e:
                logger.exception("Bybit 异常: %s", e)
                await asyncio.sleep(5)

# =========================
# OKX
# =========================
class OkxMonitor:
    def __init__(self, detector: LargeTradeDetector, symbols: List[str], spec_cache: OkxContractSpecCache):
        self.detector = detector
        self.symbols = symbols
        self.spec_cache = spec_cache

    async def run(self):
        url = "wss://ws.okx.com:8443/ws/v5/public"
        args = [{"channel": "trades", "instId": to_okx_inst_id(s)} for s in self.symbols]

        while True:
            try:
                logger.info("OKX 已连接")
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**23) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))

                    async for raw in ws:
                        msg = json.loads(raw)

                        if msg.get("event") in {"subscribe", "unsubscribe", "error"}:
                            if msg.get("event") == "error":
                                logger.warning("OKX 订阅错误: %s", msg)
                            continue

                        if "arg" not in msg or "data" not in msg:
                            continue
                        if msg["arg"].get("channel") != "trades":
                            continue

                        for item in msg["data"]:
                            inst_id = item["instId"]
                            symbol = okx_inst_id_to_unified(inst_id)

                            price = float(item["px"])
                            contracts = float(item["sz"])
                            ct_val = self.spec_cache.get_ctval(inst_id)

                            # 修正后的名义金额
                            qty_coin = contracts * ct_val
                            notional = price * qty_coin

                            side = item["side"].upper()
                            ts_ms = int(item["ts"])

                            ev = TradeEvent(
                                exchange="OKX",
                                symbol=symbol,
                                side=side,
                                price=price,
                                qty=qty_coin,
                                raw_qty=contracts,
                                contract_multiplier=ct_val,
                                notional_usdt=notional,
                                ts_ms=ts_ms,
                            )
                            await self.detector.on_trade(ev)
            except Exception as e:
                logger.exception("OKX 异常: %s", e)
                await asyncio.sleep(5)

# =========================
# MAIN
# =========================
async def main():
    notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    await notifier.start()

    okx_spec_cache = OkxContractSpecCache()
    await okx_spec_cache.start()

    detector = LargeTradeDetector(notifier)

    monitors = [
        BinanceMonitor(detector, USER_SYMBOLS),
        BybitMonitor(detector, USER_SYMBOLS),
        OkxMonitor(detector, USER_SYMBOLS, okx_spec_cache),
    ]

    logger.info("启动监控币种: %s", ", ".join(USER_SYMBOLS))

    try:
        await asyncio.gather(
            okx_spec_cache.auto_refresh_loop(),
            *(m.run() for m in monitors),
        )
    finally:
        await notifier.close()
        await okx_spec_cache.close()

if __name__ == "__main__":
    asyncio.run(main())
