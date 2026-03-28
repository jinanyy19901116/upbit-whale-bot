import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Deque, Dict, List, Optional, Set, Tuple

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

# 大额成交阈值：200000 USDT
SINGLE_TRADE_USDT = float(os.getenv("SINGLE_TRADE_USDT", "200000"))

# 同交易所+同合约+同方向 冷却
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "15"))

# 机器人刷单过滤
BOT_WINDOW_SEC = float(os.getenv("BOT_WINDOW_SEC", "3"))
BOT_MIN_COUNT = int(os.getenv("BOT_MIN_COUNT", "4"))
BOT_NOTIONAL_DIFF_RATIO = float(os.getenv("BOT_NOTIONAL_DIFF_RATIO", "0.005"))  # 0.5%
BOT_PRICE_DIFF_RATIO = float(os.getenv("BOT_PRICE_DIFF_RATIO", "0.0005"))      # 0.05%

# 双向流过滤：1分钟内买卖都活跃，且金额接近，则不推送
DUAL_FLOW_WINDOW_SEC = int(os.getenv("DUAL_FLOW_WINDOW_SEC", "60"))
DUAL_FLOW_MIN_TOTAL = float(os.getenv("DUAL_FLOW_MIN_TOTAL", "500000"))
DUAL_FLOW_RATIO_MIN = float(os.getenv("DUAL_FLOW_RATIO_MIN", "0.7"))

# 价格状态阈值
PRICE_FLAT_1M_PCT = float(os.getenv("PRICE_FLAT_1M_PCT", "0.002"))   # 0.2%
PRICE_FLAT_5M_PCT = float(os.getenv("PRICE_FLAT_5M_PCT", "0.004"))   # 0.4%

# OI 状态阈值
OI_FLAT_5M_PCT = float(os.getenv("OI_FLAT_5M_PCT", "0.005"))         # 0.5%

# 合约列表刷新间隔
CONTRACT_REFRESH_SEC = int(os.getenv("CONTRACT_REFRESH_SEC", "3600"))

# 价格/OI 轮询间隔
SNAPSHOT_POLL_SEC = int(os.getenv("SNAPSHOT_POLL_SEC", "20"))

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
    return sym.upper()


def to_gate_symbol(sym: str) -> str:
    base = sym[:-4]
    return f"{base}_USDT"


def to_mexc_symbol(sym: str) -> str:
    base = sym[:-4]
    return f"{base}_USDT"


def side_to_cn(side: str) -> str:
    return "买入" if side.upper() == "BUY" else "卖出"


def pct_change(old: Optional[float], new: Optional[float]) -> Optional[float]:
    if old is None or new is None or old == 0:
        return None
    return (new - old) / old


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
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))

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
# PRICE / OI STATE
# =========================
class MarketState:
    def __init__(self):
        self.price_history: Dict[Tuple[str, str], Deque[Tuple[int, float]]] = defaultdict(deque)
        self.oi_history: Dict[Tuple[str, str], Deque[Tuple[int, float]]] = defaultdict(deque)

    def update_price(self, exchange: str, symbol: str, price: float, ts_ms: int):
        key = (exchange, symbol)
        dq = self.price_history[key]
        dq.append((ts_ms, price))
        self._trim(dq, ts_ms, 6 * 60 * 1000)

    def update_oi(self, exchange: str, symbol: str, oi_usdt: float, ts_ms: int):
        key = (exchange, symbol)
        dq = self.oi_history[key]
        dq.append((ts_ms, oi_usdt))
        self._trim(dq, ts_ms, 6 * 60 * 1000)

    @staticmethod
    def _trim(dq: Deque[Tuple[int, float]], now_ts_ms: int, keep_ms: int):
        cutoff = now_ts_ms - keep_ms
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    @staticmethod
    def _find_value_before(dq: Deque[Tuple[int, float]], target_ts_ms: int) -> Optional[float]:
        candidate = None
        for ts, val in dq:
            if ts <= target_ts_ms:
                candidate = val
            else:
                break
        return candidate

    def get_price_change_1m(self, exchange: str, symbol: str) -> Optional[float]:
        dq = self.price_history.get((exchange, symbol))
        if not dq:
            return None
        latest_ts, latest_price = dq[-1]
        old_price = self._find_value_before(dq, latest_ts - 60 * 1000)
        return pct_change(old_price, latest_price)

    def get_price_change_5m(self, exchange: str, symbol: str) -> Optional[float]:
        dq = self.price_history.get((exchange, symbol))
        if not dq:
            return None
        latest_ts, latest_price = dq[-1]
        old_price = self._find_value_before(dq, latest_ts - 5 * 60 * 1000)
        return pct_change(old_price, latest_price)

    def get_oi_change_5m(self, exchange: str, symbol: str) -> Optional[float]:
        dq = self.oi_history.get((exchange, symbol))
        if not dq:
            return None
        latest_ts, latest_oi = dq[-1]
        old_oi = self._find_value_before(dq, latest_ts - 5 * 60 * 1000)
        return pct_change(old_oi, latest_oi)

    def get_price_status(self, exchange: str, symbol: str) -> str:
        ch1 = self.get_price_change_1m(exchange, symbol)
        ch5 = self.get_price_change_5m(exchange, symbol)

        if ch5 is not None:
            if ch5 >= PRICE_FLAT_5M_PCT:
                return "上涨"
            if ch5 <= -PRICE_FLAT_5M_PCT:
                return "下跌"
        if ch1 is not None:
            if ch1 >= PRICE_FLAT_1M_PCT:
                return "上涨"
            if ch1 <= -PRICE_FLAT_1M_PCT:
                return "下跌"
        return "横盘"

    def get_oi_status(self, exchange: str, symbol: str) -> str:
        ch5 = self.get_oi_change_5m(exchange, symbol)
        if ch5 is None:
            return "未知"
        if ch5 >= OI_FLAT_5M_PCT:
            return "增加"
        if ch5 <= -OI_FLAT_5M_PCT:
            return "减少"
        return "持平"

    def get_snapshot_text(self, exchange: str, symbol: str) -> str:
        ch1 = self.get_price_change_1m(exchange, symbol)
        ch5 = self.get_price_change_5m(exchange, symbol)
        oi5 = self.get_oi_change_5m(exchange, symbol)

        def fmt_pct(x: Optional[float]) -> str:
            if x is None:
                return "N/A"
            return f"{x * 100:+.2f}%"

        return (
            f"价格状态：{self.get_price_status(exchange, symbol)}\n"
            f"1分钟价格：{fmt_pct(ch1)}\n"
            f"5分钟价格：{fmt_pct(ch5)}\n"
            f"OI状态：{self.get_oi_status(exchange, symbol)}\n"
            f"5分钟OI：{fmt_pct(oi5)}"
        )


# =========================
# CONTRACT CACHES
# =========================
class BinanceContractCache:
    def __init__(self):
        self.valid_symbols: Set[str] = set()
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        await self.refresh()

    async def close(self):
        if self.session:
            await self.session.close()

    async def refresh(self):
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        try:
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    logger.error("Binance exchangeInfo 拉取失败: %s", resp.status)
                    return
                data = await resp.json()
                symbols = data.get("symbols", [])
                valid = set()
                for item in symbols:
                    sym = item.get("symbol")
                    status = item.get("status")
                    if sym and status == "TRADING":
                        valid.add(sym.upper())
                self.valid_symbols = valid
                logger.info("Binance 合约列表已刷新，有效数量: %d", len(valid))
        except Exception as e:
            logger.exception("Binance 合约列表刷新异常: %s", e)

    async def auto_refresh_loop(self):
        while True:
            await asyncio.sleep(CONTRACT_REFRESH_SEC)
            await self.refresh()

    def is_valid(self, symbol: str) -> bool:
        return symbol.upper() in self.valid_symbols


class GateContractCache:
    def __init__(self):
        self.valid_symbols: Set[str] = set()
        self.multiplier_map: Dict[str, float] = {}
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        await self.refresh()

    async def close(self):
        if self.session:
            await self.session.close()

    async def refresh(self):
        url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
        try:
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    logger.error("Gate contracts 拉取失败: %s", resp.status)
                    return
                data = await resp.json()

                valid: Set[str] = set()
                mults: Dict[str, float] = {}

                if isinstance(data, list):
                    for item in data:
                        contract = str(item.get("name") or item.get("contract") or "").upper()
                        if not contract:
                            continue
                        valid.add(contract)
                        # Gate 常见字段为 quanto_multiplier；若接口返回不同，则回退为 1.0
                        raw_mult = (
                            item.get("quanto_multiplier")
                            or item.get("quantoMultiplier")
                            or item.get("multiplier")
                            or 1.0
                        )
                        try:
                            mults[contract] = float(raw_mult)
                        except Exception:
                            mults[contract] = 1.0

                self.valid_symbols = valid
                self.multiplier_map = mults
                logger.info("Gate 合约列表已刷新，有效数量: %d", len(valid))
        except Exception as e:
            logger.exception("Gate 合约列表刷新异常: %s", e)

    async def auto_refresh_loop(self):
        while True:
            await asyncio.sleep(CONTRACT_REFRESH_SEC)
            await self.refresh()

    def is_valid(self, symbol: str) -> bool:
        return symbol.upper() in self.valid_symbols

    def get_multiplier(self, symbol: str) -> float:
        return self.multiplier_map.get(symbol.upper(), 1.0)


class MexcContractCache:
    def __init__(self):
        self.valid_symbols: Set[str] = set()
        self.contract_size_map: Dict[str, float] = {}
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        await self.refresh()

    async def close(self):
        if self.session:
            await self.session.close()

    async def refresh(self):
        url = "https://api.mexc.com/api/v1/contract/detail"
        try:
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    logger.error("MEXC contract detail 拉取失败: %s", resp.status)
                    return
                payload = await resp.json()
                items = payload.get("data", [])
                valid: Set[str] = set()
                sizes: Dict[str, float] = {}

                if isinstance(items, dict):
                    items = [items]

                for item in items:
                    symbol = str(item.get("symbol") or "").upper()
                    if not symbol:
                        continue
                    valid.add(symbol)
                    try:
                        sizes[symbol] = float(item.get("contractSize", 1.0))
                    except Exception:
                        sizes[symbol] = 1.0

                self.valid_symbols = valid
                self.contract_size_map = sizes
                logger.info("MEXC 合约列表已刷新，有效数量: %d", len(valid))
        except Exception as e:
            logger.exception("MEXC 合约列表刷新异常: %s", e)

    async def auto_refresh_loop(self):
        while True:
            await asyncio.sleep(CONTRACT_REFRESH_SEC)
            await self.refresh()

    def is_valid(self, symbol: str) -> bool:
        return symbol.upper() in self.valid_symbols

    def get_contract_size(self, symbol: str) -> float:
        return self.contract_size_map.get(symbol.upper(), 1.0)


# =========================
# SNAPSHOT FETCHERS
# =========================
class SnapshotPoller:
    def __init__(
        self,
        market_state: MarketState,
        binance_cache: BinanceContractCache,
        mexc_cache: MexcContractCache,
    ):
        self.market_state = market_state
        self.binance_cache = binance_cache
        self.mexc_cache = mexc_cache
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def close(self):
        if self.session:
            await self.session.close()

    # ---------- Binance ----------
    async def _poll_binance_symbol(self, symbol: str):
        sym = symbol.upper()
        if not self.binance_cache.is_valid(sym):
            return

        try:
            async with self.session.get(
                f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym}"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    mark_price = float(data["markPrice"])
                    ts_ms = int(data.get("time", int(datetime.now(tz=timezone.utc).timestamp() * 1000)))
                    self.market_state.update_price("BINANCE", sym, mark_price, ts_ms)
        except Exception as e:
            logger.debug("Binance price 拉取失败 %s: %s", sym, e)

        try:
            async with self.session.get(
                f"https://fapi.binance.com/fapi/v1/openInterest?symbol={sym}"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    oi_contracts = float(data["openInterest"])
                    ts_ms = int(data.get("time", int(datetime.now(tz=timezone.utc).timestamp() * 1000)))

                    price_dq = self.market_state.price_history.get(("BINANCE", sym))
                    if price_dq:
                        last_price = price_dq[-1][1]
                        oi_usdt = oi_contracts * last_price
                        self.market_state.update_oi("BINANCE", sym, oi_usdt, ts_ms)
        except Exception as e:
            logger.debug("Binance OI 拉取失败 %s: %s", sym, e)

    async def poll_binance_loop(self, symbols: List[str]):
        while True:
            for sym in symbols:
                try:
                    await self._poll_binance_symbol(sym)
                except Exception as e:
                    logger.debug("Binance snapshot 异常 %s: %s", sym, e)
            await asyncio.sleep(SNAPSHOT_POLL_SEC)

    # ---------- MEXC ----------
    async def _poll_mexc_symbol(self, symbol: str):
        sym = to_mexc_symbol(symbol)
        if not self.mexc_cache.is_valid(sym):
            return

        try:
            async with self.session.get(
                "https://api.mexc.com/api/v1/contract/ticker",
                params={"symbol": sym},
            ) as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    data = payload.get("data")
                    if isinstance(data, list):
                        data = data[0] if data else None
                    if data:
                        price = float(data.get("fairPrice") or data.get("lastPrice"))
                        ts_ms = int(data.get("timestamp", int(datetime.now(tz=timezone.utc).timestamp() * 1000)))
                        self.market_state.update_price("MEXC", sym, price, ts_ms)

                        hold_vol = float(data.get("holdVol", 0))
                        contract_size = self.mexc_cache.get_contract_size(sym)
                        oi_usdt = hold_vol * contract_size * price
                        self.market_state.update_oi("MEXC", sym, oi_usdt, ts_ms)
        except Exception as e:
            logger.debug("MEXC ticker 拉取失败 %s: %s", sym, e)

    async def poll_mexc_loop(self, symbols: List[str]):
        while True:
            for sym in symbols:
                try:
                    await self._poll_mexc_symbol(sym)
                except Exception as e:
                    logger.debug("MEXC snapshot 异常 %s: %s", sym, e)
            await asyncio.sleep(SNAPSHOT_POLL_SEC)


# =========================
# LARGE TRADE DETECTOR
# =========================
class LargeTradeDetector:
    def __init__(self, notifier: TelegramNotifier, market_state: MarketState):
        self.notifier = notifier
        self.market_state = market_state

        self.last_alert_at: Dict[Tuple[str, str, str], float] = {}
        self.recent_same_side_trades: Dict[Tuple[str, str, str], Deque[TradeEvent]] = defaultdict(deque)
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

        return (
            notional_ratio <= BOT_NOTIONAL_DIFF_RATIO
            and price_ratio <= BOT_PRICE_DIFF_RATIO
        )

    def _is_dual_flow_noise(self, ev: TradeEvent) -> bool:
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

        market_text = self.market_state.get_snapshot_text(ev.exchange, ev.symbol)

        msg = (
            f"{emoji} 大额成交单\n"
            f"交易所：{ev.exchange}\n"
            f"合约：{ev.symbol}\n"
            f"方向：{side_cn}\n"
            f"金额：{ev.notional_usdt:,.0f} USDT\n"
            f"价格：{ev.price}\n"
            f"数量：{ev.qty}\n"
            f"{market_text}\n"
            f"时间：{format_beijing_time(ev.ts_ms)}（北京时间）"
        )

        await self.notifier.send(msg)
        logger.info(
            "推送大额成交 | %s | %s | %s | %.0f | %s | %s",
            ev.exchange,
            ev.symbol,
            side_cn,
            ev.notional_usdt,
            self.market_state.get_price_status(ev.exchange, ev.symbol),
            self.market_state.get_oi_status(ev.exchange, ev.symbol),
        )


# =========================
# BINANCE
# =========================
class BinanceMonitor:
    def __init__(self, detector: LargeTradeDetector, market_state: MarketState, symbols: List[str], cache: BinanceContractCache):
        self.detector = detector
        self.market_state = market_state
        self.symbols = symbols
        self.cache = cache

    def _valid_symbols(self) -> List[str]:
        valid = []
        for s in self.symbols:
            if self.cache.is_valid(s):
                valid.append(s)
            else:
                logger.warning("Binance 不存在该永续合约，已跳过: %s", s)
        return valid

    async def run(self):
        while True:
            try:
                valid_symbols = self._valid_symbols()
                if not valid_symbols:
                    logger.warning("Binance 没有可订阅的有效合约，60秒后重试")
                    await asyncio.sleep(60)
                    continue

                streams = "/".join(f"{s.lower()}@aggTrade" for s in valid_symbols)
                url = f"wss://fstream.binance.com/stream?streams={streams}"

                logger.info("Binance 已连接，有效订阅数: %d", len(valid_symbols))
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

                        self.market_state.update_price("BINANCE", symbol, price, ts_ms)

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
# GATE
# =========================
class GateMonitor:
    def __init__(self, detector: LargeTradeDetector, market_state: MarketState, symbols: List[str], cache: GateContractCache):
        self.detector = detector
        self.market_state = market_state
        self.symbols = symbols
        self.cache = cache

    def _valid_symbols(self) -> List[str]:
        valid = []
        for s in self.symbols:
            gate_sym = to_gate_symbol(s)
            if self.cache.is_valid(gate_sym):
                valid.append(gate_sym)
            else:
                logger.warning("Gate 不存在该永续合约，已跳过: %s -> %s", s, gate_sym)
        return valid

    async def run(self):
        url = "wss://fx-ws.gateio.ws/v4/ws/usdt"

        while True:
            try:
                valid_symbols = self._valid_symbols()
                if not valid_symbols:
                    logger.warning("Gate 没有可订阅的有效合约，60秒后重试")
                    await asyncio.sleep(60)
                    continue

                logger.info("Gate 已连接，有效订阅数: %d", len(valid_symbols))
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**23) as ws:
                    now_sec = int(time.time())

                    await ws.send(json.dumps({
                        "time": now_sec,
                        "channel": "futures.trades",
                        "event": "subscribe",
                        "payload": valid_symbols,
                    }))

                    for sym in valid_symbols:
                        await ws.send(json.dumps({
                            "time": int(time.time()),
                            "channel": "futures.tickers",
                            "event": "subscribe",
                            "payload": [sym],
                        }))
                        await ws.send(json.dumps({
                            "time": int(time.time()),
                            "channel": "futures.contract_stats",
                            "event": "subscribe",
                            "payload": [sym, "1m"],
                        }))

                    async for raw in ws:
                        msg = json.loads(raw)
                        channel = msg.get("channel")
                        event = msg.get("event")

                        if event in {"subscribe", "unsubscribe"}:
                            continue
                        if event == "error":
                            logger.warning("Gate 订阅错误: %s", msg)
                            continue

                        if channel == "futures.trades" and event == "update":
                            result = msg.get("result", [])
                            for item in result:
                                contract = str(item.get("contract", "")).upper()
                                if not contract:
                                    continue

                                price = float(item["price"])
                                size = int(item["size"])
                                ts_ms = int(item.get("create_time_ms") or msg.get("time_ms") or int(time.time() * 1000))

                                multiplier = self.cache.get_multiplier(contract)
                                qty_coin = abs(size) * multiplier
                                notional = price * qty_coin
                                side = "BUY" if size > 0 else "SELL"

                                self.market_state.update_price("GATE", contract, price, ts_ms)

                                ev = TradeEvent(
                                    exchange="GATE",
                                    symbol=contract,
                                    side=side,
                                    price=price,
                                    qty=qty_coin,
                                    raw_qty=abs(size),
                                    contract_multiplier=multiplier,
                                    notional_usdt=notional,
                                    ts_ms=ts_ms,
                                )
                                await self.detector.on_trade(ev)

                        elif channel == "futures.tickers" and event == "update":
                            result = msg.get("result", [])
                            ts_ms = int(msg.get("time_ms") or int(time.time() * 1000))
                            for item in result:
                                contract = str(item.get("contract", "")).upper()
                                if not contract:
                                    continue
                                price = float(item.get("mark_price") or item.get("last"))
                                self.market_state.update_price("GATE", contract, price, ts_ms)

                        elif channel == "futures.contract_stats" and event == "update":
                            result = msg.get("result", [])
                            contract = None
                            # 某些实现里 ws 不回 contract，因此从最近一次订阅映射到通道粒度较难区分；
                            # 这里优先尝试消息里直接带 contract，没有则跳过 OI 更新。
                            for item in result:
                                contract = item.get("contract")
                                if contract:
                                    break

                            # 若消息未带 contract，通常只能依赖订阅上下文；这里做保守处理，不硬猜。
                            if not contract:
                                continue

                            contract = str(contract).upper()
                            ts_ms = int(item.get("time", int(time.time())) * 1000)
                            oi_usdt = float(item.get("open_interest_usd", 0))
                            if oi_usdt > 0:
                                self.market_state.update_oi("GATE", contract, oi_usdt, ts_ms)

            except Exception as e:
                logger.exception("Gate 异常: %s", e)
                await asyncio.sleep(5)


# =========================
# MEXC
# =========================
class MexcMonitor:
    def __init__(self, detector: LargeTradeDetector, market_state: MarketState, symbols: List[str], cache: MexcContractCache):
        self.detector = detector
        self.market_state = market_state
        self.symbols = symbols
        self.cache = cache

    def _valid_symbols(self) -> List[str]:
        valid = []
        for s in self.symbols:
            mexc_sym = to_mexc_symbol(s)
            if self.cache.is_valid(mexc_sym):
                valid.append(mexc_sym)
            else:
                logger.warning("MEXC 不存在该永续合约，已跳过: %s -> %s", s, mexc_sym)
        return valid

    async def _ping_loop(self, ws):
        while True:
            try:
                await asyncio.sleep(15)
                await ws.send(json.dumps({"method": "ping"}))
            except Exception:
                return

    async def run(self):
        url = "wss://contract.mexc.com/edge"

        while True:
            try:
                valid_symbols = self._valid_symbols()
                if not valid_symbols:
                    logger.warning("MEXC 没有可订阅的有效合约，60秒后重试")
                    await asyncio.sleep(60)
                    continue

                logger.info("MEXC 已连接，有效订阅数: %d", len(valid_symbols))
                async with websockets.connect(url, ping_interval=None, ping_timeout=None, max_size=2**23) as ws:
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        for sym in valid_symbols:
                            await ws.send(json.dumps({
                                "method": "sub.deal",
                                "param": {"symbol": sym},
                            }))

                        async for raw in ws:
                            msg = json.loads(raw)

                            channel = msg.get("channel")
                            if channel == "pong":
                                continue
                            if channel != "push.deal":
                                continue

                            symbol = str(msg.get("symbol", "")).upper()
                            data = msg.get("data", [])
                            if not symbol or not data:
                                continue

                            contract_size = self.cache.get_contract_size(symbol)

                            for item in data:
                                price = float(item["p"])
                                contracts = float(item["v"])
                                qty_coin = contracts * contract_size
                                notional = price * qty_coin
                                side = "BUY" if int(item["T"]) == 1 else "SELL"
                                ts_ms = int(item["t"])

                                self.market_state.update_price("MEXC", symbol, price, ts_ms)

                                ev = TradeEvent(
                                    exchange="MEXC",
                                    symbol=symbol,
                                    side=side,
                                    price=price,
                                    qty=qty_coin,
                                    raw_qty=contracts,
                                    contract_multiplier=contract_size,
                                    notional_usdt=notional,
                                    ts_ms=ts_ms,
                                )
                                await self.detector.on_trade(ev)
                    finally:
                        ping_task.cancel()
            except Exception as e:
                logger.exception("MEXC 异常: %s", e)
                await asyncio.sleep(5)


# =========================
# MAIN
# =========================
async def main():
    notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    await notifier.start()

    market_state = MarketState()

    binance_cache = BinanceContractCache()
    gate_cache = GateContractCache()
    mexc_cache = MexcContractCache()

    await binance_cache.start()
    await gate_cache.start()
    await mexc_cache.start()

    detector = LargeTradeDetector(notifier, market_state)

    snapshot_poller = SnapshotPoller(
        market_state=market_state,
        binance_cache=binance_cache,
        mexc_cache=mexc_cache,
    )
    await snapshot_poller.start()

    monitors = [
        BinanceMonitor(detector, market_state, USER_SYMBOLS, binance_cache),
        GateMonitor(detector, market_state, USER_SYMBOLS, gate_cache),
        MexcMonitor(detector, market_state, USER_SYMBOLS, mexc_cache),
    ]

    logger.info("启动监控币种: %s", ", ".join(USER_SYMBOLS))
    logger.info("当前启用交易所: BINANCE, GATE, MEXC")
    logger.info("大单阈值: %.0f USDT", SINGLE_TRADE_USDT)

    try:
        await asyncio.gather(
            binance_cache.auto_refresh_loop(),
            gate_cache.auto_refresh_loop(),
            mexc_cache.auto_refresh_loop(),
            snapshot_poller.poll_binance_loop(USER_SYMBOLS),
            snapshot_poller.poll_mexc_loop(USER_SYMBOLS),
            *(m.run() for m in monitors),
        )
    finally:
        await notifier.close()
        await snapshot_poller.close()
        await binance_cache.close()
        await gate_cache.close()
        await mexc_cache.close()


if __name__ == "__main__":
    asyncio.run(main())
