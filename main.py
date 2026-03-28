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

# 大额成交阈值：100000 USDT
SINGLE_TRADE_USDT = float(os.getenv("SINGLE_TRADE_USDT", "100000"))

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

# 合约/交易对列表刷新间隔
CONTRACT_REFRESH_SEC = int(os.getenv("CONTRACT_REFRESH_SEC", "3600"))

# 价格/OI 轮询间隔
SNAPSHOT_POLL_SEC = int(os.getenv("SNAPSHOT_POLL_SEC", "20"))

# MEXC Spot WS
MEXC_SPOT_WS_URL = os.getenv("MEXC_SPOT_WS_URL", "wss://wbs-api.mexc.com/ws")
MEXC_SPOT_STREAM_INTERVAL = os.getenv("MEXC_SPOT_STREAM_INTERVAL", "100ms").strip() or "100ms"
MEXC_SPOT_MAX_SUBS_PER_CONN = int(os.getenv("MEXC_SPOT_MAX_SUBS_PER_CONN", "30"))
MEXC_SPOT_PING_SEC = int(os.getenv("MEXC_SPOT_PING_SEC", "15"))
MEXC_SPOT_RECONNECT_SEC = int(os.getenv("MEXC_SPOT_RECONNECT_SEC", str(23 * 3600 + 50 * 60)))  # 23h50m

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


def to_gate_symbol(sym: str) -> str:
    base = sym[:-4]
    return f"{base}_USDT"


def to_mexc_spot_symbol(sym: str) -> str:
    return sym.strip().upper()


def side_to_cn(side: str) -> str:
    return "买入" if side.upper() == "BUY" else "卖出"


def pct_change(old: Optional[float], new: Optional[float]) -> Optional[float]:
    if old is None or new is None or old == 0:
        return None
    return (new - old) / old


def safe_float(v, default: float = 0.0) -> float:
    try:
        if v in (None, ""):
            return default
        return float(v)
    except Exception:
        return default


def safe_int(v, default: int = 0) -> int:
    try:
        if v in (None, ""):
            return default
        return int(float(v))
    except Exception:
        return default


def chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


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
                    logger.error("Telegram 发送失败: %s %s", resp.status, body[:500])
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
        if price <= 0:
            return
        key = (exchange, symbol)
        dq = self.price_history[key]
        dq.append((ts_ms, price))
        self._trim(dq, ts_ms, 6 * 60 * 1000)

    def update_oi(self, exchange: str, symbol: str, oi_usdt: float, ts_ms: int):
        if oi_usdt <= 0:
            return
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
# CONTRACT / SYMBOL CACHES
# =========================
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
                    body = await resp.text()
                    logger.error("Gate contracts 拉取失败: %s %s", resp.status, body[:300])
                    return
                data = await resp.json()

                valid: Set[str] = set()
                mults: Dict[str, float] = {}

                if isinstance(data, list):
                    for item in data:
                        if not isinstance(item, dict):
                            continue
                        contract = str(item.get("name") or item.get("contract") or "").upper()
                        if not contract:
                            continue
                        valid.add(contract)

                        raw_mult = (
                            item.get("quanto_multiplier")
                            or item.get("quantoMultiplier")
                            or item.get("multiplier")
                            or 1.0
                        )
                        mults[contract] = safe_float(raw_mult, 1.0)

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


class MexcSpotSymbolCache:
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
        url = "https://api.mexc.com/api/v3/exchangeInfo"
        try:
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error("MEXC Spot exchangeInfo 拉取失败: %s %s", resp.status, body[:300])
                    return

                payload = await resp.json()
                items = payload.get("symbols", [])
                valid: Set[str] = set()

                if isinstance(items, list):
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        symbol = str(item.get("symbol") or "").upper()
                        if not symbol:
                            continue
                        valid.add(symbol)

                self.valid_symbols = valid
                logger.info("MEXC Spot 交易对列表已刷新，有效数量: %d", len(valid))
        except Exception as e:
            logger.exception("MEXC Spot 交易对列表刷新异常: %s", e)

    async def auto_refresh_loop(self):
        while True:
            await asyncio.sleep(CONTRACT_REFRESH_SEC)
            await self.refresh()

    def is_valid(self, symbol: str) -> bool:
        return symbol.upper() in self.valid_symbols


# =========================
# SNAPSHOT FETCHERS
# =========================
class SnapshotPoller:
    def __init__(
        self,
        market_state: MarketState,
        gate_cache: GateContractCache,
        mexc_cache: MexcSpotSymbolCache,
    ):
        self.market_state = market_state
        self.gate_cache = gate_cache
        self.mexc_cache = mexc_cache
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def close(self):
        if self.session:
            await self.session.close()

    # ---------- Gate ----------
    async def _poll_gate_symbol(self, unified_symbol: str):
        gate_sym = to_gate_symbol(unified_symbol)
        if not self.gate_cache.is_valid(gate_sym):
            return

        try:
            async with self.session.get(
                f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{gate_sym}"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = safe_float(data.get("mark_price") or data.get("last_price"))
                    ts_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
                    self.market_state.update_price("GATE", gate_sym, price, ts_ms)
        except Exception as e:
            logger.debug("Gate price 拉取失败 %s: %s", gate_sym, e)

        try:
            async with self.session.get(
                "https://api.gateio.ws/api/v4/futures/usdt/contract_stats",
                params={"contract": gate_sym, "limit": 1},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and data:
                        item = data[-1]
                        if isinstance(item, dict):
                            oi_usdt = safe_float(item.get("open_interest_usd"))
                            ts_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
                            self.market_state.update_oi("GATE", gate_sym, oi_usdt, ts_ms)
        except Exception as e:
            logger.debug("Gate OI 拉取失败 %s: %s", gate_sym, e)

    async def poll_gate_loop(self, symbols: List[str]):
        while True:
            for sym in symbols:
                try:
                    await self._poll_gate_symbol(sym)
                except Exception as e:
                    logger.debug("Gate snapshot 异常 %s: %s", sym, e)
            await asyncio.sleep(SNAPSHOT_POLL_SEC)

    # ---------- MEXC Spot ----------
    async def _poll_mexc_symbol(self, symbol: str):
        mexc_sym = to_mexc_spot_symbol(symbol)
        if not self.mexc_cache.is_valid(mexc_sym):
            return

        try:
            async with self.session.get(
                "https://api.mexc.com/api/v3/ticker/price",
                params={"symbol": mexc_sym},
            ) as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    price = safe_float(payload.get("price"))
                    ts_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
                    self.market_state.update_price("MEXC", mexc_sym, price, ts_ms)
        except Exception as e:
            logger.debug("MEXC Spot price 拉取失败 %s: %s", mexc_sym, e)

        # Spot 无 OI，这里不更新 OI，告警中会显示“未知 / N/A”

    async def poll_mexc_loop(self, symbols: List[str]):
        while True:
            for sym in symbols:
                try:
                    await self._poll_mexc_symbol(sym)
                except Exception as e:
                    logger.debug("MEXC Spot snapshot 异常 %s: %s", sym, e)
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

                    await ws.send(json.dumps({
                        "time": int(time.time()),
                        "channel": "futures.tickers",
                        "event": "subscribe",
                        "payload": valid_symbols,
                    }))

                    async for raw in ws:
                        msg = json.loads(raw)
                        if not isinstance(msg, dict):
                            continue

                        channel = msg.get("channel")
                        event = msg.get("event")

                        if event in {"subscribe", "unsubscribe"}:
                            continue
                        if event == "error":
                            logger.warning("Gate 订阅错误: %s", msg)
                            continue

                        if channel == "futures.trades" and event == "update":
                            result = msg.get("result", [])
                            if not isinstance(result, list):
                                continue

                            for item in result:
                                if not isinstance(item, dict):
                                    continue

                                contract = str(item.get("contract", "")).upper()
                                if not contract:
                                    continue

                                price = safe_float(item.get("price"))
                                size = safe_float(item.get("size"))
                                ts_ms = safe_int(
                                    item.get("create_time_ms") or msg.get("time_ms"),
                                    int(time.time() * 1000),
                                )

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
                            if not isinstance(result, list):
                                continue

                            ts_ms = safe_int(msg.get("time_ms"), int(time.time() * 1000))
                            for item in result:
                                if not isinstance(item, dict):
                                    continue

                                contract = str(item.get("contract", "")).upper()
                                if not contract:
                                    continue

                                price = safe_float(item.get("mark_price") or item.get("last"))
                                self.market_state.update_price("GATE", contract, price, ts_ms)

            except Exception as e:
                logger.exception("Gate 异常: %s", e)
                await asyncio.sleep(5)


# =========================
# MEXC SPOT WS
# =========================
class MexcSpotMonitor:
    def __init__(self, detector: LargeTradeDetector, market_state: MarketState, symbols: List[str], cache: MexcSpotSymbolCache):
        self.detector = detector
        self.market_state = market_state
        self.symbols = symbols
        self.cache = cache

    def _valid_symbols(self) -> List[str]:
        valid = []
        for s in self.symbols:
            mexc_sym = to_mexc_spot_symbol(s)
            if self.cache.is_valid(mexc_sym):
                valid.append(mexc_sym)
            else:
                logger.warning("MEXC Spot 不存在该交易对，已跳过: %s", mexc_sym)
        return valid

    @staticmethod
    def _build_topic(symbol: str) -> str:
        return f"spot@public.aggre.deals.v3.api.pb@{MEXC_SPOT_STREAM_INTERVAL}@{symbol}"

    async def _ping_loop(self, ws):
        while True:
            try:
                await asyncio.sleep(MEXC_SPOT_PING_SEC)
                await ws.send(json.dumps({"method": "PING"}))
            except Exception:
                return

    async def _ttl_loop(self, ws):
        try:
            await asyncio.sleep(MEXC_SPOT_RECONNECT_SEC)
            await ws.close()
        except Exception:
            return

    async def _handle_message(self, raw):
        try:
            if isinstance(raw, bytes):
                # 这里按 text/json 先尝试；如果服务端实际返回 protobuf 二进制帧，
                # 该帧会被跳过并记录 debug 日志。
                try:
                    raw = raw.decode("utf-8")
                except Exception:
                    logger.debug("MEXC Spot 收到二进制帧，当前代码未做 protobuf 解码，已跳过")
                    return

            msg = json.loads(raw)
        except Exception:
            logger.debug("MEXC Spot 消息解析失败")
            return

        if not isinstance(msg, dict):
            return

        # 订阅确认 / pong
        if "code" in msg and "msg" in msg:
            if str(msg.get("msg", "")).upper() == "PONG":
                return
            code = safe_int(msg.get("code"), -1)
            if code == 0:
                logger.debug("MEXC Spot 订阅成功: %s", msg.get("msg"))
            else:
                logger.warning("MEXC Spot 响应异常: %s", msg)
            return

        channel = str(msg.get("channel", ""))
        symbol = str(msg.get("symbol", "")).upper()
        publicdeals = msg.get("publicdeals")

        if not channel.startswith("spot@public.aggre.deals.v3.api.pb@"):
            return
        if not symbol or not isinstance(publicdeals, dict):
            return

        deals_list = publicdeals.get("dealsList", [])
        if not isinstance(deals_list, list):
            return

        for item in deals_list:
            if not isinstance(item, dict):
                continue

            price = safe_float(item.get("price"))
            qty = safe_float(item.get("quantity"))
            ts_ms = safe_int(item.get("time"), int(time.time() * 1000))
            trade_type = safe_int(item.get("tradetype"), 0)
            side = "BUY" if trade_type == 1 else "SELL"
            notional = price * qty

            if price <= 0 or qty <= 0:
                continue

            self.market_state.update_price("MEXC", symbol, price, ts_ms)

            ev = TradeEvent(
                exchange="MEXC",
                symbol=symbol,
                side=side,
                price=price,
                qty=qty,
                raw_qty=qty,
                notional_usdt=notional,
                ts_ms=ts_ms,
            )
            await self.detector.on_trade(ev)

    async def _run_one_connection(self, sub_symbols: List[str], conn_index: int):
        while True:
            try:
                topics = [self._build_topic(s) for s in sub_symbols]

                logger.info(
                    "MEXC Spot 已连接，连接 #%d，有效订阅数: %d",
                    conn_index,
                    len(topics),
                )

                async with websockets.connect(
                    MEXC_SPOT_WS_URL,
                    ping_interval=None,
                    ping_timeout=None,
                    max_size=2**23,
                ) as ws:
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    ttl_task = asyncio.create_task(self._ttl_loop(ws))
                    try:
                        await ws.send(json.dumps({
                            "method": "SUBSCRIPTION",
                            "params": topics,
                        }))

                        async for raw in ws:
                            await self._handle_message(raw)
                    finally:
                        ping_task.cancel()
                        ttl_task.cancel()

            except Exception as e:
                logger.exception("MEXC Spot WS 异常（连接 #%d）: %s", conn_index, e)
                await asyncio.sleep(5)

    async def run(self):
        valid_symbols = self._valid_symbols()
        if not valid_symbols:
            while True:
                logger.warning("MEXC Spot 没有可订阅的有效交易对，60秒后重试")
                await asyncio.sleep(60)

        groups = chunked(valid_symbols, MEXC_SPOT_MAX_SUBS_PER_CONN)
        await asyncio.gather(
            *(self._run_one_connection(group, idx + 1) for idx, group in enumerate(groups))
        )


# =========================
# MAIN
# =========================
async def main():
    notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    await notifier.start()

    market_state = MarketState()

    gate_cache = GateContractCache()
    mexc_cache = MexcSpotSymbolCache()

    await gate_cache.start()
    await mexc_cache.start()

    detector = LargeTradeDetector(notifier, market_state)

    snapshot_poller = SnapshotPoller(
        market_state=market_state,
        gate_cache=gate_cache,
        mexc_cache=mexc_cache,
    )
    await snapshot_poller.start()

    monitors = [
        GateMonitor(detector, market_state, USER_SYMBOLS, gate_cache),
        MexcSpotMonitor(detector, market_state, USER_SYMBOLS, mexc_cache),
    ]

    logger.info("启动监控币种: %s", ", ".join(USER_SYMBOLS))
    logger.info("当前启用交易所: GATE, MEXC-SPOT")
    logger.info("大单阈值: %.0f USDT", SINGLE_TRADE_USDT)
    logger.info("MEXC Spot WS: %s", MEXC_SPOT_WS_URL)

    try:
        await asyncio.gather(
            gate_cache.auto_refresh_loop(),
            mexc_cache.auto_refresh_loop(),
            snapshot_poller.poll_gate_loop(USER_SYMBOLS),
            snapshot_poller.poll_mexc_loop(USER_SYMBOLS),
            *(m.run() for m in monitors),
        )
    finally:
        await notifier.close()
        await snapshot_poller.close()
        await gate_cache.close()
        await mexc_cache.close()


if __name__ == "__main__":
    asyncio.run(main())
