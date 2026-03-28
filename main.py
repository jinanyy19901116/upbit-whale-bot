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

# 大额成交阈值
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

# OKX instruments 刷新间隔
OKX_CTVAL_REFRESH_SEC = int(os.getenv("OKX_CTVAL_REFRESH_SEC", "3600"))

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
        # (exchange, symbol) -> deque[(ts_ms, value)]
        self.price_history: Dict[Tuple[str, str], Deque[Tuple[int, float]]] = defaultdict(deque)
        self.oi_history: Dict[Tuple[str, str], Deque[Tuple[int, float]]] = defaultdict(deque)

    def update_price(self, exchange: str, symbol: str, price: float, ts_ms: int):
        key = (exchange, symbol)
        dq = self.price_history[key]
        dq.append((ts_ms, price))
        self._trim(dq, ts_ms, 5 * 60 * 1000 + 60 * 1000)

    def update_oi(self, exchange: str, symbol: str, oi_usdt: float, ts_ms: int):
        key = (exchange, symbol)
        dq = self.oi_history[key]
        dq.append((ts_ms, oi_usdt))
        self._trim(dq, ts_ms, 5 * 60 * 1000 + 60 * 1000)

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
        key = (exchange, symbol)
        dq = self.price_history.get(key)
        if not dq:
            return None
        latest_ts, latest_price = dq[-1]
        old_price = self._find_value_before(dq, latest_ts - 60 * 1000)
        return pct_change(old_price, latest_price)

    def get_price_change_5m(self, exchange: str, symbol: str) -> Optional[float]:
        key = (exchange, symbol)
        dq = self.price_history.get(key)
        if not dq:
            return None
        latest_ts, latest_price = dq[-1]
        old_price = self._find_value_before(dq, latest_ts - 5 * 60 * 1000)
        return pct_change(old_price, latest_price)

    def get_oi_change_5m(self, exchange: str, symbol: str) -> Optional[float]:
        key = (exchange, symbol)
        dq = self.oi_history.get(key)
        if not dq:
            return None
        latest_ts, latest_oi = dq[-1]
        old_oi = self._find_value_before(dq, latest_ts - 5 * 60 * 1000)
        return pct_change(old_oi, latest_oi)

    def get_price_status(self, exchange: str, symbol: str) -> str:
        ch1 = self.get_price_change_1m(exchange, symbol)
        ch5 = self.get_price_change_5m(exchange, symbol)

        # 优先看 5m，再辅助看 1m
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
# OKX CONTRACT CACHE
# =========================
class OkxContractSpecCache:
    """
    缓存：
    1. instId -> ctVal
    2. 有效 instId 集合
    """
    def __init__(self):
        self.ctval_map: Dict[str, float] = {}
        self.valid_inst_ids: set[str] = set()
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        await self.refresh()

    async def close(self):
        if self.session:
            await self.session.close()

    async def refresh(self):
        url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
        try:
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error("拉取 OKX instruments 失败: %s %s", resp.status, body)
                    return

                payload = await resp.json()
                items = payload.get("data", [])

                ctval_map: Dict[str, float] = {}
                valid_inst_ids: set[str] = set()

                for item in items:
                    inst_id = item.get("instId")
                    ct_val = item.get("ctVal")
                    if not inst_id:
                        continue
                    valid_inst_ids.add(inst_id)

                    try:
                        if ct_val not in (None, ""):
                            ctval_map[inst_id] = float(ct_val)
                    except Exception:
                        pass

                self.valid_inst_ids = valid_inst_ids
                if ctval_map:
                    self.ctval_map = ctval_map

                logger.info(
                    "OKX instruments 已刷新，有效SWAP数量: %d, ctVal数量: %d",
                    len(self.valid_inst_ids),
                    len(self.ctval_map),
                )
        except Exception as e:
            logger.exception("刷新 OKX instruments 异常: %s", e)

    async def auto_refresh_loop(self):
        while True:
            await asyncio.sleep(OKX_CTVAL_REFRESH_SEC)
            try:
                await self.refresh()
            except Exception as e:
                logger.exception("OKX instruments 定时刷新异常: %s", e)

    def get_ctval(self, inst_id: str) -> float:
        return self.ctval_map.get(inst_id, 1.0)

    def is_valid_inst_id(self, inst_id: str) -> bool:
        return inst_id in self.valid_inst_ids

# =========================
# SNAPSHOT FETCHERS
# =========================
class SnapshotPoller:
    def __init__(self, market_state: MarketState, okx_cache: OkxContractSpecCache):
        self.market_state = market_state
        self.okx_cache = okx_cache
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def close(self):
        if self.session:
            await self.session.close()

    # ---------- Binance ----------
    async def _poll_binance_symbol(self, symbol: str):
        # price: premiumIndex
        # oi: /fapi/v1/openInterest
        sym = symbol.upper()
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

                    # 近似转USDT：OI合约数 * 最新价格
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

    # ---------- Bybit ----------
    async def _poll_bybit_symbol(self, symbol: str):
        sym = symbol.upper()
        try:
            async with self.session.get(
                "https://api.bybit.com/v5/market/tickers",
                params={"category": "linear", "symbol": sym},
            ) as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    lst = payload.get("result", {}).get("list", [])
                    if lst:
                        item = lst[0]
                        mark_price = float(item.get("markPrice") or item.get("lastPrice"))
                        ts_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
                        self.market_state.update_price("BYBIT", sym, mark_price, ts_ms)
        except Exception as e:
            logger.debug("Bybit price 拉取失败 %s: %s", sym, e)

        try:
            async with self.session.get(
                "https://api.bybit.com/v5/market/open-interest",
                params={"category": "linear", "symbol": sym, "intervalTime": "5min"},
            ) as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    lst = payload.get("result", {}).get("list", [])
                    if lst:
                        item = lst[0]
                        oi_val = float(item["openInterest"])
                        ts_ms = int(item.get("timestamp", int(datetime.now(tz=timezone.utc).timestamp() * 1000)))

                        price_dq = self.market_state.price_history.get(("BYBIT", sym))
                        if price_dq:
                            last_price = price_dq[-1][1]
                            oi_usdt = oi_val * last_price
                            self.market_state.update_oi("BYBIT", sym, oi_usdt, ts_ms)
        except Exception as e:
            logger.debug("Bybit OI 拉取失败 %s: %s", sym, e)

    async def poll_bybit_loop(self, symbols: List[str]):
        while True:
            for sym in symbols:
                try:
                    await self._poll_bybit_symbol(sym)
                except Exception as e:
                    logger.debug("Bybit snapshot 异常 %s: %s", sym, e)
            await asyncio.sleep(SNAPSHOT_POLL_SEC)

    # ---------- OKX ----------
    async def _poll_okx_symbol(self, symbol: str):
        inst_id = to_okx_inst_id(symbol)
        if not self.okx_cache.is_valid_inst_id(inst_id):
            return

        try:
            async with self.session.get(
                "https://www.okx.com/api/v5/market/ticker",
                params={"instId": inst_id},
            ) as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    lst = payload.get("data", [])
                    if lst:
                        item = lst[0]
                        mark_price = float(item.get("last"))
                        ts_ms = int(item.get("ts", int(datetime.now(tz=timezone.utc).timestamp() * 1000)))
                        self.market_state.update_price("OKX", symbol.upper(), mark_price, ts_ms)
        except Exception as e:
            logger.debug("OKX price 拉取失败 %s: %s", symbol, e)

        try:
            async with self.session.get(
                "https://www.okx.com/api/v5/public/open-interest",
                params={"instType": "SWAP", "instId": inst_id},
            ) as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    lst = payload.get("data", [])
                    if lst:
                        item = lst[0]
                        oi_contracts = float(item.get("oi", 0))
                        ts_ms = int(item.get("ts", int(datetime.now(tz=timezone.utc).timestamp() * 1000)))

                        price_dq = self.market_state.price_history.get(("OKX", symbol.upper()))
                        if price_dq:
                            last_price = price_dq[-1][1]
                            ct_val = self.okx_cache.get_ctval(inst_id)
                            oi_usdt = oi_contracts * ct_val * last_price
                            self.market_state.update_oi("OKX", symbol.upper(), oi_usdt, ts_ms)
        except Exception as e:
            logger.debug("OKX OI 拉取失败 %s: %s", symbol, e)

    async def poll_okx_loop(self, symbols: List[str]):
        while True:
            for sym in symbols:
                try:
                    await self._poll_okx_symbol(sym)
                except Exception as e:
                    logger.debug("OKX snapshot 异常 %s: %s", sym, e)
            await asyncio.sleep(SNAPSHOT_POLL_SEC)

# =========================
# LARGE TRADE DETECTOR
# =========================
class LargeTradeDetector:
    def __init__(self, notifier: TelegramNotifier, market_state: MarketState):
        self.notifier = notifier
        self.market_state = market_state

        self.last_alert_at: Dict[Tuple[str, str, str], float] = {}

        # 同方向刷单过滤
        self.recent_same_side_trades: Dict[Tuple[str, str, str], Deque[TradeEvent]] = defaultdict(deque)

        # 双向流过滤
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
            f"{market_text}\n"
            f"时间：{format_beijing_time(ev.ts_ms)}（北京时间）"
            f"{extra}"
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
    def __init__(self, detector: LargeTradeDetector, market_state: MarketState, symbols: List[str]):
        self.detector = detector
        self.market_state = market_state
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
# BYBIT
# =========================
class BybitMonitor:
    def __init__(self, detector: LargeTradeDetector, market_state: MarketState, symbols: List[str]):
        self.detector = detector
        self.market_state = market_state
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

                            self.market_state.update_price("BYBIT", symbol, price, ts_ms)

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
    def __init__(self, detector: LargeTradeDetector, market_state: MarketState, symbols: List[str], spec_cache: OkxContractSpecCache):
        self.detector = detector
        self.market_state = market_state
        self.symbols = symbols
        self.spec_cache = spec_cache

    async def run(self):
        url = "wss://ws.okx.com:8443/ws/v5/public"

        while True:
            try:
                valid_args = []
                invalid_symbols = []

                for sym in self.symbols:
                    inst_id = to_okx_inst_id(sym)
                    if self.spec_cache.is_valid_inst_id(inst_id):
                        valid_args.append({"channel": "trades", "instId": inst_id})
                    else:
                        invalid_symbols.append((sym, inst_id))

                for sym, inst_id in invalid_symbols:
                    logger.warning("OKX 不存在该永续合约，已跳过: %s -> %s", sym, inst_id)

                if not valid_args:
                    logger.warning("OKX 没有可订阅的有效合约，60秒后重试")
                    await asyncio.sleep(60)
                    continue

                logger.info("OKX 已连接，有效订阅数: %d", len(valid_args))

                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2**23
                ) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": valid_args}))

                    async for raw in ws:
                        msg = json.loads(raw)

                        if msg.get("event") in {"subscribe", "unsubscribe"}:
                            continue

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

                            qty_coin = contracts * ct_val
                            notional = price * qty_coin

                            side = item["side"].upper()
                            ts_ms = int(item["ts"])

                            self.market_state.update_price("OKX", symbol, price, ts_ms)

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

    market_state = MarketState()

    okx_cache = OkxContractSpecCache()
    await okx_cache.start()

    detector = LargeTradeDetector(notifier, market_state)

    snapshot_poller = SnapshotPoller(market_state, okx_cache)
    await snapshot_poller.start()

    monitors = [
        BinanceMonitor(detector, market_state, USER_SYMBOLS),
        BybitMonitor(detector, market_state, USER_SYMBOLS),
        OkxMonitor(detector, market_state, USER_SYMBOLS, okx_cache),
    ]

    logger.info("启动监控币种: %s", ", ".join(USER_SYMBOLS))

    try:
        await asyncio.gather(
            okx_cache.auto_refresh_loop(),
            snapshot_poller.poll_binance_loop(USER_SYMBOLS),
            snapshot_poller.poll_bybit_loop(USER_SYMBOLS),
            snapshot_poller.poll_okx_loop(USER_SYMBOLS),
            *(m.run() for m in monitors),
        )
    finally:
        await notifier.close()
        await snapshot_poller.close()
        await okx_cache.close()

if __name__ == "__main__":
    asyncio.run(main())
