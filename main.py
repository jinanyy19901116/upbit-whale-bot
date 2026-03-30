import asyncio
import contextlib
import json
import logging
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

load_dotenv()

BEIJING_TZ = timezone(timedelta(hours=8))

UPBIT_REGION = os.getenv("UPBIT_REGION", "sg").strip().lower()   # sg / id / th
UPBIT_QUOTE = os.getenv("UPBIT_QUOTE", "USDT").strip().upper()   # USDT / SGD / IDR / THB
UPBIT_WS_URL = f"wss://{UPBIT_REGION}-api.upbit.com/websocket/v1"
UPBIT_REST_BASE = f"https://{UPBIT_REGION}-api.upbit.com"

BYBIT_LINEAR_WS_URL = os.getenv(
    "BYBIT_LINEAR_WS_URL",
    "wss://stream.bybit.com/v5/public/linear",
).strip()

TELEGRAM_BASE = "https://api.telegram.org"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SYMBOLS_RAW = os.getenv(
    "SYMBOLS",
    "XRPUSDT,ANKRUSDT,CHZUSDT,XLMUSDT,STEEMUSDT,"
    "SUIUSDT,WLDUSDT,KITEUSDT,SEIUSDT,AKTUSDT,"
    "CPOOLUSDT,TRUMPUSDT,ANIMEUSDT"
).strip()

# 日志打印频率
LOOP_LOG_SECONDS = int(os.getenv("LOOP_LOG_SECONDS", "30"))

# 信号状态参数
STATE_COOLDOWN_SECONDS = int(os.getenv("STATE_COOLDOWN_SECONDS", "120"))
RESET_AFTER_SECONDS = int(os.getenv("RESET_AFTER_SECONDS", "600"))

# 统计窗口
TRADE_WINDOW_SECONDS = int(os.getenv("TRADE_WINDOW_SECONDS", "8"))
ORDERBOOK_STALE_SECONDS = int(os.getenv("ORDERBOOK_STALE_SECONDS", "5"))

# 大单阈值
SMALL_CAP_BIG_TRADE = float(os.getenv("SMALL_CAP_BIG_TRADE", "80000"))
LARGE_CAP_BIG_TRADE = float(os.getenv("LARGE_CAP_BIG_TRADE", "150000"))

# 成交确认
UPBIT_FOLLOW_MULTIPLIER = float(os.getenv("UPBIT_FOLLOW_MULTIPLIER", "1.2"))
BYBIT_CONFIRM_RATIO = float(os.getenv("BYBIT_CONFIRM_RATIO", "0.6"))
OPPOSITE_MAX_RATIO = float(os.getenv("OPPOSITE_MAX_RATIO", "0.75"))

# 盘口确认
BOOK_WALL_MULTIPLIER = float(os.getenv("BOOK_WALL_MULTIPLIER", "2.5"))
MIN_WALL_NOTIONAL = float(os.getenv("MIN_WALL_NOTIONAL", "50000"))
WALL_NEAR_PCT = float(os.getenv("WALL_NEAR_PCT", "0.003"))         # 离中间价 0.3%
WALL_REMOVE_RATIO = float(os.getenv("WALL_REMOVE_RATIO", "0.55"))  # 低于前值 55% 视为大幅减弱
ASK_WALL_EAT_RATIO = float(os.getenv("ASK_WALL_EAT_RATIO", "0.35"))
BID_WALL_EAT_RATIO = float(os.getenv("BID_WALL_EAT_RATIO", "0.35"))

UPBIT_ORDERBOOK_DEPTH = int(os.getenv("UPBIT_ORDERBOOK_DEPTH", "15"))
BYBIT_ORDERBOOK_DEPTH = int(os.getenv("BYBIT_ORDERBOOK_DEPTH", "50"))


def now_bj() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


def parse_symbols(raw: str) -> List[str]:
    out = []
    for x in raw.split(","):
        s = x.strip().upper()
        if not s:
            continue
        if not s.endswith("USDT"):
            s = f"{s}USDT"
        out.append(s)
    return out


SYMBOLS = parse_symbols(SYMBOLS_RAW)


def symbol_base(symbol: str) -> str:
    if not symbol.endswith("USDT"):
        raise ValueError(f"unsupported symbol: {symbol}")
    return symbol[:-4]


def symbol_to_upbit_market(symbol: str) -> str:
    # XRPUSDT -> USDT-XRP
    return f"{UPBIT_QUOTE}-{symbol_base(symbol)}"


def get_big_trade_threshold(symbol: str) -> float:
    if symbol in {"XRPUSDT", "SUIUSDT", "WLDUSDT"}:
        return LARGE_CAP_BIG_TRADE
    return SMALL_CAP_BIG_TRADE


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


@dataclass
class TradeEvent:
    ts_ms: int
    side: str           # buy / sell
    price: float
    size: float
    value: float
    trade_id: str
    source: str         # upbit / bybit


@dataclass
class OrderBookSnapshot:
    ts_ms: int
    bids: List[Tuple[float, float]]   # [(price, size), ...]
    asks: List[Tuple[float, float]]
    bid_total_size: float = 0.0
    ask_total_size: float = 0.0


class SymbolState:
    def __init__(self):
        self.upbit_trades: Deque[TradeEvent] = deque()
        self.bybit_trades: Deque[TradeEvent] = deque()

        self.upbit_book: Optional[OrderBookSnapshot] = None
        self.prev_upbit_book: Optional[OrderBookSnapshot] = None

        self.bybit_book: Optional[OrderBookSnapshot] = None
        self.prev_bybit_book: Optional[OrderBookSnapshot] = None

        self.last_state = "NONE"   # NONE / LONG_ACTIVE / SHORT_ACTIVE
        self.last_state_change_ts = 0.0
        self.last_trigger_fingerprint = ""
        self.last_log_ts = 0.0


class Bot:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.symbols = SYMBOLS
        self.states: Dict[str, SymbolState] = {s: SymbolState() for s in self.symbols}

        self.symbol_to_upbit = {s: symbol_to_upbit_market(s) for s in self.symbols}
        self.upbit_to_symbol = {v: k for k, v in self.symbol_to_upbit.items()}

        self.live_upbit_markets = set()
        self.active_symbols: List[str] = []

    async def send(self, text: str):
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
                    live.add(market)
            self.live_upbit_markets = live

        active = []
        missing = []
        for symbol in self.symbols:
            market = self.symbol_to_upbit[symbol]
            if market in self.live_upbit_markets:
                active.append(symbol)
            else:
                missing.append(f"{symbol}->{market}")

        self.active_symbols = active

        if missing:
            logging.warning("Upbit 不存在或当前区域不可交易的市场: %s", ", ".join(missing))
        if not self.active_symbols:
            raise RuntimeError("没有可用的 Upbit 市场，请检查 UPBIT_REGION / UPBIT_QUOTE / SYMBOLS")

    def cleanup_deque(self, dq: Deque[TradeEvent], now_ms: int):
        min_ts = now_ms - TRADE_WINDOW_SECONDS * 1000
        while dq and dq[0].ts_ms < min_ts:
            dq.popleft()

    def add_trade(self, symbol: str, trade: TradeEvent):
        st = self.states[symbol]
        if trade.source == "upbit":
            st.upbit_trades.append(trade)
            self.cleanup_deque(st.upbit_trades, trade.ts_ms)
        else:
            st.bybit_trades.append(trade)
            self.cleanup_deque(st.bybit_trades, trade.ts_ms)

    def update_upbit_book(self, symbol: str, snapshot: OrderBookSnapshot):
        st = self.states[symbol]
        st.prev_upbit_book = st.upbit_book
        st.upbit_book = snapshot

    def update_bybit_book(self, symbol: str, snapshot: OrderBookSnapshot):
        st = self.states[symbol]
        st.prev_bybit_book = st.bybit_book
        st.bybit_book = snapshot

    def book_mid(self, book: Optional[OrderBookSnapshot]) -> Optional[float]:
        if not book or not book.bids or not book.asks:
            return None
        return (book.bids[0][0] + book.asks[0][0]) / 2

    def book_is_fresh(self, book: Optional[OrderBookSnapshot], now_ms: int) -> bool:
        return bool(book and now_ms - book.ts_ms <= ORDERBOOK_STALE_SECONDS * 1000)

    def sum_side(self, trades: Deque[TradeEvent], side: str, after_ts_ms: Optional[int] = None) -> Tuple[float, int]:
        total = 0.0
        count = 0
        for t in trades:
            if after_ts_ms is not None and t.ts_ms <= after_ts_ms:
                continue
            if t.side == side:
                total += t.value
                count += 1
        return total, count

    def find_recent_big_trade(self, symbol: str) -> Optional[TradeEvent]:
        threshold = get_big_trade_threshold(symbol)
        st = self.states[symbol]
        candidate = None
        for t in st.upbit_trades:
            if t.value >= threshold:
                candidate = t
        return candidate

    def analyze_books(self, symbol: str, trigger_side: str) -> Dict[str, object]:
        now_ms = int(time.time() * 1000)
        st = self.states[symbol]

        result = {
            "support": 0,
            "reasons": [],
            "upbit_mid": None,
            "bybit_mid": None,
        }

        def inspect_book(name: str, prev_book: Optional[OrderBookSnapshot], book: Optional[OrderBookSnapshot]):
            if not self.book_is_fresh(book, now_ms):
                return

            mid = self.book_mid(book)
            if name == "upbit":
                result["upbit_mid"] = mid
            else:
                result["bybit_mid"] = mid

            if mid is None:
                return

            near_px = mid * WALL_NEAR_PCT
            bids = book.bids[: min(len(book.bids), 10)]
            asks = book.asks[: min(len(book.asks), 10)]

            bid_notional = [px * sz for px, sz in bids]
            ask_notional = [px * sz for px, sz in asks]
            avg_bid = sum(bid_notional) / len(bid_notional) if bid_notional else 0.0
            avg_ask = sum(ask_notional) / len(ask_notional) if ask_notional else 0.0

            if trigger_side == "buy":
                # 1) 买墙增强
                for px, sz in bids:
                    notional = px * sz
                    if (mid - px) <= near_px and notional >= max(MIN_WALL_NOTIONAL, avg_bid * BOOK_WALL_MULTIPLIER):
                        result["support"] += 1
                        result["reasons"].append(f"{name} 买墙增强 {notional:.0f}U")
                        break

                # 2) 卖一被吃
                if prev_book and prev_book.asks and book.asks:
                    prev_best_ask_px, prev_best_ask_sz = prev_book.asks[0]
                    cur_best_ask_px, cur_best_ask_sz = book.asks[0]
                    if abs(prev_best_ask_px - cur_best_ask_px) <= max(prev_best_ask_px * 0.0005, 1e-12):
                        if prev_best_ask_sz > 0 and cur_best_ask_sz <= prev_best_ask_sz * ASK_WALL_EAT_RATIO:
                            result["support"] += 1
                            result["reasons"].append(
                                f"{name} 卖一被吃 {prev_best_ask_sz:.4f}->{cur_best_ask_sz:.4f}"
                            )

                # 3) 近端卖墙明显减弱
                if prev_book:
                    ask_map_prev = {px: sz for px, sz in prev_book.asks[:10]}
                    ask_map_cur = {px: sz for px, sz in book.asks[:10]}
                    for px, prev_sz in ask_map_prev.items():
                        cur_sz = ask_map_cur.get(px, 0.0)
                        if px >= mid and (px - mid) <= near_px and prev_sz > 0 and cur_sz <= prev_sz * WALL_REMOVE_RATIO:
                            if px * prev_sz >= MIN_WALL_NOTIONAL:
                                result["support"] += 1
                                result["reasons"].append(f"{name} 近端卖墙减弱 {px * prev_sz:.0f}U")
                                break

            else:
                # 1) 卖墙增强
                for px, sz in asks:
                    notional = px * sz
                    if (px - mid) <= near_px and notional >= max(MIN_WALL_NOTIONAL, avg_ask * BOOK_WALL_MULTIPLIER):
                        result["support"] += 1
                        result["reasons"].append(f"{name} 卖墙增强 {notional:.0f}U")
                        break

                # 2) 买一被砸
                if prev_book and prev_book.bids and book.bids:
                    prev_best_bid_px, prev_best_bid_sz = prev_book.bids[0]
                    cur_best_bid_px, cur_best_bid_sz = book.bids[0]
                    if abs(prev_best_bid_px - cur_best_bid_px) <= max(prev_best_bid_px * 0.0005, 1e-12):
                        if prev_best_bid_sz > 0 and cur_best_bid_sz <= prev_best_bid_sz * BID_WALL_EAT_RATIO:
                            result["support"] += 1
                            result["reasons"].append(
                                f"{name} 买一被砸 {prev_best_bid_sz:.4f}->{cur_best_bid_sz:.4f}"
                            )

                # 3) 近端买墙明显减弱
                if prev_book:
                    bid_map_prev = {px: sz for px, sz in prev_book.bids[:10]}
                    bid_map_cur = {px: sz for px, sz in book.bids[:10]}
                    for px, prev_sz in bid_map_prev.items():
                        cur_sz = bid_map_cur.get(px, 0.0)
                        if px <= mid and (mid - px) <= near_px and prev_sz > 0 and cur_sz <= prev_sz * WALL_REMOVE_RATIO:
                            if px * prev_sz >= MIN_WALL_NOTIONAL:
                                result["support"] += 1
                                result["reasons"].append(f"{name} 近端买墙减弱 {px * prev_sz:.0f}U")
                                break

        inspect_book("upbit", st.prev_upbit_book, st.upbit_book)
        inspect_book("bybit", st.prev_bybit_book, st.bybit_book)
        return result

    def compute_signal(self, symbol: str) -> Optional[Dict[str, object]]:
        st = self.states[symbol]
        if not st.upbit_trades:
            return None

        trigger = self.find_recent_big_trade(symbol)
        if not trigger:
            return None

        direction = "LONG" if trigger.side == "buy" else "SHORT"

        up_same_value, up_same_count = self.sum_side(st.upbit_trades, trigger.side, after_ts_ms=trigger.ts_ms)
        opposite_side = "sell" if trigger.side == "buy" else "buy"
        up_opp_value, up_opp_count = self.sum_side(st.upbit_trades, opposite_side, after_ts_ms=trigger.ts_ms)

        by_same_value, by_same_count = self.sum_side(st.bybit_trades, trigger.side, after_ts_ms=trigger.ts_ms)
        by_opp_value, by_opp_count = self.sum_side(st.bybit_trades, opposite_side, after_ts_ms=trigger.ts_ms)

        # 1) Upbit 自身必须有后续同向主动成交
        if up_same_value < trigger.value * UPBIT_FOLLOW_MULTIPLIER:
            return None

        # 2) Upbit 反向成交不能太强
        if up_opp_value > up_same_value * OPPOSITE_MAX_RATIO:
            return None

        # 3) Bybit 必须同方向确认
        if by_same_value < trigger.value * BYBIT_CONFIRM_RATIO:
            return None

        # 4) Bybit 反向不能太强
        if by_opp_value > by_same_value * OPPOSITE_MAX_RATIO:
            return None

        # 5) 盘口至少要有一个支持项
        book_analysis = self.analyze_books(symbol, trigger.side)
        if book_analysis["support"] < 1:
            return None

        fingerprint = (
            f"{trigger.trade_id}|{trigger.side}|"
            f"{int(trigger.ts_ms / 1000)}|{int(trigger.value)}"
        )

        return {
            "direction": direction,
            "trigger": trigger,
            "fingerprint": fingerprint,
            "up_same_value": up_same_value,
            "up_same_count": up_same_count,
            "up_opp_value": up_opp_value,
            "up_opp_count": up_opp_count,
            "by_same_value": by_same_value,
            "by_same_count": by_same_count,
            "by_opp_value": by_opp_value,
            "by_opp_count": by_opp_count,
            "book": book_analysis,
        }

    def should_reset_long(self, signal: Optional[Dict[str, object]], last_change_ts: float) -> bool:
        if time.time() - last_change_ts > RESET_AFTER_SECONDS:
            return True
        if not signal:
            return False
        trigger = signal["trigger"]
        if trigger.side == "sell":
            return True
        return False

    def should_reset_short(self, signal: Optional[Dict[str, object]], last_change_ts: float) -> bool:
        if time.time() - last_change_ts > RESET_AFTER_SECONDS:
            return True
        if not signal:
            return False
        trigger = signal["trigger"]
        if trigger.side == "buy":
            return True
        return False

    async def maybe_emit_signal(self, symbol: str):
        st = self.states[symbol]
        signal = self.compute_signal(symbol)
        state = st.last_state

        if state == "LONG_ACTIVE" and self.should_reset_long(signal, st.last_state_change_ts):
            st.last_state = "NONE"
            st.last_state_change_ts = time.time()
            state = "NONE"
            logging.info("%s state reset from LONG_ACTIVE", symbol)

        if state == "SHORT_ACTIVE" and self.should_reset_short(signal, st.last_state_change_ts):
            st.last_state = "NONE"
            st.last_state_change_ts = time.time()
            state = "NONE"
            logging.info("%s state reset from SHORT_ACTIVE", symbol)

        if not signal:
            return

        now_ts = time.time()
        if state == "NONE" and (now_ts - st.last_state_change_ts) < STATE_COOLDOWN_SECONDS:
            return

        if st.last_trigger_fingerprint == signal["fingerprint"]:
            return

        direction = signal["direction"]
        trigger: TradeEvent = signal["trigger"]
        book = signal["book"]

        upbit_market = self.symbol_to_upbit[symbol]
        title = "🟢 做多启动" if direction == "LONG" else "🔴 做空启动"

        lines = [
            f"{title} | {symbol}",
            f"时间: {now_bj()} 北京时间",
            f"Upbit现货: {upbit_market}",
            f"Bybit合约: {symbol}",
            f"Upbit触发方向: {'BUY' if trigger.side == 'buy' else 'SELL'}",
            f"Upbit触发大单: {trigger.value:.0f} USDT",
            f"Upbit触发价格: {trigger.price:.8f}",
            f"Upbit后续同向: {signal['up_same_value']:.0f} USDT / {signal['up_same_count']}笔",
            f"Upbit后续反向: {signal['up_opp_value']:.0f} USDT / {signal['up_opp_count']}笔",
            f"Bybit同向Taker: {signal['by_same_value']:.0f} USDT / {signal['by_same_count']}笔",
            f"Bybit反向Taker: {signal['by_opp_value']:.0f} USDT / {signal['by_opp_count']}笔",
            f"盘口确认: {'；'.join(book['reasons'][:4]) if book['reasons'] else '无'}",
            f"大单阈值: {get_big_trade_threshold(symbol):.0f} USDT",
            f"判断: Upbit现货大额主动成交 + Bybit同向确认 + 盘口结构支持",
        ]

        await self.send("\n".join(lines))

        st.last_state = "LONG_ACTIVE" if direction == "LONG" else "SHORT_ACTIVE"
        st.last_state_change_ts = time.time()
        st.last_trigger_fingerprint = signal["fingerprint"]

        logging.info("SEND | %s", " | ".join(lines))

    async def upbit_ws_loop(self):
        subscribe_codes = [self.symbol_to_upbit[s] for s in self.active_symbols]
        payload = [
            {"ticket": str(uuid.uuid4())},
            {"type": "trade", "codes": subscribe_codes},
            {"type": "orderbook", "codes": subscribe_codes},
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
                    logging.info("Upbit WS connected | region=%s codes=%s", UPBIT_REGION, len(subscribe_codes))

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
                            await self.handle_upbit_message(data)
                        except Exception:
                            logging.exception("handle_upbit_message error")

            except Exception as e:
                logging.exception("Upbit WS error: %s", e)

            await asyncio.sleep(3)

    async def bybit_ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        while True:
            await asyncio.sleep(20)
            await ws.send_json({"op": "ping"})

    async def bybit_ws_loop(self):
        topics = []
        for symbol in self.active_symbols:
            topics.append(f"publicTrade.{symbol}")
            topics.append(f"orderbook.{BYBIT_ORDERBOOK_DEPTH}.{symbol}")

        while True:
            try:
                async with self.session.ws_connect(
                    BYBIT_LINEAR_WS_URL,
                    heartbeat=20,
                    autoclose=True,
                    autoping=True,
                    receive_timeout=60,
                ) as ws:
                    await ws.send_json({"op": "subscribe", "args": topics})
                    logging.info("Bybit WS connected | topics=%s", len(topics))

                    ping_task = asyncio.create_task(self.bybit_ping_loop(ws))
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                data = json.loads(msg.data.decode("utf-8"))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            else:
                                continue

                            if data.get("op") in {"ping", "pong", "subscribe"}:
                                continue
                            if data.get("success") is True:
                                continue

                            try:
                                await self.handle_bybit_message(data)
                            except Exception:
                                logging.exception("handle_bybit_message error")
                    finally:
                        ping_task.cancel()
                        with contextlib.suppress(Exception):
                            await ping_task

            except Exception as e:
                logging.exception("Bybit WS error: %s", e)

            await asyncio.sleep(3)

    async def handle_upbit_message(self, data: dict):
        msg_type = data.get("type") or data.get("ty")
        code = data.get("code") or data.get("cd") or data.get("market")
        if not code:
            return

        symbol = self.upbit_to_symbol.get(code)
        if not symbol:
            return

        if msg_type == "trade":
            side_raw = (data.get("ask_bid") or data.get("ab") or "").upper()
            side = "buy" if side_raw == "BID" else "sell" if side_raw == "ASK" else ""

            price = safe_float(data.get("trade_price", data.get("tp")))
            size = safe_float(data.get("trade_volume", data.get("tv")))
            ts_ms = safe_int(
                data.get("trade_timestamp", data.get("ttms", data.get("tms", time.time() * 1000)))
            )
            trade_id = str(data.get("sequential_id", data.get("sid", ts_ms)))
            value = price * size

            if side and price > 0 and size > 0:
                self.add_trade(symbol, TradeEvent(ts_ms, side, price, size, value, trade_id, "upbit"))
                await self.maybe_emit_signal(symbol)
            return

        if msg_type == "orderbook":
            units = data.get("orderbook_units") or data.get("obu") or []
            bids = []
            asks = []

            for u in units[:UPBIT_ORDERBOOK_DEPTH]:
                bid_px = safe_float(u.get("bid_price", u.get("bp")))
                bid_sz = safe_float(u.get("bid_size", u.get("bs")))
                ask_px = safe_float(u.get("ask_price", u.get("ap")))
                ask_sz = safe_float(u.get("ask_size", u.get("as")))

                if bid_px > 0 and bid_sz >= 0:
                    bids.append((bid_px, bid_sz))
                if ask_px > 0 and ask_sz >= 0:
                    asks.append((ask_px, ask_sz))

            ts_ms = safe_int(data.get("timestamp", data.get("tms", time.time() * 1000)))
            bid_total = safe_float(data.get("total_bid_size", data.get("tbs")))
            ask_total = safe_float(data.get("total_ask_size", data.get("tas")))

            self.update_upbit_book(
                symbol,
                OrderBookSnapshot(
                    ts_ms=ts_ms,
                    bids=bids,
                    asks=asks,
                    bid_total_size=bid_total,
                    ask_total_size=ask_total,
                ),
            )
            await self.maybe_emit_signal(symbol)
            return

    def apply_bybit_delta(self, current: Optional[OrderBookSnapshot], msg_type: str, payload: dict) -> OrderBookSnapshot:
        ts_ms = safe_int(payload.get("ts", time.time() * 1000))

        bids_raw = payload.get("b", [])
        asks_raw = payload.get("a", [])

        if current is None or msg_type == "snapshot":
            bids = [(safe_float(px), safe_float(sz)) for px, sz in bids_raw if safe_float(px) > 0]
            asks = [(safe_float(px), safe_float(sz)) for px, sz in asks_raw if safe_float(px) > 0]
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            bids = bids[:BYBIT_ORDERBOOK_DEPTH]
            asks = asks[:BYBIT_ORDERBOOK_DEPTH]
            return OrderBookSnapshot(ts_ms, bids, asks)

        bid_map = {px: sz for px, sz in current.bids}
        ask_map = {px: sz for px, sz in current.asks}

        for px_raw, sz_raw in bids_raw:
            px = safe_float(px_raw)
            sz = safe_float(sz_raw)
            if sz == 0:
                bid_map.pop(px, None)
            else:
                bid_map[px] = sz

        for px_raw, sz_raw in asks_raw:
            px = safe_float(px_raw)
            sz = safe_float(sz_raw)
            if sz == 0:
                ask_map.pop(px, None)
            else:
                ask_map[px] = sz

        bids = sorted(bid_map.items(), key=lambda x: x[0], reverse=True)[:BYBIT_ORDERBOOK_DEPTH]
        asks = sorted(ask_map.items(), key=lambda x: x[0])[:BYBIT_ORDERBOOK_DEPTH]
        return OrderBookSnapshot(ts_ms, bids, asks)

    async def handle_bybit_message(self, data: dict):
        topic = data.get("topic", "")

        if topic.startswith("publicTrade."):
            rows = data.get("data", [])
            for row in rows:
                symbol = row.get("s")
                if symbol not in self.states:
                    continue

                side_raw = (row.get("S") or "").lower()
                side = "buy" if side_raw == "buy" else "sell" if side_raw == "sell" else ""

                price = safe_float(row.get("p"))
                size = safe_float(row.get("v"))
                ts_ms = safe_int(row.get("T", data.get("ts", time.time() * 1000)))
                trade_id = str(row.get("i", ts_ms))
                value = price * size

                if side and price > 0 and size > 0:
                    self.add_trade(symbol, TradeEvent(ts_ms, side, price, size, value, trade_id, "bybit"))
                    await self.maybe_emit_signal(symbol)
            return

        if topic.startswith("orderbook."):
            payload = data.get("data", {})
            symbol = payload.get("s")
            if symbol not in self.states:
                return

            st = self.states[symbol]
            merged_payload = {
                "ts": data.get("ts", payload.get("ts", int(time.time() * 1000))),
                "b": payload.get("b", []),
                "a": payload.get("a", []),
            }
            msg_type = data.get("type", "snapshot")

            snapshot = self.apply_bybit_delta(st.bybit_book, msg_type, merged_payload)
            self.update_bybit_book(symbol, snapshot)
            await self.maybe_emit_signal(symbol)
            return

    async def status_log_loop(self):
        while True:
            try:
                for symbol in self.active_symbols:
                    st = self.states[symbol]
                    now_ts = time.time()

                    if now_ts - st.last_log_ts < LOOP_LOG_SECONDS:
                        continue
                    st.last_log_ts = now_ts

                    up_buy, up_buy_n = self.sum_side(st.upbit_trades, "buy")
                    up_sell, up_sell_n = self.sum_side(st.upbit_trades, "sell")
                    by_buy, by_buy_n = self.sum_side(st.bybit_trades, "buy")
                    by_sell, by_sell_n = self.sum_side(st.bybit_trades, "sell")

                    logging.info(
                        "%s flow | upbit buy=%.0f(%s) sell=%.0f(%s) | bybit buy=%.0f(%s) sell=%.0f(%s) | state=%s",
                        symbol,
                        up_buy, up_buy_n,
                        up_sell, up_sell_n,
                        by_buy, by_buy_n,
                        by_sell, by_sell_n,
                        st.last_state,
                    )
            except Exception:
                logging.exception("status_log_loop error")

            await asyncio.sleep(5)

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
            "User-Agent": "upbit-bybit-flow-bot/1.0",
            "Accept": "application/json",
        }

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            self.session = session

            await self.load_upbit_markets()

            logging.info("Starting Upbit Spot + Bybit Linear Bot")
            logging.info("UPBIT_REGION=%s UPBIT_QUOTE=%s", UPBIT_REGION, UPBIT_QUOTE)
            logging.info("Configured symbols: %s", self.symbols)
            logging.info("Active symbols: %s", self.active_symbols)

            await self.send(
                f"✅ 机器人启动成功\n"
                f"时间: {now_bj()} 北京时间\n"
                f"模式: Upbit现货 + Bybit合约 + 大额主动成交 + 挂墙确认\n"
                f"Upbit区域: {UPBIT_REGION}\n"
                f"Upbit计价: {UPBIT_QUOTE}\n"
                f"监控币种数量: {len(self.active_symbols)}\n"
                f"监控列表: {', '.join(self.active_symbols)}"
            )

            await asyncio.gather(
                self.upbit_ws_loop(),
                self.bybit_ws_loop(),
                self.status_log_loop(),
            )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    asyncio.run(Bot().run())
