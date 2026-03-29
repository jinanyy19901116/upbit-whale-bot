# -*- coding: utf-8 -*-
"""
WebSocket 关键信号过滤版
- Gate + MEXC
- 监控 SIREN + 相似 30 个币
- 规避套利机器人 / 对冲 / 做市噪音
- Telegram 推送关键 LONG / SHORT

依赖:
  pip install aiohttp websockets protobuf

环境变量:
  TELEGRAM_BOT_TOKEN=xxx
  TELEGRAM_CHAT_ID=xxx
"""

import os
import json
import time
import math
import asyncio
from dataclasses import dataclass, field
from collections import deque
from typing import Dict, List, Tuple, Optional

import aiohttp
import websockets

# ======= MEXC protobuf（需你自行生成）=======
try:
    import PushDataV3ApiWrapper_pb2
    import PublicAggreDealsV3Api_pb2
    import PublicAggreDepthsV3Api_pb2
    MEXC_PROTO_READY = True
except Exception:
    MEXC_PROTO_READY = False

# ============================================
# 配置
# ============================================

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

GATE_REST = "https://api.gateio.ws/api/v4"
GATE_WS = "wss://api.gateio.ws/ws/v4/"

MEXC_REST = "https://api.mexc.com"
MEXC_WS = "wss://wbs-api.mexc.com/ws"

SEED_GATE = "SIREN_USDT"
SEED_MEXC = "SIRENUSDT"

SIMILAR_COUNT = 30
TOP_N_ORDERBOOK = 20
COOLDOWN_SEC = 1800
CANDIDATE_EXPIRE_SEC = 12
MEXC_MAX_SUBS_PER_CONN = 30

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)

# ============================================
# 数据结构
# ============================================

@dataclass
class TradeTick:
    ts: float
    price: float
    amount: float
    side: str   # buy / sell


@dataclass
class BookState:
    bids: List[Tuple[float, float]] = field(default_factory=list)
    asks: List[Tuple[float, float]] = field(default_factory=list)
    ts: float = 0.0


@dataclass
class CandidateSignal:
    direction: str                 # LONG / SHORT
    created_ts: float
    initial_score: float
    reasons: List[str] = field(default_factory=list)


@dataclass
class SymbolState:
    exchange: str
    symbol: str
    trades: deque = field(default_factory=lambda: deque(maxlen=8000))
    prices: deque = field(default_factory=lambda: deque(maxlen=1200))  # (ts, price)
    book: BookState = field(default_factory=BookState)
    last_signal_ts: Dict[str, float] = field(default_factory=dict)
    direction_marks: deque = field(default_factory=lambda: deque(maxlen=40))  # (ts, dir_int)
    candidate: Optional[CandidateSignal] = None


# ============================================
# 工具函数
# ============================================

def now() -> float:
    return time.time()


def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def pct(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return (b - a) / a


def mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def chunked(items, n):
    for i in range(0, len(items), n):
        yield items[i:i + n]


async def tg_send(session: aiohttp.ClientSession, text: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        print(text)
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        async with session.post(url, json=payload) as r:
            if r.status != 200:
                print("telegram fail:", r.status, await r.text())
    except Exception as e:
        print("telegram exception:", e)


# ============================================
# REST：选相似币
# ============================================

async def gate_pick_similar(session: aiohttp.ClientSession) -> List[str]:
    url = f"{GATE_REST}/spot/tickers"
    async with session.get(url) as r:
        data = await r.json()

    picks = []
    for t in data:
        sym = t.get("currency_pair", "")
        if not sym.endswith("_USDT"):
            continue

        last = safe_float(t.get("last"))
        qv = safe_float(t.get("quote_volume"))
        chg = abs(safe_float(t.get("change_percentage")) / 100.0)

        if last <= 0 or last > 5:
            continue
        if qv < 100_000 or qv > 80_000_000:
            continue
        if chg < 0.015:
            continue

        score = 0.0
        if last < 1:
            score += 3
        if last < 0.2:
            score += 3
        if 300_000 <= qv <= 8_000_000:
            score += 1.5
        score += clamp(chg * 20, 0, 4)

        picks.append((score, sym))

    picks.sort(reverse=True)
    result = [s for _, s in picks[:SIMILAR_COUNT]]
    if SEED_GATE in result:
        result.remove(SEED_GATE)
    return [SEED_GATE] + result[:SIMILAR_COUNT]


async def mexc_pick_similar(session: aiohttp.ClientSession) -> List[str]:
    url = f"{MEXC_REST}/api/v3/ticker/24hr"
    async with session.get(url) as r:
        data = await r.json()

    picks = []
    for t in data:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue

        last = safe_float(t.get("lastPrice"))
        qv = safe_float(t.get("quoteVolume"))
        chg = abs(safe_float(t.get("priceChangePercent")))

        if last <= 0 or last > 5:
            continue
        if qv < 100_000 or qv > 80_000_000:
            continue
        if chg < 0.015:
            continue

        score = 0.0
        if last < 1:
            score += 3
        if last < 0.2:
            score += 3
        if 300_000 <= qv <= 8_000_000:
            score += 1.5
        score += clamp(chg * 20, 0, 4)

        picks.append((score, sym))

    picks.sort(reverse=True)
    result = [s for _, s in picks[:SIMILAR_COUNT]]
    if SEED_MEXC in result:
        result.remove(SEED_MEXC)
    return [SEED_MEXC] + result[:SIMILAR_COUNT]


# ============================================
# 核心信号引擎
# ============================================

class FilteredSignalEngine:
    def __init__(self):
        self.states: Dict[str, SymbolState] = {}

    def ensure(self, exchange: str, symbol: str) -> SymbolState:
        key = f"{exchange}:{symbol}"
        if key not in self.states:
            self.states[key] = SymbolState(exchange=exchange, symbol=symbol)
        return self.states[key]

    def on_trade(self, exchange: str, symbol: str, price: float, amount: float, side: str, ts: float):
        st = self.ensure(exchange, symbol)
        st.trades.append(TradeTick(ts=ts, price=price, amount=amount, side=side))
        st.prices.append((ts, price))
        # 方向打点
        dir_mark = 1 if side == "buy" else -1
        st.direction_marks.append((ts, dir_mark))

    def on_book(self, exchange: str, symbol: str, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], ts: float):
        st = self.ensure(exchange, symbol)
        st.book = BookState(
            bids=bids[:TOP_N_ORDERBOOK],
            asks=asks[:TOP_N_ORDERBOOK],
            ts=ts
        )

    # -------------------------
    # 窗口数据
    # -------------------------

    def _recent_trades(self, st: SymbolState, sec: int) -> List[TradeTick]:
        cutoff = now() - sec
        return [x for x in st.trades if x.ts >= cutoff]

    def _recent_prices(self, st: SymbolState, sec: int) -> List[Tuple[float, float]]:
        cutoff = now() - sec
        return [x for x in st.prices if x[0] >= cutoff]

    def _recent_dirs(self, st: SymbolState, sec: int) -> List[int]:
        cutoff = now() - sec
        return [d for ts, d in st.direction_marks if ts >= cutoff]

    # -------------------------
    # 基础特征
    # -------------------------

    def _book_imbalance(self, st: SymbolState) -> float:
        bids = sum(p * q for p, q in st.book.bids)
        asks = sum(p * q for p, q in st.book.asks)
        total = bids + asks
        if total <= 0:
            return 0.0
        return (bids - asks) / total

    def _trade_delta(self, trades: List[TradeTick]) -> float:
        buy_amt = sum(t.price * t.amount for t in trades if t.side == "buy")
        sell_amt = sum(t.price * t.amount for t in trades if t.side == "sell")
        total = buy_amt + sell_amt
        if total <= 0:
            return 0.0
        return (buy_amt - sell_amt) / total

    def _notional(self, trades: List[TradeTick]) -> float:
        return sum(t.price * t.amount for t in trades)

    def _momentum(self, st: SymbolState, sec: int) -> float:
        ps = self._recent_prices(st, sec)
        if len(ps) < 2:
            return 0.0
        return pct(ps[0][1], ps[-1][1])

    def _breakout(self, st: SymbolState, sec: int = 120) -> float:
        ps = self._recent_prices(st, sec)
        if len(ps) < 6:
            return 0.0
        prev_high = max(p for _, p in ps[:-1])
        last = ps[-1][1]
        return pct(prev_high, last) if last > prev_high else 0.0

    def _breakdown(self, st: SymbolState, sec: int = 120) -> float:
        ps = self._recent_prices(st, sec)
        if len(ps) < 6:
            return 0.0
        prev_low = min(p for _, p in ps[:-1])
        last = ps[-1][1]
        return pct(last, prev_low) if last < prev_low else 0.0

    def _vol_surge(self, st: SymbolState) -> float:
        # 10秒成交额 vs 前60秒平均每10秒成交额
        w10 = self._recent_trades(st, 10)
        w60 = self._recent_trades(st, 60)
        v10 = self._notional(w10)
        v60 = self._notional(w60)
        base = v60 / 6 if v60 > 0 else 0
        if base <= 0:
            return 0.0
        return v10 / base

    def _retrace_ratio(self, prices: List[Tuple[float, float]]) -> float:
        if len(prices) < 3:
            return 0.0
        vals = [p for _, p in prices]
        start = vals[0]
        end = vals[-1]
        hi = max(vals)
        lo = min(vals)

        # 上涨后的回吐
        if end >= start:
            impulse = hi - start
            if impulse <= 0:
                return 0.0
            pullback = hi - end
            return pullback / impulse

        # 下跌后的拉回
        impulse = start - lo
        if impulse <= 0:
            return 0.0
        pullback = end - lo
        return pullback / impulse

    def _direction_stability(self, st: SymbolState, sec: int = 12) -> float:
        dirs = self._recent_dirs(st, sec)
        if not dirs:
            return 0.0
        s = sum(dirs)
        return s / len(dirs)  # [-1, 1]

    # -------------------------
    # 噪音过滤
    # -------------------------

    def _noise_filter(self, st: SymbolState, ob_imb: float, breakout: float, breakdown: float) -> Tuple[bool, str]:
        trades_10 = self._recent_trades(st, 10)
        trades_30 = self._recent_trades(st, 30)
        prices_10 = self._recent_prices(st, 10)
        prices_30 = self._recent_prices(st, 30)

        td10 = self._trade_delta(trades_10)
        td30 = self._trade_delta(trades_30)
        pm10 = self._momentum(st, 10)
        pm30 = self._momentum(st, 30)
        retrace10 = self._retrace_ratio(prices_10)
        notional_10 = self._notional(trades_10)
        dir_stab = self._direction_stability(st, 12)

        # 1) 双向过于均衡：像套利/对冲
        if abs(td10) < 0.12 and abs(td30) < 0.10:
            return False, "双向成交过于均衡"

        # 2) 有量无价：有成交没位移
        if notional_10 > 0 and abs(pm10) < 0.002 and abs(td10) < 0.18:
            return False, "有量无价"

        # 3) 盘口和主动成交冲突
        if sign(ob_imb) != 0 and sign(td10) != 0 and sign(ob_imb) != sign(td10):
            return False, "盘口与成交方向冲突"

        # 4) 回吐/拉回过深
        if retrace10 > 0.55:
            return False, "回吐过深"

        # 5) 买盘在推但没有突破
        if td10 > 0 and pm10 > 0 and breakout <= 0:
            return False, "买盘未形成有效突破"

        # 6) 卖盘在砸但没有跌破
        if td10 < 0 and pm10 < 0 and breakdown <= 0:
            return False, "卖盘未形成有效跌破"

        # 7) 方向不稳定：多空翻来翻去
        if abs(dir_stab) < 0.15:
            return False, "方向稳定度不足"

        # 8) 10秒方向与30秒方向冲突
        if sign(td10) != 0 and sign(td30) != 0 and sign(td10) != sign(td30):
            return False, "短长窗口方向冲突"

        # 9) 30秒也没实质位移
        if abs(pm30) < 0.003:
            return False, "30秒位移不足"

        return True, "OK"

    # -------------------------
    # 初筛评分
    # -------------------------

    def _pre_score(self, st: SymbolState) -> Optional[Tuple[str, float, List[str]]]:
        if len(st.trades) < 30 or len(st.prices) < 30 or not st.book.bids or not st.book.asks:
            return None

        vol_surge = self._vol_surge(st)
        mom10 = self._momentum(st, 10)
        mom30 = self._momentum(st, 30)
        breakout = self._breakout(st, 120)
        breakdown = self._breakdown(st, 120)
        ob = self._book_imbalance(st)
        td10 = self._trade_delta(self._recent_trades(st, 10))
        td30 = self._trade_delta(self._recent_trades(st, 30))
        dir_stab = self._direction_stability(st, 12)

        ok, reason = self._noise_filter(st, ob, breakout, breakdown)
        if not ok:
            return None

        long_score = 0.0
        long_reasons = []

        if vol_surge > 2.2:
            long_score += 18
            long_reasons.append(f"放量 {vol_surge:.2f}x")
        if td10 > 0.22:
            long_score += 18
            long_reasons.append(f"10s主动买入 {td10:.2f}")
        if td30 > 0.16:
            long_score += 14
            long_reasons.append(f"30s主动买入 {td30:.2f}")
        if mom10 > 0.003:
            long_score += 10
            long_reasons.append(f"10s位移 {mom10*100:.2f}%")
        if mom30 > 0.006:
            long_score += 10
            long_reasons.append(f"30s位移 {mom30*100:.2f}%")
        if ob > 0.12:
            long_score += 12
            long_reasons.append(f"买盘失衡 {ob:.2f}")
        if breakout > 0.0015:
            long_score += 14
            long_reasons.append(f"有效突破 {breakout*100:.2f}%")
        if dir_stab > 0.22:
            long_score += 8
            long_reasons.append(f"方向稳定 {dir_stab:.2f}")

        short_score = 0.0
        short_reasons = []

        if vol_surge > 2.2:
            short_score += 18
            short_reasons.append(f"放量 {vol_surge:.2f}x")
        if td10 < -0.22:
            short_score += 18
            short_reasons.append(f"10s主动卖出 {td10:.2f}")
        if td30 < -0.16:
            short_score += 14
            short_reasons.append(f"30s主动卖出 {td30:.2f}")
        if mom10 < -0.003:
            short_score += 10
            short_reasons.append(f"10s位移 {mom10*100:.2f}%")
        if mom30 < -0.006:
            short_score += 10
            short_reasons.append(f"30s位移 {mom30*100:.2f}%")
        if ob < -0.12:
            short_score += 12
            short_reasons.append(f"卖盘失衡 {ob:.2f}")
        if breakdown > 0.0015:
            short_score += 14
            short_reasons.append(f"有效跌破 {breakdown*100:.2f}%")
        if dir_stab < -0.22:
            short_score += 8
            short_reasons.append(f"方向稳定 {dir_stab:.2f}")

        if long_score >= 60 and long_score > short_score + 10:
            return "LONG", long_score, long_reasons[:6]
        if short_score >= 60 and short_score > long_score + 10:
            return "SHORT", short_score, short_reasons[:6]

        return None

    # -------------------------
    # 二段确认
    # -------------------------

    def _confirm_candidate(self, st: SymbolState) -> Optional[Tuple[str, float, List[str], float]]:
        cand = st.candidate
        if not cand:
            return None

        age = now() - cand.created_ts
        if age > CANDIDATE_EXPIRE_SEC:
            st.candidate = None
            return None

        if len(st.prices) < 10 or len(st.trades) < 10:
            return None

        last_price = st.prices[-1][1]
        prices_8 = self._recent_prices(st, 8)
        trades_8 = self._recent_trades(st, 8)
        trades_15 = self._recent_trades(st, 15)

        if len(prices_8) < 3 or len(trades_8) < 5:
            return None

        move_8 = self._momentum(st, 8)
        td8 = self._trade_delta(trades_8)
        td15 = self._trade_delta(trades_15)
        ob = self._book_imbalance(st)
        retrace_8 = self._retrace_ratio(prices_8)
        dir_stab = self._direction_stability(st, 8)
        breakout = self._breakout(st, 120)
        breakdown = self._breakdown(st, 120)

        score = cand.initial_score
        reasons = list(cand.reasons)

        if cand.direction == "LONG":
            if td8 > 0.18:
                score += 8
                reasons.append(f"确认买入延续 {td8:.2f}")
            if td15 > 0.14:
                score += 6
                reasons.append("15s资金持续偏多")
            if move_8 > 0.0025:
                score += 8
                reasons.append(f"确认位移 {move_8*100:.2f}%")
            if ob > 0.10:
                score += 6
                reasons.append(f"盘口继续偏多 {ob:.2f}")
            if dir_stab > 0.18:
                score += 4
                reasons.append("方向持续")
            if breakout > 0.001:
                score += 5

            # 否决项
            if retrace_8 > 0.45:
                st.candidate = None
                return None
            if td8 < 0 or move_8 <= 0:
                return None

            if score >= 74:
                last_sent = st.last_signal_ts.get("LONG", 0)
                if now() - last_sent >= COOLDOWN_SEC:
                    st.last_signal_ts["LONG"] = now()
                    st.candidate = None
                    return "LONG", score, reasons[:6], last_price

        if cand.direction == "SHORT":
            if td8 < -0.18:
                score += 8
                reasons.append(f"确认卖出延续 {td8:.2f}")
            if td15 < -0.14:
                score += 6
                reasons.append("15s资金持续偏空")
            if move_8 < -0.0025:
                score += 8
                reasons.append(f"确认位移 {move_8*100:.2f}%")
            if ob < -0.10:
                score += 6
                reasons.append(f"盘口继续偏空 {ob:.2f}")
            if dir_stab < -0.18:
                score += 4
                reasons.append("方向持续")
            if breakdown > 0.001:
                score += 5

            # 否决项
            if retrace_8 > 0.45:
                st.candidate = None
                return None
            if td8 > 0 or move_8 >= 0:
                return None

            if score >= 74:
                last_sent = st.last_signal_ts.get("SHORT", 0)
                if now() - last_sent >= COOLDOWN_SEC:
                    st.last_signal_ts["SHORT"] = now()
                    st.candidate = None
                    return "SHORT", score, reasons[:6], last_price

        return None

    # -------------------------
    # 主评估
    # -------------------------

    def evaluate(self, exchange: str, symbol: str) -> Optional[Tuple[str, float, List[str], float]]:
        st = self.ensure(exchange, symbol)

        # 先尝试确认已有 candidate
        confirmed = self._confirm_candidate(st)
        if confirmed:
            return confirmed

        # 没有 candidate，则做初筛
        pre = self._pre_score(st)
        if not pre:
            return None

        direction, score, reasons = pre

        # 建立候选，不立刻发
        if st.candidate is None:
            st.candidate = CandidateSignal(
                direction=direction,
                created_ts=now(),
                initial_score=score,
                reasons=reasons,
            )

        return None


# ============================================
# Gate WS
# ============================================

class GateWsClient:
    def __init__(self, symbols: List[str], engine: FilteredSignalEngine, tg_session: aiohttp.ClientSession):
        self.symbols = symbols
        self.engine = engine
        self.tg = tg_session

    async def run(self):
        while True:
            try:
                async with websockets.connect(
                    GATE_WS,
                    ping_interval=15,
                    ping_timeout=10,
                    max_size=2**23
                ) as ws:
                    ts = int(time.time())

                    await ws.send(json.dumps({
                        "time": ts,
                        "channel": "spot.trades",
                        "event": "subscribe",
                        "payload": self.symbols
                    }))

                    for sym in self.symbols:
                        await ws.send(json.dumps({
                            "time": ts,
                            "channel": "spot.order_book",
                            "event": "subscribe",
                            "payload": [sym, "20", "100ms"]
                        }))

                    await tg_send(self.tg, f"✅ *Gate WS 已连接*\n监控数: `{len(self.symbols)}`")

                    async for raw in ws:
                        msg = json.loads(raw)
                        channel = msg.get("channel")
                        event = msg.get("event")
                        result = msg.get("result")

                        if event == "subscribe":
                            continue

                        if channel == "spot.trades" and event == "update":
                            if isinstance(result, list):
                                for x in result:
                                    await self._handle_trade(x)
                            elif isinstance(result, dict):
                                await self._handle_trade(result)

                        elif channel == "spot.order_book" and event == "update" and isinstance(result, dict):
                            await self._handle_book(result)

            except Exception as e:
                await tg_send(self.tg, f"⚠️ *Gate WS 断开，重连中*\n`{type(e).__name__}: {e}`")
                await asyncio.sleep(3)

    async def _handle_trade(self, x: dict):
        sym = x.get("currency_pair")
        price = safe_float(x.get("price"))
        amount = safe_float(x.get("amount"))
        side = "buy" if x.get("side") == "buy" else "sell"
        ts = safe_float(x.get("create_time_ms")) / 1000 or now()

        if not sym or price <= 0 or amount <= 0:
            return

        self.engine.on_trade("GATE", sym, price, amount, side, ts)
        await self._check_signal("GATE", sym)

    async def _handle_book(self, result: dict):
        sym = result.get("s") or result.get("currency_pair")
        bids = [(safe_float(x[0]), safe_float(x[1])) for x in result.get("bids", [])]
        asks = [(safe_float(x[0]), safe_float(x[1])) for x in result.get("asks", [])]
        ts = safe_float(result.get("t")) / 1000 or now()

        if not sym:
            return

        self.engine.on_book("GATE", sym, bids, asks, ts)
        await self._check_signal("GATE", sym)

    async def _check_signal(self, exchange: str, symbol: str):
        sig = self.engine.evaluate(exchange, symbol)
        if not sig:
            return

        side, score, reasons, price = sig
        emoji = "🟢" if side == "LONG" else "🔴"

        text = (
            f"{emoji} *关键{side}信号*\n"
            f"*交易所*: `{exchange}`\n"
            f"*币种*: `{symbol}`\n"
            f"*价格*: `{price:.10g}`\n"
            f"*分数*: `{score:.1f}`\n"
            f"*原因*:\n" +
            "\n".join(f"- {r}" for r in reasons) +
            "\n\n_已过滤套利/对冲/噪音，仍建议人工复核。_"
        )
        await tg_send(self.tg, text)


# ============================================
# MEXC WS
# ============================================

class MexcWsClient:
    def __init__(self, symbols: List[str], engine: FilteredSignalEngine, tg_session: aiohttp.ClientSession):
        self.symbols = symbols
        self.engine = engine
        self.tg = tg_session

    async def run(self):
        if not MEXC_PROTO_READY:
            await tg_send(
                self.tg,
                "⚠️ *MEXC WS 未启动*\n缺少 protobuf 生成文件，请先生成 `.proto` 对应 Python 模块。"
            )
            return

        tasks = []
        # 每个 symbol 两个订阅：deals + depth
        # 1连接最多30订阅，所以每批最多15个 symbol
        for batch in chunked(self.symbols, 15):
            tasks.append(asyncio.create_task(self._run_one_conn(batch)))

        await asyncio.gather(*tasks)

    async def _run_one_conn(self, symbols_batch: List[str]):
        while True:
            try:
                async with websockets.connect(
                    MEXC_WS,
                    ping_interval=None,
                    ping_timeout=None,
                    max_size=2**24
                ) as ws:
                    params = []
                    for sym in symbols_batch:
                        params.append(f"spot@public.aggre.deals.v3.api.pb@100ms@{sym}")
                        params.append(f"spot@public.aggre.depth.v3.api.pb@100ms@{sym}")

                    await ws.send(json.dumps({
                        "method": "SUBSCRIPTION",
                        "params": params
                    }))

                    await tg_send(self.tg, f"✅ *MEXC WS 已连接*\n本连接监控: `{len(symbols_batch)}`")

                    async def pinger():
                        while True:
                            try:
                                await ws.send(json.dumps({"method": "PING"}))
                            except Exception:
                                return
                            await asyncio.sleep(20)

                    ping_task = asyncio.create_task(pinger())

                    try:
                        async for raw in ws:
                            if isinstance(raw, str):
                                continue
                            await self._handle_binary(raw)
                    finally:
                        ping_task.cancel()

            except Exception as e:
                await tg_send(self.tg, f"⚠️ *MEXC WS 断开，重连中*\n`{type(e).__name__}: {e}`")
                await asyncio.sleep(3)

    async def _handle_binary(self, raw: bytes):
        wrapper = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
        wrapper.ParseFromString(raw)

        channel = wrapper.channel
        symbol = wrapper.symbol

        if "aggre.deals" in channel:
            deal_msg = PublicAggreDealsV3Api_pb2.PublicAggreDealsV3Api()
            deal_msg.ParseFromString(wrapper.data)

            for d in deal_msg.deals:
                price = safe_float(getattr(d, "p", 0) or getattr(d, "price", 0))
                qty = safe_float(getattr(d, "v", 0) or getattr(d, "quantity", 0))
                side_raw = getattr(d, "S", "") or getattr(d, "side", "")
                side = "buy" if str(side_raw).lower() in ("1", "buy", "bid") else "sell"
                ts = safe_float(getattr(d, "t", 0) or getattr(d, "time", 0)) / 1000 or now()

                if price > 0 and qty > 0:
                    self.engine.on_trade("MEXC", symbol, price, qty, side, ts)
                    await self._check_signal("MEXC", symbol)

        elif "aggre.depth" in channel:
            depth_msg = PublicAggreDepthsV3Api_pb2.PublicAggreDepthsV3Api()
            depth_msg.ParseFromString(wrapper.data)

            bids = []
            asks = []

            for x in depth_msg.bids[:TOP_N_ORDERBOOK]:
                price = safe_float(getattr(x, "p", 0) or getattr(x, "price", 0))
                qty = safe_float(getattr(x, "v", 0) or getattr(x, "quantity", 0))
                bids.append((price, qty))

            for x in depth_msg.asks[:TOP_N_ORDERBOOK]:
                price = safe_float(getattr(x, "p", 0) or getattr(x, "price", 0))
                qty = safe_float(getattr(x, "v", 0) or getattr(x, "quantity", 0))
                asks.append((price, qty))

            ts = safe_float(getattr(depth_msg, "sendTime", 0) or getattr(depth_msg, "t", 0)) / 1000 or now()
            self.engine.on_book("MEXC", symbol, bids, asks, ts)
            await self._check_signal("MEXC", symbol)

    async def _check_signal(self, exchange: str, symbol: str):
        sig = self.engine.evaluate(exchange, symbol)
        if not sig:
            return

        side, score, reasons, price = sig
        emoji = "🟢" if side == "LONG" else "🔴"

        text = (
            f"{emoji} *关键{side}信号*\n"
            f"*交易所*: `{exchange}`\n"
            f"*币种*: `{symbol}`\n"
            f"*价格*: `{price:.10g}`\n"
            f"*分数*: `{score:.1f}`\n"
            f"*原因*:\n" +
            "\n".join(f"- {r}" for r in reasons) +
            "\n\n_已过滤套利/对冲/噪音，仍建议人工复核。_"
        )
        await tg_send(self.tg, text)


# ============================================
# 主程序
# ============================================

async def main():
    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:
        gate_symbols, mexc_symbols = await asyncio.gather(
            gate_pick_similar(session),
            mexc_pick_similar(session),
        )

        engine = FilteredSignalEngine()

        await tg_send(
            session,
            "🚀 *关键信号监控启动*\n"
            f"Gate: `{len(gate_symbols)}` 个\n"
            f"MEXC: `{len(mexc_symbols)}` 个\n"
            "策略: `过滤套利/对冲/做市噪音，只发关键方向信号`"
        )

        gate_client = GateWsClient(gate_symbols, engine, session)
        mexc_client = MexcWsClient(mexc_symbols, engine, session)

        await asyncio.gather(
            gate_client.run(),
            mexc_client.run(),
        )


if __name__ == "__main__":
    asyncio.run(main())
