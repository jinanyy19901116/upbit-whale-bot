import os
import json
import time
import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import httpx
import websockets
from fastapi import FastAPI

# =========================================================
# 基础
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger("main")

app = FastAPI(title="Flow Signal Scanner WS", version="4.0.0")

# =========================================================
# 环境变量
# =========================================================

PORT = int(os.getenv("PORT", "8080"))
AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() in ("1", "true", "yes", "on")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# Upbit 区域
UPBIT_REGION = os.getenv("UPBIT_REGION", "sg").strip().lower()
UPBIT_WS_URL = f"wss://{UPBIT_REGION}-api.upbit.com/websocket/v1"
UPBIT_REST_BASE = f"https://{UPBIT_REGION}-api.upbit.com"

# 交易所开关
ENABLE_GATE = os.getenv("ENABLE_GATE", "true").lower() in ("1", "true", "yes", "on")
ENABLE_MEXC = os.getenv("ENABLE_MEXC", "true").lower() in ("1", "true", "yes", "on")
ENABLE_UPBIT = os.getenv("ENABLE_UPBIT", "true").lower() in ("1", "true", "yes", "on")

# 风控 / 信号参数
LARGE_TRADE_USDT = float(os.getenv("LARGE_TRADE_USDT", "20000"))
RECENT_WINDOW_MINUTES = int(os.getenv("RECENT_WINDOW_MINUTES", "3"))
BASELINE_WINDOW_MINUTES = int(os.getenv("BASELINE_WINDOW_MINUTES", "15"))
CACHE_KEEP_MINUTES = int(os.getenv("CACHE_KEEP_MINUTES", "30"))

MIN_RECENT_NOTIONAL_USDT = float(os.getenv("MIN_RECENT_NOTIONAL_USDT", "50000"))
ACTIVITY_RATIO_THRESHOLD = float(os.getenv("ACTIVITY_RATIO_THRESHOLD", "2.5"))
BUY_SELL_IMBALANCE_THRESHOLD = float(os.getenv("BUY_SELL_IMBALANCE_THRESHOLD", "1.8"))
LARGE_ORDER_IMBALANCE_THRESHOLD = float(os.getenv("LARGE_ORDER_IMBALANCE_THRESHOLD", "1.5"))
ORDERBOOK_IMBALANCE_THRESHOLD = float(os.getenv("ORDERBOOK_IMBALANCE_THRESHOLD", "1.3"))

ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "7200"))

# 扫描币
RAW_WATCHLIST = [
    "SIGNUSDT", "KITEUSDT", "HYPEUSDT", "SIRNUSDT", "PHAUSDT", "POWERUSDT",
    "SKYAIUSDT", "BARDUSDT", "QUSDT", "UAIUSDT", "HUSDT", "ICXUSDT",
    "ROBOUSDT", "OGNUSDT", "XAIUSDT", "IPUSDT", "XAGUSDT", "GUSDT",
    "ANKRUSDT", "ANIMEUSDT", "BANUSDT", "GUNUSDT", "ZROUSDT", "CUSDT",
    "LIGHTUSDT", "CVCUSDT", "AVAUSDT",
]

# =========================================================
# 工具函数
# =========================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_now_iso() -> str:
    return utc_now().isoformat()

def beijing_now() -> datetime:
    return utc_now().astimezone(timezone(timedelta(hours=8)))

def beijing_now_str() -> str:
    return beijing_now().strftime("%Y-%m-%d %H:%M:%S")

def now_ms() -> int:
    return int(time.time() * 1000)

def split_usdt_pair(symbol: str) -> str:
    s = symbol.strip().upper()
    if not s.endswith("USDT"):
        raise ValueError(f"watchlist symbol must end with USDT: {symbol}")
    return s[:-4]

def to_internal_symbol(base: str) -> str:
    return f"{base}USDT"

def gate_contract(base: str) -> str:
    return f"{base}_USDT"

def mexc_contract(base: str) -> str:
    return f"{base}_USDT"

def upbit_market(base: str) -> str:
    return f"USDT-{base}"

def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default

def format_number(x: Optional[float], digits: int = 4) -> str:
    if x is None:
        return "无"
    try:
        return f"{x:.{digits}f}"
    except Exception:
        return str(x)

def format_volume(x: Optional[float]) -> str:
    if x is None:
        return "无"
    ax = abs(x)
    if ax >= 1_000_000_000:
        return f"{x / 1_000_000_000:.2f}B"
    if ax >= 1_000_000:
        return f"{x / 1_000_000:.2f}M"
    if ax >= 1_000:
        return f"{x / 1_000:.2f}K"
    return f"{x:.2f}"

WATCHLIST_BASES = [split_usdt_pair(x) for x in RAW_WATCHLIST]
WATCHLIST_BASE_SET = set(WATCHLIST_BASES)

GATE_SYMBOLS = [gate_contract(x) for x in WATCHLIST_BASES]
MEXC_SYMBOLS = [mexc_contract(x) for x in WATCHLIST_BASES]
UPBIT_SYMBOLS = [upbit_market(x) for x in WATCHLIST_BASES]

# =========================================================
# 共享状态
# =========================================================

# 最新静态市场信息
market_state: Dict[str, Dict[str, Any]] = {}
# orderbook_state["gate:BTC_USDT"] = {"bids":[(p,s)], "asks":[(p,s)], "ts_ms": ...}
orderbook_state: Dict[str, Dict[str, Any]] = {}
# trade_cache["gate:BTC_USDT"] = deque([...])
trade_cache: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
# 去重告警
last_alert_sent_at: Dict[str, float] = {}

# 启动任务句柄
_bg_tasks: List[asyncio.Task] = []

# =========================================================
# 数据结构
# =========================================================

def market_key(exchange: str, raw_symbol: str) -> str:
    return f"{exchange}:{raw_symbol}"

def base_from_raw_symbol(exchange: str, raw_symbol: str) -> Optional[str]:
    rs = raw_symbol.upper()
    if exchange in ("gate", "mexc"):
        if rs.endswith("_USDT"):
            return rs[:-5]
    elif exchange == "upbit":
        if rs.startswith("USDT-"):
            return rs.split("-", 1)[1]
    return None

def make_market_item(
    *,
    exchange: str,
    market_type: str,
    raw_symbol: str,
    last_price: Optional[float] = None,
    volume_24h: Optional[float] = None,
    open_interest: Optional[float] = None,
    funding_rate: Optional[float] = None,
) -> Dict[str, Any]:
    base = base_from_raw_symbol(exchange, raw_symbol)
    return {
        "exchange": exchange,
        "market_type": market_type,
        "raw_symbol": raw_symbol,
        "base": base,
        "symbol": to_internal_symbol(base) if base else raw_symbol,
        "last_price": last_price,
        "volume_24h": volume_24h,
        "open_interest": open_interest,
        "funding_rate": funding_rate,
        "updated_at": utc_now_iso(),
    }

def make_trade_record(
    *,
    exchange: str,
    raw_symbol: str,
    price: float,
    size: float,
    side: str,
    ts_ms: int,
) -> Dict[str, Any]:
    return {
        "exchange": exchange,
        "raw_symbol": raw_symbol,
        "price": price,
        "size": size,
        "side": side,  # buy / sell / unknown
        "notional": price * size,
        "ts_ms": ts_ms,
    }

def prune_trade_cache(records: deque, current_ms: int, keep_minutes: int = CACHE_KEEP_MINUTES) -> None:
    min_ts = current_ms - keep_minutes * 60 * 1000
    while records and records[0]["ts_ms"] < min_ts:
        records.popleft()

# =========================================================
# 信号逻辑
# =========================================================

def calc_trade_flow_stats(records: List[Dict[str, Any]], current_ms: int) -> Dict[str, Any]:
    recent_start = current_ms - RECENT_WINDOW_MINUTES * 60 * 1000
    baseline_start = current_ms - BASELINE_WINDOW_MINUTES * 60 * 1000

    recent = [x for x in records if x["ts_ms"] >= recent_start]
    baseline = [x for x in records if baseline_start <= x["ts_ms"] < recent_start]

    recent_count = len(recent)
    recent_notional = sum(x["notional"] for x in recent)

    baseline_count = len(baseline)
    baseline_notional = sum(x["notional"] for x in baseline)

    bucket_count = max(BASELINE_WINDOW_MINUTES / RECENT_WINDOW_MINUTES, 1)

    baseline_avg_count = baseline_count / bucket_count if baseline_count > 0 else 0.0
    baseline_avg_notional = baseline_notional / bucket_count if baseline_notional > 0 else 0.0

    activity_count_ratio = (recent_count / baseline_avg_count) if baseline_avg_count > 0 else None
    activity_notional_ratio = (recent_notional / baseline_avg_notional) if baseline_avg_notional > 0 else None

    buy_notional = sum(x["notional"] for x in recent if x["side"] == "buy")
    sell_notional = sum(x["notional"] for x in recent if x["side"] == "sell")

    buy_large_notional = sum(x["notional"] for x in recent if x["side"] == "buy" and x["notional"] >= LARGE_TRADE_USDT)
    sell_large_notional = sum(x["notional"] for x in recent if x["side"] == "sell" and x["notional"] >= LARGE_TRADE_USDT)

    buy_large_count = sum(1 for x in recent if x["side"] == "buy" and x["notional"] >= LARGE_TRADE_USDT)
    sell_large_count = sum(1 for x in recent if x["side"] == "sell" and x["notional"] >= LARGE_TRADE_USDT)

    if sell_notional > 0:
        buy_sell_imbalance = buy_notional / sell_notional
    else:
        buy_sell_imbalance = 999.0 if buy_notional > 0 else None

    if sell_large_notional > 0:
        large_order_imbalance = buy_large_notional / sell_large_notional
    else:
        large_order_imbalance = 999.0 if buy_large_notional > 0 else None

    return {
        "recent_count": recent_count,
        "recent_notional": recent_notional,
        "activity_count_ratio": activity_count_ratio,
        "activity_notional_ratio": activity_notional_ratio,
        "buy_notional": buy_notional,
        "sell_notional": sell_notional,
        "buy_large_notional": buy_large_notional,
        "sell_large_notional": sell_large_notional,
        "buy_large_count": buy_large_count,
        "sell_large_count": sell_large_count,
        "buy_sell_imbalance": buy_sell_imbalance,
        "large_order_imbalance": large_order_imbalance,
    }

def calc_orderbook_imbalance(ob: Optional[Dict[str, Any]]) -> Optional[float]:
    if not ob:
        return None

    bids = ob.get("bids", [])[:5]
    asks = ob.get("asks", [])[:5]

    bid_depth = 0.0
    ask_depth = 0.0

    for p, s in bids:
        bid_depth += float(p) * float(s)

    for p, s in asks:
        ask_depth += float(p) * float(s)

    if ask_depth <= 0:
        return 999.0 if bid_depth > 0 else None

    return bid_depth / ask_depth

def build_flow_signal(item: Dict[str, Any], flow: Dict[str, Any], ob_imbalance: Optional[float]) -> Optional[Dict[str, Any]]:
    recent_notional = flow.get("recent_notional") or 0.0
    activity_notional_ratio = flow.get("activity_notional_ratio")
    buy_sell_imbalance = flow.get("buy_sell_imbalance")
    large_order_imbalance = flow.get("large_order_imbalance")

    if recent_notional < MIN_RECENT_NOTIONAL_USDT:
        return None

    long_score = 0
    short_score = 0
    long_reasons: List[str] = []
    short_reasons: List[str] = []

    if activity_notional_ratio is not None and activity_notional_ratio >= ACTIVITY_RATIO_THRESHOLD:
        long_score += 1
        short_score += 1
        long_reasons.append(f"短时成交额放大 {activity_notional_ratio:.2f} 倍")
        short_reasons.append(f"短时成交额放大 {activity_notional_ratio:.2f} 倍")

    if buy_sell_imbalance is not None:
        if buy_sell_imbalance >= BUY_SELL_IMBALANCE_THRESHOLD:
            long_score += 2
            long_reasons.append(f"主动买盘强于卖盘 {buy_sell_imbalance:.2f} 倍")
        elif buy_sell_imbalance > 0 and buy_sell_imbalance <= 1 / BUY_SELL_IMBALANCE_THRESHOLD:
            short_score += 2
            short_reasons.append(f"主动卖盘强于买盘 {(1 / buy_sell_imbalance):.2f} 倍")

    if large_order_imbalance is not None:
        if large_order_imbalance >= LARGE_ORDER_IMBALANCE_THRESHOLD:
            long_score += 2
            long_reasons.append(f"买入大单强于卖出大单 {large_order_imbalance:.2f} 倍")
        elif large_order_imbalance > 0 and large_order_imbalance <= 1 / LARGE_ORDER_IMBALANCE_THRESHOLD:
            short_score += 2
            short_reasons.append(f"卖出大单强于买入大单 {(1 / large_order_imbalance):.2f} 倍")

    if ob_imbalance is not None:
        if ob_imbalance >= ORDERBOOK_IMBALANCE_THRESHOLD:
            long_score += 1
            long_reasons.append(f"盘口买盘深度强于卖盘 {ob_imbalance:.2f} 倍")
        elif ob_imbalance > 0 and ob_imbalance <= 1 / ORDERBOOK_IMBALANCE_THRESHOLD:
            short_score += 1
            short_reasons.append(f"盘口卖盘深度强于买盘 {(1 / ob_imbalance):.2f} 倍")

    if long_score >= 4 and long_score > short_score:
        return {
            "signal_type": "LONG",
            "signal_text": "买多信号",
            "score": long_score,
            "reasons": long_reasons,
        }

    if short_score >= 4 and short_score > long_score:
        return {
            "signal_type": "SHORT",
            "signal_text": "买空信号",
            "score": short_score,
            "reasons": short_reasons,
        }

    return None

# =========================================================
# Telegram
# =========================================================

async def send_telegram_message(text: str) -> bool:
    if not TELEGRAM_ENABLED:
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return bool(data.get("ok"))
    except Exception as e:
        logger.warning("telegram 推送失败: %s", e)
        return False

def should_send_alert(exchange: str, symbol: str, signal_type: str) -> bool:
    key = f"{exchange}|{symbol}|{signal_type}"
    last_ts = last_alert_sent_at.get(key)
    now_ts = time.time()
    if last_ts is None:
        return True
    return (now_ts - last_ts) >= ALERT_COOLDOWN_SECONDS

def mark_alert_sent(exchange: str, symbol: str, signal_type: str) -> None:
    key = f"{exchange}|{symbol}|{signal_type}"
    last_alert_sent_at[key] = time.time()

def format_signal_message(item: Dict[str, Any], flow: Dict[str, Any], signal: Dict[str, Any], ob_imbalance: Optional[float]) -> str:
    lines = [
        f"",
        f"时间（北京时间）：{beijing_now_str()}",
        f"交易所：{item['exchange']}",
        f"市场：{'合约' if item['market_type'] == 'futures' else '现货'}",
        f"币种：{item['symbol']}",
        f"交易对：{item['raw_symbol']}",
        f"最新价格：{format_number(item.get('last_price'), 6)}",
        f"最近{RECENT_WINDOW_MINUTES}分钟成交额：{format_volume(flow.get('recent_notional'))} USDT",
        f"最近{RECENT_WINDOW_MINUTES}分钟成交笔数：{flow.get('recent_count', 0)}",
        f"活跃度放大倍数：{format_number(flow.get('activity_notional_ratio'), 2)}",
        f"主动买卖比：{format_number(flow.get('buy_sell_imbalance'), 2)}",
        f"大单买卖比：{format_number(flow.get('large_order_imbalance'), 2)}",
        f"买入大单次数：{flow.get('buy_large_count', 0)}",
        f"卖出大单次数：{flow.get('sell_large_count', 0)}",
        f"盘口失衡比：{format_number(ob_imbalance, 2)}",
        f"信号强度：{signal.get('score')}",
        "触发原因：",
    ]
    for r in signal.get("reasons", []):
        lines.append(f"- {r}")
    return "\n".join(lines)

# =========================================================
# 状态写入
# =========================================================

async def on_trade(exchange: str, raw_symbol: str, price: float, size: float, side: str, ts_ms: int) -> None:
    key = market_key(exchange, raw_symbol)

    # 写入 trade cache
    records = trade_cache[key]
    rec = make_trade_record(
        exchange=exchange,
        raw_symbol=raw_symbol,
        price=price,
        size=size,
        side=side,
        ts_ms=ts_ms,
    )

    # 简单去重
    signature = (rec["ts_ms"], rec["price"], rec["size"], rec["side"])
    recent_sigs = set((x["ts_ms"], x["price"], x["size"], x["side"]) for x in list(records)[-300:])
    if signature not in recent_sigs:
        records.append(rec)

    prune_trade_cache(records, now_ms())

    # 更新 last_price
    current = market_state.get(key)
    if current:
        current["last_price"] = price
        current["updated_at"] = utc_now_iso()
    else:
        market_state[key] = make_market_item(
            exchange=exchange,
            market_type="futures" if exchange in ("gate", "mexc") else "spot",
            raw_symbol=raw_symbol,
            last_price=price,
        )

    await evaluate_and_alert(exchange, raw_symbol)

async def on_orderbook(exchange: str, raw_symbol: str, bids: List[tuple], asks: List[tuple], ts_ms: int) -> None:
    key = market_key(exchange, raw_symbol)
    orderbook_state[key] = {
        "bids": bids[:10],
        "asks": asks[:10],
        "ts_ms": ts_ms,
    }
    await evaluate_and_alert(exchange, raw_symbol)

async def on_ticker(exchange: str, raw_symbol: str, last_price: Optional[float], volume_24h: Optional[float], open_interest: Optional[float], funding_rate: Optional[float]) -> None:
    key = market_key(exchange, raw_symbol)
    item = market_state.get(key)
    if item is None:
        market_state[key] = make_market_item(
            exchange=exchange,
            market_type="futures" if exchange in ("gate", "mexc") else "spot",
            raw_symbol=raw_symbol,
            last_price=last_price,
            volume_24h=volume_24h,
            open_interest=open_interest,
            funding_rate=funding_rate,
        )
    else:
        if last_price is not None:
            item["last_price"] = last_price
        if volume_24h is not None:
            item["volume_24h"] = volume_24h
        if open_interest is not None:
            item["open_interest"] = open_interest
        if funding_rate is not None:
            item["funding_rate"] = funding_rate
        item["updated_at"] = utc_now_iso()

async def evaluate_and_alert(exchange: str, raw_symbol: str) -> None:
    key = market_key(exchange, raw_symbol)
    item = market_state.get(key)
    if not item:
        return

    records = list(trade_cache.get(key, []))
    if not records:
        return

    flow = calc_trade_flow_stats(records, now_ms())
    ob_imbalance = calc_orderbook_imbalance(orderbook_state.get(key))
    signal = build_flow_signal(item, flow, ob_imbalance)
    if not signal:
        return

    if should_send_alert(exchange, item["symbol"], signal["signal_type"]):
        text = format_signal_message(item, flow, signal, ob_imbalance)
        ok = await send_telegram_message(text)
        if ok:
            mark_alert_sent(exchange, item["symbol"], signal["signal_type"])
            logger.info("已推送信号 exchange=%s symbol=%s type=%s", exchange, item["symbol"], signal["signal_type"])

# =========================================================
# Gate WS
# =========================================================

async def gate_ping_loop(ws):
    while True:
        await asyncio.sleep(20)
        try:
            await ws.send(json.dumps({
                "time": int(time.time()),
                "channel": "futures.ping"
            }))
        except Exception:
            return

async def gate_ws_loop():
    url = "wss://fx-ws.gateio.ws/v4/ws/usdt"

    while True:
        try:
            logger.info("连接 Gate WS...")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                # trades
                await ws.send(json.dumps({
                    "time": int(time.time()),
                    "channel": "futures.trades",
                    "event": "subscribe",
                    "payload": GATE_SYMBOLS,
                }))
                # full orderbook
                for sym in GATE_SYMBOLS:
                    await ws.send(json.dumps({
                        "time": int(time.time()),
                        "channel": "futures.order_book",
                        "event": "subscribe",
                        "payload": [sym, "20", "0"],
                    }))

                ping_task = asyncio.create_task(gate_ping_loop(ws))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    channel = msg.get("channel")
                    event = msg.get("event")

                    if channel == "futures.tickers" and event == "update":
                        for row in msg.get("result", []):
                            raw_symbol = str(row.get("contract", "")).upper()
                            if raw_symbol in GATE_SYMBOLS:
                                await on_ticker(
                                    exchange="gate",
                                    raw_symbol=raw_symbol,
                                    last_price=safe_float(row.get("last")),
                                    volume_24h=safe_float(row.get("volume_24h_quote")),
                                    open_interest=safe_float(row.get("total_size")),
                                    funding_rate=safe_float(row.get("funding_rate")),
                                )

                    elif channel == "futures.trades" and event == "update":
                        for row in msg.get("result", []):
                            raw_symbol = str(row.get("contract", "")).upper()
                            if raw_symbol not in GATE_SYMBOLS:
                                continue
                            price = safe_float(row.get("price"))
                            size_raw = safe_float(row.get("size"))
                            ts_ms = int(safe_float(row.get("create_time_ms"), now_ms()))
                            if price is None or size_raw is None:
                                continue
                            side = "buy" if size_raw > 0 else "sell"
                            size = abs(size_raw)
                            await on_trade("gate", raw_symbol, price, size, side, ts_ms)

                    elif channel == "futures.order_book" and event in ("all", "update"):
                        result = msg.get("result", {})
                        raw_symbol = str(result.get("contract", "")).upper()
                        if raw_symbol not in GATE_SYMBOLS:
                            continue
                        bids = []
                        asks = []
                        for x in result.get("bids", [])[:10]:
                            p = safe_float(x.get("p"))
                            s = safe_float(x.get("s"))
                            if p is not None and s is not None:
                                bids.append((p, abs(s)))
                        for x in result.get("asks", [])[:10]:
                            p = safe_float(x.get("p"))
                            s = safe_float(x.get("s"))
                            if p is not None and s is not None:
                                asks.append((p, abs(s)))
                        await on_orderbook("gate", raw_symbol, bids, asks, int(safe_float(result.get("t"), now_ms())))

                ping_task.cancel()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("Gate WS 断开，准备重连: %s", e)
            await asyncio.sleep(5)

# =========================================================
# MEXC WS
# =========================================================

async def mexc_ping_loop(ws):
    while True:
        await asyncio.sleep(15)
        try:
            await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            return

async def mexc_ws_loop():
    url = "wss://contract.mexc.com/edge"

    while True:
        try:
            logger.info("连接 MEXC WS...")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                for sym in MEXC_SYMBOLS:
                    await ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": sym}}))
                    await ws.send(json.dumps({"method": "sub.depth", "param": {"symbol": sym}}))
                    await ws.send(json.dumps({"method": "sub.ticker", "param": {"symbol": sym}}))

                ping_task = asyncio.create_task(mexc_ping_loop(ws))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    channel = msg.get("channel")
                    raw_symbol = str(msg.get("symbol", "")).upper()

                    if channel == "push.ticker":
                        data = msg.get("data", {})
                        if raw_symbol in MEXC_SYMBOLS:
                            await on_ticker(
                                exchange="mexc",
                                raw_symbol=raw_symbol,
                                last_price=safe_float(data.get("lastPrice")),
                                volume_24h=safe_float(data.get("amount24")),
                                open_interest=safe_float(data.get("holdVol")),
                                funding_rate=safe_float(data.get("fundingRate")),
                            )

                    elif channel == "push.deal":
                        if raw_symbol not in MEXC_SYMBOLS:
                            continue
                        for row in msg.get("data", []):
                            price = safe_float(row.get("p"))
                            size = safe_float(row.get("v"))
                            ts_ms = int(safe_float(row.get("t"), now_ms()))
                            side_code = row.get("T")
                            if price is None or size is None:
                                continue
                            side = "buy" if int(side_code) == 1 else "sell"
                            await on_trade("mexc", raw_symbol, price, abs(size), side, ts_ms)

                    elif channel == "push.depth":
                        if raw_symbol not in MEXC_SYMBOLS:
                            continue
                        data = msg.get("data", {})
                        bids = []
                        asks = []

                        for row in data.get("bids", [])[:10]:
                            if isinstance(row, list) and len(row) >= 2:
                                p = safe_float(row[0])
                                s = safe_float(row[1])
                                if p is not None and s is not None:
                                    bids.append((p, abs(s)))

                        for row in data.get("asks", [])[:10]:
                            if isinstance(row, list) and len(row) >= 2:
                                p = safe_float(row[0])
                                s = safe_float(row[1])
                                if p is not None and s is not None:
                                    asks.append((p, abs(s)))

                        await on_orderbook("mexc", raw_symbol, bids, asks, int(safe_float(msg.get("ts"), now_ms())))

                ping_task.cancel()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("MEXC WS 断开，准备重连: %s", e)
            await asyncio.sleep(5)

# =========================================================
# Upbit REST: 用于启动前验证实际 USDT 市场
# =========================================================

async def get_upbit_existing_usdt_markets() -> List[str]:
    url = f"{UPBIT_REST_BASE}/v1/market/all"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(url, params={"isDetails": "false"})
            resp.raise_for_status()
            data = resp.json()
            out = []
            if isinstance(data, list):
                for row in data:
                    market = str(row.get("market", "")).upper()
                    if market in UPBIT_SYMBOLS:
                        out.append(market)
            return out
    except Exception as e:
        logger.warning("Upbit 市场列表获取失败，改为直接订阅全名单: %s", e)
        return UPBIT_SYMBOLS[:]

# =========================================================
# Upbit WS
# =========================================================

async def upbit_ws_loop():
    while True:
        try:
            codes = await get_upbit_existing_usdt_markets()
            logger.info("连接 Upbit WS... 可用市场=%s", len(codes))

            async with websockets.connect(
                UPBIT_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                max_size=None,
            ) as ws:
                subscribe_payload = [
                    {"ticket": f"flow-signal-{int(time.time())}"},
                    {"type": "trade", "codes": codes, "is_only_realtime": True},
                    {"type": "orderbook", "codes": codes, "is_only_realtime": True},
                    {"format": "DEFAULT"},
                ]
                await ws.send(json.dumps(subscribe_payload))

                async for raw in ws:
                    try:
                        if isinstance(raw, bytes):
                            msg = json.loads(raw.decode("utf-8"))
                        else:
                            msg = json.loads(raw)
                    except Exception:
                        continue

                    msg_type = msg.get("type")
                    raw_symbol = str(msg.get("code", "")).upper()
                    if raw_symbol not in codes:
                        continue

                    if msg_type == "trade":
                        price = safe_float(msg.get("trade_price"))
                        size = safe_float(msg.get("trade_volume"))
                        ts_ms = int(safe_float(msg.get("trade_timestamp"), now_ms()))
                        ask_bid = str(msg.get("ask_bid", "")).upper()
                        if price is None or size is None:
                            continue
                        side = "buy" if ask_bid == "BID" else "sell" if ask_bid == "ASK" else "unknown"
                        await on_trade("upbit", raw_symbol, price, abs(size), side, ts_ms)

                    elif msg_type == "orderbook":
                        bids = []
                        asks = []
                        for row in msg.get("orderbook_units", [])[:10]:
                            bid_p = safe_float(row.get("bid_price"))
                            bid_s = safe_float(row.get("bid_size"))
                            ask_p = safe_float(row.get("ask_price"))
                            ask_s = safe_float(row.get("ask_size"))
                            if bid_p is not None and bid_s is not None:
                                bids.append((bid_p, abs(bid_s)))
                            if ask_p is not None and ask_s is not None:
                                asks.append((ask_p, abs(ask_s)))
                        await on_orderbook("upbit", raw_symbol, bids, asks, int(safe_float(msg.get("timestamp"), now_ms())))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("Upbit WS 断开，准备重连: %s", e)
            await asyncio.sleep(5)

# =========================================================
# FastAPI
# =========================================================

@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "time_beijing": beijing_now_str(),
        "auto_scan_enabled": AUTO_SCAN_ENABLED,
        "telegram_enabled": TELEGRAM_ENABLED,
        "watchlist_count": len(RAW_WATCHLIST),
        "state": {
            "market_state": len(market_state),
            "orderbook_state": len(orderbook_state),
            "trade_cache_keys": len(trade_cache),
        },
        "enabled_exchanges": {
            "gate_futures": ENABLE_GATE,
            "mexc_futures": ENABLE_MEXC,
            "upbit_spot": ENABLE_UPBIT,
        },
    }

@app.get("/watchlist")
async def watchlist() -> Dict[str, Any]:
    return {
        "raw_watchlist": RAW_WATCHLIST,
        "gate_symbols": GATE_SYMBOLS,
        "mexc_symbols": MEXC_SYMBOLS,
        "upbit_symbols": UPBIT_SYMBOLS,
    }

@app.get("/state")
async def state() -> Dict[str, Any]:
    sample_markets = list(market_state.items())[:20]
    sample_orderbooks = list(orderbook_state.items())[:20]

    return {
        "generated_at": utc_now_iso(),
        "generated_at_beijing": beijing_now_str(),
        "market_state_count": len(market_state),
        "orderbook_state_count": len(orderbook_state),
        "trade_cache_count": len(trade_cache),
        "sample_markets": dict(sample_markets),
        "sample_orderbooks": dict(sample_orderbooks),
    }

@app.get("/signals")
async def signals_preview() -> Dict[str, Any]:
    out = []
    current_ms = now_ms()

    for key, item in market_state.items():
        records = list(trade_cache.get(key, []))
        if not records:
            continue
        flow = calc_trade_flow_stats(records, current_ms)
        ob_imbalance = calc_orderbook_imbalance(orderbook_state.get(key))
        signal = build_flow_signal(item, flow, ob_imbalance)
        if signal:
            out.append({
                "exchange": item["exchange"],
                "symbol": item["symbol"],
                "raw_symbol": item["raw_symbol"],
                "signal_type": signal["signal_type"],
                "score": signal["score"],
                "recent_notional": flow.get("recent_notional"),
                "activity_notional_ratio": flow.get("activity_notional_ratio"),
                "buy_sell_imbalance": flow.get("buy_sell_imbalance"),
                "large_order_imbalance": flow.get("large_order_imbalance"),
                "orderbook_imbalance": ob_imbalance,
                "reasons": signal.get("reasons", []),
            })

    return {
        "generated_at_beijing": beijing_now_str(),
        "signal_count": len(out),
        "signals": out,
    }

# =========================================================
# 启动 / 关闭
# =========================================================

@app.on_event("startup")
async def on_startup() -> None:
    logger.info(
        "启动配置 port=%s auto_scan_enabled=%s telegram_enabled=%s watchlist_count=%s",
        PORT,
        AUTO_SCAN_ENABLED,
        TELEGRAM_ENABLED,
        len(RAW_WATCHLIST),
    )

    if not AUTO_SCAN_ENABLED:
        return

    if ENABLE_GATE:
        _bg_tasks.append(asyncio.create_task(gate_ws_loop()))

    if ENABLE_MEXC:
        _bg_tasks.append(asyncio.create_task(mexc_ws_loop()))

    if ENABLE_UPBIT:
        _bg_tasks.append(asyncio.create_task(upbit_ws_loop()))

    logger.info("已创建 WebSocket 后台任务 count=%s", len(_bg_tasks))

@app.on_event("shutdown")
async def on_shutdown() -> None:
    for task in _bg_tasks:
        task.cancel()

    for task in _bg_tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass

# =========================================================
# 本地启动
# =========================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
