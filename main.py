import asyncio
import json
import os
import time
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional

import aiohttp
import websockets
from websockets.client import WebSocketClientProtocol


# =========================
# 环境变量读取
# =========================

def env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_symbols(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name)
    if not raw:
        return default
    return [s.strip().lower() for s in raw.split(",") if s.strip()]


# =========================
# 配置
# =========================

CONFIG = {
    # 你要监控的币
    "symbols": env_symbols(
        "SYMBOLS",
        [
            "signusdt",
            "ensusdt",
            "idusdt",
            "cvcusdt",
            "fetusdt",
            "renderusdt",
            "nearusdt",
            "grtusdt",
        ],
    ),

    # Binance REST 端点
    "rest_base_urls": [
        "https://api.binance.com",
        "https://api-gcp.binance.com",
        "https://api1.binance.com",
        "https://api2.binance.com",
        "https://api3.binance.com",
        "https://api4.binance.com",
    ],

    # Binance WS 端点
    "ws_base_urls": [
        "wss://stream.binance.com:9443",
        "wss://stream.binance.com:443",
        "wss://data-stream.binance.vision",
    ],

    # Telegram
    "telegram_bot_token": "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI",
    "telegram_chat_id": "5671949305",
    "telegram_test_on_start": env_bool("TELEGRAM_TEST_ON_START", True),

    # 扫单参数
    "sweep_window_sec": env_float("SWEEP_WINDOW_SEC", 1.2),
    "sweep_min_notional_usdt": env_float("SWEEP_MIN_NOTIONAL_USDT", 50000.0),
    "cooldown_sec": env_int("COOLDOWN_SEC", 15),

    # 超时 / 重连
    "rest_timeout_sec": env_int("REST_TIMEOUT_SEC", 12),
    "telegram_timeout_sec": env_int("TELEGRAM_TIMEOUT_SEC", 12),
    "ws_open_timeout_sec": env_int("WS_OPEN_TIMEOUT_SEC", 20),
    "ws_ping_interval_sec": env_int("WS_PING_INTERVAL_SEC", 20),
    "ws_ping_timeout_sec": env_int("WS_PING_TIMEOUT_SEC", 20),
    "ws_force_reconnect_after_sec": env_int("WS_FORCE_RECONNECT_AFTER_SEC", 23 * 60 * 60),

    # 日志
    "verbose": env_bool("VERBOSE", True),
}


# =========================
# 工具函数
# =========================

def now_ts() -> float:
    return time.time()


def format_ts(ts: Optional[float] = None) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts or now_ts()))


def fmt_usdt(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f}B"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v / 1_000:.2f}K"
    return f"{v:.2f}"


def log(msg: str) -> None:
    print(msg, flush=True)


# =========================
# Telegram
# =========================

class TelegramPusher:
    def __init__(self, bot_token: str, chat_id: str, timeout_sec: int):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.timeout_sec = timeout_sec
        self.enabled = bool(bot_token and chat_id)
        self.url = f"https://api.telegram.org/bot{bot_token}/sendMessage" if self.enabled else ""

    async def send(self, session: aiohttp.ClientSession, text: str) -> bool:
        if not self.enabled:
            log("[TG] skipped: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 未配置")
            return False

        try:
            async with session.post(
                self.url,
                data={"chat_id": self.chat_id, "text": text},
                timeout=self.timeout_sec,
            ) as resp:
                body = await resp.text()
                if resp.status != 200:
                    log(f"[TG ERROR] HTTP {resp.status}: {body}")
                    return False
                return True
        except Exception as e:
            log(f"[TG ERROR] {e}")
            return False


# =========================
# 数据结构
# =========================

@dataclass
class TradeEvent:
    ts: float
    price: float
    qty: float
    notional: float
    side: str  # buy / sell


class SymbolState:
    def __init__(self, symbol: str, cfg: Dict):
        self.symbol = symbol.lower()
        self.symbol_display = self.symbol.upper()
        self.cfg = cfg

        self.mid_price: Optional[float] = None

        self.trade_window: Deque[TradeEvent] = deque()
        self.buy_notional_sum = 0.0
        self.sell_notional_sum = 0.0

        self.last_alert_at: Dict[str, float] = defaultdict(lambda: 0.0)

    def can_alert(self, key: str) -> bool:
        return now_ts() - self.last_alert_at[key] >= self.cfg["cooldown_sec"]

    def mark_alert(self, key: str) -> None:
        self.last_alert_at[key] = now_ts()

    def update_book_ticker(self, data: Dict) -> None:
        bid = float(data["b"])
        ask = float(data["a"])
        self.mid_price = (bid + ask) / 2.0

    def add_trade(self, data: Dict) -> None:
        price = float(data["p"])
        qty = float(data["q"])
        notional = price * qty

        # Binance aggTrade:
        # m=true  -> buyer is market maker -> taker sell
        # m=false -> taker buy
        side = "sell" if data.get("m", False) else "buy"

        evt = TradeEvent(
            ts=now_ts(),
            price=price,
            qty=qty,
            notional=notional,
            side=side,
        )
        self.trade_window.append(evt)

        if side == "buy":
            self.buy_notional_sum += notional
        else:
            self.sell_notional_sum += notional

        self.cleanup_old_trades()

    def cleanup_old_trades(self) -> None:
        cutoff = now_ts() - self.cfg["sweep_window_sec"]
        while self.trade_window and self.trade_window[0].ts < cutoff:
            old = self.trade_window.popleft()
            if old.side == "buy":
                self.buy_notional_sum -= old.notional
            else:
                self.sell_notional_sum -= old.notional

        if self.buy_notional_sum < 0:
            self.buy_notional_sum = 0.0
        if self.sell_notional_sum < 0:
            self.sell_notional_sum = 0.0

    def check_alerts(self) -> List[str]:
        alerts: List[str] = []
        mid_text = f"{self.mid_price:.6f}" if self.mid_price is not None else "N/A"

        if self.buy_notional_sum >= self.cfg["sweep_min_notional_usdt"]:
            if self.can_alert("buy_sweep"):
                self.mark_alert("buy_sweep")
                alerts.append(
                    f"🚀 扫单提醒\n\n"
                    f"交易对: {self.symbol_display}\n"
                    f"类型: 主动买入扫单\n"
                    f"窗口: {self.cfg['sweep_window_sec']:.1f} 秒\n"
                    f"累计成交额: {fmt_usdt(self.buy_notional_sum)} USDT\n"
                    f"中间价: {mid_text}\n"
                    f"时间: {format_ts()}"
                )

        if self.sell_notional_sum >= self.cfg["sweep_min_notional_usdt"]:
            if self.can_alert("sell_sweep"):
                self.mark_alert("sell_sweep")
                alerts.append(
                    f"🔻 扫单提醒\n\n"
                    f"交易对: {self.symbol_display}\n"
                    f"类型: 主动卖出扫单\n"
                    f"窗口: {self.cfg['sweep_window_sec']:.1f} 秒\n"
                    f"累计成交额: {fmt_usdt(self.sell_notional_sum)} USDT\n"
                    f"中间价: {mid_text}\n"
                    f"时间: {format_ts()}"
                )

        return alerts


# =========================
# REST
# =========================

async def try_exchange_info(session: aiohttp.ClientSession, cfg: Dict) -> Optional[dict]:
    for base in cfg["rest_base_urls"]:
        url = f"{base}/api/v3/exchangeInfo"
        try:
            log(f"[REST TEST] trying {url}")
            async with session.get(url, timeout=cfg["rest_timeout_sec"]) as resp:
                resp.raise_for_status()
                payload = await resp.json()
                count = len(payload.get("symbols", []))
                log(f"[REST TEST] OK: {url} | symbols={count}")
                return payload
        except Exception as e:
            log(f"[REST TEST] FAILED: {url} | {e}")
    return None


async def fetch_valid_spot_symbols(session: aiohttp.ClientSession, cfg: Dict) -> List[str]:
    requested = [s.lower() for s in cfg["symbols"]]
    log(f"[INIT] Requested symbols: {len(requested)}")

    payload = await try_exchange_info(session, cfg)
    if payload is None:
        log("[INIT] Binance REST 全部失败，直接使用配置的 symbols")
        log(f"[INIT] Using: {', '.join(requested)}")
        return requested

    valid = set()
    for item in payload.get("symbols", []):
        sym = item.get("symbol", "").lower()
        status = item.get("status", "")
        if status == "TRADING":
            valid.add(sym)

    kept = [s for s in requested if s in valid]
    dropped = [s for s in requested if s not in valid]

    log(f"[INIT] Valid symbols: {len(kept)}")
    if kept:
        log(f"[INIT] Using: {', '.join(kept)}")
    if dropped:
        log(f"[INIT] Skipped: {', '.join(dropped)}")

    return kept if kept else requested


# =========================
# Telegram 测试
# =========================

async def test_telegram(session: aiohttp.ClientSession, pusher: TelegramPusher, cfg: Dict) -> None:
    if not cfg["telegram_test_on_start"]:
        log("[TG TEST] skipped")
        return

    ok = await pusher.send(session, "✅ Telegram 测试成功，监控脚本准备启动")
    if ok:
        log("[TG TEST] OK")
    else:
        log("[TG TEST] FAILED")


# =========================
# 推送
# =========================

async def push_many(
    session: aiohttp.ClientSession,
    pusher: TelegramPusher,
    messages: List[str],
    verbose: bool = True,
) -> None:
    for msg in messages:
        if verbose:
            log("\n===== ALERT =====")
            log(msg)
            log("=================\n")
        await pusher.send(session, msg)


# =========================
# WebSocket
# =========================

def build_streams_path(symbols: List[str]) -> str:
    # 组合流：/stream?streams=<stream1>/<stream2>
    streams = []
    for s in symbols:
        streams.append(f"{s}@aggTrade")
        streams.append(f"{s}@bookTicker")
    return "/stream?streams=" + "/".join(streams)


async def connect_ws_with_fallback(cfg: Dict, streams_path: str) -> WebSocketClientProtocol:
    last_error = None

    for base in cfg["ws_base_urls"]:
        ws_url = base + streams_path
        try:
            log(f"[WS] trying {ws_url[:140]}...")
            ws = await websockets.connect(
                ws_url,
                open_timeout=cfg["ws_open_timeout_sec"],
                ping_interval=cfg["ws_ping_interval_sec"],
                ping_timeout=cfg["ws_ping_timeout_sec"],
                max_size=2**23,
            )
            log(f"[WS] Connected via {base}")
            return ws
        except Exception as e:
            last_error = e
            log(f"[WS] FAILED via {base}: {e}")

    raise RuntimeError(f"all ws endpoints failed: {last_error}")


# =========================
# 主程序
# =========================

async def run_monitor() -> None:
    cfg = CONFIG
    pusher = TelegramPusher(
        cfg["telegram_bot_token"],
        cfg["telegram_chat_id"],
        cfg["telegram_timeout_sec"],
    )

    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        await test_telegram(session, pusher, cfg)

        symbols = await fetch_valid_spot_symbols(session, cfg)
        states: Dict[str, SymbolState] = {s: SymbolState(s, cfg) for s in symbols}
        streams_path = build_streams_path(symbols)

        while True:
            ws = None
            connected_at = now_ts()

            try:
                log(f"[WS] Connecting with {len(symbols)} symbols...")
                ws = await connect_ws_with_fallback(cfg, streams_path)
                connected_at = now_ts()

                while True:
                    # 提前于 24 小时主动重连
                    if now_ts() - connected_at >= cfg["ws_force_reconnect_after_sec"]:
                        raise RuntimeError("scheduled reconnect before 24h disconnect")

                    raw = await ws.recv()
                    msg = json.loads(raw)

                    # combined stream 格式：{"stream":"...","data":{...}}
                    stream_name = msg.get("stream", "")
                    data = msg.get("data", {})

                    if "@" not in stream_name:
                        continue

                    symbol = stream_name.split("@")[0].lower()
                    state = states.get(symbol)
                    if state is None:
                        continue

                    if stream_name.endswith("@bookTicker"):
                        state.update_book_ticker(data)
                    elif stream_name.endswith("@aggTrade"):
                        state.add_trade(data)

                    alerts = state.check_alerts()
                    if alerts:
                        await push_many(session, pusher, alerts, verbose=cfg["verbose"])

            except Exception as e:
                log(f"[WS ERROR] {e}")
                log("[WS] Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

            finally:
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass


if __name__ == "__main__":
    try:
        asyncio.run(run_monitor())
    except KeyboardInterrupt:
        log("Stopped by user.")
