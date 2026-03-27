import asyncio
import json
import os
import time
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Deque, Dict, List

import aiohttp
import websockets


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


CONFIG = {
    "symbols": env_symbols(
        "SYMBOLS",
        ["signusdt", "ensusdt", "idusdt", "cvcusdt", "fetusdt", "renderusdt", "nearusdt", "grtusdt"],
    ),

    "binance_ws_urls": [
        "wss://data-stream.binance.vision",
        "wss://stream.binance.com:443",
        "wss://stream.binance.com:9443",
    ],
    "gate_ws_url": env_str("GATE_WS_URL", "wss://api.gateio.ws/ws/v4/"),
    "gate_rest_url": env_str("GATE_REST_URL", "https://api.gateio.ws/api/v4"),

    "telegram_bot_token": env_str("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": env_str("TELEGRAM_CHAT_ID", ""),
    "telegram_test_on_start": env_bool("TELEGRAM_TEST_ON_START", True),

    "sweep_window_sec": env_float("SWEEP_WINDOW_SEC", 1.2),
    "sweep_min_notional_usdt": env_float("SWEEP_MIN_NOTIONAL_USDT", 50000.0),
    "cooldown_sec": env_int("COOLDOWN_SEC", 15),

    "heartbeat_sec": env_int("HEARTBEAT_SEC", 30),
    "recv_timeout_sec": env_int("RECV_TIMEOUT_SEC", 60),
    "reconnect_delay_sec": env_int("RECONNECT_DELAY_SEC", 5),
    "ws_open_timeout_sec": env_int("WS_OPEN_TIMEOUT_SEC", 20),
    "ws_ping_interval_sec": env_int("WS_PING_INTERVAL_SEC", 20),
    "ws_ping_timeout_sec": env_int("WS_PING_TIMEOUT_SEC", 20),
    "ws_force_reconnect_after_sec": env_int("WS_FORCE_RECONNECT_AFTER_SEC", 23 * 60 * 60),

    "verbose": env_bool("VERBOSE", True),
}


def now_ts() -> float:
    return time.time()


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


class TelegramPusher:
    def __init__(self, bot_token: str, chat_id: str):
        self.enabled = bool(bot_token and chat_id)
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{bot_token}/sendMessage" if self.enabled else ""

    async def send(self, session: aiohttp.ClientSession, text: str) -> bool:
        if not self.enabled:
            log("[TG] skipped: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 未配置")
            return False

        try:
            async with session.post(
                self.url,
                data={"chat_id": self.chat_id, "text": text},
                timeout=12,
            ) as resp:
                body = await resp.text()
                if resp.status != 200:
                    log(f"[TG ERROR] HTTP {resp.status}: {body}")
                    return False
                return True
        except Exception as e:
            log(f"[TG ERROR] {e}")
            return False


@dataclass
class TradeEvent:
    ts: float
    price: float
    size: float
    side: str  # buy / sell


class SymbolState:
    def __init__(self, symbol: str):
        self.symbol = symbol.lower()
        self.symbol_display = self.symbol.upper()
        self.trades: Deque[TradeEvent] = deque()
        self.buy_notional = 0.0
        self.sell_notional = 0.0
        self.last_alert_at: Dict[str, float] = defaultdict(lambda: 0.0)
        self.last_trade_ts = 0.0
        self.last_trade_price = 0.0

    def add_trade(self, price: float, size: float, side: str) -> None:
        event = TradeEvent(
            ts=now_ts(),
            price=price,
            size=size,
            side=side,
        )
        self.trades.append(event)
        notional = price * size

        if side == "buy":
            self.buy_notional += notional
        else:
            self.sell_notional += notional

        self.last_trade_ts = event.ts
        self.last_trade_price = price
        self.cleanup()

    def cleanup(self) -> None:
        cutoff = now_ts() - CONFIG["sweep_window_sec"]
        while self.trades and self.trades[0].ts < cutoff:
            old = self.trades.popleft()
            notional = old.price * old.size
            if old.side == "buy":
                self.buy_notional -= notional
            else:
                self.sell_notional -= notional

        if self.buy_notional < 0:
            self.buy_notional = 0.0
        if self.sell_notional < 0:
            self.sell_notional = 0.0

    def can_alert(self, key: str) -> bool:
        return now_ts() - self.last_alert_at[key] >= CONFIG["cooldown_sec"]

    def mark_alert(self, key: str) -> None:
        self.last_alert_at[key] = now_ts()

    def check_alerts(self) -> List[str]:
        alerts: List[str] = []

        if self.buy_notional >= CONFIG["sweep_min_notional_usdt"] and self.can_alert("buy"):
            self.mark_alert("buy")
            alerts.append(
                f"🚀 扫单提醒\n\n"
                f"交易对: {self.symbol_display}\n"
                f"类型: 主动买入扫单\n"
                f"窗口: {CONFIG['sweep_window_sec']:.1f} 秒\n"
                f"累计成交额: {fmt_usdt(self.buy_notional)} USDT\n"
                f"最近成交价: {self.last_trade_price:.8f}\n"
                f"来源: 聚合成交流\n"
            )

        if self.sell_notional >= CONFIG["sweep_min_notional_usdt"] and self.can_alert("sell"):
            self.mark_alert("sell")
            alerts.append(
                f"🔻 扫单提醒\n\n"
                f"交易对: {self.symbol_display}\n"
                f"类型: 主动卖出扫单\n"
                f"窗口: {CONFIG['sweep_window_sec']:.1f} 秒\n"
                f"累计成交额: {fmt_usdt(self.sell_notional)} USDT\n"
                f"最近成交价: {self.last_trade_price:.8f}\n"
                f"来源: 聚合成交流\n"
            )

        return alerts


async def test_gate_rest(session: aiohttp.ClientSession) -> None:
    try:
        url = f"{CONFIG['gate_rest_url']}/spot/currencies"
        async with session.get(url, timeout=12) as resp:
            if resp.status == 200:
                log("[GATE REST TEST] OK")
            else:
                log(f"[GATE REST TEST] HTTP {resp.status}")
    except Exception as e:
        log(f"[GATE REST TEST] FAILED: {e}")


async def test_telegram(session: aiohttp.ClientSession, tg: TelegramPusher) -> None:
    if not CONFIG["telegram_test_on_start"]:
        log("[TG TEST] skipped")
        return
    ok = await tg.send(session, "✅ Telegram 测试成功，监控脚本准备启动")
    log("[TG TEST] OK" if ok else "[TG TEST] FAILED")


def build_binance_stream_path(symbols: List[str]) -> str:
    streams = []
    for s in symbols:
        streams.append(f"{s}@aggTrade")
    return "/stream?streams=" + "/".join(streams)


async def run_binance(states: Dict[str, SymbolState]) -> None:
    path = build_binance_stream_path(CONFIG["symbols"])

    while True:
        for base in CONFIG["binance_ws_urls"]:
            ws = None
            connected_at = now_ts()

            try:
                url = base + path
                log(f"[BINANCE] connecting {url}")
                ws = await websockets.connect(
                    url,
                    open_timeout=CONFIG["ws_open_timeout_sec"],
                    ping_interval=CONFIG["ws_ping_interval_sec"],
                    ping_timeout=CONFIG["ws_ping_timeout_sec"],
                    max_size=2**23,
                )
                log(f"[BINANCE] connected via {base}")
                connected_at = now_ts()

                while True:
                    if now_ts() - connected_at >= CONFIG["ws_force_reconnect_after_sec"]:
                        raise RuntimeError("scheduled reconnect before 24h disconnect")

                    raw = await asyncio.wait_for(ws.recv(), timeout=CONFIG["recv_timeout_sec"])
                    msg = json.loads(raw)

                    data = msg.get("data", {})
                    symbol = data.get("s", "").lower()
                    if symbol not in states:
                        continue

                    price = float(data["p"])
                    qty = float(data["q"])
                    side = "sell" if data.get("m", False) else "buy"

                    states[symbol].add_trade(price, qty, side)

            except Exception as e:
                log(f"[BINANCE ERROR] {e}")

            finally:
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass

        log(f"[BINANCE] retry in {CONFIG['reconnect_delay_sec']}s")
        await asyncio.sleep(CONFIG["reconnect_delay_sec"])


def gate_symbol(symbol: str) -> str:
    base = symbol[:-4].upper()
    quote = symbol[-4:].upper()
    return f"{base}_{quote}"


async def run_gate(states: Dict[str, SymbolState]) -> None:
    payload = [gate_symbol(s) for s in CONFIG["symbols"]]

    while True:
        ws = None
        connected_at = now_ts()

        try:
            log(f"[GATE] connecting {CONFIG['gate_ws_url']}")
            ws = await websockets.connect(
                CONFIG["gate_ws_url"],
                open_timeout=CONFIG["ws_open_timeout_sec"],
                ping_interval=CONFIG["ws_ping_interval_sec"],
                ping_timeout=CONFIG["ws_ping_timeout_sec"],
                max_size=2**23,
            )
            log("[GATE] connected")
            connected_at = now_ts()

            subscribe_msg = {
                "time": int(time.time()),
                "channel": "spot.trades",
                "event": "subscribe",
                "payload": payload,
            }
            await ws.send(json.dumps(subscribe_msg))
            log(f"[GATE] subscribed: {payload}")

            while True:
                if now_ts() - connected_at >= CONFIG["ws_force_reconnect_after_sec"]:
                    raise RuntimeError("scheduled reconnect")

                raw = await asyncio.wait_for(ws.recv(), timeout=CONFIG["recv_timeout_sec"])
                msg = json.loads(raw)

                event = msg.get("event")
                channel = msg.get("channel")

                if event == "subscribe":
                    continue

                if channel != "spot.trades" or event != "update":
                    continue

                result = msg.get("result", [])
                if isinstance(result, dict):
                    result = [result]

                for trade in result:
                    pair = trade.get("currency_pair", "").replace("_", "").lower()
                    if pair not in states:
                        continue

                    price = float(trade["price"])
                    size = float(trade["amount"])
                    side = trade["side"].lower()

                    states[pair].add_trade(price, size, side)

        except Exception as e:
            log(f"[GATE ERROR] {e}")

        finally:
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass

        log(f"[GATE] retry in {CONFIG['reconnect_delay_sec']}s")
        await asyncio.sleep(CONFIG["reconnect_delay_sec"])


async def alert_loop(states: Dict[str, SymbolState], tg: TelegramPusher, session: aiohttp.ClientSession) -> None:
    while True:
        await asyncio.sleep(1)

        for state in states.values():
            alerts = state.check_alerts()
            for msg in alerts:
                log("\n===== ALERT =====")
                log(msg)
                log("=================\n")
                await tg.send(session, msg)


async def heartbeat_loop(states: Dict[str, SymbolState]) -> None:
    while True:
        await asyncio.sleep(CONFIG["heartbeat_sec"])
        last_times = []
        for symbol, state in states.items():
            age = int(now_ts() - state.last_trade_ts) if state.last_trade_ts > 0 else -1
            last_times.append(f"{symbol}:{age}s")

        log(f"[HEARTBEAT] {time.strftime('%Y-%m-%d %H:%M:%S')} | {' | '.join(last_times)}")


async def main() -> None:
    if not CONFIG["symbols"]:
        raise RuntimeError("SYMBOLS is empty")

    tg = TelegramPusher(CONFIG["8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI"
], CONFIG["5671949305"])
    states = {s: SymbolState(s) for s in CONFIG["symbols"]}

    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await test_telegram(session, tg)
        await test_gate_rest(session)

        log(f"[INIT] Using symbols: {', '.join(CONFIG['symbols'])}")

        tasks = [
            asyncio.create_task(run_binance(states), name="binance"),
            asyncio.create_task(run_gate(states), name="gate"),
            asyncio.create_task(alert_loop(states, tg, session), name="alerts"),
            asyncio.create_task(heartbeat_loop(states), name="heartbeat"),
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        for task in done:
            exc = task.exception()
            if exc:
                log(f"[FATAL] task {task.get_name()} crashed: {exc}")

        for task in pending:
            task.cancel()

        await asyncio.gather(*pending, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Stopped by user.")
