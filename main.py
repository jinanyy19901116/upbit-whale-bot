import asyncio
import json
import os
import time
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional

import aiohttp
import websockets


# =========================
# ENV
# =========================

def env(name, default):
    return os.getenv(name, default)


def env_float(name, default):
    return float(os.getenv(name, default))


def env_int(name, default):
    return int(os.getenv(name, default))


def env_bool(name, default):
    return os.getenv(name, str(default)).lower() in ["1", "true", "yes"]


def env_symbols(name, default):
    raw = os.getenv(name)
    if not raw:
        return default
    return [s.strip().lower() for s in raw.split(",")]


CONFIG = {
    "symbols": env_symbols(
        "SYMBOLS",
        ["ensusdt", "fetusdt", "renderusdt", "nearusdt"],
    ),

    # Binance WS（你环境可用）
    "binance_ws": [
        "wss://data-stream.binance.vision",
        "wss://stream.binance.com:443",
    ],

    # Gate WS
    "gate_ws": "wss://api.gateio.ws/ws/v4/",

    # Gate REST
    "gate_rest": "https://api.gateio.ws/api/v4",

    # Telegram
    "tg_token": "8783197055:AAG7vbzYzTsTU0Zwyb8uQiXub_MffUb7GDI",
    "tg_chat": "5671949305",

    "threshold": env_float("SWEEP_MIN_NOTIONAL_USDT", 50000),
    "window": env_float("SWEEP_WINDOW_SEC", 1.2),
    "cooldown": env_int("COOLDOWN_SEC", 15),
}


# =========================
# UTILS
# =========================

def now():
    return time.time()


def fmt(v):
    if v > 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if v > 1_000:
        return f"{v/1_000:.2f}K"
    return f"{v:.2f}"


# =========================
# TELEGRAM
# =========================

class TG:
    def __init__(self):
        self.url = f"https://api.telegram.org/bot{CONFIG['tg_token']}/sendMessage"

    async def send(self, session, text):
        if not CONFIG["tg_token"]:
            return
        try:
            await session.post(self.url, data={
                "chat_id": CONFIG["tg_chat"],
                "text": text
            })
        except:
            print("[TG FAIL]")


# =========================
# STATE
# =========================

@dataclass
class Trade:
    ts: float
    price: float
    size: float
    side: str


class State:
    def __init__(self, symbol):
        self.symbol = symbol
        self.trades: Deque[Trade] = deque()
        self.buy = 0
        self.sell = 0
        self.last = defaultdict(lambda: 0)

    def add(self, price, size, side):
        t = Trade(now(), price, size, side)
        self.trades.append(t)

        val = price * size
        if side == "buy":
            self.buy += val
        else:
            self.sell += val

        self.clean()

    def clean(self):
        cutoff = now() - CONFIG["window"]
        while self.trades and self.trades[0].ts < cutoff:
            t = self.trades.popleft()
            val = t.price * t.size
            if t.side == "buy":
                self.buy -= val
            else:
                self.sell -= val

    def check(self):
        out = []

        if self.buy > CONFIG["threshold"] and now() - self.last["buy"] > CONFIG["cooldown"]:
            self.last["buy"] = now()
            out.append(f"🚀 {self.symbol.upper()} BUY {fmt(self.buy)}")

        if self.sell > CONFIG["threshold"] and now() - self.last["sell"] > CONFIG["cooldown"]:
            self.last["sell"] = now()
            out.append(f"🔻 {self.symbol.upper()} SELL {fmt(self.sell)}")

        return out


# =========================
# BINANCE WS
# =========================

async def run_binance(states):
    streams = []
    for s in CONFIG["symbols"]:
        streams.append(f"{s}@aggTrade")

    path = "/stream?streams=" + "/".join(streams)

    for base in CONFIG["binance_ws"]:
        try:
            url = base + path
            print("[BINANCE] connecting", url)

            async with websockets.connect(url) as ws:
                print("[BINANCE] connected")

                while True:
                    msg = json.loads(await ws.recv())
                    data = msg["data"]

                    symbol = data["s"].lower()
                    price = float(data["p"])
                    qty = float(data["q"])
                    side = "sell" if data["m"] else "buy"

                    states[symbol].add(price, qty, side)

        except Exception as e:
            print("[BINANCE ERROR]", e)


# =========================
# GATE WS
# =========================

async def run_gate(states):
    while True:
        try:
            async with websockets.connect(CONFIG["gate_ws"]) as ws:
                print("[GATE] connected")

                # subscribe
                await ws.send(json.dumps({
                    "time": int(time.time()),
                    "channel": "spot.trades",
                    "event": "subscribe",
                    "payload": CONFIG["symbols"],
                }))

                while True:
                    msg = json.loads(await ws.recv())

                    if msg.get("event") != "update":
                        continue

                    for t in msg["result"]:
                        symbol = t["currency_pair"].lower()
                        price = float(t["price"])
                        size = float(t["amount"])
                        side = t["side"]

                        states[symbol].add(price, size, side)

        except Exception as e:
            print("[GATE ERROR]", e)
            await asyncio.sleep(5)


# =========================
# MAIN
# =========================

async def main():
    states = {s: State(s) for s in CONFIG["symbols"]}
    tg = TG()

    async with aiohttp.ClientSession() as session:

        # Telegram test
        await tg.send(session, "🚀 Monitor Started")

        tasks = [
            run_binance(states),
            run_gate(states)
        ]

        while True:
            await asyncio.sleep(1)

            for s in states.values():
                alerts = s.check()
                for a in alerts:
                    print(a)
                    await tg.send(session, a)


if __name__ == "__main__":
    asyncio.run(main())
