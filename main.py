import asyncio
import logging
import os
import statistics
import time
from datetime import datetime, timezone, timedelta

import aiohttp
from dotenv import load_dotenv

load_dotenv()

BEIJING_TZ = timezone(timedelta(hours=8))

UPBIT_BASE = "https://api.upbit.com"
BINANCE_BASE = "https://fapi.binance.com"
TELEGRAM_BASE = "https://api.telegram.org"


def now_bj():
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


def parse_symbols(raw: str):
    items = [x.strip().upper() for x in raw.split(",") if x.strip()]
    result = []
    for item in items:
        if not item.endswith("USDT"):
            item = f"{item}USDT"
        result.append(item)
    return result


SYMBOLS = parse_symbols(
    os.getenv(
        "SYMBOLS",
        "SOLUSDT,DOGEUSDT,ANKRUSDT,ADAUSDT,KITEUSDT,STEEMUSDT,"
        "XLMUSDT,CHZUSDT,TRXUSDT,BCHUSDT,LINKUSDT,AKTUSDT,"
        "CPOOLUSDT,SUIUSDT,WLDUSDT,SIGNUSDT"
    )
)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
LOOP_SECONDS = int(os.getenv("LOOP_SECONDS", "20"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "600"))


class Bot:
    def __init__(self):
        self.session = None
        self.last_premium = {}
        self.last_alert_ts = {}

    async def send(self, text):
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
                raise RuntimeError(f"Telegram send failed: {r.status} {body}")

    async def get_upbit_price(self, symbol):
        base = symbol.replace("USDT", "")
        url = f"{UPBIT_BASE}/v1/ticker"
        params = {"markets": f"KRW-{base}"}

        async with self.session.get(url, params=params) as r:
            if r.status >= 400:
                return None
            data = await r.json()
            if not data:
                return None
            return float(data[0]["trade_price"])

    async def get_usdt_krw(self):
        url = f"{UPBIT_BASE}/v1/ticker"
        params = {"markets": "KRW-USDT"}

        async with self.session.get(url, params=params) as r:
            data = await r.json()
            if not data:
                raise RuntimeError("无法获取 Upbit KRW-USDT")
            return float(data[0]["trade_price"])

    async def get_binance_price(self, symbol):
        url = f"{BINANCE_BASE}/fapi/v1/ticker/price"
        params = {"symbol": symbol}

        async with self.session.get(url, params=params) as r:
            if r.status >= 400:
                return None
            data = await r.json()
            if "price" not in data:
                return None
            return float(data["price"])

    async def get_candles(self, symbol):
        base = symbol.replace("USDT", "")
        url = f"{UPBIT_BASE}/v1/candles/minutes/1"
        params = {"market": f"KRW-{base}", "count": 10}

        async with self.session.get(url, params=params) as r:
            if r.status >= 400:
                return None
            return await r.json()

    def get_signal(self, premium, premium_change, lead, volume_ratio, momentum):
        long_score = 0
        short_score = 0

        if premium > 1.5:
            long_score += 1
        if premium_change > 0.3:
            long_score += 1
        if lead > 1.0:
            long_score += 1
        if volume_ratio > 2.0:
            long_score += 1
        if momentum > 0.8:
            long_score += 1

        if premium > 2.0:
            short_score += 1
        if premium_change < -0.3:
            short_score += 1
        if 0 < lead < 0.5:
            short_score += 1
        if momentum < 0:
            short_score += 1
        if volume_ratio > 2.0 and momentum <= 0:
            short_score += 1

        if long_score >= 2 and long_score >= short_score:
            return "🟢 做多"
        if short_score >= 2:
            return "🔴 做空"
        return None

    async def process_symbol(self, symbol, usdt_krw):
        upbit_price = await self.get_upbit_price(symbol)
        if not upbit_price:
            return

        binance_price = await self.get_binance_price(symbol)
        if not binance_price or binance_price <= 0:
            return

        candles = await self.get_candles(symbol)
        if not candles or len(candles) < 6:
            return

        try:
            volumes = [float(c["candle_acc_trade_volume"]) for c in candles[1:]]
            closes = [float(c["trade_price"]) for c in candles[1:]]
            current_volume = float(candles[0]["candle_acc_trade_volume"])
            current_price = float(candles[0]["trade_price"])
        except (KeyError, TypeError, ValueError):
            return

        avg_vol = statistics.mean(volumes) if volumes else 0
        volume_ratio = current_volume / avg_vol if avg_vol > 0 else 0

        recent_closes = closes[:5] if len(closes) >= 5 else closes
        if not recent_closes:
            return
        avg_price = statistics.mean(recent_closes)
        momentum = (current_price / avg_price - 1) * 100 if avg_price > 0 else 0

        upbit_usdt = upbit_price / usdt_krw
        premium = (upbit_usdt / binance_price - 1) * 100
        prev_premium = self.last_premium.get(symbol, premium)
        premium_change = premium - prev_premium
        self.last_premium[symbol] = premium

        lead = premium
        signal = self.get_signal(
            premium=premium,
            premium_change=premium_change,
            lead=lead,
            volume_ratio=volume_ratio,
            momentum=momentum,
        )
        if not signal:
            return

        alert_key = f"{symbol}:{signal}"
        now_ts = time.time()
        if now_ts - self.last_alert_ts.get(alert_key, 0) < COOLDOWN_SECONDS:
            return
        self.last_alert_ts[alert_key] = now_ts

        msg = (
            f"{signal} | {symbol}\n"
            f"时间: {now_bj()} 北京时间\n"
            f"Upbit折算价: {upbit_usdt:.6f}\n"
            f"Binance价: {binance_price:.6f}\n"
            f"泡菜溢价: {premium:.2f}%\n"
            f"溢价变化: {premium_change:+.2f}%\n"
            f"量能比: {volume_ratio:.2f}x\n"
            f"短线动能: {momentum:+.2f}%"
        )

        logging.info(msg.replace("\n", " | "))
        await self.send(msg)

    async def run_once(self):
        usdt_krw = await self.get_usdt_krw()
        for symbol in SYMBOLS:
            try:
                await self.process_symbol(symbol, usdt_krw)
            except Exception as e:
                logging.exception("%s error: %s", symbol, e)

    async def run(self):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            raise ValueError("TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 未设置")

        timeout = aiohttp.ClientTimeout(total=20)
        headers = {
            "User-Agent": "korea-signal-bot/1.0",
            "Accept": "application/json",
        }

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            self.session = session

            logging.info("Starting Korea Signal Bot")
            logging.info("Active symbols: %s", SYMBOLS)

            await self.send(
                f"✅ 机器人启动成功\n"
                f"时间: {now_bj()} 北京时间\n"
                f"监控币种数量: {len(SYMBOLS)}"
            )

            while True:
                try:
                    await self.run_once()
                except Exception as e:
                    logging.exception("run loop error: %s", e)
                    try:
                        await self.send(
                            f"⚠️ 机器人运行异常\n"
                            f"时间: {now_bj()} 北京时间\n"
                            f"错误: {type(e).__name__}: {e}"
                        )
                    except Exception:
                        logging.exception("telegram error notify failed")
                await asyncio.sleep(LOOP_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    asyncio.run(Bot().run())
