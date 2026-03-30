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


SYMBOLS = os.getenv(
    "SYMBOLS",
    "SOLUSDT,DOGEUSDT,ANKRUSDT,ADAUSDT,KITEUSDT,STEEMUSDT,XLMUSDT,CHZUSDT,TRXUSDT,BCHUSDT,LINKUSDT,AKTUSDT,CPOOLUSDT,SUIUSDT,WLDUSDT,SIGNUSDT"
).split(",")


TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


class Bot:

    def __init__(self):
        self.session = aiohttp.ClientSession()
        self.last_alert = {}

    async def send(self, text):
        url = f"{TELEGRAM_BASE}/bot{TELEGRAM_TOKEN}/sendMessage"
        await self.session.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text
        })

    async def get_upbit_price(self, symbol):
        base = symbol.replace("USDT", "")
        url = f"{UPBIT_BASE}/v1/ticker?markets=KRW-{base}"
        async with self.session.get(url) as r:
            data = await r.json()
            if not data:
                return None
            return float(data[0]["trade_price"])

    async def get_usdt_krw(self):
        url = f"{UPBIT_BASE}/v1/ticker?markets=KRW-USDT"
        async with self.session.get(url) as r:
            data = await r.json()
            return float(data[0]["trade_price"])

    async def get_binance_price(self, symbol):
        url = f"{BINANCE_BASE}/fapi/v1/ticker/price?symbol={symbol}"
        async with self.session.get(url) as r:
            data = await r.json()
            return float(data["price"])

    async def get_candles(self, symbol):
        base = symbol.replace("USDT", "")
        url = f"{UPBIT_BASE}/v1/candles/minutes/1?market=KRW-{base}&count=10"
        async with self.session.get(url) as r:
            return await r.json()

    def get_signal(self, premium, premium_change, lead, volume_ratio, momentum):

        long_score = 0
        short_score = 0

        # 做多
        if premium > 1.5:
            long_score += 1
        if premium_change > 0.3:
            long_score += 1
        if lead > 1:
            long_score += 1
        if volume_ratio > 2:
            long_score += 1
        if momentum > 0.8:
            long_score += 1

        # 做空
        if premium > 2:
            short_score += 1
        if premium_change < -0.3:
            short_score += 1
        if lead < 0.5:
            short_score += 1
        if momentum < 0:
            short_score += 1

        if long_score >= 2 and long_score >= short_score:
            return "🟢 做多"
        elif short_score >= 2:
            return "🔴 做空"
        return None

    async def run_once(self):

        usdt_krw = await self.get_usdt_krw()

        for symbol in SYMBOLS:

            try:
                upbit_price = await self.get_upbit_price(symbol)
                if not upbit_price:
                    continue

                binance_price = await self.get_binance_price(symbol)
                candles = await self.get_candles(symbol)

                volumes = [c["candle_acc_trade_volume"] for c in candles[1:]]
                closes = [c["trade_price"] for c in candles[1:]]

                avg_vol = statistics.mean(volumes)
                cur_vol = candles[0]["candle_acc_trade_volume"]

                volume_ratio = cur_vol / avg_vol if avg_vol else 0

                avg_price = statistics.mean(closes[:5])
                momentum = (candles[0]["trade_price"] / avg_price - 1) * 100

                upbit_usdt = upbit_price / usdt_krw
                premium = (upbit_usdt / binance_price - 1) * 100

                prev = self.last_alert.get(symbol, premium)
                premium_change = premium - prev
                self.last_alert[symbol] = premium

                lead = premium

                signal = self.get_signal(
                    premium, premium_change, lead, volume_ratio, momentum
                )

                if not signal:
                    continue

                now = time.time()
                key = f"{symbol}_{signal}"

                if key in self.last_alert and now - self.last_alert[key] < 600:
                    continue

                self.last_alert[key] = now

                msg = (
                    f"{signal} | {symbol}\n"
                    f"时间: {now_bj()} 北京时间\n"
                    f"溢价: {premium:.2f}%\n"
                    f"变化: {premium_change:+.2f}%\n"
                    f"量能: {volume_ratio:.2f}x\n"
                    f"动能: {momentum:+.2f}%"
                )

                print(msg)
                await self.send(msg)

            except Exception as e:
                logging.error(f"{symbol} error: {e}")

    async def run(self):
        print("Starting Korea Signal Bot")

        while True:
            await self.run_once()
            await asyncio.sleep(20)


if __name__ == "__main__":
    bot = Bot()
    asyncio.run(bot.run())
