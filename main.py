import asyncio
import logging
import math
import os
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

load_dotenv()

BEIJING_TZ = timezone(timedelta(hours=8))
UPBIT_BASE = "https://api.upbit.com"
BINANCE_FAPI_BASE = "https://fapi.binance.com"
TELEGRAM_BASE = "https://api.telegram.org"


def parse_symbols(raw: str) -> List[str]:
    items = [x.strip().upper() for x in raw.split(",") if x.strip()]
    cleaned = []
    for item in items:
        if item.endswith("USDT"):
            cleaned.append(item)
        else:
            cleaned.append(f"{item}USDT")
    return list(dict.fromkeys(cleaned))


def now_bj() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class BotConfig:
    telegram_bot_token: str
    telegram_chat_id: str
    symbols: List[str]
    loop_seconds: int
    premium_threshold_pct: float
    premium_change_threshold_pct: float
    upbit_lead_threshold_pct: float
    volume_spike_ratio: float
    cooldown_seconds: int
    candle_count: int
    log_level: str


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, session: aiohttp.ClientSession):
        self.token = token
        self.chat_id = chat_id
        self.session = session

    async def send(self, text: str) -> None:
        url = f"{TELEGRAM_BASE}/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            body = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Telegram send failed: {resp.status} {body}")


class KoreaSignalBot:
    def __init__(self, config: BotConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.notifier: Optional[TelegramNotifier] = None
        self.last_alert_at: Dict[str, float] = {}
        self.last_premium_by_symbol: Dict[str, float] = {}

    async def __aenter__(self) -> "KoreaSignalBot":
        timeout = aiohttp.ClientTimeout(total=20)
        headers = {
            "User-Agent": "korea-signal-bot/1.0",
            "Accept": "application/json",
        }
        self.session = aiohttp.ClientSession(timeout=timeout, headers=headers)
        self.notifier = TelegramNotifier(
            token=self.config.telegram_bot_token,
            chat_id=self.config.telegram_chat_id,
            session=self.session,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.session:
            await self.session.close()

    async def _get_json(self, url: str, params: Optional[dict] = None) -> dict | list:
        assert self.session is not None
        async with self.session.get(url, params=params) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status} for {url}: {text[:200]}")
            return await resp.json()

    async def check_telegram(self) -> None:
        assert self.session is not None
        url = f"{TELEGRAM_BASE}/bot{self.config.telegram_bot_token}/getMe"
        async with self.session.get(url) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Telegram check failed: {resp.status} {text}")

    async def send_startup_message(self) -> None:
        assert self.notifier is not None
        text = (
            f"✅ 韩国现货情绪监控机器人已启动\n"
            f"时间: {now_bj()} 北京时间\n"
            f"监控币种: {', '.join(self.config.symbols)}\n"
            f"循环间隔: {self.config.loop_seconds}s\n"
            f"规则: 仅推送做多 / 做空信号"
        )
        await self.notifier.send(text)

    async def fetch_upbit_usdt_krw(self) -> float:
        data = await self._get_json(f"{UPBIT_BASE}/v1/ticker", params={"markets": "KRW-USDT"})
        if not isinstance(data, list) or not data:
            raise RuntimeError("Upbit KRW-USDT response invalid")
        return float(data[0]["trade_price"])

    async def fetch_binance_prices(self, symbols: List[str]) -> Dict[str, float]:
        url = f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/price"
        data = await self._get_json(url)
        if not isinstance(data, list):
            raise RuntimeError("Binance ticker response invalid")
        wanted = set(symbols)
        result: Dict[str, float] = {}
        for item in data:
            symbol = item.get("symbol")
            if symbol in wanted:
                result[symbol] = float(item["price"])
        return result

    async def fetch_upbit_tickers(self, symbols: List[str]) -> Dict[str, dict]:
        markets = []
        for symbol in symbols:
            base = symbol.removesuffix("USDT")
            markets.append(f"KRW-{base}")
        data = await self._get_json(f"{UPBIT_BASE}/v1/ticker", params={"markets": ",".join(markets)})
        if not isinstance(data, list):
            raise RuntimeError("Upbit ticker response invalid")
        result: Dict[str, dict] = {}
        for item in data:
            market = item["market"]
            base = market.split("-")[1]
            result[f"{base}USDT"] = item
        return result

    async def fetch_upbit_candles(self, symbol: str, count: int) -> List[dict]:
        base = symbol.removesuffix("USDT")
        market = f"KRW-{base}"
        return await self._get_json(
            f"{UPBIT_BASE}/v1/candles/minutes/1",
            params={"market": market, "count": count},
        )

    def compute_signal(
        self,
        symbol: str,
        upbit_krw_price: float,
        binance_usdt_price: float,
        upbit_usdt_krw: float,
        candles: List[dict],
    ) -> Optional[str]:
        if not candles or len(candles) < 5:
            return None

        upbit_usdt_price = upbit_krw_price / upbit_usdt_krw
        premium_pct = (upbit_usdt_price / binance_usdt_price - 1.0) * 100.0

        # Upbit 1m candles are returned newest first.
        current = candles[0]
        previous = candles[1:]
        volumes = [float(x["candle_acc_trade_volume"]) for x in previous if float(x["candle_acc_trade_volume"]) > 0]
        closes = [float(x["trade_price"]) for x in previous]
        if len(volumes) < 3 or len(closes) < 3:
            return None

        avg_volume = statistics.mean(volumes)
        current_volume = float(current["candle_acc_trade_volume"])
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0.0

        avg_close = statistics.mean(closes[: min(5, len(closes))])
        upbit_momentum_pct = (float(current["trade_price"]) / avg_close - 1.0) * 100.0
        upbit_vs_binance_pct = (upbit_usdt_price / binance_usdt_price - 1.0) * 100.0

        previous_premium = self.last_premium_by_symbol.get(symbol, premium_pct)
        premium_change_pct = premium_pct - previous_premium
        self.last_premium_by_symbol[symbol] = premium_pct

        long_score = 0
        short_score = 0
        long_reasons = []
        short_reasons = []

        # Long logic
        if premium_pct > 1.5:
            long_score += 1
            long_reasons.append(f"泡菜溢价 {premium_pct:.2f}%")
        if premium_change_pct > 0.3:
            long_score += 1
            long_reasons.append(f"溢价上升 {premium_change_pct:+.2f}%")
        if upbit_vs_binance_pct > 1.0:
            long_score += 1
            long_reasons.append(f"Upbit领先 {upbit_vs_binance_pct:.2f}%")
        if volume_ratio > 2.0:
            long_score += 1
            long_reasons.append(f"量能放大 {volume_ratio:.2f}x")
        if upbit_momentum_pct > 0.8:
            long_score += 1
            long_reasons.append(f"短线动能 {upbit_momentum_pct:+.2f}%")

        # Short logic
        if premium_pct > 2.0:
            short_score += 1
            short_reasons.append(f"高溢价 {premium_pct:.2f}%")
        if premium_change_pct < -0.3:
            short_score += 1
            short_reasons.append(f"溢价回落 {premium_change_pct:+.2f}%")
        if 0 < upbit_vs_binance_pct < 0.5:
            short_score += 1
            short_reasons.append(f"Upbit领先不足 {upbit_vs_binance_pct:.2f}%")
        if upbit_momentum_pct < 0:
            short_score += 1
            short_reasons.append(f"短线转弱 {upbit_momentum_pct:+.2f}%")
        if volume_ratio > 2.0 and upbit_momentum_pct <= 0:
            short_score += 1
            short_reasons.append("放量不涨")

        signal = None
        reasons = []
        if long_score >= 2 and long_score >= short_score:
            signal = "🟢 做多"
            reasons = long_reasons
            key = f"{symbol}:LONG"
        elif short_score >= 2:
            signal = "🔴 做空"
            reasons = short_reasons
            key = f"{symbol}:SHORT"
        else:
            return None

        now_ts = time.time()
        if now_ts - self.last_alert_at.get(key, 0) < self.config.cooldown_seconds:
            return None
        self.last_alert_at[key] = now_ts

        return (
            f"{signal} | {symbol}
"
            f"时间: {now_bj()} 北京时间
"
            f"Upbit折算价: {upbit_usdt_price:.6f}
"
            f"Binance价: {binance_usdt_price:.6f}
"
            f"泡菜溢价: {premium_pct:.2f}%
"
            f"溢价变化: {premium_change_pct:+.2f}%
"
            f"量能比: {volume_ratio:.2f}x
"
            f"短线动能: {upbit_momentum_pct:+.2f}%
"
            f"原因: {' | '.join(reasons)}"
        )

    async def process_symbol(
        self,
        symbol: str,
        upbit_ticker: dict,
        upbit_usdt_krw: float,
        binance_prices: Dict[str, float],
    ) -> Optional[str]:
        binance_price = binance_prices.get(symbol)
        if not binance_price or binance_price <= 0:
            logging.warning("Binance price missing for %s", symbol)
            return None

        upbit_krw_price = float(upbit_ticker["trade_price"])
        candles = await self.fetch_upbit_candles(symbol, self.config.candle_count)
        return self.compute_signal(
            symbol=symbol,
            upbit_krw_price=upbit_krw_price,
            binance_usdt_price=binance_price,
            upbit_usdt_krw=upbit_usdt_krw,
            candles=candles,
        )

    async def run_once(self) -> None:
        assert self.notifier is not None
        upbit_usdt_krw, binance_prices, upbit_tickers = await asyncio.gather(
            self.fetch_upbit_usdt_krw(),
            self.fetch_binance_prices(self.config.symbols),
            self.fetch_upbit_tickers(self.config.symbols),
        )

        tasks = []
        for symbol in self.config.symbols:
            ticker = upbit_tickers.get(symbol)
            if not ticker:
                logging.warning("Upbit ticker missing for %s; maybe no KRW market", symbol)
                continue
            tasks.append(self.process_symbol(symbol, ticker, upbit_usdt_krw, binance_prices))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for item in results:
            if isinstance(item, Exception):
                logging.exception("Symbol processing failed: %s", item)
                continue
            if item:
                await self.notifier.send(item)

    async def run_forever(self) -> None:
        assert self.notifier is not None
        await self.check_telegram()
        await self.send_startup_message()

        while True:
            try:
                await self.run_once()
            except Exception as exc:
                logging.exception("Loop error: %s", exc)
                try:
                    await self.notifier.send(
                        f"⚠️ 机器人运行异常\n时间: {now_bj()} 北京时间\n错误: {type(exc).__name__}: {exc}"
                    )
                except Exception:
                    logging.exception("Failed to send Telegram error message")
            await asyncio.sleep(self.config.loop_seconds)


def load_config() -> BotConfig:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    symbols_raw = os.getenv("SYMBOLS", "ONTUSDT,SENTUSDT,SOLUSDT,DOGEUSDT,ANKRUSDT,ADAUSDT,KITEUSDT,STEEMUSDT,XLMUSDT,CHZUSDT,TRUMPUSDT,ANIMEUSDT,SHIBUSDT,SEIUSDT,TRXUSDT,BCHUSDT,LINKUSDT,AKTUSDT,CPOOLUSDT,SUIUSDT,WLDUSDT,SIGNUSDT").strip()

    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN 未设置")
    if not chat_id:
        raise ValueError("TELEGRAM_CHAT_ID 未设置")

    return BotConfig(
        telegram_bot_token=token,
        telegram_chat_id=chat_id,
        symbols=parse_symbols(symbols_raw),
        loop_seconds=int(os.getenv("LOOP_SECONDS", "20")),
        premium_threshold_pct=float(os.getenv("PREMIUM_THRESHOLD_PCT", "2.0")),
        premium_change_threshold_pct=float(os.getenv("PREMIUM_CHANGE_THRESHOLD_PCT", "0.5")),
        upbit_lead_threshold_pct=float(os.getenv("UPBIT_LEAD_THRESHOLD_PCT", "1.2")),
        volume_spike_ratio=float(os.getenv("VOLUME_SPIKE_RATIO", "2.0")),
        cooldown_seconds=int(os.getenv("COOLDOWN_SECONDS", "900")),
        candle_count=int(os.getenv("CANDLE_COUNT", "20")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )


async def main() -> None:
    config = load_config()
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.info("Starting Korea Signal Bot")
    logging.info("Active symbols: %s", config.symbols)
    async with KoreaSignalBot(config) as bot:
        await bot.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
