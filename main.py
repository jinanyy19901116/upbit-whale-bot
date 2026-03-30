import asyncio
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import aiohttp
from dotenv import load_dotenv

load_dotenv()

BEIJING_TZ = timezone(timedelta(hours=8))

OKX_BASE = "https://www.okx.com"
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


def symbol_to_okx_inst_id(symbol: str) -> str:
    if not symbol.endswith("USDT"):
        raise ValueError(f"unsupported symbol: {symbol}")
    base = symbol[:-4]
    return f"{base}-USDT"


SYMBOLS = parse_symbols(
    os.getenv(
        "SYMBOLS",
        "XRPUSDT,ANKRUSDT,CHZUSDT,XLMUSDT,STEEMUSDT,"
        "SUIUSDT,WLDUSDT,KITEUSDT,SEIUSDT,AKTUSDT,"
        "CPOOLUSDT,TRUMPUSDT,ANIMEUSDT"
    )
)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

LOOP_SECONDS = int(os.getenv("LOOP_SECONDS", "8"))
STATE_COOLDOWN_SECONDS = int(os.getenv("STATE_COOLDOWN_SECONDS", "120"))
RESET_AFTER_SECONDS = int(os.getenv("RESET_AFTER_SECONDS", "600"))

OKX_TRADES_LIMIT = int(os.getenv("OKX_TRADES_LIMIT", "100"))

SMALL_CAP_BIG_ORDER = float(os.getenv("SMALL_CAP_BIG_ORDER", "80000"))
LARGE_CAP_BIG_ORDER = float(os.getenv("LARGE_CAP_BIG_ORDER", "150000"))

FOLLOW_WINDOW_SECONDS = int(os.getenv("FOLLOW_WINDOW_SECONDS", "8"))
FOLLOW_MULTIPLIER = float(os.getenv("FOLLOW_MULTIPLIER", "1.5"))
MIN_FOLLOW_COUNT = int(os.getenv("MIN_FOLLOW_COUNT", "3"))
OPPOSITE_MAX_RATIO = float(os.getenv("OPPOSITE_MAX_RATIO", "0.6"))


class Bot:
    def __init__(self):
        self.session = None
        self.okx_symbols = set()

        # 方向状态：NONE / LONG_ACTIVE / SHORT_ACTIVE
        self.last_state = {}
        self.last_state_change_ts = {}

        # 为了避免同一笔触发反复提示
        self.last_trigger_fingerprint = {}

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
                raise RuntimeError(f"Telegram send failed: {r.status} {body}")

    async def load_okx_spot_symbols(self):
        url = f"{OKX_BASE}/api/v5/public/instruments"
        params = {"instType": "SPOT"}

        async with self.session.get(url, params=params) as r:
            text = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"OKX spot instruments failed: {r.status} {text[:500]}")

            try:
                data = await r.json()
            except Exception as e:
                raise RuntimeError(
                    f"OKX spot instruments JSON parse failed: {e}; body={text[:500]}"
                )

            if str(data.get("code")) != "0":
                raise RuntimeError(f"OKX spot instruments returned error: {data}")

            symbols = set()
            for item in data.get("data", []):
                inst_id = item.get("instId")
                state = item.get("state")
                if inst_id and state == "live":
                    symbols.add(inst_id)

            return symbols

    async def get_okx_price(self, symbol: str):
        inst_id = symbol_to_okx_inst_id(symbol)
        url = f"{OKX_BASE}/api/v5/market/ticker"
        params = {"instId": inst_id}

        async with self.session.get(url, params=params) as r:
            text = await r.text()

            if r.status >= 400:
                logging.warning(
                    "OKX spot request failed | symbol=%s instId=%s status=%s body=%s",
                    symbol, inst_id, r.status, text[:500]
                )
                return None

            try:
                data = await r.json()
            except Exception:
                logging.warning(
                    "OKX spot bad JSON | symbol=%s instId=%s body=%s",
                    symbol, inst_id, text[:500]
                )
                return None

            if str(data.get("code")) != "0":
                logging.warning(
                    "OKX spot returned error | symbol=%s instId=%s body=%s",
                    symbol, inst_id, data
                )
                return None

            rows = data.get("data", [])
            if not rows:
                return None

            try:
                return float(rows[0]["last"])
            except (KeyError, TypeError, ValueError):
                logging.warning(
                    "OKX spot invalid last price | symbol=%s instId=%s body=%s",
                    symbol, inst_id, data
                )
                return None

    async def get_okx_trades(self, symbol: str):
        inst_id = symbol_to_okx_inst_id(symbol)
        url = f"{OKX_BASE}/api/v5/market/trades"
        params = {"instId": inst_id, "limit": str(OKX_TRADES_LIMIT)}

        async with self.session.get(url, params=params) as r:
            text = await r.text()

            if r.status >= 400:
                logging.warning(
                    "OKX trades request failed | symbol=%s instId=%s status=%s body=%s",
                    symbol, inst_id, r.status, text[:500]
                )
                return None

            try:
                data = await r.json()
            except Exception:
                logging.warning(
                    "OKX trades bad JSON | symbol=%s instId=%s body=%s",
                    symbol, inst_id, text[:500]
                )
                return None

            if str(data.get("code")) != "0":
                logging.warning(
                    "OKX trades returned error | symbol=%s instId=%s body=%s",
                    symbol, inst_id, data
                )
                return None

            return data.get("data", [])

    def get_big_order_threshold(self, symbol: str) -> float:
        if symbol in {"XRPUSDT", "SUIUSDT", "WLDUSDT"}:
            return LARGE_CAP_BIG_ORDER
        return SMALL_CAP_BIG_ORDER

    def normalize_trade(self, trade: dict):
        try:
            px = float(trade["px"])
            sz = float(trade["sz"])
            side = str(trade["side"]).lower()
            ts = int(trade["ts"])
            trade_id = str(trade["tradeId"])
            value = px * sz
        except (KeyError, TypeError, ValueError):
            return None

        return {
            "price": px,
            "size": sz,
            "side": side,
            "ts": ts,
            "trade_id": trade_id,
            "value": value,
        }

    def find_trigger_trade(self, symbol: str, trades: list):
        threshold = self.get_big_order_threshold(symbol)

        normalized = []
        for t in trades:
            x = self.normalize_trade(t)
            if x:
                normalized.append(x)

        if not normalized:
            return None, []

        # OKX返回通常是最新在前，这里按时间升序处理，更符合“触发后跟进”
        normalized.sort(key=lambda x: x["ts"])

        trigger = None
        for t in normalized:
            if t["value"] >= threshold:
                trigger = t

        return trigger, normalized

    def analyze_follow_through(self, trigger: dict, trades: list):
        if not trigger:
            return None

        trigger_side = trigger["side"]
        trigger_ts = trigger["ts"]
        window_end = trigger_ts + FOLLOW_WINDOW_SECONDS * 1000

        follow_same_value = 0.0
        follow_opposite_value = 0.0
        follow_same_count = 0
        follow_opposite_count = 0

        for t in trades:
            if t["ts"] <= trigger_ts:
                continue
            if t["ts"] > window_end:
                continue

            if t["side"] == trigger_side:
                follow_same_value += t["value"]
                follow_same_count += 1
            else:
                follow_opposite_value += t["value"]
                follow_opposite_count += 1

        result = {
            "trigger_side": trigger_side,
            "trigger_value": trigger["value"],
            "trigger_price": trigger["price"],
            "trigger_ts": trigger["ts"],
            "trigger_trade_id": trigger["trade_id"],
            "follow_same_value": follow_same_value,
            "follow_same_count": follow_same_count,
            "follow_opposite_value": follow_opposite_value,
            "follow_opposite_count": follow_opposite_count,
        }

        return result

    def get_signal_from_follow(self, analysis: dict):
        if not analysis:
            return None

        trigger_value = analysis["trigger_value"]
        follow_same_value = analysis["follow_same_value"]
        follow_same_count = analysis["follow_same_count"]
        follow_opposite_value = analysis["follow_opposite_value"]
        trigger_side = analysis["trigger_side"]

        if follow_same_value < trigger_value * FOLLOW_MULTIPLIER:
            return None

        if follow_same_count < MIN_FOLLOW_COUNT:
            return None

        if follow_opposite_value > follow_same_value * OPPOSITE_MAX_RATIO:
            return None

        if trigger_side == "buy":
            return "LONG"
        if trigger_side == "sell":
            return "SHORT"
        return None

    def should_reset_long(self, analysis: dict, last_change_ts: float):
        if time.time() - last_change_ts > RESET_AFTER_SECONDS:
            return True
        if not analysis:
            return False
        if analysis["trigger_side"] == "sell":
            return True
        if analysis["follow_same_value"] < analysis["trigger_value"] * 0.5:
            return True
        return False

    def should_reset_short(self, analysis: dict, last_change_ts: float):
        if time.time() - last_change_ts > RESET_AFTER_SECONDS:
            return True
        if not analysis:
            return False
        if analysis["trigger_side"] == "buy":
            return True
        if analysis["follow_same_value"] < analysis["trigger_value"] * 0.5:
            return True
        return False

    async def process_symbol(self, symbol: str):
        inst_id = symbol_to_okx_inst_id(symbol)
        if inst_id not in self.okx_symbols:
            logging.info("%s skipped: not in OKX spot instruments", symbol)
            return

        trades_raw = await self.get_okx_trades(symbol)
        if not trades_raw:
            logging.info("%s skipped: no OKX trades", symbol)
            return

        trigger, trades = self.find_trigger_trade(symbol, trades_raw)
        if not trigger:
            logging.info("%s skipped: no trigger big trade", symbol)
            return

        analysis = self.analyze_follow_through(trigger, trades)
        signal = self.get_signal_from_follow(analysis)

        state = self.last_state.get(symbol, "NONE")
        state_change_ts = self.last_state_change_ts.get(symbol, 0)
        fingerprint = f'{analysis["trigger_trade_id"]}:{analysis["trigger_side"]}'
        last_fp = self.last_trigger_fingerprint.get(symbol)

        logging.info(
            "%s follow | triggerSide=%s triggerValue=%.0f followSame=%.0f sameCount=%s "
            "followOpp=%.0f oppCount=%s signal=%s state=%s",
            symbol,
            analysis["trigger_side"],
            analysis["trigger_value"],
            analysis["follow_same_value"],
            analysis["follow_same_count"],
            analysis["follow_opposite_value"],
            analysis["follow_opposite_count"],
            signal,
            state,
        )

        # reset
        if state == "LONG_ACTIVE" and self.should_reset_long(analysis, state_change_ts):
            self.last_state[symbol] = "NONE"
            self.last_state_change_ts[symbol] = time.time()
            logging.info("%s state reset from LONG_ACTIVE", symbol)
            state = "NONE"

        if state == "SHORT_ACTIVE" and self.should_reset_short(analysis, state_change_ts):
            self.last_state[symbol] = "NONE"
            self.last_state_change_ts[symbol] = time.time()
            logging.info("%s state reset from SHORT_ACTIVE", symbol)
            state = "NONE"

        if time.time() - self.last_state_change_ts.get(symbol, 0) < STATE_COOLDOWN_SECONDS and state == "NONE":
            return

        if last_fp == fingerprint:
            return

        okx_price = await self.get_okx_price(symbol)

        if signal == "LONG" and state != "LONG_ACTIVE":
            self.last_state[symbol] = "LONG_ACTIVE"
            self.last_state_change_ts[symbol] = time.time()
            self.last_trigger_fingerprint[symbol] = fingerprint

            msg = (
                f"🟢 做多启动 | {symbol}\n"
                f"时间: {now_bj()} 北京时间\n"
                f"OKX现货价: {okx_price:.6f}\n"
                f"触发方向: BUY\n"
                f"触发大单: {analysis['trigger_value']:.0f} USDT\n"
                f"后续买单总额: {analysis['follow_same_value']:.0f} USDT\n"
                f"后续买单笔数: {analysis['follow_same_count']}\n"
                f"后续卖单总额: {analysis['follow_opposite_value']:.0f} USDT\n"
                f"后续卖单笔数: {analysis['follow_opposite_count']}\n"
                f"大单阈值: {self.get_big_order_threshold(symbol):.0f} USDT\n"
                f"判断: 大额成交后，更多市价买单继续跟进"
            )
            logging.info("SEND | %s", msg.replace("\n", " | "))
            await self.send(msg)
            return

        if signal == "SHORT" and state != "SHORT_ACTIVE":
            self.last_state[symbol] = "SHORT_ACTIVE"
            self.last_state_change_ts[symbol] = time.time()
            self.last_trigger_fingerprint[symbol] = fingerprint

            msg = (
                f"🔴 做空启动 | {symbol}\n"
                f"时间: {now_bj()} 北京时间\n"
                f"OKX现货价: {okx_price:.6f}\n"
                f"触发方向: SELL\n"
                f"触发大单: {analysis['trigger_value']:.0f} USDT\n"
                f"后续卖单总额: {analysis['follow_same_value']:.0f} USDT\n"
                f"后续卖单笔数: {analysis['follow_same_count']}\n"
                f"后续买单总额: {analysis['follow_opposite_value']:.0f} USDT\n"
                f"后续买单笔数: {analysis['follow_opposite_count']}\n"
                f"大单阈值: {self.get_big_order_threshold(symbol):.0f} USDT\n"
                f"判断: 大额成交后，更多市价卖单继续跟进"
            )
            logging.info("SEND | %s", msg.replace("\n", " | "))
            await self.send(msg)
            return

    async def run_once(self):
        for symbol in SYMBOLS:
            try:
                await self.process_symbol(symbol)
            except Exception as e:
                logging.exception("%s error: %s", symbol, e)

    async def run(self):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            raise ValueError("TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 未设置")

        timeout = aiohttp.ClientTimeout(total=20)
        headers = {
            "User-Agent": "okx-follow-through-bot/1.0",
            "Accept": "application/json",
        }

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            self.session = session

            logging.info("Starting OKX Follow Through Bot")
            logging.info("Configured symbols: %s", SYMBOLS)

            self.okx_symbols = await self.load_okx_spot_symbols()
            logging.info("Loaded OKX spot symbols: %s", len(self.okx_symbols))

            test_symbol = "XRPUSDT"
            test_price = await self.get_okx_price(test_symbol)
            logging.info("Startup OKX spot test | %s = %s", test_symbol, test_price)

            await self.send(
                f"✅ 机器人启动成功\n"
                f"时间: {now_bj()} 北京时间\n"
                f"信号模式: 大额成交 + 后续市价单跟进\n"
                f"监控币种数量: {len(SYMBOLS)}\n"
                f"监控列表: {', '.join(SYMBOLS)}"
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
