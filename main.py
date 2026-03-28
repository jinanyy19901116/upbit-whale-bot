import asyncio
import logging
import math
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BINANCE_FAPI = "https://fapi.binance.com"
BYBIT_API = "https://api.bybit.com"
OKX_API = "https://www.okx.com"
GATE_API = "https://api.gateio.ws/api/v4"
MEXC_FUTURES_API = "https://api.mexc.com/api/v1/contract"

UPBIT_REGION = os.getenv("UPBIT_REGION", "sg")
UPBIT_API = f"https://{UPBIT_REGION}-api.upbit.com"

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
DEFAULT_WATCHLIST = os.getenv(
    "WATCHLIST",
    (
        "SIGNUSDT,KITEUSDT,HYPEUSDT,SIRENUSDT,PHAUSDT,POWERUSDT,SKYAIUSDT,"
        "BARDUSDT,QUSDT,UAIUSDT,HUSDT,ICXUSDT,ROBOUSDT,OGNUSDT,XAIUSDT,IPUSDT,"
        "XAGUSDT,GUSDT,ANKRUSDT,ANIMEUSDT,BANUSDT,GUNUSDT,ZROUSDT,CUSDT,"
        "LIGHTUSDT,CVCUSDT,AVAUSDT"
    ),
)
MIN_SIGNAL_TO_ALERT = float(os.getenv("MIN_SIGNAL_TO_ALERT", "55"))
MIN_EXCHANGES_TO_ALERT = int(os.getenv("MIN_EXCHANGES_TO_ALERT", "2"))
UPBIT_QUOTE = os.getenv("UPBIT_QUOTE", "USDT")
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()
DATABASE_URL = (os.getenv("DATABASE_URL", "") or "").strip()
SNAPSHOT_LOOKBACK_MINUTES = int(os.getenv("SNAPSHOT_LOOKBACK_MINUTES", "60"))
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "90"))
LEADER_LIMIT = int(os.getenv("LEADER_LIMIT", "50"))
AUTO_SCAN_ENABLED = (os.getenv("AUTO_SCAN_ENABLED", "true") or "true").strip().lower() == "true"
AUTO_SCAN_INTERVAL_SECONDS = int(os.getenv("AUTO_SCAN_INTERVAL_SECONDS", "300"))
PORT = int(os.getenv("PORT", "8080"))

SUPPORTED_INTERVALS = {"1m", "5m", "15m", "1h", "4h", "1d"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def pct_change(new: float, old: float) -> float:
    if old == 0:
        return 0.0
    return (new - old) / abs(old) * 100.0


def zscore(value: float, values: List[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / max(len(values), 1)
    std = math.sqrt(variance)
    if std == 0:
        return 0.0
    return (value - mean) / std


def parse_watchlist(raw: str) -> List[str]:
    items = []
    for part in raw.split(","):
        s = part.strip().upper().replace(" ", "")
        if s:
            items.append(s)
    return list(dict.fromkeys(items))


WATCHLIST = parse_watchlist(DEFAULT_WATCHLIST)


def base_asset(usdt_symbol: str) -> str:
    return usdt_symbol[:-4] if usdt_symbol.endswith("USDT") else usdt_symbol


def gate_pair(usdt_symbol: str) -> str:
    return f"{base_asset(usdt_symbol)}_USDT"


def mexc_symbol(usdt_symbol: str) -> str:
    return f"{base_asset(usdt_symbol)}_USDT"


def okx_inst_id(usdt_symbol: str) -> str:
    return f"{base_asset(usdt_symbol)}-USDT-SWAP"


def upbit_market(usdt_symbol: str) -> str:
    return f"{UPBIT_QUOTE}-{base_asset(usdt_symbol)}"


def validate_interval(interval: str) -> str:
    v = (interval or "").strip().lower()
    if v not in SUPPORTED_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的 interval={interval}，支持: {sorted(SUPPORTED_INTERVALS)}",
        )
    return v


def interval_to_seconds(interval: str) -> int:
    mapping = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }
    return mapping[interval]


async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[Dict[str, Any]] = None,
) -> Any:
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


def compute_signal(row: Dict[str, Any], peer_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    volumes = [safe_float(r.get("quote_volume_24h")) for r in peer_rows]
    oi_values = [
        safe_float(r.get("open_interest_value_usd"))
        for r in peer_rows
        if r.get("open_interest_value_usd") is not None
    ]
    changes = [safe_float(r.get("price_change_pct_24h")) for r in peer_rows]

    price_move = safe_float(row.get("price_change_pct_24h"))
    volume_usd = safe_float(row.get("quote_volume_24h"))
    oi_usd = safe_float(row.get("open_interest_value_usd"))

    volume_score = max(0.0, min(30.0, (zscore(volume_usd, volumes) + 1.0) * 9.5))
    oi_score = max(0.0, min(22.0, (zscore(oi_usd, oi_values) + 1.0) * 7.5)) if oi_values else 0.0
    momo_score = max(0.0, min(18.0, abs(price_move) / 1.6))
    trend_bias = 7.0 if price_move > 0 else 0.0

    low_24h = safe_float(row.get("low_price_24h"))
    high_24h = safe_float(row.get("high_price_24h"))
    last = safe_float(row.get("last_price"))
    breakout_ratio = 0.0
    if high_24h > low_24h > 0:
        breakout_ratio = (last - low_24h) / (high_24h - low_24h)
    breakout_score = max(0.0, min(10.0, breakout_ratio * 10.0))

    total = round(volume_score + oi_score + momo_score + trend_bias + breakout_score, 2)

    tags = []
    if volume_score >= 13:
        tags.append("资金放量")
    if oi_score >= 9:
        tags.append("OI活跃")
    if momo_score >= 8:
        tags.append("波动增强")
    if breakout_score >= 7:
        tags.append("接近区间上沿")
    if price_move >= 8:
        tags.append("多头情绪升温")
    if price_move <= -8:
        tags.append("空头情绪升温")

    return {
        **row,
        "signal_score": total,
        "signal_reason": " / ".join(tags) if tags else "暂无强信号",
        "peer_price_change_zscore": round(zscore(price_move, changes), 2) if changes else 0.0,
        "as_of": utc_now_iso(),
    }


async def get_binance(client: httpx.AsyncClient, watchlist: List[str]) -> List[Dict[str, Any]]:
    tickers = await fetch_json(client, f"{BINANCE_FAPI}/fapi/v1/ticker/24hr")
    wanted = set(watchlist)
    rows: Dict[str, Dict[str, Any]] = {}

    for t in tickers:
        symbol = t.get("symbol", "")
        if symbol not in wanted:
            continue
        rows[symbol] = {
            "exchange": "binance",
            "symbol": symbol,
            "market_type": "perp",
            "last_price": safe_float(t.get("lastPrice")),
            "price_change_pct_24h": safe_float(t.get("priceChangePercent")),
            "quote_volume_24h": safe_float(t.get("quoteVolume")),
            "base_volume_24h": safe_float(t.get("volume")),
            "high_price_24h": safe_float(t.get("highPrice")),
            "low_price_24h": safe_float(t.get("lowPrice")),
            "open_interest": None,
            "open_interest_value_usd": None,
            "funding_rate": None,
        }

    async def enrich(symbol: str) -> None:
        try:
            oi = await fetch_json(client, f"{BINANCE_FAPI}/fapi/v1/openInterest", params={"symbol": symbol})
            contracts = safe_float(oi.get("openInterest"))
            rows[symbol]["open_interest"] = contracts
            rows[symbol]["open_interest_value_usd"] = contracts * rows[symbol]["last_price"]
        except Exception:
            logger.exception("Binance OI 获取失败: %s", symbol)

    await asyncio.gather(*(enrich(symbol) for symbol in rows.keys()))
    return list(rows.values())


async def get_bybit(client: httpx.AsyncClient, watchlist: List[str]) -> List[Dict[str, Any]]:
    data = await fetch_json(client, f"{BYBIT_API}/v5/market/tickers", params={"category": "linear"})
    wanted = set(watchlist)
    rows = []

    for t in data.get("result", {}).get("list", []):
        symbol = t.get("symbol", "")
        if symbol not in wanted:
            continue
        rows.append({
            "exchange": "bybit",
            "symbol": symbol,
            "market_type": "perp",
            "last_price": safe_float(t.get("lastPrice")),
            "price_change_pct_24h": safe_float(t.get("price24hPcnt")) * 100.0,
            "quote_volume_24h": safe_float(t.get("turnover24h")),
            "base_volume_24h": safe_float(t.get("volume24h")),
            "high_price_24h": safe_float(t.get("highPrice24h")),
            "low_price_24h": safe_float(t.get("lowPrice24h")),
            "open_interest": safe_float(t.get("openInterest")),
            "open_interest_value_usd": safe_float(t.get("openInterestValue")),
            "funding_rate": safe_float(t.get("fundingRate")),
        })
    return rows


async def get_okx(client: httpx.AsyncClient, watchlist: List[str]) -> List[Dict[str, Any]]:
    data = await fetch_json(client, f"{OKX_API}/api/v5/market/tickers", params={"instType": "SWAP"})
    wanted = {okx_inst_id(s): s for s in watchlist}
    rows: Dict[str, Dict[str, Any]] = {}

    for t in data.get("data", []):
        inst_id = t.get("instId", "")
        if inst_id not in wanted:
            continue
        symbol = wanted[inst_id]
        rows[symbol] = {
            "exchange": "okx",
            "symbol": symbol,
            "market_type": "swap",
            "last_price": safe_float(t.get("last")),
            "price_change_pct_24h": pct_change(safe_float(t.get("last")), safe_float(t.get("open24h"))),
            "quote_volume_24h": safe_float(t.get("volCcy24h")),
            "base_volume_24h": safe_float(t.get("vol24h")),
            "high_price_24h": safe_float(t.get("high24h")),
            "low_price_24h": safe_float(t.get("low24h")),
            "open_interest": None,
            "open_interest_value_usd": None,
            "funding_rate": None,
        }

    async def enrich(symbol: str) -> None:
        try:
            payload = await fetch_json(
                client,
                f"{OKX_API}/api/v5/public/open-interest",
                params={"instId": okx_inst_id(symbol)},
            )
            if payload.get("data"):
                oi_row = payload["data"][0]
                rows[symbol]["open_interest"] = safe_float(oi_row.get("oi"))
                rows[symbol]["open_interest_value_usd"] = safe_float(oi_row.get("oiCcy"))
        except Exception:
            logger.exception("OKX OI 获取失败: %s", symbol)

    await asyncio.gather(*(enrich(symbol) for symbol in rows.keys()))
    return list(rows.values())


async def get_gate(client: httpx.AsyncClient, watchlist: List[str]) -> List[Dict[str, Any]]:
    data = await fetch_json(client, f"{GATE_API}/spot/tickers")
    wanted = {gate_pair(s): s for s in watchlist}
    rows = []

    for t in data:
        pair = t.get("currency_pair", "")
        if pair not in wanted:
            continue
        rows.append({
            "exchange": "gate",
            "symbol": wanted[pair],
            "market_type": "spot",
            "last_price": safe_float(t.get("last")),
            "price_change_pct_24h": safe_float(t.get("change_percentage")),
            "quote_volume_24h": safe_float(t.get("quote_volume")),
            "base_volume_24h": safe_float(t.get("base_volume")),
            "high_price_24h": safe_float(t.get("high_24h") or t.get("high24h")),
            "low_price_24h": safe_float(t.get("low_24h") or t.get("low24h")),
            "open_interest": None,
            "open_interest_value_usd": None,
            "funding_rate": None,
        })
    return rows


async def get_mexc(client: httpx.AsyncClient, watchlist: List[str]) -> List[Dict[str, Any]]:
    data = await fetch_json(client, f"{MEXC_FUTURES_API}/ticker")
    wanted = {mexc_symbol(s): s for s in watchlist}
    rows = []

    for t in data.get("data", []):
        symbol_key = t.get("symbol", "")
        if symbol_key not in wanted:
            continue
        last = safe_float(t.get("lastPrice"))
        hold_vol = safe_float(t.get("holdVol"))
        rows.append({
            "exchange": "mexc",
            "symbol": wanted[symbol_key],
            "market_type": "perp",
            "last_price": last,
            "price_change_pct_24h": safe_float(t.get("riseFallRate")) * 100.0,
            "quote_volume_24h": safe_float(t.get("amount24")),
            "base_volume_24h": safe_float(t.get("volume24")),
            "high_price_24h": safe_float(t.get("high24Price")),
            "low_price_24h": safe_float(t.get("lower24Price")),
            "open_interest": hold_vol,
            "open_interest_value_usd": hold_vol * last if hold_vol and last else None,
            "funding_rate": safe_float(t.get("fundingRate")),
        })
    return rows


async def get_upbit(client: httpx.AsyncClient, watchlist: List[str]) -> List[Dict[str, Any]]:
    markets = [upbit_market(s) for s in watchlist]
    try:
        data = await fetch_json(client, f"{UPBIT_API}/v1/ticker", params={"markets": ",".join(markets)})
    except Exception:
        logger.exception("Upbit 数据获取失败")
        return []

    reverse = {upbit_market(s): s for s in watchlist}
    rows = []

    for t in data:
        market = t.get("market", "")
        if market not in reverse:
            continue
        last = safe_float(t.get("trade_price"))
        prev = safe_float(t.get("prev_closing_price"))
        rows.append({
            "exchange": "upbit",
            "symbol": reverse[market],
            "market_type": f"spot-{UPBIT_QUOTE.lower()}",
            "last_price": last,
            "price_change_pct_24h": pct_change(last, prev),
            "quote_volume_24h": safe_float(t.get("acc_trade_price_24h")),
            "base_volume_24h": safe_float(t.get("acc_trade_volume_24h")),
            "high_price_24h": safe_float(t.get("high_price")),
            "low_price_24h": safe_float(t.get("low_price")),
            "open_interest": None,
            "open_interest_value_usd": None,
            "funding_rate": None,
        })
    return rows


def score_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    scored = [compute_signal(row, rows) for row in rows]
    scored.sort(key=lambda x: x["signal_score"], reverse=True)
    return scored


async def gather_market_state(watchlist: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    timeout = httpx.Timeout(REQUEST_TIMEOUT)
    headers = {"User-Agent": "rotation-scanner-cn/0.8.1"}

    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        tasks = {
            "binance": get_binance(client, watchlist),
            "bybit": get_bybit(client, watchlist),
            "okx": get_okx(client, watchlist),
            "gate": get_gate(client, watchlist),
            "mexc": get_mexc(client, watchlist),
            "upbit": get_upbit(client, watchlist),
        }
        raw_results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    markets: Dict[str, List[Dict[str, Any]]] = {}
    for name, payload in zip(tasks.keys(), raw_results):
        if isinstance(payload, Exception):
            logger.exception("交易所数据获取失败 %s: %s", name, payload)
            markets[name] = []
        else:
            markets[name] = score_rows(payload)

    return markets


def generate_trade_signal(row: Dict[str, Any]) -> str:
    score = safe_float(row.get("avg_signal_score"))
    vol = safe_float(row.get("volume_growth_pct_vs_baseline"))
    oi = safe_float(row.get("oi_growth_pct_vs_baseline"))
    price = safe_float(row.get("price_change_pct_24h_avg"))
    ex_count = len(row.get("exchanges", []))
    signal_growth = safe_float(row.get("signal_growth_vs_baseline"))

    if score >= 60 and vol >= 20 and oi >= 10 and 0 <= price < 25 and ex_count >= 2:
        return "做多买入"

    if score >= 55 and vol >= 10 and 0 <= price < 35 and ex_count >= 2:
        return "做多观察"

    if score >= 60 and vol >= 20 and oi >= 10 and -25 < price <= 0 and ex_count >= 2:
        return "做空买入"

    if score >= 55 and vol >= 10 and -35 < price <= 0 and ex_count >= 2:
        return "做空观察"

    if price > 60 or (price > 35 and (signal_growth < 0 or oi < 0)):
        return "多单止盈"

    if price < -60 or (price < -35 and (signal_growth > 0 or oi > 0)):
        return "空单止盈"

    if score < 40 or signal_growth <= -8 or (vol < -15 and oi < -10):
        return "止损"

    return "观望"


def build_cross_exchange_summary(markets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}

    for ex, rows in markets.items():
        for row in rows:
            symbol = row["symbol"]
            item = merged.setdefault(symbol, {
                "symbol": symbol,
                "exchanges": [],
                "best_signal_score": 0.0,
                "avg_signal_score": 0.0,
                "quote_volume_24h_sum": 0.0,
                "open_interest_value_usd_sum": 0.0,
                "price_change_pct_24h_avg": 0.0,
                "reasons": [],
                "market_types": [],
            })
            item["exchanges"].append(ex)
            item["market_types"].append(row.get("market_type"))
            item["best_signal_score"] = max(item["best_signal_score"], row["signal_score"])
            item["quote_volume_24h_sum"] += safe_float(row.get("quote_volume_24h"))
            item["open_interest_value_usd_sum"] += safe_float(row.get("open_interest_value_usd"))
            item["price_change_pct_24h_avg"] += safe_float(row.get("price_change_pct_24h"))
            item["reasons"].append(f"{ex}:{row['signal_reason']}")

    final_rows = []
    for item in merged.values():
        n = len(item["exchanges"])
        if n:
            item["avg_signal_score"] = round(
                item["best_signal_score"] * 0.55
                + min(n, 6) * 7
                + math.log10(item["quote_volume_24h_sum"] + 1) * 2.3,
                2,
            )
            item["price_change_pct_24h_avg"] = round(item["price_change_pct_24h_avg"] / n, 2)

        item["quote_volume_24h_sum"] = round(item["quote_volume_24h_sum"], 2)
        item["open_interest_value_usd_sum"] = round(item["open_interest_value_usd_sum"], 2)
        item["market_types"] = sorted(set(mt for mt in item["market_types"] if mt))
        item["reasons"] = item["reasons"][:8]
        final_rows.append(item)

    final_rows.sort(key=lambda x: x["avg_signal_score"], reverse=True)
    return final_rows[:LEADER_LIMIT]


class Database:
    def __init__(self, dsn: str):
        self.dsn = (dsn or "").strip()
        self.pool: Optional[asyncpg.Pool] = None
        self.enabled: bool = bool(self.dsn)
        self.last_error: Optional[str] = None

    async def connect(self) -> bool:
        if not self.dsn:
            self.enabled = False
            self.last_error = "DATABASE_URL 为空"
            logger.warning("数据库未启用：DATABASE_URL 为空。")
            return False

        if self.pool is not None:
            return True

        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=1,
                max_size=4,
                timeout=10,
            )
            await self.init_schema()
            self.enabled = True
            self.last_error = None
            logger.info("数据库连接成功。")
            return True
        except Exception as e:
            self.pool = None
            self.enabled = False
            self.last_error = f"{type(e).__name__}: {e}"
            logger.exception("主数据库连接失败。")
            return False

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def init_schema(self) -> None:
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id BIGSERIAL PRIMARY KEY,
                snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                market_type TEXT,
                last_price DOUBLE PRECISION,
                price_change_pct_24h DOUBLE PRECISION,
                quote_volume_24h DOUBLE PRECISION,
                base_volume_24h DOUBLE PRECISION,
                high_price_24h DOUBLE PRECISION,
                low_price_24h DOUBLE PRECISION,
                open_interest DOUBLE PRECISION,
                open_interest_value_usd DOUBLE PRECISION,
                funding_rate DOUBLE PRECISION,
                signal_score DOUBLE PRECISION,
                signal_reason TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_symbol_exchange_time
            ON snapshots(symbol, exchange, snapshot_at DESC);

            CREATE INDEX IF NOT EXISTS idx_snapshots_time
            ON snapshots(snapshot_at DESC);

            CREATE TABLE IF NOT EXISTS alerts_sent (
                id BIGSERIAL PRIMARY KEY,
                sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                symbol TEXT NOT NULL,
                fingerprint TEXT NOT NULL UNIQUE
            );
            """)

    async def insert_snapshots(self, markets: Dict[str, List[Dict[str, Any]]]) -> int:
        if not self.pool:
            return 0

        rows = []
        for _, items in markets.items():
            for r in items:
                rows.append((
                    r.get("exchange"),
                    r.get("symbol"),
                    r.get("market_type"),
                    r.get("last_price"),
                    r.get("price_change_pct_24h"),
                    r.get("quote_volume_24h"),
                    r.get("base_volume_24h"),
                    r.get("high_price_24h"),
                    r.get("low_price_24h"),
                    r.get("open_interest"),
                    r.get("open_interest_value_usd"),
                    r.get("funding_rate"),
                    r.get("signal_score"),
                    r.get("signal_reason"),
                ))

        if not rows:
            return 0

        async with self.pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO snapshots (
                    exchange, symbol, market_type, last_price, price_change_pct_24h, quote_volume_24h,
                    base_volume_24h, high_price_24h, low_price_24h, open_interest, open_interest_value_usd,
                    funding_rate, signal_score, signal_reason
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            """, rows)

        return len(rows)

    async def get_baseline(self, minutes: int) -> Dict[Tuple[str, str], Dict[str, float]]:
        if not self.pool:
            return {}

        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (symbol, exchange)
                    symbol, exchange, quote_volume_24h, open_interest_value_usd, signal_score, snapshot_at
                FROM snapshots
                WHERE snapshot_at <= NOW() - ($1::text || ' minutes')::interval
                ORDER BY symbol, exchange, snapshot_at DESC
            """, str(minutes))

        out: Dict[Tuple[str, str], Dict[str, float]] = {}
        for r in rows:
            out[(r["symbol"], r["exchange"])] = {
                "quote_volume_24h": float(r["quote_volume_24h"] or 0),
                "open_interest_value_usd": float(r["open_interest_value_usd"] or 0),
                "signal_score": float(r["signal_score"] or 0),
            }
        return out

    async def recently_alerted(self, fingerprint: str, cooldown_minutes: int) -> bool:
        if not self.pool:
            return False

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 1
                FROM alerts_sent
                WHERE fingerprint = $1
                  AND sent_at >= NOW() - ($2::text || ' minutes')::interval
                LIMIT 1
            """, fingerprint, str(cooldown_minutes))
            return bool(row)

    async def mark_alert_sent(self, symbol: str, fingerprint: str) -> None:
        if not self.pool:
            return

        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO alerts_sent (symbol, fingerprint)
                VALUES ($1, $2)
                ON CONFLICT (fingerprint) DO NOTHING
            """, symbol, fingerprint)

    async def get_recent_snapshots(self, limit: int = 200) -> List[Dict[str, Any]]:
        if not self.pool:
            return []

        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    id, snapshot_at, exchange, symbol, market_type, last_price,
                    price_change_pct_24h, quote_volume_24h, base_volume_24h,
                    high_price_24h, low_price_24h, open_interest,
                    open_interest_value_usd, funding_rate, signal_score, signal_reason
                FROM snapshots
                ORDER BY snapshot_at DESC, id DESC
                LIMIT $1
            """, limit)

        return [self._snapshot_row_to_dict(r) for r in rows]

    async def get_symbol_history(
        self,
        symbol: str,
        minutes: int = 1440,
        exchange: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        if not self.pool:
            return []

        symbol = symbol.upper().strip()

        async with self.pool.acquire() as conn:
            if exchange:
                rows = await conn.fetch("""
                    SELECT
                        id, snapshot_at, exchange, symbol, market_type, last_price,
                        price_change_pct_24h, quote_volume_24h, base_volume_24h,
                        high_price_24h, low_price_24h, open_interest,
                        open_interest_value_usd, funding_rate, signal_score, signal_reason
                    FROM snapshots
                    WHERE symbol = $1
                      AND exchange = $2
                      AND snapshot_at >= NOW() - ($3::text || ' minutes')::interval
                    ORDER BY snapshot_at ASC, id ASC
                    LIMIT $4
                """, symbol, exchange, str(minutes), limit)
            else:
                rows = await conn.fetch("""
                    SELECT
                        id, snapshot_at, exchange, symbol, market_type, last_price,
                        price_change_pct_24h, quote_volume_24h, base_volume_24h,
                        high_price_24h, low_price_24h, open_interest,
                        open_interest_value_usd, funding_rate, signal_score, signal_reason
                    FROM snapshots
                    WHERE symbol = $1
                      AND snapshot_at >= NOW() - ($2::text || ' minutes')::interval
                    ORDER BY snapshot_at ASC, id ASC
                    LIMIT $3
                """, symbol, str(minutes), limit)

        return [self._snapshot_row_to_dict(r) for r in rows]

    async def get_symbol_history_aggregated(
        self,
        symbol: str,
        minutes: int,
        interval: str,
        exchange: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        if not self.pool:
            return []

        symbol = symbol.upper().strip()
        bucket_seconds = interval_to_seconds(interval)

        async with self.pool.acquire() as conn:
            if exchange:
                rows = await conn.fetch("""
                    WITH base AS (
                        SELECT
                            *,
                            to_timestamp(floor(extract(epoch from snapshot_at) / $4) * $4) AT TIME ZONE 'UTC' AS bucket_ts
                        FROM snapshots
                        WHERE symbol = $1
                          AND exchange = $2
                          AND snapshot_at >= NOW() - ($3::text || ' minutes')::interval
                    ),
                    agg AS (
                        SELECT
                            bucket_ts,
                            exchange,
                            symbol,
                            COUNT(*) AS sample_count,
                            MIN(last_price) AS low_price,
                            MAX(last_price) AS high_price,
                            SUM(COALESCE(quote_volume_24h, 0)) AS volume_sum,
                            AVG(COALESCE(open_interest_value_usd, 0)) AS oi_value_avg,
                            AVG(COALESCE(signal_score, 0)) AS signal_score_avg
                        FROM base
                        GROUP BY bucket_ts, exchange, symbol
                    ),
                    opens AS (
                        SELECT DISTINCT ON (bucket_ts, exchange, symbol)
                            bucket_ts, exchange, symbol, last_price AS open_price
                        FROM base
                        ORDER BY bucket_ts, exchange, symbol, snapshot_at ASC, id ASC
                    ),
                    closes AS (
                        SELECT DISTINCT ON (bucket_ts, exchange, symbol)
                            bucket_ts, exchange, symbol, last_price AS close_price
                        FROM base
                        ORDER BY bucket_ts, exchange, symbol, snapshot_at DESC, id DESC
                    )
                    SELECT
                        agg.bucket_ts,
                        agg.exchange,
                        agg.symbol,
                        opens.open_price,
                        agg.high_price,
                        agg.low_price,
                        closes.close_price,
                        agg.volume_sum,
                        agg.oi_value_avg,
                        agg.signal_score_avg,
                        agg.sample_count
                    FROM agg
                    JOIN opens USING (bucket_ts, exchange, symbol)
                    JOIN closes USING (bucket_ts, exchange, symbol)
                    ORDER BY agg.bucket_ts ASC
                    LIMIT $5
                """, symbol, exchange, str(minutes), bucket_seconds, limit)
            else:
                rows = await conn.fetch("""
                    WITH base AS (
                        SELECT
                            *,
                            to_timestamp(floor(extract(epoch from snapshot_at) / $3) * $3) AT TIME ZONE 'UTC' AS bucket_ts
                        FROM snapshots
                        WHERE symbol = $1
                          AND snapshot_at >= NOW() - ($2::text || ' minutes')::interval
                    ),
                    agg AS (
                        SELECT
                            bucket_ts,
                            exchange,
                            symbol,
                            COUNT(*) AS sample_count,
                            MIN(last_price) AS low_price,
                            MAX(last_price) AS high_price,
                            SUM(COALESCE(quote_volume_24h, 0)) AS volume_sum,
                            AVG(COALESCE(open_interest_value_usd, 0)) AS oi_value_avg,
                            AVG(COALESCE(signal_score, 0)) AS signal_score_avg
                        FROM base
                        GROUP BY bucket_ts, exchange, symbol
                    ),
                    opens AS (
                        SELECT DISTINCT ON (bucket_ts, exchange, symbol)
                            bucket_ts, exchange, symbol, last_price AS open_price
                        FROM base
                        ORDER BY bucket_ts, exchange, symbol, snapshot_at ASC, id ASC
                    ),
                    closes AS (
                        SELECT DISTINCT ON (bucket_ts, exchange, symbol)
                            bucket_ts, exchange, symbol, last_price AS close_price
                        FROM base
                        ORDER BY bucket_ts, exchange, symbol, snapshot_at DESC, id DESC
                    )
                    SELECT
                        agg.bucket_ts,
                        agg.exchange,
                        agg.symbol,
                        opens.open_price,
                        agg.high_price,
                        agg.low_price,
                        closes.close_price,
                        agg.volume_sum,
                        agg.oi_value_avg,
                        agg.signal_score_avg,
                        agg.sample_count
                    FROM agg
                    JOIN opens USING (bucket_ts, exchange, symbol)
                    JOIN closes USING (bucket_ts, exchange, symbol)
                    ORDER BY agg.bucket_ts ASC, agg.exchange ASC
                    LIMIT $4
                """, symbol, str(minutes), bucket_seconds, limit)

        out = []
        for r in rows:
            bucket_ts = r["bucket_ts"]
            if bucket_ts and bucket_ts.tzinfo is None:
                bucket_ts = bucket_ts.replace(tzinfo=timezone.utc)

            out.append({
                "bucket_at": bucket_ts.isoformat() if bucket_ts else None,
                "exchange": r["exchange"],
                "symbol": r["symbol"],
                "open": round(safe_float(r["open_price"]), 8),
                "high": round(safe_float(r["high_price"]), 8),
                "low": round(safe_float(r["low_price"]), 8),
                "close": round(safe_float(r["close_price"]), 8),
                "volume": round(safe_float(r["volume_sum"]), 2),
                "open_interest_value_usd": round(safe_float(r["oi_value_avg"]), 2),
                "avg_signal_score": round(safe_float(r["signal_score_avg"]), 2),
                "sample_count": int(r["sample_count"] or 0),
            })
        return out

    async def get_history_leaders(self, minutes: int = 60, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.pool:
            return []

        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                WITH latest_per_exchange AS (
                    SELECT DISTINCT ON (symbol, exchange)
                        symbol,
                        exchange,
                        snapshot_at,
                        signal_score,
                        quote_volume_24h,
                        open_interest_value_usd,
                        price_change_pct_24h,
                        signal_reason,
                        market_type,
                        last_price
                    FROM snapshots
                    WHERE snapshot_at >= NOW() - ($1::text || ' minutes')::interval
                    ORDER BY symbol, exchange, snapshot_at DESC
                )
                SELECT
                    symbol,
                    ARRAY_AGG(exchange ORDER BY exchange) AS exchanges,
                    MAX(signal_score) AS best_signal_score,
                    AVG(signal_score) AS avg_signal_score_raw,
                    SUM(COALESCE(quote_volume_24h, 0)) AS quote_volume_24h_sum,
                    SUM(COALESCE(open_interest_value_usd, 0)) AS open_interest_value_usd_sum,
                    AVG(COALESCE(price_change_pct_24h, 0)) AS price_change_pct_24h_avg,
                    ARRAY_AGG(signal_reason ORDER BY exchange) AS reasons,
                    ARRAY_AGG(market_type ORDER BY exchange) AS market_types,
                    MAX(snapshot_at) AS latest_snapshot_at
                FROM latest_per_exchange
                GROUP BY symbol
                ORDER BY MAX(signal_score) DESC, SUM(COALESCE(quote_volume_24h, 0)) DESC
                LIMIT $2
            """, str(minutes), limit)

        result = []
        for r in rows:
            latest_snapshot_at = r["latest_snapshot_at"]
            if latest_snapshot_at and latest_snapshot_at.tzinfo is None:
                latest_snapshot_at = latest_snapshot_at.replace(tzinfo=timezone.utc)

            result.append({
                "symbol": r["symbol"],
                "exchanges": list(r["exchanges"] or []),
                "best_signal_score": round(float(r["best_signal_score"] or 0), 2),
                "avg_signal_score_raw": round(float(r["avg_signal_score_raw"] or 0), 2),
                "quote_volume_24h_sum": round(float(r["quote_volume_24h_sum"] or 0), 2),
                "open_interest_value_usd_sum": round(float(r["open_interest_value_usd_sum"] or 0), 2),
                "price_change_pct_24h_avg": round(float(r["price_change_pct_24h_avg"] or 0), 2),
                "reasons": [x for x in (r["reasons"] or []) if x],
                "market_types": sorted(set(x for x in (r["market_types"] or []) if x)),
                "latest_snapshot_at": latest_snapshot_at.isoformat() if latest_snapshot_at else None,
            })
        return result

    def _snapshot_row_to_dict(self, r: asyncpg.Record) -> Dict[str, Any]:
        snapshot_at = r["snapshot_at"]
        if snapshot_at and snapshot_at.tzinfo is None:
            snapshot_at = snapshot_at.replace(tzinfo=timezone.utc)

        return {
            "id": int(r["id"]),
            "snapshot_at": snapshot_at.isoformat() if snapshot_at else None,
            "exchange": r["exchange"],
            "symbol": r["symbol"],
            "market_type": r["market_type"],
            "last_price": safe_float(r["last_price"]),
            "price_change_pct_24h": safe_float(r["price_change_pct_24h"]),
            "quote_volume_24h": safe_float(r["quote_volume_24h"]),
            "base_volume_24h": safe_float(r["base_volume_24h"]),
            "high_price_24h": safe_float(r["high_price_24h"]),
            "low_price_24h": safe_float(r["low_price_24h"]),
            "open_interest": safe_float(r["open_interest"]),
            "open_interest_value_usd": safe_float(r["open_interest_value_usd"]),
            "funding_rate": safe_float(r["funding_rate"]),
            "signal_score": safe_float(r["signal_score"]),
            "signal_reason": r["signal_reason"],
        }


db = Database(DATABASE_URL)
background_scan_task: Optional[asyncio.Task] = None


async def send_telegram_message(client: httpx.AsyncClient, text: str) -> Tuple[bool, Any]:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False, {"error": "TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 未配置"}

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        resp = await client.post(url, json=payload)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        return resp.is_success, data
    except Exception as e:
        return False, {"error": f"{type(e).__name__}: {e}"}


def leader_fingerprint(row: Dict[str, Any]) -> str:
    exchanges = ",".join(sorted(row["exchanges"]))
    signal = row.get("交易信号", "观望")
    score_bucket = int(safe_float(row["avg_signal_score"]) // 5)
    return f"{row['symbol']}|{signal}|{exchanges}|{score_bucket}"


def build_alert_text(candidates: List[Dict[str, Any]]) -> str:
    if not candidates:
        return ""

    emoji_map = {
        "做多买入": "🟢",
        "做多观察": "🟡",
        "做空买入": "🟣",
        "做空观察": "🟠",
        "多单止盈": "🔴",
        "空单止盈": "🔵",
        "止损": "⚫",
        "观望": "⚪",
    }

    lines = [f"🚨 中文交易信号 {utc_now_iso()}"]
    for row in candidates:
        signal = row.get("交易信号", "观望")
        emoji = emoji_map.get(signal, "⚪")
        lines.append(
            f"{emoji} {signal} {row['symbol']}\n"
            f"评分={row['avg_signal_score']} | 交易所={','.join(row['exchanges'])}\n"
            f"24h涨跌={row['price_change_pct_24h_avg']}% | "
            f"量能变化={row.get('volume_growth_pct_vs_baseline')}% | "
            f"OI变化={row.get('oi_growth_pct_vs_baseline')}% | "
            f"评分变化={row.get('signal_growth_vs_baseline')}\n"
        )

    return "\n".join(lines)


def build_telegram_test_text(note: Optional[str] = None) -> str:
    lines = [
        "🧪 Telegram 测试消息",
        f"应用: 资金轮动监控器",
        f"版本: 0.8.1",
        f"时间: {utc_now_iso()}",
        f"watchlist_count: {len(WATCHLIST)}",
        f"database_configured: {bool(DATABASE_URL)}",
        f"telegram_configured: {bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)}",
    ]
    if note:
        lines.append(f"备注: {note}")
    return "\n".join(lines)


def enrich_with_history(
    leaders: List[Dict[str, Any]],
    baseline: Dict[Tuple[str, str], Dict[str, float]],
    markets: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    current_map = {}
    for ex, rows in markets.items():
        for row in rows:
            current_map[(row["symbol"], ex)] = row

    for leader in leaders:
        vol_growth = []
        oi_growth = []
        score_growth = []

        for ex in leader["exchanges"]:
            curr = current_map.get((leader["symbol"], ex))
            prev = baseline.get((leader["symbol"], ex))
            if not curr or not prev:
                continue

            prev_vol = safe_float(prev.get("quote_volume_24h"))
            curr_vol = safe_float(curr.get("quote_volume_24h"))
            prev_oi = safe_float(prev.get("open_interest_value_usd"))
            curr_oi = safe_float(curr.get("open_interest_value_usd"))
            prev_score = safe_float(prev.get("signal_score"))
            curr_score = safe_float(curr.get("signal_score"))

            if prev_vol > 0:
                vol_growth.append((curr_vol - prev_vol) / prev_vol * 100.0)
            if prev_oi > 0:
                oi_growth.append((curr_oi - prev_oi) / prev_oi * 100.0)
            if prev_score > 0:
                score_growth.append(curr_score - prev_score)

        leader["volume_growth_pct_vs_baseline"] = round(sum(vol_growth) / len(vol_growth), 2) if vol_growth else None
        leader["oi_growth_pct_vs_baseline"] = round(sum(oi_growth) / len(oi_growth), 2) if oi_growth else None
        leader["signal_growth_vs_baseline"] = round(sum(score_growth) / len(score_growth), 2) if score_growth else None

        flags = []
        if leader.get("volume_growth_pct_vs_baseline") is not None and leader["volume_growth_pct_vs_baseline"] >= 20:
            flags.append("量能抬升")
        if leader.get("oi_growth_pct_vs_baseline") is not None and leader["oi_growth_pct_vs_baseline"] >= 10:
            flags.append("OI扩张")
        if leader.get("signal_growth_vs_baseline") is not None and leader["signal_growth_vs_baseline"] >= 6:
            flags.append("评分加速")
        if leader.get("signal_growth_vs_baseline") is not None and leader["signal_growth_vs_baseline"] <= -6:
            flags.append("评分走弱")

        leader["historical_flags"] = flags
        leader["交易信号"] = generate_trade_signal(leader)

    return leaders


def build_alert_candidates(leaders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for row in leaders:
        signal = row.get("交易信号", "观望")
        if signal == "观望":
            continue

        strong_enough = row["avg_signal_score"] >= MIN_SIGNAL_TO_ALERT or signal in {"多单止盈", "空单止盈", "止损"}
        multi_ex = len(row["exchanges"]) >= MIN_EXCHANGES_TO_ALERT or signal in {"多单止盈", "空单止盈", "止损"}

        if strong_enough and multi_ex:
            out.append(row)

    priority = {
        "做多买入": 1,
        "做空买入": 2,
        "做多观察": 3,
        "做空观察": 4,
        "多单止盈": 5,
        "空单止盈": 6,
        "止损": 7,
        "观望": 99,
    }

    out.sort(key=lambda x: (priority.get(x.get("交易信号", "观望"), 99), -x["avg_signal_score"]))
    return out[:8]


async def scan_once(save: bool = True) -> Dict[str, Any]:
    markets = await gather_market_state(WATCHLIST)

    if save and db.pool:
        try:
            await db.insert_snapshots(markets)
        except Exception as e:
            db.last_error = f"{type(e).__name__}: {e}"
            logger.exception("快照写入失败")

    leaders = build_cross_exchange_summary(markets)

    baseline = {}
    if db.pool:
        try:
            baseline = await db.get_baseline(SNAPSHOT_LOOKBACK_MINUTES)
        except Exception as e:
            db.last_error = f"{type(e).__name__}: {e}"
            logger.exception("基线读取失败")

    leaders = enrich_with_history(leaders, baseline, markets)
    candidates = build_alert_candidates(leaders)

    return {
        "generated_at": utc_now_iso(),
        "watchlist": WATCHLIST,
        "leaders_cross_exchange": leaders,
        "alert_candidates": candidates,
        "by_exchange": markets,
        "history_enabled": bool(db.pool),
        "database_configured": bool(DATABASE_URL),
        "database_error": db.last_error,
        "lookback_minutes": SNAPSHOT_LOOKBACK_MINUTES,
    }


async def periodic_scan_loop() -> None:
    logger.info(
        "自动扫描任务启动 enabled=%s interval=%ss",
        AUTO_SCAN_ENABLED,
        AUTO_SCAN_INTERVAL_SECONDS,
    )

    while True:
        try:
            payload = await scan_once(save=True)
            logger.info(
                "自动扫描完成 generated_at=%s alert_candidates=%s",
                payload.get("generated_at"),
                len(payload.get("alert_candidates", [])),
            )
        except asyncio.CancelledError:
            logger.info("自动扫描任务已取消")
            raise
        except Exception:
            logger.exception("自动扫描任务执行失败")

        await asyncio.sleep(max(30, AUTO_SCAN_INTERVAL_SECONDS))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global background_scan_task

    ok = await db.connect()
    if not ok:
        logger.warning("主程序：数据库不可用。应用将继续运行，但不会使用 Postgres。错误=%s", db.last_error)

    if AUTO_SCAN_ENABLED:
        background_scan_task = asyncio.create_task(periodic_scan_loop())
        logger.info("已创建自动扫描后台任务")
    else:
        logger.info("自动扫描已关闭")

    yield

    if background_scan_task:
        background_scan_task.cancel()
        try:
            await background_scan_task
        except asyncio.CancelledError:
            pass
        background_scan_task = None

    await db.close()


app = FastAPI(
    title="资金轮动监控器",
    version="0.8.1",
    lifespan=lifespan,
)


@app.get("/")
async def root() -> Dict[str, Any]:
    return {
        "name": app.title,
        "version": app.version,
        "port": PORT,
        "watchlist_count": len(WATCHLIST),
        "history_enabled": bool(db.pool),
        "database_configured": bool(DATABASE_URL),
        "supported_exchanges": ["binance", "bybit", "okx", "gate", "mexc", "upbit"],
        "supported_history_intervals": sorted(SUPPORTED_INTERVALS),
        "endpoints": [
            "/health",
            "/watchlist",
            "/scan",
            "/scan?symbol=SIRENUSDT",
            "/scan?store=false",
            "/alerts/preview",
            "/alerts/send",
            "/telegram/test",
            "/telegram/test?note=hello",
            "/history/status",
            "/history/latest?limit=200",
            "/history/symbol?symbol=SIRENUSDT&minutes=1440",
            "/history/symbol?symbol=SIRENUSDT&minutes=1440&exchange=binance",
            "/history/symbol?symbol=SIRENUSDT&minutes=1440&interval=5m",
            "/history/leaders?minutes=60&limit=20",
        ],
        "now": utc_now_iso(),
    }


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "watchlist_count": len(WATCHLIST),
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "database_configured": bool(DATABASE_URL),
        "database_connected": bool(db.pool),
        "database_error": db.last_error,
        "upbit_region": UPBIT_REGION,
        "upbit_quote": UPBIT_QUOTE,
        "auto_scan_enabled": AUTO_SCAN_ENABLED,
        "auto_scan_interval_seconds": AUTO_SCAN_INTERVAL_SECONDS,
        "now": utc_now_iso(),
    }


@app.get("/watchlist")
async def get_watchlist() -> Dict[str, Any]:
    return {"watchlist": WATCHLIST, "count": len(WATCHLIST)}


@app.get("/history/status")
async def history_status() -> Dict[str, Any]:
    if not db.pool:
        return {
            "history_enabled": False,
            "database_configured": bool(DATABASE_URL),
            "database_error": db.last_error,
        }

    async with db.pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM snapshots")
        latest = await conn.fetchval("SELECT MAX(snapshot_at) FROM snapshots")

    if latest and latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)

    return {
        "history_enabled": True,
        "snapshot_count": count,
        "latest_snapshot_at": latest.isoformat() if latest else None,
    }


@app.get("/history/latest")
async def history_latest(
    limit: int = Query(default=200, ge=1, le=2000),
) -> JSONResponse:
    if not db.pool:
        return JSONResponse({
            "history_enabled": False,
            "database_configured": bool(DATABASE_URL),
            "database_error": db.last_error,
            "rows": [],
        })

    rows = await db.get_recent_snapshots(limit=limit)
    return JSONResponse({
        "history_enabled": True,
        "count": len(rows),
        "limit": limit,
        "rows": rows,
    })


@app.get("/history/symbol")
async def history_symbol(
    symbol: str = Query(..., description="例如 SIRENUSDT"),
    minutes: int = Query(default=1440, ge=1, le=60 * 24 * 30),
    exchange: Optional[str] = Query(default=None),
    interval: Optional[str] = Query(default=None, description="可选: 1m,5m,15m,1h,4h,1d"),
    limit: int = Query(default=1000, ge=1, le=5000),
) -> JSONResponse:
    if not db.pool:
        return JSONResponse({
            "history_enabled": False,
            "database_configured": bool(DATABASE_URL),
            "database_error": db.last_error,
            "symbol": symbol.upper().strip(),
            "exchange": exchange,
            "rows": [],
        })

    symbol = symbol.upper().strip()
    exchange = exchange.lower().strip() if exchange else None

    if interval:
        interval = validate_interval(interval)
        rows = await db.get_symbol_history_aggregated(
            symbol=symbol,
            minutes=minutes,
            interval=interval,
            exchange=exchange,
            limit=limit,
        )
        if not rows:
            raise HTTPException(status_code=404, detail=f"未找到 {symbol} 的聚合历史数据")

        return JSONResponse({
            "history_enabled": True,
            "symbol": symbol,
            "exchange": exchange,
            "minutes": minutes,
            "interval": interval,
            "count": len(rows),
            "first_bucket_at": rows[0]["bucket_at"],
            "latest_bucket_at": rows[-1]["bucket_at"],
            "rows": rows,
        })

    rows = await db.get_symbol_history(
        symbol=symbol,
        minutes=minutes,
        exchange=exchange,
        limit=limit,
    )

    if not rows:
        raise HTTPException(status_code=404, detail=f"未找到 {symbol} 的历史快照数据")

    return JSONResponse({
        "history_enabled": True,
        "symbol": symbol,
        "exchange": exchange,
        "minutes": minutes,
        "count": len(rows),
        "first_snapshot_at": rows[0]["snapshot_at"],
        "latest_snapshot_at": rows[-1]["snapshot_at"],
        "rows": rows,
    })


@app.get("/history/leaders")
async def history_leaders(
    minutes: int = Query(default=60, ge=1, le=60 * 24 * 30),
    limit: int = Query(default=20, ge=1, le=200),
) -> JSONResponse:
    if not db.pool:
        return JSONResponse({
            "history_enabled": False,
            "database_configured": bool(DATABASE_URL),
            "database_error": db.last_error,
            "rows": [],
        })

    rows = await db.get_history_leaders(minutes=minutes, limit=limit)
    return JSONResponse({
        "history_enabled": True,
        "minutes": minutes,
        "count": len(rows),
        "rows": rows,
    })


@app.get("/scan")
async def scan(
    symbol: Optional[str] = Query(default=None),
    store: bool = Query(default=True),
) -> JSONResponse:
    payload = await scan_once(save=store)

    if symbol:
        symbol = symbol.upper().strip()
        exact = []
        for ex, rows in payload["by_exchange"].items():
            for row in rows:
                if row["symbol"] == symbol:
                    exact.append({"exchange": ex, **row})

        if not exact:
            raise HTTPException(status_code=404, detail=f"未在监控列表或当前交易所结果中找到 {symbol}")

        exact.sort(key=lambda x: x["signal_score"], reverse=True)
        return JSONResponse({
            "symbol": symbol,
            "matches": exact,
            "cross_exchange_summary": next(
                (x for x in payload["leaders_cross_exchange"] if x["symbol"] == symbol),
                None,
            ),
            "generated_at": payload["generated_at"],
            "history_enabled": payload["history_enabled"],
            "database_configured": payload["database_configured"],
            "database_error": payload["database_error"],
        })

    return JSONResponse(payload)


@app.get("/alerts/preview")
async def alerts_preview() -> JSONResponse:
    payload = await scan_once(save=True)
    text = build_alert_text(payload["alert_candidates"])
    return JSONResponse({
        "candidate_count": len(payload["alert_candidates"]),
        "preview": text,
        "candidates": payload["alert_candidates"],
        "generated_at": payload["generated_at"],
        "database_configured": payload["database_configured"],
        "database_error": payload["database_error"],
    })


@app.post("/alerts/send")
@app.get("/alerts/send")
async def alerts_send() -> JSONResponse:
    payload = await scan_once(save=True)
    candidates = []
    skipped = []

    for row in payload["alert_candidates"]:
        fingerprint = leader_fingerprint(row)
        if db.pool:
            try:
                if await db.recently_alerted(fingerprint, ALERT_COOLDOWN_MINUTES):
                    skipped.append({"symbol": row["symbol"], "reason": "冷却中"})
                    continue
            except Exception as e:
                db.last_error = f"{type(e).__name__}: {e}"
                logger.exception("冷却检测失败")

        candidates.append(row)

    text = build_alert_text(candidates)
    if not text:
        return JSONResponse({
            "sent": False,
            "reason": "当前没有新的中文交易信号。",
            "skipped": skipped,
            "generated_at": payload["generated_at"],
            "database_configured": payload["database_configured"],
            "database_error": payload["database_error"],
        })

    async with httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT)) as client:
        ok, telegram_payload = await send_telegram_message(client, text)

    if ok and db.pool:
        for row in candidates:
            try:
                await db.mark_alert_sent(row["symbol"], leader_fingerprint(row))
            except Exception as e:
                db.last_error = f"{type(e).__name__}: {e}"
                logger.exception("告警记录写入失败")

    return JSONResponse({
        "sent": ok,
        "preview": text,
        "sent_symbols": [x["symbol"] for x in candidates],
        "skipped": skipped,
        "telegram_response": telegram_payload,
        "generated_at": payload["generated_at"],
        "database_configured": payload["database_configured"],
        "database_error": payload["database_error"],
    })


@app.post("/telegram/test")
@app.get("/telegram/test")
async def telegram_test(
    note: Optional[str] = Query(default=None, description="附加测试备注"),
) -> JSONResponse:
    text = build_telegram_test_text(note=note)

    async with httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT)) as client:
        ok, telegram_payload = await send_telegram_message(client, text)

    return JSONResponse({
        "sent": ok,
        "preview": text,
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "telegram_response": telegram_payload,
        "generated_at": utc_now_iso(),
    })
