
import asyncio
import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="Watchlist Rotation Scanner", version="0.3.0")

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
    "SIGNUSDT,KITEUSDT,HYPEUSDT,SIRENUSDT,PHAUSDT,POWERUSDT,SKYAIUSDT,BARDUSDT,QUSDT,UAIUSDT,HUSDT,ICXUSDT,ROBOUSDT,OGNUSDT,XAIUSDT,IPUSDT,XAGUSDT,GUSDT,ANKRUSDT,ANIMEUSDT,BANUSDT,GUNUSDT,ZROUSDT,CUSDT,LIGHTUSDT,CVCUSDT,AVAUSDT",
)
MIN_SIGNAL_TO_ALERT = float(os.getenv("MIN_SIGNAL_TO_ALERT", "55"))
MIN_EXCHANGES_TO_ALERT = int(os.getenv("MIN_EXCHANGES_TO_ALERT", "2"))
UPBIT_QUOTE = os.getenv("UPBIT_QUOTE", "USDT")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
SNAPSHOT_LOOKBACK_MINUTES = int(os.getenv("SNAPSHOT_LOOKBACK_MINUTES", "60"))
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "90"))
LEADER_LIMIT = int(os.getenv("LEADER_LIMIT", "50"))


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


async def fetch_json(client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


def compute_signal(row: Dict[str, Any], peer_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    volumes = [safe_float(r.get("quote_volume_24h")) for r in peer_rows]
    oi_values = [safe_float(r.get("open_interest_value_usd")) for r in peer_rows if r.get("open_interest_value_usd") is not None]
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
        tags.append("接近突破")
    if price_move >= 8:
        tags.append("情绪升温")
    if price_move <= -8:
        tags.append("负向异动")

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

    async def enrich(symbol: str):
        try:
            oi = await fetch_json(client, f"{BINANCE_FAPI}/fapi/v1/openInterest", params={"symbol": symbol})
            contracts = safe_float(oi.get("openInterest"))
            rows[symbol]["open_interest"] = contracts
            rows[symbol]["open_interest_value_usd"] = contracts * rows[symbol]["last_price"]
        except Exception:
            pass

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

    async def enrich(symbol: str):
        try:
            payload = await fetch_json(client, f"{OKX_API}/api/v5/public/open-interest", params={"instId": okx_inst_id(symbol)})
            if payload.get("data"):
                oi_row = payload["data"][0]
                rows[symbol]["open_interest"] = safe_float(oi_row.get("oi"))
                rows[symbol]["open_interest_value_usd"] = safe_float(oi_row.get("oiCcy"))
        except Exception:
            pass

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
    headers = {"User-Agent": "watchlist-rotation-scanner/0.3"}
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
            markets[name] = []
        else:
            markets[name] = score_rows(payload)
    return markets


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
                item["best_signal_score"] * 0.55 + min(n, 6) * 7 + math.log10(item["quote_volume_24h_sum"] + 1) * 2.3,
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
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if not self.dsn:
            return
        if self.pool is None:
            self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=4)
            await self.init_schema()

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
        out = {}
        for r in rows:
            out[(r["symbol"], r["exchange"])] = {
                "quote_volume_24h": safe_float(r["quote_volume_24h"]),
                "open_interest_value_usd": safe_float(r["open_interest_value_usd"]),
                "signal_score": safe_float(r["signal_score"]),
            }
        return out

    async def recently_alerted(self, fingerprint: str, cooldown_minutes: int) -> bool:
        if not self.pool:
            return False
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 1
                FROM alerts_sent
                WHERE fingerprint = $1 AND sent_at >= NOW() - ($2::text || ' minutes')::interval
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


db = Database(DATABASE_URL)


@app.on_event("startup")
async def on_startup() -> None:
    await db.connect()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await db.close()


def enrich_with_history(leaders: List[Dict[str, Any]], baseline: Dict[Tuple[str, str], Dict[str, float]], markets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
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

        reasons = []
        if leader.get("volume_growth_pct_vs_baseline") is not None and leader["volume_growth_pct_vs_baseline"] >= 20:
            reasons.append("量能抬升")
        if leader.get("oi_growth_pct_vs_baseline") is not None and leader["oi_growth_pct_vs_baseline"] >= 10:
            reasons.append("OI扩张")
        if leader.get("signal_growth_vs_baseline") is not None and leader["signal_growth_vs_baseline"] >= 6:
            reasons.append("评分加速")
        leader["historical_flags"] = reasons
    return leaders


def build_alert_candidates(leaders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for row in leaders:
        strong = row["avg_signal_score"] >= MIN_SIGNAL_TO_ALERT
        multi_ex = len(row["exchanges"]) >= MIN_EXCHANGES_TO_ALERT
        improving = (
            (row.get("signal_growth_vs_baseline") is not None and row["signal_growth_vs_baseline"] >= 4)
            or (row.get("volume_growth_pct_vs_baseline") is not None and row["volume_growth_pct_vs_baseline"] >= 15)
            or (row.get("oi_growth_pct_vs_baseline") is not None and row["oi_growth_pct_vs_baseline"] >= 8)
        )
        if strong and multi_ex and improving:
            out.append(row)
    out.sort(key=lambda x: (x["avg_signal_score"], len(x["exchanges"])), reverse=True)
    return out[:8]


async def send_telegram_message(client: httpx.AsyncClient, text: str) -> Tuple[bool, Any]:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False, {"error": "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing"}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    resp = await client.post(url, json=payload)
    return resp.is_success, resp.json()


def leader_fingerprint(row: Dict[str, Any]) -> str:
    exchanges = ",".join(sorted(row["exchanges"]))
    score_bucket = int(row["avg_signal_score"] // 5)
    return f"{row['symbol']}|{exchanges}|{score_bucket}"


def build_alert_text(candidates: List[Dict[str, Any]]) -> str:
    if not candidates:
        return ""
    lines = [f"🚨 Rotation Scanner Alert {utc_now_iso()}"]
    for row in candidates:
        flags = ",".join(row.get("historical_flags") or [])
        lines.append(
            f"- {row['symbol']} | score={row['avg_signal_score']} | ex={','.join(row['exchanges'])} | "
            f"chg24h={row['price_change_pct_24h_avg']}% | vol24h=${row['quote_volume_24h_sum']:,.0f} | "
            f"volΔ={row.get('volume_growth_pct_vs_baseline')}% | oiΔ={row.get('oi_growth_pct_vs_baseline')}% | flags={flags}"
        )
    return "\n".join(lines)


async def scan_once(save: bool = True) -> Dict[str, Any]:
    markets = await gather_market_state(WATCHLIST)
    if save and DATABASE_URL:
        await db.insert_snapshots(markets)

    leaders = build_cross_exchange_summary(markets)
    baseline = await db.get_baseline(SNAPSHOT_LOOKBACK_MINUTES) if DATABASE_URL else {}
    leaders = enrich_with_history(leaders, baseline, markets)
    candidates = build_alert_candidates(leaders)

    return {
        "generated_at": utc_now_iso(),
        "watchlist": WATCHLIST,
        "leaders_cross_exchange": leaders,
        "alert_candidates": candidates,
        "by_exchange": markets,
        "history_enabled": bool(DATABASE_URL),
        "lookback_minutes": SNAPSHOT_LOOKBACK_MINUTES,
    }


@app.get("/")
async def root() -> Dict[str, Any]:
    return {
        "name": app.title,
        "version": app.version,
        "watchlist_count": len(WATCHLIST),
        "history_enabled": bool(DATABASE_URL),
        "supported_exchanges": ["binance", "bybit", "okx", "gate", "mexc", "upbit"],
        "endpoints": [
            "/health", "/watchlist", "/scan", "/scan?symbol=SIRENUSDT", "/scan/store=false",
            "/alerts/preview", "/alerts/send", "/history/status"
        ],
        "now": utc_now_iso(),
    }


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "watchlist_count": len(WATCHLIST),
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "history_enabled": bool(DATABASE_URL),
        "upbit_region": UPBIT_REGION,
        "upbit_quote": UPBIT_QUOTE,
        "now": utc_now_iso(),
    }


@app.get("/watchlist")
async def get_watchlist() -> Dict[str, Any]:
    return {"watchlist": WATCHLIST, "count": len(WATCHLIST)}


@app.get("/history/status")
async def history_status() -> Dict[str, Any]:
    if not DATABASE_URL or not db.pool:
        return {"history_enabled": False}
    async with db.pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM snapshots")
        latest = await conn.fetchval("SELECT MAX(snapshot_at) FROM snapshots")
    return {"history_enabled": True, "snapshot_count": count, "latest_snapshot_at": latest.isoformat() if latest else None}


@app.get("/scan")
async def scan(symbol: Optional[str] = Query(default=None), store: bool = Query(default=True)) -> JSONResponse:
    payload = await scan_once(save=store)

    if symbol:
        symbol = symbol.upper().strip()
        exact = []
        for ex, rows in payload["by_exchange"].items():
            for row in rows:
                if row["symbol"] == symbol:
                    exact.append({"exchange": ex, **row})
        if not exact:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found on configured watchlist or exchanges.")
        exact.sort(key=lambda x: x["signal_score"], reverse=True)
        return JSONResponse({
            "symbol": symbol,
            "matches": exact,
            "cross_exchange_summary": next((x for x in payload["leaders_cross_exchange"] if x["symbol"] == symbol), None),
            "generated_at": payload["generated_at"],
            "history_enabled": payload["history_enabled"],
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
    })


@app.post("/alerts/send")
@app.get("/alerts/send")
async def alerts_send() -> JSONResponse:
    payload = await scan_once(save=True)
    candidates = []
    skipped = []

    for row in payload["alert_candidates"]:
        fingerprint = leader_fingerprint(row)
        if await db.recently_alerted(fingerprint, ALERT_COOLDOWN_MINUTES) if DATABASE_URL else False:
            skipped.append({"symbol": row["symbol"], "reason": "cooldown"})
            continue
        candidates.append(row)

    text = build_alert_text(candidates)
    if not text:
        return JSONResponse({
            "sent": False,
            "reason": "No fresh candidates met thresholds.",
            "skipped": skipped,
            "generated_at": payload["generated_at"],
        })

    async with httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT)) as client:
        ok, telegram_payload = await send_telegram_message(client, text)

    if ok and DATABASE_URL:
        for row in candidates:
            await db.mark_alert_sent(row["symbol"], leader_fingerprint(row))

    return JSONResponse({
        "sent": ok,
        "preview": text,
        "sent_symbols": [x["symbol"] for x in candidates],
        "skipped": skipped,
        "telegram_response": telegram_payload,
        "generated_at": payload["generated_at"],
    })
