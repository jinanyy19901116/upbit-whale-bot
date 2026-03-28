import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI

# =========================================================
# 配置
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger("main")

app = FastAPI(title="Market Scanner", version="1.0.0")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "300"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))

ENABLE_GATE = True
ENABLE_MEXC = True
ENABLE_UPBIT = True

# 你要求监控的币
RAW_WATCHLIST = [
    "SIGNUSDT", "KITEUSDT", "HYPEUSDT", "SIRNUSDT", "PHAUSDT", "POWERUSDT",
    "SKYAIUSDT", "BARDUSDT", "QUSDT", "UAIUSDT", "HUSDT", "ICXUSDT",
    "ROBOUSDT", "OGNUSDT", "XAIUSDT", "IPUSDT", "XAGUSDT", "GUSDT",
    "ANKRUSDT", "ANIMEUSDT", "BANUSDT", "GUNUSDT", "ZROUSDT", "CUSDT",
    "LIGHTUSDT", "CVCUSDT", "AVAUSDT",
]

# 可选：仅日志展示
AUTO_SCAN_ENABLED = os.getenv("AUTO_SCAN_ENABLED", "true").lower() in ("1", "true", "yes", "on")

# Upbit Global 不同地区有不同 base URL；你日志里已经在用 sg
UPBIT_BASE_URL = os.getenv("UPBIT_BASE_URL", "https://sg-api.upbit.com")

# =========================================================
# 工具函数
# =========================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def split_usdt_pair(symbol: str) -> str:
    """
    SIGNUSDT -> SIGN
    """
    s = symbol.strip().upper()
    if not s.endswith("USDT"):
        raise ValueError(f"watchlist symbol must end with USDT: {symbol}")
    return s[:-4]

def to_internal_symbol(base: str) -> str:
    return f"{base}USDT"

def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default

def first_non_none(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

def chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]

async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[dict] = None,
) -> Any:
    resp = await client.get(url, params=params, follow_redirects=False)
    if resp.status_code in (301, 302, 307, 308):
        location = resp.headers.get("location", "")
        raise RuntimeError(f"redirected status={resp.status_code} location={location}")
    resp.raise_for_status()
    return resp.json()

def normalize_result(
    *,
    exchange: str,
    market_type: str,
    base: str,
    raw_symbol: str,
    last_price: Optional[float],
    volume_24h: Optional[float],
    open_interest: Optional[float] = None,
    funding_rate: Optional[float] = None,
    extra: Optional[dict] = None,
) -> Dict[str, Any]:
    payload = {
        "exchange": exchange,
        "market_type": market_type,   # futures / spot
        "symbol": to_internal_symbol(base),
        "base": base,
        "quote": "USDT",
        "raw_symbol": raw_symbol,
        "last_price": last_price,
        "volume_24h": volume_24h,
        "open_interest": open_interest,
        "funding_rate": funding_rate,
        "timestamp": utc_now_iso(),
    }
    if extra:
        payload["extra"] = extra
    return payload

WATCHLIST_BASES = [split_usdt_pair(x) for x in RAW_WATCHLIST]
WATCHLIST_BASE_SET = set(WATCHLIST_BASES)

# =========================================================
# Gate Futures
# 官方 futures ticker 路径为 /futures/{settle}/tickers
# =========================================================

GATE_FUTURES_TICKERS_URL = "https://api.gateio.ws/api/v4/futures/usdt/tickers"

def build_gate_contract(base: str) -> str:
    return f"{base}_USDT"

async def scan_gate_futures(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    data = await fetch_json(client, GATE_FUTURES_TICKERS_URL)
    if not isinstance(data, list):
        logger.warning("gate futures returned unexpected payload type=%s", type(data).__name__)
        return []

    wanted_contracts = {build_gate_contract(base): base for base in WATCHLIST_BASES}
    results: List[Dict[str, Any]] = []

    for item in data:
        contract = str(first_non_none(item, ["contract", "name", "symbol"], "")).upper()
        if contract not in wanted_contracts:
            continue

        base = wanted_contracts[contract]
        last_price = safe_float(first_non_none(item, ["last", "last_price", "mark_price"]))
        volume_24h = safe_float(first_non_none(item, ["volume_24h_quote", "volume_24h_base", "volume_24h", "volume"]))
        funding_rate = safe_float(first_non_none(item, ["funding_rate", "funding_rate_indicative"]))
        open_interest = safe_float(first_non_none(item, ["total_size", "open_interest"]))

        results.append(
            normalize_result(
                exchange="gate",
                market_type="futures",
                base=base,
                raw_symbol=contract,
                last_price=last_price,
                volume_24h=volume_24h,
                open_interest=open_interest,
                funding_rate=funding_rate,
                extra={"source": "gate_futures_tickers"}
            )
        )

    found = {x["base"] for x in results}
    missing = sorted(WATCHLIST_BASE_SET - found)
    if missing:
        logger.info("gate futures 未匹配到: %s", ",".join(missing))

    return results

# =========================================================
# MEXC Futures
# 官方 futures 文档包含 Get Ticker (Contract Market Data)
# 你日志里已验证 /api/v1/contract/ticker 在当前环境可用
# =========================================================

MEXC_CONTRACT_TICKER_URL = "https://api.mexc.com/api/v1/contract/ticker"

def possible_mexc_symbols(base: str) -> List[str]:
    # 尽量兼容不同返回格式
    return [
        f"{base}_USDT",
        f"{base}USDT",
        f"{base}-USDT",
        f"{base}/USDT",
    ]

async def scan_mexc_futures(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    payload = await fetch_json(client, MEXC_CONTRACT_TICKER_URL)

    # 常见情况：{"success": true, "data": [...]}
    if isinstance(payload, dict):
        data = payload.get("data", [])
    elif isinstance(payload, list):
        data = payload
    else:
        logger.warning("mexc futures returned unexpected payload type=%s", type(payload).__name__)
        return []

    wanted_map: Dict[str, str] = {}
    for base in WATCHLIST_BASES:
        for s in possible_mexc_symbols(base):
            wanted_map[s.upper()] = base

    results: List[Dict[str, Any]] = []

    for item in data:
        raw_symbol = str(first_non_none(item, ["symbol", "contractCode", "contract_code"], "")).upper()
        if raw_symbol not in wanted_map:
            continue

        base = wanted_map[raw_symbol]
        last_price = safe_float(first_non_none(item, ["lastPrice", "last_price", "last", "fairPrice", "fair_price"]))
        volume_24h = safe_float(first_non_none(item, ["amount24", "volume24", "turnover24", "volume", "amount"]))
        funding_rate = safe_float(first_non_none(item, ["fundingRate", "funding_rate"]))
        open_interest = safe_float(first_non_none(item, ["holdVol", "hold_vol", "openInterest", "open_interest"]))

        results.append(
            normalize_result(
                exchange="mexc",
                market_type="futures",
                base=base,
                raw_symbol=raw_symbol,
                last_price=last_price,
                volume_24h=volume_24h,
                open_interest=open_interest,
                funding_rate=funding_rate,
                extra={"source": "mexc_contract_ticker"}
            )
        )

    found = {x["base"] for x in results}
    missing = sorted(WATCHLIST_BASE_SET - found)
    if missing:
        logger.info("mexc futures 未匹配到: %s", ",".join(missing))

    return results

# =========================================================
# Upbit Spot
# Upbit Global 提供 market list 和 ticker 接口
# 现货市场代码通常是 USDT-BTC 这种格式
# =========================================================

UPBIT_MARKETS_URL = f"{UPBIT_BASE_URL}/v1/market/all"
UPBIT_TICKER_URL = f"{UPBIT_BASE_URL}/v1/ticker"

async def get_upbit_spot_markets(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    data = await fetch_json(client, UPBIT_MARKETS_URL, params={"isDetails": "false"})
    if not isinstance(data, list):
        logger.warning("upbit market/all returned unexpected payload type=%s", type(data).__name__)
        return []
    return data

def select_upbit_usdt_markets(markets: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    返回:
      {
        "USDT-BTC": "BTC",
        "USDT-ICX": "ICX",
      }
    """
    selected: Dict[str, str] = {}
    for item in markets:
        market = str(item.get("market", "")).upper()
        if not market.startswith("USDT-"):
            continue

        parts = market.split("-", 1)
        if len(parts) != 2:
            continue

        base = parts[1]
        if base in WATCHLIST_BASE_SET:
            selected[market] = base

    return selected

async def get_upbit_tickers(client: httpx.AsyncClient, market_codes: List[str]) -> List[Dict[str, Any]]:
    if not market_codes:
        return []

    results: List[Dict[str, Any]] = []

    # Upbit ticker 支持 markets 参数，分批更稳一点
    for batch in chunked(market_codes, 50):
        part = await fetch_json(client, UPBIT_TICKER_URL, params={"markets": ",".join(batch)})
        if isinstance(part, list):
            results.extend(part)

    return results

async def scan_upbit_spot(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    markets = await get_upbit_spot_markets(client)
    market_to_base = select_upbit_usdt_markets(markets)

    if not market_to_base:
        logger.warning("upbit 当前区域没有匹配到 watchlist 的 USDT 现货市场")
        return []

    tickers = await get_upbit_tickers(client, list(market_to_base.keys()))
    results: List[Dict[str, Any]] = []

    for item in tickers:
        raw_symbol = str(item.get("market", "")).upper()
        base = market_to_base.get(raw_symbol)
        if not base:
            continue

        last_price = safe_float(first_non_none(item, ["trade_price", "closing_price"]))
        volume_24h = safe_float(first_non_none(item, ["acc_trade_price_24h", "acc_trade_volume_24h"]))

        results.append(
            normalize_result(
                exchange="upbit",
                market_type="spot",
                base=base,
                raw_symbol=raw_symbol,
                last_price=last_price,
                volume_24h=volume_24h,
                open_interest=None,
                funding_rate=None,
                extra={"source": "upbit_spot_ticker"}
            )
        )

    found = {x["base"] for x in results}
    missing = sorted(WATCHLIST_BASE_SET - found)
    if missing:
        logger.info("upbit spot 未匹配到: %s", ",".join(missing))

    return results

# =========================================================
# 聚合扫描
# =========================================================

async def run_market_scan() -> Dict[str, Any]:
    timeout = httpx.Timeout(REQUEST_TIMEOUT)
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)

    headers = {
        "User-Agent": "market-scanner/1.0",
        "Accept": "application/json",
    }

    all_results: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    async with httpx.AsyncClient(timeout=timeout, limits=limits, headers=headers) as client:
        if ENABLE_GATE:
            try:
                gate_rows = await scan_gate_futures(client)
                all_results.extend(gate_rows)
                logger.info("gate futures matched=%s", len(gate_rows))
            except Exception as e:
                logger.warning("gate futures scan failed: %s", e)
                errors.append({"exchange": "gate", "error": str(e)})

        if ENABLE_MEXC:
            try:
                mexc_rows = await scan_mexc_futures(client)
                all_results.extend(mexc_rows)
                logger.info("mexc futures matched=%s", len(mexc_rows))
            except Exception as e:
                logger.warning("mexc futures scan failed: %s", e)
                errors.append({"exchange": "mexc", "error": str(e)})

        if ENABLE_UPBIT:
            try:
                upbit_rows = await scan_upbit_spot(client)
                all_results.extend(upbit_rows)
                logger.info("upbit spot matched=%s", len(upbit_rows))
            except Exception as e:
                logger.warning("upbit spot scan failed: %s", e)
                errors.append({"exchange": "upbit", "error": str(e)})

    # 以内部 symbol 聚合，方便看每个币在哪些交易所有数据
    merged: Dict[str, List[Dict[str, Any]]] = {}
    for row in all_results:
        merged.setdefault(row["symbol"], []).append(row)

    summary = {
        "generated_at": utc_now_iso(),
        "watchlist_count": len(RAW_WATCHLIST),
        "matched_symbol_count": len(merged),
        "result_count": len(all_results),
        "errors": errors,
        "data": merged,
    }
    return summary

# =========================================================
# FastAPI
# =========================================================

@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "generated_at": utc_now_iso(),
        "auto_scan_enabled": AUTO_SCAN_ENABLED,
        "watchlist_count": len(RAW_WATCHLIST),
        "enabled_exchanges": {
            "gate_futures": ENABLE_GATE,
            "mexc_futures": ENABLE_MEXC,
            "upbit_spot": ENABLE_UPBIT,
        },
    }

@app.get("/watchlist")
async def watchlist() -> Dict[str, Any]:
    return {
        "count": len(RAW_WATCHLIST),
        "raw_watchlist": RAW_WATCHLIST,
        "bases": WATCHLIST_BASES,
    }

@app.get("/scan")
async def scan_once() -> Dict[str, Any]:
    result = await run_market_scan()
    logger.info(
        "scan completed generated_at=%s matched_symbol_count=%s result_count=%s",
        result["generated_at"],
        result["matched_symbol_count"],
        result["result_count"],
    )
    return result

# =========================================================
# 后台定时任务
# =========================================================

_scan_task: Optional[asyncio.Task] = None

async def auto_scan_loop() -> None:
    logger.info("自动扫描任务启动 enabled=%s interval=%ss", AUTO_SCAN_ENABLED, SCAN_INTERVAL_SECONDS)
    while True:
        try:
            result = await run_market_scan()
            logger.info(
                "自动扫描完成 generated_at=%s matched_symbol_count=%s result_count=%s errors=%s",
                result["generated_at"],
                result["matched_symbol_count"],
                result["result_count"],
                len(result["errors"]),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("自动扫描异常: %s", e)

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

@app.on_event("startup")
async def on_startup() -> None:
    global _scan_task
    logger.info(
        "启动配置 host=0.0.0.0 port=8080 auto_scan_enabled=%s watchlist_count=%s",
        AUTO_SCAN_ENABLED,
        len(RAW_WATCHLIST),
    )

    if AUTO_SCAN_ENABLED:
        _scan_task = asyncio.create_task(auto_scan_loop())
        logger.info("已创建自动扫描后台任务")

@app.on_event("shutdown")
async def on_shutdown() -> None:
    global _scan_task
    if _scan_task:
        _scan_task.cancel()
        try:
            await _scan_task
        except asyncio.CancelledError:
            pass

# =========================================================
# 本地启动
# =========================================================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
        reload=False,
    )
