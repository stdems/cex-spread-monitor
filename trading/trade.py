import asyncio
import aiohttp
import base64
import hashlib
import hmac
import json
import math
import sys
import time
import uuid
import logging
from datetime import datetime, timezone

from core.config import (
    GATE_API_KEY,    GATE_SECRET_KEY,    GATE_LEVERAGE,
    BYBIT_API_KEY,   BYBIT_SECRET_KEY,   BYBIT_LEVERAGE,
    MEXC_API_KEY,    MEXC_SECRET_KEY,    MEXC_LEVERAGE,
    BINGX_API_KEY,   BINGX_SECRET_KEY,   BINGX_LEVERAGE,
    OKX_API_KEY,     OKX_SECRET_KEY,     OKX_PASSPHRASE,     OKX_LEVERAGE,
    KUCOIN_API_KEY,  KUCOIN_SECRET_KEY,  KUCOIN_PASSPHRASE,  KUCOIN_LEVERAGE,
    BITGET_API_KEY,  BITGET_SECRET_KEY,  BITGET_PASSPHRASE,  BITGET_LEVERAGE,
    BINANCE_API_KEY, BINANCE_SECRET_KEY, BINANCE_LEVERAGE,
    TRADE_AMOUNT as amount,
)
from storage.price_store import r as price_store

logger = logging.getLogger(__name__)

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def to_gate_symbol(pair: str) -> str:
    return pair[:-4] + "_USDT" if pair.endswith("USDT") else pair

def to_mexc_symbol(pair: str) -> str:
    return pair[:-4] + "_USDT" if pair.endswith("USDT") else pair

def to_bingx_symbol(pair: str) -> str:
    return pair[:-4] + "-USDT" if pair.endswith("USDT") else pair

def to_okx_symbol(pair: str) -> str:
    return pair[:-4] + "-USDT-SWAP" if pair.endswith("USDT") else pair

def to_kucoin_symbol(pair: str) -> str:
    if pair == "BTCUSDT":
        return "XBTUSDTM"
    return pair + "M" if pair.endswith("USDT") else pair

def _lev(exchange: str) -> int:
    return {
        "GATE": GATE_LEVERAGE,    "BYBIT": BYBIT_LEVERAGE,
        "MEXC": MEXC_LEVERAGE,    "BINGX": BINGX_LEVERAGE,
        "OKX":  OKX_LEVERAGE,     "KUCOIN": KUCOIN_LEVERAGE,
        "BITGET": BITGET_LEVERAGE, "BINANCE": BINANCE_LEVERAGE,
    }.get(exchange.upper(), 2)


async def get_stored_price(pair: str, exchange: str) -> float | None:
    val = await price_store.hget(f"avg_prices:{pair}", exchange.upper())
    return float(val) if val else None

async def resolve_tokens(pair: str, exchange: str, tokens_from_book, leverage: int) -> float | None:
    if tokens_from_book is not None:
        return float(tokens_from_book)
    price = await get_stored_price(pair, exchange)
    if not price:
        logger.warning(f"no price for {exchange}/{pair}, skipping order")
        return None
    return (amount * leverage) / price

def _step_places(step: float) -> int:
    s = f"{step:.10f}".rstrip("0")
    return len(s.split(".")[1]) if "." in s else 0


GATE_BASE = "https://api.gateio.ws"
gate_contracts_info: dict = {}


async def _gate_request(method: str, endpoint: str, query: str = "", body: dict = None):
    body_json = json.dumps(body) if body else ""
    ts = str(int(time.time()))
    body_hash = hashlib.sha512(body_json.encode()).hexdigest()
    sign_str = f"{method}\n{endpoint}\n{query}\n{body_hash}\n{ts}"
    sig = hmac.new(GATE_SECRET_KEY.encode(), sign_str.encode(), hashlib.sha512).hexdigest()
    headers = {"KEY": GATE_API_KEY, "Timestamp": ts, "SIGN": sig, "Content-Type": "application/json"}
    params = dict(p.split("=") for p in query.split("&")) if query else None
    async with aiohttp.ClientSession() as session:
        async with session.request(method, f"{GATE_BASE}{endpoint}",
                                   headers=headers, data=body_json, params=params) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                logger.warning(f"[Gate] error {resp.status}: {data}")
            return data


async def init_gate_contracts():
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{GATE_BASE}/api/v4/futures/usdt/contracts") as resp:
            for item in await resp.json():
                gate_contracts_info[item["name"]] = {
                    "quanto_multiplier": float(item["quanto_multiplier"]),
                    "order_size_min":    float(item["order_size_min"]),
                }

async def gate_set_leverage(contract: str, leverage: int):
    return await _gate_request(
        "POST", f"/api/v4/futures/usdt/positions/{contract}/leverage",
        query=f"leverage={leverage}",
    )

async def gate_order(contract: str, size: float, reduce_only: bool = False):
    return await _gate_request("POST", "/api/v4/futures/usdt/orders", body={
        "contract": contract, "size": size,
        "price": "0", "order_type": "market", "tif": "ioc", "reduce_only": reduce_only,
    })

def fix_gate_size(contract: str, raw_tokens: float) -> float:
    info = gate_contracts_info.get(contract)
    if not info: return 0
    contracts = math.floor(abs(raw_tokens) / info["quanto_multiplier"])
    if contracts < info["order_size_min"]: return 0
    return float(contracts) if raw_tokens > 0 else float(-contracts)


BYBIT_BASE = "https://api.bybit.com"
bybit_contracts_info: dict = {}


async def _bybit_request(method: str, endpoint: str, payload: dict):
    ts = str(int(time.time() * 1000))
    recv_window = "5000"
    body_str = json.dumps(payload)
    sig = hmac.new(BYBIT_SECRET_KEY.encode(),
                   (ts + BYBIT_API_KEY + recv_window + body_str).encode(),
                   hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": BYBIT_API_KEY, "X-BAPI-SIGN": sig,
        "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json",
    }
    async with aiohttp.ClientSession() as session:
        async with session.request(method, f"{BYBIT_BASE}{endpoint}",
                                   headers=headers, json=payload) as resp:
            data = await resp.json()
            if data.get("retCode") != 0:
                logger.warning(f"[Bybit] error: {data.get('retMsg')}")
            return data


async def init_bybit_contract(symbol: str):
    url = f"{BYBIT_BASE}/v5/market/instruments-info?category=linear&symbol={symbol}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            if data["retCode"] == 0 and data["result"]["list"]:
                f = data["result"]["list"][0]["lotSizeFilter"]
                bybit_contracts_info[symbol] = {
                    "minOrderQty": float(f["minOrderQty"]),
                    "qtyStep":     float(f["qtyStep"]),
                }

async def bybit_set_leverage(symbol: str, leverage: int):
    return await _bybit_request("POST", "/v5/position/set-leverage", {
        "category": "linear", "symbol": symbol,
        "buyLeverage": str(leverage), "sellLeverage": str(leverage),
    })

async def bybit_order(symbol: str, qty: float, side: str, reduce_only: bool = False):
    return await _bybit_request("POST", "/v5/order/create", {
        "category": "linear", "symbol": symbol, "side": side,
        "orderType": "Market", "qty": str(qty),
        "reduceOnly": reduce_only, "timeInForce": "IOC",
    })

def fix_bybit_qty(symbol: str, raw_tokens: float) -> float:
    info = bybit_contracts_info.get(symbol)
    if not info: return 0
    step = info["qtyStep"]
    qty = math.floor(abs(raw_tokens) / step) * step
    if qty < info["minOrderQty"]: return 0
    return round(qty, _step_places(step))


MEXC_BASE = "https://contract.mexc.com"
mexc_contracts_info: dict = {}


async def _mexc_request(method: str, endpoint: str, body: dict = None):
    ts = str(int(time.time() * 1000))
    body_json = json.dumps(body) if body else ""
    sig = hmac.new(MEXC_SECRET_KEY.encode(),
                   (MEXC_API_KEY + ts + body_json).encode(),
                   hashlib.sha256).hexdigest()
    headers = {
        "ApiKey": MEXC_API_KEY, "Request-Time": ts,
        "Signature": sig, "Content-Type": "application/json",
    }
    async with aiohttp.ClientSession() as session:
        async with session.request(method, f"{MEXC_BASE}{endpoint}",
                                   headers=headers, data=body_json) as resp:
            data = await resp.json()
            if not data.get("success", True):
                logger.warning(f"[MEXC] error {data.get('code')}: {data.get('message')}")
            return data


async def init_mexc_contract(symbol: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{MEXC_BASE}/api/v1/contract/detail",
                               params={"symbol": symbol}) as resp:
            data = await resp.json()
            for item in data.get("data", []):
                if item["symbol"] == symbol:
                    mexc_contracts_info[symbol] = {
                        "contractSize": float(item["contractSize"]),
                        "minVol":       float(item["minVol"]),
                        "volUnit":      float(item["volUnit"]),
                    }

async def mexc_set_leverage(symbol: str, leverage: int, pos_type: int):
    return await _mexc_request("POST", "/api/v1/private/position/change_leverage", {
        "symbol": symbol, "leverage": leverage, "openType": 2, "positionType": pos_type,
    })

async def mexc_order(symbol: str, side: int, vol: int):
    return await _mexc_request("POST", "/api/v1/private/order/submit", {
        "symbol": symbol, "side": side, "openType": 2, "type": 5, "vol": vol,
    })

def fix_mexc_vol(symbol: str, raw_tokens: float) -> int:
    info = mexc_contracts_info.get(symbol)
    if not info: return 0
    vol_unit = info["volUnit"]
    vol = math.floor(raw_tokens / info["contractSize"] / vol_unit) * vol_unit
    if vol < info["minVol"]: return 0
    return int(vol)


BINGX_BASE = "https://open-api.bingx.com"
bingx_contracts_info: dict = {}


def _bingx_sign(params: dict) -> str:
    qs = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(BINGX_SECRET_KEY.encode(), qs.encode(), hashlib.sha256).hexdigest()

async def _bingx_request(method: str, endpoint: str, params: dict):
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _bingx_sign(params)
    headers = {"X-BX-APIKEY": BINGX_API_KEY}
    async with aiohttp.ClientSession() as session:
        async with session.request(method, f"{BINGX_BASE}{endpoint}",
                                   headers=headers, params=params) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                logger.warning(f"[BingX] error {data.get('code')}: {data.get('msg')}")
            return data


async def init_bingx_contract(symbol: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{BINGX_BASE}/openApi/swap/v2/quote/contracts") as resp:
            data = await resp.json()
            for item in data.get("data", []):
                if item["symbol"] == symbol:
                    bingx_contracts_info[symbol] = {
                        "minQty":            float(item.get("minQty", 0.001)),
                        "stepSize":          float(item.get("stepSize", 0.001)),
                        "quantityPrecision": int(item.get("quantityPrecision", 3)),
                    }

async def bingx_set_leverage(symbol: str, leverage: int):
    for side in ("LONG", "SHORT"):
        await _bingx_request("POST", "/openApi/swap/v2/trade/leverage", {
            "symbol": symbol, "side": side, "leverage": leverage,
        })

async def bingx_order(symbol: str, side: str, position_side: str, quantity: float):
    return await _bingx_request("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol, "side": side,
        "positionSide": position_side,
        "type": "MARKET", "quantity": quantity,
    })

def fix_bingx_qty(symbol: str, raw_tokens: float) -> float:
    info = bingx_contracts_info.get(symbol)
    step = info["stepSize"] if info else 0.001
    min_qty = info["minQty"] if info else 0.001
    places = info["quantityPrecision"] if info else 3
    qty = math.floor(raw_tokens / step) * step
    if qty < min_qty: return 0
    return round(qty, places)


OKX_BASE = "https://www.okx.com"
okx_contracts_info: dict = {}


def _okx_ts() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

def _okx_sign(ts: str, method: str, path: str, body: str) -> str:
    return base64.b64encode(
        hmac.new(OKX_SECRET_KEY.encode(), (ts + method + path + body).encode(),
                 hashlib.sha256).digest()
    ).decode()

async def _okx_request(method: str, path: str, body: dict = None):
    ts = _okx_ts()
    body_json = json.dumps(body) if body else ""
    headers = {
        "OK-ACCESS-KEY":        OKX_API_KEY,
        "OK-ACCESS-SIGN":       _okx_sign(ts, method, path, body_json),
        "OK-ACCESS-TIMESTAMP":  ts,
        "OK-ACCESS-PASSPHRASE": OKX_PASSPHRASE,
        "Content-Type":         "application/json",
    }
    async with aiohttp.ClientSession() as session:
        async with session.request(method, f"{OKX_BASE}{path}",
                                   headers=headers, data=body_json) as resp:
            data = await resp.json()
            if data.get("code") != "0":
                logger.warning(f"[OKX] error {data.get('code')}: {data.get('msg')}")
            return data


async def init_okx_contract(inst_id: str):
    path = f"/api/v5/public/instruments?instType=SWAP&instId={inst_id}"
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{OKX_BASE}{path}") as resp:
            data = await resp.json()
            for item in data.get("data", []):
                if item["instId"] == inst_id:
                    okx_contracts_info[inst_id] = {
                        "ctVal":  float(item["ctVal"]),
                        "minSz":  float(item["minSz"]),
                        "lotSz":  float(item["lotSz"]),
                    }

async def okx_set_leverage(inst_id: str, leverage: int):
    for pos_side in ("long", "short"):
        await _okx_request("POST", "/api/v5/account/set-leverage", {
            "instId": inst_id, "lever": str(leverage),
            "mgnMode": "cross", "posSide": pos_side,
        })

async def okx_order(inst_id: str, side: str, pos_side: str, sz: str):
    return await _okx_request("POST", "/api/v5/trade/order", {
        "instId": inst_id, "tdMode": "cross",
        "side": side, "posSide": pos_side,
        "ordType": "market", "sz": sz,
    })

def fix_okx_sz(inst_id: str, raw_tokens: float) -> str:
    info = okx_contracts_info.get(inst_id)
    if not info: return "0"
    lot_sz = info["lotSz"]
    sz = math.floor(raw_tokens / info["ctVal"] / lot_sz) * lot_sz
    if sz < info["minSz"]: return "0"
    return str(int(sz)) if lot_sz >= 1 else str(round(sz, _step_places(lot_sz)))


KUCOIN_BASE = "https://api-futures.kucoin.com"
kucoin_contracts_info: dict = {}

_kucoin_signed_passphrase = base64.b64encode(
    hmac.new(KUCOIN_SECRET_KEY.encode(), KUCOIN_PASSPHRASE.encode(), hashlib.sha256).digest()
).decode() if KUCOIN_SECRET_KEY else ""


def _kucoin_sign(ts: str, method: str, path: str, body: str) -> str:
    return base64.b64encode(
        hmac.new(KUCOIN_SECRET_KEY.encode(), (ts + method + path + body).encode(),
                 hashlib.sha256).digest()
    ).decode()

async def _kucoin_request(method: str, path: str, body: dict = None):
    ts = str(int(time.time() * 1000))
    body_json = json.dumps(body) if body else ""
    headers = {
        "KC-API-KEY":         KUCOIN_API_KEY,
        "KC-API-SIGN":        _kucoin_sign(ts, method, path, body_json),
        "KC-API-TIMESTAMP":   ts,
        "KC-API-PASSPHRASE":  _kucoin_signed_passphrase,
        "KC-API-KEY-VERSION": "2",
        "Content-Type":       "application/json",
    }
    async with aiohttp.ClientSession() as session:
        async with session.request(method, f"{KUCOIN_BASE}{path}",
                                   headers=headers, data=body_json) as resp:
            data = await resp.json()
            if data.get("code") != "200000":
                logger.warning(f"[KuCoin] error {data.get('code')}: {data.get('msg')}")
            return data


async def init_kucoin_contract(symbol: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{KUCOIN_BASE}/api/v1/contracts/{symbol}") as resp:
            data = await resp.json()
            if data.get("code") == "200000":
                info = data["data"]
                kucoin_contracts_info[symbol] = {
                    "multiplier": float(info["multiplier"]),
                    "lotSize":    int(info["lotSize"]),
                }

async def kucoin_set_leverage(symbol: str, leverage: int):
    return await _kucoin_request("POST", "/api/v2/position/changeLeverage", {
        "symbol": symbol, "leverage": str(leverage),
    })

async def kucoin_order(symbol: str, side: str, size: int, leverage: int,
                       reduce_only: bool = False):
    return await _kucoin_request("POST", "/api/v1/orders", {
        "clientOid":  str(uuid.uuid4()),
        "symbol":     symbol,
        "side":       side,
        "type":       "market",
        "leverage":   str(leverage),
        "size":       size,
        "reduceOnly": reduce_only,
    })

def fix_kucoin_size(symbol: str, raw_tokens: float) -> int:
    info = kucoin_contracts_info.get(symbol)
    if not info: return 0
    lots = math.floor(raw_tokens / info["multiplier"])
    return lots if lots >= info["lotSize"] else 0


BITGET_BASE = "https://api.bitget.com"
bitget_contracts_info: dict = {}


def _bitget_sign(ts: str, method: str, path: str, body: str) -> str:
    return base64.b64encode(
        hmac.new(BITGET_SECRET_KEY.encode(), (ts + method + path + body).encode(),
                 hashlib.sha256).digest()
    ).decode()

async def _bitget_request(method: str, path: str, body: dict = None, params: dict = None):
    ts = str(int(time.time() * 1000))
    body_json = json.dumps(body) if body else ""
    sign_path = path
    if params:
        sign_path += "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    headers = {
        "ACCESS-KEY":        BITGET_API_KEY,
        "ACCESS-SIGN":       _bitget_sign(ts, method, sign_path, body_json),
        "ACCESS-TIMESTAMP":  ts,
        "ACCESS-PASSPHRASE": BITGET_PASSPHRASE,
        "Content-Type":      "application/json",
        "locale":            "en-US",
    }
    async with aiohttp.ClientSession() as session:
        async with session.request(method, f"{BITGET_BASE}{path}",
                                   headers=headers,
                                   data=body_json if method == "POST" else None,
                                   params=params) as resp:
            data = await resp.json()
            if data.get("code") != "00000":
                logger.warning(f"[Bitget] error {data.get('code')}: {data.get('msg')}")
            return data


async def init_bitget_contract(symbol: str):
    params = {"productType": "USDT-FUTURES", "symbol": symbol}
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{BITGET_BASE}/api/v2/mix/market/contracts",
                               params=params) as resp:
            data = await resp.json()
            for item in data.get("data", []):
                if item["symbol"] == symbol:
                    bitget_contracts_info[symbol] = {
                        "minTradeNum":    float(item["minTradeNum"]),
                        "sizeMultiplier": float(item["sizeMultiplier"]),
                        "volumePlace":    int(item["volumePlace"]),
                    }

async def bitget_set_leverage(symbol: str, leverage: int):
    return await _bitget_request("POST", "/api/v2/mix/account/set-leverage", {
        "symbol": symbol, "productType": "USDT-FUTURES",
        "marginCoin": "USDT", "leverage": str(leverage),
    })

async def bitget_order(symbol: str, side: str, trade_side: str, size: str):
    return await _bitget_request("POST", "/api/v2/mix/order/place-order", {
        "symbol":      symbol,
        "productType": "USDT-FUTURES",
        "marginMode":  "crossed",
        "marginCoin":  "USDT",
        "size":        size,
        "orderType":   "market",
        "side":        side,
        "tradeSide":   trade_side,
    })

def fix_bitget_size(symbol: str, raw_tokens: float) -> str:
    info = bitget_contracts_info.get(symbol)
    if not info: return "0"
    step = info["sizeMultiplier"]
    size = math.floor(raw_tokens / step) * step
    if size < info["minTradeNum"]: return "0"
    return f"{size:.{info['volumePlace']}f}"


BINANCE_BASE = "https://fapi.binance.com"
binance_contracts_info: dict = {}


def _binance_qs(params: dict) -> str:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    sig = hmac.new(BINANCE_SECRET_KEY.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return f"{qs}&signature={sig}"

async def _binance_request(method: str, endpoint: str, params: dict):
    params["timestamp"] = int(time.time() * 1000)
    qs_signed = _binance_qs(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    url = f"{BINANCE_BASE}{endpoint}"
    async with aiohttp.ClientSession() as session:
        if method == "GET":
            async with session.get(f"{url}?{qs_signed}", headers=headers) as resp:
                data = await resp.json()
                if resp.status != 200:
                    logger.warning(f"[Binance] error {resp.status}: {data}")
                return data
        else:
            async with session.post(url, headers={
                **headers, "Content-Type": "application/x-www-form-urlencoded",
            }, data=qs_signed) as resp:
                data = await resp.json()
                if resp.status != 200:
                    logger.warning(f"[Binance] error {resp.status}: {data}")
                return data


async def init_binance_contract(symbol: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{BINANCE_BASE}/fapi/v1/exchangeInfo",
                               params={"symbol": symbol}) as resp:
            data = await resp.json()
            for item in data.get("symbols", []):
                if item["symbol"] == symbol:
                    for f in item.get("filters", []):
                        if f["filterType"] == "LOT_SIZE":
                            binance_contracts_info[symbol] = {
                                "minQty":   float(f["minQty"]),
                                "stepSize": float(f["stepSize"]),
                            }
                    break

async def binance_set_leverage(symbol: str, leverage: int):
    return await _binance_request("POST", "/fapi/v1/leverage", {
        "symbol": symbol, "leverage": leverage,
    })

async def binance_order(symbol: str, side: str, position_side: str, quantity: float):
    return await _binance_request("POST", "/fapi/v1/order", {
        "symbol":       symbol,
        "side":         side,
        "positionSide": position_side,
        "type":         "MARKET",
        "quantity":     quantity,
    })

def fix_binance_qty(symbol: str, raw_tokens: float) -> float:
    info = binance_contracts_info.get(symbol)
    if not info: return 0
    step = info["stepSize"]
    qty = math.floor(raw_tokens / step) * step
    if qty < info["minQty"]: return 0
    return round(qty, _step_places(step))


async def place_orders(pair: str, exch1: str, exch2: str,
                       qty_sell, qty_buy, cur_spread: float):
    gate_sym   = to_gate_symbol(pair)
    mexc_sym   = to_mexc_symbol(pair)
    bingx_sym  = to_bingx_symbol(pair)
    okx_sym    = to_okx_symbol(pair)
    kucoin_sym = to_kucoin_symbol(pair)

    tasks: list = []

    async def open_short(exch: str, tokens_from_book):
        t = await resolve_tokens(pair, exch, tokens_from_book, _lev(exch))
        if t is None:
            return
        match exch.upper():
            case "GATE":
                await gate_set_leverage(gate_sym, GATE_LEVERAGE)
                size = fix_gate_size(gate_sym, -abs(t))
                if size != 0: tasks.append(gate_order(gate_sym, size))

            case "BYBIT":
                await bybit_set_leverage(pair, BYBIT_LEVERAGE)
                qty = fix_bybit_qty(pair, t)
                if qty != 0: tasks.append(bybit_order(pair, qty, "Sell"))

            case "MEXC":
                await mexc_set_leverage(mexc_sym, MEXC_LEVERAGE, 2)
                vol = fix_mexc_vol(mexc_sym, t)
                if vol != 0: tasks.append(mexc_order(mexc_sym, 3, vol))

            case "BINGX":
                await bingx_set_leverage(bingx_sym, BINGX_LEVERAGE)
                qty = fix_bingx_qty(bingx_sym, t)
                if qty != 0: tasks.append(bingx_order(bingx_sym, "SELL", "SHORT", qty))

            case "OKX":
                await okx_set_leverage(okx_sym, OKX_LEVERAGE)
                sz = fix_okx_sz(okx_sym, t)
                if sz != "0": tasks.append(okx_order(okx_sym, "sell", "short", sz))

            case "KUCOIN":
                await kucoin_set_leverage(kucoin_sym, KUCOIN_LEVERAGE)
                size = fix_kucoin_size(kucoin_sym, t)
                if size != 0: tasks.append(kucoin_order(kucoin_sym, "sell", size, KUCOIN_LEVERAGE))

            case "BITGET":
                await bitget_set_leverage(pair, BITGET_LEVERAGE)
                sz = fix_bitget_size(pair, t)
                if sz != "0": tasks.append(bitget_order(pair, "sell", "open", sz))

            case "BINANCE":
                await binance_set_leverage(pair, BINANCE_LEVERAGE)
                qty = fix_binance_qty(pair, t)
                if qty != 0: tasks.append(binance_order(pair, "SELL", "SHORT", qty))

    async def open_long(exch: str, tokens_from_book):
        t = await resolve_tokens(pair, exch, tokens_from_book, _lev(exch))
        if t is None:
            return
        match exch.upper():
            case "GATE":
                await gate_set_leverage(gate_sym, GATE_LEVERAGE)
                size = fix_gate_size(gate_sym, abs(t))
                if size != 0: tasks.append(gate_order(gate_sym, size))

            case "BYBIT":
                await bybit_set_leverage(pair, BYBIT_LEVERAGE)
                qty = fix_bybit_qty(pair, t)
                if qty != 0: tasks.append(bybit_order(pair, qty, "Buy"))

            case "MEXC":
                await mexc_set_leverage(mexc_sym, MEXC_LEVERAGE, 1)
                vol = fix_mexc_vol(mexc_sym, t)
                if vol != 0: tasks.append(mexc_order(mexc_sym, 1, vol))

            case "BINGX":
                await bingx_set_leverage(bingx_sym, BINGX_LEVERAGE)
                qty = fix_bingx_qty(bingx_sym, t)
                if qty != 0: tasks.append(bingx_order(bingx_sym, "BUY", "LONG", qty))

            case "OKX":
                await okx_set_leverage(okx_sym, OKX_LEVERAGE)
                sz = fix_okx_sz(okx_sym, t)
                if sz != "0": tasks.append(okx_order(okx_sym, "buy", "long", sz))

            case "KUCOIN":
                await kucoin_set_leverage(kucoin_sym, KUCOIN_LEVERAGE)
                size = fix_kucoin_size(kucoin_sym, t)
                if size != 0: tasks.append(kucoin_order(kucoin_sym, "buy", size, KUCOIN_LEVERAGE))

            case "BITGET":
                await bitget_set_leverage(pair, BITGET_LEVERAGE)
                sz = fix_bitget_size(pair, t)
                if sz != "0": tasks.append(bitget_order(pair, "buy", "open", sz))

            case "BINANCE":
                await binance_set_leverage(pair, BINANCE_LEVERAGE)
                qty = fix_binance_qty(pair, t)
                if qty != 0: tasks.append(binance_order(pair, "BUY", "LONG", qty))

    await open_short(exch1, qty_sell)
    await open_long(exch2, qty_buy)

    if not tasks:
        logger.warning(f"no orders for {pair} {exch1}/{exch2}, size below minimum")
        return

    results = await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"[{pair}] {exch1} SHORT / {exch2} LONG: {results}")
    return results


async def enter_trade(pair: str, exch1: str, exch2: str,
                      qty_sell, qty_buy, cur_spread: float):
    logger.info(f"entering trade: {pair} | {exch1} SHORT / {exch2} LONG | spread {cur_spread:.4f}%")

    inits = []
    for exch in (exch1, exch2):
        match exch.upper():
            case "GATE":    inits.append(init_gate_contracts())
            case "BYBIT":   inits.append(init_bybit_contract(pair))
            case "MEXC":    inits.append(init_mexc_contract(to_mexc_symbol(pair)))
            case "BINGX":   inits.append(init_bingx_contract(to_bingx_symbol(pair)))
            case "OKX":     inits.append(init_okx_contract(to_okx_symbol(pair)))
            case "KUCOIN":  inits.append(init_kucoin_contract(to_kucoin_symbol(pair)))
            case "BITGET":  inits.append(init_bitget_contract(pair))
            case "BINANCE": inits.append(init_binance_contract(pair))

    await asyncio.gather(*inits)
    await place_orders(pair, exch1, exch2, qty_sell, qty_buy, cur_spread)
