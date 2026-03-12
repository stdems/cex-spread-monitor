import logging
import aiohttp
import asyncio
from trading.trade import enter_trade
from core import multipliers
from core.config import (
    SPREAD_OPEN as spread_open,
    TRADE_AMOUNT as amount,
    GATE_LIMIT as gate_limit,
    BYBIT_LIMIT as bybit_limit,
    MEXC_LIMIT as mexc_limit,
    GATE_LEVERAGE as gate_leverage,
    BYBIT_LEVERAGE as bybit_leverage,
    MEXC_LEVERAGE as mexc_leverage,
)

logger = logging.getLogger(__name__)


async def gate_info():
    url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
    connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.DefaultResolver())
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()


async def gate_order_book(contract, limit):
    url = f"https://api.gateio.ws/api/v4/futures/usdt/order_book?contract={contract}"
    params = {"contract": contract, "limit": limit}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()


def gate_calculate(order_book, target_usd, side, multiplier):
    orders = order_book["bids"] if side == "sell" else order_book["asks"]
    remaining = target_usd
    total_qty = 0.0
    total_value = 0.0
    for order in orders:
        price = float(order["p"])
        qty = float(order["s"]) * multiplier
        value = price * qty
        if value >= remaining:
            total_qty += remaining / price
            total_value += remaining
            remaining = 0
            break
        else:
            total_qty += qty
            total_value += value
            remaining -= value
    if remaining > 0:
        return None
    return total_value / total_qty, total_qty


async def bybit_order_book(symbol, limit):
    url = "https://api.bybit.com/v5/market/orderbook"
    params = {"category": "linear", "symbol": symbol, "limit": limit}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()
    if data.get("retCode", 1) != 0:
        raise Exception(f"bybit api error: {data.get('retMsg')}")
    return data["result"]


def bybit_calculate(order_book, target_usd, side):
    orders = order_book["b"] if side == "sell" else order_book["a"]
    remaining = target_usd
    total_qty = 0.0
    total_value = 0.0
    for order in orders:
        price = float(order[0])
        qty = float(order[1])
        value = price * qty
        if value >= remaining:
            total_qty += remaining / price
            total_value += remaining
            remaining = 0
            break
        else:
            total_qty += qty
            total_value += value
            remaining -= value
    if remaining > 0:
        return None
    return total_value / total_qty, total_qty


async def mexc_order_book(symbol, limit):
    url = f"https://contract.mexc.com/api/v1/contract/depth/{symbol}"
    params = {"limit": limit}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
            response.raise_for_status()
            data = await response.json()
    if not data.get("success", False) or data.get("code") != 0:
        raise Exception(f"mexc api error: {data.get('message', 'unknown')}")
    snapshot = data.get("data")
    if not snapshot:
        raise Exception("mexc returned no data")
    return snapshot


def mexc_calculate(order_book, target_usd, side, multiplier):
    orders = order_book["bids"] if side == "sell" else order_book["asks"]
    remaining = target_usd
    total_qty = 0.0
    total_value = 0.0
    for order in orders:
        price = float(order[0])
        qty = float(order[1]) * multiplier
        value = price * qty
        if value >= remaining:
            total_qty += remaining / price
            total_value += remaining
            remaining = 0
            break
        else:
            total_qty += qty
            total_value += value
            remaining -= value
    if remaining > 0:
        return None
    return total_value / total_qty, total_qty


async def mexc_info():
    url = "https://contract.mexc.com/api/v1/contract/detail"
    connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.DefaultResolver())
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()


async def _get_prices_for_exchanges(pair: str, exch_sell: str, exch_buy: str, ws_prices: dict = None):
    sell, buy, qty_sell, qty_buy = None, None, None, None

    logger.debug(f"order books: {pair}, sell={exch_sell}, buy={exch_buy}")

    if exch_sell.upper() == "GATE":
        pair_gate = pair.replace("USDT", "_USDT")
        multiplier = multipliers.gate_multipliers.get(pair_gate, 1)
        ob = await gate_order_book(pair_gate, gate_limit)
        result = gate_calculate(ob, amount * gate_leverage, "sell", multiplier)
        if result is not None:
            sell, qty_sell = result

    elif exch_sell.upper() == "BYBIT":
        ob = await bybit_order_book(pair, bybit_limit)
        result = bybit_calculate(ob, amount * bybit_leverage, "sell")
        if result is not None:
            sell, qty_sell = result

    elif exch_sell.upper() == "MEXC":
        pair_mexc = pair.replace("USDT", "_USDT")
        multiplier = multipliers.mexc_multipliers.get(pair_mexc, 1)
        ob = await mexc_order_book(pair_mexc, mexc_limit)
        result = mexc_calculate(ob, amount * mexc_leverage, "sell", multiplier)
        if result is not None:
            sell, qty_sell = result

    elif ws_prices and exch_sell.upper() in ws_prices:
        sell = ws_prices[exch_sell.upper()]

    if exch_buy.upper() == "GATE":
        pair_gate = pair.replace("USDT", "_USDT")
        multiplier = multipliers.gate_multipliers.get(pair_gate, 1)
        ob = await gate_order_book(pair_gate, gate_limit)
        result = gate_calculate(ob, amount * gate_leverage, "buy", multiplier)
        if result is not None:
            buy, qty_buy = result

    elif exch_buy.upper() == "BYBIT":
        ob = await bybit_order_book(pair, bybit_limit)
        result = bybit_calculate(ob, amount * bybit_leverage, "buy")
        if result is not None:
            buy, qty_buy = result

    elif exch_buy.upper() == "MEXC":
        pair_mexc = pair.replace("USDT", "_USDT")
        multiplier = multipliers.mexc_multipliers.get(pair_mexc, 1)
        ob = await mexc_order_book(pair_mexc, mexc_limit)
        result = mexc_calculate(ob, amount * mexc_leverage, "buy", multiplier)
        if result is not None:
            buy, qty_buy = result

    elif ws_prices and exch_buy.upper() in ws_prices:
        buy = ws_prices[exch_buy.upper()]

    if sell is None or buy is None:
        return None, None, None, None

    return sell, buy, qty_sell, qty_buy


async def get_current_spread(pair: str, old_spread: float, exch1: str, exch2: str, caller: str, ws_prices: dict = None):
    sell1, buy1, qty_sell, qty_buy = await _get_prices_for_exchanges(pair, exch1, exch2, ws_prices)

    if sell1 is None or buy1 is None:
        if caller == "notifier":
            return "not enough liquidity"
        return 0, 0, 0, exch1, exch2

    spread1 = (sell1 - buy1) / buy1 * 100

    if spread1 < 0:
        sell2, buy2, qty1, qty2 = await _get_prices_for_exchanges(pair, exch2, exch1, ws_prices)
        if sell2 is not None and buy2 is not None:
            spread2 = (sell2 - buy2) / buy2 * 100
            if spread2 > 0:
                sell1, buy1 = sell2, buy2
                qty_sell, qty_buy = qty1, qty2
                spread1 = spread2
                exch1, exch2 = exch2, exch1

    if caller == "notifier":
        if spread1 > spread_open:
            await enter_trade(pair, exch1, exch2, qty_sell, qty_buy, spread1)
            return f"trade opened: {spread1:.4f}% (was {old_spread:.4f}%), {exch1}/{exch2}, {sell1}/{buy1}"
        else:
            return f"spread dropped: {spread1:.4f}% (was {old_spread:.4f}%), {exch1}/{exch2}"

    if spread1 > spread_open:
        return spread1, sell1, buy1, exch1, exch2
    return 0, 0, 0, exch1, exch2


async def main():
    pair = "BTCUSDT"
    await multipliers.init_multipliers()
    result = await get_current_spread(pair, 1.0, "GATE", "MEXC", "spread_monitor")
    logger.info(f"result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
