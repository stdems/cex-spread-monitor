import logging
import aiohttp
import asyncio
from storage import price_store as table

logger = logging.getLogger(__name__)


async def start_binance_funding():
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        data = await response.json()
                        for item in data:
                            symbol = item.get("symbol")
                            rate = item.get("fundingRate")
                            next_time = item.get("fundingTime") + 8 * 60 * 60 * 1000 - 1
                            await table.update_table("funding_rates", symbol, "binance", rate)
                            await table.update_table("funding_time", symbol, "binance", next_time)
                    else:
                        logger.error(f"[Binance funding] bad status: {response.status}")
            except Exception as e:
                logger.error(f"[Binance funding] request failed: {e}")
            await asyncio.sleep(1)


async def start_binance_futures():
    url = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        data = await response.json()
                        for item in data:
                            symbol = item.get("symbol")
                            try:
                                ask = float(item.get("askPrice", 0))
                                bid = float(item.get("bidPrice", 0))
                            except Exception:
                                continue
                            avg = (ask + bid) / 2
                            await table.update_table("avg_prices", symbol, "binance", avg)
                            await table.update_table("top_bids", symbol, "binance", bid)
                            await table.update_table("top_asks", symbol, "binance", ask)
                    else:
                        logger.error(f"[Binance futures] bad status: {response.status}")
            except Exception as e:
                logger.error(f"[Binance futures] request failed: {e}")
            await asyncio.sleep(1)
