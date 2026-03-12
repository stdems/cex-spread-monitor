import logging
import asyncio
import time
import ujson as json
import aiohttp
from storage import price_store as table
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)


def _normalize(symbol: str) -> str:
    return symbol.replace("USDTM", "USDT").replace("USDCM", "USDC").replace("_", "").replace("-", "")


class KuCoinFuturesExchange(BaseExchange):
    name = "KuCoin"
    heartbeat_interval = 10
    reconnect_interval = 5
    batch_size = 50

    async def get_symbols(self, session):
        url = "https://api-futures.kucoin.com/api/v1/contracts/active"
        async with session.get(url) as response:
            data = await response.json()
            return [item["symbol"].split("_")[0] for item in data.get("data", [])]

    async def get_ws_url(self, session):
        url = "https://api-futures.kucoin.com/api/v1/bullet-public"
        async with session.post(url) as response:
            data = await response.json()
            if data.get("data"):
                endpoint = data["data"]["instanceServers"][0]["endpoint"]
                token    = data["data"]["token"]
                return f"{endpoint}?token={token}&connectId={time.time()}"
        raise ConnectionError("[KuCoin] failed to get ws token")

    def build_subscribe(self, batch):
        return [
            json.dumps({
                "id":             str(int(time.time() * 1000)),
                "type":           "subscribe",
                "topic":          f"/contractMarket/tickerV2:{s}",
                "privateChannel": False,
                "response":       True,
            })
            for s in batch
        ]

    def heartbeat_msg(self):
        return json.dumps({"id": str(int(time.time() * 1000)), "type": "ping"})

    async def on_message(self, msg, ws):
        data = json.loads(msg).get("data")
        if not data:
            return

        symbol = _normalize(data.get("symbol", ""))
        ask = float(data.get("bestAskPrice") or 0)
        bid = float(data.get("bestBidPrice") or 0)
        avg = (ask + bid) / 2
        await table.update_table("avg_prices", symbol, "kucoin", avg)
        await table.update_table("top_bids",   symbol, "kucoin", bid)
        await table.update_table("top_asks",   symbol, "kucoin", ask)


async def start_kucoin_funding():
    url = "https://api-futures.kucoin.com/api/v1/contracts/active"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        for contract in data.get("data", []):
                            symbol = _normalize(contract.get("symbol", ""))
                            await table.update_table("funding_rates", symbol, "kucoin", contract.get("fundingFeeRate"))
                            await table.update_table("funding_time",  symbol, "kucoin", contract.get("nextFundingRateTime"))
            except Exception as e:
                logger.error(f"[KuCoin funding] error: {e}")
            await asyncio.sleep(60)


async def start_kucoin_futures():
    await KuCoinFuturesExchange().start()
