import logging
import ujson as json
from storage import price_store as table
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)

_WS_URL      = "wss://contract.mexc.com/edge"
_SYMBOLS_URL = "https://contract.mexc.com/api/v1/contract/funding_rate"


async def _get_symbols(session):
    async with session.get(_SYMBOLS_URL) as response:
        data = await response.json()
        return [item["symbol"] for item in data.get("data", [])]


class MexcFundingExchange(BaseExchange):
    name = "MEXC-Funding"
    ws_url = _WS_URL
    heartbeat_interval = 10
    reconnect_interval = 5
    batch_size = 50

    async def get_symbols(self, session):
        return await _get_symbols(session)

    def build_subscribe(self, batch):
        return [
            json.dumps({"method": "sub.funding.rate", "param": {"symbol": s}})
            for s in batch
        ]

    def heartbeat_msg(self):
        return json.dumps({"method": "ping"})

    async def on_message(self, msg, ws):
        data = json.loads(msg).get("data", [])
        if isinstance(data, dict):
            symbol = data.get("symbol", "").replace("_", "")
            await table.update_table("funding_rates", symbol, "mexc", data.get("rate"))
            await table.update_table("funding_time",  symbol, "mexc", data.get("nextSettleTime"))


class MexcFuturesExchange(BaseExchange):
    name = "MEXC-Futures"
    ws_url = _WS_URL
    heartbeat_interval = 10
    reconnect_interval = 5
    batch_size = 50

    async def get_symbols(self, session):
        return await _get_symbols(session)

    def build_subscribe(self, batch):
        return [
            json.dumps({"method": "sub.depth.full", "param": {"symbol": s}})
            for s in batch
        ]

    def heartbeat_msg(self):
        return json.dumps({"method": "ping"})

    async def on_message(self, msg, ws):
        parsed = json.loads(msg)
        data = parsed.get("data", [])
        if not isinstance(data, dict):
            return

        symbol = parsed.get("symbol", "").replace("_", "")
        asks = data.get("asks", [])
        bids = data.get("bids", [])
        if not asks or not bids:
            return

        ask = float(asks[0][0])
        bid = float(bids[0][0])
        avg = (ask + bid) / 2
        await table.update_table("avg_prices", symbol, "mexc", avg)
        await table.update_table("top_bids",   symbol, "mexc", bid)
        await table.update_table("top_asks",   symbol, "mexc", ask)


async def start_funding_socket():
    await MexcFundingExchange().start()


async def start_futures_socket():
    await MexcFuturesExchange().start()
