import logging
import time
import ujson as json
from storage import price_store as table
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)

_WS_URL      = "wss://ws.okx.com:8443/ws/v5/public"
_SYMBOLS_URL = "https://www.okx.com/api/v5/market/tickers?instType=SWAP"


async def _get_symbols(session):
    async with session.get(_SYMBOLS_URL) as response:
        data = await response.json()
        return [item["instId"] for item in data.get("data", [])]


def _heartbeat_payload():
    return json.dumps({"op": "ping", "args": [int(time.time() * 1000)]})


class OkxFuturesExchange(BaseExchange):
    name = "OKX-Futures"
    ws_url = _WS_URL
    heartbeat_interval = 20
    reconnect_interval = 5
    batch_size = 50

    async def get_symbols(self, session):
        return await _get_symbols(session)

    def build_subscribe(self, batch):
        return [json.dumps({
            "op": "subscribe",
            "args": [{"channel": "tickers", "instId": s} for s in batch],
        })]

    def heartbeat_msg(self):
        return _heartbeat_payload()

    async def on_message(self, msg, ws):
        data = json.loads(msg).get("data", [])
        if not data:
            return

        symbol = data[0].get("instId", "").replace("-SWAP", "").replace("-", "")
        ask = float(data[0].get("askPx") or 0)
        bid = float(data[0].get("bidPx") or 0)
        if not ask or not bid:
            return

        avg = (ask + bid) / 2
        await table.update_table("avg_prices", symbol, "okx", avg)
        await table.update_table("top_bids",   symbol, "okx", bid)
        await table.update_table("top_asks",   symbol, "okx", ask)


class OkxFundingExchange(BaseExchange):
    name = "OKX-Funding"
    ws_url = _WS_URL
    heartbeat_interval = 20
    reconnect_interval = 5
    batch_size = 50

    async def get_symbols(self, session):
        return await _get_symbols(session)

    def build_subscribe(self, batch):
        return [json.dumps({
            "op": "subscribe",
            "args": [{"channel": "funding-rate", "instId": s} for s in batch],
        })]

    def heartbeat_msg(self):
        return _heartbeat_payload()

    async def on_message(self, msg, ws):
        msg_json = json.loads(msg)
        data = msg_json.get("data", [])
        if not data:
            return

        channel_info = msg_json.get("arg", {})
        symbol = channel_info.get("instId", "").replace("-SWAP", "").replace("-", "")
        await table.update_table("funding_rates", symbol, "okx", data[0].get("fundingRate"))
        await table.update_table("funding_time",  symbol, "okx", data[0].get("fundingTime"))


async def start_okx_futures_socket():
    await OkxFuturesExchange().start()


async def start_okx_funding_socket():
    await OkxFundingExchange().start()
