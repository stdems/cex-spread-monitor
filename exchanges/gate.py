import logging
import time
import ujson as json
import aiohttp
from storage import price_store as table
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)


class GateExchange(BaseExchange):
    name = "Gate"
    ws_url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    heartbeat_interval = 20
    reconnect_interval = 5
    batch_size = 100

    async def get_symbols(self, session):
        url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            response.raise_for_status()
            contracts = await response.json(content_type=None)
            return [c["name"] for c in contracts]

    def build_subscribe(self, batch):
        return [json.dumps({
            "time": int(time.time()),
            "channel": "futures.book_ticker",
            "event": "subscribe",
            "payload": batch,
        })]

    def heartbeat_msg(self):
        return None

    async def on_message(self, msg, ws):
        data = json.loads(msg)
        result = data.get("result")
        if not result or "s" not in result:
            return

        symbol = result["s"].replace("_", "").upper()
        bid = result.get("b") or result.get("bid")
        ask = result.get("a") or result.get("ask")
        if not bid or not ask:
            return

        bid = float(bid)
        ask = float(ask)
        avg = (bid + ask) / 2
        await table.update_table("avg_prices", symbol, "gate", avg)
        await table.update_table("top_bids",   symbol, "gate", bid)
        await table.update_table("top_asks",   symbol, "gate", ask)


async def start_gate_socket():
    await GateExchange().start()
