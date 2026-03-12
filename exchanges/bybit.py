import logging
import ujson as json
from storage import price_store as table
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)


class BybitExchange(BaseExchange):
    name = "Bybit"
    ws_url = "wss://stream.bybit.com/v5/public/linear"
    heartbeat_interval = 20
    reconnect_interval = 5
    batch_size = 200

    async def get_symbols(self, session):
        url = "https://api.bybit.com/v5/market/tickers?category=linear"
        async with session.get(url) as response:
            data = await response.json()
            return [item["symbol"] for item in data["result"]["list"]]

    def build_subscribe(self, batch):
        return [json.dumps({
            "op": "subscribe",
            "args": [f"tickers.{s}" for s in batch],
        })]

    def heartbeat_msg(self):
        return json.dumps({"op": "ping"})

    async def on_message(self, msg, ws):
        parsed = json.loads(msg)
        data = parsed.get("data", {})
        if not data:
            return

        symbol = data.get("symbol")
        bid = float(data.get("bid1Price") or 0)
        ask = float(data.get("ask1Price") or 0)
        if not bid or not ask:
            return

        avg = (bid + ask) / 2
        await table.update_table("avg_prices", symbol, "bybit", avg)
        await table.update_table("top_bids",   symbol, "bybit", bid)
        await table.update_table("top_asks",   symbol, "bybit", ask)

        try:
            rate = float(data.get("fundingRate") or 0)
            next_time = data.get("nextFundingTime")
            if rate and next_time:
                await table.update_table("funding_rates", symbol, "bybit", rate)
                await table.update_table("funding_time",  symbol, "bybit", next_time)
        except (TypeError, ValueError):
            pass


async def start_bybit_socket():
    await BybitExchange().start()
