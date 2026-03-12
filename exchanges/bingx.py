import logging
import asyncio
import gzip
import io
import hmac
import time
import uuid
import ujson as json
import aiohttp
from hashlib import sha256
from storage import price_store as table
from core.config import BINGX_API_KEY, BINGX_SECRET_KEY
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)


def _get_sign(secret: str, payload: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), digestmod=sha256).hexdigest()


def _build_params(params: dict) -> str:
    qs = "&".join(f"{k}={params[k]}" for k in sorted(params))
    return qs + ("&" if qs else "") + f"timestamp={int(time.time() * 1000)}"


def _decompress(raw: bytes) -> str:
    return gzip.GzipFile(fileobj=io.BytesIO(raw), mode="rb").read().decode("utf-8")


class BingxFuturesExchange(BaseExchange):
    name = "BingX-Futures"
    ws_url = "wss://open-api-swap.bingx.com/swap-market"
    reconnect_interval = 5
    batch_size = 50

    async def get_symbols(self, session):
        params_str = _build_params({})
        url = (
            f"https://open-api.bingx.com/openApi/swap/v2/quote/price"
            f"?{params_str}&signature={_get_sign(BINGX_SECRET_KEY, params_str)}"
        )
        headers = {"X-BX-APIKEY": BINGX_API_KEY}
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            return [item["symbol"] for item in data.get("data", [])]

    def build_subscribe(self, batch):
        return [
            json.dumps({
                "id": str(uuid.uuid4()),
                "reqType": "sub",
                "dataType": f"{s}@depth5@100ms",
            })
            for s in batch
        ]

    def heartbeat_msg(self):
        return None

    async def on_message(self, msg, ws):
        text = _decompress(msg)

        if text == "Ping":
            await ws.send("Pong")
            return

        msg_json = json.loads(text)
        data = msg_json.get("data", [])
        if not isinstance(data, dict):
            return

        symbol = msg_json.get("dataType", "").split("@")[0].replace("_", "").replace("-", "")
        asks = data.get("asks", [])
        bids = data.get("bids", [])
        if not asks or not bids:
            return

        ask = float(asks[0][0])
        bid = float(bids[0][0])
        avg = (ask + bid) / 2
        await table.update_table("avg_prices", symbol, "bingx", avg)
        await table.update_table("top_bids",   symbol, "bingx", bid)
        await table.update_table("top_asks",   symbol, "bingx", ask)


async def start_bingx_funding_socket():
    headers = {"X-BX-APIKEY": BINGX_API_KEY}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                params_str = _build_params({})
                url = (
                    f"https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex"
                    f"?{params_str}&signature={_get_sign(BINGX_SECRET_KEY, params_str)}"
                )
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        for contract in data.get("data", []):
                            symbol = contract.get("symbol", "").replace("_", "").replace("-", "")
                            await table.update_table("funding_rates", symbol, "bingx", contract.get("lastFundingRate"))
                            await table.update_table("funding_time",  symbol, "bingx", contract.get("nextFundingTime"))
                    else:
                        logger.error(f"[BingX funding] bad status: {response.status}")
            except Exception as e:
                logger.error(f"[BingX funding] error: {e}")
            await asyncio.sleep(1)


async def start_bingx_futures_socket():
    await BingxFuturesExchange().start()
