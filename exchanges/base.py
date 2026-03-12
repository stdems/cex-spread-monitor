import asyncio
import logging
from abc import ABC, abstractmethod

import aiohttp
import websockets

logger = logging.getLogger(__name__)


class BaseExchange(ABC):
    name: str = ""
    ws_url: str = ""
    heartbeat_interval: int = 20
    reconnect_interval: int = 5
    batch_size: int = 50

    @abstractmethod
    async def get_symbols(self, session: aiohttp.ClientSession) -> list[str]:
        ...

    @abstractmethod
    def build_subscribe(self, batch: list[str]) -> list[str]:
        ...

    @abstractmethod
    async def on_message(self, msg: str | bytes, ws) -> None:
        ...

    def heartbeat_msg(self) -> str | None:
        return None

    async def get_ws_url(self, session: aiohttp.ClientSession) -> str:
        return self.ws_url

    def _make_batches(self, symbols: list[str]) -> list[list[str]]:
        return [symbols[i:i + self.batch_size] for i in range(0, len(symbols), self.batch_size)]

    async def _heartbeat_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            msg = self.heartbeat_msg()
            if msg is None:
                return
            try:
                await ws.send(msg)
            except Exception as e:
                logger.warning(f"[{self.name}] heartbeat failed: {e}")
                return

    async def _connect_and_listen(self, batch: list[str], session: aiohttp.ClientSession) -> None:
        url = await self.get_ws_url(session)
        async with websockets.connect(url, open_timeout=10, ping_interval=None, ping_timeout=None) as ws:
            for sub_msg in self.build_subscribe(batch):
                await ws.send(sub_msg)

            heartbeat = asyncio.create_task(self._heartbeat_loop(ws))
            try:
                async for msg in ws:
                    try:
                        await self.on_message(msg, ws)
                    except Exception as e:
                        logger.error(f"[{self.name}] on_message error: {e}")
            finally:
                heartbeat.cancel()

    async def _run_batch(self, batch: list[str], session: aiohttp.ClientSession) -> None:
        while True:
            try:
                await self._connect_and_listen(batch, session)
                logger.warning(f"[{self.name}] connection closed, reconnecting...")
            except Exception as e:
                logger.error(f"[{self.name}] connection error: {e}, retrying in {self.reconnect_interval}s")
            await asyncio.sleep(self.reconnect_interval)

    async def _load_symbols(self, session: aiohttp.ClientSession) -> list[str]:
        while True:
            try:
                symbols = await self.get_symbols(session)
                if symbols:
                    logger.info(f"[{self.name}] loaded {len(symbols)} symbols")
                    return symbols
                logger.warning(f"[{self.name}] got empty symbol list, retrying in 10s...")
            except Exception as e:
                logger.error(f"[{self.name}] failed to load symbols: {type(e).__name__}: {e}, retrying in 10s...")
            await asyncio.sleep(10)

    async def start(self) -> None:
        async with aiohttp.ClientSession() as session:
            symbols = await self._load_symbols(session)
            batches = self._make_batches(symbols)
            logger.info(f"[{self.name}] starting {len(batches)} connection(s)")
            await asyncio.gather(*[self._run_batch(batch, session) for batch in batches])
