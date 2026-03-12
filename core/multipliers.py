from __future__ import annotations

import asyncio
import logging
import sys

import aiohttp

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)

_GATE_URL = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
_MEXC_URL = "https://contract.mexc.com/api/v1/contract/detail"
_FETCH_TIMEOUT = 5  # seconds


class MultiplierRegistry:
    def __init__(self) -> None:
        self.gate: dict[str, float] = {}
        self.mexc: dict[str, int] = {}

    async def initialize(self) -> None:
        try:
            gate, mexc = await asyncio.gather(
                asyncio.wait_for(self._fetch_gate(), timeout=_FETCH_TIMEOUT),
                asyncio.wait_for(self._fetch_mexc(), timeout=_FETCH_TIMEOUT),
            )
            self.gate = gate
            self.mexc = mexc
        except Exception as e:
            logger.warning("failed to load multipliers: %s, defaulting to 1.0", e)
            self.gate = {}
            self.mexc = {}

    @staticmethod
    async def _fetch_gate() -> dict[str, float]:
        async with aiohttp.ClientSession() as session:
            async with session.get(_GATE_URL) as response:
                response.raise_for_status()
                contracts = await response.json()

        return {
            c["name"]: float(c["quanto_multiplier"])
            for c in contracts
        }

    @staticmethod
    async def _fetch_mexc() -> dict[str, int]:
        async with aiohttp.ClientSession() as session:
            async with session.get(_MEXC_URL) as response:
                response.raise_for_status()
                data = await response.json()

        return {
            c["symbol"]: int(c["contractSize"])
            for c in data.get("data", [])
        }


multipliers = MultiplierRegistry()

gate_multipliers: dict[str, float] = multipliers.gate
mexc_multipliers: dict[str, int] = multipliers.mexc


async def init_multipliers() -> None:
    global gate_multipliers, mexc_multipliers
    await multipliers.initialize()
    gate_multipliers = multipliers.gate
    mexc_multipliers = multipliers.mexc
