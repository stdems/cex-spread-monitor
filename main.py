import logging
import asyncio
import exchanges.mexc as mexc
import exchanges.bybit as bybit
import exchanges.bingx as bingx
import exchanges.kucoin as kucoin
import exchanges.okx as okx
import exchanges.binance as binance
import exchanges.gate as gate
from monitor import spread_monitor
from bot.notifier import start_bot
from core.multipliers import init_multipliers
from core.logger import setup_logging

logger = logging.getLogger(__name__)

async def main_async():
    logger.info("starting up")

    logger.info("loading multipliers...")
    try:
        await asyncio.wait_for(init_multipliers(), timeout=10)
    except asyncio.TimeoutError:
        logger.error("multipliers timed out")
    except Exception as e:
        logger.error(f"multipliers failed: {e}")

    logger.info("starting services")
    await asyncio.gather(
        start_bot(),
        spread_monitor.monitor_spreads(),
        binance.start_binance_funding(),
        binance.start_binance_futures(),
        bybit.start_bybit_socket(),
        mexc.start_funding_socket(),
        mexc.start_futures_socket(),
        bingx.start_bingx_funding_socket(),
        bingx.start_bingx_futures_socket(),
        okx.start_okx_futures_socket(),
        okx.start_okx_funding_socket(),
        kucoin.start_kucoin_futures(),
        kucoin.start_kucoin_funding(),
        gate.start_gate_socket(),
    )


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main_async())
