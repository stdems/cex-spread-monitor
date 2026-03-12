from __future__ import annotations

import asyncio
import logging
import sys

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from core.config import settings
from core.schemas import TradeCallbackData
from monitor.spread_utils import get_current_spread

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)


class SpreadNotifier:
    def __init__(self, futures_token: str, funding_token: str, chat_id: str) -> None:
        self._chat_id = chat_id
        self._funding_url = f"https://api.telegram.org/bot{funding_token}/sendMessage"
        self._bot = Bot(
            token=futures_token,
            default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2),
        )
        self._dp = Dispatcher()

    async def send_funding_alert(self, text: str) -> None:
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self._funding_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    if response.status != 200:
                        body = await response.text()
                        logger.warning(
                            "send_funding_alert: status %d — %s", response.status, body
                        )
        except Exception as e:
            logger.warning("send_funding_alert failed: %s", e)

    async def send_trade_signal(
        self,
        text: str,
        pair: str,
        exchange_sell: str,
        exchange_buy: str,
        spread: float,
    ) -> None:
        try:
            keyboard = self._make_keyboard(pair, exchange_sell, exchange_buy, spread)
            msg = await self._bot.send_message(
                chat_id=self._chat_id,
                text=text,
                reply_markup=keyboard,
                parse_mode=None,
            )
            logger.info("signal sent (message_id=%d)", msg.message_id)
        except Exception as e:
            logger.warning("send_trade_signal failed: %s", e)

    async def _handle_callback(self, callback: CallbackQuery) -> None:
        try:
            trade = TradeCallbackData.from_str(callback.data)
        except Exception as e:
            logger.warning("callback validation error: %s", e)
            await callback.answer("bad data")
            return

        if trade.action == "yes":
            logger.info("trade confirmed: %s", trade.pair)
            result = await get_current_spread(
                trade.pair,
                trade.spread,
                trade.exch_sell,
                trade.exch_buy,
                "notifier",
            )
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=f"{result}, ${trade.pair}",
            )
        else:
            logger.info("trade skipped: %s", trade.pair)

        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.answer(f"ok: {trade.pair}", show_alert=False)

    @staticmethod
    def _make_keyboard(
        pair: str,
        exchange_sell: str,
        exchange_buy: str,
        spread: float,
    ) -> InlineKeyboardMarkup:
        yes_data = TradeCallbackData(
            action="yes", pair=pair,
            exch_sell=exchange_sell, exch_buy=exchange_buy,
            spread=spread,
        )
        no_data = TradeCallbackData(
            action="no", pair=pair,
            exch_sell=exchange_sell, exch_buy=exchange_buy,
            spread=spread,
        )
        return InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Enter",   callback_data=yes_data.to_str()),
            InlineKeyboardButton(text="Skip",    callback_data=no_data.to_str()),
        ]])

    async def start(self) -> None:
        self._dp.callback_query.register(self._handle_callback)
        logger.info("starting polling...")
        await self._dp.start_polling(self._bot)
        logger.info("polling stopped")


notifier = SpreadNotifier(
    futures_token=settings.telegram.futures_bot_token,
    funding_token=settings.telegram.funding_bot_token,
    chat_id=settings.telegram.chat_id,
)

async def funding_notification(data: str) -> None:
    await notifier.send_funding_alert(data)


async def send_message(
    message_text: str,
    pair: str,
    exchange1: str,
    exchange2: str,
    spread: float,
) -> None:
    await notifier.send_trade_signal(message_text, pair, exchange1, exchange2, spread)


async def start_bot() -> None:
    await notifier.start()
