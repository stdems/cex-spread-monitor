import logging
import time
import yaml
import asyncio
from bot import notifier
from storage.price_store import r
from monitor.spread_utils import get_current_spread
from core.config import AP_THRESHOLD as ap_thresh, AP_COOLDOWN as ap_cooldown
from core.config import FR_THRESHOLD as fr_thresh, FR_COOLDOWN as fr_cooldown

logger = logging.getLogger(__name__)

with open("config/blacklist.yaml", encoding="utf-8") as f:
    blacklist = yaml.safe_load(f)

# эту хуйню потом заменить надо
def format_url(exchange, pair):
    exch = exchange.upper()
    if exch == "BINANCE":
        return f"https://www.binance.com/futures/{pair}"
    elif exch == "KUCOIN":
        return f"https://www.kucoin.com/futures/trade/{pair}M"
    elif exch == "GATE":
        if pair.endswith("USDT"):
            return f"https://www.gate.io/futures/USDT/{pair[:-4]}_USDT"
        return f"https://www.gate.io/futures/USDT/{pair}"
    elif exch == "BYBIT":
        return f"https://www.bybit.com/trade/usdt/{pair}"
    elif exch == "BITGET":
        return f"https://www.bitget.com/futures/usdt/{pair}"
    elif exch == "MEXC":
        if pair.endswith("USDT"):
            return f"https://futures.mexc.com/exchange/{pair[:-4]}_USDT"
        return f"https://futures.mexc.com/exchange/{pair}"
    elif exch == "BINGX":
        if pair.endswith("USDT"):
            return f"https://bingx.com/perpetual/{pair[:-4]}-USDT"
        return f"https://bingx.com/perpetual/{pair}"
    elif exch == "OKX":
        return f"https://www.okx.com/trade-swap/{pair.lower()[:-4]}-usdt-swap"
    return f"{exchange} - URL unknown"


def format_number(num: float) -> str:
    formatted = "{:.8f}".format(num)
    return formatted.rstrip('0').rstrip('.') if '.' in formatted else formatted


async def check_cooldown(pair: str, cooldown: float) -> bool:
    cd_key = f"cooldown:{pair}"
    last = await r.get(cd_key)
    now = time.time()
    if last and now - float(last) < cooldown:
        return False
    await r.set(cd_key, now)
    return True


async def process_pair(key):
    pair = key.split(":", 1)[1]

    fr_data = await r.hgetall(f"funding_rates:{pair}")
    ap_data = await r.hgetall(f"avg_prices:{pair}")

    if not ap_data or len(ap_data) < 2:
        return

    try:
        fr_values = {
            exch.upper(): float(v)
            for exch, v in fr_data.items()
            if v is not None and pair not in blacklist.get(exch.upper(), [])
        }
        ap_values = {
            exch.upper(): float(v)
            for exch, v in ap_data.items()
            if v is not None and pair not in blacklist.get(exch.upper(), [])
        }
    except Exception as e:
        logger.warning(f"failed to parse values for {pair}: {e}")
        return

    if len(ap_values) < 2:
        return

    sorted_ap = sorted(ap_values.items(), key=lambda x: x[1])
    lowest_exch, lowest_val = sorted_ap[0]
    highest_exch, highest_val = sorted_ap[-1]

    percent_spread = 0.0
    if lowest_val != 0:
        percent_spread = ((highest_val - lowest_val) / lowest_val) * 100

    fr_spread = (max(fr_values.values()) - min(fr_values.values())) if len(fr_values) >= 2 else 0.0

    if percent_spread >= ap_thresh:
        spread_type = "futures"
    elif fr_spread >= fr_thresh:
        spread_type = "funding"
    else:
        return

    if spread_type == "funding":
        if not await check_cooldown(pair, fr_cooldown):
            return

        sorted_fr = sorted(fr_values.items(), key=lambda x: x[1])
        fr_low_exch, fr_low_val = sorted_fr[0]
        fr_high_exch, fr_high_val = sorted_fr[-1]

        spread_pct = float(format_number(fr_spread)) * 100
        high_pct = float(format_number(fr_high_val)) * 100
        low_pct = float(format_number(fr_low_val)) * 100

        msg_lines = [
            f"Funding rate ${pair}",
            "",
            f'<a href="{format_url(fr_high_exch, pair)}">{fr_high_exch}</a> ↔️ <a href="{format_url(fr_low_exch, pair)}">{fr_low_exch}</a>',
            f"Spread: {spread_pct:.4f}%",
            "",
            f'<a href="{format_url(fr_high_exch, pair)}">{fr_high_exch}</a>: {high_pct:.4f}%',
            f'<a href="{format_url(fr_low_exch, pair)}">{fr_low_exch}</a>: {low_pct:.4f}%',
            "",
        ]
        detailed_message = "\n".join(msg_lines)
        logger.info(f"[FUNDING] {pair} | {fr_high_exch} ↔ {fr_low_exch} | Spread: {spread_pct:.4f}%")
        try:
            await notifier.funding_notification(detailed_message)
        except Exception as e:
            logger.warning(f"failed to send funding alert: {e}")
        return

    if not await check_cooldown(pair, ap_cooldown):
        return

    try:
        result = await get_current_spread(
            pair, percent_spread, highest_exch, lowest_exch, "spread_monitor", ap_values
        )
        if not isinstance(result, tuple) or len(result) < 5:
            return
        spread_value, price_sell, price_buy, exch_sell, exch_buy = result
    except Exception as e:
        logger.warning(f"spread calc error for {pair}: {e}")
        return

    if spread_value <= 0:
        return

    msg_lines = [
        f"Futures ${pair}",
        "",
        f"[{exch_sell}]({format_url(exch_sell, pair)}) [{exch_buy}]({format_url(exch_buy, pair)})",
        f"Spread: {format_number(spread_value)}%",
        "",
        f"[{exch_sell}]({format_url(exch_sell, pair)}): {format_number(price_sell)}",
        f"[{exch_buy}]({format_url(exch_buy, pair)}): {format_number(price_buy)}",
        "",
    ]
    detailed_message = "\n".join(msg_lines)
    logger.info(f"[FUTURES] {pair} | {exch_sell} → {exch_buy} | Spread: {format_number(spread_value)}%")

    try:
        await notifier.send_message(detailed_message, pair, exch_sell, exch_buy, spread_value)
    except Exception as e:
        logger.warning(f"failed to send notification: {e}")


async def monitor_spreads():
    logger.info("monitoring loop started")
    while True:
        all_keys = await r.keys("avg_prices:*")

        if not all_keys:
            logger.debug("no data yet, waiting...")
            await asyncio.sleep(2)
            continue

        for key in all_keys:
            try:
                await process_pair(key)
            except Exception as e:
                logger.warning(f"process_pair error: {e}")

        await asyncio.sleep(0.5)
