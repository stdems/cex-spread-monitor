from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


@dataclass
class TelegramSettings:
    futures_bot_token: str
    funding_bot_token: str
    chat_id: str


@dataclass
class ExchangeCredentials:
    api_key: str
    secret_key: str
    passphrase: str = ""


@dataclass
class SpreadMonitorSettings:
    avg_prices_threshold: float
    avg_prices_cooldown: float
    funding_rates_threshold: float
    funding_rates_cooldown: float


@dataclass
class TradeSettings:
    spread_open: float
    spread_close: float
    amount: float
    leverage: dict[str, int] = field(default_factory=dict)


@dataclass
class LimitsSettings:
    gate: int
    bybit: int
    mexc: int


@dataclass
class AppSettings:
    telegram: TelegramSettings
    exchanges: dict[str, ExchangeCredentials]
    spread_monitor: SpreadMonitorSettings
    trade: TradeSettings
    limits: LimitsSettings


def _load() -> AppSettings:
    config_path = Path("config/config.yaml")
    with config_path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    telegram = TelegramSettings(
        futures_bot_token=os.environ["FUTURES_BOT_TOKEN"],
        funding_bot_token=os.environ["FUNDING_BOT_TOKEN"],
        chat_id=os.environ["TELEGRAM_CHAT_ID"],
    )

    exchanges: dict[str, ExchangeCredentials] = {
        "gate": ExchangeCredentials(
            api_key=os.getenv("GATE_API_KEY", ""),
            secret_key=os.getenv("GATE_SECRET_KEY", ""),
        ),
        "bybit": ExchangeCredentials(
            api_key=os.getenv("BYBIT_API_KEY", ""),
            secret_key=os.getenv("BYBIT_SECRET_KEY", ""),
        ),
        "mexc": ExchangeCredentials(
            api_key=os.getenv("MEXC_API_KEY", ""),
            secret_key=os.getenv("MEXC_SECRET_KEY", ""),
        ),
        "bingx": ExchangeCredentials(
            api_key=os.getenv("BINGX_API_KEY", ""),
            secret_key=os.getenv("BINGX_SECRET_KEY", ""),
        ),
        "okx": ExchangeCredentials(
            api_key=os.getenv("OKX_API_KEY", ""),
            secret_key=os.getenv("OKX_SECRET_KEY", ""),
            passphrase=os.getenv("OKX_PASSPHRASE", ""),
        ),
        "kucoin": ExchangeCredentials(
            api_key=os.getenv("KUCOIN_API_KEY", ""),
            secret_key=os.getenv("KUCOIN_SECRET_KEY", ""),
            passphrase=os.getenv("KUCOIN_PASSPHRASE", ""),
        ),
        "bitget": ExchangeCredentials(
            api_key=os.getenv("BITGET_API_KEY", ""),
            secret_key=os.getenv("BITGET_SECRET_KEY", ""),
            passphrase=os.getenv("BITGET_PASSPHRASE", ""),
        ),
        "binance": ExchangeCredentials(
            api_key=os.getenv("BINANCE_API_KEY", ""),
            secret_key=os.getenv("BINANCE_SECRET_KEY", ""),
        ),
    }

    sm = cfg["spread_monitor"]
    spread_monitor = SpreadMonitorSettings(
        avg_prices_threshold=sm["avg_prices_threshold"],
        avg_prices_cooldown=sm["avg_prices_cooldown"],
        funding_rates_threshold=sm["funding_rates_threshold"],
        funding_rates_cooldown=sm["funding_rates_cooldown"],
    )

    tr = cfg["trade"]
    trade = TradeSettings(
        spread_open=tr["spread_open"],
        spread_close=tr["spread_close"],
        amount=tr["amount"],
        leverage={
            "gate":    tr["gate_leverage"],
            "bybit":   tr["bybit_leverage"],
            "mexc":    tr["mexc_leverage"],
            "bingx":   tr["bingx_leverage"],
            "okx":     tr["okx_leverage"],
            "kucoin":  tr["kucoin_leverage"],
            "bitget":  tr["bitget_leverage"],
            "binance": tr["binance_leverage"],
        },
    )

    lim = cfg["limits"]
    limits = LimitsSettings(
        gate=lim["GATE"],
        bybit=lim["BYBIT"],
        mexc=lim["MEXC"],
    )

    return AppSettings(
        telegram=telegram,
        exchanges=exchanges,
        spread_monitor=spread_monitor,
        trade=trade,
        limits=limits,
    )


settings: AppSettings = _load()

FUTURES_BOT_TOKEN: str = settings.telegram.futures_bot_token
FUNDING_BOT_TOKEN: str = settings.telegram.funding_bot_token
TELEGRAM_CHAT_ID:  str = settings.telegram.chat_id

GATE_API_KEY:    str = settings.exchanges["gate"].api_key
GATE_SECRET_KEY: str = settings.exchanges["gate"].secret_key

BYBIT_API_KEY:    str = settings.exchanges["bybit"].api_key
BYBIT_SECRET_KEY: str = settings.exchanges["bybit"].secret_key

MEXC_API_KEY:    str = settings.exchanges["mexc"].api_key
MEXC_SECRET_KEY: str = settings.exchanges["mexc"].secret_key

BINGX_API_KEY:    str = settings.exchanges["bingx"].api_key
BINGX_SECRET_KEY: str = settings.exchanges["bingx"].secret_key

OKX_API_KEY:    str = settings.exchanges["okx"].api_key
OKX_SECRET_KEY: str = settings.exchanges["okx"].secret_key
OKX_PASSPHRASE: str = settings.exchanges["okx"].passphrase

KUCOIN_API_KEY:    str = settings.exchanges["kucoin"].api_key
KUCOIN_SECRET_KEY: str = settings.exchanges["kucoin"].secret_key
KUCOIN_PASSPHRASE: str = settings.exchanges["kucoin"].passphrase

BITGET_API_KEY:    str = settings.exchanges["bitget"].api_key
BITGET_SECRET_KEY: str = settings.exchanges["bitget"].secret_key
BITGET_PASSPHRASE: str = settings.exchanges["bitget"].passphrase

BINANCE_API_KEY:    str = settings.exchanges["binance"].api_key
BINANCE_SECRET_KEY: str = settings.exchanges["binance"].secret_key

AP_THRESHOLD: float = settings.spread_monitor.avg_prices_threshold
AP_COOLDOWN:  float = settings.spread_monitor.avg_prices_cooldown
FR_THRESHOLD: float = settings.spread_monitor.funding_rates_threshold
FR_COOLDOWN:  float = settings.spread_monitor.funding_rates_cooldown

SPREAD_OPEN:     float = settings.trade.spread_open
SPREAD_CLOSE:    float = settings.trade.spread_close
TRADE_AMOUNT:    float = settings.trade.amount
GATE_LEVERAGE:   int = settings.trade.leverage["gate"]
BYBIT_LEVERAGE:  int = settings.trade.leverage["bybit"]
MEXC_LEVERAGE:   int = settings.trade.leverage["mexc"]
BINGX_LEVERAGE:  int = settings.trade.leverage["bingx"]
OKX_LEVERAGE:    int = settings.trade.leverage["okx"]
KUCOIN_LEVERAGE: int = settings.trade.leverage["kucoin"]
BITGET_LEVERAGE: int = settings.trade.leverage["bitget"]
BINANCE_LEVERAGE: int = settings.trade.leverage["binance"]

GATE_LIMIT:  int = settings.limits.gate
BYBIT_LIMIT: int = settings.limits.bybit
MEXC_LIMIT:  int = settings.limits.mexc
