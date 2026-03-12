# cex-spread-monitor

Monitors two types of spreads across crypto exchanges and sends alerts to Telegram bots

## What it does

**Price spreads** - tracks futures prices across exchanges in real time. When the price of the same pair differs enough between two exchanges, you get a Telegram alert with the pair, both exchanges, the spread %, and buy/sell prices.

**Funding rate spreads** - compares funding rates for the same pair across exchanges. When the difference between rates is significant, sends an alert to a separate Telegram bot.

## Supported exchanges

Binance, Bybit, Gate.io, MEXC, BingX, OKX, KuCoin, Bitget

## Entering trades from Telegram

Price spread alerts come with two inline buttons — **Enter** and **Skip**. If you hit Enter, the bot checks the current spread again and opens positions automatically: short on the higher-priced exchange, long on the lower-priced one.

For this to work you need API keys added to the `.env` file for the exchanges you want to trade on. Exchanges without keys are still monitored and shown in alerts, just not traded.

## Setup

**1. Clone the repo**
```bash
git clone https://github.com/stdems/cex-spread-monitor.git
cd cex-spread-monitor
```

**2. Configure `.env`**
```bash
cp .env.example .env
```
Fill in your Telegram bot tokens, chat ID, and any exchange API keys you want to trade on.

**3. Start**
```bash
docker-compose up -d
```

To check logs:
```bash
docker-compose logs -f
```

To stop:
```bash
docker-compose down
```

## Config

Spread thresholds, trade amount, leverage per exchange, and cooldowns are set in `config/config.yaml`.

Pairs you want to exclude from alerts can be added to `config/blacklist.yaml`.
