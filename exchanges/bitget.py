import logging
import websocket
import threading
import ujson as json
import time
import requests
from storage import price_store as table

logger = logging.getLogger(__name__)


class SocketConn(websocket.WebSocketApp):
    def __init__(self, url, params_list):
        super().__init__(
            url=url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.params_list = params_list
        self.heartbeat_stop_event = threading.Event()

    def on_open(self, ws):
        for params in self.params_list:
            sub_data = {
                "op": "subscribe",
                "args": [{
                    "instType": "USDT-FUTURES",
                    "channel": "ticker",
                    "instId": params["symbol"],
                }],
            }
            ws.send(json.dumps(sub_data))
        threading.Thread(target=self._send_heartbeat, args=(ws,), daemon=True).start()

    def _send_heartbeat(self, ws):
        while ws.keep_running and not self.heartbeat_stop_event.wait(timeout=10):
            try:
                ws.send(json.dumps({"op": "ping", "args": [int(time.time() * 1000)]}))
            except Exception as e:
                logger.error(f"heartbeat failed: {e}")
                break

    def on_message(self, ws, msg):
        try:
            parsed = json.loads(msg)
        except Exception:
            return

        data = parsed.get("data", [])
        if not data:
            return

        item = data[0]
        symbol = item.get("instId", "")
        try:
            ask = float(item.get("askPr", 0))
            bid = float(item.get("bidPr", 0))
        except ValueError:
            return

        avg = (ask + bid) / 2
        rate = item.get("fundingRate")
        next_time = item.get("nextFundingTime")

        logger.debug(f"bitget {[symbol, avg, ask, bid, rate, next_time]}")

        table.update_table_sync("avg_prices", symbol, "bitget", avg)
        table.update_table_sync("top_bids",   symbol, "bitget", bid)
        table.update_table_sync("top_asks",   symbol, "bitget", ask)
        if rate is not None:
            table.update_table_sync("funding_rates", symbol, "bitget", rate)
        if next_time is not None:
            table.update_table_sync("funding_time", symbol, "bitget", next_time)

    def on_error(self, ws, error):
        logger.error(f"ws error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.info(f"connection closed: {close_status_code} {close_msg}")
        self.heartbeat_stop_event.set()


def get_symbols():
    url = "https://api.bitget.com/api/mix/v1/market/tickers?productType=umcbl"
    response = requests.get(url).json().get("data", [])
    return [item["symbol"].split("_")[0] for item in response]


def start_bitget_sockets():
    symbols = get_symbols()
    threads = []
    for i in range(0, len(symbols), 50):
        batch = [{"symbol": s} for s in symbols[i:i + 50]]
        ws_app = SocketConn("wss://ws.bitget.com/v2/ws/public", batch)
        t = threading.Thread(target=ws_app.run_forever, daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.01)

    for t in threads:
        t.join()
