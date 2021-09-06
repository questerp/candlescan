
from __future__ import unicode_literals
import frappe
import time
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user, validate_data, build_response, json_encoder, keep_alive
from datetime import timedelta, datetime as dt
from frappe.utils import cstr, add_days, get_datetime
from candlescan.utils.candlescan import get_active_symbols, get_connection
from candlescan.services.price_service import chunks
import signal
import sys
from alpaca_trade_api.stream import Stream


stop_threads = False
old_quotes = []


def handler(signum, frame):
    global stop_threads
    stop_threads = True
    print('Ctrl+Z pressed, wait 5 sec')
    time.sleep(5)
    sys.exit()


signal.signal(signal.SIGTSTP, handler)

sio = socketio.AsyncClient(logger=True, json=json_encoder, engineio_logger=True, reconnection=True,
                           reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)


def start():
    asyncio.get_event_loop().run_until_complete(run())
    asyncio.get_event_loop().run_forever()


async def run():
    try:
        await sio.connect('http://localhost:9002', headers={"microservice": "quote_service"})
        broadcast()
    except socketio.exceptions.ConnectionError as err:
        print("error", sio.sid, err)
        await sio.sleep(5)
        await run()


def broadcast():
    try:
        global old_quotes
        stream = Stream(raw_data=True)
        while(1):
            quotes = get_redis_server().smembers("quotes")
            if quotes:
                try:
                    if old_quotes:
                        stream.unsubscribe_quotes(old_quotes)
                    stream.subscribe_quotes(quote_callback, quotes)
                    old_quotes = quotes
                    get_redis_server().delete_key("quotes")
                except Exception as ex:
                    print("ERROR", ex)
                	# print("data",get_redis_server().keys(),data)
                time.sleep(10)

    except Exception as e:
        print(e)
        time.sleep(1)
        broadcast()


def quote_callback(q):
    print("quote_callback", q)
