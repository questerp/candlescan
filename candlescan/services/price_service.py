from __future__ import unicode_literals
import frappe, logging, time
from frappe.utils import cstr
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder
from alpaca_trade_api import Stream
from alpaca_trade_api.common import URL

log = logging.getLogger(__name__)

def start():
	logging.basicConfig(level=logging.INFO)
	stream = Stream(base_url=URL('https://paper-api.alpaca.markets'), data_feed='iex', raw_data=True)
	stream.subscribe_quotes(handle_subs,"AAPL")
	#stream.run()
	run_connection(stream)
		
def run_connection(conn):
	try:
		conn.run()
	except Exception as e:
		print(f'Exception from websocket connection: {e}')
	finally:
		print("Trying to re-establish connection")
		time.sleep(3)
		run_connection(conn)

	
async def handle_subs(price):
	print(price)
	#☺if price:
	#	#price = price[0]
	#	price['t'] = price['t'].seconds * int(1e9) + price['t'].nanoseconds
	#	price['doctype'] = "Bars"
	#	frappe.get_doc(price).insert(ignore_permissions=True, ignore_if_duplicate=True,	ignore_mandatory=True)

	#[
	#{
	#"T": "b",
	#"S": "AMC",
	#"o": 33.44,
	#"c": 33.35,
	#"h": 33.52,
	#""l": 33.35,
	#""v": 1407,
	#""t": "2021-08-05T19:32:00Z",
	#"n": 18,
	#""vw": 33.419701
	#"}
	#]
		