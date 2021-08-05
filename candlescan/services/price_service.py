from __future__ import unicode_literals
import frappe
from frappe.utils import cstr
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder
from alpaca_trade_api import Stream
from alpaca_trade_api.common import URL


def start():
	stream = Stream(base_url=URL('https://paper-api.alpaca.markets'), data_feed='iex', raw_data=True)
	stream.subscribe_bars(handle_subs,"*")
	stream.run()
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
	print("price",price)
	if price:
		price = price[0]
		price['doctype'] = "Bars"
		frappe.get_doc(price).insert(ignore_permissions=True, ignore_if_duplicate=True,	ignore_mandatory=True,  set_child_names=False)
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
		
