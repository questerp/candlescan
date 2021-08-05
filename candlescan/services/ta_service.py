
from __future__ import unicode_literals
import frappe
from frappe.utils import cstr
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder
from alpaca_trade_api.rest import REST


sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
stream = None
def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"ta_service"})
		stream = Stream()  # <- replace to SIP if you have PRO subscription
		stream.subscribe_bars(handle_subs,["*"])

		await sio.wait()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()

async def handle_subs(data):
	if data:
		price = data[0]
		price['doctype'] = "Bars"
		print(price)
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
			
		

@sio.event
async def subscribe_symbol(message):
	init()
	source = message.get("source_sid")
	symbol = message.get("data")
	if not symbol:
		return
	print("sub",symbol)
	get_redis_server().sadd("symbols",symbol)
	
def init():
	if not frappe.local.db:
		frappe.connect()	

@sio.event
async def connect():
	init()
	print("I'm connected!")
