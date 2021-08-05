
from __future__ import unicode_literals
import frappe
from frappe.utils import cstr
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder
from alpaca_trade_api.rest import REST


sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
api = REST(raw_data=True)

def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"ta_service"})
		while(1):
			handle_subs()
			await sio.sleep(5)
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()

def handle_subs():
	_symbols = get_redis_server().smembers("symbols")
	symbols = [cstr(a) for a in _symbols]
	print(_symbols)
	if symbols:
		snap = api.get_snapshots(symbols)
		for st in snap:
			print(st)
			s = snap[st]
			if s:
				latest_trade = s.get("latestTrade")
				latest_quote = s.get("latestQuote")
				minute_bar =  s.get("minuteBar")
				daily_bar =  s.get("dailyBar")
				prev_daily_bar =  s.get("prevDailyBar")

				if latest_trade:
					price = latest_trade.get("p") if latest_trade else 0
					volume = daily_bar.get("v") if daily_bar else 0
					bid = latest_quote.get("bp") if latest_quote else 0
					ask = latest_quote.get("ap") if latest_quote else 0
					print(s,price,volume,bid,ask)
					
					frappe.db.sql(""" update tabSymbol set price=%s, volume=%s, bid=%s, ask=%s where symbol='%s'""" % (price,volume,bid,ask,st))
		frappe.db.commit()
			
		

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
