
from __future__ import unicode_literals
import frappe,time
from frappe.utils import cstr
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder,keep_alive
from candlescan.services.price_service import synchronized_open_file,synchronized_close_file
from datetime import timedelta,datetime as dt
from frappe.utils import cstr,add_days, get_datetime
import pandas as pd
import talib as ta


sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"ta_service"})
		while(1):
			if dt.now().second != 30:
				time.sleep(1)
				continue
			frappe.db.sql("""update tabSymbol set daily_change_per=ROUND(100*((price - today_open)/today_open),2), daily_change_val=ROUND((price - today_open),2) where today_open>0 and price > 0""")
			frappe.db.commit()
			time.sleep(1)
			frappe.db.sql("""update tabSymbol set daily_close_change_per=ROUND(100*((price - prev_day_close)/prev_day_close),2), daily_close_change_val=ROUND((price - prev_day_close),2) where prev_day_close>0 and price > 0""")
			frappe.db.commit()
			time.sleep(1)
			frappe.db.sql("""update tabSymbol set gap_per=ROUND(100*((price - prev_day_close)/prev_day_close),2), gap_val=ROUND((price - prev_day_close),2) where prev_day_close>0 and price > 0""")
			frappe.db.commit()
			time.sleep(1)
			
			
			
		#await keep_alive()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()

	
def ta(volume_min,volume_max):
	symbols = frappe.db.sql("select symbol from tabSymbol where active=1 and 1m_volume >=%s and 1m_volume<=%s" %( volume_min,volume_max),as_list=1)
	symbols = [a[0] for a in symbols]
	file = synchronized_open_file("r")
	try:
		table = file.root.bars_group.bars
		start = dt.now()
		for symbol in symbols:
			id = 0
			candles = []
			for candle in table.where("ticker==b'%s'" % symbol):
				id +=1
				if id >50:
					break
				candles.append(candle[:])
			print("candles",len(candles))
			
			
			ema = ta.stream(candles)
			
			#data = [ x[:] for x in table.where("""(ticker == b'%s') & (time>=%s) & (time<=%s) & (valide)""" % (symbol,start,end) ) ]
	except Exception as ex:
		print("ERROR ta",ex)
		return ex
	finally:
		synchronized_close_file(file)
		
		


@sio.event
async def connect():
	print("I'm connected!")
