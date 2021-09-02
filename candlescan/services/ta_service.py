
from __future__ import unicode_literals
import frappe,time
from frappe.utils import cstr
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder,keep_alive
from datetime import timedelta,datetime as dt
from frappe.utils import cstr,add_days, get_datetime
import pandas as pd
import talib as ta
from candlescan.utils.candlescan import get_active_symbols 
from candlescan.libs import pystore
store = pystore.store('bars' )
collection = store.collection('1MIN' )

sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"ta_service"})
		while(1):
			if dt.now().second <= 30:
				time.sleep(1)
				continue
			frappe.db.sql("""update tabSymbol set 
			daily_change_per=ROUND(100*((price - today_open)/today_open),2), 
			daily_change_val=ROUND((price - today_open),2) 
			where today_open>0 and price > 0""")
			frappe.db.commit()
			time.sleep(1)
			frappe.db.sql("""update tabSymbol set 
			daily_close_change_per=ROUND(100*((price - prev_day_close)/prev_day_close),2), 
			daily_close_change_val=ROUND((price - prev_day_close),2) 
			where prev_day_close>0 and price > 0""")
			frappe.db.commit()
			time.sleep(1)
			frappe.db.sql("""update tabSymbol set 
			gap_per=ROUND(100*((price - prev_day_close)/prev_day_close),2), 
			gap_val=ROUND((price - prev_day_close),2) 
			where prev_day_close>0 and price > 0""")
			frappe.db.commit()
			time.sleep(1)

			for symbol in get_active_symbols():
				data = collection.item(symbol,start,end ).data()
			
			
			
			
		#await keep_alive()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()



		
		


@sio.event
async def connect():
	print("I'm connected!")
