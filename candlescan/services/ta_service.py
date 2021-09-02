
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
import talib._ta_lib as tl
from candlescan.utils.candlescan import get_active_symbols 
from candlescan.services.price_service import chunks 
from candlescan.libs import pystore
import numpy as np
import threading
import pymysql
from pymysql.converters import conversions, escape_string

store = pystore.store('bars' )
collection = store.collection('1MIN' )

sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

ta_func = ["SMA_5","RSI_5"]

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"ta_service"})
		while(1):
			if dt.now().second <= 30:
				time.sleep(1)
				continue

			# for symbol in get_active_symbols():

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

			

			
			
			
		#await keep_alive()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()

def ta_snapshot_all():
	conf = frappe.conf.copy()
	for symbols in chunks(get_active_symbols(),500):
		threading.Thread(target=ta_snapshot,args=(symbols,conf,)).start()	


def ta_snapshot(symbols=None,conf=None):
	start = dt.now()
	if symbols is None:
		symbols= get_active_symbols()
	_cursor = None
	conn = None
	if conf:
		conn = pymysql.connect(
				user= conf.db_name,
				password= conf.db_password,
				database=conf.db_name,
				host='127.0.0.1',
				port='',
				charset='utf8mb4',
				use_unicode=True,
				ssl=  None,
				conv=conversions,
				local_infile=conf.local_infile
			)
		_cursor = conn.cursor()
	for symbol in symbols:
		close = collection.item(symbol).snapshot(50,["c"]) # [(a,b,...),()...]
		if close:
			close = np.array([v[0] for v in close if v[0]],dtype=np.double)
			analysis = {}
			#t,o,c,h,l,v 
			for t in ta_func:
				try:
					fun = t
					period = None
					if "_" in  t:
						targets = t.split("_")
						fun = targets[0]
						period = targets[1]
					if period:
						f = getattr(tl,"stream_%s"%fun,period)
					else:
						f = getattr(tl,"stream_%s"%fun)
					
					val = f(close)
					if val and val != 'nan':
						analysis[t] = val

				except Exception as e:
					print("ERROR TA",e,close)
			print(symbol)
			if _cursor and analysis:
				fields = [field.lower() for field in ta_func if analysis[field]] + [""]
				args = ("=%s, ".join(fields))
				args = args[:-2]
				#print(args)
				#print(tuple([analysis[t] for t in ta_func]))
				fargs= args % tuple([analysis[field] for field in ta_func if analysis[field]])
				#print(fargs)

				sql = """  update tabIndicators set 
						%s
						where symbol='%s'
				 """ %  (fargs,symbol)
				#print(sql)
				try:
					sql = str(sql)
					_cursor.execute(sql)
				except Exception as e:
					print("error sql",e,sql)

	end = dt.now()
	if conn:
		_cursor.execute("COMMIT;")
		conn.close()
		_cursor = None
		conn = None		
	print("DONE",end-start)


@sio.event
async def connect():
	print("I'm connected!")
