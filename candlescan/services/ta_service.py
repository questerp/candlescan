
from __future__ import unicode_literals
import frappe,time
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
import math
from talib import stream
import signal
import sys


stop_threads = False
def handler(signum, frame):
	global stop_threads
	stop_threads = True
	print('Ctrl+Z pressed, wait 5 sec')
	time.sleep(5)
	sys.exit()


signal.signal(signal.SIGTSTP, handler)
store = pystore.store('bars' )
collection = store.collection('1MIN' )

sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()
 

ta_func = [
	"CLOSE",
	"OPEN",
	"LOW",
	"HIGH",
	"M_VOLUME",
	"VOLUME",
		# 'HT_DCPERIOD',
        # 'HT_DCPHASE',
        # 'HT_PHASOR',
        # 'HT_SINE',
        # 'HT_TRENDMODE',
        # 'ADD',
        # 'DIV',
        # 'MAX',
        # 'MAXINDEX',
        # 'MIN',
        # 'MININDEX',
        # 'MINMAX',
        # 'MINMAXINDEX',
        # 'MULT',
        # 'SUB',
        # 'SUM',
        # 'ACOS',
        # 'ASIN',
        # 'ATAN',
        # 'CEIL',
        # 'COS',
        # 'COSH',
        # 'EXP',
        # 'FLOOR',
        # 'LN',
        # 'LOG10',
        # 'SIN',
        # 'SINH',
        # 'SQRT',
        # 'TAN',
        # 'TANH',
		#  'ADX',
        # 'ADXR',
         'APO',
        # 'AROON',
        # 'AROONOSC',
        # 'BOP',
         'CCI',
        # 'CMO',
        # 'DX',
        # 'MACD',
        # 'MACDEXT',
        # 'MACDFIX',
        # 'MFI',
        # 'MINUS_DI',
        # 'MINUS_DM',
         'MOM',
        # 'PLUS_DI',
        # 'PLUS_DM',
         'PPO',
         'ROC',
         'ROCP',
         'ROCR',
         'ROCR100',
        'RSI',
        # 'STOCH',
        # 'STOCHF',
        # 'STOCHRSI',
         'TRIX',
        # 'ULTOSC',
        # 'WILLR',
	  	# 'BBANDS',
        # 'DEMA',
        # 'EMA',
		'EMA7',
		'EMA8',
		'EMA9',
		'EMA10',
		'EMA11',
		'EMA12',
		'EMA15',
		'EMA20',
		'EMA50',
		'EMA200',
        # 'HT_TRENDLINE',
        # 'KAMA',
        # 'MA',
        # 'MAMA',
        # 'MAVP',
        # 'MIDPOINT',
        # 'MIDPRICE',
        # 'SAR',
        # 'SAREXT',
         'SMA7',
         'SMA8',
         'SMA9',
         'SMA10',
         'SMA11',
         'SMA12',
         'SMA15',
         'SMA20',
         'SMA50',
         'SMA200',
        # 'T3',
        # 'TEMA',
        # 'TRIMA',
        # 'WMA',
		# 'CDL2CROWS',
        # 'CDL3BLACKCROWS',
        # 'CDL3INSIDE',
        # 'CDL3LINESTRIKE',
        # 'CDL3OUTSIDE',
        # 'CDL3STARSINSOUTH',
        # 'CDL3WHITESOLDIERS',
        # 'CDLABANDONEDBABY',
        # 'CDLADVANCEBLOCK',
        # 'CDLBELTHOLD',
        # 'CDLBREAKAWAY',
        # 'CDLCLOSINGMARUBOZU',
        # 'CDLCONCEALBABYSWALL',
        # 'CDLCOUNTERATTACK',
        # 'CDLDARKCLOUDCOVER',
        # 'CDLDOJI',
        # 'CDLDOJISTAR',
        # 'CDLDRAGONFLYDOJI',
        # 'CDLENGULFING',
        # 'CDLEVENINGDOJISTAR',
        # 'CDLEVENINGSTAR',
        # 'CDLGAPSIDESIDEWHITE',
        # 'CDLGRAVESTONEDOJI',
        # 'CDLHAMMER',
        # 'CDLHANGINGMAN',
        # 'CDLHARAMI',
        # 'CDLHARAMICROSS',
        # 'CDLHIGHWAVE',
        # 'CDLHIKKAKE',
        # 'CDLHIKKAKEMOD',
        # 'CDLHOMINGPIGEON',
        # 'CDLIDENTICAL3CROWS',
        # 'CDLINNECK',
        # 'CDLINVERTEDHAMMER',
        # 'CDLKICKING',
        # 'CDLKICKINGBYLENGTH',
        # 'CDLLADDERBOTTOM',
        # 'CDLLONGLEGGEDDOJI',
        # 'CDLLONGLINE',
        # 'CDLMARUBOZU',
        # 'CDLMATCHINGLOW',
        # 'CDLMATHOLD',
        # 'CDLMORNINGDOJISTAR',
        # 'CDLMORNINGSTAR',
        # 'CDLONNECK',
        # 'CDLPIERCING',
        # 'CDLRICKSHAWMAN',
        # 'CDLRISEFALL3METHODS',
        # 'CDLSEPARATINGLINES',
        # 'CDLSHOOTINGSTAR',
        # 'CDLSHORTLINE',
        # 'CDLSPINNINGTOP',
        # 'CDLSTALLEDPATTERN',
        # 'CDLSTICKSANDWICH',
        # 'CDLTAKURI',
        # 'CDLTASUKIGAP',
        # 'CDLTHRUSTING',
        # 'CDLTRISTAR',
        # 'CDLUNIQUE3RIVER',
        # 'CDLUPSIDEGAP2CROWS',
        # 'CDLXSIDEGAP3METHODS',
	 	# 'AVGPRICE',
        # 'MEDPRICE',
        # 'TYPPRICE',
        # 'WCLPRICE',
	 	# 'BETA',
        # 'CORREL',
        # 'LINEARREG',
        # 'LINEARREG_ANGLE',
        # 'LINEARREG_INTERCEPT',
        # 'LINEARREG_SLOPE',
        # 'STDDEV',
        # 'TSF',
        # 'VAR',
		# 'ATR',
        # 'NATR',
        # 'TRANGE',
	    # 'AD',
        # 'ADOSC',
        # 'OBV'
	]

async def run():
	try:
		global stop_threads

		await sio.connect('http://localhost:9002',headers={"microservice":"ta_service"})
		while(1):
			try:
				if dt.now().second != 30:
					if stop_threads:
						break
					time.sleep(1)
					continue

				# for symbol in get_active_symbols():
				if dt.now().minute % 5 == 0:
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

				
					ta_snapshot_all(True)
					time.sleep(2)

			except Exception as e:
				print(e)
				stop_threads = True
			
			
			
		#await keep_alive()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()

def ta_snapshot_all(apply_priority=False):
	try:
		global stop_threads
		conf = frappe.conf.copy()
		i=0
		all_symbols = []
		print("---- START -----")
		if apply_priority:
			minute = dt.now().minute
			if minute % 5 == 0:
				all_symbols = get_active_symbols()
			elif minute % 2 == 0:
				all_symbols =  get_active_symbols()[:2000]
			else:
				all_symbols =  get_active_symbols()[:1000]

		for symbols in chunks(all_symbols,500):
			if stop_threads:
				print("breaking")
				break
			i+=1
			threading.Thread(target=ta_snapshot,args=(i,symbols,conf,)).start()
			
	except KeyboardInterrupt as e:
		print("error ta_snapshot_all",e)
		stop_threads = True


def ta_snapshot(i,symbols=None,conf=None):
	start = dt.now()
	global stop_threads
	if symbols is None:
		symbols= get_active_symbols()
	_cursor = None
	conn = None
	try:
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
			if stop_threads:
				print("breaking")
				break
			data = collection.item(symbol).snapshot(200,["c","h","l","o","v"]) # [(a,b,...),()...]
			if data:
				#print(symbol)
				close = np.array([v[0] for v in data if v[0]],dtype=np.double)
				heigh = np.array([v[1] for v in data if v[1]],dtype=np.double)
				low = np.array([v[2] for v in data if v[2]],dtype=np.double)
				open = np.array([v[3] for v in data if v[3]],dtype=np.double)
				volume = np.array([v[4] for v in data if v[4]],dtype=np.double)
				analysis = {}
				#t,o,c,h,l,v 
				for t in ta_func:
					try:
						result = calculate_ta(symbol,t,open,close,heigh,low,volume)
						if result and not math.isnan(result):
							analysis[t] = result

					except Exception as e:
						print("ERROR TA",e)
				if _cursor and analysis:
					fields = [field for field in analysis.keys() ] + [""]
					args = ("=ROUND(%s, 2), ".join(fields))
					args = args[:-2]
					#print(args)
					#print(tuple([analysis[t] for t in ta_func]))
					fargs= args % tuple([ val  for val in  analysis.values() ])
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
	except Exception as e:
		print("error ta_snapshot",e)
	finally:
		if conn:
			_cursor.execute("COMMIT;")
			conn.close()
			_cursor = None
			conn = None		
		end = dt.now()
		print(i,"DONE",end-start)


@sio.event
async def connect():
	print("I'm connected!")



def calculate_ta(symbol,func,o,c,h,l,v):
	result = 0
	long_ops = dt.now().minute % 5 == 0
	try:
		if func == "CLOSE":
			result = c[-1]
		if func == "OPEN":
			result = o[-1]
		if func == "LOW":
			result = l[-1]
		if func == "HIGH":
			result = h[-1]
		if long_ops and func == "VOLUME":
			result = collection.item(symbol).today_volume()
		if func == "M_VOLUME":
			result = v[-1]
		if func == "APO":
			result = stream.APO(c)
		if func == "MOM":
			result = stream.MOM(c)
		if func == "PPO":
			result = stream.PPO(c)
		if func == "CCI":
			result = stream.CCI(h,l,c)
		if func == "ROC":
			result = stream.ROC(c)
		if func == "ROCP":
			result = stream.ROCP(c)	
		if func == "ROCR":
			result = stream.ROCR(c)	
		if func == "ROCR100":
			result = stream.ROCR100(c)	
		if func == "RSI":
			result = stream.RSI(c)		
		if func == "TRIX":
			result = stream.TRIX(c)		 
		if func == "EMA7":
			result = stream.EMA(c,7)		 
		if func == "EMA8":
			result = stream.EMA(c,8)	
		if func == "EMA9":
			result = stream.EMA(c,9)	
		if func == "EMA10":
			result = stream.EMA(c,10)	
		if func == "EMA11":
			result = stream.EMA(c,11)	
		if func == "EMA12":
			result = stream.EMA(c,12)	
		if func == "EMA15":
			result = stream.EMA(c,15)	
		if func == "EMA20":
			result = stream.EMA(c,20)	
		if func == "EMA50":
			result = stream.EMA(c,50)	
		if long_ops and func == "EMA200":
			result = stream.EMA(c,200)	

	except Exception as e:
		print("ta_fun error",e,func)
	finally:
		return result

