
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
import math


store = pystore.store('bars' )
collection = store.collection('1MIN' )

sio = socketio.AsyncClient(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
def start():
	asyncio.get_event_loop().run_until_complete(run())
	asyncio.get_event_loop().run_forever()

__function_groups__ = {
    'Cycle Indicators': [
        'HT_DCPERIOD',
        'HT_DCPHASE',
        'HT_PHASOR',
        'HT_SINE',
        'HT_TRENDMODE',
        ],
    'Math Operators': [
        'ADD',
        'DIV',
        'MAX',
        'MAXINDEX',
        'MIN',
        'MININDEX',
        'MINMAX',
        'MINMAXINDEX',
        'MULT',
        'SUB',
        'SUM',
        ],
    'Math Transform': [
        'ACOS',
        'ASIN',
        'ATAN',
        'CEIL',
        'COS',
        'COSH',
        'EXP',
        'FLOOR',
        'LN',
        'LOG10',
        'SIN',
        'SINH',
        'SQRT',
        'TAN',
        'TANH',
        ],
    'Momentum Indicators': [
        'ADX',
        'ADXR',
        'APO',
        'AROON',
        'AROONOSC',
        'BOP',
        'CCI',
        'CMO',
        'DX',
        'MACD',
        'MACDEXT',
        'MACDFIX',
        'MFI',
        'MINUS_DI',
        'MINUS_DM',
        'MOM',
        'PLUS_DI',
        'PLUS_DM',
        'PPO',
        'ROC',
        'ROCP',
        'ROCR',
        'ROCR100',
        'RSI',
        'STOCH',
        'STOCHF',
        'STOCHRSI',
        'TRIX',
        'ULTOSC',
        'WILLR',
        ],
    'Overlap Studies': [
        'BBANDS',
        'DEMA',
        'EMA',
        'HT_TRENDLINE',
        'KAMA',
        'MA',
        'MAMA',
        'MAVP',
        'MIDPOINT',
        'MIDPRICE',
        'SAR',
        'SAREXT',
        'SMA',
        'T3',
        'TEMA',
        'TRIMA',
        'WMA',
        ],
    'Pattern Recognition': [
        'CDL2CROWS',
        'CDL3BLACKCROWS',
        'CDL3INSIDE',
        'CDL3LINESTRIKE',
        'CDL3OUTSIDE',
        'CDL3STARSINSOUTH',
        'CDL3WHITESOLDIERS',
        'CDLABANDONEDBABY',
        'CDLADVANCEBLOCK',
        'CDLBELTHOLD',
        'CDLBREAKAWAY',
        'CDLCLOSINGMARUBOZU',
        'CDLCONCEALBABYSWALL',
        'CDLCOUNTERATTACK',
        'CDLDARKCLOUDCOVER',
        'CDLDOJI',
        'CDLDOJISTAR',
        'CDLDRAGONFLYDOJI',
        'CDLENGULFING',
        'CDLEVENINGDOJISTAR',
        'CDLEVENINGSTAR',
        'CDLGAPSIDESIDEWHITE',
        'CDLGRAVESTONEDOJI',
        'CDLHAMMER',
        'CDLHANGINGMAN',
        'CDLHARAMI',
        'CDLHARAMICROSS',
        'CDLHIGHWAVE',
        'CDLHIKKAKE',
        'CDLHIKKAKEMOD',
        'CDLHOMINGPIGEON',
        'CDLIDENTICAL3CROWS',
        'CDLINNECK',
        'CDLINVERTEDHAMMER',
        'CDLKICKING',
        'CDLKICKINGBYLENGTH',
        'CDLLADDERBOTTOM',
        'CDLLONGLEGGEDDOJI',
        'CDLLONGLINE',
        'CDLMARUBOZU',
        'CDLMATCHINGLOW',
        'CDLMATHOLD',
        'CDLMORNINGDOJISTAR',
        'CDLMORNINGSTAR',
        'CDLONNECK',
        'CDLPIERCING',
        'CDLRICKSHAWMAN',
        'CDLRISEFALL3METHODS',
        'CDLSEPARATINGLINES',
        'CDLSHOOTINGSTAR',
        'CDLSHORTLINE',
        'CDLSPINNINGTOP',
        'CDLSTALLEDPATTERN',
        'CDLSTICKSANDWICH',
        'CDLTAKURI',
        'CDLTASUKIGAP',
        'CDLTHRUSTING',
        'CDLTRISTAR',
        'CDLUNIQUE3RIVER',
        'CDLUPSIDEGAP2CROWS',
        'CDLXSIDEGAP3METHODS',
        ],
    'Price Transform': [
        'AVGPRICE',
        'MEDPRICE',
        'TYPPRICE',
        'WCLPRICE',
        ],
    'Statistic Functions': [
        'BETA',
        'CORREL',
        'LINEARREG',
        'LINEARREG_ANGLE',
        'LINEARREG_INTERCEPT',
        'LINEARREG_SLOPE',
        'STDDEV',
        'TSF',
        'VAR',
        ],
    'Volatility Indicators': [
        'ATR',
        'NATR',
        'TRANGE',
        ],
    'Volume Indicators': [
        'AD',
        'ADOSC',
        'OBV'
        ],
    }

ta_func = [
		'HT_DCPERIOD',
        'HT_DCPHASE',
        'HT_PHASOR',
        'HT_SINE',
        'HT_TRENDMODE',
        'ADD',
        'DIV',
        'MAX',
        'MAXINDEX',
        'MIN',
        'MININDEX',
        'MINMAX',
        'MINMAXINDEX',
        'MULT',
        'SUB',
        'SUM',
        'ACOS',
        'ASIN',
        'ATAN',
        'CEIL',
        'COS',
        'COSH',
        'EXP',
        'FLOOR',
        'LN',
        'LOG10',
        'SIN',
        'SINH',
        'SQRT',
        'TAN',
        'TANH',
		 'ADX',
        'ADXR',
        'APO',
        'AROON',
        'AROONOSC',
        'BOP',
        'CCI',
        'CMO',
        'DX',
        'MACD',
        'MACDEXT',
        'MACDFIX',
        'MFI',
        'MINUS_DI',
        'MINUS_DM',
        'MOM',
        'PLUS_DI',
        'PLUS_DM',
        'PPO',
        'ROC',
        'ROCP',
        'ROCR',
        'ROCR100',
        'RSI',
        'STOCH',
        'STOCHF',
        'STOCHRSI',
        'TRIX',
        'ULTOSC',
        'WILLR',
	  	'BBANDS',
        'DEMA',
        'EMA',
        'HT_TRENDLINE',
        'KAMA',
        'MA',
        'MAMA',
        'MAVP',
        'MIDPOINT',
        'MIDPRICE',
        'SAR',
        'SAREXT',
        'SMA',
        'T3',
        'TEMA',
        'TRIMA',
        'WMA',
		'CDL2CROWS',
        'CDL3BLACKCROWS',
        'CDL3INSIDE',
        'CDL3LINESTRIKE',
        'CDL3OUTSIDE',
        'CDL3STARSINSOUTH',
        'CDL3WHITESOLDIERS',
        'CDLABANDONEDBABY',
        'CDLADVANCEBLOCK',
        'CDLBELTHOLD',
        'CDLBREAKAWAY',
        'CDLCLOSINGMARUBOZU',
        'CDLCONCEALBABYSWALL',
        'CDLCOUNTERATTACK',
        'CDLDARKCLOUDCOVER',
        'CDLDOJI',
        'CDLDOJISTAR',
        'CDLDRAGONFLYDOJI',
        'CDLENGULFING',
        'CDLEVENINGDOJISTAR',
        'CDLEVENINGSTAR',
        'CDLGAPSIDESIDEWHITE',
        'CDLGRAVESTONEDOJI',
        'CDLHAMMER',
        'CDLHANGINGMAN',
        'CDLHARAMI',
        'CDLHARAMICROSS',
        'CDLHIGHWAVE',
        'CDLHIKKAKE',
        'CDLHIKKAKEMOD',
        'CDLHOMINGPIGEON',
        'CDLIDENTICAL3CROWS',
        'CDLINNECK',
        'CDLINVERTEDHAMMER',
        'CDLKICKING',
        'CDLKICKINGBYLENGTH',
        'CDLLADDERBOTTOM',
        'CDLLONGLEGGEDDOJI',
        'CDLLONGLINE',
        'CDLMARUBOZU',
        'CDLMATCHINGLOW',
        'CDLMATHOLD',
        'CDLMORNINGDOJISTAR',
        'CDLMORNINGSTAR',
        'CDLONNECK',
        'CDLPIERCING',
        'CDLRICKSHAWMAN',
        'CDLRISEFALL3METHODS',
        'CDLSEPARATINGLINES',
        'CDLSHOOTINGSTAR',
        'CDLSHORTLINE',
        'CDLSPINNINGTOP',
        'CDLSTALLEDPATTERN',
        'CDLSTICKSANDWICH',
        'CDLTAKURI',
        'CDLTASUKIGAP',
        'CDLTHRUSTING',
        'CDLTRISTAR',
        'CDLUNIQUE3RIVER',
        'CDLUPSIDEGAP2CROWS',
        'CDLXSIDEGAP3METHODS',
	 	'AVGPRICE',
        'MEDPRICE',
        'TYPPRICE',
        'WCLPRICE',
	 	'BETA',
        'CORREL',
        'LINEARREG',
        'LINEARREG_ANGLE',
        'LINEARREG_INTERCEPT',
        'LINEARREG_SLOPE',
        'STDDEV',
        'TSF',
        'VAR',
		'ATR',
        'NATR',
        'TRANGE',
	    'AD',
        'ADOSC',
        'OBV'
	]

async def run():
	try:
		await sio.connect('http://localhost:9002',headers={"microservice":"ta_service"})
		while(1):
			if dt.now().second != 30:
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

			try:
				ta_snapshot_all(True)
				time.sleep(2)
			except Exception as e:
				pass
			
			
			
		#await keep_alive()
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		await sio.sleep(5)
		await run()

def ta_snapshot_all(apply_priority=False):
	try:
		ts  = []
		conf = frappe.conf.copy()
		i=0
		all_symbols = get_active_symbols()
		if apply_priority:
			if dt.now().minute % 5 != 0:
				all_symbols = all_symbols[:2000]
		for symbols in chunks(all_symbols,500):
			i+=1
			t = threading.Thread(target=ta_snapshot,args=(i,symbols,conf,))
			ts.append(t)
			t.start()	
	except KeyboardInterrupt as e:
		print("error ta_snapshot_all",e)
		for t in ts:
			t.raise_exception()
			t.join()


def ta_snapshot(i,symbols=None,conf=None):
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
			#print(symbol)
			close = np.array([v[0] for v in close if v[0]],dtype=np.double)
			analysis = {}
			#t,o,c,h,l,v 
			for t in ta_func:
				try:
					fun = t
					period = None
					if "." in  t:
						targets = t.split(".")
						fun = targets[0]
						period = targets[1]
					if period:
						f = getattr(tl,"stream_%s"%fun,period)
					else:
						f = getattr(tl,"stream_%s"%fun)
					
					val = f(close)
					if val and not math.isnan(val):
						analysis[t] = val

				except Exception as e:
					print("ERROR TA",e,close)
			if _cursor and analysis:
				fields = [field.lower() for field in analysis.keys() ] + [""]
				args = ("=%s, ".join(fields))
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
