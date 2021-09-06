
from __future__ import unicode_literals
import frappe
import time
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user, validate_data, build_response, json_encoder, keep_alive
from datetime import timedelta, datetime as dt
from frappe.utils import cstr, add_days, get_datetime
import pandas as pd
import talib as ta
import talib._ta_lib as tl
from candlescan.utils.candlescan import get_active_symbols,get_connection
from candlescan.services.price_service import chunks
import numpy as np
import threading
import pymysql
from pymysql.converters import conversions, escape_string
import math
from talib import stream
import signal
import sys
import pytz

stop_threads = False
estern = pytz.timezone("US/Eastern")


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


ta_func = [
	"close",
	"open",
	"low",
	"high",
	"m_volume",
	"volume",
	"today_open",
	"today_close",
	"high_200",
	"low_200",
	"high_day",
	"low_day",
	"change_v",
	"change_p",
		# 'ht_dcperiod',
		# 'ht_dcphase',
		# 'ht_phasor',
		# 'ht_sine',
		# 'ht_trendmode',
		# 'add',
		# 'div',
		# 'max',
		# 'maxindex',
		# 'min',
		# 'minindex',
		# 'minmax',
		# 'minmaxindex',
		# 'mult',
		# 'sub',
		# 'sum',
		# 'acos',
		# 'asin',
		# 'atan',
		# 'ceil',
		# 'cos',
		# 'cosh',
		# 'exp',
		# 'floor',
		# 'ln',
		# 'log10',
		# 'sin',
		# 'sinh',
		# 'sqrt',
		# 'tan',
		# 'tanh',
		#  'adx',
		# 'adxr',
		'apo',
		# 'aroon',
		# 'aroonosc',
		# 'bop',
		'cci',
		# 'cmo',
		# 'dx',
		# 'macd',
		# 'macdext',
		# 'macdfix',
		# 'mfi',
		# 'minus_di',
		# 'minus_dm',
		'mom',
		# 'plus_di',
		# 'plus_dm',
		'ppo',
		'roc',
		'rocp',
		'rocr',
		'rocr100',
		'rsi',
		# 'stoch',
		# 'stochf',
		# 'stochrsi',
		'trix',
		# 'ultosc',
		# 'willr',
		# 'bbands',
		# 'dema',
		# 'ema',
		'ema7',
		'ema8',
		'ema9',
		'ema10',
		'ema11',
		'ema12',
		'ema15',
		'ema20',
		'ema50',
		'ema200',
		# 'ht_trendline',
		# 'kama',
		# 'ma',
		# 'mama',
		# 'mavp',
		# 'midpoint',
		# 'midprice',
		# 'sar',
		# 'sarext',
		'sma7',
		'sma8',
		'sma9',
		'sma10',
		'sma11',
		'sma12',
		'sma15',
		'sma20',
		'sma50',
		'sma200',
		# 't3',
		# 'tema',
		# 'trima',
		# 'wma',
		# 'cdl2crows',
		# 'cdl3blackcrows',
		# 'cdl3inside',
		# 'cdl3linestrike',
		# 'cdl3outside',
		# 'cdl3starsinsouth',
		# 'cdl3whitesoldiers',
		# 'cdlabandonedbaby',
		# 'cdladvanceblock',
		# 'cdlbelthold',
		# 'cdlbreakaway',
		# 'cdlclosingmarubozu',
		# 'cdlconcealbabyswall',
		# 'cdlcounterattack',
		# 'cdldarkcloudcover',
		# 'cdldoji',
		# 'cdldojistar',
		# 'cdldragonflydoji',
		# 'cdlengulfing',
		# 'cdleveningdojistar',
		# 'cdleveningstar',
		# 'cdlgapsidesidewhite',
		# 'cdlgravestonedoji',
		# 'cdlhammer',
		# 'cdlhangingman',
		# 'cdlharami',
		# 'cdlharamicross',
		# 'cdlhighwave',
		# 'cdlhikkake',
		# 'cdlhikkakemod',
		# 'cdlhomingpigeon',
		# 'cdlidentical3crows',
		# 'cdlinneck',
		# 'cdlinvertedhammer',
		# 'cdlkicking',
		# 'cdlkickingbylength',
		# 'cdlladderbottom',
		# 'cdllongleggeddoji',
		# 'cdllongline',
		# 'cdlmarubozu',
		# 'cdlmatchinglow',
		# 'cdlmathold',
		# 'cdlmorningdojistar',
		# 'cdlmorningstar',
		# 'cdlonneck',
		# 'cdlpiercing',
		# 'cdlrickshawman',
		# 'cdlrisefall3methods',
		# 'cdlseparatinglines',
		# 'cdlshootingstar',
		# 'cdlshortline',
		# 'cdlspinningtop',
		# 'cdlstalledpattern',
		# 'cdlsticksandwich',
		# 'cdltakuri',
		# 'cdltasukigap',
		# 'cdlthrusting',
		# 'cdltristar',
		# 'cdlunique3river',
		# 'cdlupsidegap2crows',
		# 'cdlxsidegap3methods',
		# 'avgprice',
		# 'medprice',
		# 'typprice',
		# 'wclprice',
		# 'beta',
		# 'correl',
		# 'linearreg',
		# 'linearreg_angle',
		# 'linearreg_intercept',
		# 'linearreg_slope',
		# 'stddev',
		# 'tsf',
		# 'var',
		'atr',
		# 'natr',
		# 'trange',
		# 'ad',
		# 'adosc',
		# 'obv'
	]


async def run():
	try:
		global stop_threads

		await sio.connect('http://localhost:9002', headers={"microservice": "ta_service"})
		while(1):
			try:
				if dt.now().second != 30:
					if stop_threads:
						print("BREAKING")
						break
					time.sleep(1)
					continue

				# for symbol in get_active_symbols():
				# if dt.now().minute % 5 == 0:
					# frappe.db.sql("""update tabSymbol set
					# daily_change_per=ROUND(100*((price - today_open)/today_open),2),
					# daily_change_val=ROUND((price - today_open),2)
					# where today_open>0 and price > 0""")
					# frappe.db.commit()
					# time.sleep(1)
					# frappe.db.sql("""update tabSymbol set
					# daily_close_change_per=ROUND(100*((price - prev_day_close)/prev_day_close),2),
					# daily_close_change_val=ROUND((price - prev_day_close),2)
					# where prev_day_close>0 and price > 0""")
					# frappe.db.commit()
					# time.sleep(1)
					# frappe.db.sql("""update tabSymbol set
					# gap_per=ROUND(100*((price - prev_day_close)/prev_day_close),2),
					# gap_val=ROUND((price - prev_day_close),2)
					# where prev_day_close>0 and price > 0""")
					# frappe.db.commit()
					# time.sleep(1)

				ta_snapshot_all(True)
				time.sleep(2)

			except Exception as e:
				print(e)
				stop_threads = True

		# await keep_alive()
	except socketio.exceptions.ConnectionError as err:
		print("error", sio.sid, err)
		await sio.sleep(5)
		await run()


def ta_snapshot_all(apply_priority=False):
	try:
		global stop_threads
		i = 0
		all_symbols = []
		tchunk = 250
		print("---- START -----")
		if apply_priority:
			minute = dt.now().minute
			if minute % 5 == 0:
				all_symbols = get_active_symbols()
				tchunk = 2000
			elif minute % 2 == 0:
				all_symbols = get_active_symbols()[:2000]
				tchunk = 500
			else:
				all_symbols = get_active_symbols()[:1000]
				tchunk = 500

		for symbols in chunks(all_symbols, tchunk):
			if stop_threads:
				print("breaking")
				break
			i += 1
			threading.Thread(target=ta_snapshot, args=(i, symbols,)).start()

	except KeyboardInterrupt as e:
		print("error ta_snapshot_all", e)
		stop_threads = True

today = None
today930 = None

def ta_snapshot(i, symbols=None,):
	start = dt.now()
	
	global today
	global today930

	today = start.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
	today930= start.replace(hour=9,minute=30,second=0,microsecond=0).timestamp()
	global stop_threads
	if symbols is None:
		symbols = get_active_symbols()

	market_hour = start.astimezone(estern)
	minutes = (market_hour.hour * 60) + market_hour.minute
	# TODO stop ta after market close (remove comments)
	# if minutes<240 or minutes > 1200:
	# 	return
	long_ops = dt.now().minute % 5 == 0
	ts  = start.timestamp()
	# TODP tsm should be ts -12000 (200 minutes/bars)
	tsm = ts-12000
	if minutes<440:
		tsm = ts-(((200-(minutes-240)) + 480)*60)

	with get_connection() as conn:
		try:
			for symbol in symbols:
				if stop_threads:
					print("breaking")
					break
				conn.execute("select c,h,l,o,v from tabBars where s=%s and t BETWEEN %s AND %s",(symbol,tsm,ts))
				data = conn.fetchall()

				if data:
					# print(symbol)
					close = np.array([v[0] for v in data if v[0]], dtype=np.double)
					heigh = np.array([v[1] for v in data if v[1]], dtype=np.double)
					low = np.array([v[2] for v in data if v[2]], dtype=np.double)
					open = np.array([v[3] for v in data if v[3]], dtype=np.double)
					volume = np.array([v[4] for v in data if v[4]], dtype=np.double)

					analysis = {}
					analysis = calculate_ta(symbol, open, close, heigh,	low, volume, conn, analysis, minutes,long_ops)
					# t,o,c,h,l,v
					if stop_threads:
						print("breaking")
						break
					# for t in ta_func:
					# 	if stop_threads:
					# 		print("breaking")
					# 		break
					# 	try:
					# 		result = calculate_ta(symbol, t, open, close, heigh,
					# 							low, volume, conn, analysis, minutes,long_ops)
					# 		if result and not math.isnan(result) and result > 0:
					# 			analysis[t] = result

					# 	except Exception as e:
					# 		print("ERROR TA", e)
					if analysis:
						fields = [field for field in analysis.keys()] + [""]
						args = ("=ROUND(%s, 2), ".join(fields))
						args = args[:-2]
						# print(args)
						# print(tuple([analysis[t] for t in ta_func]))
						fargs = args % tuple([val for val in analysis.values()])
						# print(fargs)

						sql = """  update tabIndicators set
								%s
								where symbol='%s'
						""" % (fargs, symbol)
						# print(sql)
						try:
							sql = str(sql)
							conn.execute(sql)
							conn.execute("COMMIT;")
						except Exception as e:
							print("error sql", e, sql)

		except Exception as e:
			print("error ta_snapshot", e)
		finally:
			end = dt.now()
			print(i, "time", end-start )


@sio.event
async def connect():
	print("I'm connected!")


def calculate_ta(symbol, func, o, c, h, l, v, cursor, analysis, minutes,long_ops):
	result = 0
	global today
	global today930


	try:
		
		cursor.execute("select o,c,h,l,v from tabBarsday where s=%s and t=%s limit 1",(symbol,today))
		today_values = cursor.fetchone()
		if today_values and today_values[0]:
			today_open = today_values[0]
			today_close = today_values[1]
			today_high = today_values[2]
			today_low = today_values[3]
			today_volume = today_values[4]
			analysis["today_open"] = today_open
			analysis["change_v"] =  c[-1]-today_open
			analysis["change_p"] = 100*(analysis["change_v"] / today_open)
			analysis["today_close"] = today_close
			analysis["high_day"] = today_high
			analysis["low_day"] = today_low
			analysis["volume"] = today_volume


		analysis["close"] = c[-1]
		analysis["open"] = o[-1]
		analysis["high"] = h[-1]
		analysis["low"] = l[-1]
		analysis["m_volume"] = v[-1]
		analysis["high_200"] = stream.MAX(h,200)
		analysis["low_200"] = stream.MIN(l,200)
		analysis["atr"] = stream.ATR(h,l,c)
		analysis["apo"] = stream.APO(c)
		analysis["mom"] = stream.MOM(c)
		analysis["ppo"] = stream.PPO(c)
		analysis["cci"] =stream.CCI(h,l,c)
		analysis["roc"] = stream.ROC(c)
		analysis["rocp"] = stream.ROCP(c)	
		analysis["rocr"] = stream.ROCR(c)	
		analysis["rocr100"] = stream.ROCR100(c)	
		analysis["rsi"] = stream.RSI(c)	
		analysis["trix"] = stream.TRIX(c)	
		analysis["ema7"] = stream.EMA(c,7)	
		analysis["ema8"] = stream.EMA(c,8)	
		analysis["ema9"] = stream.EMA(c,9)	
		analysis["ema10"] = stream.EMA(c,10)	
		analysis["ema11"] = stream.EMA(c,11)	
		analysis["ema12"] = stream.EMA(c,12)	
		analysis["ema15"] = stream.EMA(c,15)	
		analysis["ema20"] = stream.EMA(c,20)	
		analysis["ema50"] = stream.EMA(c,50)	
		analysis["ema200"] = stream.EMA(c,200)	

	

			

	except Exception as e:
		print("ta_fun error",e,func)
	finally:
		return analysis

