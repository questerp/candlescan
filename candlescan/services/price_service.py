from __future__ import unicode_literals
import frappe, logging, time
from frappe.utils import cstr,add_days, get_datetime
from datetime import timezone,timedelta,datetime as dt
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder,keep_alive,queue_data
from candlescan.utils.candlescan import to_candle,get_active_symbols,clear_active_symbols,get_connection
from alpaca_trade_api import Stream
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import REST
import pandas as pd
import threading
import numba
import multitasking
import signal
import threading
from numpy import random
import numpy as np
import apsw
import pymysql
from pymysql.converters import conversions, escape_string
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


multitasking.set_max_threads(30)
#multitasking.set_engine("process")
signal.signal(signal.SIGINT, multitasking.killall)	 
bar_symbols = []
sio = socketio.Client(logger=False,json=json_encoder, engineio_logger=False,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
log = logging.getLogger(__name__)
api = None


def connect():
	try:
		if not sio or (sio and not sio.connected):
			sio.connect('http://localhost:9002',headers={"microservice":"price_service"})
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		sio.sleep(5)
		connect()

def disconnect():
	print("I'm disconnected!")
	connect()
	
def start():
	try:
		update_chart_subs(get_redis_server())
		_start()
	except Exception as e:
		print(e)
		time.sleep(2)
		start()




def _start():
	print("staring...")
	connect()
	api = REST(raw_data=True)
	logging.basicConfig(level=logging.INFO)
	redis = get_redis_server()
	symbols = get_active_symbols()
	minutedelta = timedelta(minutes=1)
	conf = frappe.conf.copy()

	while(1):
		nw  = dt.now()
		if nw.hour < 4 or nw.hour > 20:
			lapseh = 0
			lapsem = 0
			if nw.hour < 4:
				lapseh = 4 - nw.hour -1 # 2.45 -> 1
				lapsem = 60 - nw.minute # -> 15
			if nw.hour > 20:
				lapseh = 3
			#TODO remove comment
			#time.sleep((lapseh*60*60)+(lapsem*60))
			
		if dt.now().second != 0:
			time.sleep(1)
			continue
		frappe.db.sql("select 'KEEP_ALIVE'")
		print("------------")
		utc =  dt.utcnow()
		
		utcminute =   utc - minutedelta
		utcminute = utcminute.replace(second=0).replace(microsecond=0) 
		
		print("utcminute",utcminute)
		i = 0
		for _symbols in chunks(symbols,2000):
			i +=1
			threading.Thread(target=get_snapshots,args=(conf,i, api,utcminute,_symbols,)).start()	
		frappe.db.commit()
		print("----> DONE", dt.now())
		time.sleep(1)
		
		
 
def get_snapshots(conf,i,api,utcminute,symbols):
	bcall = dt.now()
	print("START",i,bcall)
	mrows = -1
	drows = -1
	try:
		snap = api.get_snapshots(symbols)
	except Exception as e: 
		print(e)
		return

	tcall = dt.now()
	with get_connection() as cursor:
		bars = [ ]
		dailyBars = [ ]
		try:
			for s in snap:
				data = snap[s]
				if not data:
					continue
				minuteBar = data.get("minuteBar") 
				dailyBar = data.get("dailyBar") 
				
				if minuteBar:
					_date = dt.strptime(minuteBar['t'], DATE_FORMAT)
					minuteBar['t'] = _date.timestamp() #get_datetime(minuteBar['t'].replace("Z",""))#.timestamp()
					minuteBar['s'] = s

					#TODO remove to ignore old candles
					if utcminute != _date:
						continue

					bars.append(minuteBar)
					if dailyBar:
						# nearHigh =  dailyBar["h"] and (minuteBar['h'] >= .95*dailyBar["h"])
						# nearLow =  dailyBar["l"] and (minuteBar['l'] <= 1.05*dailyBar["l"])

						# if nearLow or nearHigh:
						_ddate = dt.strptime(dailyBar['t'], DATE_FORMAT)
						dailyBar['t'] = _ddate.timestamp() 
						dailyBar['s'] = s
						dailyBars.append(dailyBar)
				
			if bars:
				mrows = insert_minute_bars(cursor,bars,True)
			if dailyBars:
				drows = insert_minute_bars(cursor,dailyBars,False,"d")

			endcall = dt.now()
			print("min",mrows,"day",drows,endcall-tcall,tcall-bcall)
		except Exception as e:
				print("error",e)
		

	
				
def backfill(days=0,symbols=None,daily=False ):
	api = REST(raw_data=True)
	
	#all_symbols = frappe.db.sql("""select symbol from tabSymbol where active=1 """,as_list=True)
	#all_symbols = get_active_symbols()# [a[0] for a in all_symbols] 
	if not daily and days == 0 and dt.now().hour < 8:
		days = days + 1
		print("out of hours")

	print("backfill",dt.now())
	chuck = 200
	limit = 1000
	TZ = 'America/New_York'
	#redis = get_redis_server()
	#empty_candle = get_empty_candle()
	
	print("symbols",symbols)
	if not symbols:
		symbols  = get_active_symbols()

	def _insert(i,start,chunk_symbols):
		try:
			with get_connection() as cursor :
				print("start",i,start)
				tcall = dt.now()
				bars = api.get_barset(chunk_symbols,"minute",limit=1000,start=start)	
				tstart = dt.now()
				if bars :
					minute_bars  =[]
					for b in bars:
						_bars = bars[b]
						for a in _bars:
							a['s'] = b
							minute_bars.append(a)

					if minute_bars:
						insert_minute_bars(cursor,minute_bars)
					tend = dt.now()
					print(i,len(minute_bars),"DONE","time:" ,tend-tstart,"api",tstart-tcall)

		except Exception as e:
			print("_insert ERROR",e)	

	def _insert_day(i,start,chunk_symbols):
		try:
			with get_connection() as cursor :
				print("start",i,start)
				tcall = dt.now()
				bars = api.get_barset(chunk_symbols,"day",limit=1000 )	
				tstart = dt.now()
				if bars :
					minute_bars  =[]
					for b in bars:
						_bars = bars[b]
						for a in _bars:
							a['s'] = b
							minute_bars.append(a)

					if minute_bars:
						insert_minute_bars(cursor,minute_bars,col="d")
					tend = dt.now()
					print(i,len(minute_bars),"DONE D","time:" ,tend-tstart,"api",tstart-tcall)

		except Exception as e:
			print("_insert ERROR",e)	

	try:
		threads = 0
		_range = [0] if daily else range(days+1) 
		for d in _range:
			start =  add_days(dt.now(),-1*d) #-1*d
			if not daily and start.weekday() in [5,6]:
				continue
			start = start.replace(second=0).replace(microsecond=0).replace(hour=4).replace(minute=0)	
			beg = pd.Timestamp(start, tz=TZ).isoformat()
			print("BACKFILL FOR",beg)
			for result in chunks(symbols,chuck):
				threads+=1
				if result:
					#_insert(threads,beg,result,start)
					func =  _insert_day if daily else _insert
					threading.Thread(target=func,args=(threads,beg,result,)).start()	
		
			time.sleep(20)

	except Exception as e:
		print("backfill ERROR",e)

	print("--- backfill DONE ---")
 

def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))	
 

@multitasking.task 
def update_chart_subs(redis):
	#redis = get_redis_server()
	global bar_symbols
	while(1):
		symbols = redis.smembers("chart")
		if symbols:
			bar_symbols = [cstr(a).upper() for a in symbols]
		print("update bar_symbols",len(bar_symbols))

		time.sleep(60)
		#if dt.now().minute % 5 == 0:
		clear_active_symbols()


	
def insert_minute_bars(cursor,minuteBars,send_last=False,col="m"):
	global bar_symbols
	if not minuteBars:
		print("not minuteBars")
		return 0
	rows = 0
	try:

		try:
			args = [(a['t'],a['o'],a['c'],a['h'],a['l'],a['v'],a['s']) for a in minuteBars]
			if col=="d":
				rows = cursor.executemany("INSERT INTO tabBarsday (t,o,c,h,l,v,s) values(%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE o=VALUES(o),c=VALUES(c),h=VALUES(h),l=VALUES(l)",args)
			else:
				rows = cursor.executemany("INSERT INTO tabBars (t,o,c,h,l,v,s) values(%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE t=t",args)
			cursor.execute("commit")
		except Exception as ve:
			print("--- ValueError ---",ve)
		
		if send_last  :
			for ticker in minuteBars:
				s = ticker.get("s")
				if s in bar_symbols:
					ev  = "bars_%s"%  s.lower()
					add_to_queue(ev,ev,ticker)
	except Exception as e:
		print("insert_minute_bars ERROR",e)
	finally:
		return rows

@multitasking.task 
def add_to_queue(event,ev,last):
	queue_data(event,ev,last)


def get_minute_bars(symbol,timeframe,start,end=None ):
	if not (symbol and start and timeframe):
		return
	
	try:
		with get_connection() as conn:
			table = "tabBarsday" if timeframe=="d" else "tabBars"
			result = []
			attr = [symbol,start]
			print("start",start)
			sql = """select t,o,c,h,l,v from {} where s=%s and  t>=%s""".format(table)#,symbol,start)
			if end:
				sql = sql + " and t<=%s"%end
				attr.append(end)
			conn.execute(sql,attr)
			data = conn.fetchall()
			if data:
				result = [{"t":d[0],"o":d[1],"c":d[2],"h":d[3],"l":d[4],"v":d[5]} for d in data]
			return result
	except Exception as ex:
		print("ERROR get_minute_bars",ex)
		return []




