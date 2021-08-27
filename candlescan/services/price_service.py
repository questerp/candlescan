from __future__ import unicode_literals
import frappe, logging, time
from frappe.utils import cstr,add_days, get_datetime
from datetime import timezone,timedelta,datetime as dt
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder,keep_alive,queue_data
from candlescan.utils.candlescan import to_candle,get_active_symbols,clear_active_symbols
from alpaca_trade_api import Stream
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import REST
import pandas as pd
import threading
import numba
from candlescan.libs import pystore
import multitasking
import signal
import threading
from numpy import random
import numpy as np

multitasking.set_max_threads(30)
#multitasking.set_engine("process")
signal.signal(signal.SIGINT, multitasking.killall)	 
bar_symbols = []
sio = socketio.Client(logger=False,json=json_encoder, engineio_logger=False,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
lock = threading.Lock()
log = logging.getLogger(__name__)
api = None
store = pystore.store('bars')
collection = store.collection('1MIN' )
collection_day = store.collection("1DAY" )

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
	#s = frappe.db.sql(""" select symbol from tabSymbol where active=1""",as_list=True)
	symbols = get_active_symbols()
	minutedelta = timedelta(minutes=1)
	conf = frappe.conf.copy()
	print(conf)

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
			#time.sleep((lapseh*60*60)+(lapsem*60))
			
		if dt.now().second != 0:
			time.sleep(1)
			continue
		frappe.db.sql("select 'KEEP_ALIVE'")
		nw =  dt.now()
		print("------------")
		#print(nw)
		utc =  dt.utcnow()
		
		utcminute =   utc - minutedelta
		utcminute = utcminute.replace(second=0).replace(microsecond=0)
		
		print("utcminute",utcminute)
		i = 0
		for _symbols in chunks(symbols,1000):
			i +=1
			threading.Thread(target=get_snapshots,args=(conf,i, api,utcminute,_symbols,)).start()	
			#get_snapshots(i, api,utcminute,_symbols)
			# 200 27sec
			# 2000 22sec process: 
			# 1000 23 sec
			# 500  31 sec
			# 3000 
		frappe.db.commit()
		print("----> DONE", dt.now())
		
		#minuteBars = []	
		time.sleep(1)
		
		

import pymysql
from pymysql.converters import conversions, escape_string

#@multitasking.task 
def get_snapshots(conf,i,api,utcminute,symbols):
	print("START",i,dt.now())
	snap = api.get_snapshots(symbols)
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
	for s in snap:
		try:
			data = snap[s]
			if not data:
				continue
			minuteBar = data.get("minuteBar") or {}
			latestTrade = data.get("latestTrade") or {}
			latestQuote = data.get("latestQuote") or {}
			dailyBar = data.get("dailyBar") or {}
			prevDailyBar = data.get("prevDailyBar")  or {}
			
			if minuteBar.get('t'):
				minuteBar['t'] = get_datetime(minuteBar['t'].replace("Z",""))#.timestamp()
			if not minuteBar.get('t') or utcminute != minuteBar['t']:
				continue

			vol = minuteBar.get("v") or 0
			minuteBar['s'] = s
			insert_minute_bars(s,[minuteBar],True)		
			price = latestTrade.get("p") or 0
			if price:
				sql = """ update tabSymbol set 
				price=%s, 
				volume=%s, 
				1m_volume=%s,
				today_high=%s, 
				today_low=%s ,
				today_open=%s ,
				today_close=%s ,
				today_trades=%s ,
				bid=%s , 
				ask=%s ,
				vwap=%s , 
				prev_day_open = %s ,
				prev_day_close = %s , 
				prev_day_high = %s ,
				prev_day_low = %s , 
				prev_day_vwap = %s ,
				prev_day_volume = %s ,
				prev_day_trades = %s 
				where name='%s' """ % (
							price or 0,
							dailyBar.get("v") or 0,
							vol,
							dailyBar.get("h") or 0,
							dailyBar.get("l") or 0,
							dailyBar.get("o") or 0,
							dailyBar.get("c") or 0,
							dailyBar.get("n") or 0,
							latestQuote.get("bp") or 0,
							latestQuote.get("ap") or 0,
							minuteBar.get("vw") or 0,
							prevDailyBar.get("o") or 0,
							prevDailyBar.get("c") or 0,
							prevDailyBar.get("h") or 0,
							prevDailyBar.get("l") or 0,
							prevDailyBar.get("vw") or 0,
							prevDailyBar.get("v") or 0,
							prevDailyBar.get("n") or 0,
							s )
				try:
					sql = str(sql)
					_cursor.execute(sql)
				except Exception as e:
					print(s,"error sql",e,sql)

		except Exception as e:
			print("error",e)
	conn.close()
	_cursor = None
	conn = None
	print("DONE",i,dt.now())
				
def backfill(days=0,symbols=None):
	api = REST(raw_data=True)
	
	#all_symbols = frappe.db.sql("""select symbol from tabSymbol where active=1 """,as_list=True)
	#all_symbols = get_active_symbols()# [a[0] for a in all_symbols] 
	if days == 0 and dt.now().hour < 8:
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
			sleeptime = random.uniform(0, 500)
			time.sleep(sleeptime)
			print("start",i,start)
			tcall = dt.now()
			bars = api.get_barset(chunk_symbols,"minute",limit=1000,start=start)	
			#print(i,"BARS",len(bars))

			#minute_bars = []
			tstart = dt.now()
			if bars :
				for b in bars:
					_bars = bars[b]
					for a in _bars:
						a['s'] = b
						a['t'] = dt.utcfromtimestamp(a['t'])
					#minute_bars.extend(_bars)
					#candles = [to_candle(a,b) for a in candles]
					if _bars:
						insert_minute_bars(b,_bars)
				tend = dt.now()
				print(i,"DONE","time:" ,tend-tstart,"api",tstart-tcall)
				

		except Exception as e:
			print("_insert ERROR",e)	
		

	try:
		for d in range(days+1):
			start =  add_days(dt.now(),-1*d) #-1*d
			if start.weekday() in [5,6]:
				continue
			start = start.replace(second=0).replace(microsecond=0).replace(hour=4).replace(minute=0)	
			beg = pd.Timestamp(start, tz=TZ).isoformat()
			threads = 0
			for result in chunks(symbols,chuck):
				threads+=1
				if result:
					#_insert(threads,beg,result)
					threading.Thread(target=_insert,args=(threads,beg,result,)).start()	
			
			#time.sleep(5 )

	except Exception as e:
		print("backfill ERROR",e)

	print("--- backfill DONE ---")

def backfill_daily(days=1000):
	api = REST(raw_data=True)
	
	#all_symbols = frappe.db.sql("""select symbol from tabSymbol where active=1 """,as_list=True)
	#all_symbols = get_active_symbols()# [a[0] for a in all_symbols] 
	print("backfill_daily",dt.now())
	chuck = 200
	TZ = 'America/New_York'
	collection_day = store.collection("1DAY",overwrite=True)
	i = 0
	#empty_candle = get_empty_candle()
	try:
		for result in chunks(get_active_symbols(),chuck):
			i+=1
			bars = api.get_barset(result,"day",limit=days)
			if bars :
				for b in bars:
					try:
						day_bars = []
						for a in bars[b]:
							a['t'] = dt.fromtimestamp(a['t'])
							day_bars.append(to_candle(a,b))
						
						df = pd.DataFrame(day_bars)
						df.set_index("time",inplace=True,drop=True)

						if not df.empty :
							try:
								collection_day.append(b, df)
							except ValueError as ve:
								#print(ticker,"--- ValueError ---",ve)
								collection_day.write(b, df,overwrite=True)
						print(len(day_bars),"DONE - symbols:",i*chuck)

					except Exception as e:
						print("backfill ERROR",e)

			else:
				print("No data")
					
			
	except Exception as e:
		print("backfill ERROR",e)

	print("backfill DONE")

def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))	

def init_bars_db(target = 0):
	print("init")
	try:
		#store = pystore.store('bars')
		#collection = store.collection('1MIN')
		#items = collection.list_items()
		day = target in [0,2]
		minute = target in [0,1]
		if minute:
			store.delete_collection("1MIN" )
			collection = store.collection("1MIN",overwrite=True)
		if day:
			store.delete_collection("1DAY")
			collection_day = store.collection("1DAY",overwrite=True)

		#symbols = frappe.db.sql("""select symbol from tabSymbol where active=1 """,as_list=True)
		symbols =  get_active_symbols()#[a[0] for a in symbols]
		date = dt.now().replace(year=1990)
		print(date)
		df = pd.DataFrame([{"ticker":"ticker","open":0.1,"close":0.1,"high":0.1,"low":0.1,"volume":0,"trades":0,"time":date,"timestamp":date}])
		#df  = df.astype({"ticker":'str',"open":"float64","close":"float64","high":"float64","low":"float64","volume":"float64","trades":"int32","time":"datetime64[ns]"})
		#df.ticker = df.ticker.apply(str)
		#df.ticker = df.ticker.astype(basestring)
		#df["timestamp"] = df.time.astype(str)

		df.set_index("time",inplace=True,drop=True)
		df.index = df.index.values.astype(np.int64)
		
		print(df.info())
		ct= len(symbols)
		input()
		for idx,s in enumerate(symbols):
			#if s not in items:
			print(idx,ct)
			if minute:
				collection.write(s, df,overwrite=True,npartitions=1 )
			if day:
				collection_day.write(s, df,overwrite=True,npartitions=1 )
		print("DONE")
		#print(collection.list_items())
		#collection = store.collection("1DAY",overwrite=True)

		
	except Exception as e:
		print("init_bars_db",e)

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
		if dt.now().minute % 5 == 0:
			clear_active_symbols()

	
#@multitasking.task 
def insert_minute_bars(ticker,minuteBars,send_last=False):
	global bar_symbols
	if not minuteBars:
		print(ticker,"not minuteBars")
		return

	try:
		_bars = [to_candle(a) for a in minuteBars ]
		items = pd.DataFrame.from_dict(_bars)
		last = None
		
		if not items.empty :
			if send_last :
				last = _bars[-1]# items.iloc[-1].to_dict()
			#items["timestamp"] = items.time#.astype(str)
			items.set_index("time",inplace=True,drop=True)
			items.index = items.index.values.astype(np.int64)
			try:
				#print(ticker)
				collection.append(ticker, items,npartitions=1)
			except ValueError as ve:
				print(ticker,"--- ValueError ---",ve)
				print(items)
				collection.write(ticker, items,overwrite=True)
			
			if last and send_last and  (ticker in bar_symbols):
				
				ev  = "bars_%s"%  ticker.lower()
				add_to_queue(ev,ev,last)
		else:
			print(ticker,"empty")
	except Exception as e:
		print("insert_minute_bars ERROR",e)

@multitasking.task 
def add_to_queue(event,ev,last):
	print("queue",event)
	queue_data(event,ev,last)


def get_minute_bars(symbol,timeframe,start,end=None):
	if not (symbol and start ):
		return
	#♥start = dt.fromtimestamp(start)
	#if not end:
	#	end = time.time_ns() # dt.utcnow().timestamp()#.isoformat()
	#else:
	#	end = dt.utcfromtimestamp(end)
	try:
		result = []
		item = None
		if timeframe == "m":
			item = collection.item(symbol)
		else:
			item = collection_day.item(symbol)

		print("start",start)
		print("end",end)
		if item != None:
			if end:
				data = item.data.loc[(item.data.index>=start) & (item.data.index <=end)].compute()
			else:
				data = item.data.loc[(item.data.index>=start) ].compute()
			#print("data",data)
			data['timestamp'] = data.timestamp.astype(str)
			result = data.to_dict("records")
		return result
	except Exception as ex:
		print("ERROR get_minute_bars",ex)
		return []



