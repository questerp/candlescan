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
store = pystore.store('bars' )
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
	#print(conf)

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
		print("------------")
		#print(nw)
		utc =  dt.utcnow()
		
		utcminute =   utc - minutedelta
		utcminute = utcminute.replace(second=0).replace(microsecond=0) 
		
		print("utcminute",utcminute)
		i = 0
		for _symbols in chunks(symbols,2000):
			i +=1
			#get_snapshots(conf,i, api,utcminute,_symbols)
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
		
		
 
def get_snapshots(conf,i,api,utcminute,symbols):
	print("START",i,dt.now())
	bcall = dt.now()
	snap = api.get_snapshots(symbols)
	tcall = dt.now()

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
	bars = [ ]
	try:
		for s in snap:
			data = snap[s]
			if not data:
				continue
			
			minuteBar = data.get("minuteBar") 
			#latestTrade = data.get("latestTrade") or {}
			# dailyBar = data.get("dailyBar") or {}
			# prevDailyBar = data.get("prevDailyBar") or {}
			# latestQuote = data.get("latestQuote") or {}
			
			
			if minuteBar:
				_date = dt.strptime(minuteBar['t'], DATE_FORMAT)
				minuteBar['t'] = _date.timestamp() #get_datetime(minuteBar['t'].replace("Z",""))#.timestamp()
				minuteBar['s'] = s

				if utcminute != _date:
					continue
				#insert_minute_bars(s,[minuteBar],True)

				bars.append(minuteBar)
				#price = latestTrade.get("p") or 0
				# if price:
				# 	sql = """ update tabSymbol set 
				# 	price=%s, 
				# 	volume=%s, 
				# 	1m_volume=%s,
				# 	today_high=%s, 
				# 	today_low=%s ,
				# 	today_open=%s ,
				# 	today_close=%s ,
				# 	today_trades=%s ,
				# 	bid=%s , 
				# 	ask=%s ,
				# 	vwap=%s , 
				# 	prev_day_open = %s ,
				# 	prev_day_close = %s , 
				# 	prev_day_high = %s ,
				# 	prev_day_low = %s , 
				# 	prev_day_vwap = %s ,
				# 	prev_day_volume = %s ,
				# 	prev_day_trades = %s 
				# 	where name='%s' """ % (
				# 				price or 0,
				# 				dailyBar.get("v") or 0,
				# 				minuteBar.get("v") or 0,
				# 				dailyBar.get("h") or 0,
				# 				dailyBar.get("l") or 0,
				# 				dailyBar.get("o") or 0,
				# 				dailyBar.get("c") or 0,
				# 				dailyBar.get("n") or 0,
				# 				latestQuote.get("bp") or 0,
				# 				latestQuote.get("ap") or 0,
				# 				minuteBar.get("vw") or 0,
				# 				prevDailyBar.get("o") or 0,
				# 				prevDailyBar.get("c") or 0,
				# 				prevDailyBar.get("h") or 0,
				# 				prevDailyBar.get("l") or 0,
				# 				prevDailyBar.get("vw") or 0,
				# 				prevDailyBar.get("v") or 0,
				# 				prevDailyBar.get("n") or 0,
				# 				s )
				# 	try:
				# 		sql = str(sql)
				# 		_cursor.execute(sql)

				# 	except Exception as e:
				# 		print(s,"error sql",e)
		if bars:
			#print("inserting",len(bars))
			insert_minute_bars(_cursor,bars,True)
			#_cursor.execute("commit")
		endcall = dt.now()
		print("DONE",len(bars),endcall-tcall,tcall-bcall)
	except Exception as e:
			print("error",e)
	finally:
		conn.close()
		_cursor = None
		conn = None

	
				
def backfill(days=0,symbols=None,daily=False ):
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
			#sleeptime = random.uniform(0, i)
			#time.sleep(i)
			print("start",i,start)
			tcall = dt.now()
			bars = api.get_barset(chunk_symbols,"minute",limit=1000,start=start)	
			#print(i,"BARS",len(bars))

			tstart = dt.now()
			if bars :
				for b in bars:
					_bars = bars[b]
					for a in _bars:
						a['s'] = b
						# a['n'] = 0
						# a['vw'] = 0.0
						#a['t'] = dt.utcfromtimestamp(a['t'])
						#minute_bars.append(a)
					#minute_bars.extend(_bars)
					#candles = [to_candle(a,b) for a in candles]
					if _bars:
						insert_minute_bars(_bars)
				tend = dt.now()
				print(i,"DONE","time:" ,tend-tstart,"api",tstart-tcall)

		except Exception as e:
			print("_insert ERROR",e)	

	def _insert_day(i,start,chunk_symbols):
		try:
			#sleeptime = random.uniform(0, i)
			#time.sleep(i)
			print("start",i,start)
			tcall = dt.now()
			bars = api.get_barset(chunk_symbols,"day",limit=1000 )	
			#print(i,"BARS",len(bars))

			tstart = dt.now()
			if bars :
				for b in bars:
					_bars = bars[b]
					for a in _bars:
						a['s'] = b
						# a['n'] = 0
						# a['vw'] = 0.0
						#a['t'] = dt.utcfromtimestamp(a['t'])
						#minute_bars.append(a)
					#minute_bars.extend(_bars)
					#candles = [to_candle(a,b) for a in candles]
					if _bars:
						insert_minute_bars(_bars,col="d")
				tend = dt.now()
				print(i,"DONE","time:" ,tend-tstart,"api",tstart-tcall)

		except Exception as e:
			print("_insert ERROR",e)	

	try:
		threads = 0
		_range =  range(days+1) 
		for d in _range:
			start =  add_days(dt.now(),-1*d) #-1*d
			if start.weekday() in [5,6]:
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


# def create_ta_table(symbols=None):
# 	collection.create_ta_table(symbols)

def init_bars_db(target = 0):
	print("init")
	day = target in [0,2]
	minute = target in [0,1]
	if minute:
		store.delete_collection("1MIN" )
		collection = store.collection("1MIN",overwrite=True)
	if day:
		store.delete_collection("1DAY")
		collection_day = store.collection("1DAY",overwrite=True)
	if minute:
		collection.create_table("data")
	if day:
		collection_day.create_table("data")



	#symbols = frappe.db.sql("""select symbol from tabSymbol where active=1 """,as_list=True)
	# symbols =  get_active_symbols()#[a[0] for a in symbols]
	# date = dt.now().replace(year=1990)
	# for idx,s in enumerate(symbols):
	# 	#if s not in items:
	# 	print(idx)
	# 	if minute:
	# 		collection.create_table(s)
	# 	if day:
	# 		collection_day.create_table(s)
	print("DONE")

	
#@multitasking.task 
def insert_minute_bars(cursor,minuteBars,send_last=False,col="m"):
	global bar_symbols
	if not minuteBars:
		print("not minuteBars")
		return
	# print("path",path)
	try:
		#_bars = minuteBars #[to_candle(a) for a in minuteBars ]
		# items = pd.DataFrame.from_dict(minuteBars)
		# items = items.astype(dtype= {
		# 	"s":"str",
		# 	"t":"int64", 
		# 	"o":"float64",
		# 	"c":"float64",
		# 	"h":"float64",
		# 	"l":"float64",
		# 	"n":"int64",
		# 	"v":"int64",
		# 	"vw":"float64",
		# 	})
		
		#if not items.empty :
		try:
			args = [(a['t'],a['o'],a['c'],a['h'],a['l'],a['v'],a['s']) for a in minuteBars]
			cursor.executemany("INSERT INTO tabBars (t,o,c,h,l,v,s) values(%s,%s,%s,%s,%s,%s,%s)",args)

		except Exception as ve:
			print("--- ValueError ---",ve)
		
		if send_last  :
			# if not isinstance(minuteBars,list):
			# 	minuteBars = [minuteBars]
			for ticker in minuteBars:
				s = ticker.get("s")
				if s in bar_symbols:
					ev  = "bars_%s"%  s.lower()
					add_to_queue(ev,ev,ticker)
		# else:
		# 	print(ticker,"empty")
	except Exception as e:
		print("insert_minute_bars ERROR",e)

@multitasking.task 
def add_to_queue(event,ev,last):
	#print("queue",event)
	queue_data(event,ev,last)


def get_minute_bars(symbol,timeframe,start,end=None ):
	if not (symbol and start):
		return
	
	#start = dt.utcfromtimestamp(start)
	#if end:
	#	end = dt.utcfromtimestamp(end)
	# try:
	# 	result = []
	# 	print("start",start)
	# 	# print("end",end)
	# 	_collection = None
	# 	if timeframe == "m":
	# 		_collection = collection#.item(symbol,filters=[('t','')])
	# 	else:
	# 		_collection = collection_day#.item(symbol)
		
	# 	# filters = []
	# 	# if start and end:
	# 	# 	filters =  't >= start & t <=end'
	# 	# else:
	# 	# 	filters = 't >= start '
			
		
	# 	data = _collection.item(symbol,start=start,end=end ).data()
	# 	if data:
	# 		#data = data[~data.t.duplicated(keep='first')]
	# 		result = data
	# 	return result
	# except Exception as ex:
	# 	print("ERROR get_minute_bars",ex)
	return []



