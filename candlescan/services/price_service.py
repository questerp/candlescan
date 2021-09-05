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
	print("START",i,dt.now())
	bcall = dt.now()
	snap = api.get_snapshots(symbols)
	tcall = dt.now()

	with get_connection() as cursor:
		bars = [ ]
		try:
			for s in snap:
				data = snap[s]
				if not data:
					continue
				minuteBar = data.get("minuteBar") 
				
				if minuteBar:
					_date = dt.strptime(minuteBar['t'], DATE_FORMAT)
					minuteBar['t'] = _date.timestamp() #get_datetime(minuteBar['t'].replace("Z",""))#.timestamp()
					minuteBar['s'] = s

					# if utcminute != _date:
					# 	continue
					#insert_minute_bars(s,[minuteBar],True)

					bars.append(minuteBar)
				
			if bars:
				insert_minute_bars(cursor,bars,True)
			endcall = dt.now()
			print("DONE",len(bars),endcall-tcall,tcall-bcall)
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
				print(len(bars))
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

	
def insert_minute_bars(cursor,minuteBars,send_last=False,col="m"):
	global bar_symbols
	if not minuteBars:
		print("not minuteBars")
		return
	try:

		try:
			table = "tabBarsday" if col=="d" else "tabBars"
			args = [(a['t'],a['o'],a['c'],a['h'],a['l'],a['v'],a['s']) for a in minuteBars]
			cursor.executemany("INSERT IGNORE INTO {0} (t,o,c,h,l,v,s) values(%s,%s,%s,%s,%s,%s,%s)".format(table),args)
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

@multitasking.task 
def add_to_queue(event,ev,last):
	queue_data(event,ev,last)


def get_minute_bars(symbol,timeframe,start,end=None ):
	if not (symbol and start and timeframe):
		return
	
	try:
		table = "tabBarsday" if timeframe=="d" else "tabBars"
		result = []
		print("start",start)
		sql = """select t,o,c,h,l,v from %s where s='%s' and  t>=%s""" % (table,symbol,start)
		if end:
			sql = sql + " and t<=%s"%end
		data = frappe.db.sql(sql,as_dict=True)
		if data:
			result = data
		return result
	except Exception as ex:
		print("ERROR get_minute_bars",ex)
		return []




