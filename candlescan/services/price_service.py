from __future__ import unicode_literals
import frappe, logging, time
from frappe.utils import cstr,add_days, get_datetime
from datetime import timezone,timedelta,datetime as dt
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder,keep_alive,queue_data
from candlescan.utils.candlescan import to_candle,get_active_symbols
from alpaca_trade_api import Stream
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import REST
import pandas as pd
import threading
import pystore
import multitasking

	 

sio = socketio.Client(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
lock = threading.Lock()
log = logging.getLogger(__name__)
api = None
store = pystore.store('bars')
collection = store.collection('1MIN')
collection_day = store.collection("1DAY")

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
		_start()
	except Exception as e:
		print(e)
		time.sleep(2)
		start()




def _start():
	connect()
	api = REST(raw_data=True)
	logging.basicConfig(level=logging.INFO)
	redis = get_redis_server()
	#s = frappe.db.sql(""" select symbol from tabSymbol where active=1""",as_list=True)
	symbols = get_active_symbols()
	minutedelta = timedelta(minutes=1)
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
			
		if dt.now().second != 5:
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
		snap = api.get_snapshots(symbols)
		print("get_snapshots DONE",dt.now())
		minuteBars = []
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
					#print("t",minuteBar['t'])
				
					
				if not minuteBar.get('t') or utcminute != minuteBar['t']:
					#print("continue",utcminute,minuteBar.get('t'))
					#input()
					continue
				vol = minuteBar.get("v") or 0
				minuteBar['s'] = s
				minuteBars.append(minuteBar)
				if s == "BPTH":
					print(minuteBar)
								
				price = latestTrade.get("p")
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
						frappe.db.sql(sql)
					except Exception as e:
						print("error sql",e)

			except Exception as e:
				print("error",e)
					
		frappe.db.commit()
		if minuteBars:
			insert_minute_bars(symbols,minuteBars,True)
		print("----> DONE",len(minuteBars),dt.now())
		
		#minuteBars = []	
		time.sleep(1)


	

def backfill(days=0):
	api = REST(raw_data=True)
	
	#all_symbols = frappe.db.sql("""select symbol from tabSymbol where active=1 """,as_list=True)
	#all_symbols = get_active_symbols()# [a[0] for a in all_symbols] 
	print("backfill",dt.now())
	chuck = 200
	limit = 1000
	TZ = 'America/New_York'
	
	#empty_candle = get_empty_candle()
	try:
		for d in range(days+1):
			start =  add_days(dt.now(),-1*d) #-1
			if start.weekday() in [5,6]:
				continue
			start = start.replace(second=0).replace(microsecond=0).replace(hour=4).replace(minute=0)	
			beg = pd.Timestamp(start, tz=TZ).isoformat()
			
			print("start",beg)
			i = 0
			for result in chunks(get_active_symbols(),chuck):
				i+=1
				bars = api.get_barset(result,"minute",limit=limit,start=beg)					
				minute_bars = []
				if bars :
					for b in bars:
						_bars = bars[b]
						for a in _bars:
							a['s'] = b
							a['t'] = dt.utcfromtimestamp(a['t'])
						minute_bars.extend(_bars)
						#candles = [to_candle(a,b) for a in candles]
					insert_minute_bars(result,minute_bars)
					print(len(minute_bars),"DONE - symbols:",i*chuck,"/" ,"start",beg)
				else:
					print("No data")
					
			
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
			collection = store.collection("1MIN",overwrite=True)
		if day:
			collection_day = store.collection("1DAY",overwrite=True)

		#symbols = frappe.db.sql("""select symbol from tabSymbol where active=1 """,as_list=True)
		symbols =  get_active_symbols()#[a[0] for a in symbols]
		df = pd.DataFrame([{"ticker":"","open":0,"close":0,"high":0,"low":0,"volume":0,"trades":0,"time":dt.now()}])
		df.set_index("time",inplace=True,drop=True)
		
		for s in symbols:
			#if s not in items:
			if minute:
				collection.write(s, df,overwrite=True)
			if day:
				collection_day.write(s, df,overwrite=True)
		print("DONE")
		#print(collection.list_items())
		#collection = store.collection("1DAY",overwrite=True)

		
	except Exception as e:
		print("init_bars_db",e)

	
#@multitasking.task 
def insert_minute_bars(tickers,minuteBars,send_last=False):
	if not minuteBars:
		return
	symbols = []
	if send_last:
		redis = get_redis_server()
		symbols = redis.smembers("symbols")
		if symbols:
			symbols = [cstr(a) for a in symbols]

	try:
		_bars = [to_candle(a) for a in minuteBars ]
		df = pd.DataFrame(_bars)
		#print("tickers",len(tickers))
		for ticker in tickers:
			items  = df.loc[df['ticker'].str.fullmatch(ticker, case=False )]
			last = None
			
			if not items.empty :
				#print(" items not empty", len(items))

				if send_last :
					last = items.iloc[-1].to_dict()
				items.set_index("time",inplace=True,drop=True)
				try:
					collection.append(ticker, items)
				except ValueError as ve:
					print(ticker,"--- ValueError ---",ve)
					collection.write(ticker, items,overwrite=True)

				if last and send_last and  ticker in symbols:
					ev  = "bars_%s"%  ticker.lower()
					queue_data(ev,ev,last)

	except Exception as e:
		print("insert_minute_bars ERROR",e)
		#input()
	
	
def get_minute_bars(symbol,timeframe,start,end=None):
	if not (symbol and start ):
		return
	start = dt.fromtimestamp(start)
	if not end:
		end = dt.utcnow()#.isoformat()
	else:
		end = dt.fromtimestamp(end)
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
			data = item.data.loc[(item.data.index>=start) & (item.data.index <=end)].compute()
			#print("data",data)
			data['time'] = data.index.astype(str)
			result = data.to_dict("records")
		return result
	except Exception as ex:
		print("ERROR get_minute_bars",ex)
		return []



