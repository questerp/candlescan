from __future__ import unicode_literals
import frappe, logging, time
from frappe.utils import cstr,add_days, get_datetime
from datetime import timedelta,datetime as dt
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder,keep_alive
from alpaca_trade_api import Stream
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import REST
import tables as tb
import numpy as np
import threading


class Symbol(tb.IsDescription):
	ticker = tb.StringCol(16)
	time = tb.Float64Col()
	open = tb.Float64Col()
	close = tb.Float64Col()
	high = tb.Float64Col()
	low = tb.Float64Col()
	volume = tb.Float64Col()
	trades = tb.Float64Col()
	valide = tb.BoolCol()
	 

sio = socketio.Client(logger=True,json=json_encoder, engineio_logger=True,reconnection=True, reconnection_attempts=10, reconnection_delay=1, reconnection_delay_max=5)
lock = threading.Lock()
log = logging.getLogger(__name__)
api = None
global_h5file =None

def connect():
	try:
		sio.connect('http://localhost:9002',headers={"microservice":"price_service"})
	except socketio.exceptions.ConnectionError as err:
		print("error",sio.sid,err)
		sio.sleep(5)
		connect()

def disconnect():
	print("I'm disconnected!")
	connect()
	
def start():
	connect()
	api = REST(raw_data=True)
	logging.basicConfig(level=logging.INFO)
	redis = get_redis_server()
	#counter = 0
	# init symbols 
	s = frappe.db.sql(""" select symbol from tabSymbol where active=1""",as_list=True)
	symbols = [a[0] for a in s]
	#for sym in s:
	#	#print("adding", sym)
	#	redis.sadd("1m_symbols",sym)
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
			
		if dt.now().second != 1:
			time.sleep(1)
			continue
		frappe.db.sql("select 'KEEP_ALIVE'")
		print("------------")
		print(dt.now())

		snap = api.get_snapshots(symbols)
		print(len(symbols),dt.now())
		#m1s = []
		#m5s = []
		minuteBars = []
		for s in snap:
			data = snap[s]
			if not data:
				#m5s.append(s)
				continue
			#print(data)
			minuteBar = data.get("minuteBar") or {}
			latestTrade = data.get("latestTrade") or {}
			latestQuote = data.get("latestQuote") or {}
			dailyBar = data.get("dailyBar") or {}
			prevDailyBar = data.get("prevDailyBar")  or {}
			#minuteBar['doctype'] = "Bars"
			#minuteBar['s'] = s
			#frappe.get_doc(minuteBar).insert(ignore_permissions=True, ignore_if_duplicate=True, ignore_mandatory=True)

			# decide refresh rate
			vol = minuteBar.get("v") or 0
			#if vol >= 0:
			#	m1s.append(s)
			#else:
			#	m5s.append(s)
			if minuteBar and minuteBar.get("t"):
				minuteBar['s'] = s
				try:
					minuteBar['t'] = get_datetime(minuteBar['t']).timestamp()
					minuteBars.append(minuteBar)
				except:
					pass
				
			price = latestTrade.get("p")
			if price:
				#if s in sub_symbols:
				#	sio.emit("transfer",build_response("symbol",s,{
				#		"symbol":s,
				#		"price":price
				#	}))
				
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
				#print(sql)
				frappe.db.sql(sql)
				
		frappe.db.commit()
		#for s in m1s:
		#	redis.sadd("1m_symbols",s)
		#	redis.srem("5m_symbols",s)
		#for s in m5s:
		#	redis.srem("1m_symbols",s)
		#	redis.sadd("5m_symbols",s)
		if minuteBars:
			#try:
			insert_minute_bars(minuteBars,False)
		frappe.db.commit()
		minuteBars = []	
		print("DONE",dt.now())
		
		# fetching backfill
		
		
		
		time.sleep(2)
		#time.sleep(60)

def backfill():
	api = REST(raw_data=True)
	now = dt.now()
	start = add_days(dt.now(),-2)
	start = start.replace(second=0).replace(microsecond=0)
	all_symbols = frappe.db.sql("""select symbol from tabSymbol where active=1 """,as_list=True)
	all_symbols = [a[0] for a in all_symbols] 
	print("backfill",len(all_symbols),dt.now())
	h5file = get_h5file()
	table = h5file.root.bars_group.bars
	chuck = 400
	
	try:
		while(start<now):
			start = start + timedelta(minutes=1)
			print("start",start)
			if start.hour >= 4 or start.hour <= 20:

				exist_symbols = [ x['ticker'] for x in table.where("""(time == %s)""" % start.timestamp()) ]
				#exist_symbols = frappe.db.sql(""" select DISTINCT s from tabBars where t='%s'""" % start,as_list=True)

				#if exist_symbols:
				#	exist_symbols = [a[0] for a in exist_symbols]
				#else:
				#	exist_symbols = []
				allresult = [a for a in all_symbols if a not in exist_symbols]
				i = 0
				print("exist_symbols",len(exist_symbols))
				end = start + timedelta(minutes=1000)
				for result in chunks(allresult,chuck):
					i+=1
					strstart = start.astimezone().isoformat()
					print(strstart)
					bars = api.get_barset(result,"minute",limit=1000,start=strstart)					
					minute_bars = []
					if bars :
						for b in bars:
							candles = bars[b]
							print(b,len(candles))
							#for  c in candles:
							#	print(dt.fromtimestamp(c['t']))
							for m in range(1000):
								current = start +  timedelta(minutes=m)
								ts = current.timestamp()
								candle = list(filter(lambda x: x['t'] == ts, candles))
								#print(ts,candles[0])
								if candle:
									candle = candle[0]
									#print("candle",candle)

									#candle['t'] = cstr(dt.fromtimestamp(candle['t']))
									candle['s'] = b
									candle['vw'] = 0
									candle['n'] = 0
								else:
									#print("no candle",current)
									candle = {
										"s":b,
										"t": ts,
										"o":0,
										"c":0,
										"h":0,
										"l":0,
										"n":0,
										"v":0,
										"vw":0,
									}
								minute_bars.append(candle)
								#time.sleep(1)
								#start = start +  timedelta(minutes=1)

						print(len(minute_bars),"DONE - symbols:",i*chuck,"/",len(allresult),"between",start,"-",end)
						insert_minute_bars(minute_bars,True)
						minute_bars = []
						bars = None
						frappe.db.sql("select 'KEEP_ALIVE'")

				start = end
	except:
		print("ERROR")
	finally:
		table.flush()
		synchronized_close_file()
			
				
def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))	

def init_bars_db():
	print("init")
	h5file = get_h5file()
	group = h5file.create_group("/", 'bars_group', 'Candlebars')
	table = h5file.create_table(group, 'bars', Symbol, "1 minute Candlebars")
	indexrows = table.cols.time.create_index()
	indexrows = table.cols.ticker.create_index()
	indexrows = table.cols.valide.create_index()
	
	table.flush()
	print(h5file)
	synchronized_close_file()
	
	

def insert_minute_bars(minuteBars,commit=True):
	if not minuteBars:
		return
	h5file = get_h5file()
	table = h5file.root.bars_group.bars
	
	try:
		symbol = table.row
		for bar in minuteBars:
			#print(bar)

			symbol['ticker'] = bar['s']
			symbol['time'] = bar['t']
			symbol['open'] = bar['o']
			symbol['close'] = bar['c']
			symbol['high'] = bar['h']
			symbol['low'] = bar['l']
			symbol['volume'] = bar['v']
			symbol['trades'] = bar['n']
			symbol['valide'] = symbol['open'] > 0
			symbol.append()
	except:
		print("ERROR")
	finally:
		table.flush()
		synchronized_close_file()
	
	#frappe.db.sql("""SET @@session.unique_checks = 0""")
	#frappe.db.sql("""SET @@session.foreign_key_checks = 0""")
	#frappe.db.sql("""INSERT IGNORE INTO `tabBars` (name,s,t,o,h,l,c,v,n,vw)
	#VALUES {values}""".format(values = ", ".join(["('%s_%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (s['s'],s['t'],s['s'],s['t'],s['o'],s['h'],s['l'],s['c'],s['v'],s['n'],s['vw']) for s in minuteBars])))
	#if commit:
		
	#	frappe.db.commit()
	
def get_minute_bars(symbol,start,end=None):
	if not (symbol and start ):
		return
	h5file = get_h5file()
	table = h5file.root.bars_group.bars
	if not end:
		end = dt.now().timestamp()
	try:
		data = [ x[:] for x in table.where("""(ticker == '%s') & (time>=%s) & (time<=%s) & (valide)""" % (symbol,start,end) ) ]
		return data
	except Exception as ex:
		print("ERROR",ex)
		return []

		
def get_h5file():
	global global_h5file
	if not  global_h5file or not global_h5file.isopen:
		with lock:
			global_h5file = tb.open_file("bars.h5", mode="a", title="Bars")
	return global_h5file

def synchronized_close_file():
    with lock:
        get_h5file().close()
