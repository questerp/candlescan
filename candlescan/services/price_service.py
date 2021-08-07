from __future__ import unicode_literals
import frappe, logging, time
from frappe.utils import cstr
from datetime import datetime as dt
import socketio
import asyncio
from frappe.realtime import get_redis_server
from candlescan.utils.socket_utils import get_user,validate_data,build_response,json_encoder
from alpaca_trade_api import Stream
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import REST


log = logging.getLogger(__name__)
api = None

def start():
	logging.basicConfig(level=logging.INFO)
	api = REST(raw_data=True)
	redis = get_redis_server()
	counter = 0
	# init symbols 
	s = frappe.db.sql(""" select symbol from tabSymbol where active=1""",as_list=True)
	s = [a[0] for a in s]
	for sym in s:
		print("adding", sym)
		redis.sadd("symbols",sym)
	while(1):
		if dt.now().second != 1:
			time.sleep(1)
			continue
		counter += 1
		_symbols = redis.smembers("symbols")
		symbols = [cstr(a) for a in _symbols if a]
		print("1 min",symbols)
		if counter >=5:
			counter = 0
			__5m_symbols = redis.smembers("5m_symbols")
			_5m_symbols = [cstr(a) for a in __5m_symbols if a]
			if _5m_symbols:
				print("5 min",_5m_symbols)
				symbols.extend(_5m_symbols)
				symbols = list(set(symbols))
				
		snap = api.get_snapshots(symbols)
		m1s = []
		m5s = []
		for s in snap:
			data = snap[s]
			if not data:
				continue
			#print(data)
			minuteBar = data.get("minuteBar")
			latestTrade = data.get("latestTrade")
			latestQuote = data.get("latestQuote")
			dailyBar = data.get("dailyBar")
			prevDailyBar = data.get("prevDailyBar")
			if minuteBar:
				#minuteBar['doctype'] = "Bars"
				#minuteBar['s'] = s
				#frappe.get_doc(minuteBar).insert(ignore_permissions=True, ignore_if_duplicate=True, ignore_mandatory=True)
				
				# decide refresh rate
				vol = minuteBar.get("v") or 0
				if vol < 20000:
					m5s.append(s)
				else:
					m1s.append(s)
				
			if latestTrade and dailyBar:
				frappe.db.sql(""" update tabSymbol set 
				price=%s, 
				volume=%s, 
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
				prev_day_trades = %s ,
				
				where 
				name='%s'
				""" % 
					      (
						      latestTrade.get("p"),
						      dailyBar.get("v"),
						      dailyBar.get("h"),
						      dailyBar.get("l"),
						      dailyBar.get("o"),
						      dailyBar.get("c"),
						      dailyBar.get("n"),
						      latestQuote.get("bp"),
						      latestQuote.get("ap"),
						      minuteBar.get("vw"),
						      prevDailyBar.get("o"),
						      prevDailyBar.get("c"),
						      prevDailyBar.get("h"),
						      prevDailyBar.get("l"),
						      prevDailyBar.get("vw"),
						      prevDailyBar.get("v"),
						      prevDailyBar.get("n"),
						      s
					      ))
		frappe.db.commit()
		for s in m1s:
			redis.sadd("symbols",s)
			redis.srem("5m_symbols",s)
		for s in m5s:
			redis.srem("symbols",s)
			redis.sadd("5m_symbols",s)
		#time.sleep(60)
