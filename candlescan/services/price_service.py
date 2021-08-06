from __future__ import unicode_literals
import frappe, logging, time
from frappe.utils import cstr
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
	while(1):
		_symbols = redis.smembers("symbols")
		symbols = [cstr(a) for a in _symbols if a]
		print(symbols)
		snap = api.get_snapshots(symbols)
		for s in snap:
			data = snap[s]
			if not data:
				continue
			print(data)
			minuteBar = data.get("minuteBar")
			latestTrade = data.get("latestTrade")
			dailyBar = data.get("dailyBar")
			if minuteBar:
				minuteBar['doctype'] = "Bars"
				frappe.get_doc(minuteBar).insert(ignore_permissions=True, ignore_if_duplicate=True, ignore_mandatory=True)
				
			if latestTrade and dailyBar:
				frappe.db.sql(""" update tabSymbol set price=%s, volume=%s where name='%s'""" % (latestTrade.get("p"),dailyBar.get("v"),s))
		frappe.db.commit()
		time.sleep(60)
