import frappe,json
from frappe.realtime import get_redis_server
from candlescan.candlescan_api import handle
from candlescan.platform import get_platform_data
from frappe.utils import cstr
import asyncio

async def handle(sid,data):
	action = data['action']
	user =  get_redis_server().hget("sockets",sid)
	if not user:
		return handle(False,"Connection lost")
	user = cstr(user)
	# return as handle always
	if action == "get_platform_data":
		return get_platform_data(user)
	if action == "get_extras":
		return handle(True,"Working on it")
	if action == "subscribe_bars":
		symbol = data['symbol']
		timeframe = data['timeframe']
		get_redis_server().hset("bars",{"sid":sid,"symbol":symbol,"timeframe":timeframe})
		return handle(True,"Working on it")

def broadcast(sid,data):
	pass
