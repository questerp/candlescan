import frappe,json
from frappe.realtime import get_redis_server
from candlescan.candlescan_api import handle
from candlescan.platform import get_platform_data
from frappe.utils import cstr
import asyncio
import socketio

sio = socketio.AsyncClient()

async run():
	await sio.connect('http://localhost:9002',auth:{"microservice":"broadcaster"})

@sio.event
async def from_client(server_sid,data):
	action = data['action']
	sid = data['source_sid']
	user =  get_redis_server().hget("sockets",sid)
	if not user:
		return handle(False,"Connection lost")
	user = cstr(user)
	# return as handle always
	
	if action == "get_platform_data":
		await sio.emit('to_client',{"client_sid":sid, "data":get_platform_data(user)})

	
	if action == "get_extras":
		return handle(True,"Working on it")
	
	if action == "subscribe_bars":
		symbol = data['symbol']
		timeframe = data['timeframe']
		get_redis_server().hset("bars",{"sid":sid,"symbol":symbol,"timeframe":timeframe})
		return handle(True,"Working on it")
	
	

def broadcast(sid,data):
	pass

asyncio.run(run())
