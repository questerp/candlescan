import frappe,json
from frappe.realtime import get_redis_server
from candlescan.candlescan_api import handle
from candlescan.platform import get_platform_data
from frappe.utils import cstr
import asyncio
import socketio

sio = socketio.AsyncClient()

async def run():
	await sio.connect('http://localhost:9002',auth={"microservice":"broadcaster"})
	await sio.emit("join", "platform")
	await sio.wait()
	
@sio.event
async def from_client(data):
	pass
	
	

def broadcast(sid,data):
	pass

asyncio.get_event_loop().run_until_complete(run())
